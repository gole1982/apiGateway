package db

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"gateway/internal/crypto"
	"gateway/internal/models"
)

type DB struct {
	conn *sql.DB
	mu   sync.RWMutex
}

var instance *DB

func Init() error {
	if err := crypto.Init(); err != nil {
		return fmt.Errorf("crypto init: %w", err)
	}

	exePath, err := getExecutableDir()
	if err != nil {
		return err
	}
	dbPath := filepath.Join(exePath, "gateway.db")

	// _busy_timeout=5000: wait up to 5 s before returning SQLITE_BUSY instead of
	// failing immediately. This prevents log writes from being silently dropped when
	// the gateway goroutines (logger worker, request handler, scheduler) contend on
	// the same SQLite file.
	dsn := dbPath + "?_busy_timeout=5000"
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	// Single writer: SQLite supports only one concurrent writer. With the default
	// pool (MaxOpenConns=unlimited) multiple goroutines can queue concurrent writes
	// that all hit the busy lock. Limiting to 1 open connection serialises all DB
	// access and eliminates "database is locked" errors entirely.
	conn.SetMaxOpenConns(1)

	// Enable foreign key enforcement (SQLite defaults to OFF)
	if _, err = conn.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable foreign keys: %w", err)
	}

	instance = &DB{conn: conn}

	_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS platform (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			base_url TEXT NOT NULL,
			url_auto_complete INTEGER NOT NULL DEFAULT 1,
			token TEXT NOT NULL DEFAULT '',
			last_token_fetch DATETIME,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			notes TEXT NOT NULL DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS platform_keys (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			platform_id INTEGER NOT NULL,
			key_index INTEGER NOT NULL DEFAULT 0,
			token TEXT NOT NULL DEFAULT '',
			label TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
			UNIQUE(platform_id, key_index)
		);

		CREATE TABLE IF NOT EXISTS rapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL UNIQUE,
			model TEXT NOT NULL DEFAULT '',
			platform_id INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			unavailable_reason TEXT NOT NULL DEFAULT '',
			base_cost INTEGER NOT NULL DEFAULT 0,
			high_cost INTEGER NOT NULL DEFAULT 0,
			rpm_limit INTEGER NOT NULL DEFAULT 0,
			rph_limit INTEGER NOT NULL DEFAULT 0,
			rpd_limit INTEGER NOT NULL DEFAULT 0,
			tpm_limit INTEGER NOT NULL DEFAULT 0,
			tph_limit INTEGER NOT NULL DEFAULT 0,
			tpd_limit INTEGER NOT NULL DEFAULT 0,
			time_period_rules TEXT DEFAULT '',
			supported_formats TEXT NOT NULL DEFAULT '["openai"]',
			custom_headers TEXT NOT NULL DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS lapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL UNIQUE,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS lapi_rapi_order (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			lapi_id INTEGER NOT NULL,
			rapi_id INTEGER NOT NULL,
			order_index INTEGER NOT NULL,
			FOREIGN KEY (lapi_id) REFERENCES lapi(id) ON DELETE CASCADE,
			FOREIGN KEY (rapi_id) REFERENCES rapi(id) ON DELETE CASCADE,
			UNIQUE(lapi_id, rapi_id),
			UNIQUE(lapi_id, order_index)
		);

		CREATE TABLE IF NOT EXISTS token_cache (
			platform_key_id INTEGER PRIMARY KEY,
			token TEXT NOT NULL,
			expires_at DATETIME,
			fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_key_id) REFERENCES platform_keys(id) ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS rapi_metrics (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			rapi_id INTEGER NOT NULL,
			lapi_id INTEGER NOT NULL,
			total_requests INTEGER DEFAULT 0,
			success_requests INTEGER DEFAULT 0,
			fail_401 INTEGER DEFAULT 0,
			fail_429 INTEGER DEFAULT 0,
			fail_500 INTEGER DEFAULT 0,
			fail_other INTEGER DEFAULT 0,
			total_latency_ms INTEGER DEFAULT 0,
			token_count INTEGER DEFAULT 0,
			last_used DATETIME,
			FOREIGN KEY (rapi_id) REFERENCES rapi(id) ON DELETE CASCADE,
			FOREIGN KEY (lapi_id) REFERENCES lapi(id) ON DELETE CASCADE,
			UNIQUE(rapi_id, lapi_id)
		);

		CREATE INDEX IF NOT EXISTS idx_rapi_metrics_rapi ON rapi_metrics(rapi_id);
		CREATE INDEX IF NOT EXISTS idx_rapi_metrics_lapi ON rapi_metrics(lapi_id);

		CREATE TABLE IF NOT EXISTS request_trends (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			minute_bucket TEXT NOT NULL,
			lapi_id INTEGER NOT NULL DEFAULT 0,
			request_count INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(minute_bucket, lapi_id)
		);
		CREATE INDEX IF NOT EXISTS idx_trends_minute ON request_trends(minute_bucket);
	`)
	if err != nil {
		return err
	}

	// Migration: if old rapi table had url/token columns, migrate them to platform
	if err := instance.migrateOldRAPISchema(); err != nil {
		return err
	}

	// Create indexes after migration (rapi table may have been recreated)
	_, err = conn.Exec(`
		CREATE INDEX IF NOT EXISTS idx_rapi_platform ON rapi(platform_id);
	`)
	if err != nil {
		return err
	}

	// Migration: add enabled/available columns if missing
	instance.migrateAddStatusColumns()
	instance.migrateAddCostColumns()
	instance.migrateAddSupportedFormatsColumn()
	instance.migrateAddNotesColumn()
	instance.migrateAddLAPIEnabledColumn()
	instance.migrateAddCustomHeadersColumn()
	instance.migrateAddURLAutoCompleteColumn()
	// Migration: platform_keys table + migrate existing platform.token
	if err := instance.migrateAddPlatformKeys(); err != nil {
		return fmt.Errorf("migrateAddPlatformKeys: %w", err)
	}
	// Migration: token_cache rename primary key column (platform_id → platform_key_id)
	if err := instance.migrateTokenCacheToPlatformKeyID(); err != nil {
		return fmt.Errorf("migrateTokenCache: %w", err)
	}
	// Migration: add unavailable_reason column to rapi table
	instance.migrateAddRAPIUnavailableReason()
	// Migration: add notes column to rapi and lapi tables
	instance.migrateAddRAPINotesColumn()
	instance.migrateAddLAPINotesColumn()
	// Migration: add custom_headers column to platform table
	instance.migrateAddPlatformCustomHeadersColumn()
	// Migration: change rapi.alias uniqueness from global to per-platform
	if err := instance.migrateRAPIUniqueAliasToPerPlatform(); err != nil {
		return fmt.Errorf("migrateRAPIUniqueAliasToPerPlatform: %w", err)
	}
	// Migration: encrypt existing plaintext tokens with AES-256-GCM
	if err := instance.migrateEncryptTokens(); err != nil {
		return fmt.Errorf("migrateEncryptTokens: %w", err)
	}

	// Cleanup: remove orphan RAPIs whose platform no longer exists
	instance.cleanupOrphanedRAPIs()

	return nil
}

// migrateOldRAPISchema checks if the old schema exists (rapi.url column) and migrates data
func (db *DB) migrateOldRAPISchema() error {
	// Check if old url column exists
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name='url'`)
	var count int
	if err := row.Scan(&count); err != nil || count == 0 {
		return nil // Already migrated or clean install
	}

	// Check if token column exists
	row = db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name='token'`)
	var tokenCount int
	if err := row.Scan(&tokenCount); err != nil || tokenCount == 0 {
		return nil
	}

	// Migrate: create platforms from unique (url, token) combos, then update rapi
	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.Query(`SELECT DISTINCT url, token, is_dynamic, token_command FROM rapi`)
	if err != nil {
		return err
	}

	type oldPlatformData struct {
		url       string
		token     string
		isDynamic int
		tokenCmd  sql.NullString
	}
	var platforms []oldPlatformData
	for rows.Next() {
		var p oldPlatformData
		if err := rows.Scan(&p.url, &p.token, &p.isDynamic, &p.tokenCmd); err != nil {
			rows.Close()
			return err
		}
		platforms = append(platforms, p)
	}
	rows.Close()

	for _, p := range platforms {
		_, err = tx.Exec(`
			INSERT OR IGNORE INTO platform (name, base_url, token, is_dynamic, token_command)
			VALUES (?, ?, ?, ?, ?)
		`, p.url, p.url, p.token, p.isDynamic, p.tokenCmd)
		if err != nil {
			return err
		}

		var platformID int64
		err = tx.QueryRow(`SELECT id FROM platform WHERE base_url = ?`, p.url).Scan(&platformID)
		if err != nil {
			return err
		}

		_, err = tx.Exec(`UPDATE rapi SET platform_id = ? WHERE url = ?`, platformID, p.url)
		if err != nil {
			return err
		}
	}

	// Drop old columns (SQLite doesn't support DROP COLUMN before 3.35, so recreate table)
	_, err = tx.Exec(`
		CREATE TABLE rapi_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL UNIQUE,
			model TEXT NOT NULL DEFAULT '',
			platform_id INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE
		);
		INSERT INTO rapi_new (id, alias, model, platform_id, created_at, updated_at)
			SELECT id, alias, model, platform_id, created_at, updated_at FROM rapi;
		DROP TABLE rapi;
		ALTER TABLE rapi_new RENAME TO rapi;
		CREATE INDEX IF NOT EXISTS idx_rapi_platform ON rapi(platform_id);
	`)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (db *DB) migrateAddStatusColumns() {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if platform has enabled column
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform') WHERE name='enabled'`)
	var count int
	if err := row.Scan(&count); err == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE platform ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1`)
		db.conn.Exec(`ALTER TABLE platform ADD COLUMN available INTEGER NOT NULL DEFAULT 1`)
	}

	// Check if rapi has enabled column
	row = db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name='enabled'`)
	if err := row.Scan(&count); err == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE rapi ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1`)
		db.conn.Exec(`ALTER TABLE rapi ADD COLUMN available INTEGER NOT NULL DEFAULT 1`)
	}
}

func (db *DB) migrateAddCostColumns() {
	columns := map[string]string{
		"base_cost":         "INTEGER NOT NULL DEFAULT 0",
		"high_cost":         "INTEGER NOT NULL DEFAULT 0",
		"rpm_limit":         "INTEGER NOT NULL DEFAULT 0",
		"rph_limit":         "INTEGER NOT NULL DEFAULT 0",
		"rpd_limit":         "INTEGER NOT NULL DEFAULT 0",
		"tpm_limit":         "INTEGER NOT NULL DEFAULT 0",
		"tph_limit":         "INTEGER NOT NULL DEFAULT 0",
		"tpd_limit":         "INTEGER NOT NULL DEFAULT 0",
		"time_period_rules": "TEXT DEFAULT ''",
	}
	for name, definition := range columns {
		var count int
		row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name=?`, name)
		if row.Scan(&count) == nil && count == 0 {
			db.conn.Exec("ALTER TABLE rapi ADD COLUMN " + name + " " + definition)
		}
	}
}

func (db *DB) migrateAddSupportedFormatsColumn() {
	// Add supported_formats column to rapi if missing
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name='supported_formats'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE rapi ADD COLUMN supported_formats TEXT NOT NULL DEFAULT '["openai"]'`)
	}
	// Fix any NULL or empty supported_formats values from older schemas
	db.conn.Exec(`UPDATE rapi SET supported_formats = '["openai"]' WHERE supported_formats IS NULL OR supported_formats = ''`)
}

func (db *DB) migrateAddNotesColumn() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform') WHERE name='notes'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE platform ADD COLUMN notes TEXT NOT NULL DEFAULT ''`)
	}
}

func (db *DB) migrateAddLAPIEnabledColumn() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('lapi') WHERE name='enabled'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE lapi ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1`)
	}
}

func (db *DB) migrateAddCustomHeadersColumn() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name='custom_headers'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE rapi ADD COLUMN custom_headers TEXT NOT NULL DEFAULT ''`)
	}
}

func (db *DB) migrateAddURLAutoCompleteColumn() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform') WHERE name='url_auto_complete'`)
	if row.Scan(&count) == nil && count == 0 {
		// Default 1 (true) — preserve existing behaviour for all current platforms.
		db.conn.Exec(`ALTER TABLE platform ADD COLUMN url_auto_complete INTEGER NOT NULL DEFAULT 1`)
	}
}

// migrateAddPlatformKeys creates the platform_keys table if not present, then copies
// existing platform.token values as key_index=0 entries (idempotent).
func (db *DB) migrateAddPlatformKeys() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check whether platform_keys table already exists.
	var cnt int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='platform_keys'`)
	if err := row.Scan(&cnt); err != nil {
		return err
	}
	if cnt == 0 {
		// Table was not created by the initial CREATE TABLE IF NOT EXISTS above
		// (happens when upgrading an existing DB). Create it now.
		_, err := db.conn.Exec(`
			CREATE TABLE IF NOT EXISTS platform_keys (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				platform_id INTEGER NOT NULL,
				key_index INTEGER NOT NULL DEFAULT 0,
				token TEXT NOT NULL DEFAULT '',
				label TEXT NOT NULL DEFAULT '',
				enabled INTEGER NOT NULL DEFAULT 1,
				created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
				updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
				FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
				UNIQUE(platform_id, key_index)
			);
			CREATE INDEX IF NOT EXISTS idx_platform_keys_platform ON platform_keys(platform_id);
		`)
		if err != nil {
			return err
		}
	} else {
		// Ensure index exists even if table was created earlier without it.
		db.conn.Exec(`CREATE INDEX IF NOT EXISTS idx_platform_keys_platform ON platform_keys(platform_id)`)
	}

	// Migrate existing platform.token → platform_keys row with key_index=0.
	// Only insert where no key_index=0 row exists yet.
	_, err := db.conn.Exec(`
		INSERT OR IGNORE INTO platform_keys (platform_id, key_index, token, label, enabled)
		SELECT id, 0, token, 'default', 1 FROM platform WHERE token != ''
	`)
	return err
}

// migrateTokenCacheToPlatformKeyID migrates the old token_cache table
// (keyed by platform_id) to the new schema (keyed by platform_key_id).
func (db *DB) migrateTokenCacheToPlatformKeyID() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if old column 'platform_id' still exists on token_cache.
	var cnt int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('token_cache') WHERE name='platform_id'`)
	if err := row.Scan(&cnt); err != nil {
		return err
	}
	if cnt == 0 {
		// Already migrated or new install.
		return nil
	}

	// Recreate token_cache with new primary key referencing platform_keys.
	_, err := db.conn.Exec(`
		DROP TABLE IF EXISTS token_cache_old;
		ALTER TABLE token_cache RENAME TO token_cache_old;
		CREATE TABLE token_cache (
			platform_key_id INTEGER PRIMARY KEY,
			token TEXT NOT NULL,
			expires_at DATETIME,
			fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_key_id) REFERENCES platform_keys(id) ON DELETE CASCADE
		);
		-- Migrate rows: join old platform_id to the first key of each platform.
		INSERT OR IGNORE INTO token_cache (platform_key_id, token, expires_at, fetched_at)
		SELECT pk.id, tc.token, tc.expires_at, tc.fetched_at
		FROM token_cache_old tc
		INNER JOIN platform_keys pk ON pk.platform_id = tc.platform_id AND pk.key_index = 0;
		DROP TABLE token_cache_old;
	`)
	return err
}

func (db *DB) cleanupOrphanedRAPIs() {
	// Remove lapi_rapi_order entries for orphaned RAPIs (platform doesn't exist)
	db.conn.Exec(`
		DELETE FROM lapi_rapi_order WHERE rapi_id IN (
			SELECT r.id FROM rapi r
			LEFT JOIN platform p ON r.platform_id = p.id
			WHERE p.id IS NULL
		)
	`)
	// Remove rapi_metrics entries for orphaned RAPIs
	db.conn.Exec(`
		DELETE FROM rapi_metrics WHERE rapi_id IN (
			SELECT r.id FROM rapi r
			LEFT JOIN platform p ON r.platform_id = p.id
			WHERE p.id IS NULL
		)
	`)
	// Remove orphaned RAPIs themselves
	result, err := db.conn.Exec(`
		DELETE FROM rapi WHERE id IN (
			SELECT r.id FROM rapi r
			LEFT JOIN platform p ON r.platform_id = p.id
			WHERE p.id IS NULL
		)
	`)
	if err == nil {
		if rows, _ := result.RowsAffected(); rows > 0 {
			log.Printf("[DB] cleaned up %d orphaned RAPIs (platform no longer exists)", rows)
		}
	}
}

func Get() *DB {
	return instance
}

func (db *DB) Conn() *sql.DB {
	return db.conn
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func getExecutableDir() (string, error) {
	exePath, err := getExecutablePath()
	if err != nil {
		return "", err
	}
	return filepath.Dir(exePath), nil
}

func getExecutablePath() (string, error) {
	return exePath()
}

var exePath = func() (string, error) {
	return "", nil
}

func SetExePath(path string) {
	exePath = func() (string, error) {
		return path, nil
	}
}

// ============ Platform Operations ============

func (db *DB) GetPlatforms() ([]models.Platform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT id, name, base_url, url_auto_complete, token,
		       last_token_fetch, enabled, available, notes, custom_headers, created_at, updated_at
		FROM platform ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	platforms := make([]models.Platform, 0)
	for rows.Next() {
		var p models.Platform
		var enabled, available, urlAutoComplete sql.NullInt64
		var lastFetch, created, updated sql.NullTime
		var notes, customHeaders sql.NullString

		var encToken string
		err := rows.Scan(&p.ID, &p.Name, &p.BaseURL, &urlAutoComplete, &encToken,
			&lastFetch, &enabled, &available, &notes, &customHeaders, &created, &updated)
		if err != nil {
			return nil, err
		}
		if p.Token, err = crypto.Decrypt(encToken); err != nil {
			return nil, fmt.Errorf("decrypt platform %d token: %w", p.ID, err)
		}

		p.URLAutoComplete = urlAutoComplete.Int64 != 0
		p.Enabled = enabled.Int64 != 0
		p.Available = available.Int64 != 0
		p.Notes = notes.String
		p.CustomHeaders = customHeaders.String
		if lastFetch.Valid {
			p.LastTokenFetch = lastFetch.Time
		}
		if created.Valid {
			p.CreatedAt = created.Time
		}
		if updated.Valid {
			p.UpdatedAt = updated.Time
		}

		platforms = append(platforms, p)
	}
	return platforms, nil
}

func (db *DB) GetPlatformByID(id int64) (*models.Platform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var p models.Platform
	var enabled, available, urlAutoComplete sql.NullInt64
	var lastFetch, created, updated sql.NullTime
	var notes, customHeaders sql.NullString

	var encToken string
	err := db.conn.QueryRow(`
		SELECT id, name, base_url, url_auto_complete, token,
		       last_token_fetch, enabled, available, notes, custom_headers, created_at, updated_at
		FROM platform WHERE id = ?
	`, id).Scan(&p.ID, &p.Name, &p.BaseURL, &urlAutoComplete, &encToken,
		&lastFetch, &enabled, &available, &notes, &customHeaders, &created, &updated)

	if err != nil {
		return nil, err
	}
	if p.Token, err = crypto.Decrypt(encToken); err != nil {
		return nil, fmt.Errorf("decrypt platform %d token: %w", id, err)
	}

	p.URLAutoComplete = urlAutoComplete.Int64 != 0
	p.Enabled = enabled.Int64 != 0
	p.Available = available.Int64 != 0
	p.Notes = notes.String
	p.CustomHeaders = customHeaders.String
	if lastFetch.Valid {
		p.LastTokenFetch = lastFetch.Time
	}
	if created.Valid {
		p.CreatedAt = created.Time
	}
	if updated.Valid {
		p.UpdatedAt = updated.Time
	}

	return &p, nil
}

func (db *DB) CreatePlatform(p *models.Platform) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	encToken, err := crypto.Encrypt(p.Token)
	if err != nil {
		return fmt.Errorf("encrypt token: %w", err)
	}

	result, err := db.conn.Exec(`
		INSERT INTO platform (name, base_url, url_auto_complete, token, last_token_fetch, enabled, available, notes, custom_headers)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, p.Name, p.BaseURL, boolToInt(p.URLAutoComplete), encToken, time.Now(), boolToInt(p.Enabled), boolToInt(p.Available), p.Notes, p.CustomHeaders)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	p.ID = id
	return nil
}

func (db *DB) UpdatePlatform(p *models.Platform) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	encToken, err := crypto.Encrypt(p.Token)
	if err != nil {
		return fmt.Errorf("encrypt token: %w", err)
	}

	_, err = db.conn.Exec(`
		UPDATE platform SET name = ?, base_url = ?, url_auto_complete = ?, token = ?, enabled = ?, available = ?, notes = ?, custom_headers = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, p.Name, p.BaseURL, boolToInt(p.URLAutoComplete), encToken, boolToInt(p.Enabled), boolToInt(p.Available), p.Notes, p.CustomHeaders, p.ID)

	return err
}

func (db *DB) DeletePlatform(id int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check for child RAPIs
	var rapiCount int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM rapi WHERE platform_id = ?", id).Scan(&rapiCount)
	if err != nil {
		return fmt.Errorf("检查子模型失败: %w", err)
	}
	if rapiCount > 0 {
		// Get the aliases for the error message
		rows, err := db.conn.Query("SELECT alias FROM rapi WHERE platform_id = ?", id)
		if err != nil {
			return fmt.Errorf("该平台下还有 %d 个模型，请先删除所有模型", rapiCount)
		}
		defer rows.Close()
		var aliases []string
		for rows.Next() {
			var alias string
			if err := rows.Scan(&alias); err == nil {
				aliases = append(aliases, alias)
			}
		}
		return fmt.Errorf("该平台下还有 %d 个模型，请先删除: %s", rapiCount, strings.Join(aliases, ", "))
	}

	_, err = db.conn.Exec("DELETE FROM platform WHERE id = ?", id)
	return err
}

func (db *DB) UpdatePlatformToken(id int64, token string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	encToken, err := crypto.Encrypt(token)
	if err != nil {
		return fmt.Errorf("encrypt token: %w", err)
	}

	now := time.Now()
	_, err = db.conn.Exec(`
		UPDATE platform SET token = ?, last_token_fetch = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, encToken, now, id)
	if err != nil {
		return err
	}

	// Upsert key_index=0 so the gateway key-scheduler always has a row to work with
	// even if this platform was created before platform_keys existed.
	if token != "" {
		if _, upsertErr := db.conn.Exec(`
			INSERT INTO platform_keys (platform_id, key_index, token, label, enabled)
			VALUES (?, 0, ?, 'dynamic', 1)
			ON CONFLICT(platform_id, key_index) DO UPDATE SET token=excluded.token, updated_at=CURRENT_TIMESTAMP
		`, id, encToken); upsertErr != nil {
			return fmt.Errorf("upsert platform key failed: %w", upsertErr)
		}
	}

	return nil
}

// ============ RAPI Operations ============

func (db *DB) GetRAPIs() ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       0 AS order_index, r.supported_formats, r.custom_headers, p.url_auto_complete, r.notes, p.custom_headers
		FROM rapi r
		LEFT JOIN platform p ON r.platform_id = p.id
		ORDER BY r.id ASC
	`)
}

func (db *DB) GetRAPIByID(id int64) (*models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	results, err := db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       0 AS order_index, r.supported_formats, r.custom_headers, p.url_auto_complete, r.notes, p.custom_headers
		FROM rapi r
		LEFT JOIN platform p ON r.platform_id = p.id
		WHERE r.id = ?
	`, id)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, sql.ErrNoRows
	}
	return &results[0], nil
}

func (db *DB) queryRAPIsWithPlatform(query string, args ...interface{}) ([]models.RAPIWithPlatform, error) {
	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rapis := make([]models.RAPIWithPlatform, 0)
	for rows.Next() {
		var r models.RAPIWithPlatform
		var rEnabled, rAvailable, urlAutoComplete sql.NullInt64
		var lastFetch, created, updated sql.NullTime
		var pName, pURL, pToken sql.NullString
		var timePeriodRules, supportedFormats, customHeaders, unavailableReason, notes, platformCustomHeaders sql.NullString

		err := rows.Scan(&r.ID, &r.Alias, &r.Model, &r.PlatformID, &rEnabled, &rAvailable, &unavailableReason,
			&r.BaseCost, &r.HighCost, &r.RPMLimit, &r.RPHLimit, &r.RPDLimit, &r.TPMLimit, &r.TPHLimit, &r.TPDLimit, &timePeriodRules, &created, &updated,
			&pName, &pURL, &pToken, &lastFetch, &r.OrderIndex, &supportedFormats, &customHeaders, &urlAutoComplete, &notes, &platformCustomHeaders)
		if err != nil {
			return nil, err
		}

		r.Enabled = rEnabled.Int64 != 0 // default to true if NULL
		r.Available = rAvailable.Int64 != 0
		r.UnavailableReason = unavailableReason.String
		r.URLAutoComplete = urlAutoComplete.Int64 != 0
		r.TimePeriodRules = timePeriodRules.String
		r.SupportedFormats = supportedFormats.String
		r.CustomHeaders = customHeaders.String
		r.Notes = notes.String
		r.PlatformCustomHeaders = platformCustomHeaders.String
		r.PlatformName = pName.String
		r.BaseURL = pURL.String
		decToken, decErr := crypto.Decrypt(pToken.String)
		if decErr != nil {
			return nil, fmt.Errorf("decrypt rapi %d platform token: %w", r.ID, decErr)
		}
		r.Token = decToken
		if lastFetch.Valid {
			r.LastTokenFetch = lastFetch.Time
		}
		if created.Valid {
			r.CreatedAt = created.Time
		}
		if updated.Valid {
			r.UpdatedAt = updated.Time
		}

		rapis = append(rapis, r)
	}
	return rapis, nil
}

func (db *DB) CreateRAPI(r *models.RAPI) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	formats := r.SupportedFormats
	if formats == "" {
		formats = `["openai"]`
	}

	result, err := db.conn.Exec(`
		INSERT INTO rapi (alias, model, notes, platform_id, enabled, available, base_cost, high_cost, rpm_limit, rph_limit, rpd_limit, tpm_limit, tph_limit, tpd_limit, time_period_rules, supported_formats, custom_headers)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.Alias, r.Model, r.Notes, r.PlatformID, boolToInt(r.Enabled), boolToInt(r.Available), r.BaseCost, r.HighCost, r.RPMLimit, r.RPHLimit, r.RPDLimit, r.TPMLimit, r.TPHLimit, r.TPDLimit, r.TimePeriodRules, formats, r.CustomHeaders)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	r.ID = id
	r.SupportedFormats = formats
	return nil
}

func (db *DB) UpdateRAPI(r *models.RAPI) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		UPDATE rapi SET alias = ?, model = ?, notes = ?, platform_id = ?, enabled = ?, available = ?,
			base_cost = ?, high_cost = ?, rpm_limit = ?, rph_limit = ?, rpd_limit = ?, tpm_limit = ?, tph_limit = ?, tpd_limit = ?, time_period_rules = ?, supported_formats = ?, custom_headers = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, r.Alias, r.Model, r.Notes, r.PlatformID, boolToInt(r.Enabled), boolToInt(r.Available),
		r.BaseCost, r.HighCost, r.RPMLimit, r.RPHLimit, r.RPDLimit, r.TPMLimit, r.TPHLimit, r.TPDLimit, r.TimePeriodRules, r.SupportedFormats, r.CustomHeaders, r.ID)

	return err
}

func (db *DB) SetPlatformEnabled(id int64, enabled bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		UPDATE platform SET enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, boolToInt(enabled), id)
	return err
}

func (db *DB) SetPlatformAvailable(id int64, available bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		UPDATE platform SET available = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, boolToInt(available), id)
	return err
}

func (db *DB) SetRAPIEnabled(id int64, enabled bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		UPDATE rapi SET enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, boolToInt(enabled), id)
	return err
}

func (db *DB) SetRAPIAvailable(id int64, available bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		UPDATE rapi SET available = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, boolToInt(available), id)
	return err
}

// SetRAPIUnavailableWithReason marks a RAPI as unavailable (available=false) and records
// the reason string. Use this for platform-level failures so admins can see why the RAPI
// was automatically disabled. Passing available=true clears the reason.
func (db *DB) SetRAPIUnavailableWithReason(id int64, available bool, reason string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	r := reason
	if available {
		r = ""
	}
	_, err := db.conn.Exec(`
		UPDATE rapi SET available = ?, unavailable_reason = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, boolToInt(available), r, id)
	return err
}

// UpdateRAPIFormats updates only the supported_formats field for a RAPI.
func (db *DB) UpdateRAPIFormats(id int64, formatsJSON string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		UPDATE rapi SET supported_formats = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, formatsJSON, id)
	return err
}

// UpdateRAPIHeaders updates only the custom_headers field for a RAPI.
func (db *DB) UpdateRAPIHeaders(id int64, headersJSON string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		UPDATE rapi SET custom_headers = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, headersJSON, id)
	return err
}

func (db *DB) SetLAPIEnabled(id int64, enabled bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`UPDATE lapi SET enabled = ? WHERE id = ?`, boolToInt(enabled), id)
	return err
}

// GetEnabledRAPIsForLAPI returns only enabled+available RAPIs for a LAPI
func (db *DB) GetEnabledRAPIsForLAPI(lapiID int64) ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       o.order_index, r.supported_formats, r.custom_headers, p.url_auto_complete, r.notes, p.custom_headers
		FROM rapi r
		LEFT JOIN platform p ON r.platform_id = p.id
		INNER JOIN lapi_rapi_order o ON r.id = o.rapi_id
		WHERE o.lapi_id = ? AND r.enabled = 1 AND r.available = 1 AND p.enabled = 1 AND p.available = 1
		ORDER BY o.order_index ASC
	`, lapiID)
}

func (db *DB) DeleteRAPI(id int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if this RAPI is referenced by any LAPI
	var lapiCount int
	err := db.conn.QueryRow(
		"SELECT COUNT(*) FROM lapi_rapi_order WHERE rapi_id = ?", id,
	).Scan(&lapiCount)
	if err != nil {
		return fmt.Errorf("检查关联路由失败: %w", err)
	}
	if lapiCount > 0 {
		rows, err := db.conn.Query(`
			SELECT l.alias FROM lapi l
			INNER JOIN lapi_rapi_order o ON l.id = o.lapi_id
			WHERE o.rapi_id = ?
		`, id)
		if err != nil {
			return fmt.Errorf("该模型被 %d 个路由引用，请先移除关联", lapiCount)
		}
		defer rows.Close()
		var aliases []string
		for rows.Next() {
			var alias string
			if err := rows.Scan(&alias); err == nil {
				aliases = append(aliases, alias)
			}
		}
		return fmt.Errorf("该模型被路由引用，请先从以下路由中移除: %s", strings.Join(aliases, ", "))
	}

	_, err = db.conn.Exec("DELETE FROM rapi WHERE id = ?", id)
	return err
}

// GetRAPIsByPlatform returns all RAPIs for a specific platform
func (db *DB) GetRAPIsByPlatform(platformID int64) ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       0 AS order_index, r.supported_formats, r.custom_headers, p.url_auto_complete, r.notes, p.custom_headers
		FROM rapi r
		LEFT JOIN platform p ON r.platform_id = p.id
		WHERE r.platform_id = ?
		ORDER BY r.id ASC
	`, platformID)
}

// ============ LAPI Operations ============

func (db *DB) GetLAPIs() ([]models.LAPI, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT id, alias, notes, enabled, created_at FROM lapi ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lapis := make([]models.LAPI, 0)
	for rows.Next() {
		var u models.LAPI
		var enabled int
		var created sql.NullTime
		var notesNull sql.NullString

		err := rows.Scan(&u.ID, &u.Alias, &notesNull, &enabled, &created)
		if err != nil {
			return nil, err
		}
		u.Enabled = enabled == 1
		u.Notes = notesNull.String

		if created.Valid {
			u.CreatedAt = created.Time
		}

		lapis = append(lapis, u)
	}
	return lapis, nil
}

func (db *DB) GetLAPIByAlias(alias string) (*models.LAPI, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var u models.LAPI
	var enabled int
	var created sql.NullTime

	err := db.conn.QueryRow(`
		SELECT id, alias, notes, enabled, created_at FROM lapi WHERE LOWER(alias) = LOWER(?)
	`, alias).Scan(&u.ID, &u.Alias, &u.Notes, &enabled, &created)
	u.Enabled = enabled == 1

	if err != nil {
		return nil, err
	}

	if created.Valid {
		u.CreatedAt = created.Time
	}

	return &u, nil
}

func (db *DB) CreateLAPI(u *models.LAPI) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	enabledVal := 1
	if !u.Enabled {
		enabledVal = 0
	}
	result, err := db.conn.Exec(`
		INSERT INTO lapi (alias, notes, enabled) VALUES (?, ?, ?)
	`, u.Alias, u.Notes, enabledVal)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	u.ID = id
	return nil
}

func (db *DB) UpdateLAPI(l *models.LAPI) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	enabledVal := 1
	if !l.Enabled {
		enabledVal = 0
	}
	_, err := db.conn.Exec("UPDATE lapi SET alias = ?, notes = ?, enabled = ? WHERE id = ?", l.Alias, l.Notes, enabledVal, l.ID)
	return err
}

func (db *DB) DeleteLAPI(id int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete associated mappings first
	if _, err := tx.Exec("DELETE FROM lapi_rapi_order WHERE lapi_id = ?", id); err != nil {
		return fmt.Errorf("清理路由映射失败: %w", err)
	}
	if _, err := tx.Exec("DELETE FROM rapi_metrics WHERE lapi_id = ?", id); err != nil {
		return fmt.Errorf("清理统计数据失败: %w", err)
	}

	_, err = tx.Exec("DELETE FROM lapi WHERE id = ?", id)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// ============ LAPI-RAPI Mapping Operations ============

func (db *DB) GetRAPIsForLAPI(lapiID int64) ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       o.order_index, r.supported_formats, r.custom_headers, p.url_auto_complete, r.notes, p.custom_headers
		FROM rapi r
		LEFT JOIN platform p ON r.platform_id = p.id
		INNER JOIN lapi_rapi_order o ON r.id = o.rapi_id
		WHERE o.lapi_id = ?
		ORDER BY o.order_index ASC
	`, lapiID)
}

func (db *DB) SetLAPIRAPIOrder(lapiID int64, rapiIDs []int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete existing mappings
	_, err = tx.Exec("DELETE FROM lapi_rapi_order WHERE lapi_id = ?", lapiID)
	if err != nil {
		return err
	}

	// Insert new mappings
	for i, rapiID := range rapiIDs {
		_, err = tx.Exec(`
			INSERT INTO lapi_rapi_order (lapi_id, rapi_id, order_index) VALUES (?, ?, ?)
		`, lapiID, rapiID, i)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (db *DB) GetLAPIRAPIMapping(lapiID int64) ([]int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT rapi_id FROM lapi_rapi_order WHERE lapi_id = ? ORDER BY order_index ASC
	`, lapiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rapiIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		rapiIDs = append(rapiIDs, id)
	}
	return rapiIDs, nil
}

// ============ Platform Key CRUD ============

// GetPlatformKeys returns all keys for a platform, ordered by key_index.
func (db *DB) GetPlatformKeys(platformID int64) ([]models.PlatformKey, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT id, platform_id, key_index, token, label, enabled, created_at, updated_at
		FROM platform_keys WHERE platform_id = ? ORDER BY key_index ASC
	`, platformID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make([]models.PlatformKey, 0)
	for rows.Next() {
		var k models.PlatformKey
		var encToken string
		var enabled sql.NullInt64
		var created, updated sql.NullTime
		if err := rows.Scan(&k.ID, &k.PlatformID, &k.KeyIndex, &encToken, &k.Label, &enabled, &created, &updated); err != nil {
			return nil, err
		}
		var decErr error
		if k.Token, decErr = crypto.Decrypt(encToken); decErr != nil {
			return nil, fmt.Errorf("decrypt platform_key %d: %w", k.ID, decErr)
		}
		k.Enabled = enabled.Int64 != 0
		if created.Valid {
			k.CreatedAt = created.Time
		}
		if updated.Valid {
			k.UpdatedAt = updated.Time
		}
		keys = append(keys, k)
	}
	return keys, nil
}

// EnsureDynamicPlatformKey guarantees that a platform_keys row with key_index=0 exists
// for the given platform. Used by API-push dynamic platforms which have no static keys.
// Returns the key ID of the upserted row.
func (db *DB) EnsureDynamicPlatformKey(platformID int64) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		INSERT INTO platform_keys (platform_id, key_index, token, label, enabled)
		VALUES (?, 0, '', 'dynamic', 1)
		ON CONFLICT(platform_id, key_index) DO NOTHING
	`, platformID)
	if err != nil {
		return 0, fmt.Errorf("ensure dynamic platform key failed: %w", err)
	}

	var keyID int64
	err = db.conn.QueryRow(
		"SELECT id FROM platform_keys WHERE platform_id = ? AND key_index = 0", platformID,
	).Scan(&keyID)
	return keyID, err
}

// SetPlatformKeys replaces all keys for a platform with the provided list.
// key_index values are assigned sequentially (0, 1, 2, …) in the given order.
func (db *DB) SetPlatformKeys(platformID int64, keys []models.PlatformKey) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM platform_keys WHERE platform_id = ?", platformID)
	if err != nil {
		return err
	}

	for i, k := range keys {
		encToken, encErr := crypto.Encrypt(k.Token)
		if encErr != nil {
			return fmt.Errorf("encrypt key %d: %w", i, encErr)
		}
		_, err = tx.Exec(`
			INSERT INTO platform_keys (platform_id, key_index, token, label, enabled)
			VALUES (?, ?, ?, ?, ?)
		`, platformID, i, encToken, k.Label, boolToInt(k.Enabled))
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// AddPlatformKey appends a new key to a platform (key_index = max+1).
func (db *DB) AddPlatformKey(k *models.PlatformKey) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	encToken, err := crypto.Encrypt(k.Token)
	if err != nil {
		return fmt.Errorf("encrypt token: %w", err)
	}

	var maxIdx int
	db.conn.QueryRow("SELECT COALESCE(MAX(key_index), -1) FROM platform_keys WHERE platform_id = ?", k.PlatformID).Scan(&maxIdx)
	k.KeyIndex = maxIdx + 1

	result, err := db.conn.Exec(`
		INSERT INTO platform_keys (platform_id, key_index, token, label, enabled)
		VALUES (?, ?, ?, ?, ?)
	`, k.PlatformID, k.KeyIndex, encToken, k.Label, boolToInt(k.Enabled))
	if err != nil {
		return err
	}
	id, _ := result.LastInsertId()
	k.ID = id
	return nil
}

// UpdatePlatformKey updates token/label/enabled for an existing key.
func (db *DB) UpdatePlatformKey(k *models.PlatformKey) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	encToken, err := crypto.Encrypt(k.Token)
	if err != nil {
		return fmt.Errorf("encrypt token: %w", err)
	}

	_, err = db.conn.Exec(`
		UPDATE platform_keys SET token = ?, label = ?, enabled = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, encToken, k.Label, boolToInt(k.Enabled), k.ID)
	return err
}

// DisablePlatformKey sets enabled=false for a single key (auth failure, e.g. 401).
func (db *DB) DisablePlatformKey(keyID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`
		UPDATE platform_keys SET enabled = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, keyID)
	return err
}

// DeletePlatformKey removes a single key and re-sequences remaining keys.
func (db *DB) DeletePlatformKey(keyID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var platformID int64
	if err := tx.QueryRow("SELECT platform_id FROM platform_keys WHERE id = ?", keyID).Scan(&platformID); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM platform_keys WHERE id = ?", keyID); err != nil {
		return err
	}
	// Re-sequence key_index values to keep them contiguous.
	rows, err := tx.Query("SELECT id FROM platform_keys WHERE platform_id = ? ORDER BY key_index ASC", platformID)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	for i, id := range ids {
		if _, err := tx.Exec("UPDATE platform_keys SET key_index = ? WHERE id = ?", i, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ============ Token Cache ============

func (db *DB) GetCachedTokenForKey(platformKeyID int64) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var encToken string
	var expiresAt sql.NullTime

	err := db.conn.QueryRow("SELECT token, expires_at FROM token_cache WHERE platform_key_id = ?", platformKeyID).Scan(&encToken, &expiresAt)
	if err != nil {
		return "", err
	}

	if expiresAt.Valid && time.Now().After(expiresAt.Time) {
		return "", sql.ErrNoRows
	}

	return crypto.Decrypt(encToken)
}

func (db *DB) SetCachedTokenForKey(platformKeyID int64, token string, ttl time.Duration) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	encToken, err := crypto.Encrypt(token)
	if err != nil {
		return fmt.Errorf("encrypt cached token: %w", err)
	}

	expiresAt := time.Now().Add(ttl)
	_, err = db.conn.Exec(`
		INSERT OR REPLACE INTO token_cache (platform_key_id, token, expires_at, fetched_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)
	`, platformKeyID, encToken, expiresAt)

	return err
}

// GetCachedToken is kept for backward compatibility; it delegates to the first key of the platform.
func (db *DB) GetCachedToken(platformID int64) (string, error) {
	db.mu.RLock()
	var keyID int64
	err := db.conn.QueryRow("SELECT id FROM platform_keys WHERE platform_id = ? AND key_index = 0", platformID).Scan(&keyID)
	db.mu.RUnlock()
	if err != nil {
		return "", err
	}
	return db.GetCachedTokenForKey(keyID)
}

// SetCachedToken is kept for backward compatibility; it sets the cache for the first key.
func (db *DB) SetCachedToken(platformID int64, token string, ttl time.Duration) error {
	db.mu.RLock()
	var keyID int64
	err := db.conn.QueryRow("SELECT id FROM platform_keys WHERE platform_id = ? AND key_index = 0", platformID).Scan(&keyID)
	db.mu.RUnlock()
	if err != nil {
		return err
	}
	return db.SetCachedTokenForKey(keyID, token, ttl)
}

// ============ Metrics Operations ============

type RAPIMetrics struct {
	RapiID          int64      `json:"rapi_id"`
	LapiID          int64      `json:"lapi_id"`
	TotalRequests   int        `json:"total_requests"`
	SuccessRequests int        `json:"success_requests"`
	Fail401         int        `json:"fail_401"`
	Fail429         int        `json:"fail_429"`
	Fail500         int        `json:"fail_500"`
	FailOther       int        `json:"fail_other"`
	TotalLatencyMs  int        `json:"total_latency_ms"`
	TokenCount      int        `json:"token_count"`
	LastUsed        *time.Time `json:"last_used"`
}

type RAPIStat struct {
	RapiID          int64   `json:"id"`
	Alias           string  `json:"alias"`
	TotalRequests   int     `json:"total_requests"`
	SuccessRequests int     `json:"success_requests"`
	SuccessRate     float64 `json:"success_rate"`
	AvgLatencyMs    float64 `json:"avg_latency_ms"`
	Fail401         int     `json:"fail_401"`
	Fail429         int     `json:"fail_429"`
	Fail500         int     `json:"fail_500"`
	FailOther       int     `json:"fail_other"`
	TokenCount      int     `json:"token_count"`
	LastUsed        string  `json:"last_used"`
}

type LAPIStat struct {
	LapiID          int64      `json:"id"`
	Alias           string     `json:"alias"`
	TotalRequests   int        `json:"total_requests"`
	SuccessRequests int        `json:"success_requests"`
	SuccessRate     float64    `json:"success_rate"`
	AvgLatencyMs    float64    `json:"avg_latency_ms"`
	RAPIs           []RAPIStat `json:"rapis"`
}

func (db *DB) RecordRequest(rapiID, lapiID int64, statusCode int, latencyMs int, tokensUsed int) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	now := time.Now()

	success := 0
	if statusCode >= 200 && statusCode <= 299 {
		success = 1
	}
	fail401 := 0
	if statusCode == 401 {
		fail401 = 1
	}
	fail429 := 0
	if statusCode == 429 {
		fail429 = 1
	}
	fail500 := 0
	if statusCode >= 500 && statusCode <= 599 {
		fail500 = 1
	}
	failOther := 0
	if statusCode != 200 && statusCode != 201 && statusCode != 202 && statusCode != 204 && statusCode != 401 && statusCode != 429 && statusCode >= 400 && statusCode <= 599 {
		failOther = 1
	}

	_, err := db.conn.Exec(`
		INSERT INTO rapi_metrics (rapi_id, lapi_id, total_requests, success_requests, fail_401, fail_429, fail_500, fail_other, total_latency_ms, token_count, last_used)
		VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(rapi_id, lapi_id) DO UPDATE SET
			total_requests = total_requests + 1,
			success_requests = success_requests + ?,
			fail_401 = fail_401 + ?,
			fail_429 = fail_429 + ?,
			fail_500 = fail_500 + ?,
			fail_other = fail_other + ?,
			total_latency_ms = total_latency_ms + ?,
			token_count = token_count + ?,
			last_used = ?
	`, rapiID, lapiID, success, fail401, fail429, fail500, failOther, latencyMs, tokensUsed, now,
		success, fail401, fail429, fail500, failOther, latencyMs, tokensUsed, now)

	return err
}

func (db *DB) GetRAPIStats() ([]RAPIStat, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT
			m.rapi_id,
			r.alias,
			COALESCE(SUM(m.total_requests), 0) as total_requests,
			COALESCE(SUM(m.success_requests), 0) as success_requests,
			COALESCE(SUM(m.fail_401), 0) as fail_401,
			COALESCE(SUM(m.fail_429), 0) as fail_429,
			COALESCE(SUM(m.fail_500), 0) as fail_500,
			COALESCE(SUM(m.fail_other), 0) as fail_other,
			COALESCE(SUM(m.total_latency_ms), 0) as total_latency_ms,
			COALESCE(SUM(m.token_count), 0) as token_count,
			MAX(m.last_used) as last_used
		FROM rapi_metrics m
		INNER JOIN rapi r ON m.rapi_id = r.id
		GROUP BY m.rapi_id, r.alias
		ORDER BY total_requests DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make([]RAPIStat, 0)
	for rows.Next() {
		var s RAPIStat
		var totalReq, successReq, fail401, fail429, fail500, failOther, totalLatency, tokenCount int
		var lastUsed string

		err := rows.Scan(&s.RapiID, &s.Alias, &totalReq, &successReq, &fail401, &fail429, &fail500, &failOther, &totalLatency, &tokenCount, &lastUsed)
		if err != nil {
			return nil, err
		}

		s.TotalRequests = totalReq
		s.SuccessRequests = successReq
		s.Fail401 = fail401
		s.Fail429 = fail429
		s.Fail500 = fail500
		s.FailOther = failOther
		s.TokenCount = tokenCount

		if totalReq > 0 {
			s.SuccessRate = float64(successReq) / float64(totalReq) * 100
			s.AvgLatencyMs = float64(totalLatency) / float64(totalReq)
		}

		if lastUsed != "" {
			s.LastUsed = lastUsed
		} else {
			s.LastUsed = "Never"
		}

		stats = append(stats, s)
	}
	return stats, nil
}

func (db *DB) GetLAPIStats() ([]LAPIStat, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT
			m.lapi_id,
			u.alias,
			COALESCE(SUM(m.total_requests), 0) as total_requests,
			COALESCE(SUM(m.success_requests), 0) as success_requests,
			COALESCE(SUM(m.total_latency_ms), 0) as total_latency_ms
		FROM rapi_metrics m
		INNER JOIN lapi u ON m.lapi_id = u.id
		GROUP BY m.lapi_id, u.alias
		ORDER BY total_requests DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make([]LAPIStat, 0)
	for rows.Next() {
		var s LAPIStat
		var totalReq, successReq, totalLatency int

		err := rows.Scan(&s.LapiID, &s.Alias, &totalReq, &successReq, &totalLatency)
		if err != nil {
			return nil, err
		}

		s.TotalRequests = totalReq
		s.SuccessRequests = successReq

		if totalReq > 0 {
			s.SuccessRate = float64(successReq) / float64(totalReq) * 100
			s.AvgLatencyMs = float64(totalLatency) / float64(totalReq)
		}

		stats = append(stats, s)
	}
	return stats, nil
}

func (db *DB) GetRAPIsForLAPIWithStats(lapiID int64) ([]RAPIStat, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT
			m.rapi_id,
			r.alias,
			COALESCE(m.total_requests, 0) as total_requests,
			COALESCE(m.success_requests, 0) as success_requests,
			COALESCE(m.fail_401, 0) as fail_401,
			COALESCE(m.fail_429, 0) as fail_429,
			COALESCE(m.fail_500, 0) as fail_500,
			COALESCE(m.fail_other, 0) as fail_other,
			COALESCE(m.total_latency_ms, 0) as total_latency_ms,
			COALESCE(m.token_count, 0) as token_count,
			m.last_used
		FROM lapi_rapi_order o
		INNER JOIN rapi r ON o.rapi_id = r.id
		LEFT JOIN rapi_metrics m ON m.rapi_id = r.id AND m.lapi_id = ?
		WHERE o.lapi_id = ?
		ORDER BY o.order_index ASC
	`, lapiID, lapiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make([]RAPIStat, 0)
	for rows.Next() {
		var s RAPIStat
		var totalReq, successReq, fail401, fail429, fail500, failOther, totalLatency, tokenCount int
		var lastUsed string

		err := rows.Scan(&s.RapiID, &s.Alias, &totalReq, &successReq, &fail401, &fail429, &fail500, &failOther, &totalLatency, &tokenCount, &lastUsed)
		if err != nil {
			return nil, err
		}

		s.TotalRequests = totalReq
		s.SuccessRequests = successReq
		s.Fail401 = fail401
		s.Fail429 = fail429
		s.Fail500 = fail500
		s.FailOther = failOther
		s.TokenCount = tokenCount

		if totalReq > 0 {
			s.SuccessRate = float64(successReq) / float64(totalReq) * 100
			s.AvgLatencyMs = float64(totalLatency) / float64(totalReq)
		}

		if lastUsed != "" {
			s.LastUsed = lastUsed
		} else {
			s.LastUsed = "Never"
		}

		stats = append(stats, s)
	}
	return stats, nil
}

func (db *DB) GetTokenCount(rapiID int64) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var count int
	err := db.conn.QueryRow("SELECT COALESCE(SUM(token_count), 0) FROM rapi_metrics WHERE rapi_id = ?", rapiID).Scan(&count)
	return count, err
}

// ============ Request Trends ============

type TrendPoint struct {
	MinuteBucket string `json:"time"`
	RequestCount int    `json:"count"`
}

func (db *DB) RecordTrend(lapiID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	bucket := time.Now().UTC().Format("2006-01-02T15:04")

	_, err := db.conn.Exec(`
		INSERT INTO request_trends (minute_bucket, lapi_id, request_count)
		VALUES (?, ?, 1)
		ON CONFLICT(minute_bucket, lapi_id) DO UPDATE SET
			request_count = request_count + 1
	`, bucket, lapiID)
	return err
}

func (db *DB) GetRequestTrends(minutes int) ([]TrendPoint, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	since := time.Now().UTC().Add(-time.Duration(minutes) * time.Minute).Format("2006-01-02T15:04")

	rows, err := db.conn.Query(`
		SELECT minute_bucket, SUM(request_count) as cnt
		FROM request_trends
		WHERE minute_bucket >= ?
		GROUP BY minute_bucket
		ORDER BY minute_bucket ASC
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var points []TrendPoint
	for rows.Next() {
		var p TrendPoint
		if err := rows.Scan(&p.MinuteBucket, &p.RequestCount); err != nil {
			return nil, err
		}
		points = append(points, p)
	}
	return points, nil
}

func (db *DB) CleanupOldTrends(maxAgeMinutes int) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	since := time.Now().UTC().Add(-time.Duration(maxAgeMinutes) * time.Minute).Format("2006-01-02T15:04")
	_, err := db.conn.Exec("DELETE FROM request_trends WHERE minute_bucket < ?", since)
	return err
}

func (db *DB) migrateAddRAPIUnavailableReason() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name='unavailable_reason'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE rapi ADD COLUMN unavailable_reason TEXT NOT NULL DEFAULT ''`)
	}
}

func (db *DB) migrateAddRAPINotesColumn() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name='notes'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE rapi ADD COLUMN notes TEXT NOT NULL DEFAULT ''`)
	}
}

func (db *DB) migrateAddPlatformCustomHeadersColumn() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform') WHERE name='custom_headers'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE platform ADD COLUMN custom_headers TEXT NOT NULL DEFAULT ''`)
	}
}



func (db *DB) migrateAddLAPINotesColumn() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('lapi') WHERE name='notes'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE lapi ADD COLUMN notes TEXT NOT NULL DEFAULT ''`)
	}
}



// migrateRAPIUniqueAliasToPerPlatform changes the rapi.alias uniqueness constraint from
// a global UNIQUE(alias) to UNIQUE(platform_id, alias), allowing different platforms to
// share the same model alias (e.g. glm-5.2 on JD and on ZhipuAI).
// This is a table-recreation migration (SQLite does not support DROP CONSTRAINT).
// Idempotent: skips if the new index already exists.
func (db *DB) migrateRAPIUniqueAliasToPerPlatform() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check whether the per-platform index already exists.
	var cnt int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_rapi_platform_alias'`)
	if err := row.Scan(&cnt); err != nil {
		return err
	}
	if cnt > 0 {
		return nil // Already migrated.
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Recreate the rapi table without the global UNIQUE(alias) constraint,
	// preserving all existing data.
	_, err = tx.Exec(`
		CREATE TABLE rapi_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			notes TEXT NOT NULL DEFAULT '',
			platform_id INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			unavailable_reason TEXT NOT NULL DEFAULT '',
			base_cost INTEGER NOT NULL DEFAULT 0,
			high_cost INTEGER NOT NULL DEFAULT 0,
			rpm_limit INTEGER NOT NULL DEFAULT 0,
			rph_limit INTEGER NOT NULL DEFAULT 0,
			rpd_limit INTEGER NOT NULL DEFAULT 0,
			tpm_limit INTEGER NOT NULL DEFAULT 0,
			tph_limit INTEGER NOT NULL DEFAULT 0,
			tpd_limit INTEGER NOT NULL DEFAULT 0,
			time_period_rules TEXT DEFAULT '',
			supported_formats TEXT NOT NULL DEFAULT '["openai"]',
			custom_headers TEXT NOT NULL DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
			UNIQUE(platform_id, alias)
		);
		INSERT INTO rapi_new SELECT id, alias, model, notes, platform_id, enabled, available, unavailable_reason,
			base_cost, high_cost, rpm_limit, rph_limit, rpd_limit, tpm_limit, tph_limit, tpd_limit,
			time_period_rules, supported_formats, custom_headers, created_at, updated_at
		FROM rapi;
		DROP TABLE rapi;
		ALTER TABLE rapi_new RENAME TO rapi;
		CREATE INDEX IF NOT EXISTS idx_rapi_platform ON rapi(platform_id);
		CREATE UNIQUE INDEX idx_rapi_platform_alias ON rapi(platform_id, alias);
	`)
	if err != nil {
		return err
	}

	log.Println("[DB] migrated rapi.alias uniqueness: global → per-platform")
	return tx.Commit()
}

// migrateEncryptTokens encrypts all existing plaintext tokens in platform_keys and platform
// tables using AES-256-GCM. Rows that already carry the "enc:" prefix are skipped.
// This migration is idempotent.
func (db *DB) migrateEncryptTokens() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Encrypt platform_keys.token
	rows, err := db.conn.Query(`SELECT id, token FROM platform_keys`)
	if err != nil {
		return err
	}
	type tokenRow struct {
		id    int64
		token string
	}
	var keyRows []tokenRow
	for rows.Next() {
		var r tokenRow
		if err := rows.Scan(&r.id, &r.token); err != nil {
			rows.Close()
			return err
		}
		keyRows = append(keyRows, r)
	}
	rows.Close()

	for _, r := range keyRows {
		if strings.HasPrefix(r.token, "enc:") {
			continue // Already encrypted.
		}
		enc, err := crypto.Encrypt(r.token)
		if err != nil {
			return fmt.Errorf("encrypt platform_keys id=%d: %w", r.id, err)
		}
		if _, err = db.conn.Exec(`UPDATE platform_keys SET token = ? WHERE id = ?`, enc, r.id); err != nil {
			return fmt.Errorf("update platform_keys id=%d: %w", r.id, err)
		}
	}

	// Encrypt platform.token (legacy column, kept for compat)
	platRows2, err := db.conn.Query(`SELECT id, token FROM platform`)
	if err != nil {
		return err
	}
	var platRows []tokenRow
	for platRows2.Next() {
		var r tokenRow
		if err := platRows2.Scan(&r.id, &r.token); err != nil {
			platRows2.Close()
			return err
		}
		platRows = append(platRows, r)
	}
	platRows2.Close()

	for _, r := range platRows {
		if r.token == "" || strings.HasPrefix(r.token, "enc:") {
			continue
		}
		enc, err := crypto.Encrypt(r.token)
		if err != nil {
			return fmt.Errorf("encrypt platform id=%d: %w", r.id, err)
		}
		if _, err = db.conn.Exec(`UPDATE platform SET token = ? WHERE id = ?`, enc, r.id); err != nil {
			return fmt.Errorf("update platform id=%d: %w", r.id, err)
		}
	}

	// Encrypt token_cache.token
	cacheRows2, err := db.conn.Query(`SELECT platform_key_id, token FROM token_cache`)
	if err != nil {
		return err
	}
	type cacheRow struct {
		keyID int64
		token string
	}
	var cacheRows []cacheRow
	for cacheRows2.Next() {
		var r cacheRow
		if err := cacheRows2.Scan(&r.keyID, &r.token); err != nil {
			cacheRows2.Close()
			return err
		}
		cacheRows = append(cacheRows, r)
	}
	cacheRows2.Close()

	for _, r := range cacheRows {
		if r.token == "" || strings.HasPrefix(r.token, "enc:") {
			continue
		}
		enc, err := crypto.Encrypt(r.token)
		if err != nil {
			return fmt.Errorf("encrypt token_cache key_id=%d: %w", r.keyID, err)
		}
		if _, err = db.conn.Exec(`UPDATE token_cache SET token = ? WHERE platform_key_id = ?`, enc, r.keyID); err != nil {
			return fmt.Errorf("update token_cache key_id=%d: %w", r.keyID, err)
		}
	}

	return nil
}
