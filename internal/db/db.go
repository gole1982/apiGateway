package db

import (
	"database/sql"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"gateway/internal/models"
)

type DB struct {
	conn *sql.DB
	mu   sync.RWMutex
}

var instance *DB

func Init() error {
	exePath, err := getExecutableDir()
	if err != nil {
		return err
	}
	dbPath := filepath.Join(exePath, "gateway.db")

	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}

	instance = &DB{conn: conn}

	_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS platform (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			base_url TEXT NOT NULL,
			token TEXT NOT NULL DEFAULT '',
			is_dynamic INTEGER NOT NULL DEFAULT 0,
			token_command TEXT,
			last_token_fetch DATETIME,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS rapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL UNIQUE,
			model TEXT NOT NULL DEFAULT '',
			platform_id INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			base_cost INTEGER NOT NULL DEFAULT 0,
			high_cost INTEGER NOT NULL DEFAULT 0,
			rpm_limit INTEGER NOT NULL DEFAULT 0,
			rph_limit INTEGER NOT NULL DEFAULT 0,
			rpd_limit INTEGER NOT NULL DEFAULT 0,
			tpm_limit INTEGER NOT NULL DEFAULT 0,
			tph_limit INTEGER NOT NULL DEFAULT 0,
			tpd_limit INTEGER NOT NULL DEFAULT 0,
			time_period_rules TEXT DEFAULT '',
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
			platform_id INTEGER PRIMARY KEY,
			token TEXT NOT NULL,
			expires_at DATETIME,
			fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE
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
		SELECT id, name, base_url, token, is_dynamic, token_command, last_token_fetch, enabled, available,
		       created_at, updated_at
		FROM platform ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	platforms := make([]models.Platform, 0)
	for rows.Next() {
		var p models.Platform
		var isDynamic, enabled, available int
		var lastFetch, created, updated sql.NullTime
		var tokenCmd sql.NullString

		err := rows.Scan(&p.ID, &p.Name, &p.BaseURL, &p.Token, &isDynamic, &tokenCmd, &lastFetch, &enabled, &available,
			&created, &updated)
		if err != nil {
			return nil, err
		}

		p.IsDynamic = isDynamic == 1
		p.Enabled = enabled == 1
		p.Available = available == 1
		p.TokenCommand = tokenCmd.String
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
	var isDynamic, enabled, available int
	var lastFetch, created, updated sql.NullTime
	var tokenCmd sql.NullString

	err := db.conn.QueryRow(`
		SELECT id, name, base_url, token, is_dynamic, token_command, last_token_fetch,
		       enabled, available, created_at, updated_at
		FROM platform WHERE id = ?
	`, id).Scan(&p.ID, &p.Name, &p.BaseURL, &p.Token, &isDynamic, &tokenCmd, &lastFetch,
		&enabled, &available, &created, &updated)

	if err != nil {
		return nil, err
	}

	p.IsDynamic = isDynamic == 1
	p.Enabled = enabled == 1
	p.Available = available == 1
	p.TokenCommand = tokenCmd.String
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

	result, err := db.conn.Exec(`
		INSERT INTO platform (name, base_url, token, is_dynamic, token_command, last_token_fetch, enabled, available)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, p.Name, p.BaseURL, p.Token, boolToInt(p.IsDynamic), p.TokenCommand, time.Now(), boolToInt(p.Enabled), boolToInt(p.Available))

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

	_, err := db.conn.Exec(`
		UPDATE platform SET name = ?, base_url = ?, token = ?, is_dynamic = ?, token_command = ?, enabled = ?, available = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, p.Name, p.BaseURL, p.Token, boolToInt(p.IsDynamic), p.TokenCommand, boolToInt(p.Enabled), boolToInt(p.Available), p.ID)

	return err
}

func (db *DB) DeletePlatform(id int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec("DELETE FROM platform WHERE id = ?", id)
	return err
}

func (db *DB) UpdatePlatformToken(id int64, token string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		UPDATE platform SET token = ?, last_token_fetch = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, token, time.Now(), id)

	return err
}

// ============ RAPI Operations ============

func (db *DB) GetRAPIs() ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.is_dynamic, p.token_command, p.last_token_fetch,
		       0 AS order_index
		FROM rapi r
		LEFT JOIN platform p ON r.platform_id = p.id
		ORDER BY r.id ASC
	`)
}

func (db *DB) GetRAPIByID(id int64) (*models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	results, err := db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.is_dynamic, p.token_command, p.last_token_fetch,
		       0 AS order_index
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
		var isDynamic, rEnabled, rAvailable int
		var lastFetch, created, updated sql.NullTime
		var tokenCmd, pName, pURL, pToken sql.NullString
		var timePeriodRules sql.NullString

		err := rows.Scan(&r.ID, &r.Alias, &r.Model, &r.PlatformID, &rEnabled, &rAvailable,
			&r.BaseCost, &r.HighCost, &r.RPMLimit, &r.RPHLimit, &r.RPDLimit, &r.TPMLimit, &r.TPHLimit, &r.TPDLimit, &timePeriodRules, &created, &updated,
			&pName, &pURL, &pToken, &isDynamic, &tokenCmd, &lastFetch, &r.OrderIndex)
		if err != nil {
			return nil, err
		}

		r.Enabled = rEnabled == 1
		r.Available = rAvailable == 1
		r.IsDynamic = isDynamic == 1
		r.TokenCommand = tokenCmd.String
		r.TimePeriodRules = timePeriodRules.String
		r.PlatformName = pName.String
		r.BaseURL = pURL.String
		r.Token = pToken.String
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

	result, err := db.conn.Exec(`
		INSERT INTO rapi (alias, model, platform_id, enabled, available, base_cost, high_cost, rpm_limit, rph_limit, rpd_limit, tpm_limit, tph_limit, tpd_limit, time_period_rules)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.Alias, r.Model, r.PlatformID, boolToInt(r.Enabled), boolToInt(r.Available), r.BaseCost, r.HighCost, r.RPMLimit, r.RPHLimit, r.RPDLimit, r.TPMLimit, r.TPHLimit, r.TPDLimit, r.TimePeriodRules)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	r.ID = id
	return nil
}

func (db *DB) UpdateRAPI(r *models.RAPI) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec(`
		UPDATE rapi SET alias = ?, model = ?, platform_id = ?, enabled = ?, available = ?,
			base_cost = ?, high_cost = ?, rpm_limit = ?, rph_limit = ?, rpd_limit = ?, tpm_limit = ?, tph_limit = ?, tpd_limit = ?, time_period_rules = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, r.Alias, r.Model, r.PlatformID, boolToInt(r.Enabled), boolToInt(r.Available),
		r.BaseCost, r.HighCost, r.RPMLimit, r.RPHLimit, r.RPDLimit, r.TPMLimit, r.TPHLimit, r.TPDLimit, r.TimePeriodRules, r.ID)

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

// GetEnabledRAPIsForLAPI returns only enabled+available RAPIs for a LAPI
func (db *DB) GetEnabledRAPIsForLAPI(lapiID int64) ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.is_dynamic, p.token_command, p.last_token_fetch,
		       o.order_index
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

	_, err := db.conn.Exec("DELETE FROM rapi WHERE id = ?", id)
	return err
}

// GetRAPIsByPlatform returns all RAPIs for a specific platform
func (db *DB) GetRAPIsByPlatform(platformID int64) ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.is_dynamic, p.token_command, p.last_token_fetch,
		       0 AS order_index
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
		SELECT id, alias, created_at FROM lapi ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lapis := make([]models.LAPI, 0)
	for rows.Next() {
		var u models.LAPI
		var created sql.NullTime

		err := rows.Scan(&u.ID, &u.Alias, &created)
		if err != nil {
			return nil, err
		}

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
	var created sql.NullTime

	err := db.conn.QueryRow(`
		SELECT id, alias, created_at FROM lapi WHERE alias = ?
	`, alias).Scan(&u.ID, &u.Alias, &created)

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

	result, err := db.conn.Exec(`
		INSERT INTO lapi (alias) VALUES (?)
	`, u.Alias)

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

	_, err := db.conn.Exec("UPDATE lapi SET alias = ? WHERE id = ?", l.Alias, l.ID)
	return err
}

func (db *DB) DeleteLAPI(id int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err := db.conn.Exec("DELETE FROM lapi WHERE id = ?", id)
	return err
}

// ============ LAPI-RAPI Mapping Operations ============

func (db *DB) GetRAPIsForLAPI(lapiID int64) ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.is_dynamic, p.token_command, p.last_token_fetch,
		       o.order_index
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

// ============ Token Cache ============

func (db *DB) GetCachedToken(platformID int64) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var token string
	var expiresAt sql.NullTime

	err := db.conn.QueryRow("SELECT token, expires_at FROM token_cache WHERE platform_id = ?", platformID).Scan(&token, &expiresAt)
	if err != nil {
		return "", err
	}

	if expiresAt.Valid && time.Now().After(expiresAt.Time) {
		return "", sql.ErrNoRows
	}

	return token, nil
}

func (db *DB) SetCachedToken(platformID int64, token string, ttl time.Duration) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	expiresAt := time.Now().Add(ttl)
	_, err := db.conn.Exec(`
		INSERT OR REPLACE INTO token_cache (platform_id, token, expires_at, fetched_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)
	`, platformID, token, expiresAt)

	return err
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
