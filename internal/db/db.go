package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
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
	// 报告主密钥来源：操作员据此确认 APIGATEWAY_KEY 是否生效（env vs 密钥文件），
	// 避免"以为用了环境变量、实际在用旧文件"导致的密文不可解。
	slog.Info("[DB] crypto master key loaded", "component", "db", "source", crypto.KeySource())

	dataDir, err := selectDataDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir %q: %w", dataDir, err)
	}
	dbPath := filepath.Join(dataDir, "gateway.db")
	slog.Info("[DB] database path selected", "component", "db", "path", dbPath, "data_dir", dataDir)
	return initAtPath(dbPath)
}

// selectDataDir resolves the persistent database directory. An explicit
// APIGATEWAY_DATA_DIR always wins. For local installs we preserve an existing
// gateway.db beside the executable; a fresh go run install falls back to the
// working directory when it contains proxy.cfg, then to the user config dir.
func selectDataDir() (string, error) {
	if dir := strings.TrimSpace(os.Getenv("APIGATEWAY_DATA_DIR")); dir != "" {
		return dir, nil
	}
	exeDir, err := getExecutableDir()
	if err != nil {
		return "", err
	}
	if fileExists(filepath.Join(exeDir, "gateway.db")) {
		return exeDir, nil
	}
	if cwd, err := os.Getwd(); err == nil {
		for _, name := range []string{"gateway.db", "proxy.cfg"} {
			if fileExists(filepath.Join(cwd, name)) {
				return cwd, nil
			}
		}
	}
	if configDir, err := os.UserConfigDir(); err == nil && configDir != "" {
		return filepath.Join(configDir, "apiGateway"), nil
	}
	return exeDir, nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// initAtPath runs the complete schema/migration/startup-maintenance chain for
// dbPath. Init owns key initialization and executable-path discovery; tests call
// this helper directly so they exercise the same ordering as production.
func initAtPath(dbPath string) error {
	// _busy_timeout=5000: wait up to 5 s before returning SQLITE_BUSY instead of
	// failing immediately. This prevents log writes from being silently dropped when
	// the gateway goroutines (logger worker, request handler, scheduler) contend on
	// the same SQLite file.
	// _time_format=sqlite makes the driver write time.Time bindings as
	// "2006-01-02 15:04:05.999999999-07:00" (SQLite-parseable) instead of the
	// default time.Time.String() form ("... +0000 UTC m=+123") which SQLite's
	// strftime() cannot parse — that produced NULL hours/days and 500s on
	// /api/analytics/*. The driver reads both formats back into time.Time.
	dsn := dbPath + "?_busy_timeout=5000&_time_format=sqlite"
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	keepConn := false
	defer func() {
		if !keepConn {
			if instance != nil && instance.conn == conn {
				instance = nil
			}
			_ = conn.Close()
		}
	}()
	// Single writer: SQLite supports only one concurrent writer. With the default
	// pool (MaxOpenConns=unlimited) multiple goroutines can queue concurrent writes
	// that all hit the busy lock. Limiting to 1 open connection serialises all DB
	// access and eliminates "database is locked" errors entirely.
	conn.SetMaxOpenConns(1)

	// WAL improves concurrent read/write behaviour and reduces lock contention with
	// the request logger writing request_logs while the proxy also updates metrics.
	if _, err = conn.Exec("PRAGMA journal_mode=WAL"); err != nil {
		slog.Warn("[DB] could not enable WAL mode", "component", "db", "error", err.Error())
	}

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
			token TEXT NOT NULL DEFAULT '',
			last_token_fetch DATETIME,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			notes TEXT NOT NULL DEFAULT '',
			supported_formats TEXT NOT NULL DEFAULT '["openai"]',
			format_endpoints TEXT NOT NULL DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		-- 注意：platform_keys 是自然键迁移前的旧凭据表，故意不在此处建表。
		-- 自然键迁移（migrateNaturalKeys）的终态是 credential + DROP platform_keys；
		-- 若在这里无条件 CREATE，每次启动都会把已删除的旧表复活，导致
		-- migrateEncryptTokens / migrateNaturalKeys 在终态库上反复失败。
		-- 需要它的旧路径由 migrateAddPlatformKeys 按需创建（仅当 credential 尚不存在）。

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
			key_ids TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT 'manual',
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

		-- 中心配置（Supabase）：URL + 加密后的 API key。proxy.cfg [management]
		-- 仍作无头启动回退；本表优先。key 用本机密钥 AES 加密（crypto.Encrypt）。
		CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL DEFAULT '',
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		-- 系统/中心互联日志：syncOnce 成败、管理端直写成败、池冷却事件等。
		-- 与 request_logs 解耦（不依赖请求 id），日志页"系统/中心"区块展示。
		CREATE TABLE IF NOT EXISTS system_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			level TEXT NOT NULL DEFAULT 'info',
			category TEXT NOT NULL DEFAULT '',
			message TEXT NOT NULL DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_system_logs_created ON system_logs(created_at);
		CREATE INDEX IF NOT EXISTS idx_system_logs_category ON system_logs(category);
	`)
	if err != nil {
		return err
	}

	// Drop the legacy persisted change_log table: unread notifications are now
	// kept in-memory only (see notify.NotificationService unread feed).
	conn.Exec(`DROP TABLE IF EXISTS change_log`)

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

	// Migration: add sort_order columns to platform and rapi tables
	instance.migrateAddSortOrderColumns()
	// Migration: add enabled/available columns if missing
	instance.migrateAddStatusColumns()
	instance.migrateAddCostColumns()
	instance.migrateAddSupportedFormatsColumn()
	instance.migrateAddNotesColumn()
	instance.migrateAddLAPIEnabledColumn()
	instance.migrateAddCustomHeadersColumn()
	instance.migrateDropURLAutoCompleteColumn()
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
	// Migration: drop webpage-platform-related columns (webpage_domain, is_dynamic, push_secret,
	// session_headers, reusable_status, reusable_reasons) — webpage support removed.
	instance.migrateDropWebpageColumns()
	// Migration: add failure_type/failure_reason/failed_at columns to platform_keys table
	instance.migrateAddPlatformKeyFailureColumns()
	// Migration: add expires_at/is_free columns to platform_keys table
	instance.migrateAddPlatformKeyExpiryColumns()
	// Migration: change rapi.alias uniqueness from global to per-platform
	if err := instance.migrateRAPIUniqueAliasToPerPlatform(); err != nil {
		return fmt.Errorf("migrateRAPIUniqueAliasToPerPlatform: %w", err)
	}
	// Migration: encrypt existing plaintext tokens with AES-256-GCM
	if err := instance.migrateEncryptTokens(); err != nil {
		return fmt.Errorf("migrateEncryptTokens: %w", err)
	}
	// Migration: add series/name/version columns to rapi and lapi tables
	instance.migrateAddModelIdentityColumns()
	// Migration: unified model naming (vendor/suffix columns, model_name→suffix,
	// backfill rapi.notes from the original upstream model string)
	instance.migrateUnifiedModelNaming()
	// Migration: add supported_formats to platform table (platform-level authority)
	instance.migrateAddPlatformSupportedFormats()
	instance.migrateAddPlatformFormatEndpoints()
	// Migration: add source column to rapi table (auto_discover | manual)
	instance.migrateAddRAPISource()
	// Migration: add key_ids column to rapi table (model→key capability whitelist)
	instance.migrateAddRAPIKeyIDs()
	// Migration: key×model capability block table (platform revoked a key's
	// access to a model; the pair is skipped until the block expires).
	instance.migrateKeyModelBlocks()
	// Migration: platform console/account columns (billing_address,
	// login_account, login_password — the latter stored encrypted, write-only).
	instance.migrateAddPlatformAccountColumns()

	// Migration: 自然键身份模型（platform_keys→credential 平移、key_ids→
	// endpoint_credential 绑定物化、platform.name/rapi.alias 唯一约束降级为
	// 显示名、base_url/(platform,model) 自然键唯一索引）。必须位于全部历史
	// 迁移之后、seed 之前。设计：docs/superpowers/specs/2026-09-23-natural-key-identity-design.md
	if err := instance.migrateNaturalKeys(); err != nil {
		return fmt.Errorf("migrateNaturalKeys: %w", err)
	}

	// Cleanup: remove orphan RAPIs whose platform no longer exists
	instance.cleanupOrphanedRAPIs()

	// Cleanup: auto-disable platform keys that have passed their ExpiresAt.
	// Sets enabled=0 so the gateway skips them and the UI shows them disabled.
	// Runtime guard in scheduler.PickAvailableKey also handles the brief window
	// between expiry and the next process start.
	if n, err := instance.DisableExpiredKeys(); err != nil {
		slog.Warn("[WARN] disable expired keys failed", "component", "db", "error", err.Error())
	} else if n > 0 {
		slog.Info("[INFO] disabled expired keys", "component", "db", "count", n)
	}

	// 预置默认平台（首次启动自动种入，按 name UNIQUE 幂等）
	if err := instance.seedDefaultPlatforms(); err != nil {
		return fmt.Errorf("seedDefaultPlatforms: %w", err)
	}

	// One-time fix: deduct the 5xx double-count from historical fail_other
	// (see migrateFailOtherDedup). Runs once, guarded by a settings flag.
	if err := instance.migrateFailOtherDedup(); err != nil {
		slog.Warn("[WARN] fail_other dedup migration failed", "component", "db", "error", err.Error())
	}

	keepConn = true
	return nil
}

// seedDefaultPlatforms 种入预置厂商平台，方便用户开箱即用。
// 仅种入网关已实现格式转换器的厂商（目前只有 Google Gemini 原生格式）。
// 通过 platform.name 的 UNIQUE 约束 + INSERT OR IGNORE 实现幂等：重启不会重复创建。
// 初始状态：enabled=true, available=false, token="" —— 用户在 UI 填入 API Key 后即可恢复可用。
func (db *DB) seedDefaultPlatforms() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	type preset struct {
		name             string
		baseURL          string
		supportedFormats string
		notes            string
	}

	presets := []preset{
		{
			name:             "Google Gemini",
			baseURL:          "https://generativelanguage.googleapis.com",
			supportedFormats: `["gemini"]`,
			notes:            "Google AI Studio 原生接口。填入 API Key (x-goog-api-key) 即可启用，网关会自动转换为 OpenAI 格式对外暴露。",
		},
	}

	for _, p := range presets {
		// 身份 = 归一化 base_url（name 只是显示名，允许重名/改名）。
		nb := models.NormalizeBaseURL(p.baseURL)
		var cnt int
		if err := db.conn.QueryRow(`SELECT COUNT(*) FROM platform WHERE base_url = ?`, nb).Scan(&cnt); err != nil {
			return fmt.Errorf("seed check %s: %w", p.name, err)
		}
		if cnt > 0 {
			continue
		}
		// token 为空 → crypto.Encrypt 返回 ""，available=0 表示填 key 前不可用
		if _, err := db.conn.Exec(`
			INSERT INTO platform
				(name, base_url, token, last_token_fetch,
				 enabled, available, notes, custom_headers, supported_formats)
			VALUES (?, ?, '', NULL, 1, 0, ?, '', ?)
		`, p.name, nb, p.notes, p.supportedFormats); err != nil {
			return fmt.Errorf("seed %s: %w", p.name, err)
		}
	}
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

	rows, err := tx.Query(`SELECT DISTINCT url, token FROM rapi`)
	if err != nil {
		return err
	}

	type oldPlatformData struct {
		url   string
		token string
	}
	var platforms []oldPlatformData
	for rows.Next() {
		var p oldPlatformData
		if err := rows.Scan(&p.url, &p.token); err != nil {
			rows.Close()
			return err
		}
		platforms = append(platforms, p)
	}
	rows.Close()

	for _, p := range platforms {
		_, err = tx.Exec(`
			INSERT OR IGNORE INTO platform (name, base_url, token)
			VALUES (?, ?, ?)
		`, p.url, p.url, p.token)
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
	if _, err = tx.Exec(`
		CREATE TABLE rapi_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL UNIQUE,
			model TEXT NOT NULL DEFAULT '',
			platform_id INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE
		);
	`); err != nil {
		return err
	}
	// Copy every column the new table keeps, resolved dynamically so no old
	// column is silently dropped (see migrateRAPIUniqueAliasToPerPlatform).
	keep := []string{"id", "alias", "model", "platform_id", "created_at", "updated_at"}
	copySQL, err := buildCopyCommonColumns(tx, "rapi", "rapi_new", keep)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(copySQL); err != nil {
		return err
	}
	if _, err = tx.Exec(`
		DROP TABLE rapi;
		ALTER TABLE rapi_new RENAME TO rapi;
		CREATE INDEX IF NOT EXISTS idx_rapi_platform ON rapi(platform_id);
	`); err != nil {
		return err
	}

	return tx.Commit()
}

// buildCopyCommonColumns builds "INSERT INTO newTbl (cols...) SELECT cols... FROM oldTbl"
// copying each keep column that exists in oldTbl. The intersection is read live from
// PRAGMA table_info so a column added by an earlier migration is never silently dropped
// when a table is recreated. keep must list every column of newTbl; a keep column absent
// from oldTbl is skipped (its new-table default applies).
func buildCopyCommonColumns(tx *sql.Tx, oldTbl, newTbl string, keep []string) (string, error) {
	oldCols, err := tableColumnsTx(tx, oldTbl)
	if err != nil {
		return "", err
	}
	var cols []string
	for _, c := range keep {
		if oldCols[c] {
			cols = append(cols, c)
		}
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("no common columns between %s and %s", oldTbl, newTbl)
	}
	list := strings.Join(cols, ", ")
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", newTbl, list, list, oldTbl), nil
}

// tableColumnsTx returns the set of column names of tbl within tx.
func tableColumnsTx(tx *sql.Tx, tbl string) (map[string]bool, error) {
	rows, err := tx.Query(`SELECT name FROM pragma_table_info(?)`, tbl)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
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

func (db *DB) migrateAddPlatformSupportedFormats() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform') WHERE name='supported_formats'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE platform ADD COLUMN supported_formats TEXT NOT NULL DEFAULT '["openai"]'`)
	}
	db.conn.Exec(`UPDATE platform SET supported_formats = '["openai"]' WHERE supported_formats IS NULL OR supported_formats = ''`)
}

// migrateAddPlatformFormatEndpoints adds the format_endpoints column to the
// platform table. Stores JSON {"format":"url"} mapping each supported format
// to the exact upstream URL detection proved works.
func (db *DB) migrateAddPlatformFormatEndpoints() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform') WHERE name='format_endpoints'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE platform ADD COLUMN format_endpoints TEXT NOT NULL DEFAULT ''`)
	}
	db.conn.Exec(`UPDATE platform SET format_endpoints = '' WHERE format_endpoints IS NULL`)
}

func (db *DB) migrateAddRAPISource() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name='source'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE rapi ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'`)
	}
}

// migrateAddRAPIKeyIDs adds the key_ids column to the rapi table. Empty string
// means "all platform keys are eligible" — the pre-existing behavior, so this
// migration is a no-op semantically for existing data. Idempotent.
func (db *DB) migrateAddRAPIKeyIDs() {
	db.mu.Lock()
	defer db.mu.Unlock()
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rapi') WHERE name='key_ids'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE rapi ADD COLUMN key_ids TEXT NOT NULL DEFAULT ''`)
	}
}

// migrateKeyModelBlocks creates the key_model_blocks table (key×model
// capability blacklist). Idempotent — CREATE TABLE IF NOT EXISTS.
func (db *DB) migrateKeyModelBlocks() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.conn.Exec(`
		CREATE TABLE IF NOT EXISTS key_model_blocks (
			key_id INTEGER NOT NULL,
			rapi_id INTEGER NOT NULL,
			reason TEXT NOT NULL DEFAULT '',
			expires_at DATETIME,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (key_id, rapi_id)
		)
	`)
}

// migrateAddPlatformAccountColumns adds the provider-console columns to the
// platform table: billing_address (console URL, e.g. billing page),
// login_account (console login), login_password (console password, stored
// encrypted — never selected on read, write-only). Idempotent.
func (db *DB) migrateAddPlatformAccountColumns() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for name, definition := range map[string]string{
		"billing_address": "TEXT NOT NULL DEFAULT ''",
		"login_account":   "TEXT NOT NULL DEFAULT ''",
		"login_password":  "TEXT NOT NULL DEFAULT ''",
	} {
		var count int
		row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform') WHERE name=?`, name)
		if row.Scan(&count) == nil && count == 0 {
			db.conn.Exec("ALTER TABLE platform ADD COLUMN " + name + " " + definition)
		}
	}
}

// BlockKeyForModel records (or refreshes) a key×model capability block: the
// key is not permitted to serve this model. expiresAt bounds how long the
// block lives; after expiry the pair is retried naturally and re-blocked on
// the next denial (or unblocked on success).
func (db *DB) BlockKeyForModel(keyID, rapiID int64, reason string, expiresAt time.Time) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`
		INSERT INTO key_model_blocks (key_id, rapi_id, reason, expires_at, created_at)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(key_id, rapi_id) DO UPDATE SET
			reason = excluded.reason, expires_at = excluded.expires_at
	`, keyID, rapiID, reason, expiresAt)
	return err
}

// UnblockKeyForModel removes a key×model capability block (platform re-granted
// access, a probe succeeded, or the operator cleared it).
func (db *DB) UnblockKeyForModel(keyID, rapiID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`DELETE FROM key_model_blocks WHERE key_id = ? AND rapi_id = ?`, keyID, rapiID)
	return err
}

// ClearKeyModelBlocksForKey removes every block referencing a key (key deleted).
func (db *DB) ClearKeyModelBlocksForKey(keyID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`DELETE FROM key_model_blocks WHERE key_id = ?`, keyID)
	return err
}

// GetKeyModelBlocks returns all key×model capability blocks (expired ones
// included — callers filter by ExpiresAt at lookup time).
func (db *DB) GetKeyModelBlocks() ([]models.KeyModelBlock, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	rows, err := db.conn.Query(`SELECT key_id, rapi_id, reason, expires_at, created_at FROM key_model_blocks`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.KeyModelBlock
	for rows.Next() {
		var b models.KeyModelBlock
		var exp, created sql.NullTime
		if err := rows.Scan(&b.KeyID, &b.RAPIID, &b.Reason, &exp, &created); err != nil {
			return nil, err
		}
		if exp.Valid {
			b.ExpiresAt = exp.Time
		}
		if created.Valid {
			b.CreatedAt = created.Time
		}
		out = append(out, b)
	}
	return out, rows.Err()
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

func (db *DB) migrateDropURLAutoCompleteColumn() {
	// url_auto_complete was removed: all platforms now use format_endpoints
	// (exact detected URL) with BuildURL() fallback for upstream URL resolution.
	// SQLite ≥3.35 supports ALTER TABLE DROP COLUMN; guard with pragma_table_info
	// so this is idempotent and a no-op on fresh installs that never had the column.
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform') WHERE name='url_auto_complete'`)
	if row.Scan(&count) == nil && count > 0 {
		if _, err := db.conn.Exec(`ALTER TABLE platform DROP COLUMN url_auto_complete`); err != nil {
			slog.Warn("[WARN] drop platform.url_auto_complete column failed",
				"component", "db", "column", "platform.url_auto_complete", "error", err.Error())
		}
	}
}

// migrateAddPlatformKeys creates the platform_keys table if not present, then copies
// existing platform.token values as key_index=0 entries (idempotent).
//
// 一旦自然键迁移已生效（credential 表存在），platform_keys 就是已废弃的旧表：
// 此处必须整体跳过，否则每次启动都会从 platform.token 回填出一个全新的
// platform_keys（id 重新从 1 开始，与 credential.id 冲突），进而让后续的
// migrateEncryptTokens / migrateNaturalKeys 在终态库上失败。
func (db *DB) migrateAddPlatformKeys() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// credential 非空才代表自然键终态。空 credential 可能是一次失败迁移在旧
	// 实现中留下的 DDL 残影；若就此跳过旧表创建，数据库将永久无法恢复。
	// 表存在性探测必须在 Begin 之前完成（单连接池下事务中查询 db.conn 会死锁）。
	if tableExists(db.conn, "credential") {
		var count int
		if err := db.conn.QueryRow(`SELECT COUNT(*) FROM credential`).Scan(&count); err != nil {
			return fmt.Errorf("count credential: %w", err)
		}
		if count > 0 {
			return nil // 自然键终态：credential 已是唯一凭据表
		}
	}

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
			slog.Info("[DB] cleaned up orphaned RAPIs",
				"component", "db", "rows", rows)
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
		SELECT id, name, base_url, token,
		       last_token_fetch, enabled, available, notes, custom_headers,
		       supported_formats, format_endpoints,
		       billing_address, login_account,
		       created_at, updated_at
		FROM platform ORDER BY sort_order ASC, id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	platforms := make([]models.Platform, 0)
	for rows.Next() {
		var p models.Platform
		var enabled, available sql.NullInt64
		var lastFetch, created, updated sql.NullTime
		var notes, customHeaders, supportedFormats, formatEndpoints sql.NullString
		var billingAddress, loginAccount sql.NullString

		var encToken string
		err := rows.Scan(&p.ID, &p.Name, &p.BaseURL, &encToken,
			&lastFetch, &enabled, &available, &notes, &customHeaders,
			&supportedFormats, &formatEndpoints,
			&billingAddress, &loginAccount,
			&created, &updated)
		if err != nil {
			return nil, err
		}
		if p.Token, err = crypto.Decrypt(encToken); err != nil {
			return nil, fmt.Errorf("decrypt platform %d token: %w", p.ID, err)
		}

		p.Enabled = enabled.Int64 != 0
		p.Available = available.Int64 != 0
		p.Notes = notes.String
		p.CustomHeaders = customHeaders.String
		p.SupportedFormats = supportedFormats.String
		p.FormatEndpoints = formatEndpoints.String
		p.BillingAddress = billingAddress.String
		p.LoginAccount = loginAccount.String
		// login_password is intentionally NOT selected: it is write-only, so the
		// list/read API can never leak the encrypted console password.
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
	var enabled, available sql.NullInt64
	var lastFetch, created, updated sql.NullTime
	var notes, customHeaders, supportedFormats, formatEndpoints sql.NullString
	var billingAddress, loginAccount sql.NullString

	var encToken string
	err := db.conn.QueryRow(`
		SELECT id, name, base_url, token,
		       last_token_fetch, enabled, available, notes, custom_headers,
		       supported_formats, format_endpoints,
		       billing_address, login_account,
		       created_at, updated_at
		FROM platform WHERE id = ?
	`, id).Scan(&p.ID, &p.Name, &p.BaseURL, &encToken,
		&lastFetch, &enabled, &available, &notes, &customHeaders,
		&supportedFormats, &formatEndpoints,
		&billingAddress, &loginAccount,
		&created, &updated)

	if err != nil {
		return nil, err
	}
	if p.Token, err = crypto.Decrypt(encToken); err != nil {
		return nil, fmt.Errorf("decrypt platform %d token: %w", id, err)
	}

	p.Enabled = enabled.Int64 != 0
	p.Available = available.Int64 != 0
	p.Notes = notes.String
	p.CustomHeaders = customHeaders.String
	p.SupportedFormats = supportedFormats.String
	p.FormatEndpoints = formatEndpoints.String
	p.BillingAddress = billingAddress.String
	p.LoginAccount = loginAccount.String
	// login_password is intentionally NOT selected (write-only).
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

	// 身份 = 归一化 base_url：同一地址只允许登记一次（name 仅为显示名）。
	p.BaseURL = models.NormalizeBaseURL(p.BaseURL)
	if p.BaseURL != "" {
		var cnt int
		if err := db.conn.QueryRow(`SELECT COUNT(*) FROM platform WHERE base_url = ?`, p.BaseURL).Scan(&cnt); err != nil {
			return err
		} else if cnt > 0 {
			return fmt.Errorf("base_url %s 已存在：平台身份即接入地址，同一地址只需登记一次", p.BaseURL)
		}
	}

	encToken, err := crypto.Encrypt(p.Token)
	if err != nil {
		return fmt.Errorf("encrypt token: %w", err)
	}

	// Login password is stored encrypted with the same mechanism as Token;
	// an empty password is stored as empty (no ciphertext for empty input).
	encLoginPw, err := crypto.Encrypt(p.LoginPassword)
	if err != nil {
		return fmt.Errorf("encrypt login password: %w", err)
	}

	formats := p.SupportedFormats
	if formats == "" {
		formats = `["openai"]`
	}

	result, err := db.conn.Exec(`
		INSERT INTO platform (name, base_url, token, last_token_fetch, enabled, available, notes, custom_headers, supported_formats, format_endpoints, billing_address, login_account, login_password)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, p.Name, p.BaseURL, encToken, time.Now(), boolToInt(p.Enabled), boolToInt(p.Available), p.Notes, p.CustomHeaders, formats, p.FormatEndpoints, p.BillingAddress, p.LoginAccount, encLoginPw)

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

	// 身份 = 归一化 base_url：改名随意，改地址不得撞到其他平台。
	p.BaseURL = models.NormalizeBaseURL(p.BaseURL)
	if p.BaseURL != "" {
		var cnt int
		if err := db.conn.QueryRow(`SELECT COUNT(*) FROM platform WHERE base_url = ? AND id != ?`, p.BaseURL, p.ID).Scan(&cnt); err != nil {
			return err
		} else if cnt > 0 {
			return fmt.Errorf("base_url %s 已被其他平台占用：平台身份即接入地址", p.BaseURL)
		}
	}

	encToken, err := crypto.Encrypt(p.Token)
	if err != nil {
		return fmt.Errorf("encrypt token: %w", err)
	}

	// Login password semantics: an empty payload password means "keep the
	// existing one" (mirrors the key-token edit semantics). Reuse the stored
	// ciphertext from the DB so a password save never blanks it out.
	encLoginPw := ""
	if strings.TrimSpace(p.LoginPassword) != "" {
		encLoginPw, err = crypto.Encrypt(p.LoginPassword)
		if err != nil {
			return fmt.Errorf("encrypt login password: %w", err)
		}
	} else {
		if err := db.conn.QueryRow(`SELECT login_password FROM platform WHERE id = ?`, p.ID).Scan(&encLoginPw); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("load existing login password: %w", err)
		}
	}

	formats := p.SupportedFormats
	if formats == "" {
		formats = `["openai"]`
	}

	_, err = db.conn.Exec(`
		UPDATE platform SET name = ?, base_url = ?, token = ?, enabled = ?, available = ?, notes = ?, custom_headers = ?, supported_formats = ?, format_endpoints = ?, billing_address = ?, login_account = ?, login_password = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, p.Name, p.BaseURL, encToken, boolToInt(p.Enabled), boolToInt(p.Available), p.Notes, p.CustomHeaders, formats, p.FormatEndpoints, p.BillingAddress, p.LoginAccount, encLoginPw, p.ID)

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

// ============ RAPI Operations ============

func (db *DB) GetRAPIs() ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.vendor, r.series, r.model_name, r.version, r.suffix, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       0 AS order_index, r.supported_formats, r.custom_headers, r.key_ids, r.notes, r.source, p.custom_headers, p.supported_formats, p.format_endpoints
		FROM rapi r
		LEFT JOIN platform p ON r.platform_id = p.id
		ORDER BY r.sort_order ASC, r.id ASC
	`)
}

func (db *DB) GetRAPIByID(id int64) (*models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	results, err := db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.vendor, r.series, r.model_name, r.version, r.suffix, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       0 AS order_index, r.supported_formats, r.custom_headers, r.key_ids, r.notes, r.source, p.custom_headers, p.supported_formats, p.format_endpoints
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
		var rEnabled, rAvailable sql.NullInt64
		var lastFetch, created, updated sql.NullTime
		var pName, pURL, pToken sql.NullString
		var vendor, series, modelName, version, suffix sql.NullString
		var timePeriodRules, supportedFormats, customHeaders, keyIDs, unavailableReason, notes, platformCustomHeaders, source, platformSupportedFormats, platformFormatEndpoints sql.NullString

		err := rows.Scan(&r.ID, &r.Alias, &r.Model, &vendor, &series, &modelName, &version, &suffix, &r.PlatformID, &rEnabled, &rAvailable, &unavailableReason,
			&r.BaseCost, &r.HighCost, &r.RPMLimit, &r.RPHLimit, &r.RPDLimit, &r.TPMLimit, &r.TPHLimit, &r.TPDLimit, &timePeriodRules, &created, &updated,
			&pName, &pURL, &pToken, &lastFetch, &r.OrderIndex, &supportedFormats, &customHeaders, &keyIDs, &notes, &source, &platformCustomHeaders, &platformSupportedFormats, &platformFormatEndpoints)
		if err != nil {
			return nil, err
		}

		r.Enabled = rEnabled.Int64 != 0 // default to true if NULL
		r.Available = rAvailable.Int64 != 0
		r.Vendor = vendor.String
		r.Series = series.String
		r.ModelName = modelName.String
		r.Version = version.String
		r.Suffix = suffix.String
		r.UnavailableReason = unavailableReason.String
		r.PlatformFormatEndpoints = platformFormatEndpoints.String
		r.TimePeriodRules = timePeriodRules.String
		r.SupportedFormats = supportedFormats.String
		r.CustomHeaders = customHeaders.String
		r.KeyIDs = keyIDs.String
		r.Notes = notes.String
		r.Source = source.String
		r.PlatformCustomHeaders = platformCustomHeaders.String
		r.PlatformSupportedFormats = platformSupportedFormats.String
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

	source := r.Source
	if source == "" {
		source = "manual"
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// key_ids 是绑定派生列：先占位空串，绑定物化后由 syncBindingsTx 重建。
	result, err := tx.Exec(`
		INSERT INTO rapi (alias, model, notes, vendor, series, model_name, version, suffix, platform_id, enabled, available, base_cost, high_cost, rpm_limit, rph_limit, rpd_limit, tpm_limit, tph_limit, tpd_limit, time_period_rules, supported_formats, custom_headers, key_ids, source)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?)
	`, r.Alias, r.Model, r.Notes, r.Vendor, r.Series, r.ModelName, r.Version, r.Suffix, r.PlatformID, boolToInt(r.Enabled), boolToInt(r.Available), r.BaseCost, r.HighCost, r.RPMLimit, r.RPHLimit, r.RPDLimit, r.TPMLimit, r.TPHLimit, r.TPDLimit, r.TimePeriodRules, formats, r.CustomHeaders, source)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	r.ID = id
	// 绑定物化：KeyIDs 空 = 归属平台全部凭据（旧默认语义）；非空 = 白名单。
	if err := syncBindingsTx(tx, id, r.PlatformID, r.KeyIDs); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.KeyIDs = db.derivedKeyIDs(id)
	r.SupportedFormats = formats
	r.Source = source
	return nil
}

func (db *DB) UpdateRAPI(r *models.RAPI) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// key_ids 不入列：它是 endpoint_credential 的派生投影，由 syncBindingsTx 重建。
	_, err = tx.Exec(`
		UPDATE rapi SET alias = ?, model = ?, notes = ?, vendor = ?, series = ?, model_name = ?, version = ?, suffix = ?, platform_id = ?, enabled = ?, available = ?,
			base_cost = ?, high_cost = ?, rpm_limit = ?, rph_limit = ?, rpd_limit = ?, tpm_limit = ?, tph_limit = ?, tpd_limit = ?, time_period_rules = ?, supported_formats = ?, custom_headers = ?, source = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, r.Alias, r.Model, r.Notes, r.Vendor, r.Series, r.ModelName, r.Version, r.Suffix, r.PlatformID, boolToInt(r.Enabled), boolToInt(r.Available),
		r.BaseCost, r.HighCost, r.RPMLimit, r.RPHLimit, r.RPDLimit, r.TPMLimit, r.TPHLimit, r.TPDLimit, r.TimePeriodRules, r.SupportedFormats, r.CustomHeaders, r.Source, r.ID)
	if err != nil {
		return err
	}
	if err := syncBindingsTx(tx, r.ID, r.PlatformID, r.KeyIDs); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.KeyIDs = db.derivedKeyIDs(r.ID)
	return nil
}

// RAPIModelExists 报告同平台（=同 base_url）下 model 是否已存在 —— 端点身份
// 即 (base_url, model)，重复即冲突。model 为空不查（历史空 model 行不参与身份）。
// excludeID 让更新路径忽略自身（0 = 不排除）。
func (db *DB) RAPIModelExists(platformID int64, model string, excludeID int64) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if strings.TrimSpace(model) == "" {
		return false, nil
	}
	var cnt int
	err := db.conn.QueryRow(
		`SELECT COUNT(*) FROM rapi WHERE platform_id = ? AND model = ? AND id != ?`,
		platformID, model, excludeID,
	).Scan(&cnt)
	return cnt > 0, err
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

// UpdatePlatformFormats writes the platform-level supported_formats (authoritative)
// and optionally propagates the same value to every child RAPI as a cache.
// Also accepts a formatEndpointsJSON mapping each supported format to the
// exact upstream URL detection proved works (pass "" to leave it unchanged).
func (db *DB) UpdatePlatformFormats(platformID int64, formatsJSON string, propagateToRAPIs bool, formatEndpointsJSON string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if formatEndpointsJSON != "" {
		if _, err := tx.Exec(`UPDATE platform SET supported_formats = ?, format_endpoints = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, formatsJSON, formatEndpointsJSON, platformID); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(`UPDATE platform SET supported_formats = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, formatsJSON, platformID); err != nil {
			return err
		}
	}
	if propagateToRAPIs {
		if _, err := tx.Exec(`UPDATE rapi SET supported_formats = ?, updated_at = CURRENT_TIMESTAMP WHERE platform_id = ?`, formatsJSON, platformID); err != nil {
			return err
		}
	}
	return tx.Commit()
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
		SELECT r.id, r.alias, r.model, r.vendor, r.series, r.model_name, r.version, r.suffix, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       o.order_index, r.supported_formats, r.custom_headers, r.key_ids, r.notes, r.source, p.custom_headers, p.supported_formats, p.format_endpoints
		FROM rapi r
		LEFT JOIN platform p ON r.platform_id = p.id
		INNER JOIN lapi_rapi_order o ON r.id = o.rapi_id
		WHERE o.lapi_id = ? AND r.enabled = 1 AND r.available = 1 AND p.enabled = 1
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

// DeleteRAPICascade deletes a RAPI and removes all its references from lapi_rapi_order and rapi_metrics.
// Used when the RAPI is in invalid/disabled state and the user confirms cascade deletion.
func (db *DB) DeleteRAPICascade(id int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM lapi_rapi_order WHERE rapi_id = ?", id); err != nil {
		return fmt.Errorf("清理路由映射失败: %w", err)
	}
	if _, err := tx.Exec("DELETE FROM rapi_metrics WHERE rapi_id = ?", id); err != nil {
		return fmt.Errorf("清理统计数据失败: %w", err)
	}
	if _, err := tx.Exec("DELETE FROM rapi WHERE id = ?", id); err != nil {
		return fmt.Errorf("删除模型失败: %w", err)
	}
	return tx.Commit()
}

// DeletePlatformCascade deletes a platform and all its child RAPIs (with their references).
// Used when the platform is in invalid/disabled state and the user confirms cascade deletion.
func (db *DB) DeletePlatformCascade(id int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Clean up all child RAPI references first
	if _, err := tx.Exec(`
		DELETE FROM lapi_rapi_order WHERE rapi_id IN (SELECT id FROM rapi WHERE platform_id = ?)
	`, id); err != nil {
		return fmt.Errorf("清理路由映射失败: %w", err)
	}
	if _, err := tx.Exec(`
		DELETE FROM rapi_metrics WHERE rapi_id IN (SELECT id FROM rapi WHERE platform_id = ?)
	`, id); err != nil {
		return fmt.Errorf("清理统计数据失败: %w", err)
	}
	// Delete all child RAPIs
	if _, err := tx.Exec("DELETE FROM rapi WHERE platform_id = ?", id); err != nil {
		return fmt.Errorf("删除子模型失败: %w", err)
	}
	// Delete platform credentials（绑定/封锁/令牌缓存由 FK ON DELETE CASCADE 清理）
	if _, err := tx.Exec("DELETE FROM credential WHERE platform_id = ?", id); err != nil {
		return fmt.Errorf("删除平台凭据失败: %w", err)
	}
	// Delete the platform itself
	if _, err := tx.Exec("DELETE FROM platform WHERE id = ?", id); err != nil {
		return fmt.Errorf("删除平台失败: %w", err)
	}
	return tx.Commit()
}

// GetRAPIsByPlatform returns all RAPIs for a specific platform
func (db *DB) GetRAPIsByPlatform(platformID int64) ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.vendor, r.series, r.model_name, r.version, r.suffix, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       0 AS order_index, r.supported_formats, r.custom_headers, r.key_ids, r.notes, r.source, p.custom_headers, p.supported_formats, p.format_endpoints
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
		SELECT id, alias, notes, vendor, series, model_name, version, suffix, enabled, created_at FROM lapi ORDER BY sort_order ASC, id ASC
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
		var notesNull, vendorNull, seriesNull, nameNull, versionNull, suffixNull sql.NullString

		err := rows.Scan(&u.ID, &u.Alias, &notesNull, &vendorNull, &seriesNull, &nameNull, &versionNull, &suffixNull, &enabled, &created)
		if err != nil {
			return nil, err
		}
		u.Enabled = enabled == 1
		u.Notes = notesNull.String
		u.Vendor = vendorNull.String
		u.Series = seriesNull.String
		u.ModelName = nameNull.String
		u.Version = versionNull.String
		u.Suffix = suffixNull.String

		if created.Valid {
			u.CreatedAt = created.Time
		}

		lapis = append(lapis, u)
	}
	return lapis, nil
}

func (db *DB) GetLAPIByID(id int64) (*models.LAPI, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var u models.LAPI
	var enabled int
	var created sql.NullTime
	var notesNull, vendorNull, seriesNull, nameNull, versionNull, suffixNull sql.NullString

	err := db.conn.QueryRow(`
		SELECT id, alias, notes, vendor, series, model_name, version, suffix, enabled, created_at FROM lapi WHERE id = ?
	`, id).Scan(&u.ID, &u.Alias, &notesNull, &vendorNull, &seriesNull, &nameNull, &versionNull, &suffixNull, &enabled, &created)
	if err != nil {
		return nil, err
	}
	u.Enabled = enabled == 1
	u.Notes = notesNull.String
	u.Vendor = vendorNull.String
	u.Series = seriesNull.String
	u.ModelName = nameNull.String
	u.Version = versionNull.String
	u.Suffix = suffixNull.String
	if created.Valid {
		u.CreatedAt = created.Time
	}
	return &u, nil
}

func (db *DB) GetLAPIByAlias(alias string) (*models.LAPI, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var u models.LAPI
	var enabled int
	var notes, vendorNull, seriesNull, nameNull, versionNull, suffixNull sql.NullString
	var created sql.NullTime

	err := db.conn.QueryRow(`
		SELECT id, alias, notes, vendor, series, model_name, version, suffix, enabled, created_at FROM lapi WHERE LOWER(alias) = LOWER(?)
	`, alias).Scan(&u.ID, &u.Alias, &notes, &vendorNull, &seriesNull, &nameNull, &versionNull, &suffixNull, &enabled, &created)
	u.Enabled = enabled == 1

	if err != nil {
		return nil, err
	}

	u.Notes = notes.String
	u.Vendor = vendorNull.String
	u.Series = seriesNull.String
	u.ModelName = nameNull.String
	u.Version = versionNull.String
	u.Suffix = suffixNull.String

	if created.Valid {
		u.CreatedAt = created.Time
	}

	return &u, nil
}

// FindLAPIByModelIdentity finds a LAPI whose (vendor, series, version, suffix)
// naming identity matches the given values. Used for auto-mapping: when a new
// RAPI is created with matching identity, it is added to this LAPI.
// Returns nil if no match found or if all four fields are empty.
func (db *DB) FindLAPIByModelIdentity(vendor, series, version, suffix string) (*models.LAPI, error) {
	if vendor == "" && series == "" && version == "" && suffix == "" {
		return nil, nil // No identity to match
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	var u models.LAPI
	var enabled int
	var notes, vendorNull, seriesNull, nameNull, versionNull, suffixNull sql.NullString
	var created sql.NullTime

	err := db.conn.QueryRow(`
		SELECT id, alias, notes, vendor, series, model_name, version, suffix, enabled, created_at FROM lapi
		WHERE COALESCE(vendor,'') = ? AND COALESCE(series,'') = ? AND COALESCE(version,'') = ? AND COALESCE(suffix,'') = ?
		LIMIT 1
	`, vendor, series, version, suffix).Scan(&u.ID, &u.Alias, &notes, &vendorNull, &seriesNull, &nameNull, &versionNull, &suffixNull, &enabled, &created)
	if err != nil {
		return nil, err // sql.ErrNoRows if not found
	}

	u.Enabled = enabled == 1
	u.Notes = notes.String
	u.Vendor = vendorNull.String
	u.Series = seriesNull.String
	u.ModelName = nameNull.String
	u.Version = versionNull.String
	u.Suffix = suffixNull.String
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
		INSERT INTO lapi (alias, notes, vendor, series, model_name, version, suffix, enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, u.Alias, u.Notes, u.Vendor, u.Series, u.ModelName, u.Version, u.Suffix, enabledVal)

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
	_, err := db.conn.Exec("UPDATE lapi SET alias = ?, notes = ?, vendor = ?, series = ?, model_name = ?, version = ?, suffix = ?, enabled = ? WHERE id = ?", l.Alias, l.Notes, l.Vendor, l.Series, l.ModelName, l.Version, l.Suffix, enabledVal, l.ID)
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
		SELECT r.id, r.alias, r.model, r.vendor, r.series, r.model_name, r.version, r.suffix, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       o.order_index, r.supported_formats, r.custom_headers, r.key_ids, r.notes, r.source, p.custom_headers, p.supported_formats, p.format_endpoints
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

// expiresArg converts an *time.Time into a driver.Value suitable for SQL binding:
// nil/zero becomes NULL, otherwise the time itself. Used by the platform_keys
// INSERT/UPDATE statements for the expires_at column.
func expiresArg(t *time.Time) interface{} {
	if t == nil || t.IsZero() {
		return nil
	}
	return *t
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
	// failOther = 4xx other than 401/429; 5xx goes to fail500 only. The old
	// range 400..599 (minus 2xx/401/429) double-counted every 5xx into both
	// fail500 and failOther, inflating insights errRate (which sums all four).
	failOther := 0
	if statusCode >= 400 && statusCode <= 499 && statusCode != 401 && statusCode != 429 {
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

// migrateFailOtherDedup corrects historical fail_other inflation from the
// pre-fix RecordRequest, which double-counted every 5xx into both fail500
// and fail_other. The inflation equals the 5xx count exactly (fail_500), so
// true_fail_other = stored_fail_other − stored_fail_500. Guarded by a
// settings flag so it runs once: after the fix, new 5xx no longer touch
// fail_other, and re-running would over-subtract.
func (db *DB) migrateFailOtherDedup() error {
	const flag = "migrations.fail_other_dedup"
	if done, _ := db.GetSetting(flag); done != "" {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, err := db.conn.Exec(`UPDATE rapi_metrics SET fail_other = fail_other - fail_500 WHERE fail_other > fail_500`); err != nil {
		return err
	}
	if _, err := db.conn.Exec(`UPDATE rapi_metrics SET fail_other = 0 WHERE fail_other < 0`); err != nil {
		return err
	}
	_, err := db.conn.Exec(`INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
		flag, "1", time.Now())
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

// migrateDropWebpageColumns removes all webpage-platform-related columns from
// platform and platform_keys tables. Webpage platform support (browser session
// replay) has been removed as the approach was unimplementable. SQLite ≥3.35
// supports ALTER TABLE DROP COLUMN; guard with pragma_table_info so this is
// idempotent and a no-op on fresh installs.
func (db *DB) migrateDropWebpageColumns() {
	dropColumnIfExists := func(table, col string) {
		var count int
		row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?`, table, col)
		if row.Scan(&count) == nil && count > 0 {
			if _, err := db.conn.Exec(fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`, table, col)); err != nil {
				slog.Warn("[WARN] drop column failed",
					"component", "db", "table", table, "column", col, "error", err.Error())
			}
		}
	}
	dropColumnIfExists("platform", "webpage_domain")
	dropColumnIfExists("platform", "is_dynamic")
	dropColumnIfExists("platform", "push_secret")
	dropColumnIfExists("platform_keys", "session_headers")
	dropColumnIfExists("platform_keys", "reusable_status")
	dropColumnIfExists("platform_keys", "reusable_reasons")
}

func (db *DB) migrateAddPlatformKeyFailureColumns() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform_keys') WHERE name='failure_type'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE platform_keys ADD COLUMN failure_type INTEGER NOT NULL DEFAULT 0`)
		db.conn.Exec(`ALTER TABLE platform_keys ADD COLUMN failure_reason TEXT NOT NULL DEFAULT ''`)
		db.conn.Exec(`ALTER TABLE platform_keys ADD COLUMN failed_at DATETIME`)
	}
}

// migrateAddPlatformKeyExpiryColumns adds the expires_at and is_free columns to
// platform_keys. expires_at is nullable (NULL = never expires); is_free is 0/1.
// Idempotent: a no-op on fresh installs (CREATE TABLE already has the columns)
// and on already-migrated databases.
func (db *DB) migrateAddPlatformKeyExpiryColumns() {
	addIfMissing := func(col, ddl string) {
		var count int
		row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform_keys') WHERE name=?`, col)
		if row.Scan(&count) == nil && count == 0 {
			db.conn.Exec(ddl)
		}
	}
	// DATETIME allows NULL; no DEFAULT so existing rows get NULL (never expires).
	addIfMissing("expires_at", `ALTER TABLE platform_keys ADD COLUMN expires_at DATETIME`)
	// Existing keys default to paid (0) — operators explicitly mark free keys.
	addIfMissing("is_free", `ALTER TABLE platform_keys ADD COLUMN is_free INTEGER NOT NULL DEFAULT 0`)
}

func (db *DB) migrateAddLAPINotesColumn() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('lapi') WHERE name='notes'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE lapi ADD COLUMN notes TEXT NOT NULL DEFAULT ''`)
	}
}

// migrateAddModelIdentityColumns adds series, model_name, and version columns to both
// rapi and lapi tables. These three fields together form the "model identity" used for
// auto-mapping: when a new RAPI is created, if its (series, model_name, version) matches
// an existing LAPI's, it is automatically added to that LAPI's routing chain.
func (db *DB) migrateAddModelIdentityColumns() {
	for _, table := range []string{"rapi", "lapi"} {
		for _, col := range []string{"series", "model_name", "version"} {
			var count int
			row := db.conn.QueryRow(
				fmt.Sprintf(`SELECT COUNT(*) FROM pragma_table_info('%s') WHERE name='%s'`, table, col),
			)
			if row.Scan(&count) == nil && count == 0 {
				db.conn.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s TEXT NOT NULL DEFAULT ''`, table, col))
			}
		}
	}
}

// migrateUnifiedModelNaming adds the vendor/suffix naming columns to rapi and
// lapi, migrates legacy model_name values into suffix, and backfills empty
// rapi.notes with the original upstream model string (auto-discovered models
// keep their original name as the 模型备注). Idempotent.
func (db *DB) migrateUnifiedModelNaming() {
	for _, table := range []string{"rapi", "lapi"} {
		for _, col := range []string{"vendor", "suffix"} {
			var count int
			row := db.conn.QueryRow(
				fmt.Sprintf(`SELECT COUNT(*) FROM pragma_table_info('%s') WHERE name='%s'`, table, col),
			)
			if row.Scan(&count) == nil && count == 0 {
				db.conn.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s TEXT NOT NULL DEFAULT ''`, table, col))
			}
		}
		// Legacy "名 (model_name)" values become the suffix (e.g. sonnet),
		// preserving display data such as claude-sonnet-4.7 → claude-4.7-sonnet.
		db.conn.Exec(fmt.Sprintf(`UPDATE %s SET suffix = model_name WHERE suffix = '' AND model_name != ''`, table))
	}
	// Backfill empty rapi.notes as the model remark (模型备注):
	// - auto-discovered models keep their original upstream name as the remark;
	// - manually added models whose alias differs from the upstream name keep
	//   the user's own naming (用户命名) as the remark.
	db.conn.Exec(`UPDATE rapi SET notes = model WHERE notes = '' AND model != '' AND (source != 'manual' OR alias = '' OR alias = LOWER(model))`)
	db.conn.Exec(`UPDATE rapi SET notes = alias WHERE notes = '' AND alias != '' AND source = 'manual' AND alias != LOWER(model)`)
}

// migrateRAPIUniqueAliasToPerPlatform changes the rapi.alias uniqueness constraint from
// a global UNIQUE(alias) to UNIQUE(platform_id, alias), allowing different platforms to
// share the same model alias (e.g. glm-5.2 on JD and on ZhipuAI).
// This is a table-recreation migration (SQLite does not support DROP CONSTRAINT).
// Idempotent: skips if the new index already exists.
func (db *DB) migrateRAPIUniqueAliasToPerPlatform() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 自然键终态（credential 已存在）：rapi.alias 已降级为显示名，身份唯一性由
	// idx_rapi_platform_model 承担。若继续执行本迁移，会在每次启动时把
	// UNIQUE(platform_id, alias) 加回来，随后被 migrateNaturalKeys 的表重建拆掉，
	// 形成"每次启动重建一次 rapi 表"的无谓抖动。
	if tableExists(db.conn, "credential") {
		return nil
	}

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
	// preserving all existing data. The schema must include every column any
	// earlier migration may have added (notably sort_order, added before this
	// migration runs) or that data is silently dropped on upgrade.
	if _, err = tx.Exec(`
		CREATE TABLE rapi_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			notes TEXT NOT NULL DEFAULT '',
			vendor TEXT NOT NULL DEFAULT '',
			series TEXT NOT NULL DEFAULT '',
			model_name TEXT NOT NULL DEFAULT '',
			version TEXT NOT NULL DEFAULT '',
			suffix TEXT NOT NULL DEFAULT '',
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
			key_ids TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT 'manual',
			sort_order INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
			UNIQUE(platform_id, alias)
		);
	`); err != nil {
		return err
	}
	// Copy every column the new table keeps, resolved dynamically so a column
	// present in the old table is never left behind.
	keep := []string{
		"id", "alias", "model", "notes", "vendor", "series", "model_name", "version", "suffix",
		"platform_id", "enabled", "available", "unavailable_reason",
		"base_cost", "high_cost", "rpm_limit", "rph_limit", "rpd_limit", "tpm_limit", "tph_limit", "tpd_limit",
		"time_period_rules", "supported_formats", "custom_headers", "key_ids", "source", "sort_order",
		"created_at", "updated_at",
	}
	copySQL, err := buildCopyCommonColumns(tx, "rapi", "rapi_new", keep)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(copySQL); err != nil {
		return err
	}
	if _, err = tx.Exec(`
		DROP TABLE rapi;
		ALTER TABLE rapi_new RENAME TO rapi;
		CREATE INDEX IF NOT EXISTS idx_rapi_platform ON rapi(platform_id);
		CREATE UNIQUE INDEX idx_rapi_platform_alias ON rapi(platform_id, alias);
	`); err != nil {
		return err
	}

	slog.Info("[DB] migrated rapi.alias uniqueness: global → per-platform", "component", "db")
	return tx.Commit()
}

// migrateEncryptTokens encrypts all existing plaintext tokens in the credential
// (or, pre-natural-key, platform_keys), platform and token_cache tables using
// AES-256-GCM. Rows that are empty or already carry the "enc:" prefix are skipped.
//
// 该迁移必须对表结构演进免疫：token_cache 的主键列在自然键迁移前是
// platform_key_id、迁移后是 credential_id；platform_keys 在自然键终态下已被
// DROP。早期版本写死列名/表名，导致已迁移完成的库每次启动都报
// "no such column: platform_key_id" 而无法启动。现在按实际存在的表与列处理。
//
// This migration is idempotent.
func (db *DB) migrateEncryptTokens() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// token_cache 主键列名随 schema 演进（platform_key_id → credential_id）。
	cachePK := "platform_key_id"
	if columnExists(db.conn, "token_cache", "credential_id") {
		cachePK = "credential_id"
	}

	// (表名, 主键列)：仅在表存在时处理。platform_keys 与 credential 是同一
	// 语义的前后两代表，自然键迁移后只有后者存在。
	//
	// 注意：这些探测必须在 Begin 之前完成。连接池被限制为单连接
	// （SetMaxOpenConns(1)），事务会独占该连接，此时再用 db.conn 查询会永久
	// 阻塞（死锁）。
	type tokenTable struct{ table, pk string }
	var targets []tokenTable
	for _, t := range []tokenTable{
		{"platform_keys", "id"},
		{"credential", "id"},
		{"token_cache", cachePK},
	} {
		// 表存在还不够：中途迁移/手工恢复可能留下未知列形态。逐项验证
		// 固定字面量主键列存在，未知形态安全跳过而不是让启动因 no such column 退出。
		if tableExists(db.conn, t.table) && columnExists(db.conn, t.table, t.pk) {
			targets = append(targets, t)
		}
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, t := range targets {
		if err := encryptTokenColumn(tx, t.table, t.pk); err != nil {
			return err
		}
	}

	// Encrypt platform.token (legacy column, kept for compat)
	platRows2, err := tx.Query(`SELECT id, token FROM platform`)
	if err != nil {
		return err
	}
	type tokenRow struct {
		id    int64
		token string
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
		if _, err = tx.Exec(`UPDATE platform SET token = ? WHERE id = ?`, enc, r.id); err != nil {
			return fmt.Errorf("update platform id=%d: %w", r.id, err)
		}
	}

	return tx.Commit()
}

// encryptTokenColumn 把 table.pkCol/token 中尚未加密的明文令牌就地加密。
// table 与 pkCol 只来自 migrateEncryptTokens 内的固定字面量集合，不接受外部输入。
func encryptTokenColumn(tx *sql.Tx, table, pkCol string) error {
	rows, err := tx.Query(fmt.Sprintf(`SELECT %s, token FROM %s`, pkCol, table))
	if err != nil {
		return fmt.Errorf("scan %s: %w", table, err)
	}
	type tokenRow struct {
		id    int64
		token string
	}
	var all []tokenRow
	for rows.Next() {
		var r tokenRow
		if err := rows.Scan(&r.id, &r.token); err != nil {
			rows.Close()
			return fmt.Errorf("scan %s: %w", table, err)
		}
		all = append(all, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", table, err)
	}

	for _, r := range all {
		if r.token == "" || strings.HasPrefix(r.token, "enc:") {
			continue // 空值或已加密，跳过。
		}
		enc, err := crypto.Encrypt(r.token)
		if err != nil {
			return fmt.Errorf("encrypt %s id=%d: %w", table, r.id, err)
		}
		if _, err = tx.Exec(fmt.Sprintf(`UPDATE %s SET token = ? WHERE %s = ?`, table, pkCol), enc, r.id); err != nil {
			return fmt.Errorf("update %s id=%d: %w", table, r.id, err)
		}
	}
	return nil
}

// migrateAddSortOrderColumns adds sort_order column to platform, rapi and lapi tables if missing,
// and initialises existing rows with their current id order.
func (db *DB) migrateAddSortOrderColumns() {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, tbl := range []string{"platform", "rapi", "lapi"} {
		row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name='sort_order'`, tbl)
		var cnt int
		if err := row.Scan(&cnt); err != nil || cnt > 0 {
			continue // already exists
		}
		if _, err := db.conn.Exec(`ALTER TABLE ` + tbl + ` ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0`); err != nil {
			slog.Warn("[DB] migrateAddSortOrderColumns: add column failed",
				"component", "db", "table", tbl, "error", err.Error())
			continue
		}
		// Initialise sort_order = rowid so existing rows keep their original order
		if _, err := db.conn.Exec(`UPDATE ` + tbl + ` SET sort_order = id`); err != nil {
			slog.Warn("[DB] migrateAddSortOrderColumns: init order failed",
				"component", "db", "table", tbl, "error", err.Error())
		}
	}
}

// SetPlatformSortOrder persists a new display order for platforms.
// ids must contain every platform id; each receives order index = its position in the slice.
func (db *DB) SetPlatformSortOrder(ids []int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i, id := range ids {
		if _, err := tx.Exec(`UPDATE platform SET sort_order = ? WHERE id = ?`, i, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SetRAPISortOrder persists a new display order for rapis.
// ids must contain every rapi id; each receives order index = its position in the slice.
func (db *DB) SetRAPISortOrder(ids []int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i, id := range ids {
		if _, err := tx.Exec(`UPDATE rapi SET sort_order = ? WHERE id = ?`, i, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SetLAPISortOrder persists a new display order for lapis.
// ids must contain every lapi id; each receives order index = its position in the slice.
func (db *DB) SetLAPISortOrder(ids []int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i, id := range ids {
		if _, err := tx.Exec(`UPDATE lapi SET sort_order = ? WHERE id = ?`, i, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ============ Change Log ============
