package db

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"gateway/internal/crypto"
	"gateway/internal/models"

	_ "modernc.org/sqlite"
)

// TestMigrateNaturalKeysUpgradeAndIdempotent 用旧形态库（platform_keys /
// UNIQUE(platform_id, alias) / 旧 token_cache / 旧 key_model_blocks）验证：
//  1. PreviewNaturalKeyMigration 只读——预测合并清单但不落任何写；
//  2. 真实迁移完成 token_hash 去重合并、平台 base_url 归一化合并、端点
//     (platform, model) 合并、key_ids → endpoint_credential 物化与派生投影；
//  3. 第二次运行整体跳过（幂等，报告为空）。
func TestMigrateNaturalKeysUpgradeAndIdempotent(t *testing.T) {
	initTestCrypto(t)
	conn, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "mig.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	instance = &DB{conn: conn}

	// 旧形态最小 schema（只含迁移代码路径真正访问的列；表重建按列交集拷贝）。
	if _, err := conn.Exec(`
		CREATE TABLE platform (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			base_url TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE platform_keys (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			platform_id INTEGER NOT NULL,
			key_index INTEGER NOT NULL DEFAULT 0,
			token TEXT NOT NULL DEFAULT '',
			label TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1,
			failure_type INTEGER NOT NULL DEFAULT 0,
			failure_reason TEXT NOT NULL DEFAULT '',
			failed_at DATETIME,
			expires_at DATETIME,
			is_free INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(platform_id, key_index)
		);
		CREATE TABLE rapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			platform_id INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			key_ids TEXT NOT NULL DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(platform_id, alias)
		);
		CREATE TABLE lapi (id INTEGER PRIMARY KEY AUTOINCREMENT, alias TEXT NOT NULL UNIQUE);
		CREATE TABLE lapi_rapi_order (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			lapi_id INTEGER NOT NULL, rapi_id INTEGER NOT NULL, order_index INTEGER NOT NULL,
			UNIQUE(lapi_id, rapi_id), UNIQUE(lapi_id, order_index)
		);
		CREATE TABLE key_model_blocks (
			key_id INTEGER NOT NULL, rapi_id INTEGER NOT NULL,
			reason TEXT NOT NULL DEFAULT '', expires_at DATETIME,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (key_id, rapi_id)
		);
		CREATE TABLE token_cache (
			platform_key_id INTEGER PRIMARY KEY,
			token TEXT NOT NULL, expires_at DATETIME,
			fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE rapi_metrics (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			rapi_id INTEGER NOT NULL, lapi_id INTEGER NOT NULL,
			total_requests INTEGER DEFAULT 0, success_requests INTEGER DEFAULT 0,
			fail_401 INTEGER DEFAULT 0, fail_429 INTEGER DEFAULT 0, fail_500 INTEGER DEFAULT 0,
			fail_other INTEGER DEFAULT 0, total_latency_ms INTEGER DEFAULT 0,
			token_count INTEGER DEFAULT 0, last_used DATETIME,
			UNIQUE(rapi_id, lapi_id)
		);
	`); err != nil {
		t.Fatalf("create old schema: %v", err)
	}

	encDup1, err := crypto.Encrypt("sk-dup")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	encDup2, err := crypto.Encrypt("sk-dup") // 同明文不同密文（随机 nonce）
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	encOther, err := crypto.Encrypt("sk-other")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// 两个平台归一化后同 base_url（大小写/默认端口/尾斜杠差异）；各持一把同
	// token 的 key（应合并），平台 2 另有一把独立 key。两平台各有 model=gpt-4
	// 的端点（平台合并后碰撞 → 端点合并）。
	if _, err := conn.Exec(`
		INSERT INTO platform (name, base_url) VALUES
			('A', 'https://API.Example.com:443/v1/'),
			('B', 'https://api.example.com/v1');
		INSERT INTO rapi (alias, model, platform_id, key_ids) VALUES
			('a', 'gpt-4', 1, '1'),
			('b', 'gpt-4', 2, '2,3');
		INSERT INTO lapi (alias) VALUES ('chat');
		INSERT INTO lapi_rapi_order (lapi_id, rapi_id, order_index) VALUES (1, 1, 0), (1, 2, 1);
		INSERT INTO rapi_metrics (rapi_id, lapi_id, total_requests, success_requests) VALUES (2, 1, 5, 4);
	`); err != nil {
		t.Fatalf("seed defs: %v", err)
	}
	if _, err := conn.Exec(`
		INSERT INTO platform_keys (platform_id, key_index, token, label) VALUES
			(1, 0, ?, 'k1'), (2, 0, ?, 'k2'), (2, 1, ?, 'k3');
	`, encDup1, encDup2, encOther); err != nil {
		t.Fatalf("seed keys: %v", err)
	}
	// ---- 1. dry-run 预览：预测准确且不写库 ----
	preview, err := PreviewNaturalKeyMigration(conn)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if preview.CredentialsCreated != 2 || len(preview.CredentialsMerged) != 1 {
		t.Errorf("preview credentials: created=%d merged=%+v, want 2 + 1 merge",
			preview.CredentialsCreated, preview.CredentialsMerged)
	}
	if cm := preview.CredentialsMerged[0]; cm.KeptID != 1 || cm.DroppedID != 2 ||
		cm.TokenHash != models.TokenHash("sk-dup") {
		t.Errorf("preview credential merge = %+v, want keep=1 drop=2 hash=sk-dup", cm)
	}
	if preview.PlatformsRenormalized != 1 || len(preview.PlatformsMerged) != 1 {
		t.Errorf("preview platforms: renorm=%d merged=%+v, want 1 + 1 merge",
			preview.PlatformsRenormalized, preview.PlatformsMerged)
	}
	if pm := preview.PlatformsMerged[0]; pm.KeptID != 1 || pm.DroppedID != 2 ||
		pm.BaseURL != "https://api.example.com/v1" {
		t.Errorf("preview platform merge = %+v, want keep=1 drop=2 normalized url", pm)
	}
	if len(preview.RAPIsMerged) != 1 || preview.RAPIsMerged[0].KeptID != 1 ||
		preview.RAPIsMerged[0].DroppedID != 2 || preview.RAPIsMerged[0].PlatformID != 1 {
		t.Errorf("preview rapi merges = %+v, want [keep=1 drop=2 platform=1]", preview.RAPIsMerged)
	}
	// dry-run 不落写：旧表与行数原样。
	if !tableExists(conn, "platform_keys") {
		t.Fatal("preview must not write: platform_keys already gone")
	}
	var n int
	if err := conn.QueryRow(`SELECT COUNT(*) FROM platform`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("preview must not write: platform count = %d (err %v), want 2", n, err)
	}

	// ---- 2. 真实迁移 ----
	rep, err := MigrateNaturalKeysConn(conn)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if rep.CredentialsCreated != 2 || len(rep.CredentialsMerged) != 1 {
		t.Errorf("credentials: created=%d merged=%+v, want 2 + 1", rep.CredentialsCreated, rep.CredentialsMerged)
	}
	if rep.BindingsCreated != 3 {
		t.Errorf("bindings created = %d, want 3 (1 + 去重后的 2)", rep.BindingsCreated)
	}
	if len(rep.RAPIsMerged) != 1 || len(rep.PlatformsMerged) != 1 || rep.PlatformsRenormalized != 1 {
		t.Errorf("merges: rapi=%+v platform=%+v renorm=%d", rep.RAPIsMerged, rep.PlatformsMerged, rep.PlatformsRenormalized)
	}

	// 平台：合并为 1 行，base_url 已归一化，name 唯一约束已去掉。
	var baseURL string
	if err := conn.QueryRow(`SELECT base_url FROM platform`).Scan(&baseURL); err != nil {
		t.Fatalf("platform after merge: %v", err)
	}
	if baseURL != "https://api.example.com/v1" {
		t.Errorf("base_url = %q, want normalized https://api.example.com/v1", baseURL)
	}
	if strings.Contains(tableSchemaSQL(conn, "platform"), "UNIQUE") {
		t.Error("platform schema still has UNIQUE (name should be display-only)")
	}

	// 凭据：保留原 id 1/3，同 token 的 id 2 被合并；幸存行都挂到保留平台。
	if err := conn.QueryRow(`SELECT COUNT(*) FROM credential`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("credential count = %d (err %v), want 2", n, err)
	}
	var hash string
	var pid int64
	if err := conn.QueryRow(`SELECT token_hash, platform_id FROM credential WHERE id = 3`).Scan(&hash, &pid); err != nil {
		t.Fatalf("credential 3: %v", err)
	}
	if hash != models.TokenHash("sk-other") || pid != 1 {
		t.Errorf("credential 3 = (hash %q, platform %d), want (sk-other hash, 1)", hash, pid)
	}

	// 端点：合并为 1 行；绑定 (1,1)+(1,3)；key_ids 派生投影 = "1,3"。
	var keyIDs string
	if err := conn.QueryRow(`SELECT key_ids FROM rapi`).Scan(&keyIDs); err != nil {
		t.Fatalf("rapi after merge: %v", err)
	}
	if keyIDs != "1,3" {
		t.Errorf("rapi key_ids = %q, want \"1,3\"", keyIDs)
	}
	if err := conn.QueryRow(`SELECT COUNT(*) FROM endpoint_credential`).Scan(&n); err != nil || n != 2 {
		t.Errorf("endpoint_credential rows = %d (err %v), want 2", n, err)
	}

	// 引用改挂：链上只剩幸存端点；统计并入幸存行。
	if err := conn.QueryRow(`SELECT COUNT(*) FROM lapi_rapi_order`).Scan(&n); err != nil || n != 1 {
		t.Errorf("lapi_rapi_order rows = %d (err %v), want 1", n, err)
	}
	var total int
	if err := conn.QueryRow(`SELECT total_requests FROM rapi_metrics WHERE rapi_id = 1 AND lapi_id = 1`).Scan(&total); err != nil || total != 5 {
		t.Errorf("rapi_metrics total = %d (err %v), want 5 re-homed to kept rapi", total, err)
	}

	// 运行时表换形：token_cache → credential_id；key_model_blocks FK → credential。
	if !columnExists(conn, "token_cache", "credential_id") || columnExists(conn, "token_cache", "platform_key_id") {
		t.Error("token_cache not rebuilt to credential_id shape")
	}
	if !strings.Contains(tableSchemaSQL(conn, "key_model_blocks"), "credential(") {
		t.Error("key_model_blocks not rebuilt with credential FK")
	}
	if tableExists(conn, "platform_keys") {
		t.Error("platform_keys should be dropped after migration")
	}

	// 自然键部分唯一索引生效：同平台同 model / 同归一化 base_url 插入必须失败。
	if _, err := conn.Exec(`INSERT INTO rapi (alias, model, platform_id) VALUES ('dup', 'gpt-4', 1)`); err == nil {
		t.Error("duplicate (platform_id, model) insert should fail on idx_rapi_platform_model")
	}
	if _, err := conn.Exec(`INSERT INTO platform (name, base_url) VALUES ('C', 'https://api.example.com/v1')`); err == nil {
		t.Error("duplicate base_url insert should fail on idx_platform_base_url")
	}

	// ---- 3. 幂等：第二次运行整体跳过 ----
	rep2, err := MigrateNaturalKeysConn(conn)
	if err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if !rep2.empty() {
		t.Errorf("second run report not empty: %+v", rep2)
	}
	if err := conn.QueryRow(`SELECT key_ids FROM rapi`).Scan(&keyIDs); err != nil || keyIDs != "1,3" {
		t.Errorf("after idempotent re-run key_ids = %q (err %v), want \"1,3\"", keyIDs, err)
	}
}

// TestMigrateNaturalKeysStalePlatformKeysResidue 回归测试：网关"无法启动"。
//
// 旧版本的 migrateAddPlatformKeys 每次启动都会把 platform.token 回填成一份全新的
// platform_keys（AUTOINCREMENT 从 1 重新开始），自然键迁移随后拿这些行去 merge，
// 撞上 credential.id 主键冲突 —— 于是每次启动都失败：
//
//	database init failed: migrateNaturalKeys: platform_keys to credential:
//	insert credential from key 1: UNIQUE constraint failed: credential.id (1555)
//
// 这里复现"自然键终态 + 回填残影"的库形态，要求迁移自行清理残影并正常通过，
// 且不把残影令牌当成真实凭据采纳（残影 token 仍保留在 platform.token 列）。
func TestMigrateNaturalKeysStalePlatformKeysResidue(t *testing.T) {
	initTestCrypto(t)
	conn, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "residue.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	instance = &DB{conn: conn}

	// 自然键终态 schema：credential 已存在、token_cache 以 credential_id 为键、
	// rapi 无 UNIQUE(platform_id, alias)。
	if _, err := conn.Exec(`
		CREATE TABLE platform (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL, base_url TEXT NOT NULL DEFAULT '', token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE credential (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			platform_id INTEGER NOT NULL DEFAULT 0, token_hash TEXT,
			token TEXT NOT NULL DEFAULT '', label TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1, failure_type INTEGER NOT NULL DEFAULT 0,
			failure_reason TEXT NOT NULL DEFAULT '', failed_at DATETIME, expires_at DATETIME,
			is_free INTEGER NOT NULL DEFAULT 0, sort_order INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE rapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT, alias TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '', platform_id INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1, available INTEGER NOT NULL DEFAULT 1,
			key_ids TEXT NOT NULL DEFAULT '', created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE token_cache (
			credential_id INTEGER PRIMARY KEY, token TEXT NOT NULL, expires_at DATETIME,
			fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE key_model_blocks (
			key_id INTEGER NOT NULL, rapi_id INTEGER NOT NULL, reason TEXT NOT NULL DEFAULT '',
			expires_at DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (key_id, rapi_id)
		);
		CREATE TABLE rapi_metrics (
			id INTEGER PRIMARY KEY AUTOINCREMENT, rapi_id INTEGER NOT NULL, lapi_id INTEGER NOT NULL,
			total_requests INTEGER DEFAULT 0, success_requests INTEGER DEFAULT 0,
			fail_401 INTEGER DEFAULT 0, fail_429 INTEGER DEFAULT 0, fail_500 INTEGER DEFAULT 0,
			fail_other INTEGER DEFAULT 0, total_latency_ms INTEGER DEFAULT 0,
			token_count INTEGER DEFAULT 0, last_used DATETIME, UNIQUE(rapi_id, lapi_id)
		);
		CREATE TABLE lapi (id INTEGER PRIMARY KEY AUTOINCREMENT, alias TEXT NOT NULL UNIQUE);
		CREATE TABLE lapi_rapi_order (
			id INTEGER PRIMARY KEY AUTOINCREMENT, lapi_id INTEGER NOT NULL, rapi_id INTEGER NOT NULL,
			order_index INTEGER NOT NULL, UNIQUE(lapi_id, rapi_id), UNIQUE(lapi_id, order_index)
		);
		CREATE TABLE platform_keys (
			id INTEGER PRIMARY KEY AUTOINCREMENT, platform_id INTEGER NOT NULL,
			key_index INTEGER NOT NULL DEFAULT 0, token TEXT NOT NULL DEFAULT '',
			label TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
			failure_type INTEGER NOT NULL DEFAULT 0, failure_reason TEXT NOT NULL DEFAULT '',
			failed_at DATETIME, expires_at DATETIME, is_free INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(platform_id, key_index)
		);
	`); err != nil {
		t.Fatalf("create terminal schema: %v", err)
	}

	encLive, err := crypto.Encrypt("sk-live")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	encLegacy, err := crypto.Encrypt("sk-legacy")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// platform.token 保存着 legacy 默认令牌；credential 已有真实凭据（id=1）；
	// platform_keys 是旧版本回填出来的残影——id 恰好也从 1 开始，与 credential.id 冲突。
	// 注意：每条 Exec 只带一条语句——多语句 + 占位参数在驱动里绑定顺序不可靠。
	if _, err := conn.Exec(
		`INSERT INTO platform (id, name, base_url, token) VALUES (1, 'A', 'https://a.example.com/v1', ?)`,
		encLegacy); err != nil {
		t.Fatalf("seed platform: %v", err)
	}
	if _, err := conn.Exec(
		`INSERT INTO credential (id, platform_id, token_hash, token, label) VALUES (1, 1, ?, ?, 'live')`,
		models.TokenHash("sk-live"), encLive); err != nil {
		t.Fatalf("seed credential: %v", err)
	}
	if _, err := conn.Exec(
		`INSERT INTO rapi (id, alias, model, platform_id) VALUES (1, 'a', 'gpt-4', 1)`); err != nil {
		t.Fatalf("seed rapi: %v", err)
	}
	if _, err := conn.Exec(
		`INSERT INTO platform_keys (id, platform_id, key_index, token, label, enabled)
		 VALUES (1, 1, 0, ?, 'default', 1)`, encLegacy); err != nil {
		t.Fatalf("seed platform_keys residue: %v", err)
	}

	rep, err := MigrateNaturalKeysConn(conn)
	if err != nil {
		t.Fatalf("migrate must not fail on stale platform_keys residue: %v", err)
	}
	if rep.CredentialsCreated != 0 || len(rep.CredentialsMerged) != 0 {
		t.Errorf("residue must not be adopted as credentials: created=%d merged=%+v",
			rep.CredentialsCreated, rep.CredentialsMerged)
	}
	if tableExists(conn, "platform_keys") {
		t.Error("stale platform_keys residue should be dropped")
	}

	// 真实凭据原样保留，残影令牌未被采纳。
	var n int
	if err := conn.QueryRow(`SELECT COUNT(*) FROM credential`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("credential rows = %d (err %v), want 1", n, err)
	}
	var tok string
	if err := conn.QueryRow(`SELECT token FROM credential WHERE id = 1`).Scan(&tok); err != nil || tok != encLive {
		t.Errorf("credential 1 token = %q (err %v), want live token preserved", tok, err)
	}
	// 残影的 token 未丢失：仍原样保存在 platform.token 列。
	if err := conn.QueryRow(`SELECT token FROM platform WHERE id = 1`).Scan(&tok); err != nil || tok != encLegacy {
		t.Errorf("platform.token = %q (err %v), want legacy token preserved", tok, err)
	}
	// 幂等：再跑一次不应报错，也不应有任何变更。
	rep2, err := MigrateNaturalKeysConn(conn)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if !rep2.empty() {
		t.Errorf("second run report not empty: %+v", rep2)
	}
}

// TestMigrateNaturalKeysMixedCredentialState verifies a partially migrated database
// can be reconciled without dropping legitimate rows or colliding old/new IDs.
func TestMigrateNaturalKeysMixedCredentialState(t *testing.T) {
	initTestCrypto(t)
	conn, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "mixed.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	instance = &DB{conn: conn}

	if _, err := conn.Exec(`
		CREATE TABLE platform (
			id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
			base_url TEXT NOT NULL DEFAULT '', token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE credential (
			id INTEGER PRIMARY KEY AUTOINCREMENT, platform_id INTEGER NOT NULL DEFAULT 0,
			token_hash TEXT, token TEXT NOT NULL DEFAULT '', label TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1, failure_type INTEGER NOT NULL DEFAULT 0,
			failure_reason TEXT NOT NULL DEFAULT '', failed_at DATETIME, expires_at DATETIME,
			is_free INTEGER NOT NULL DEFAULT 0, sort_order INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE UNIQUE INDEX idx_credential_token_hash ON credential(token_hash) WHERE token_hash IS NOT NULL;
		CREATE TABLE platform_keys (
			id INTEGER PRIMARY KEY AUTOINCREMENT, platform_id INTEGER NOT NULL,
			key_index INTEGER NOT NULL DEFAULT 0, token TEXT NOT NULL DEFAULT '',
			label TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
			failure_type INTEGER NOT NULL DEFAULT 0, failure_reason TEXT NOT NULL DEFAULT '',
			failed_at DATETIME, expires_at DATETIME, is_free INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(platform_id, key_index)
		);
		CREATE TABLE rapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT, alias TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '', platform_id INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1, available INTEGER NOT NULL DEFAULT 1,
			key_ids TEXT NOT NULL DEFAULT '', created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE key_model_blocks (
			key_id INTEGER NOT NULL, rapi_id INTEGER NOT NULL, reason TEXT NOT NULL DEFAULT '',
			expires_at DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (key_id, rapi_id)
		);
		CREATE TABLE token_cache (
			platform_key_id INTEGER PRIMARY KEY, token TEXT NOT NULL,
			expires_at DATETIME, fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE lapi (id INTEGER PRIMARY KEY AUTOINCREMENT, alias TEXT NOT NULL UNIQUE);
		CREATE TABLE lapi_rapi_order (
			id INTEGER PRIMARY KEY AUTOINCREMENT, lapi_id INTEGER NOT NULL, rapi_id INTEGER NOT NULL,
			order_index INTEGER NOT NULL, UNIQUE(lapi_id, rapi_id), UNIQUE(lapi_id, order_index)
		);
		CREATE TABLE rapi_metrics (
			id INTEGER PRIMARY KEY AUTOINCREMENT, rapi_id INTEGER NOT NULL, lapi_id INTEGER NOT NULL,
			total_requests INTEGER NOT NULL DEFAULT 0, success_requests INTEGER NOT NULL DEFAULT 0,
			fail_401 INTEGER NOT NULL DEFAULT 0, fail_429 INTEGER NOT NULL DEFAULT 0,
			fail_500 INTEGER NOT NULL DEFAULT 0, fail_other INTEGER NOT NULL DEFAULT 0,
			total_latency_ms INTEGER NOT NULL DEFAULT 0, token_count INTEGER NOT NULL DEFAULT 0,
			last_used DATETIME, UNIQUE(rapi_id, lapi_id)
		);
		INSERT INTO platform (id, name, base_url, token)
			VALUES (1, 'mixed', 'https://mixed.example/v1', 'sk-default');
	`); err != nil {
		t.Fatalf("create mixed schema: %v", err)
	}

	enc := func(plain string) string {
		t.Helper()
		v, err := crypto.Encrypt(plain)
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		return v
	}
	if _, err := conn.Exec(`INSERT INTO credential (id, platform_id, token_hash, token, label)
		VALUES (1, 1, ?, ?, 'existing')`, models.TokenHash("sk-existing"), enc("sk-existing")); err != nil {
		t.Fatalf("seed credential: %v", err)
	}
	if _, err := conn.Exec(`INSERT INTO platform_keys
		(id, platform_id, key_index, token, label) VALUES (1, 1, 0, ?, 'default'), (2, 1, 1, ?, 'real')`,
		enc("sk-default"), enc("sk-real")); err != nil {
		t.Fatalf("seed platform_keys: %v", err)
	}
	if _, err := conn.Exec(`INSERT INTO rapi (id, alias, model, platform_id, key_ids)
		VALUES (1, 'model', 'model', 1, '1,2')`); err != nil {
		t.Fatalf("seed rapi: %v", err)
	}
	if _, err := conn.Exec(`INSERT INTO key_model_blocks (key_id, rapi_id) VALUES (2, 1)`); err != nil {
		t.Fatalf("seed key_model_blocks: %v", err)
	}
	if _, err := conn.Exec(`INSERT INTO token_cache (platform_key_id, token) VALUES (2, 'cache')`); err != nil {
		t.Fatalf("seed token_cache: %v", err)
	}

	rep, err := MigrateNaturalKeysConn(conn)
	if err != nil {
		t.Fatalf("mixed migration: %v", err)
	}
	if rep.CredentialsCreated != 2 {
		t.Fatalf("created = %d, want 2", rep.CredentialsCreated)
	}
	var count int
	if err := conn.QueryRow(`SELECT COUNT(*) FROM credential`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("credential count = %d, %v; want 3", count, err)
	}
	var token string
	if err := conn.QueryRow(`SELECT token FROM credential WHERE token_hash = ?`, models.TokenHash("sk-real")).Scan(&token); err != nil {
		t.Fatalf("real credential: %v", err)
	}
	if plain, err := crypto.Decrypt(token); err != nil || plain != "sk-real" {
		t.Fatalf("real credential decrypt = %q, %v", plain, err)
	}
	var keyIDs string
	if err := conn.QueryRow(`SELECT key_ids FROM rapi WHERE id = 1`).Scan(&keyIDs); err != nil {
		t.Fatalf("read key_ids: %v", err)
	}
	if keyIDs == "" || keyIDs == "1" || keyIDs == "2" {
		t.Fatalf("key_ids were not remapped: %q", keyIDs)
	}
	if tableExists(conn, "platform_keys") {
		t.Fatal("platform_keys not dropped")
	}
}
