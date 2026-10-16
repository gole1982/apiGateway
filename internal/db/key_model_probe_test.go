package db

import (
	"strings"
	"testing"
	"time"

	"gateway/internal/models"
)

// TestRecordKeyModelProbeDedupes 真实事故回归：上游目录含重复模型名，
// 原实现逐个 INSERT 撞 PRIMARY KEY 后回滚整个事务，导致该平台整轮探测白跑
// （实测曾让全部 19 个平台的探测都读不回来）。现在必须能顶住。
func TestRecordKeyModelProbeDedupes(t *testing.T) {
	db := setupTestDB(t)
	db.migrateKeyModelProbe()

	p := &models.Platform{Name: "dup-plat", BaseURL: "https://api.dup.test/v1", Enabled: true, Available: true}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	k := models.PlatformKey{PlatformID: p.ID, Token: "sk-dup", Enabled: true}
	if err := db.AddPlatformKey(&k); err != nil {
		t.Fatalf("AddPlatformKey: %v", err)
	}
	hash := models.TokenHash("sk-dup")

	// 完全重复 + 大小写变体 + 带空白 —— 都算同一个模型
	dupCatalog := []string{
		"inclusionAI/Ling-3.0-flash-fin",
		"inclusionAI/Ling-3.0-flash-fin", // 撞 PK 的那个
		"  inclusionai/ling-3.0-flash-fin  ",
		"gpt-4o",
		"gpt-4o",
	}
	if err := db.RecordKeyModelProbe(hash, dupCatalog, time.Now()); err != nil {
		t.Fatalf("RecordKeyModelProbe 被重复模型名打回: %v —— 整轮探测会白跑", err)
	}
	got, err := db.GetKeyModelProbe(hash)
	if err != nil {
		t.Fatalf("GetKeyModelProbe: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("去重后应有 2 个模型, got %v", got)
	}
	if _, ok, err := db.CredentialProbedAt(hash); err != nil || !ok {
		t.Errorf("写成功必须能读回 probed_at, ok=%v err=%v", ok, err)
	}
}

// TestRecordKeyModelProbeEmptyTokenSkipped 空 token 的凭据（动态令牌占位行）
// 没有可探测的上游凭据，必须安静跳过而不是写出一堆空记录。
func TestRecordKeyModelProbeEmptyTokenSkipped(t *testing.T) {
	db := setupTestDB(t)
	db.migrateKeyModelProbe()

	if err := db.RecordKeyModelProbe("", []string{"m1", "m2"}, time.Now()); err != nil {
		t.Fatalf("空 token_hash 应安静跳过, got %v", err)
	}
	all, err := db.GetAllKeyModelProbes()
	if err != nil {
		t.Fatalf("GetAllKeyModelProbes: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("空 token_hash 不该写入任何行, got %v", all)
	}
}

// TestKeyModelProbeRoundTrip 探测记录落库/读回，且 probed_at 能区分"从未探测"
// 与"探测过但目录为空" —— 这两者对偏差报告的含义完全相反。
func TestKeyModelProbeRoundTrip(t *testing.T) {
	db := setupTestDB(t)
	db.migrateKeyModelProbe()

	p := &models.Platform{Name: "probe-plat", BaseURL: "https://api.probe.test/v1", Enabled: true, Available: true}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	k := models.PlatformKey{PlatformID: p.ID, Token: "sk-probe", Enabled: true, Label: "K1"}
	if err := db.AddPlatformKey(&k); err != nil {
		t.Fatalf("AddPlatformKey: %v", err)
	}
	hash := models.TokenHash("sk-probe")

	// 1. 从未探测
	if _, ok, _ := db.CredentialProbedAt(hash); ok {
		t.Error("新凭据不应有 probed_at")
	}
	if got, err := db.GetKeyModelProbe(hash); err != nil || len(got) != 0 {
		t.Errorf("未探测时目录应为空: got=%v err=%v", got, err)
	}

	// 2. 探测到两个模型
	at := time.Now().Add(-90 * time.Second)
	if err := db.RecordKeyModelProbe(hash, []string{"m-a", "m-b"}, at); err != nil {
		t.Fatalf("RecordKeyModelProbe: %v", err)
	}
	got, err := db.GetKeyModelProbe(hash)
	if err != nil {
		t.Fatalf("GetKeyModelProbe: %v", err)
	}
	if len(got) != 2 || got[0] != "m-a" || got[1] != "m-b" {
		t.Errorf("目录 = %v, want [m-a m-b]", got)
	}
	gotAt, ok, err := db.CredentialProbedAt(hash)
	if err != nil || !ok {
		t.Fatalf("探测后应能读回 probed_at, ok=%v err=%v", ok, err)
	}
	if d := gotAt.Sub(at); d > 2*time.Second || d < -2*time.Second {
		t.Errorf("probed_at 偏差 %v，期望贴近 %v", d, at)
	}

	// 3. 二次探测整体替换：上游下架 m-b 后，旧记录不得残留
	if err := db.RecordKeyModelProbe(hash, []string{"m-a", "m-c"}, time.Now()); err != nil {
		t.Fatalf("RecordKeyModelProbe 2nd: %v", err)
	}
	got2, _ := db.GetKeyModelProbe(hash)
	if len(got2) != 2 || got2[0] != "m-a" || got2[1] != "m-c" {
		t.Errorf("二次探测后目录 = %v, want [m-a m-c]（m-b 必须消失）", got2)
	}

	// 4. 空目录 + probed_at —— 与"从未探测"语义不同，必须区分得开
	hash2 := models.TokenHash("sk-probe-2")
	if err := db.RecordKeyModelProbe(hash2, []string{"  ", ""}, time.Now()); err != nil {
		t.Fatalf("RecordKeyModelProbe 空目录: %v", err)
	}
	if _, ok, _ := db.CredentialProbedAt(hash2); !ok {
		t.Error("空目录的探测仍应标记 probed_at（它证明探测发生过）")
	}
	if got, _ := db.GetKeyModelProbe(hash2); len(got) != 0 {
		t.Errorf("空白模型名应被丢弃, got %v", got)
	}
}

// TestKeyModelProbeIsIndependentOfBindings 关键不变式：探测表与生效配置完全
// 解耦。往探测表写任何东西，都不得改变 rapi.key_ids（网关读的是它）。
func TestKeyModelProbeIsIndependentOfBindings(t *testing.T) {
	db := setupTestDB(t)
	db.migrateKeyModelProbe()

	p := &models.Platform{Name: "probe-iso", BaseURL: "https://api.iso.test/v1", Enabled: true, Available: true}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	k := models.PlatformKey{PlatformID: p.ID, Token: "sk-iso", Enabled: true}
	if err := db.AddPlatformKey(&k); err != nil {
		t.Fatalf("AddPlatformKey: %v", err)
	}
	r := &models.RAPI{Alias: "r", Model: "m-x", PlatformID: p.ID, Enabled: true, Available: true}
	if err := db.CreateRAPI(r); err != nil {
		t.Fatalf("CreateRAPI: %v", err)
	}

	before, err := db.GetRAPIByID(r.ID)
	if err != nil {
		t.Fatalf("GetRAPIByID: %v", err)
	}
	beforeIDs := before.KeyIDs

	// 探测到一堆这个 Key 根本无权访问的模型
	if err := db.RecordKeyModelProbe(models.TokenHash("sk-iso"), []string{"m-x", "m-y", "m-z"}, time.Now()); err != nil {
		t.Fatalf("RecordKeyModelProbe: %v", err)
	}

	after, err := db.GetRAPIByID(r.ID)
	if err != nil {
		t.Fatalf("GetRAPIByID: %v", err)
	}
	if after.KeyIDs != beforeIDs {
		t.Errorf("探测改变了生效配置！key_ids: %q -> %q", beforeIDs, after.KeyIDs)
	}
}

// TestMigrateKeyModelProbeKeepsDataWhenSchemaCorrect 关键回归：schema 已正确时
// 迁移必须**什么都不做**。旧实现每次启动都 DROP + CREATE，清空所有探测数据，
// 导致"该平台是否已探测过"永远为假 → 每轮启动都全量重探（实测触发过）。
func TestMigrateKeyModelProbeKeepsDataWhenSchemaCorrect(t *testing.T) {
	db := setupTestDB(t)
	db.migrateKeyModelProbe()

	hash := models.TokenHash("sk-keep")
	if err := db.RecordKeyModelProbe(hash, []string{"m1", "m2"}, time.Now()); err != nil {
		t.Fatalf("RecordKeyModelProbe: %v", err)
	}

	// 再跑一次迁移：schema 已正确，不应清空数据。
	db.migrateKeyModelProbe()

	got, err := db.GetKeyModelProbe(hash)
	if err != nil {
		t.Fatalf("GetKeyModelProbe: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("迁移后数据被清空, got %v, want 2 行", got)
	}
}

// TestMigrateKeyModelProbeRebuildsLegacyIdKeyedTable 旧 schema（id 主键）必须被
// 重建为 token_hash 主键。旧数据在管理端下全部悬空，无可抢救价值。
func TestMigrateKeyModelProbeRebuildsLegacyIdKeyedTable(t *testing.T) {
	db := setupTestDB(t)
	db.migrateKeyModelProbe()

	// 模拟旧 schema：先 DROP 再建 id 主键版本（CREATE IF NOT EXISTS 不会改结构）。
	db.execMigrationDDL("test/drop key_model_probe", `DROP TABLE IF EXISTS key_model_probe`)
	if _, err := db.conn.Exec(`CREATE TABLE key_model_probe (
		key_id INTEGER NOT NULL, model TEXT NOT NULL,
		probed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (key_id, model))`); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := db.conn.Exec(`INSERT INTO key_model_probe (key_id, model) VALUES (999, 'legacy')`); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	// 再跑一次迁移：应重建为 token_hash 主键。
	db.migrateKeyModelProbe()

	var cols string
	if err := db.conn.QueryRow(`SELECT group_concat(name) FROM pragma_table_info('key_model_probe')`).Scan(&cols); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if strings.Contains(cols, "key_id") {
		t.Errorf("旧主键 key_id 未被替换, cols=%s", cols)
	}
	if !strings.Contains(cols, "credential_token_hash") {
		t.Errorf("新主键 credential_token_hash 未出现, cols=%s", cols)
	}

	var legacyRows int
	if err := db.conn.QueryRow(`SELECT COUNT(*) FROM key_model_probe`).Scan(&legacyRows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if legacyRows != 0 {
		t.Errorf("旧表数据应被清空, got %d 行", legacyRows)
	}

	// 幂等：再跑一次不报错、不清数据。
	if err := db.RecordKeyModelProbe(models.TokenHash("sk-new"), []string{"m"}, time.Now()); err != nil {
		t.Fatalf("RecordKeyModelProbe after rebuild: %v", err)
	}
	db.migrateKeyModelProbe()
	got, _ := db.GetKeyModelProbe(models.TokenHash("sk-new"))
	if len(got) != 1 {
		t.Errorf("二次迁移后数据丢失, got %v", got)
	}
}
