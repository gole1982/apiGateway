package db

import (
	"testing"

	"gateway/internal/models"
)

// TestLocalRAPIIDByNaturalKey 锁定管理端的关键约束：
// 中心侧 rapi.id 与本地 SQLite 自增 id 是两套空间，健康态必须按
// (platform.base_url, model) 自然键回查本地 id，否则会改错行。
func TestLocalRAPIIDByNaturalKey(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{Name: "p1", BaseURL: "https://api.example.com/v1/", Enabled: true, Available: true}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	idA := insertTestRAPI(t, db, p.ID, "model-a", true, true)
	idB := insertTestRAPI(t, db, p.ID, "model-b", true, true)

	got, ok, err := db.LocalRAPIIDByNaturalKey("https://api.example.com/v1", "model-a")
	if err != nil || !ok || got != idA {
		t.Fatalf("model-a: got (%d,%v,%v), want (%d,true,nil)", got, ok, err, idA)
	}

	// base_url 尾斜杠必须归一化后仍能命中。
	got, ok, err = db.LocalRAPIIDByNaturalKey("https://api.example.com/v1/", "model-b")
	if err != nil || !ok || got != idB {
		t.Fatalf("model-b: got (%d,%v,%v), want (%d,true,nil)", got, ok, err, idB)
	}

	if _, ok, err := db.LocalRAPIIDByNaturalKey("https://other.example.com", "model-a"); err != nil || ok {
		t.Fatalf("其他平台不应命中: ok=%v err=%v", ok, err)
	}
	if _, ok, err := db.LocalRAPIIDByNaturalKey(p.BaseURL, "missing"); err != nil || ok {
		t.Fatalf("不存在的模型不应命中: ok=%v err=%v", ok, err)
	}
}

// TestLocalPlatformHasRAPIs 目录同步前的就绪检查：本地镜像没有端点时必须
// 返回 false，避免探测记录写下了却没有 rapi 可应用判定。
func TestLocalPlatformHasRAPIs(t *testing.T) {
	db := setupTestDB(t)

	withRAPI := &models.Platform{Name: "with-rapi", BaseURL: "https://api.with.test/", Enabled: true, Available: true}
	empty := &models.Platform{Name: "empty", BaseURL: "https://api.empty.test", Enabled: true, Available: true}
	if err := db.CreatePlatform(withRAPI); err != nil {
		t.Fatalf("CreatePlatform withRAPI: %v", err)
	}
	if err := db.CreatePlatform(empty); err != nil {
		t.Fatalf("CreatePlatform empty: %v", err)
	}
	insertTestRAPI(t, db, withRAPI.ID, "model-a", true, true)

	ready, err := db.LocalPlatformHasRAPIs("https://api.with.test")
	if err != nil || !ready {
		t.Fatalf("有端点应为就绪: ready=%v err=%v", ready, err)
	}
	ready, err = db.LocalPlatformHasRAPIs("https://api.empty.test")
	if err != nil || ready {
		t.Fatalf("无端点不应就绪: ready=%v err=%v", ready, err)
	}
	ready, err = db.LocalPlatformHasRAPIs("https://unknown.test")
	if err != nil || ready {
		t.Fatalf("未知平台不应就绪: ready=%v err=%v", ready, err)
	}
}

// TestLocalUnavailableRAPIModels 恢复轮次的目标来源：
// 只返回启用且不可用的模型（小写），禁用或健康的模型不能触发重探。
func TestLocalUnavailableRAPIModels(t *testing.T) {
	db := setupTestDB(t)

	p1 := &models.Platform{Name: "p1", BaseURL: "https://api.one.test", Enabled: true, Available: true}
	p2 := &models.Platform{Name: "p2", BaseURL: "https://api.two.test", Enabled: true, Available: true}
	if err := db.CreatePlatform(p1); err != nil {
		t.Fatalf("CreatePlatform p1: %v", err)
	}
	if err := db.CreatePlatform(p2); err != nil {
		t.Fatalf("CreatePlatform p2: %v", err)
	}

	insertTestRAPI(t, db, p1.ID, "Model-Healthy", true, true)
	insertTestRAPI(t, db, p1.ID, "Model-Down", true, false)
	insertTestRAPI(t, db, p1.ID, "model-disabled", false, false)
	insertTestRAPI(t, db, p2.ID, "model-other-platform", true, false)
	// 自定义模型没有"失效"态，目录同步永远恢复不了它；若把它算进"待恢复"，
	// 该平台会每 30 分钟无意义地重探一次上游。
	insertTestRAPISource(t, db, p1.ID, "manual-down", true, false, "manual")

	got, err := db.LocalUnavailableRAPIModels("https://api.one.test/")
	if err != nil {
		t.Fatalf("LocalUnavailableRAPIModels: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 unavailable model, got %v", got)
	}
	if _, ok := got["model-down"]; !ok {
		t.Errorf("want lowercase model-down, got %v", got)
	}
	if _, ok := got["model-other-platform"]; ok {
		t.Error("其他平台的不可用模型不应混入")
	}
	if _, ok := got["manual-down"]; ok {
		t.Error("自定义模型没有失效态，不应进入待恢复集合")
	}

	empty, err := db.LocalUnavailableRAPIModels("https://unknown.test")
	if err != nil || len(empty) != 0 {
		t.Fatalf("未知平台应返回空集合: %v err=%v", empty, err)
	}
}

// TestMigrateManualRAPIAvailable 自定义模型没有失效态：历史遗留的 available=0
// 必须复位，且绝不能碰 auto_discover 行（那些是真失效，要等目录同步恢复）。
func TestMigrateManualRAPIAvailable(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{Name: "p1", BaseURL: "https://api.repair.test", Enabled: true, Available: true}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	manualID := insertTestRAPISource(t, db, p.ID, "manual-dead", true, false, "manual")
	autoID := insertTestRAPISource(t, db, p.ID, "auto-dead", true, false, "auto_discover")

	db.migrateManualRAPIAvailable()

	if got, err := db.LocalRAPIAvailable(manualID); err != nil || !got {
		t.Errorf("manual 行应被复位为 available=1: got=%v err=%v", got, err)
	}
	if got, err := db.LocalRAPIAvailable(autoID); err != nil || got {
		t.Errorf("auto_discover 行不应被改动: got=%v err=%v", got, err)
	}

	// 幂等：再跑一次不应报错，也不改变结果。
	db.migrateManualRAPIAvailable()
	if got, err := db.LocalRAPIAvailable(autoID); err != nil || got {
		t.Errorf("二次执行不应影响 auto 行: got=%v err=%v", got, err)
	}
}

// TestLocalRAPIAvailable 锁定"状态翻转才通知"的前提：读到的必须是写入前的旧值。
func TestLocalRAPIAvailable(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{Name: "p1", BaseURL: "https://api.state.test", Enabled: true, Available: true}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	id := insertTestRAPI(t, db, p.ID, "model-a", true, true)

	got, err := db.LocalRAPIAvailable(id)
	if err != nil || !got {
		t.Fatalf("初始应为 available=true: got=%v err=%v", got, err)
	}

	if err := db.SetRAPIUnavailableWithReason(id, false, "目录中不存在"); err != nil {
		t.Fatalf("SetRAPIUnavailableWithReason: %v", err)
	}
	got, err = db.LocalRAPIAvailable(id)
	if err != nil || got {
		t.Fatalf("写入后应为 available=false: got=%v err=%v", got, err)
	}

	if _, err := db.LocalRAPIAvailable(id + 9999); err == nil {
		t.Error("不存在的 rapi 应返回错误，否则翻转判定会把缺失行当成 true→false")
	}
}

func insertTestRAPI(t *testing.T, db *DB, platformID int64, model string, enabled, available bool) int64 {
	t.Helper()
	return insertTestRAPISource(t, db, platformID, model, enabled, available, "auto_discover")
}

// insertTestRAPISource 显式指定 source：目录同步只判 auto_discover，
// manual 行必须能被单独造出来验证"不参与判定"。
func insertTestRAPISource(t *testing.T, db *DB, platformID int64, model string, enabled, available bool, source string) int64 {
	t.Helper()
	res, err := db.conn.Exec(`
		INSERT INTO rapi (alias, model, platform_id, enabled, available, source)
		VALUES (?, ?, ?, ?, ?, ?)`,
		model, model, platformID, b2iTest(enabled), b2iTest(available), source)
	if err != nil {
		t.Fatalf("insert rapi %q: %v", model, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	return id
}

func b2iTest(v bool) int {
	if v {
		return 1
	}
	return 0
}
