package db

import (
	"testing"
	"time"

	"gateway/internal/models"
)

// TestKeyModelProbeRoundTrip 探测记录落库/读回，且 probed_at 能区分"从未探测"
// 与"探测过但目录为空" —— 这两者对偏差报告的含义完全相反。
func TestKeyModelProbeRoundTrip(t *testing.T) {
	db := setupTestDB(t)
	// setupTestDB 只跑到自然键终态；探测表由本测试单独跑迁移（与 db_test.go
	// 里 migrateRAPIUniqueAliasToPerPlatform 的用法一致），顺带回归迁移路径。
	db.migrateKeyModelProbe()

	p := &models.Platform{Name: "probe-plat", BaseURL: "https://api.probe.test/v1", Enabled: true, Available: true}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	k := models.PlatformKey{PlatformID: p.ID, Token: "sk-probe", Enabled: true, Label: "K1"}
	if err := db.AddPlatformKey(&k); err != nil {
		t.Fatalf("AddPlatformKey: %v", err)
	}

	// 1. 从未探测
	if _, ok := db.KeyProbedAt(k.ID); ok {
		t.Error("新 Key 不应有 probed_at")
	}
	if got, err := db.GetKeyModelProbe(k.ID); err != nil || len(got) != 0 {
		t.Errorf("未探测时目录应为空: got=%v err=%v", got, err)
	}

	// 2. 探测到两个模型
	at := time.Now().Add(-90 * time.Second)
	if err := db.RecordKeyModelProbe(k.ID, []string{"m-a", "m-b"}, at); err != nil {
		t.Fatalf("RecordKeyModelProbe: %v", err)
	}
	got, err := db.GetKeyModelProbe(k.ID)
	if err != nil {
		t.Fatalf("GetKeyModelProbe: %v", err)
	}
	if len(got) != 2 || got[0] != "m-a" || got[1] != "m-b" {
		t.Errorf("目录 = %v, want [m-a m-b]", got)
	}
	gotAt, ok := db.KeyProbedAt(k.ID)
	if !ok {
		t.Fatal("探测后应有 probed_at")
	}
	if d := gotAt.Sub(at); d > 2*time.Second || d < -2*time.Second {
		t.Errorf("probed_at 偏差 %v，期望贴近 %v", d, at)
	}

	// 3. 二次探测整体替换：上游下架 m-b 后，旧记录不得残留
	if err := db.RecordKeyModelProbe(k.ID, []string{"m-a", "m-c"}, time.Now()); err != nil {
		t.Fatalf("RecordKeyModelProbe 2nd: %v", err)
	}
	got2, _ := db.GetKeyModelProbe(k.ID)
	if len(got2) != 2 || got2[0] != "m-a" || got2[1] != "m-c" {
		t.Errorf("二次探测后目录 = %v, want [m-a m-c]（m-b 必须消失）", got2)
	}

	// 4. 空目录 + probed_at —— 与"从未探测"语义不同，必须区分得开
	k2 := models.PlatformKey{PlatformID: p.ID, Token: "sk-probe-2", Enabled: true}
	if err := db.AddPlatformKey(&k2); err != nil {
		t.Fatalf("AddPlatformKey k2: %v", err)
	}
	if err := db.RecordKeyModelProbe(k2.ID, []string{"  ", ""}, time.Now()); err != nil {
		t.Fatalf("RecordKeyModelProbe 空目录: %v", err)
	}
	if _, ok := db.KeyProbedAt(k2.ID); !ok {
		t.Error("空目录的探测仍应标记 probed_at（它证明探测发生过）")
	}
	if got, _ := db.GetKeyModelProbe(k2.ID); len(got) != 0 {
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
	if err := db.RecordKeyModelProbe(k.ID, []string{"m-x", "m-y", "m-z"}, time.Now()); err != nil {
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
