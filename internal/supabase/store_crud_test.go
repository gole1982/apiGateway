package supabase

import (
	"errors"
	"strings"
	"testing"

	"gateway/internal/models"
)

// ---------------------------------------------------------------------------
// 中心 store CRUD 回归：写入行的字段形状（v2 契约）+ 读路径组装。
// 复用 store_v2_test.go 的 restRouter（PostgREST 模拟）。
// ---------------------------------------------------------------------------

// AddPlatformKey 必须一次写对三件事：token_hash 由明文算、sort_order 携带
// 轮换序号、token 按 center_key 加密（此处空 key → 明文直写）。
func TestAddPlatformKeyWritesV2Row(t *testing.T) {
	rt := newRouter()
	srv := rt.serve(t)
	s, _ := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)

	k := &models.PlatformKey{PlatformID: 7, KeyIndex: 2, Token: "sk-new", Label: "n", Enabled: true}
	if err := s.AddPlatformKey(k); err != nil {
		t.Fatalf("AddPlatformKey: %v", err)
	}
	if k.ID == 0 {
		t.Error("ID not read back from representation")
	}
	posts := rt.writesTo("credential")
	if len(posts) != 1 {
		t.Fatalf("posted %d rows, want 1", len(posts))
	}
	body := posts[0].Body
	if body["token_hash"] != models.TokenHash("sk-new") {
		t.Errorf("token_hash = %v, want hash of plaintext", body["token_hash"])
	}
	if body["sort_order"] != float64(2) {
		t.Errorf("sort_order = %v, want 2 (KeyIndex)", body["sort_order"])
	}
	if body["platform_id"] != float64(7) {
		t.Errorf("platform_id = %v, want 7", body["platform_id"])
	}
	if body["token"] != "sk-new" {
		t.Errorf("token = %v, want plaintext (empty center_key)", body["token"])
	}
}

// Update / Delete 走 PATCH / DELETE 到 credential 表。
func TestUpdateDeletePlatformKey(t *testing.T) {
	rt := newRouter()
	srv := rt.serve(t)
	s, _ := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)

	k := &models.PlatformKey{ID: 9, PlatformID: 1, KeyIndex: 0, Token: "sk-u", Enabled: false}
	if err := s.UpdatePlatformKey(k); err != nil {
		t.Fatalf("UpdatePlatformKey: %v", err)
	}
	posts := rt.writesTo("credential")
	if len(posts) != 1 || posts[0].Method != "PATCH" {
		t.Fatalf("update = %+v, want one PATCH", posts)
	}
	if !strings.Contains(posts[0].Query, "id=eq.9") {
		t.Errorf("PATCH query = %q, want id=eq.9", posts[0].Query)
	}

	if err := s.DeletePlatformKey(9); err != nil {
		t.Fatalf("DeletePlatformKey: %v", err)
	}
	if q := rt.deleteQuery("credential"); q != "id=eq.9" {
		t.Errorf("delete query = %q, want id=eq.9", q)
	}
}

// GetRAPIByID：平台 + 平台凭据 + 绑定一次拼好；无绑定时 KeyIDs 为空。
func TestGetRAPIByIDAssemblesBindings(t *testing.T) {
	rt := newRouter()
	rt.tables["rapi"] = []map[string]any{
		{"id": 50, "platform_id": 3, "alias": "m", "model": "gpt-x", "enabled": true},
	}
	rt.tables["platform"] = []map[string]any{
		{"id": 3, "name": "P3", "base_url": "https://p3/v1", "enabled": true},
	}
	rt.tables["credential"] = []map[string]any{
		{"id": 21, "platform_id": 3, "token_hash": "h21", "sort_order": 0, "token": "sk-21", "enabled": true},
	}
	rt.tables["endpoint_credential"] = []map[string]any{
		{"rapi_id": 50, "credential_id": 21},
	}
	srv := rt.serve(t)
	s, _ := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)

	wp, err := s.GetRAPIByID(50)
	if err != nil {
		t.Fatalf("GetRAPIByID: %v", err)
	}
	if wp == nil {
		t.Fatal("GetRAPIByID returned nil")
	}
	if wp.PlatformName != "P3" || wp.BaseURL != "https://p3/v1" {
		t.Errorf("platform not filled: %+v", wp)
	}
	if len(wp.Keys) != 1 || wp.Keys[0].Token != "sk-21" {
		t.Errorf("keys = %+v, want the decrypted platform credential", wp.Keys)
	}
	if wp.KeyIDs != "21" {
		t.Errorf("KeyIDs = %q, want 21 (credential id CSV from bindings)", wp.KeyIDs)
	}

	missing, err := s.GetRAPIByID(999)
	if err != nil || missing != nil {
		t.Errorf("GetRAPIByID(999) = %+v,%v; want nil,nil", missing, err)
	}
}

// DumpAll 必须读全 6 张 v2 定义表 —— 少一张，推送前的自动备份就是残的。
func TestDumpAllReadsSixTables(t *testing.T) {
	rt := newRouter()
	srv := rt.serve(t)
	s, _ := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)

	dump, err := s.DumpAll()
	if err != nil {
		t.Fatalf("DumpAll: %v", err)
	}
	for _, tbl := range []string{"platform", "credential", "rapi", "endpoint_credential", "lapi", "lapi_rapi_order"} {
		if _, ok := dump[tbl]; !ok {
			t.Errorf("dump missing table %q", tbl)
		}
	}
	if _, ok := dump["platform_keys"]; ok {
		t.Error("dump still reads platform_keys; the v1 table is gone")
	}
	got := map[string]bool{}
	for _, g := range rt.gotGets() {
		got[g] = true
	}
	for _, tbl := range []string{"platform", "credential", "rapi", "endpoint_credential", "lapi", "lapi_rapi_order"} {
		if !got[tbl] {
			t.Errorf("no GET issued for %q", tbl)
		}
	}
}

// wrapCredErr：只翻译 token_hash 冲突，其他错误原样透传。
func TestWrapCredErrPassthrough(t *testing.T) {
	if err := wrapCredErr(nil); err != nil {
		t.Errorf("wrapCredErr(nil) = %v, want nil", err)
	}
	other := errors.New("supabase GET rapi: http 500: boom")
	if wrapCredErr(other) != other {
		t.Error("non-conflict error must pass through unchanged")
	}
}
