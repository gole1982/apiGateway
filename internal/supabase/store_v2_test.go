package supabase

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"gateway/internal/models"
)

// ---------------------------------------------------------------------------
// v2 中心契约回归：credential（token_hash 身份）+ endpoint_credential（绑定表）
// 取代 v1 的 platform_keys(key_index) + rapi.key_ids CSV。
// 下面每条都对应一次真实的 v1→v2 迁移事故面。
// ---------------------------------------------------------------------------

// restRouter 按 PostgREST 路径末段（表名）分发，内置并发安全。
// rec 收集所有写请求的 body，供断言"中心收到了哪些行"。
type restRouter struct {
	mu      sync.Mutex
	tables  map[string][]map[string]any // GET 返回
	writes  []recordedWrite             // POST/PATCH 记录
	deleted map[string]string           // 表 → DELETE 的 query
	seq     map[string]int64            // 表 → 自增 id 分配游标
}

type recordedWrite struct {
	Method string
	Table  string
	Query  string
	Body   map[string]any
}

func newRouter() *restRouter {
	return &restRouter{
		tables:  map[string][]map[string]any{},
		deleted: map[string]string{},
		seq:     map[string]int64{},
	}
}

func (rt *restRouter) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// /rest/v1/<table>
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		table := parts[len(parts)-1]

		var body map[string]any
		if raw, _ := io.ReadAll(r.Body); len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
		}

		rt.mu.Lock()
		switch r.Method {
		case http.MethodGet:
			rows := rt.tables[table]
			rt.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if rows == nil {
				_ = json.NewEncoder(w).Encode([]map[string]any{})
				return
			}
			_ = json.NewEncoder(w).Encode(rows)
			return
		case http.MethodDelete:
			rt.deleted[table] = r.URL.RawQuery
			rt.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		default:
			rt.writes = append(rt.writes, recordedWrite{
				Method: r.Method, Table: table, Query: r.URL.RawQuery, Body: body,
			})
			rt.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if body == nil {
				// DELETE 风格：空 body
				_ = json.NewEncoder(w).Encode([]map[string]any{})
				return
			}
			// 模拟服务端自增 + 回读：把请求体加上 id 后原样返回。
			out := map[string]any{}
			for k, v := range body {
				out[k] = v
			}
			rt.mu.Lock()
			out["id"] = rt.nextID(table)
			rt.mu.Unlock()
			_ = json.NewEncoder(w).Encode([]map[string]any{out})
			return
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (rt *restRouter) nextID(table string) int64 {
	rt.seq[table]++
	return rt.seq[table]
}

// writesTo 返回发往某表的自增 id 分配序列（按出现顺序）。
func (rt *restRouter) writesTo(table string) []recordedWrite {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var out []recordedWrite
	for _, w := range rt.writes {
		if w.Table == table {
			out = append(out, w)
		}
	}
	return out
}

func (rt *restRouter) deleteQuery(table string) string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.deleted[table]
}

// keyRow 必须用明文算 token_hash，且把 v1 的 key_index 换成 sort_order。
// 空 token → NULL（不是空串）：唯一索引是 ... WHERE token_hash IS NOT NULL，
// 多条动态令牌占位凭据写空串会互相撞约束。
func TestKeyRowV2NaturalKey(t *testing.T) {
	row := keyRow(&models.PlatformKey{
		PlatformID: 7, KeyIndex: 3, Token: "sk-live-abc", Label: "prod", Enabled: true,
	})
	if got, want := row["token_hash"], models.TokenHash("sk-live-abc"); got != want {
		t.Errorf("token_hash = %v, want %v (sha256(plaintext)[:16])", got, want)
	}
	if got, want := row["sort_order"], 3; got != want {
		t.Errorf("sort_order = %v, want %v (v2 轮换序号)", got, want)
	}
	if _, ok := row["key_index"]; ok {
		t.Error("row still carries key_index; v2 credential has no such column")
	}
	if row["token"] != "sk-live-abc" {
		t.Errorf("token = %v, want plaintext (caller overwrites with ciphertext)", row["token"])
	}

	empty := keyRow(&models.PlatformKey{PlatformID: 1, Token: ""})
	if empty["token_hash"] != nil {
		t.Errorf("empty token: token_hash = %#v, want nil (SQL NULL)", empty["token_hash"])
	}
}

// 读出时 sort_order → KeyIndex，且 token 解密回明文，上层对中心/本地无感知。
func TestCredRowToPlatformKeyMapsSortOrder(t *testing.T) {
	plain := func(s string) string { return s } // 空 center_key → 解密即恒等
	r := credRow{
		ID: 5, PlatformID: 2, TokenHash: models.TokenHash("sk-x"),
		SortOrder: 4, Token: "sk-x", Label: "L", Enabled: true, IsFree: true,
	}
	k := r.toPlatformKey(plain)
	if k.ID != 5 || k.PlatformID != 2 {
		t.Errorf("id/platform = %d/%d, want 5/2", k.ID, k.PlatformID)
	}
	if k.KeyIndex != 4 {
		t.Errorf("KeyIndex = %d, want 4 (sort_order)", k.KeyIndex)
	}
	if k.Token != "sk-x" || k.Label != "L" || !k.Enabled || !k.IsFree {
		t.Errorf("field mapping wrong: %+v", k)
	}
}

func TestGetAllPlatformKeysReadsCredentialBySortOrder(t *testing.T) {
	rt := newRouter()
	rt.tables["credential"] = []map[string]any{
		{"id": 9, "platform_id": 1, "token_hash": "h9", "sort_order": 0,
			"token": "sk-a", "label": "a", "enabled": true},
		{"id": 8, "platform_id": 1, "token_hash": "h8", "sort_order": 1,
			"token": "sk-b", "label": "b", "enabled": false},
	}
	srv := rt.serve(t)
	s, err := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	keys, err := s.GetAllPlatformKeys()
	if err != nil {
		t.Fatalf("GetAllPlatformKeys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("got %d keys, want 2", len(keys))
	}
	if keys[0].KeyIndex != 0 || keys[1].KeyIndex != 1 {
		t.Errorf("KeyIndex = %d/%d, want 0/1 (from sort_order)", keys[0].KeyIndex, keys[1].KeyIndex)
	}
	if keys[0].Token != "sk-a" || keys[1].Token != "sk-b" {
		t.Errorf("token not decrypted to plaintext: %q/%q", keys[0].Token, keys[1].Token)
	}
}

// v2 的 rapi 表没有 key_ids 列 —— 带了就是 PostgREST 未知列错误。
func TestRapiRowHasNoKeyIDsColumn(t *testing.T) {
	row := rapiRow(&models.RAPI{PlatformID: 1, Alias: "m", Model: "gpt", KeyIDs: "4,5"})
	if _, ok := row["key_ids"]; ok {
		t.Error("rapiRow still emits key_ids; v2 rapi table has no such column")
	}
	if row["model"] != "gpt" || row["alias"] != "m" {
		t.Errorf("core fields lost: %+v", row)
	}
}

// listRAPIs 的 KeyIDs 必须来自 endpoint_credential，而不是 rapi.key_ids。
// 端点无绑定行 → KeyIDs 为空（= 用平台全部凭据，与本地语义同义）。
func TestListRAPIsKeyIDsComeFromBindingTable(t *testing.T) {
	rt := newRouter()
	rt.tables["rapi"] = []map[string]any{
		{"id": 100, "platform_id": 1, "alias": "bound", "model": "m1", "enabled": true},
		{"id": 200, "platform_id": 1, "alias": "unbound", "model": "m2", "enabled": true},
	}
	rt.tables["platform"] = []map[string]any{
		{"id": 1, "name": "P", "base_url": "https://x/v1", "enabled": true},
	}
	rt.tables["credential"] = []map[string]any{
		{"id": 11, "platform_id": 1, "token_hash": "h1", "sort_order": 0, "token": "sk-1", "enabled": true},
		{"id": 12, "platform_id": 1, "token_hash": "h2", "sort_order": 1, "token": "sk-2", "enabled": true},
	}
	rt.tables["endpoint_credential"] = []map[string]any{
		{"rapi_id": 100, "credential_id": 12},
		{"rapi_id": 100, "credential_id": 11},
	}
	srv := rt.serve(t)
	s, err := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rapis, err := s.GetRAPIs()
	if err != nil {
		t.Fatalf("GetRAPIs: %v", err)
	}
	if len(rapis) != 2 {
		t.Fatalf("got %d rapis, want 2", len(rapis))
	}
	byID := map[int64]models.RAPIWithPlatform{}
	for _, r := range rapis {
		byID[r.ID] = r
	}
	if got := byID[100].KeyIDs; got != "12,11" {
		t.Errorf("rapi 100 KeyIDs = %q, want %q (credential id CSV from bindings)", got, "12,11")
	}
	if got := byID[200].KeyIDs; got != "" {
		t.Errorf("rapi 200 KeyIDs = %q, want \"\" (no binding = all platform creds)", got)
	}
	if len(byID[100].Keys) != 2 {
		t.Errorf("rapi 100 Keys = %d, want 2", len(byID[100].Keys))
	}
}

// DetachKeyFromRAPIs：v2 只需删绑定行，并回报受影响端点的 alias。
func TestDetachKeyFromRAPIsDeletesBindingRows(t *testing.T) {
	rt := newRouter()
	rt.tables["endpoint_credential"] = []map[string]any{
		{"rapi_id": 100, "credential_id": 12},
		{"rapi_id": 200, "credential_id": 12},
	}
	rt.tables["rapi"] = []map[string]any{
		{"id": 100, "alias": "alpha", "model": "m1"},
		{"id": 200, "alias": "beta", "model": "m2"},
		{"id": 300, "alias": "gamma", "model": "m3"},
	}
	srv := rt.serve(t)
	s, err := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := s.DetachKeyFromRAPIs(12)
	if err != nil {
		t.Fatalf("DetachKeyFromRAPIs: %v", err)
	}
	if q := rt.deleteQuery("endpoint_credential"); q != "credential_id=eq.12" {
		t.Errorf("deleted binding rows with %q, want credential_id=eq.12", q)
	}
	if len(got) != 2 || got[0] != "alpha" || got[1] != "beta" {
		t.Errorf("affected aliases = %v, want [alpha beta]", got)
	}
}

// 凭据无绑定 → (nil, nil)，不该发任何删除。
func TestDetachKeyFromRAPIsNoBindingIsNoop(t *testing.T) {
	rt := newRouter()
	rt.tables["endpoint_credential"] = []map[string]any{}
	srv := rt.serve(t)
	s, _ := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)

	got, err := s.DetachKeyFromRAPIs(12)
	if err != nil {
		t.Fatalf("DetachKeyFromRAPIs: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("affected = %v, want empty", got)
	}
	if q := rt.deleteQuery("endpoint_credential"); q != "" {
		t.Errorf("issued a delete (%q) despite no bindings", q)
	}
}

// v2 起凭据全局唯一，重复 token 会撞 idx_credential_token_hash → 409。
// 必须翻成中文提示，否则 dashboard 只显示裸 PostgREST 错误。
func TestCredConflictErrorTranslated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"23505","details":null,` +
			`"hint":null,"message":"duplicate key value violates unique constraint \"idx_credential_token_hash\""}`))
	}))
	defer srv.Close()
	s, _ := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)

	err := s.AddPlatformKey(&models.PlatformKey{PlatformID: 1, Token: "sk-dup"})
	if err == nil {
		t.Fatal("expected error on 409")
	}
	if !strings.Contains(err.Error(), "该 token 已登记过") {
		t.Errorf("error not translated: %v", err)
	}
}

func TestSplitCSV(t *testing.T) {
	cases := []struct {
		in   string
		want []int64
	}{
		{"", nil},
		{"   ", nil},
		{"5", []int64{5}},
		{"1,2,3", []int64{1, 2, 3}},
		{" 1 , 2 ,, 3 ", []int64{1, 2, 3}},
		{"1,abc,3", []int64{1, 3}}, // 坏值跳过，不整批失败
		{"1,,3", []int64{1, 3}},
	}
	for _, c := range cases {
		got := splitCSV(c.in)
		if len(got) != len(c.want) {
			t.Errorf("splitCSV(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("splitCSV(%q) = %v, want %v", c.in, got, c.want)
				break
			}
		}
	}
}

// replaceBindings：先删该端点全部绑定，再逐条插新绑定。
func TestReplaceBindings(t *testing.T) {
	rt := newRouter()
	srv := rt.serve(t)
	s, _ := New(Config{URL: srv.URL, ServiceKey: "k"}, nil)

	if err := s.replaceBindings(100, "3, 4"); err != nil {
		t.Fatalf("replaceBindings: %v", err)
	}
	if q := rt.deleteQuery("endpoint_credential"); q != "rapi_id=eq.100" {
		t.Errorf("delete query = %q, want rapi_id=eq.100", q)
	}
	posts := rt.writesTo("endpoint_credential")
	if len(posts) != 2 {
		t.Fatalf("posted %d binding rows, want 2", len(posts))
	}
	for i, want := range []float64{3, 4} {
		if got := posts[i].Body["credential_id"]; got != want {
			t.Errorf("binding[%d].credential_id = %v, want %v", i, got, want)
		}
		if got := posts[i].Body["rapi_id"]; got != float64(100) {
			t.Errorf("binding[%d].rapi_id = %v, want 100", i, got)
		}
	}

	// 空 KeyIDs = 不绑定：只删不插。
	rt2 := newRouter()
	srv2 := rt2.serve(t)
	s2, _ := New(Config{URL: srv2.URL, ServiceKey: "k"}, nil)
	if err := s2.replaceBindings(100, ""); err != nil {
		t.Fatalf("replaceBindings(empty): %v", err)
	}
	if n := len(rt2.writesTo("endpoint_credential")); n != 0 {
		t.Errorf("posted %d rows for empty KeyIDs, want 0", n)
	}
}
