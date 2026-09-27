package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 中心连接纯函数 + 角色探测（httptest 模拟 PostgREST）。
// 角色判定错了会把管理端当代理（写被吞）或把代理当管理（越权写中心），
// 必须锁死。probeSBRole 的写探测特意用"不存在的 id"做到无副作用。
// ---------------------------------------------------------------------------

func TestNormalizeSBURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://abc.supabase.co/", "https://abc.supabase.co"},
		{"https://abc.supabase.co", "https://abc.supabase.co"},
		{"abc.supabase.co", "https://abc.supabase.co"},
		{"lmuqwcsfsyarvwaybnao", "https://lmuqwcsfsyarvwaybnao.supabase.co"},
		{"", ""},
		{"   ", ""},
	}
	for _, c := range cases {
		if got := normalizeSBURL(c.in); got != c.want {
			t.Errorf("normalizeSBURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestMaskKey(t *testing.T) {
	if got := maskKey("sb_secret_abcdef123456"); !strings.HasPrefix(got, "sb_s") || !strings.HasSuffix(got, "3456") {
		t.Errorf("maskKey = %q, want prefix+...+suffix", got)
	}
	// 掩码保持 rune 数（• 是 3 字节，byte 长度必然变长；UI 按字符排版）。
	if got, want := len([]rune(maskKey("sb_secret_abcdef123456"))), len("sb_secret_abcdef123456"); got != want {
		t.Errorf("maskKey runes = %d, want %d", got, want)
	}
	if got := maskKey("short"); got != strings.Repeat("•", 5) {
		t.Errorf("short key = %q, want fully masked", got)
	}
	if got := maskKey(""); got != "" {
		t.Errorf("empty = %q, want empty", got)
	}
}

func TestParseCenterVersion(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"249", 249},
		{"  249\n", 249},
		{"249.0", 249},
		{"", 0},
		{"null", 0},
		{"oops", 0},
	}
	for _, c := range cases {
		if got := parseCenterVersion([]byte(c.in)); got != c.want {
			t.Errorf("parseCenterVersion(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// roleProbeServer 模拟 PostgREST：get_version 返回版本号；platform 的
// 空 PATCH 按配置码返回（204=可写，401/403=只读，404=表不存在也算只读）。
func roleProbeServer(t *testing.T, patchCode int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/rpc/get_version"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("249"))
		case strings.HasSuffix(r.URL.Path, "/platform") && r.Method == http.MethodPatch:
			w.WriteHeader(patchCode)
		default:
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestProbeSBRole(t *testing.T) {
	srv := roleProbeServer(t, http.StatusNoContent)
	role, ver, err := probeSBRole(srv.URL, "k")
	if err != nil || role != sbRoleManagement || ver != 249 {
		t.Errorf("writable = (%q,%d,%v), want (management,249,nil)", role, ver, err)
	}

	srv = roleProbeServer(t, http.StatusUnauthorized)
	role, ver, err = probeSBRole(srv.URL, "k")
	if err != nil || role != sbRoleProxy || ver != 249 {
		t.Errorf("read-only = (%q,%d,%v), want (proxy,249,nil)", role, ver, err)
	}

	srv = roleProbeServer(t, http.StatusNotFound)
	role, _, err = probeSBRole(srv.URL, "k")
	if err != nil || role != sbRoleProxy {
		t.Errorf("missing table = (%q,%v), want (proxy,nil)", role, err)
	}

	// 连不通 → offline + 错误（调用方据此显示"未连接"，而不是静默当代理）。
	role, ver, err = probeSBRole("http://127.0.0.1:1", "k")
	if role != sbRoleOffline || ver != 0 || err == nil {
		t.Errorf("unreachable = (%q,%d,%v), want (offline,0,err)", role, ver, err)
	}
}

// handleSyncPullMode POST 非法 mode → 400（在碰 DB 之前，前端下拉框写错值时
// 有明确反馈；db.Get() 在单测里为 nil，此路径不碰 DB）。
func TestSyncPullModeRejectsBadMode(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/api/sync/pull-mode", strings.NewReader(`{"mode":"sometimes"}`))
	w := httptest.NewRecorder()
	handleSyncPullMode(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}

	r = httptest.NewRequest(http.MethodPost, "/api/sync/pull-mode", strings.NewReader(`{bad json`))
	w = httptest.NewRecorder()
	handleSyncPullMode(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad JSON status = %d, want 400", w.Code)
	}

	r = httptest.NewRequest(http.MethodDelete, "/api/sync/pull-mode", nil)
	w = httptest.NewRecorder()
	handleSyncPullMode(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("DELETE status = %d, want 405", w.Code)
	}
}
