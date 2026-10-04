package service

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// ---------------------------------------------------------------------------
// syncAction：版本号 + 脏标记 + 拉取模式 + 强制标志共同决定同步动作。
// handleSyncRefresh：异步化后守卫顺序不变（方法 → 配置）。
// ---------------------------------------------------------------------------

func TestSyncAction(t *testing.T) {
	cases := []struct {
		name             string
		remote, lastGood int64
		dirty, manual    bool
		force            bool
		want             string
	}{
		{"版本一致直接跳过", 7, 7, false, false, false, "skip"},
		{"版本一致且脏也跳过（横幅常驻提示，不打扰）", 7, 7, true, false, false, "skip"},
		{"manual 非强制只记 pending（含脏也不合并）", 8, 7, true, true, false, "pending"},
		{"manual 强制拉取走合并", 8, 7, true, true, true, "merge"},
		{"版本不同且脏则合并", 8, 7, true, false, false, "merge"},
		{"版本不同且干净则直接拉取", 8, 7, false, false, false, "pull"},
		{"版本不同干净强制拉取", 8, 7, false, true, true, "pull"},
	}
	for _, c := range cases {
		if got := syncAction(c.remote, c.lastGood, c.dirty, c.manual, c.force); got != c.want {
			t.Errorf("%s: syncAction = %q, want %q", c.name, got, c.want)
		}
	}
}

// handleSyncRefresh 未配置同步时 403（异步化后守卫顺序不变：方法→配置）。
func TestSyncRefreshRequiresConfigured(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/api/sync/refresh", nil)
	w := httptest.NewRecorder()
	handleSyncRefresh(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", w.Code)
	}

	r = httptest.NewRequest(http.MethodPost, "/api/sync/refresh", nil)
	w = httptest.NewRecorder()
	handleSyncRefresh(w, r)
	// 单测里同步未配置 → 403。
	if w.Code != http.StatusForbidden {
		t.Errorf("POST status = %d, want 403 (sync not configured in tests)", w.Code)
	}
}
