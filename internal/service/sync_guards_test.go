package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 破坏性端点的服务端守卫：/api/sync/push 用本地定义整体覆盖中心。
// 前端 confirm() 可被绕过直接 POST，因此确认串、角色、DB 就绪按顺序硬约束。
// 全部只需 httptest + 包级开关，无需 DB（db.Get() 在单测里为 nil）。
// ---------------------------------------------------------------------------

func doPush(t *testing.T, manage bool, method, body string) *httptest.ResponseRecorder {
	t.Helper()
	was := manageMode.Load()
	manageMode.Store(manage)
	t.Cleanup(func() { manageMode.Store(was) })

	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, "/api/sync/push", nil)
	} else {
		r = httptest.NewRequest(method, "/api/sync/push", strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	handleSyncPush(w, r)
	return w
}

func pushErr(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var d map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
		t.Fatalf("response not JSON: %q", w.Body.String())
	}
	return d["error"]
}

// 非 POST 直接 405，不触及任何守卫。
func TestSyncPushRejectsNonPost(t *testing.T) {
	w := doPush(t, true, http.MethodGet, "")
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", w.Code)
	}
}

// 非管理模式 403 —— 代理节点即使被直接 POST 也不执行覆盖。
func TestSyncPushRequiresManageMode(t *testing.T) {
	w := doPush(t, false, http.MethodPost, `{"confirm":"OVERWRITE-CENTER"}`)
	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
}

// 坏 JSON / 缺确认串 → 400，且错误信息必须点名确认串格式。
func TestSyncPushRequiresConfirmToken(t *testing.T) {
	w := doPush(t, true, http.MethodPost, `{oops`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad JSON status = %d, want 400", w.Code)
	}

	w = doPush(t, true, http.MethodPost, `{"confirm":"yes"}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("wrong confirm status = %d, want 400", w.Code)
	}
	if msg := pushErr(t, w); !strings.Contains(msg, "OVERWRITE-CENTER") {
		t.Errorf("error %q must name the required token", msg)
	}

	w = doPush(t, true, http.MethodPost, `{}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("missing confirm status = %d, want 400", w.Code)
	}
}

// 确认串正确但 DB 未就绪 → 500（守卫顺序：方法→角色→确认→DB）。
// 这条同时锁死"确认检查在 DB 之前"：若顺序反了，无 DB 时会先报 db not ready。
func TestSyncPushDbNotReadyAfterConfirm(t *testing.T) {
	w := doPush(t, true, http.MethodPost, `{"confirm":"OVERWRITE-CENTER"}`)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 (db nil in unit tests)", w.Code)
	}
	if msg := pushErr(t, w); !strings.Contains(msg, "db not ready") {
		t.Errorf("error = %q, want db-not-ready (confirm passed, stopped at DB)", msg)
	}
}

// writeJSONError：固定 {"error": msg} 形状 + 状态码，前端按此解析。
func TestWriteJSONErrorShape(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSONError(w, 418, errTestMsg("teapot"))
	if w.Code != 418 {
		t.Errorf("status = %d, want 418", w.Code)
	}
	var d map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
		t.Fatalf("not JSON: %q", w.Body.String())
	}
	if d["error"] != "teapot" {
		t.Errorf("error = %q, want teapot", d["error"])
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q", ct)
	}
}

type errTestMsg string

func (e errTestMsg) Error() string { return string(e) }
