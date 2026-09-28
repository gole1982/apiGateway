package service

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// okHandler 只是记录自己被调用过，用来断言"请求到底有没有穿过守卫"。
func okHandler(hit *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*hit = true
		w.WriteHeader(http.StatusOK)
	})
}

// TestOriginGuardBlocksCrossOriginWrite 是本守卫存在的理由：面板 API 无认证，
// 且 handler 不校验 Content-Type，浏览器可用 text/plain 简单请求跨站写入。
func TestOriginGuardBlocksCrossOriginWrite(t *testing.T) {
	cases := []struct {
		name    string
		method  string
		origin  string
		referer string
		host    string
		wantHit bool
		want    int
	}{
		{
			name: "same-origin write passes", method: http.MethodPost,
			origin: "http://127.0.0.1:24680", host: "127.0.0.1:24680",
			wantHit: true, want: http.StatusOK,
		},
		{
			name: "cross-origin write blocked", method: http.MethodPost,
			origin: "http://evil.example", host: "127.0.0.1:24680",
			wantHit: false, want: http.StatusForbidden,
		},
		{
			name: "cross-origin PUT blocked", method: http.MethodPut,
			origin: "http://evil.example", host: "127.0.0.1:24680",
			wantHit: false, want: http.StatusForbidden,
		},
		{
			name: "cross-origin DELETE blocked", method: http.MethodDelete,
			origin: "http://evil.example", host: "127.0.0.1:24680",
			wantHit: false, want: http.StatusForbidden,
		},
		{
			// 端口不同也算跨源：同 host 不同端口是不同源。
			name: "same host different port blocked", method: http.MethodPost,
			origin: "http://127.0.0.1:9999", host: "127.0.0.1:24680",
			wantHit: false, want: http.StatusForbidden,
		},
		{
			// https 页面发到 http 面板：scheme 不同即跨源。
			name: "scheme mismatch blocked", method: http.MethodPost,
			origin: "https://127.0.0.1:24680", host: "127.0.0.1:24680",
			wantHit: false, want: http.StatusForbidden,
		},
		{
			// Host 大小写不敏感，不该因此误拦。
			name: "host case insensitive", method: http.MethodPost,
			origin: "http://LOCALHOST:24680", host: "localhost:24680",
			wantHit: true, want: http.StatusOK,
		},
		{
			name: "cross-origin GET allowed (read-only)", method: http.MethodGet,
			origin: "http://evil.example", host: "127.0.0.1:24680",
			wantHit: true, want: http.StatusOK,
		},
		{
			// 无 Origin/Referer = 非浏览器客户端（curl / 脚本），不受 CSRF 影响。
			name: "no origin header passes", method: http.MethodPost,
			host:    "127.0.0.1:24680",
			wantHit: true, want: http.StatusOK,
		},
		{
			// 只有 Referer（老浏览器）也按同源判。
			name: "referer same-origin passes", method: http.MethodPost,
			referer: "http://127.0.0.1:24680/settings", host: "127.0.0.1:24680",
			wantHit: true, want: http.StatusOK,
		},
		{
			name: "referer cross-origin blocked", method: http.MethodPost,
			referer: "http://evil.example/x", host: "127.0.0.1:24680",
			wantHit: false, want: http.StatusForbidden,
		},
		{
			// Origin 优先于 Referer：两者冲突时以 Origin 判。
			name: "origin takes precedence over referer", method: http.MethodPost,
			origin: "http://evil.example", referer: "http://127.0.0.1:24680/",
			host:    "127.0.0.1:24680",
			wantHit: false, want: http.StatusForbidden,
		},
		{
			// 畸形 Origin 按不同源处理，不能靠构造怪值绕过。
			name: "malformed origin blocked", method: http.MethodPost,
			origin: "://not a url", host: "127.0.0.1:24680",
			wantHit: false, want: http.StatusForbidden,
		},
		{
			name: "null origin blocked", method: http.MethodPost,
			origin: "null", host: "127.0.0.1:24680",
			wantHit: false, want: http.StatusForbidden,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hit := false
			req := httptest.NewRequest(c.method, "/api/rapis", nil)
			req.Host = c.host
			if c.origin != "" {
				req.Header.Set("Origin", c.origin)
			}
			if c.referer != "" {
				req.Header.Set("Referer", c.referer)
			}
			rec := httptest.NewRecorder()

			originGuard(okHandler(&hit)).ServeHTTP(rec, req)

			if hit != c.wantHit {
				t.Errorf("handler reached = %v, want %v (status %d)", hit, c.wantHit, rec.Code)
			}
			if rec.Code != c.want {
				t.Errorf("status = %d, want %d", rec.Code, c.want)
			}
		})
	}
}

// TestOriginGuardPreservesResponse：放行时不能篡改下游响应（面板 SPA 与
// JSON API 都依赖正常输出）。
func TestOriginGuardPreservesResponse(t *testing.T) {
	h := originGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/x", nil)
	req.Host = "h:1"
	req.Header.Set("Origin", "http://h:1")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", rec.Code)
	}
	if got := rec.Body.String(); got != `{"ok":true}` {
		t.Errorf("body = %q, want the handler's own body", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
}
