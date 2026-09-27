package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// discoverUpstreamModels：上游模型列表拉取 + 双引擎解析。
// 新增向导里的"发现模型"（免入库一次性发现）与平台详情页共用它。
// 上游返回形状一变（空 data / 换字段），这里是第一道哨卡。
// ---------------------------------------------------------------------------

func openAIMock(t *testing.T, status int, body string, checkAuth *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/v1/models") {
			http.Error(w, "wrong path "+r.URL.Path, http.StatusNotFound)
			return
		}
		if checkAuth != nil {
			*checkAuth = r.Header.Get("Authorization")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestDiscoverOpenAIParsesIDs(t *testing.T) {
	var auth string
	srv := openAIMock(t, 200, `{"data":[{"id":"gpt-4o"},{"id":""},{"id":"kimi-k3"}]}`, &auth)
	got, status, err := discoverUpstreamModels(context.Background(), srv.URL, "sk-test")
	if err != nil || status != 0 {
		t.Fatalf("discover = %v,%d,%v", got, status, err)
	}
	if len(got) != 2 || got[0] != "gpt-4o" || got[1] != "kimi-k3" {
		t.Errorf("models = %v, want [gpt-4o kimi-k3] (empty id dropped)", got)
	}
	if auth != "Bearer sk-test" {
		t.Errorf("auth header = %q, want Bearer sk-test", auth)
	}
}

func TestDiscoverUpstreamErrorPassthrough(t *testing.T) {
	srv := openAIMock(t, 401, `{"error":"bad key"}`, nil)
	_, status, err := discoverUpstreamModels(context.Background(), srv.URL, "sk-bad")
	if err == nil {
		t.Fatal("expected error on 401")
	}
	if status != 401 {
		t.Errorf("status = %d, want 401 (upstream code preserved for the UI)", status)
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error %q must carry the upstream status", err)
	}
}

func TestDiscoverBadJSON(t *testing.T) {
	srv := openAIMock(t, 200, `not json at all`, nil)
	_, status, err := discoverUpstreamModels(context.Background(), srv.URL, "sk-x")
	if err == nil || status != 502 {
		t.Errorf("bad JSON = status %d err %v, want 502 + err", status, err)
	}
}

func TestDiscoverNetworkError(t *testing.T) {
	_, status, err := discoverUpstreamModels(context.Background(), "http://127.0.0.1:1", "sk-x")
	if err == nil || status != 502 {
		t.Errorf("unreachable = status %d err %v, want 502 + err", status, err)
	}
}

// Google 原生分支（x-goog-api-key + models[].name）故意不在此做端到端测试：
// 分支判定只认 *.googleapis.com 真实域名，mock 绕不过，打 live 会让单测依赖
// 外网且消耗配额。分支判定、URL 拼装、响应解析在 apiformat 包已有单测
// （TestIsGoogleNativeBaseURL / TestBuildGoogleListModelsURL /
// TestParseGoogleListModelsResponse），此处只锁 OpenAI 分支与错误映射。
func TestDiscoverNonGoogleHostUsesOpenAIPath(t *testing.T) {
	var auth string
	srv := openAIMock(t, 200, `{"data":[{"id":"m1"}]}`, &auth)
	got, _, err := discoverUpstreamModels(context.Background(), srv.URL+"/v1", "sk-test")
	if err != nil || len(got) != 1 || got[0] != "m1" {
		t.Fatalf("discover = %v,%v", got, err)
	}
	if auth != "Bearer sk-test" {
		t.Errorf("non-google host must use Bearer auth, got %q", auth)
	}
}
