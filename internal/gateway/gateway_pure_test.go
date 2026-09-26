package gateway

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"gateway/internal/models"
)

// normalizeProbe：并发数下限 1，超时下限 15s。探测参数配错时不允许 0 并发
// （会跳过恢复）或 0 超时（立即失败）。
func TestNormalizeProbe(t *testing.T) {
	c, d := normalizeProbe(8, 10)
	if c != 8 || d != 10*time.Second {
		t.Errorf("got (%d,%v), want (8,10s)", c, d)
	}
	c, d = normalizeProbe(0, 0)
	if c != 1 || d != 15*time.Second {
		t.Errorf("got (%d,%v), want (1,15s)", c, d)
	}
	c, d = normalizeProbe(-3, -5)
	if c != 1 || d != 15*time.Second {
		t.Errorf("got (%d,%v), want (1,15s)", c, d)
	}
}

// expandHeaderValue：{{timestamp}} / {{uuid}} 占位展开，无占位原样返回。
func TestExpandHeaderValue(t *testing.T) {
	if got := expandHeaderValue("static-value"); got != "static-value" {
		t.Errorf("static = %q", got)
	}
	got := expandHeaderValue("ts-{{timestamp}}")
	ts := strings.TrimPrefix(got, "ts-")
	if ts == "{{timestamp}}" || ts == "" {
		t.Errorf("timestamp not expanded: %q", got)
	}
	for _, c := range ts {
		if c < '0' || c > '9' {
			t.Errorf("timestamp %q not numeric", ts)
			break
		}
	}
	a := expandHeaderValue("id-{{uuid}}")
	b := expandHeaderValue("id-{{uuid}}")
	if a == b {
		t.Error("two uuid expansions identical; expected random")
	}
	if len(strings.TrimPrefix(a, "id-")) != 36 {
		t.Errorf("uuid %q wrong shape", a)
	}
	// 双占位同串。
	if got := expandHeaderValue("{{timestamp}}/{{uuid}}"); strings.Contains(got, "{{") {
		t.Errorf("placeholders left unexpanded: %q", got)
	}
}

// isTimeoutError：超时走短冷却，非超时走长冷却 —— 分错类直接改变重试节奏。
func TestIsTimeoutError(t *testing.T) {
	if isTimeoutError(nil) {
		t.Error("nil error is not a timeout")
	}
	if !isTimeoutError(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded must be a timeout")
	}
	if !isTimeoutError(&net.DNSError{IsTimeout: true}) {
		t.Error("net timeout error must be a timeout")
	}
	if isTimeoutError(&net.DNSError{IsTimeout: false}) {
		t.Error("non-timeout net error must not be a timeout")
	}
	if isTimeoutError(errors.New("connection refused")) {
		t.Error("connection refused must not be a timeout")
	}
	wrapped := &urlTimeoutError{msg: "dial timeout", timeout: true}
	if !isTimeoutError(wrapped) {
		t.Error("wrapped net timeout must be a timeout")
	}
}

type urlTimeoutError struct {
	msg     string
	timeout bool
}

func (e *urlTimeoutError) Error() string { return e.msg }

// 注意：本工具链（go1.27）的 net.Error 有三个方法（Error/Temporary/Timeout）。
// fake 必须实现全部三个，否则 errors.As 匹配不上 —— 这正是本用例要锁的行为
// （只实现 Timeout 的"半吊子"错误不能被当成网络超时）。
func (e *urlTimeoutError) Timeout() bool   { return e.timeout }
func (e *urlTimeoutError) Temporary() bool { return e.timeout }

// pickTargetFormat：客户端格式优先（零转换），否则按 openai→anthropic→gemini
// 顺序取端点支持的第一个，都不支持回退 openai。
func TestPickTargetFormat(t *testing.T) {
	native := func(formats string) models.RAPIWithPlatform {
		return models.RAPIWithPlatform{SupportedFormats: formats}
	}
	if got := pickTargetFormat(native(`["openai","anthropic"]`), "anthropic"); got != "anthropic" {
		t.Errorf("native format not preferred: %q", got)
	}
	if got := pickTargetFormat(native(`["gemini"]`), "openai"); got != "gemini" {
		t.Errorf("fallback = %q, want gemini", got)
	}
	if got := pickTargetFormat(native(`[]`), "anthropic"); got != "openai" {
		t.Errorf("empty support = %q, want openai fallback", got)
	}
}

// resolveUpstreamURL：探测到的精确端点优先，坏 JSON/缺 key 时回退拼接。
func TestResolveUpstreamURL(t *testing.T) {
	withEP := models.RAPIWithPlatform{
		BaseURL:                 "https://x.example/v1",
		Model:                   "m",
		PlatformFormatEndpoints: `{"openai":"https://x.example/v1/chat/completions","anthropic":"https://x.example/v1/messages"}`,
	}
	if got := resolveUpstreamURL(withEP, "anthropic"); got != "https://x.example/v1/messages" {
		t.Errorf("recorded endpoint not used: %q", got)
	}
	badJSON := models.RAPIWithPlatform{BaseURL: "https://x.example/v1", Model: "m", PlatformFormatEndpoints: "{oops"}
	got := resolveUpstreamURL(badJSON, "openai")
	if !strings.Contains(got, "https://x.example/v1") || !strings.Contains(got, "m") {
		t.Errorf("bad JSON should fall back to built URL, got %q", got)
	}
	empty := models.RAPIWithPlatform{BaseURL: "https://x.example/v1", Model: "m"}
	if got := resolveUpstreamURL(empty, "openai"); !strings.Contains(got, "m") {
		t.Errorf("empty endpoints should fall back to built URL, got %q", got)
	}
}

// respHeaders：只取每键首值。
func TestRespHeaders(t *testing.T) {
	resp := &http.Response{Header: http.Header{
		"Content-Type": {"application/json"},
		"X-Multi":      {"a", "b"},
	}}
	got := respHeaders(resp)
	if got["Content-Type"] != "application/json" || got["X-Multi"] != "a" {
		t.Errorf("respHeaders = %v", got)
	}
	if len(respHeaders(&http.Response{})) != 0 {
		t.Error("empty headers should yield empty map")
	}
}
