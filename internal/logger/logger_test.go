package logger

import (
	"encoding/json"
	"strings"
	"testing"
)

// getString must return a plain string value unchanged.
func TestGetStringPlainString(t *testing.T) {
	data := map[string]interface{}{"key": "hello"}
	if got := getString(data, "key"); got != "hello" {
		t.Errorf("getString string = %q; want %q", got, "hello")
	}
}

// getString must serialise a map[string]interface{} (headers stored by RecordRequestReceived)
// to a JSON string so request_logs.request_headers is never silently empty.
func TestGetStringMapSerialised(t *testing.T) {
	headers := map[string]interface{}{
		"Content-Type":  "application/json",
		"Authorization": "Bearer ***",
	}
	data := map[string]interface{}{"request_headers": headers}

	got := getString(data, "request_headers")
	if got == "" {
		t.Fatal("getString returned empty string for map value; want JSON")
	}
	// Must be valid JSON.
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("getString result is not valid JSON: %q — %v", got, err)
	}
	if parsed["Content-Type"] != "application/json" {
		t.Errorf("Content-Type = %v; want application/json", parsed["Content-Type"])
	}
}

// getString must return "" for a missing key.
func TestGetStringMissingKey(t *testing.T) {
	data := map[string]interface{}{"other": "x"}
	if got := getString(data, "missing"); got != "" {
		t.Errorf("getString missing key = %q; want empty", got)
	}
}

// truncateBody must not truncate when body is within the limit.
func TestTruncateBodyWithinLimit(t *testing.T) {
	body := `{"messages":[{"role":"user","content":"hello"}]}`
	got := truncateBody(body, 50) // 50 KB — body is ~48 bytes
	if got != body {
		t.Errorf("truncateBody modified body within limit")
	}
}

// truncateBody must append " [TRUNCATED]" and cut at the byte boundary.
func TestTruncateBodyOverLimit(t *testing.T) {
	const suffix = " [TRUNCATED]"
	body := strings.Repeat("x", 10*1024+1) // 10 KB + 1 byte
	got := truncateBody(body, 10)           // 10 KB limit
	if !strings.HasSuffix(got, suffix) {
		t.Errorf("truncateBody did not append %q", suffix)
	}
	if len(got) != 10*1024+len(suffix) {
		t.Errorf("truncateBody result length = %d; want %d", len(got), 10*1024+len(suffix))
	}
}

// extractMaxTokens must parse a simple max_tokens field.
func TestExtractMaxTokens(t *testing.T) {
	body := `{"model":"gpt-4","max_tokens":4096,"messages":[]}`
	if got := extractMaxTokens(body); got != 4096 {
		t.Errorf("extractMaxTokens = %d; want 4096", got)
	}
}

// extractMaxTokens must return 0 when field is absent.
func TestExtractMaxTokensAbsent(t *testing.T) {
	body := `{"model":"gpt-4","messages":[]}`
	if got := extractMaxTokens(body); got != 0 {
		t.Errorf("extractMaxTokens = %d; want 0", got)
	}
}

// extractFinishReason must find the last non-null finish_reason in an SSE body.
func TestExtractFinishReasonStop(t *testing.T) {
	body := `data: {"choices":[{"delta":{"content":"hi"},"finish_reason":null}]}` + "\n\n" +
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
		`data: [DONE]`
	if got := extractFinishReason(body); got != "stop" {
		t.Errorf("extractFinishReason = %q; want stop", got)
	}
}

// extractFinishReason must return "" when body contains only null values.
func TestExtractFinishReasonAllNull(t *testing.T) {
	body := `data: {"choices":[{"finish_reason":null}]}` + "\n\n"
	if got := extractFinishReason(body); got != "" {
		t.Errorf("extractFinishReason = %q; want empty", got)
	}
}
