package gateway

import (
	"net/http"
	"testing"
)

func TestReplaceModelInBody(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		oldModel string
		newModel string
		contains string
	}{
		{"no space", `{"model":"gpt-4","messages":[]}`, "gpt-4", "claude-3", `"model":"claude-3"`},
		{"with space", `{"model": "gpt-4", "messages":[]}`, "gpt-4", "claude-3", `"model": "claude-3"`},
		{"no match", `{"model":"gpt-3.5","messages":[]}`, "gpt-4", "claude-3", `"model":"gpt-3.5"`},
		{"multiple occurrences", `{"model":"gpt-4","model":"gpt-4"}`, "gpt-4", "claude-3", `"model":"claude-3"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := replaceModelInBody([]byte(tt.body), tt.oldModel, tt.newModel)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !containsString(string(result), tt.contains) {
				t.Errorf("result %q does not contain %q", string(result), tt.contains)
			}
		})
	}
}

func TestExtractTokenUsage(t *testing.T) {
	tests := []struct {
		name     string
		headers  http.Header
		expected int
	}{
		{"openai-usage", http.Header{"Openai-Usage": []string{"150"}}, 150},
		{"x-token-usage", http.Header{"X-Token-Usage": []string{"200"}}, 200},
		{"no usage header", http.Header{}, 0},
		{"invalid number", http.Header{"Openai-Usage": []string{"abc"}}, 0},
		{"openai takes precedence", http.Header{"Openai-Usage": []string{"100"}, "X-Token-Usage": []string{"200"}}, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractTokenUsage(tt.headers)
			if got != tt.expected {
				t.Errorf("extractTokenUsage() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
