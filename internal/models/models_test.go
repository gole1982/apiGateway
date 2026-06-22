package models

import "testing"

func TestNormalizeToCompletionsURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"bare base", "https://api.openai.com", "https://api.openai.com/v1/chat/completions"},
		{"trailing slash", "https://api.openai.com/", "https://api.openai.com/v1/chat/completions"},
		{"with /v1", "https://api.openai.com/v1", "https://api.openai.com/v1/chat/completions"},
		{"with /v1/", "https://api.openai.com/v1/", "https://api.openai.com/v1/chat/completions"},
		{"with /v1/chat", "https://api.openai.com/v1/chat", "https://api.openai.com/v1/chat/completions"},
		{"with /v1/chat/", "https://api.openai.com/v1/chat/", "https://api.openai.com/v1/chat/completions"},
		{"already full path", "https://api.openai.com/v1/chat/completions", "https://api.openai.com/v1/chat/completions"},
		{"multiple trailing slashes", "https://api.openai.com///", "https://api.openai.com/v1/chat/completions"},
		{"custom base", "https://my-proxy.internal:8080", "https://my-proxy.internal:8080/v1/chat/completions"},
		{"custom base with /v1", "https://my-proxy.internal:8080/v1", "https://my-proxy.internal:8080/v1/chat/completions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeToCompletionsURL(tt.input)
			if got != tt.expected {
				t.Errorf("NormalizeToCompletionsURL(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
