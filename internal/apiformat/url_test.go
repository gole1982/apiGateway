package apiformat

import "testing"

func TestBuildURL(t *testing.T) {
	cases := []struct {
		name    string
		baseURL string
		model   string
		format  APIFormat
		want    string
	}{
		// Basic: clean base URL
		{"openai clean", "https://api.example.com", "gpt-4o", FormatOpenAI, "https://api.example.com/v1/chat/completions"},
		{"anthropic clean", "https://api.example.com", "claude-4", FormatAnthropic, "https://api.example.com/v1/messages"},
		{"gemini clean", "https://api.example.com", "gemini-2.0-flash", FormatGemini, "https://api.example.com/v1beta/models/gemini-2.0-flash:generateContent"},

		// Strip /v1 suffix
		{"openai strip /v1", "https://api.example.com/v1", "gpt-4o", FormatOpenAI, "https://api.example.com/v1/chat/completions"},
		{"gemini strip /v1", "https://api.example.com/v1", "gemini-2.0-flash", FormatGemini, "https://api.example.com/v1beta/models/gemini-2.0-flash:generateContent"},

		// Strip /v1beta suffix (Bug fix: previously produced /v1beta/v1beta/...)
		{"openai strip /v1beta", "https://api.example.com/v1beta", "gpt-4o", FormatOpenAI, "https://api.example.com/v1/chat/completions"},
		{"gemini strip /v1beta", "https://api.example.com/v1beta", "gemini-2.0-flash", FormatGemini, "https://api.example.com/v1beta/models/gemini-2.0-flash:generateContent"},
		{"anthropic strip /v1beta", "https://api.example.com/v1beta", "claude-4", FormatAnthropic, "https://api.example.com/v1/messages"},

		// Strip /v1beta/models suffix
		{"gemini strip /v1beta/models", "https://api.example.com/v1beta/models", "gemini-2.0-flash", FormatGemini, "https://api.example.com/v1beta/models/gemini-2.0-flash:generateContent"},

		// Strip full gemini path
		{"gemini strip full path", "https://api.example.com/v1beta/models/gemini-2.0-flash:generateContent", "gemini-2.0-flash", FormatGemini, "https://api.example.com/v1beta/models/gemini-2.0-flash:generateContent"},

		// Strip /v1/chat/completions suffix
		{"openai strip full path", "https://api.example.com/v1/chat/completions", "gpt-4o", FormatOpenAI, "https://api.example.com/v1/chat/completions"},

		// Strip /v1/messages suffix
		{"anthropic strip full path", "https://api.example.com/v1/messages", "claude-4", FormatAnthropic, "https://api.example.com/v1/messages"},

		// Trailing slash
		{"trailing slash", "https://api.example.com/", "gpt-4o", FormatOpenAI, "https://api.example.com/v1/chat/completions"},
		{"trailing slash + v1beta", "https://api.example.com/v1beta/", "gemini-2.0-flash", FormatGemini, "https://api.example.com/v1beta/models/gemini-2.0-flash:generateContent"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildURL(tc.baseURL, tc.model, tc.format)
			if got != tc.want {
				t.Errorf("BuildURL(%q, %q, %v) = %q, want %q", tc.baseURL, tc.model, tc.format, got, tc.want)
			}
		})
	}
}

// TestEnsureGeminiStreamSSE is a regression test for the bug where streaming
// requests to a real Google Gemini endpoint returned 200 to the gateway but
// produced zero output frames for the client: the URL was rewritten to
// :streamGenerateContent without ?alt=sse, so Google answered with a buffered
// JSON array of chunks rather than SSE (data: <json>\n\n) frames — the
// StreamConverter parsed it as plain lines and emitted nothing.
func TestEnsureGeminiStreamSSE(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"plain generateContent gets suffix + alt=sse",
			"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent",
			"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:streamGenerateContent?alt=sse",
		},
		{
			"already streamGenerateContent gets only alt=sse",
			"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:streamGenerateContent",
			"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:streamGenerateContent?alt=sse",
		},
		{
			"already has alt=sse is idempotent",
			"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:streamGenerateContent?alt=sse",
			"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:streamGenerateContent?alt=sse",
		},
		{
			"existing query param gets &alt=sse",
			"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent?key=AIzaXYZ",
			"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:streamGenerateContent?key=AIzaXYZ&alt=sse",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EnsureGeminiStreamSSE(tc.in)
			if got != tc.want {
				t.Errorf("EnsureGeminiStreamSSE(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestNormalizeModelsBaseURL is a regression test for a real incident where a
// baseURL ending in "/v1/" (trailing slash) produced the doubled, rejected
// path ".../v1/v1/models". Serverless/HTTP-node gateways reject that doubled
// path with "only allows access to inference API paths" (
// Gateway key-probe returned {"message":"...inference API paths..."}).
// NormalizeModelsBaseURL must trim trailing slashes BEFORE stripping a known
// "/v1"/"/v1beta" suffix so the two steps do not fight.
func TestNormalizeModelsBaseURL(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantURL string
	}{
		{"clean root", "https://api.example.com", "https://api.example.com/v1/models"},
		{"single trailing slash", "https://api.example.com/", "https://api.example.com/v1/models"},
		{"root with /v1", "https://api.example.com/v1", "https://api.example.com/v1/models"},
		// REGRESSION: trailing slash after /v1 must not double the path.
		{"v1 with trailing slash", "https://api.example.com/v1/", "https://api.example.com/v1/models"},
		{"v1 with multiple trailing slashes", "https://api.example.com/v1///", "https://api.example.com/v1/models"},
		{"full chat path", "https://api.example.com/v1/chat/completions", "https://api.example.com/v1/models"},
		{"full messages path", "https://api.example.com/v1/messages", "https://api.example.com/v1/models"},
		{"v1beta", "https://api.example.com/v1beta", "https://api.example.com/v1/models"},
		{"v1beta trailing slash", "https://api.example.com/v1beta/", "https://api.example.com/v1/models"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NormalizeModelsBaseURL(tc.in) + "/v1/models"
			if got != tc.wantURL {
				t.Errorf("NormalizeModelsBaseURL(%q) + /v1/models = %q, want %q", tc.in, got, tc.wantURL)
			}
		})
	}
}
