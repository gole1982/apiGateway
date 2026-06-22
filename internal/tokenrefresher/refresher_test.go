package tokenrefresher

import "testing"

func TestParseCommandArgs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{"single arg", "C:\\tools\\get-token.exe", []string{"C:\\tools\\get-token.exe"}},
		{"two args", "C:\\tools\\get-token.exe --key=abc", []string{"C:\\tools\\get-token.exe", "--key=abc"}},
		{"quoted args", `"C:\tools\get-token.exe" "arg1" "arg2"`, []string{"C:\\tools\\get-token.exe", "arg1", "arg2"}},
		{"mixed quotes", `C:\tools\get-token.exe "arg with space"`, []string{`C:\tools\get-token.exe`, "arg with space"}},
		{"empty string", "", nil},
		{"spaces only", "   ", nil},
		{"multiple spaces", "cmd   arg1   arg2", []string{"cmd", "arg1", "arg2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCommandArgs(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("parseCommandArgs(%q) returned %d args, want %d", tt.input, len(got), len(tt.expected))
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("arg[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestTrimTokenOutput(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain token", "sk-abc123def456", "sk-abc123def456"},
		{"token with newline", "sk-abc123\n", "sk-abc123"},
		{"token with leading space", "  sk-abc123", "sk-abc123"},
		{"token with trailing space", "sk-abc123  ", "sk-abc123"},
		{"json line skipped", "{\"token\":\"sk-abc\"}\nsk-abc", "sk-abc"},
		{"empty output", "", ""},
		{"whitespace only", "   \n  ", ""},
		{"multiline takes first non-json", "line1\nline2\nline3", "line1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := trimTokenOutput(tt.input)
			if got != tt.expected {
				t.Errorf("trimTokenOutput(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestSplitLines(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"single line", "hello", 1},
		{"two lines", "hello\nworld", 2},
		{"with crlf", "hello\r\nworld", 2},
		{"empty", "", 0},
		{"trailing newline", "a\nb\n", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitLines(tt.input)
			if len(got) != tt.expected {
				t.Errorf("splitLines(%q) returned %d lines, want %d", tt.input, len(got), tt.expected)
			}
		})
	}
}

func TestIsJSON(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"{}", true},
		{"{\"key\":\"value\"}", true},
		{"  {}", true},
		{"not json", false},
		{"", false},
		{"[]", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := isJSON(tt.input)
			if got != tt.expected {
				t.Errorf("isJSON(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
