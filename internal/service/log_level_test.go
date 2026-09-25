package service

import "testing"

func TestNormalizeLogLevel(t *testing.T) {
	cases := []struct {
		input string
		want  string
		ok    bool
	}{
		{input: " DEBUG ", want: "debug", ok: true},
		{input: "warning", want: "warn", ok: true},
		{input: "err", want: "error", ok: true},
		{input: "trace", want: "trace", ok: false},
		{input: "", want: "", ok: false},
	}
	for _, tc := range cases {
		got, ok := normalizeLogLevel(tc.input)
		if got != tc.want || ok != tc.ok {
			t.Errorf("normalizeLogLevel(%q) = (%q, %v), want (%q, %v)", tc.input, got, ok, tc.want, tc.ok)
		}
	}
}
