package service

import "testing"

func TestParseID(t *testing.T) {
	cases := []struct {
		in     string
		want   int64
		wantOK bool
	}{
		{"123", 123, true},
		{"0", 0, true},   // id=0 也合法（不依赖 SQLite 自增从 1 起的假设）
		{"-7", -7, true},
		{"", 0, false},   // 空串
		{"abc", 0, false}, // 别名（模型/接口维度）
		{"12a", 0, false}, // 混排非纯数字
		{"-", 0, false},   // 仅负号
	}
	for _, c := range cases {
		got, ok := parseID(c.in)
		if got != c.want || ok != c.wantOK {
			t.Errorf("parseID(%q) = (%d,%v), want (%d,%v)", c.in, got, ok, c.want, c.wantOK)
		}
	}
}
