package prom

import (
	"strings"
	"testing"
)

// TestValidMetricName 覆盖 Prometheus 指标名的首字符/后续字符规则。
func TestValidMetricName(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"apigateway_up", true},
		{"apigateway_requests_total", true},
		{"_private", true},
		{":colon", true},
		{"a1_b:c", true},
		{"", false},
		{"1leading_digit", false},
		{"has-dash", false}, // '-' 是 recording rule 保留字符
		{"has space", false},
		{"has.dot", false},
	}
	for _, c := range cases {
		if got := validMetricName(c.in); got != c.want {
			t.Errorf("validMetricName(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestValidLabelName：label 名不允许冒号（那是指标名的特权）。
func TestValidLabelName(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"model", true},
		{"_x9", true},
		{"", false},
		{"1a", false},
		{"a-b", false},
		{"a:b", false},
	}
	for _, c := range cases {
		if got := validLabelName(c.in); got != c.want {
			t.Errorf("validLabelName(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestParseLabelsOddLength：k,v 必须成对，否则注册应报错而不是产出一行坏样本。
func TestParseLabelsOddLength(t *testing.T) {
	if _, err := parseLabels([]string{"model"}); err == nil {
		t.Error("parseLabels with odd length = nil error, want error")
	}
}

// TestParseLabelsDuplicate：重复 label key 必须报错 —— Prometheus 侧
// 会直接拒绝解析整份曝光，错误在这里抛出会好定位得多。
func TestParseLabelsDuplicate(t *testing.T) {
	if _, err := parseLabels([]string{"model", "a", "model", "b"}); err == nil {
		t.Error("parseLabels with duplicate key = nil error, want error")
	}
}

// TestParseLabelsSorted 保证 label 按 key 排序，输出才可复现。
func TestParseLabelsSorted(t *testing.T) {
	got, err := parseLabels([]string{"state", "total", "kind", "counter"})
	if err != nil {
		t.Fatalf("parseLabels: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].k != "kind" || got[1].k != "state" {
		t.Errorf("order = %s,%s; want kind,state", got[0].k, got[1].k)
	}
}

// TestGaugeRejectsInvalidName：非法名必须返回 error（调用点虽忽略 error，
// 但单测锁住"错误确实被报出来"，避免将来有人把校验删了没人发现）。
func TestGaugeRejectsInvalidName(t *testing.T) {
	r := New()
	if err := r.Gauge("bad-name", 1); err == nil {
		t.Error("Gauge with invalid name = nil error, want error")
	}
	if err := r.Counter("1bad", 1); err == nil {
		t.Error("Counter with invalid name = nil error, want error")
	}
}

// TestEscapeLabelValue：反斜杠必须先转义，否则后补的转义符会被二次转义。
// 这正是 PromQL 里 label 值含引号时最常见的坑。
func TestEscapeLabelValue(t *testing.T) {
	cases := []struct{ in, want string }{
		{`plain`, `plain`},
		{`say "hi"`, `say \"hi\"`},
		{`back\slash`, `back\\slash`},
		{"line\nbreak", `line\nbreak`},
		{`a"b\c`, `a\"b\\c`},
		{"中文", "中文"},
	}
	for _, c := range cases {
		if got := escapeLabelValue(c.in); got != c.want {
			t.Errorf("escapeLabelValue(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestFormatValue：整数不该渲染成 "0.000000"，否则 /metrics 文本难读。
func TestFormatValue(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{-3, "-3"},
		{1234567, "1234567"},
		{0.5, "0.5"},
		{1.25, "1.25"},
	}
	for _, c := range cases {
		if got := formatValue(c.in); got != c.want {
			t.Errorf("formatValue(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestGatherDeterministic 是这个包最关键的一条测试：同一份输入必须产出
// 逐字节相同的输出。map 遍历顺序会让输出随机，Prometheus 的 change
// detection 会把噪声当真实变化，持续误报。
func TestGatherDeterministic(t *testing.T) {
	build := func() string {
		r := New()
		// 故意乱序注册，并让多个指标共享 label key，考验排序。
		_ = r.Gauge("zeta_metric", 1, "b", "2", "a", "1")
		_ = r.Gauge("alpha_metric", 2)
		_ = r.Gauge("zeta_metric", 3, "a", "9")
		_ = r.Counter("req_total", 7, "result", "ok")
		_ = r.Counter("req_total", 8, "result", "fail")
		_ = r.Counter("aaa_total", 5)
		return r.Gather()
	}
	first := build()
	for i := 0; i < 20; i++ {
		if got := build(); got != first {
			t.Fatalf("Gather not deterministic on run %d:\n--- first ---\n%s\n--- got ---\n%s", i, first, got)
		}
	}
}

// TestGatherFormat 校验产出的文本符合 text exposition format 的骨架：
// 每个指标名恰好一条 # TYPE，且 TYPE 行出现在其样本行之前。
func TestGatherFormat(t *testing.T) {
	r := New()
	_ = r.Gauge("apigateway_rapi", 3, "state", "total")
	_ = r.Counter("apigateway_requests_total", 42, "result", "success")
	_ = r.Counter("apigateway_requests_total", 2, "result", "failed")
	out := r.Gather()

	if !strings.Contains(out, "# TYPE apigateway_rapi gauge\n") {
		t.Errorf("missing gauge TYPE line:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE apigateway_requests_total counter\n") {
		t.Errorf("missing counter TYPE line:\n%s", out)
	}
	// label 按 key 排序后输出：apigateway_requests_total{result="failed"}
	want := `apigateway_requests_total{result="failed"} 2` + "\n"
	if !strings.Contains(out, want) {
		t.Errorf("missing sample %q in:\n%s", want, out)
	}

	// TYPE 行后面必须紧跟它描述的样本行（不能是另一条 TYPE，也不能是别的
	// 指标名）—— 同名样本必须聚在同一个 TYPE 之下。
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	for i, ln := range lines {
		if !strings.HasPrefix(ln, "# TYPE ") {
			continue
		}
		if i == len(lines)-1 {
			t.Errorf("TYPE line %q has no samples after it", ln)
			continue
		}
		name := strings.Fields(ln)[2]
		if !strings.HasPrefix(lines[i+1], name) {
			t.Errorf("TYPE line %q is followed by %q, want a sample of %q", ln, lines[i+1], name)
		}
	}
}

// TestGatherEmpty：空 registry 产出空串而不是带一堆头部噪声。
func TestGatherEmpty(t *testing.T) {
	if got := New().Gather(); got != "" {
		t.Errorf("empty Gather = %q, want empty", got)
	}
}
