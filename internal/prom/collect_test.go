package prom

import (
	"strings"
	"testing"
	"time"

	"gateway/internal/models"
	"gateway/internal/scheduler"
)

func TestTruncateRuneAware(t *testing.T) {
	// 按字节切会把多字节中文切出半个字符（乱码）。本包按 rune 计数。
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"short", 10, "short"},
		{"exactly-10", 10, "exactly-10"},
		{"truncate-me-please", 8, "truncate"},
		{"中文中文中文", 2, "中文"},
		{"中文中文中文", 4, "中文中文"},
		{"abc", 0, ""},
		{"abc", -1, ""},
	}
	for _, c := range cases {
		if got := truncate(c.in, c.n); got != c.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
	// 截断结果必须是合法 UTF-8，且 rune 数不超过 n。
	got := truncate("中文中文中文", 3)
	if n := len([]rune(got)); n != 3 {
		t.Errorf("truncated rune count = %d, want 3", n)
	}
}

// TestCollectEmptyRuntime：没有任何定义对象时也必须产出可用的 up/build_info，
// 否则 Prometheus 会因"目标 down"之外的原因看不到服务（空响应=抓取失败）。
func TestCollectEmptyRuntime(t *testing.T) {
	out := Collect(Runtime{Version: "test"})
	for _, want := range []string{
		`apigateway_build_info{version="test"} 1`,
		"apigateway_up 1",
		`apigateway_rapi{state="total"} 0`,
		`apigateway_key{state="cooling"} 0`,
		"apigateway_requests_total 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestCollectCountsStates 验证存量/可用/冷却的计数口径。
func TestCollectCountsStates(t *testing.T) {
	now := time.Now()
	rt := Runtime{
		Version: "v1",
		RAPIs: []models.RAPIWithPlatform{
			{ID: 1, Alias: "ok", Enabled: true, Available: true},
			{ID: 2, Alias: "cooling", Enabled: true, Available: false},
			{ID: 3, Alias: "off", Enabled: false, Available: false},
		},
		LAPIs: []models.LAPI{
			{ID: 1, Alias: "a", Enabled: true},
			{ID: 2, Alias: "b", Enabled: false},
		},
		Keys: []models.PlatformKey{
			{ID: 10, Enabled: true},
			{ID: 11, Enabled: true},
			{ID: 12, Enabled: false},
		},
		Snap: scheduler.Snapshot{
			Keys: []scheduler.KeySnapshot{
				{ID: 10, Cooling: true, RecoverAt: now},
				{ID: 11, Cooling: false},
			},
			Queues: []scheduler.QueueSnapshot{{LapiID: 1, Count: 3}},
		},
	}
	out := Collect(rt)
	for _, want := range []string{
		`apigateway_rapi{state="total"} 3`,
		`apigateway_rapi{state="enabled"} 2`,
		`apigateway_rapi{state="available"} 1`,
		`apigateway_lapi{state="total"} 2`,
		`apigateway_lapi{state="enabled"} 1`,
		`apigateway_key{state="total"} 3`,
		`apigateway_key{state="enabled"} 2`,
		`apigateway_key{state="cooling"} 1`,
		`apigateway_queue_depth{lapi_id="1"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestCollectCoolingRAPILabels：冷却的 RAPI 逐个打点，带模型名。
func TestCollectCoolingRAPILabels(t *testing.T) {
	rt := Runtime{
		RAPIs: []models.RAPIWithPlatform{{ID: 1, Alias: "gpt-4o", Enabled: true}},
		Snap: scheduler.Snapshot{
			RAPIs: []scheduler.RAPISnapshot{
				{ID: 1, Cooling: true, Reason: `429 "rate limit"`},
				{ID: 2, Cooling: false},
				// 快照里有但 store 查不到（刚被删/同步中间态）——仍要出样本。
				{ID: 99, Cooling: true, Reason: "boom"},
			},
		},
	}
	out := Collect(rt)
	if !strings.Contains(out, `apigateway_rapi_cooling{model="gpt-4o",reason="429 \"rate limit\""} 1`) {
		t.Errorf("cooling sample for known model missing / reason not escaped:\n%s", out)
	}
	if !strings.Contains(out, `apigateway_rapi_cooling{model="unknown",reason="boom"} 1`) {
		t.Errorf("orphan RAPI snapshot should still be exposed:\n%s", out)
	}
	// 非冷却的不该出现。
	if strings.Contains(out, `apigateway_rapi_cooling{model="gpt-4o",reason=""}`) {
		t.Errorf("non-cooling RAPI should not emit a cooling sample:\n%s", out)
	}
}

// TestCollectTrafficCounters 验证 counter 值原样透出。
func TestCollectTrafficCounters(t *testing.T) {
	out := Collect(Runtime{
		Traffic: TrafficCounters{
			TotalRequests: 100, SuccessRequests: 90, FailedRequests: 10,
			InputTokens: 1000, OutputTokens: 500, CachedTokens: 200,
			RetryTotal: 7, FallbackTotal: 3,
		},
	})
	for _, want := range []string{
		"apigateway_requests_total 100",
		`apigateway_requests_total{result="success"} 90`,
		`apigateway_requests_total{result="failed"} 10`,
		`apigateway_tokens_total{kind="input"} 1000`,
		`apigateway_tokens_total{kind="output"} 500`,
		`apigateway_tokens_total{kind="cached"} 200`,
		"apigateway_retries_total 7",
		"apigateway_fallbacks_total 3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestCollectIsDeterministic：Collect 内部有 map 遍历，必须仍然稳定。
func TestCollectIsDeterministic(t *testing.T) {
	rt := Runtime{
		RAPIs: []models.RAPIWithPlatform{
			{ID: 1, Alias: "a", Enabled: true},
			{ID: 2, Alias: "b", Enabled: true},
			{ID: 3, Alias: "c", Enabled: true},
		},
		Keys: []models.PlatformKey{{ID: 1, Enabled: true}, {ID: 2, Enabled: true}},
		Snap: scheduler.Snapshot{
			RAPIs: []scheduler.RAPISnapshot{
				{ID: 1, Cooling: true, Reason: "r1"},
				{ID: 2, Cooling: true, Reason: "r2"},
				{ID: 3, Cooling: true, Reason: "r3"},
			},
		},
	}
	first := Collect(rt)
	for i := 0; i < 20; i++ {
		if got := Collect(rt); got != first {
			t.Fatalf("Collect not deterministic on run %d", i)
		}
	}
}

// TestCollectCoolingReasonTruncated 锁住 reason 长度上限：reason 可能是整段
// 错误响应，无限增长会让 /metrics 变成数 MB 的响应导致抓取超时。
func TestCollectCoolingReasonTruncated(t *testing.T) {
	long := strings.Repeat("错", 500)
	rt := Runtime{
		RAPIs: []models.RAPIWithPlatform{{ID: 1, Alias: "m", Enabled: true}},
		Snap: scheduler.Snapshot{
			RAPIs: []scheduler.RAPISnapshot{{ID: 1, Cooling: true, Reason: long}},
		},
	}
	out := Collect(rt)
	// 截断发生在 rune 边界，不得产生半个字符。
	if strings.Contains(out, "�") {
		t.Errorf("truncation split a multi-byte rune:\n%s", out)
	}
	if n := strings.Count(out, "错"); n != 120 {
		t.Errorf("reason rune count = %d, want 120", n)
	}
}
