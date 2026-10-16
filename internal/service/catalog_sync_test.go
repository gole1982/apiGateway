package service

import "testing"

// TestJudgeRAPI 覆盖判定的三条分支：失效 / 恢复 / 保持。
// 这是整个自动失效机制的核心逻辑，必须逐分支锁住。
func TestJudgeRAPI(t *testing.T) {
	const model = "gpt-4o"

	enabled := keyProbeState{Enabled: true, Catalog: []string{"gpt-4o", "gpt-4o-mini"}}
	otherModel := keyProbeState{Enabled: true, Catalog: []string{"claude-sonnet"}}
	dead := keyProbeState{Enabled: true, Dead: true, Catalog: []string{"gpt-4o"}}
	failed := keyProbeState{Enabled: true, Failed: true}
	disabled := keyProbeState{Enabled: false, Catalog: []string{"gpt-4o"}}

	cases := []struct {
		name    string
		keys    []keyProbeState
		want    judgmentAction
		wantWhy string
	}{
		{
			"pool 非空 → 恢复候选",
			[]keyProbeState{enabled},
			judgmentRecover, "",
		},
		{
			"pool 空 + 有未知 → 保持",
			[]keyProbeState{otherModel, failed},
			judgmentHold, "有 Key 尚未探测成功",
		},
		{
			"pool 空 + 无未知 → 失效",
			[]keyProbeState{otherModel},
			judgmentInvalidate, "所有 Key 的目录均不含此模型",
		},
		{
			"dead Key 不进任何集合：只剩 dead → 失效",
			[]keyProbeState{dead, otherModel},
			judgmentInvalidate, "所有 Key 的目录均不含此模型",
		},
		{
			"dead Key 不算未知：dead + failed → 保持",
			[]keyProbeState{dead, failed},
			judgmentHold, "有 Key 尚未探测成功",
		},
		{
			"禁用 Key 被忽略：只剩禁用 → 失效",
			[]keyProbeState{disabled, otherModel},
			judgmentInvalidate, "所有 Key 的目录均不含此模型",
		},
		{
			"目录大小写不敏感：GPT-4O 也算命中",
			[]keyProbeState{{Enabled: true, Catalog: []string{"GPT-4O"}}},
			judgmentRecover, "",
		},
		{
			"空目录不算命中",
			[]keyProbeState{{Enabled: true, Catalog: []string{}}},
			judgmentInvalidate, "所有 Key 的目录均不含此模型",
		},
		{
			"无 Key → 失效（pool 和 unknown 都空）",
			[]keyProbeState{},
			judgmentInvalidate, "所有 Key 的目录均不含此模型",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, why := judgeRAPI(model, c.keys)
			if got != c.want {
				t.Errorf("judgeRAPI() = %v, want %v", got, c.want)
			}
			if c.wantWhy != "" && !containsSubstr(why, c.wantWhy) {
				t.Errorf("reason = %q, want 含 %q", why, c.wantWhy)
			}
		})
	}
}

func containsSubstr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestJudgeManualRAPI manual 模型的判定入口，与 judgeRAPI 同口径。
func TestJudgeManualRAPI(t *testing.T) {
	cases := []struct {
		name                string
		pool, dead, unknown int
		want                judgmentAction
	}{
		{"有 Key 调用成功 → 恢复", 1, 0, 0, judgmentRecover},
		{"全部失败但有未知 → 保持", 0, 0, 2, judgmentHold},
		{"全部凭据级失败 → 失效", 0, 2, 0, judgmentInvalidate},
		{"无 Key → 失效", 0, 0, 0, judgmentInvalidate},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := judgeManualRAPI(c.pool, c.dead, c.unknown)
			if got != c.want {
				t.Errorf("judgeManualRAPI(%d,%d,%d) = %v, want %v", c.pool, c.dead, c.unknown, got, c.want)
			}
		})
	}
}

// TestCatalogHasModel 目录匹配的大小写与空白容忍度。
func TestCatalogHasModel(t *testing.T) {
	cases := []struct {
		catalog []string
		model   string
		want    bool
	}{
		{[]string{"gpt-4o"}, "gpt-4o", true},
		{[]string{"GPT-4O"}, "gpt-4o", true},
		{[]string{"  gpt-4o  "}, "gpt-4o", true},
		{[]string{"gpt-4o-mini"}, "gpt-4o", false},
		{[]string{}, "gpt-4o", false},
		{[]string{"gpt-4o"}, "", false},
	}
	for _, c := range cases {
		if got := catalogHasModel(c.catalog, c.model); got != c.want {
			t.Errorf("catalogHasModel(%v, %q) = %v, want %v", c.catalog, c.model, got, c.want)
		}
	}
}
