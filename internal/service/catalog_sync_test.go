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
	empty := keyProbeState{Enabled: true}
	blank := keyProbeState{Enabled: true, Catalog: []string{"", "   "}}
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
			judgmentHold, "有 Key 目录为空或尚未探测成功",
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
			judgmentHold, "有 Key 目录为空或尚未探测成功",
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
			// 回归：空目录不能证明模型缺席（聚合站常见 200 + 空 data）。
			"空目录 → 未知，不能判定失效",
			[]keyProbeState{empty},
			judgmentHold, "目录为空",
		},
		{
			"全空白目录 → 未知",
			[]keyProbeState{blank},
			judgmentHold, "目录为空",
		},
		{
			"空目录 + 有目录但不含 → 保持",
			[]keyProbeState{empty, otherModel},
			judgmentHold, "目录为空",
		},
		{
			"无 Key → 失效（pool 和 unknown 都空）",
			[]keyProbeState{},
			judgmentInvalidate, "所有 Key 的目录均不含此模型",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, why := judgeRAPI([]string{model}, c.keys)
			if got != c.want {
				t.Errorf("judgeRAPI() = %v, want %v", got, c.want)
			}
			if c.wantWhy != "" && !containsSubstr(why, c.wantWhy) {
				t.Errorf("reason = %q, want 含 %q", why, c.wantWhy)
			}
		})
	}
}

// TestJudgeRAPIMatchesAlias 回归：上游目录可能只返回 alias 形式，
// 而 Model 与 Alias 不是简单的大小写关系（实测 zai-org/GLM-5.3-Flash
// vs z.ai/glm-5.3-flash）。任一命中都必须算在场。
func TestJudgeRAPIMatchesAlias(t *testing.T) {
	keys := []keyProbeState{{Enabled: true, Catalog: []string{"z.ai/glm-5.3-flash"}}}

	if got, _ := judgeRAPI([]string{"zai-org/GLM-5.3-Flash", "z.ai/glm-5.3-flash"}, keys); got != judgmentRecover {
		t.Errorf("alias 命中应恢复, got %v", got)
	}
	if got, _ := judgeRAPI([]string{"zai-org/GLM-5.3-Flash", ""}, keys); got != judgmentInvalidate {
		t.Errorf("alias 不匹配时应失效, got %v", got)
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

func TestCatalogHasAnyModel(t *testing.T) {
	catalog := []string{"alias-name"}
	if !catalogHasAnyModel(catalog, []string{"model-name", "ALIAS-NAME"}) {
		t.Error("alias 命中应返回 true")
	}
	if catalogHasAnyModel(catalog, []string{"", "other"}) {
		t.Error("未命中应返回 false")
	}
}

// TestManualNeedsProbe 恢复轮只重探不可用的 manual 模型，控制真实调用成本。
func TestManualNeedsProbe(t *testing.T) {
	unavailable := map[string]struct{}{"model-down": {}}

	if !manualNeedsProbe(false, "model-healthy", nil) {
		t.Error("首轮应探测全部 manual 模型")
	}
	if !manualNeedsProbe(true, "Model-Down", unavailable) {
		t.Error("恢复轮应探测不可用模型（大小写不敏感）")
	}
	if manualNeedsProbe(true, "model-healthy", unavailable) {
		t.Error("恢复轮不应重探健康模型")
	}
	if manualNeedsProbe(true, "model-healthy", nil) {
		t.Error("空的不可用集合不应触发探测")
	}
}

// TestNextRecoverStreak 锁定"连续 N 次确认才恢复"的语义。
// 单轮目录同步 streak 只能到 1，这正是需要周期重跑的原因。
func TestNextRecoverStreak(t *testing.T) {
	cases := []struct {
		streak, confirmations int
		wantStreak            int
		wantReady             bool
	}{
		{0, 2, 1, false},
		{1, 2, 2, true},
		{0, 1, 1, true},
		{0, 0, 1, true}, // 非法配置按 1 处理
	}
	for _, c := range cases {
		streak, ready := nextRecoverStreak(c.streak, c.confirmations)
		if streak != c.wantStreak || ready != c.wantReady {
			t.Errorf("nextRecoverStreak(%d,%d) = (%d,%v), want (%d,%v)",
				c.streak, c.confirmations, streak, ready, c.wantStreak, c.wantReady)
		}
	}
}

// TestCatalogSyncNeeded 锁定恢复轮次的目标选择：
// 已探测过但仍有不可用端点的平台必须再次进入同步，否则恢复永远不可达。
func TestCatalogSyncNeeded(t *testing.T) {
	cases := []struct {
		probed      bool
		unavailable int
		want        bool
	}{
		{false, 0, true}, // 从未探测 → 首轮
		{true, 0, false}, // 已探测且健康 → 跳过
		{true, 1, true},  // 已探测但有不可用端点 → 恢复轮
		{true, 99, true}, // 多个待恢复
	}
	for _, c := range cases {
		if got := catalogSyncNeeded(c.probed, c.unavailable); got != c.want {
			t.Errorf("catalogSyncNeeded(%v,%d) = %v, want %v", c.probed, c.unavailable, got, c.want)
		}
	}
}
