package service

import (
	"testing"

	"gateway/internal/models"
	"gateway/internal/notify"
)

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
			got, why := judgeRAPI([]string{model}, c.keys, nil)
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

	if got, _ := judgeRAPI([]string{"zai-org/GLM-5.3-Flash", "z.ai/glm-5.3-flash"}, keys, nil); got != judgmentRecover {
		t.Errorf("alias 命中应恢复, got %v", got)
	}
	if got, _ := judgeRAPI([]string{"zai-org/GLM-5.3-Flash", ""}, keys, nil); got != judgmentInvalidate {
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

// TestJudgeRAPIRespectsKeyBinding 回归：判定只能采信"服务该模型的 Key"的目录。
// 模型只绑 Key 1 时，Key 2 目录里有它不代表 Key 1 还能服务 —— 把 Key 2 算进
// pool 会让这种端点永远失效不掉（网关只会拿 Key 1 去请求）。
func TestJudgeRAPIRespectsKeyBinding(t *testing.T) {
	const model = "gpt-4o"
	servesKey1 := func(id int64) bool { return id == 1 }

	// Key 1（绑定）目录里没有，Key 2（未绑定）目录里有 → 必须失效。
	keys := []keyProbeState{
		{ID: 1, Enabled: true, Catalog: []string{"claude-sonnet"}},
		{ID: 2, Enabled: true, Catalog: []string{"gpt-4o"}},
	}
	if got, _ := judgeRAPI([]string{model}, keys, servesKey1); got != judgmentInvalidate {
		t.Errorf("未绑定 Key 的目录不应救活模型, got %v", got)
	}

	// 绑定 Key 自己目录里有 → 恢复。
	keys[0].Catalog = []string{"gpt-4o"}
	if got, _ := judgeRAPI([]string{model}, keys, servesKey1); got != judgmentRecover {
		t.Errorf("绑定 Key 命中应恢复, got %v", got)
	}

	// 未绑定 Key 探测失败也不该把判定拖成"保持"。
	keys[0].Catalog = []string{"claude-sonnet"}
	keys[1].Failed = true
	if got, _ := judgeRAPI([]string{model}, keys, servesKey1); got != judgmentInvalidate {
		t.Errorf("未绑定 Key 的失败不应阻塞失效判定, got %v", got)
	}
}

// TestSplitCatalogChanges 汇总通知的分组口径。
func TestSplitCatalogChanges(t *testing.T) {
	changes := []catalogChange{
		{Model: "a", Action: judgmentInvalidate},
		{Model: "b", Action: judgmentRecover},
		{Model: "c", Action: judgmentInvalidate},
		{Model: "d", Action: judgmentHold}, // 不该出现在任何一组
	}
	invalidated, recovered := splitCatalogChanges(changes)
	if len(invalidated) != 2 || invalidated[0] != "a" || invalidated[1] != "c" {
		t.Errorf("invalidated = %v, want [a c]", invalidated)
	}
	if len(recovered) != 1 || recovered[0] != "b" {
		t.Errorf("recovered = %v, want [b]", recovered)
	}
}

// TestJoinModelNames 通知详情按 rune 截断，不能把多字节字符切坏。
func TestJoinModelNames(t *testing.T) {
	if got := joinModelNames([]string{"a", "b"}, 10); got != "a、b" {
		t.Errorf("未超长不应截断, got %q", got)
	}
	if got := joinModelNames([]string{"模型一", "模型二", "模型三"}, 4); got != "模型一、…" {
		t.Errorf("截断结果 = %q, want %q", got, "模型一、…")
	}
	if got := joinModelNames(nil, 10); got != "" {
		t.Errorf("空列表应为空串, got %q", got)
	}
}

// TestNotifyCatalogChangesAggregates 一个平台一轮只发一条失效通知 + 一条恢复通知，
// 而不是逐模型刷屏。用真实的 NotificationService 验证未读条目，不 mock。
func TestNotifyCatalogChangesAggregates(t *testing.T) {
	prev := notifySvc
	notifySvc = notify.NewNotificationService()
	defer func() { notifySvc = prev }()

	plat := models.Platform{ID: 7, Name: "测试平台"}
	notifyCatalogChanges(plat, []catalogChange{
		{Model: "m1", Action: judgmentInvalidate},
		{Model: "m2", Action: judgmentInvalidate},
		{Model: "m3", Action: judgmentRecover},
	})

	items := notifySvc.UnreadItems()
	if len(items) != 2 {
		t.Fatalf("want 2 条未读（失效+恢复各一条），got %d: %+v", len(items), items)
	}
	byKind := map[string]notify.UnreadItem{}
	for _, it := range items {
		byKind[it.Kind] = it
	}
	failure, ok := byKind["failure"]
	if !ok {
		t.Fatal("缺少失效通知")
	}
	if failure.Menu != notify.MenuModels {
		t.Errorf("失效通知应落在模型菜单, got %q", failure.Menu)
	}
	// 汇总通知没有单一模型可定位：entity_id 必须为 0，否则面板会拿平台 id
	// 当 rapi.id 回查并误命中同号模型。
	if failure.EntityID != 0 {
		t.Errorf("汇总通知不应绑定实体, got entity_id=%d", failure.EntityID)
	}
	if !containsSubstr(failure.Title, "2 个模型") || !containsSubstr(failure.Detail, "m1、m2") {
		t.Errorf("失效通知内容不对: title=%q detail=%q", failure.Title, failure.Detail)
	}
	if _, ok := byKind["update"]; !ok {
		t.Error("缺少恢复通知")
	}

	// 空变更不得产生任何通知（每 30 分钟的恢复轮会大量走到这里）。
	notifyCatalogChanges(plat, nil)
	if got := len(notifySvc.UnreadItems()); got != 2 {
		t.Errorf("无变更时不应新增通知, got %d", got)
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
