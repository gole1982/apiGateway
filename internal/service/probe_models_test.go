package service

import (
	"testing"

	"gateway/internal/models"
)

// TestKeyServesModelNow 锁定"现状判定"的每一条分支。它必须与网关的
// filterKeysByKeyIDs 逐字一致 —— 基准不一致，整个偏差报告就失去意义。
func TestKeyServesModelNow(t *testing.T) {
	const k1, k2 int64 = 1, 2
	cases := []struct {
		name   string
		keyIDs string
		probe  int64
		want   bool
		why    string
	}{
		{"空串=全部Key", "", k2, true, "旧默认语义"},
		{"纯空白=全部Key", "   ", k2, true, "TrimSpace 后同空串"},
		{"白名单含自己", "1,2", k2, true, ""},
		{"白名单不含", "1", k2, false, ""},
		{"带空格的白名单", "1, 2", k2, true, "parseID 会 TrimSpace"},
		{"全非法片段=不限制", "abc,def", k2, true, "对应 filterKeysByKeyIDs 的 len(allowed)==0 分支"},
		{"悬空id被丢弃后为空", "999", k1, false, "999 解析成功但不含 k1"},
		{"混入非法片段仍取合法项", "xyz,2", k2, true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := keyServesModelNow(c.probe, c.keyIDs); got != c.want {
				t.Errorf("keyServesModelNow(%d, %q) = %v, want %v (%s)",
					c.probe, c.keyIDs, got, c.want, c.why)
			}
		})
	}
}

// TestDiffKeyAuthorization 用用户实际遇到的场景驱动：
// DAHL 平台上 Gom 有 z.ai/glm-5.3-flash 的权限，A9j 没有；deepseek 两把都有。
func TestDiffKeyAuthorization(t *testing.T) {
	rapis := []models.RAPIWithPlatform{
		{ID: 1, Model: "z.ai/glm-5.3-flash", KeyIDs: "1"},                // 只绑 Gom
		{ID: 2, Model: "deepseek-ai/deepseek-v4-flash-0731", KeyIDs: ""}, // 全部 Key
	}
	const gom, a9j int64 = 1, 2

	t.Run("Gom：现状即真相", func(t *testing.T) {
		catalog := []string{"z.ai/glm-5.3-flash", "deepseek-ai/deepseek-v4-flash-0731"}
		over, under, agreed := diffKeyAuthorization(gom, catalog, rapis)
		if len(over) != 0 {
			t.Errorf("Gom 过度授权 = %v, want 空（Gom 确有该模型权限）", over)
		}
		if len(under) != 0 {
			t.Errorf("Gom 授权不足 = %v, want 空", under)
		}
		if len(agreed) != 2 {
			t.Errorf("Gom 一致 = %v, want 2 个", agreed)
		}
	})

	t.Run("A9j：若真相也缺 glm，则现状与真相一致（不报偏差）", func(t *testing.T) {
		catalog := []string{"deepseek-ai/deepseek-v4-flash-0731"} // A9j 探测不到 glm
		over, under, agreed := diffKeyAuthorization(a9j, catalog, rapis)

		// glm 被白名单绑在 Gom 身上（key_ids="1"），A9j 现状就不服务它；
		// A9j 的目录里也没有它 —— 两者一致，不该报任何偏差。
		if len(over) != 0 || len(under) != 0 {
			t.Errorf("不该有偏差: over=%v under=%v", over, under)
		}
		if len(agreed) != 1 || agreed[0] != "deepseek-ai/deepseek-v4-flash-0731" {
			t.Errorf("一致 = %v, want 只有 deepseek", agreed)
		}
	})

	t.Run("A9j：真相有 glm 但现状不服务 = 授权不足（用户原始困惑）", func(t *testing.T) {
		// 同一份配置，只把 A9j 的探测目录换成含 glm 的版本：
		// 说明白名单把一把**本来有权限**的 Key 挡在了外面。
		catalog := []string{"z.ai/glm-5.3-flash", "deepseek-ai/deepseek-v4-flash-0731"}
		over, under, agreed := diffKeyAuthorization(a9j, catalog, rapis)

		if len(over) != 0 {
			t.Errorf("过度授权 = %v, want 空", over)
		}
		if len(under) != 1 || under[0] != "z.ai/glm-5.3-flash" {
			t.Errorf("授权不足 = %v, want [z.ai/glm-5.3-flash]（A9j 有权限却被白名单挡住）", under)
		}
		if len(agreed) != 1 || agreed[0] != "deepseek-ai/deepseek-v4-flash-0731" {
			t.Errorf("一致 = %v, want 只有 deepseek", agreed)
		}
	})

	t.Run("Gom：白名单说服务但目录里没有 = 过度授权（线上会 403）", func(t *testing.T) {
		// glm 的白名单写着 Gom，但 Gom 探测不到它 —— 现状在骗网关。
		catalog := []string{"deepseek-ai/deepseek-v4-flash-0731"}
		over, under, _ := diffKeyAuthorization(gom, catalog, rapis)
		if len(over) != 1 || over[0] != "z.ai/glm-5.3-flash" {
			t.Errorf("过度授权 = %v, want [z.ai/glm-5.3-flash]", over)
		}
		if len(under) != 0 {
			t.Errorf("授权不足 = %v, want 空", under)
		}
	})

	t.Run("授权不足：真相有但现状不服务", func(t *testing.T) {
		// 白名单绑在 Gom（id=1）身上，A9j（id=2）现状不服务；
		// 而 A9j 的目录里确实有它 —— 反向偏差。
		rs := []models.RAPIWithPlatform{{ID: 1, Model: "m-only-9j", KeyIDs: "1"}}
		catalog := []string{"m-only-9j"}
		over, under, _ := diffKeyAuthorization(a9j, catalog, rs)
		if len(over) != 0 {
			t.Errorf("过度授权 = %v, want 空", over)
		}
		if len(under) != 1 || under[0] != "m-only-9j" {
			t.Errorf("授权不足 = %v, want [m-only-9j]", under)
		}
	})
}

// TestDiffCaseInsensitive 上游各站模型名大小写不一致，dashboard 也按小写做
// 存在性判定 —— 探测匹配必须同样宽松，否则会误报成过度授权。
func TestDiffCaseInsensitive(t *testing.T) {
	rapis := []models.RAPIWithPlatform{{ID: 1, Model: "GPT-4o", KeyIDs: ""}}
	catalog := []string{"gpt-4o", "  GPT-4O  "} // 含首尾空白
	over, under, agreed := diffKeyAuthorization(1, catalog, rapis)
	if len(over) != 0 || len(under) != 0 {
		t.Errorf("大小写/空白差异被误判成偏差: over=%v under=%v", over, under)
	}
	if len(agreed) != 1 {
		t.Errorf("一致 = %v, want 1 个", agreed)
	}
}

// TestDiffIgnoresModelsAbsentFromBoth 两处都没有的模型不产生任何输出 ——
// 报告里出现的每一条都是需要人决策的偏差，噪音会淹没真问题。
// 注意"两处都没有"要求现状判定为**不服务**：key_ids 非空且不含该 Key
// （如 "999"），且探测目录里也没有它。
func TestDiffIgnoresModelsAbsentFromBoth(t *testing.T) {
	rapis := []models.RAPIWithPlatform{
		{ID: 1, Model: "served", KeyIDs: ""},
		{ID: 2, Model: "whitelisted-to-someone-else", KeyIDs: "999"},
	}
	over, under, agreed := diffKeyAuthorization(1, []string{"served"}, rapis)
	if len(over) != 0 || len(under) != 0 {
		t.Errorf("不该有偏差: over=%v under=%v", over, under)
	}
	if len(agreed) != 1 || agreed[0] != "served" {
		t.Errorf("一致 = %v, want [served]", agreed)
	}
}

// TestDiffMatchesAliasToo 线上真实存在 model=zai-org/GLM-5.3-Flash 而
// alias=z.ai/glm-5.3-flash 的情况（两者不是大小写关系）。若只比 Model，
// 上游返回正确模型时会被误判成"过度授权" —— 那是最危险的误报方向。
func TestDiffMatchesAliasToo(t *testing.T) {
	rapis := []models.RAPIWithPlatform{{
		ID: 1, Model: "zai-org/GLM-5.3-Flash", Alias: "z.ai/glm-5.3-flash", KeyIDs: "1",
	}}

	t.Run("上游返回 model 原名 → 不该误报", func(t *testing.T) {
		over, under, agreed := diffKeyAuthorization(1, []string{"zai-org/GLM-5.3-Flash"}, rapis)
		if len(over) != 0 || len(under) != 0 || len(agreed) != 1 {
			t.Errorf("over=%v under=%v agreed=%v, want 无偏差且 agreed=1", over, under, agreed)
		}
	})

	t.Run("上游只返回 alias 形式 → 同样不该误报", func(t *testing.T) {
		over, under, agreed := diffKeyAuthorization(1, []string{"z.ai/glm-5.3-flash"}, rapis)
		if len(over) != 0 || len(under) != 0 || len(agreed) != 1 {
			t.Errorf("over=%v under=%v agreed=%v, want 无偏差且 agreed=1", over, under, agreed)
		}
	})

	t.Run("两种形式都没有 → 确实是过度授权", func(t *testing.T) {
		over, _, _ := diffKeyAuthorization(1, []string{"deepseek-ai/DeepSeek-V4"}, rapis)
		if len(over) != 1 {
			t.Errorf("over=%v, want 报出过度授权", over)
		}
	})
}

// TestDiffRealDAHLData 用真实探测数据锚定判定结果（2026-10-15 实测）：
//
//	DAHL https://inference.dahl.global/v1，两把 Key 的 /v1/models 目录**完全相同**：
//	    MiniMaxAI/MiniMax-M2.7
//	    deepseek-ai/DeepSeek-V4-Flash-0731
//	    zai-org/GLM-5.3-Flash
//	现状：GLM 的 key_ids='72'（只绑一把），DeepSeek 的 key_ids=''（全部）
//
// 结论：该平台不存在权益分档，key_ids='72' 是一份把**有权限的 Key 挡在外面**
// 的过时白名单 —— 授权不足，正是用户最初报的"K2 为空"。
func TestDiffRealDAHLData(t *testing.T) {
	const k72, k74 int64 = 72, 74
	catalog72 := []string{
		"MiniMaxAI/MiniMax-M2.7",
		"deepseek-ai/DeepSeek-V4-Flash-0731",
		"zai-org/GLM-5.3-Flash",
	}
	// 实测两把目录逐项相同
	catalog74 := append([]string(nil), catalog72...)

	rapis := []models.RAPIWithPlatform{
		{ID: 155, Model: "zai-org/GLM-5.3-Flash", Alias: "z.ai/glm-5.3-flash", KeyIDs: "72"},
		{ID: 156, Model: "deepseek-ai/DeepSeek-V4-Flash-0731", Alias: "deepseek-ai/deepseek-v4-flash-0731", KeyIDs: ""},
	}

	t.Run("Key 72：现状与真相完全一致", func(t *testing.T) {
		over, under, agreed := diffKeyAuthorization(k72, catalog72, rapis)
		if len(over) != 0 || len(under) != 0 {
			t.Errorf("不该有偏差: over=%v under=%v", over, under)
		}
		if len(agreed) != 2 {
			t.Errorf("agreed = %v, want 2 个", agreed)
		}
	})

	t.Run("Key 74：GLM 授权不足，DeepSeek 一致", func(t *testing.T) {
		over, under, agreed := diffKeyAuthorization(k74, catalog74, rapis)

		if len(over) != 0 {
			t.Errorf("过度授权 = %v, want 空（Key 74 目录与 72 完全相同，不会有过度授权）", over)
		}
		if len(under) != 1 || under[0] != "zai-org/GLM-5.3-Flash" {
			t.Errorf("授权不足 = %v, want [zai-org/GLM-5.3-Flash] —— key_ids='72' 挡住了有权限的 Key 74", under)
		}
		if len(agreed) != 1 || agreed[0] != "deepseek-ai/DeepSeek-V4-Flash-0731" {
			t.Errorf("一致 = %v, want 只有 DeepSeek", agreed)
		}
	})

	t.Run("切量闸门：只有授权不足 → 不阻断", func(t *testing.T) {
		rep := probePlatformReport{OverTotal: 0, UnderTotal: 1}
		if !rep.cutoverSafe() {
			t.Error("只有授权不足时不应阻断切量（那是浪费容量，不是故障）")
		}
	})
}

// TestCutoverSafe healthy 是切量闸门，不能因为"没查出问题"就放行 ——
// 必须同时排除"没探测"和"探测失败"这两种无知状态。
func TestCutoverSafe(t *testing.T) {
	cases := []struct {
		name string
		rep  probePlatformReport
		want bool
	}{
		{
			"全部探测成功且无过度授权",
			probePlatformReport{}, true,
		},
		{
			"有过度授权 —— 阻断",
			probePlatformReport{OverTotal: 1}, false,
		},
		{
			"一把都没探测 —— 无知而非无害，必须阻断",
			probePlatformReport{UnprobedKeys: 2}, false,
		},
		{
			"有探测失败 —— 无知而非无害，必须阻断",
			probePlatformReport{FailedKeys: 1}, false,
		},
		{
			"只有授权不足 —— 不阻断（只是浪费容量，不是故障）",
			probePlatformReport{UnderTotal: 5}, true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.rep.cutoverSafe(); got != c.want {
				t.Errorf("cutoverSafe() = %v, want %v (over=%d unprobed=%d failed=%d under=%d)",
					got, c.want, c.rep.OverTotal, c.rep.UnprobedKeys, c.rep.FailedKeys, c.rep.UnderTotal)
			}
		})
	}
}

// TestDiffEmptyCatalog 一把 Key 探测到空目录 ≠ 它什么都访问不了，而是上游没给
// 目录（聚合站的常见情况）。此时全部已定义模型都会被算成过度授权，报告里的
// over_granted_total 会很大 —— 调用方必须能区分这两种情况，因此 catalog_size
// 与 probed 标志要一起看。
func TestDiffEmptyCatalog(t *testing.T) {
	rapis := []models.RAPIWithPlatform{{ID: 1, Model: "m1", KeyIDs: ""}}
	over, _, _ := diffKeyAuthorization(1, []string{}, rapis)
	if len(over) != 1 {
		t.Errorf("空目录应全部算过度授权, got %v", over)
	}
}
