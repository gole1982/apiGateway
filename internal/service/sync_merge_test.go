package service

import (
	"sort"
	"testing"
	"time"

	"gateway/internal/bundle"
)

func timeUnix(sec int64) time.Time       { return time.Unix(sec, 0) }
func timeUnixNs(sec, ns int64) time.Time { return time.Unix(sec, ns) }

// ---------------------------------------------------------------------------
// 并集合并：自然键取并集、同键冲突本地胜、不传播删除、输出顺序确定。
// ---------------------------------------------------------------------------

func sortedMergeKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func mkPlat(base, name string) bundle.Platform {
	return bundle.Platform{BaseURL: base, Name: name, Enabled: true}
}

func mkRAPI(base, model, alias string) bundle.RAPI {
	return bundle.RAPI{PlatformBaseURL: base, Model: model, Alias: alias, Enabled: true}
}

// 两边完全不相交 → 全保留，FromCenter / KeptLocal 记全，无冲突。
func TestMergeBundles_DisjointUnion(t *testing.T) {
	local := bundle.Bundle{Platforms: []bundle.Platform{mkPlat("https://a", "A")}}
	center := bundle.Bundle{Platforms: []bundle.Platform{mkPlat("https://b", "B")}}
	merged, sum := MergeBundles(local, center)
	if len(merged.Platforms) != 2 {
		t.Fatalf("platforms = %d, want 2", len(merged.Platforms))
	}
	// 顺序确定：本地在前、中心独有追加。
	if merged.Platforms[0].BaseURL != "https://a" || merged.Platforms[1].BaseURL != "https://b" {
		t.Errorf("order = %v, want [a b]", merged.Platforms)
	}
	if len(sum.Conflicts) != 0 {
		t.Errorf("conflicts = %v, want none", sum.Conflicts)
	}
	if sum.FromCenter["platform"] != 1 || sum.KeptLocal["platform"] != 1 {
		t.Errorf("summary = %+v, want from=1 kept=1", sum)
	}
	if !sum.AddsToCenter {
		t.Error("AddsToCenter must be true (local-only row exists)")
	}
}

// 同键内容不同 → 本地胜 + 记一条冲突 + 需要回推。
func TestMergeBundles_ConflictLocalWins(t *testing.T) {
	local := bundle.Bundle{Platforms: []bundle.Platform{mkPlat("https://a", "本地改名")}}
	center := bundle.Bundle{Platforms: []bundle.Platform{mkPlat("https://a", "中心旧名")}}
	merged, sum := MergeBundles(local, center)
	if len(merged.Platforms) != 1 || merged.Platforms[0].Name != "本地改名" {
		t.Fatalf("merged = %+v, want local row kept", merged.Platforms)
	}
	if len(sum.Conflicts) != 1 || sum.Conflicts[0].Table != "platform" {
		t.Fatalf("conflicts = %+v, want 1 platform conflict", sum.Conflicts)
	}
	if !sum.AddsToCenter {
		t.Error("AddsToCenter must be true on conflict")
	}
}

// 完全相同 → 无冲突、无需回推（避免空转 bump 中心版本）。
func TestMergeBundles_IdenticalNoPush(t *testing.T) {
	b := bundle.Bundle{
		Platforms: []bundle.Platform{mkPlat("https://a", "A")},
		RAPIs:     []bundle.RAPI{mkRAPI("https://a", "m", "别名")},
		LAPIs:     []bundle.LAPI{{Alias: "l", Enabled: true}},
	}
	merged, sum := MergeBundles(b, b)
	if len(merged.Platforms) != 1 || len(merged.RAPIs) != 1 || len(merged.LAPIs) != 1 {
		t.Fatalf("merged lost rows: %+v", merged)
	}
	if len(sum.Conflicts) != 0 || sum.AddsToCenter {
		t.Errorf("identical input must be conflict-free and push-free: %+v", sum)
	}
	if len(sum.FromCenter) != 0 || len(sum.KeptLocal) != 0 {
		t.Errorf("identical input must not count from/kept: %+v", sum)
	}
}

// 空本地 + 非空中心 = 纯拉取等价；空中心 + 非空本地 = 纯推送等价。
func TestMergeBundles_EmptySides(t *testing.T) {
	center := bundle.Bundle{Platforms: []bundle.Platform{mkPlat("https://b", "B")}}
	merged, sum := MergeBundles(bundle.Bundle{}, center)
	if len(merged.Platforms) != 1 || sum.FromCenter["platform"] != 1 {
		t.Errorf("empty local: %+v", sum)
	}
	if sum.AddsToCenter {
		t.Error("empty local must not push back")
	}
	local := bundle.Bundle{Platforms: []bundle.Platform{mkPlat("https://a", "A")}}
	merged2, sum2 := MergeBundles(local, bundle.Bundle{})
	if len(merged2.Platforms) != 1 || sum2.KeptLocal["platform"] != 1 || !sum2.AddsToCenter {
		t.Errorf("empty center: %+v", sum2)
	}
}

// 凭据/绑定/路由链按自然键取并集；绑定冲突（同端点同凭据、限额不同）本地胜。
func TestMergeBundles_BindingsAndOrders(t *testing.T) {
	local := bundle.Bundle{
		Bindings: []bundle.CredentialBinding{
			{PlatformBaseURL: "https://a", Model: "m", TokenHash: "h1", RPMLimit: 60, Enabled: true},
		},
		LAPIRapiOrder: []bundle.LAPIRapiOrder{
			{LAPIAlias: "l", RAPIPlatformBaseURL: "https://a", RAPIModel: "m", OrderIndex: 0},
		},
	}
	center := bundle.Bundle{
		Bindings: []bundle.CredentialBinding{
			{PlatformBaseURL: "https://a", Model: "m", TokenHash: "h1", RPMLimit: 10, Enabled: true},
			{PlatformBaseURL: "https://a", Model: "m", TokenHash: "h2", Enabled: true},
		},
		LAPIRapiOrder: []bundle.LAPIRapiOrder{
			{LAPIAlias: "l", RAPIPlatformBaseURL: "https://a", RAPIModel: "m", OrderIndex: 5},
		},
	}
	merged, sum := MergeBundles(local, center)
	if len(merged.Bindings) != 2 {
		t.Fatalf("bindings = %d, want 2 (h1 local-wins + h2 from center)", len(merged.Bindings))
	}
	var h1 *bundle.CredentialBinding
	for i := range merged.Bindings {
		if merged.Bindings[i].TokenHash == "h1" {
			h1 = &merged.Bindings[i]
		}
	}
	if h1 == nil || h1.RPMLimit != 60 {
		t.Errorf("h1 binding = %+v, want local RPMLimit=60", h1)
	}
	if len(merged.LAPIRapiOrder) != 1 || merged.LAPIRapiOrder[0].OrderIndex != 0 {
		t.Errorf("order = %+v, want local OrderIndex=0", merged.LAPIRapiOrder)
	}
	if len(sum.Conflicts) != 2 {
		t.Errorf("conflicts = %v, want 2 (binding + order)", sum.Conflicts)
	}
	if !sum.AddsToCenter {
		t.Error("AddsToCenter must be true")
	}
}

// 输入不被修改 + 重复合并幂等（输出再与任一边合并，行数与冲突数不变）。
func TestMergeBundles_PureAndIdempotent(t *testing.T) {
	local := bundle.Bundle{
		Platforms: []bundle.Platform{mkPlat("https://a", "本地")},
		RAPIs:     []bundle.RAPI{mkRAPI("https://a", "m1", "本地模型")},
	}
	center := bundle.Bundle{
		Platforms: []bundle.Platform{mkPlat("https://a", "中心"), mkPlat("https://b", "B")},
		RAPIs:     []bundle.RAPI{mkRAPI("https://a", "m1", "中心模型"), mkRAPI("https://b", "m2", "B模型")},
	}
	m1, _ := MergeBundles(local, center)
	if len(local.Platforms) != 1 || len(center.Platforms) != 2 {
		t.Fatal("inputs mutated")
	}
	m2, _ := MergeBundles(m1, center)
	m3, _ := MergeBundles(m1, local)
	if len(m2.Platforms) != 2 || len(m2.RAPIs) != 2 {
		t.Fatalf("re-merge with center changed result: %d platforms / %d rapis",
			len(m2.Platforms), len(m2.RAPIs))
	}
	if len(m3.Platforms) != 2 || len(m3.RAPIs) != 2 {
		t.Fatalf("re-merge with local changed result: %d platforms / %d rapis",
			len(m3.Platforms), len(m3.RAPIs))
	}
	// 冲突数稳定：m1 相对 center / local 仍是同样两处（平台 a、端点 (a,m1)）。
	_, s2 := MergeBundles(m1, center)
	if len(s2.Conflicts) != 2 {
		t.Errorf("conflicts = %d, want 2 (stable across re-merge)", len(s2.Conflicts))
	}
}

// LastTokenFetch 秒级相等视为不冲突（本地 DATETIME 与中心 JSON 回环的
// 亚秒级差异不该报冲突，否则每次合并都误报）。
func TestMergeBundles_TimestampTolerance(t *testing.T) {
	base := timeUnix(1_700_000_000)
	withNs := timeUnixNs(1_700_000_000, 500_000_000)
	local := bundle.Bundle{Platforms: []bundle.Platform{{BaseURL: "https://a", Name: "A", LastTokenFetch: &base}}}
	center := bundle.Bundle{Platforms: []bundle.Platform{{BaseURL: "https://a", Name: "A", LastTokenFetch: &withNs}}}
	_, sum := MergeBundles(local, center)
	if len(sum.Conflicts) != 0 {
		t.Errorf("sub-second timestamp diff must not be a conflict: %+v", sum.Conflicts)
	}
	// 跨秒才算冲突。
	later := timeUnix(1_700_000_060)
	center2 := bundle.Bundle{Platforms: []bundle.Platform{{BaseURL: "https://a", Name: "A", LastTokenFetch: &later}}}
	_, sum2 := MergeBundles(local, center2)
	if len(sum2.Conflicts) != 1 {
		t.Errorf("60s timestamp diff must be a conflict: %+v", sum2.Conflicts)
	}
}
