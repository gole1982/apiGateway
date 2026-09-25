package bundle

import (
	"reflect"
	"testing"
)

// plat / key / rapi / lapi 构造助手，字段少写其余留零值。
func p(name string) Platform { return Platform{Name: name, BaseURL: "https://" + name} }

func pk(plat string, idx int) PlatformKey {
	return PlatformKey{PlatformName: plat, KeyIndex: idx, Label: "k"}
}

func rp(plat, alias, keys string) RAPI {
	return RAPI{PlatformName: plat, Alias: alias, Model: alias, KeyIDs: keys}
}

func lp(alias string) LAPI { return LAPI{Alias: alias} }

func ord(lapi, plat, alias string, i int) LAPIRapiOrder {
	return LAPIRapiOrder{LAPIAlias: lapi, RAPIPlatformName: plat, RAPIAlias: alias, OrderIndex: i}
}

func findPlat(bs *Bundle, name string) *Platform {
	for i := range bs.Platforms {
		if bs.Platforms[i].Name == name {
			return &bs.Platforms[i]
		}
	}
	return nil
}

func findRAPI(bs *Bundle, k RAPIKey) *RAPI {
	for i := range bs.RAPIs {
		if bs.RAPIs[i].PlatformName == k.PlatformName && bs.RAPIs[i].Alias == k.Alias {
			return &bs.RAPIs[i]
		}
	}
	return nil
}

// 基础：两端完全相同 → 无冲突、无新增、无裁剪。
func TestMerge_IdenticalIsNoop(t *testing.T) {
	c := &Bundle{
		Platforms:    []Platform{p("A")},
		PlatformKeys: []PlatformKey{pk("A", 0), pk("A", 1)},
		RAPIs:        []RAPI{rp("A", "m1", "0")},
		LAPIs:        []LAPI{lp("chat")},
		LAPIRapiOrder: []LAPIRapiOrder{
			ord("chat", "A", "m1", 0),
		},
	}
	l := &Bundle{
		Platforms:    []Platform{p("A")},
		PlatformKeys: []PlatformKey{pk("A", 0), pk("A", 1)},
		RAPIs:        []RAPI{rp("A", "m1", "0")},
		LAPIs:        []LAPI{lp("chat")},
		LAPIRapiOrder: []LAPIRapiOrder{
			ord("chat", "A", "m1", 0),
		},
	}
	got, rep, err := Merge(c, l, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if !rep.Empty() {
		t.Fatalf("expected empty report, got %+v", rep)
	}
	if len(got.Platforms) != 1 || len(got.PlatformKeys) != 2 || len(got.RAPIs) != 1 ||
		len(got.LAPIs) != 1 || len(got.LAPIRapiOrder) != 1 {
		t.Fatalf("union changed cardinality: %+v", got)
	}
}

// 冲突规则：中心优先保留中心值；本地优先保留本地值；两者都要记录差异字段。
func TestMerge_ConflictRule(t *testing.T) {
	mk := func(bu string) *Bundle {
		return &Bundle{
			Platforms:    []Platform{{Name: "A", BaseURL: bu}},
			PlatformKeys: []PlatformKey{pk("A", 0)},
			RAPIs:        []RAPI{rp("A", "m1", "")},
			LAPIs:        []LAPI{lp("chat")},
		}
	}
	center, local := mk("https://center"), mk("https://local")

	got, rep, err := Merge(center, local, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if v := findPlat(got, "A").BaseURL; v != "https://center" {
		t.Errorf("center_wins: BaseURL = %q, want center", v)
	}
	if len(rep.Conflicts) != 1 {
		t.Fatalf("want 1 conflict, got %+v", rep.Conflicts)
	}
	if c := rep.Conflicts[0]; c.Kind != "platform" || c.Key != "A" ||
		!reflect.DeepEqual(c.Fields, []string{"BaseURL"}) {
		t.Errorf("conflict detail wrong: %+v", c)
	}

	got2, rep2, err := Merge(center, local, RuleLocalWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if v := findPlat(got2, "A").BaseURL; v != "https://local" {
		t.Errorf("local_wins: BaseURL = %q, want local", v)
	}
	if len(rep2.Conflicts) != 1 {
		t.Errorf("local_wins should still report the conflict, got %+v", rep2.Conflicts)
	}
}

// 只在本地存在的行要全部收进结果并登记为"待补推"。
func TestMerge_LocalOnlyRowsAreKept(t *testing.T) {
	center := &Bundle{
		Platforms:    []Platform{p("A")},
		PlatformKeys: []PlatformKey{pk("A", 0)},
		RAPIs:        []RAPI{rp("A", "m1", "")},
		LAPIs:        []LAPI{lp("chat")},
	}
	local := &Bundle{
		Platforms:    []Platform{p("A"), p("B")},
		PlatformKeys: []PlatformKey{pk("A", 0), pk("B", 0)},
		RAPIs:        []RAPI{rp("A", "m1", ""), rp("B", "m2", "0")},
		LAPIs:        []LAPI{lp("chat"), lp("code")},
		LAPIRapiOrder: []LAPIRapiOrder{
			ord("code", "B", "m2", 0),
		},
	}
	got, rep, err := Merge(center, local, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if findPlat(got, "B") == nil {
		t.Error("local-only platform B must survive the union")
	}
	if findRAPI(got, RAPIKey{"B", "m2"}) == nil {
		t.Error("local-only rapi (B,m2) must survive the union")
	}
	if len(got.LAPIs) != 2 {
		t.Errorf("want 2 lapis, got %d", len(got.LAPIs))
	}
	if len(got.LAPIRapiOrder) != 1 || got.LAPIRapiOrder[0].LAPIAlias != "code" {
		t.Errorf("local-only route chain missing: %+v", got.LAPIRapiOrder)
	}
	if !reflect.DeepEqual(rep.LocalOnlyPlatforms, []string{"B"}) {
		t.Errorf("LocalOnlyPlatforms = %v", rep.LocalOnlyPlatforms)
	}
	if !reflect.DeepEqual(rep.LocalOnlyRAPIs, []RAPIKey{{"B", "m2"}}) {
		t.Errorf("LocalOnlyRAPIs = %v", rep.LocalOnlyRAPIs)
	}
	if !reflect.DeepEqual(rep.LocalOnlyKeys, []PKRef{{"B", 0}}) {
		t.Errorf("LocalOnlyKeys = %v", rep.LocalOnlyKeys)
	}
	if !reflect.DeepEqual(rep.LocalOnlyLAPIs, []string{"code"}) {
		t.Errorf("LocalOnlyLAPIs = %v", rep.LocalOnlyLAPIs)
	}
	if !reflect.DeepEqual(rep.LocalOnlyOrderLAPI, []string{"code"}) {
		t.Errorf("LocalOnlyOrderLAPI = %v", rep.LocalOnlyOrderLAPI)
	}
}

// 关键约束：同一 lapi 的链绝不两侧交错（落库有 UNIQUE(lapi_id, order_index)，
// 交错会产生重复 order_index，ApplyBundle 必失败）。
func TestMerge_OrderChainNeverInterleaves(t *testing.T) {
	center := &Bundle{
		Platforms: []Platform{p("A"), p("B")},
		RAPIs:     []RAPI{rp("A", "m1", ""), rp("B", "m2", "")},
		LAPIs:     []LAPI{lp("chat")},
		LAPIRapiOrder: []LAPIRapiOrder{
			ord("chat", "A", "m1", 0),
			ord("chat", "B", "m2", 1),
		},
	}
	local := &Bundle{
		Platforms: []Platform{p("A"), p("B")},
		RAPIs:     []RAPI{rp("A", "m1", ""), rp("B", "m2", "")},
		LAPIs:     []LAPI{lp("chat")},
		LAPIRapiOrder: []LAPIRapiOrder{
			ord("chat", "A", "m1", 0),
		},
	}
	for _, rule := range []ConflictRule{RuleCenterWins, RuleLocalWins} {
		got, _, err := Merge(center, local, rule)
		if err != nil {
			t.Fatalf("merge(%v): %v", rule, err)
		}
		if len(got.LAPIRapiOrder) != 2 {
			t.Fatalf("rule %v: want the center's full 2-step chain, got %+v", rule, got.LAPIRapiOrder)
		}
		seen := map[int]bool{}
		for _, o := range got.LAPIRapiOrder {
			if seen[o.OrderIndex] {
				t.Fatalf("rule %v: duplicate order_index %d — would violate UNIQUE(lapi_id, order_index)", rule, o.OrderIndex)
			}
			seen[o.OrderIndex] = true
		}
	}
}

// 悬空引用必须裁掉，否则 Validate 会拒、结果无法落库。
func TestMerge_PrunesDanglingRefs(t *testing.T) {
	center := &Bundle{
		Platforms:    []Platform{p("A")},
		PlatformKeys: []PlatformKey{pk("A", 0)},
		RAPIs:        []RAPI{rp("A", "ok", "0")},
		LAPIs:        []LAPI{lp("chat")},
		LAPIRapiOrder: []LAPIRapiOrder{
			ord("chat", "A", "ok", 0),
			ord("chat", "A", "ghost", 1), // 指向不存在的 rapi
		},
	}
	local := &Bundle{
		Platforms: []Platform{p("A")},
		// 本地有个孤儿平台 Z（两端都没有）→ 其下的 rapi/密钥应被裁掉
		PlatformKeys: []PlatformKey{pk("A", 0), pk("Z", 0)},
		RAPIs: []RAPI{
			rp("A", "ok", "0"),
			rp("A", "badkeys", "7"), // key_index 7 不存在
			rp("Z", "orphan", ""),   // 平台 Z 不在合并结果里
		},
		LAPIs: []LAPI{lp("chat")},
	}
	got, rep, err := Merge(center, local, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if findRAPI(got, RAPIKey{"A", "ok"}) == nil {
		t.Error("valid rapi dropped")
	}
	if findRAPI(got, RAPIKey{"A", "badkeys"}) != nil {
		t.Error("rapi with unresolvable key_ids must be pruned")
	}
	if findRAPI(got, RAPIKey{"Z", "orphan"}) != nil {
		t.Error("rapi under a non-merged platform must be pruned")
	}
	if len(got.PlatformKeys) != 1 {
		t.Errorf("key under non-merged platform must be pruned, got %+v", got.PlatformKeys)
	}
	if len(got.LAPIRapiOrder) != 1 || got.LAPIRapiOrder[0].RAPIAlias != "ok" {
		t.Errorf("route pointing at a pruned rapi must be dropped: %+v", got.LAPIRapiOrder)
	}
	if len(rep.DroppedRAPIs) != 2 {
		t.Errorf("DroppedRAPIs = %v, want 2", rep.DroppedRAPIs)
	}
	if len(rep.DroppedKeys) != 1 {
		t.Errorf("DroppedKeys = %v, want 1", rep.DroppedKeys)
	}
	if len(rep.DroppedOrder) != 1 {
		t.Errorf("DroppedOrder = %v, want 1", rep.DroppedOrder)
	}
}

// 合并语义下不产生删除：只在中心的行即使本地没有也必须保留。
func TestMerge_NeverDeletesCenterOnlyRows(t *testing.T) {
	center := &Bundle{
		Platforms:    []Platform{p("A"), p("B")},
		PlatformKeys: []PlatformKey{pk("A", 0), pk("B", 0)},
		RAPIs:        []RAPI{rp("A", "m1", ""), rp("B", "m2", "")},
		LAPIs:        []LAPI{lp("chat"), lp("code")},
	}
	local := &Bundle{Platforms: []Platform{p("A")}} // 本地几乎空白
	got, _, err := Merge(center, local, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(got.Platforms) != 2 || len(got.PlatformKeys) != 2 ||
		len(got.RAPIs) != 2 || len(got.LAPIs) != 2 {
		t.Fatalf("merge must not delete center-only rows, got %+v", got)
	}
}

// 幂等：把结果再与自身合并应是不动点（无新增/无冲突/无裁剪）。
func TestMerge_Idempotent(t *testing.T) {
	local := &Bundle{
		Platforms:    []Platform{p("A"), p("B")},
		PlatformKeys: []PlatformKey{pk("A", 0), pk("B", 0)},
		RAPIs:        []RAPI{rp("A", "m1", "0"), rp("B", "m2", "")},
		LAPIs:        []LAPI{lp("chat")},
		LAPIRapiOrder: []LAPIRapiOrder{
			ord("chat", "A", "m1", 0),
		},
	}
	first, _, err := Merge(&Bundle{}, local, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	second, rep, err := Merge(first, first, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if !rep.Empty() {
		t.Errorf("self-merge should be a no-op, got %+v", rep)
	}
	if !reflect.DeepEqual(first, second) {
		t.Errorf("merge is not idempotent:\n first =%+v\n second=%+v", first, second)
	}
}

// 两侧皆空 / 一侧为 nil 都不能 panic。
func TestMerge_NilAndEmpty(t *testing.T) {
	if _, _, err := Merge(nil, nil, RuleCenterWins); err == nil {
		t.Error("expected error when both sides are nil")
	}
	got, _, err := Merge(nil, &Bundle{Platforms: []Platform{p("A")}}, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge(nil, local): %v", err)
	}
	if len(got.Platforms) != 1 {
		t.Errorf("local-only rows lost when center is nil: %+v", got)
	}
	if _, _, err := Merge(&Bundle{}, &Bundle{}, RuleCenterWins); err != nil {
		t.Errorf("empty+empty should not error: %v", err)
	}
}
