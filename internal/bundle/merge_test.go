package bundle

import (
	"reflect"
	"testing"
)

// 构造助手（v2 自然键）
func pf(name, baseURL string) Platform { return Platform{Name: name, BaseURL: baseURL} }

func cf(hash, label string) Credential { return Credential{TokenHash: hash, Label: label} }

func rf(baseURL, model, alias string) RAPI {
	return RAPI{PlatformBaseURL: baseURL, Model: model, Alias: alias}
}

func lf(alias string) LAPI { return LAPI{Alias: alias} }

func of(lapi, baseURL, model string, i int) LAPIRapiOrder {
	return LAPIRapiOrder{LAPIAlias: lapi, RAPIPlatformBaseURL: baseURL, RAPIModel: model, OrderIndex: i}
}

func bf(baseURL, model, hash string) CredentialBinding {
	return CredentialBinding{PlatformBaseURL: baseURL, Model: model, TokenHash: hash, Enabled: true}
}

func findPlat(bs *Bundle, baseURL string) *Platform {
	for i := range bs.Platforms {
		if platformKey(bs.Platforms[i].BaseURL) == platformKey(baseURL) {
			return &bs.Platforms[i]
		}
	}
	return nil
}

func findRAPI(bs *Bundle, e EndpointRef) *RAPI {
	for i := range bs.RAPIs {
		if platformKey(bs.RAPIs[i].PlatformBaseURL) == e.BaseURL && bs.RAPIs[i].Model == e.Model {
			return &bs.RAPIs[i]
		}
	}
	return nil
}

func chainOf(bs *Bundle, alias string) []string {
	var out []string
	for _, o := range bs.LAPIRapiOrder {
		if o.LAPIAlias == alias {
			out = append(out, platformKey(o.RAPIPlatformBaseURL)+"#"+o.RAPIModel)
		}
	}
	return out
}

// 完全一致 → 无冲突、无新增、无裁剪。
func TestMerge2_IdenticalIsNoop(t *testing.T) {
	b := func() *Bundle {
		return &Bundle{
			Platforms:   []Platform{pf("A", "https://a.example.com")},
			Credentials: []Credential{cf("h1", "k1")},
			RAPIs:       []RAPI{rf("https://a.example.com", "m1", "a-m1")},
			LAPIs:       []LAPI{lf("chat")},
			Bindings:    []CredentialBinding{bf("https://a.example.com", "m1", "h1")},
			LAPIRapiOrder: []LAPIRapiOrder{
				of("chat", "https://a.example.com", "m1", 0),
			},
		}
	}
	got, rep, err := Merge(b(), b(), RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if !rep.Empty() {
		t.Fatalf("expected empty report, got %+v", rep)
	}
	if !reflect.DeepEqual(got, b()) {
		t.Errorf("union changed content:\n got=%+v\nwant=%+v", got, b())
	}
}

// 键语义：base_url **原样**作键，刻意不归一化——镜像 idx_platform_base_url
// 建在原始列上的事实。大小写/尾斜杠不同的两个平台，库里是两行，契约也必须当两行。
func TestMerge2_BaseURLIsExactKeyNotNormalized(t *testing.T) {
	center := &Bundle{
		Platforms:   []Platform{pf("A", "https://A.Example.com/")},
		Credentials: []Credential{cf("h1", "k1")},
		RAPIs:       []RAPI{rf("https://A.Example.com/", "m1", "x")},
	}
	local := &Bundle{
		Platforms:   []Platform{pf("B", "https://a.example.com")},
		Credentials: []Credential{cf("h2", "k2")},
		RAPIs:       []RAPI{rf("https://a.example.com", "m1", "y")},
	}
	got, rep, err := Merge(center, local, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(got.Platforms) != 2 {
		t.Fatalf("trailing-slash variant must stay a separate platform (DB keeps them apart), got %d: %+v", len(got.Platforms), got.Platforms)
	}
	if !reflect.DeepEqual(rep.LocalOnlyPlatforms, []string{"https://a.example.com"}) {
		t.Errorf("LocalOnlyPlatforms = %v", rep.LocalOnlyPlatforms)
	}
	if len(rep.Conflicts) != 0 {
		t.Errorf("different keys are not conflicts, got %+v", rep.Conflicts)
	}
}

// 冲突规则：同键但非键字段不同 → 中心优先 / 本地优先。
func TestMerge2_ConflictRule(t *testing.T) {
	mk := func(name string) *Bundle {
		return &Bundle{
			Platforms:   []Platform{{Name: name, BaseURL: "https://x.example.com"}},
			Credentials: []Credential{cf("h1", "k1")},
			RAPIs:       []RAPI{rf("https://x.example.com", "m1", "x")},
		}
	}
	c, l := mk("center-name"), mk("local-name")

	got, rep, err := Merge(c, l, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if got.Platforms[0].Name != "center-name" {
		t.Errorf("center_wins: got %q", got.Platforms[0].Name)
	}
	if len(rep.Conflicts) != 1 || rep.Conflicts[0].Kind != "platform" {
		t.Errorf("want 1 platform conflict, got %+v", rep.Conflicts)
	} else if !reflect.DeepEqual(rep.Conflicts[0].Fields, []string{"Name"}) {
		t.Errorf("conflict fields = %v, want [Name]", rep.Conflicts[0].Fields)
	}
	got2, _, err := Merge(c, l, RuleLocalWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if got2.Platforms[0].Name != "local-name" {
		t.Errorf("local_wins: got %q", got2.Platforms[0].Name)
	}
}

// 本地独有的平台/凭据/端点/绑定都要收进结果并登记待补推。
func TestMerge2_LocalOnlyRowsAreKept(t *testing.T) {
	center := &Bundle{
		Platforms:   []Platform{pf("A", "https://a")},
		Credentials: []Credential{cf("h1", "k1")},
		RAPIs:       []RAPI{rf("https://a", "m1", "x")},
		Bindings:    []CredentialBinding{bf("https://a", "m1", "h1")},
	}
	local := &Bundle{
		Platforms:   []Platform{pf("A", "https://a"), pf("B", "https://b")},
		Credentials: []Credential{cf("h1", "k1"), cf("h2", "k2")},
		RAPIs:       []RAPI{rf("https://a", "m1", "x"), rf("https://b", "m2", "y")},
		Bindings:    []CredentialBinding{bf("https://a", "m1", "h1"), bf("https://b", "m2", "h2")},
	}
	got, rep, err := Merge(center, local, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if findPlat(got, "https://b") == nil || findRAPI(got, EndpointRef{"https://b", "m2"}) == nil {
		t.Errorf("local-only rows lost: %+v", got)
	}
	if len(rep.LocalOnlyPlatforms) != 1 || rep.LocalOnlyPlatforms[0] != "https://b" {
		t.Errorf("LocalOnlyPlatforms = %v", rep.LocalOnlyPlatforms)
	}
	if len(rep.LocalOnlyCreds) != 1 || rep.LocalOnlyCreds[0].TokenHash != "h2" {
		t.Errorf("LocalOnlyCreds = %v", rep.LocalOnlyCreds)
	}
	if len(rep.LocalOnlyRAPIs) != 1 {
		t.Errorf("LocalOnlyRAPIs = %v", rep.LocalOnlyRAPIs)
	}
	if len(rep.LocalOnlyBindings) != 1 {
		t.Errorf("LocalOnlyBindings = %v", rep.LocalOnlyBindings)
	}
}

// 悬空引用必须裁掉：端点平台消失、绑定指向不存在的端点/凭据。
func TestMerge2_PrunesDanglingRefs(t *testing.T) {
	center := &Bundle{
		Platforms:   []Platform{pf("A", "https://a")},
		Credentials: []Credential{cf("h1", "k1")},
		RAPIs:       []RAPI{rf("https://a", "ok", "x")},
		LAPIs:       []LAPI{lf("chat")},
		Bindings:    []CredentialBinding{bf("https://a", "ok", "h1")},
		LAPIRapiOrder: []LAPIRapiOrder{
			of("chat", "https://a", "ok", 0),
			of("chat", "https://a", "ghost", 1),
		},
	}
	local := &Bundle{
		Platforms:   []Platform{pf("A", "https://a")},
		Credentials: []Credential{cf("h1", "k1")},
		RAPIs:       []RAPI{rf("https://a", "ok", "x"), rf("https://zzz", "orphan", "z")},
		Bindings: []CredentialBinding{
			bf("https://a", "ok", "h1"),
			bf("https://a", "ok", "nosuchhash"),
		},
	}
	got, rep, err := Merge(center, local, RuleCenterWins)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if findRAPI(got, EndpointRef{"https://zzz", "orphan"}) != nil {
		t.Error("rapi under a non-merged platform must be pruned")
	}
	if len(got.Bindings) != 1 {
		t.Errorf("binding with unknown token_hash must be pruned, got %+v", got.Bindings)
	}
	if len(rep.DroppedBindings) != 1 {
		t.Errorf("DroppedBindings = %v", rep.DroppedBindings)
	}
	if len(got.LAPIRapiOrder) != 1 || got.LAPIRapiOrder[0].RAPIModel != "ok" {
		t.Errorf("chain node pointing at a pruned endpoint must be dropped: %+v", got.LAPIRapiOrder)
	}
}

// 端点与平台（供链测试复用）
func chainFixture() *Bundle {
	return &Bundle{
		Platforms:   []Platform{pf("A", "https://a")},
		Credentials: []Credential{cf("h1", "k1")},
		RAPIs:       []RAPI{rf("https://a", "m1", "e1"), rf("https://a", "m2", "e2"), rf("https://a", "m3", "e3")},
	}
}

// lapi 链比较矩阵（§5.2）：
//
//	L0  链相同        → 只留中心版，不产生副本
//	L1  同集异序      → 中心占原名 + 本地改名 #local
//	L2c 中心更长      → 只留中心长链
//	L2l 本地更长      → 本地占原名（中心短链丢弃）
//	L3  无关          → 中心占原名 + 本地改名 #local
func TestMerge2_LAPIChainMatrix(t *testing.T) {
	cases := []struct {
		name       string
		cChain     []LAPIRapiOrder
		lChain     []LAPIRapiOrder
		wantLevel  string
		wantKept   string
		wantLAPI   []string // 期望的 lapi 别名集合
		wantChains map[string][]string
	}{
		{
			name:      "L0 链相同去重",
			cChain:    []LAPIRapiOrder{of("chat", "https://a", "m1", 0), of("chat", "https://a", "m2", 1)},
			lChain:    []LAPIRapiOrder{of("chat", "https://a", "m1", 0), of("chat", "https://a", "m2", 1)},
			wantLevel: "L0", wantKept: "center",
			wantLAPI:   []string{"chat"},
			wantChains: map[string][]string{"chat": {"https://a#m1", "https://a#m2"}},
		},
		{
			name:      "L1 同集异序→本地改名保留",
			cChain:    []LAPIRapiOrder{of("chat", "https://a", "m1", 0), of("chat", "https://a", "m2", 1)},
			lChain:    []LAPIRapiOrder{of("chat", "https://a", "m2", 0), of("chat", "https://a", "m1", 1)},
			wantLevel: "L1", wantKept: "center",
			wantLAPI: []string{"chat", "chat#local2"},
			wantChains: map[string][]string{
				"chat":        {"https://a#m1", "https://a#m2"},
				"chat#local2": {"https://a#m2", "https://a#m1"},
			},
		},
		{
			name:      "L2c 中心更长→只用中心长链",
			cChain:    []LAPIRapiOrder{of("chat", "https://a", "m1", 0), of("chat", "https://a", "m2", 1)},
			lChain:    []LAPIRapiOrder{of("chat", "https://a", "m1", 0)},
			wantLevel: "L2c", wantKept: "center",
			wantLAPI:   []string{"chat"},
			wantChains: map[string][]string{"chat": {"https://a#m1", "https://a#m2"}},
		},
		{
			name:      "L2l 本地更长→本地占原名",
			cChain:    []LAPIRapiOrder{of("chat", "https://a", "m1", 0)},
			lChain:    []LAPIRapiOrder{of("chat", "https://a", "m1", 0), of("chat", "https://a", "m3", 1)},
			wantLevel: "L2l", wantKept: "local",
			wantLAPI:   []string{"chat"},
			wantChains: map[string][]string{"chat": {"https://a#m1", "https://a#m3"}},
		},
		{
			name:      "L3 无关→中心占原名+本地改名",
			cChain:    []LAPIRapiOrder{of("chat", "https://a", "m1", 0)},
			lChain:    []LAPIRapiOrder{of("chat", "https://a", "m3", 0)},
			wantLevel: "L3", wantKept: "center",
			wantLAPI: []string{"chat", "chat#local2"},
			wantChains: map[string][]string{
				"chat":        {"https://a#m1"},
				"chat#local2": {"https://a#m3"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			center := chainFixture()
			center.LAPIs = []LAPI{lf("chat")}
			center.LAPIRapiOrder = tc.cChain
			local := chainFixture()
			local.LAPIs = []LAPI{lf("chat")}
			local.LAPIRapiOrder = tc.lChain

			got, rep, err := Merge(center, local, RuleCenterWins)
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			if len(rep.LAPIs) != 1 {
				t.Fatalf("want 1 lapi decision, got %+v", rep.LAPIs)
			}
			d := rep.LAPIs[0]
			if d.Level != tc.wantLevel || d.Kept != tc.wantKept {
				t.Errorf("decision = %s/%s, want %s/%s", d.Level, d.Kept, tc.wantLevel, tc.wantKept)
			}
			var aliases []string
			for _, l := range got.LAPIs {
				aliases = append(aliases, l.Alias)
			}
			if !reflect.DeepEqual(aliases, tc.wantLAPI) {
				t.Errorf("lapi aliases = %v, want %v", aliases, tc.wantLAPI)
			}
			for alias, want := range tc.wantChains {
				if g := chainOf(got, alias); !reflect.DeepEqual(g, want) {
					t.Errorf("chain[%s] = %v, want %v", alias, g, want)
				}
			}
			// 每个 lapi 的 order_index 必须从 0 连续（UNIQUE 约束前提）
			seen := map[string]map[int]bool{}
			for _, o := range got.LAPIRapiOrder {
				if seen[o.LAPIAlias] == nil {
					seen[o.LAPIAlias] = map[int]bool{}
				}
				if seen[o.LAPIAlias][o.OrderIndex] {
					t.Errorf("duplicate order_index %d for lapi %s", o.OrderIndex, o.LAPIAlias)
				}
				seen[o.LAPIAlias][o.OrderIndex] = true
			}
		})
	}
}
