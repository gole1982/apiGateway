package bundle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// ---- Merge（中心 ↔ 本地 定义快照合并，契约 v2 自然键）---------------------
//
// 业务键（见 bundle.go v2）：platform=base_url、credential=token_hash、
// rapi=(base_url, model)、lapi=alias、绑定=(endpoint 自然键, token_hash)、
// 路由链节点=(lapi alias, base_url, model)。
//
// 分表策略（自然键迁移设计 §5.2）：
//   - platform / credential / rapi / bindings：**中心全权**。两端求并集，冲突留中心，
//     仅本地独有的行收进结果并登记为"待补推"（由调用方决定是否回推中心）。
//   - lapi + 路由链：**中心权威 + 本地保留区**。按公开名配对后比链：
//     L0 链相同 → 去重留中心；L2 子链 → 留长链；L1/L3 不可比 → 中心占原名、
//     本地版改名 `name#local` 保留（撞名递增）。
//
// 明确不做：**除 lapi 的 #local 保留区外不产生删除**。某一侧删掉的行不会传染到
// 另一侧（删除传播需要独立 tombstone 机制，不在此范围内）。

// ConflictRule 决定"中心全权"类定义在两端同键不同值时以哪一侧为准。
type ConflictRule int

const (
	// RuleCenterWins：中心权威。与既有设计一致（"权威：定义类=中心权威"），
	// 任一陈旧节点都不会反向覆盖全网配置。
	RuleCenterWins ConflictRule = iota
	// RuleLocalWins：本地优先。仅在明确要把本机当作权威源时才用。
	RuleLocalWins
)

func (r ConflictRule) String() string {
	if r == RuleLocalWins {
		return "local_wins"
	}
	return "center_wins"
}

// CredRef 标识一条凭据（业务键 token_hash）。Label 仅供人读/报告展示。
type CredRef struct {
	TokenHash string `json:"token_hash"`
	Label     string `json:"label,omitempty"`
}

func (c CredRef) String() string { return c.TokenHash }

// EndpointRef 标识一个上游端点（业务键 base_url + model）。
type EndpointRef struct {
	BaseURL string `json:"base_url"`
	Model   string `json:"model"`
}

func (e EndpointRef) String() string { return fmt.Sprintf("(%s,%s)", e.BaseURL, e.Model) }

// BindingRef 标识一条端点↔凭据绑定。
type BindingRef struct {
	Endpoint EndpointRef `json:"endpoint"`
	Cred     CredRef     `json:"cred"`
}

// Conflict 记录一次"两端同键不同值"的冲突及具体差异字段。
type Conflict struct {
	Kind   string   `json:"kind"` // platform | credential | rapi | binding
	Key    string   `json:"key"`
	Fields []string `json:"fields"`
}

// LAPIDecision 记录一处 lapi 合并判定（供面板展示"为什么保留了本地链"）。
type LAPIDecision struct {
	Alias   string `json:"alias"`
	Level   string `json:"level"` // L0 | L1 | L2 | L3 | local_only
	Kept    string `json:"kept"`  // center | local
	Renamed string `json:"renamed,omitempty"`
}

// MergeReport 汇总一次合并结果，供面板"预览差异"展示与审计。
type MergeReport struct {
	Rule ConflictRule `json:"rule"`
	// 仅本地存在、待补推上中心的行。
	LocalOnlyPlatforms []string      `json:"local_only_platforms"`
	LocalOnlyCreds     []CredRef     `json:"local_only_credentials"`
	LocalOnlyRAPIs     []EndpointRef `json:"local_only_rapis"`
	LocalOnlyBindings  []BindingRef  `json:"local_only_bindings"`
	LocalOnlyLAPIs     []string      `json:"local_only_lapis"`
	// 两端同键不同值（平台/凭据/端点/绑定）。
	Conflicts []Conflict `json:"conflicts"`
	// lapi 逐条判定。
	LAPIs []LAPIDecision `json:"lapis"`
	// 因引用消失而被裁剪的悬空行。
	DroppedRAPIs    []EndpointRef `json:"dropped_rapis"`
	DroppedBindings []BindingRef  `json:"dropped_bindings"`
	DroppedOrder    []string      `json:"dropped_order"`
}

// Empty 报告是否"两端完全一致"。
func (r *MergeReport) Empty() bool {
	if r == nil {
		return true
	}
	return len(r.LocalOnlyPlatforms) == 0 && len(r.LocalOnlyCreds) == 0 &&
		len(r.LocalOnlyRAPIs) == 0 && len(r.LocalOnlyBindings) == 0 &&
		len(r.LocalOnlyLAPIs) == 0 && len(r.Conflicts) == 0 &&
		len(r.DroppedRAPIs) == 0 && len(r.DroppedBindings) == 0 && len(r.DroppedOrder) == 0
}

// diffFields 列出两个同类型结构体取值不同的字段名（忽略未导出字段）。
func diffFields(a, b any) []string {
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if va.Kind() != reflect.Struct || va.Type() != vb.Type() {
		if reflect.DeepEqual(a, b) {
			return nil
		}
		return []string{"*"}
	}
	t := va.Type()
	var out []string
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).PkgPath != "" {
			continue
		}
		if !reflect.DeepEqual(va.Field(i).Interface(), vb.Field(i).Interface()) {
			out = append(out, t.Field(i).Name)
		}
	}
	return out
}

// ChainSignature 是 lapi 路由链的归一化签名（设计 §4）：
// canonical = 按 order_index 升序，normalize(base_url)#model 以 " > " 连接；
// signature = sha256(canonical) hex 前 16。
// 节点用自然键而非别名/id：改名不产生假差异，跨实例可直接比较。
func ChainSignature(order []LAPIRapiOrder) string {
	seq := append([]LAPIRapiOrder(nil), order...)
	sort.SliceStable(seq, func(i, j int) bool { return seq[i].OrderIndex < seq[j].OrderIndex })
	parts := make([]string, 0, len(seq))
	for _, o := range seq {
		parts = append(parts, canonicalNodeKey(o.RAPIPlatformBaseURL, o.RAPIModel))
	}
	canonical := strings.Join(parts, " > ")
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])[:16]
}

// isSubsequence 报 b 是否 a 的子序列（保持相对顺序；L2 判定用）。
func isSubsequence(short, long []string) bool {
	i := 0
	for _, x := range long {
		if i < len(short) && short[i] == x {
			i++
		}
	}
	return i == len(short)
}

// Merge 把中心快照与本地快照求并集。
//
// rule 只影响 platform/credential/rapi/bindings 的冲突取舍；lapi 走 §5.2 的
// 链比较语义，与 rule 无关。返回的 Bundle 一定通过 Validate。
func Merge(center, local *Bundle, rule ConflictRule) (*Bundle, *MergeReport, error) {
	if center == nil && local == nil {
		return nil, nil, fmt.Errorf("bundle: merge: both sides nil")
	}
	if center == nil {
		center = &Bundle{}
	}
	if local == nil {
		local = &Bundle{}
	}
	centerFirst := rule != RuleLocalWins
	rep := &MergeReport{Rule: rule}
	out := &Bundle{}

	out.Platforms = mergePlatforms(center.Platforms, local.Platforms, centerFirst, rep)
	platOK := make(map[string]bool, len(out.Platforms))
	for _, p := range out.Platforms {
		platOK[platformKey(p.BaseURL)] = true
	}

	out.Credentials = mergeCredentials(center.Credentials, local.Credentials, centerFirst, rep)
	credOK := make(map[string]bool, len(out.Credentials))
	for _, c := range out.Credentials {
		credOK[strings.TrimSpace(c.TokenHash)] = true
	}

	out.RAPIs, rep.DroppedRAPIs = mergeRAPIs(center.RAPIs, local.RAPIs, platOK, centerFirst, rep)
	epOK := make(map[EndpointRef]bool, len(out.RAPIs))
	for _, r := range out.RAPIs {
		epOK[EndpointRef{platformKey(r.PlatformBaseURL), r.Model}] = true
	}

	out.Bindings, rep.DroppedBindings = mergeBindings(center.Bindings, local.Bindings, epOK, credOK, centerFirst, rep)

	out.LAPIs, out.LAPIRapiOrder, rep.DroppedOrder, rep.LocalOnlyLAPIs =
		mergeLAPIsAndChains(center, local, epOK, rep)

	chk := &Envelope{SchemaVersion: SchemaVersion, Bundle: *out}
	if err := Validate(chk); err != nil {
		return nil, rep, fmt.Errorf("bundle: merge produced invalid bundle: %w", err)
	}
	return out, rep, nil
}

// mergePlatforms 按归一化 base_url 求并集。
func mergePlatforms(center, local []Platform, centerFirst bool, rep *MergeReport) []Platform {
	out := make([]Platform, 0, len(center)+len(local))
	idx := make(map[string]int, len(center)+len(local))
	add := func(p Platform) bool { // 返回是否新增
		k := platformKey(p.BaseURL)
		if i, ok := idx[k]; ok {
			if f := diffFields(out[i], p); len(f) > 0 {
				rep.Conflicts = append(rep.Conflicts, Conflict{Kind: "platform", Key: k, Fields: f})
				if !centerFirst {
					out[i] = p
				}
			}
			return false
		}
		idx[k] = len(out)
		out = append(out, p)
		return true
	}
	for _, p := range center {
		add(p)
	}
	for _, p := range local {
		if add(p) {
			rep.LocalOnlyPlatforms = append(rep.LocalOnlyPlatforms, platformKey(p.BaseURL))
		}
	}
	return out
}

// mergeCredentials 按 token_hash 求并集。
func mergeCredentials(center, local []Credential, centerFirst bool, rep *MergeReport) []Credential {
	out := make([]Credential, 0, len(center)+len(local))
	idx := make(map[string]int, len(center)+len(local))
	add := func(c Credential) bool {
		h := strings.TrimSpace(c.TokenHash)
		if i, ok := idx[h]; ok {
			if f := diffFields(out[i], c); len(f) > 0 {
				rep.Conflicts = append(rep.Conflicts, Conflict{Kind: "credential", Key: h, Fields: f})
				if !centerFirst {
					out[i] = c
				}
			}
			return false
		}
		idx[h] = len(out)
		out = append(out, c)
		return true
	}
	for _, c := range center {
		add(c)
	}
	for _, c := range local {
		if add(c) {
			rep.LocalOnlyCreds = append(rep.LocalOnlyCreds, CredRef{TokenHash: strings.TrimSpace(c.TokenHash), Label: c.Label})
		}
	}
	return out
}

// mergeRAPIs 按 (base_url, model) 求并集，裁掉平台已消失的悬空端点。
func mergeRAPIs(center, local []RAPI, platOK map[string]bool, centerFirst bool, rep *MergeReport) ([]RAPI, []EndpointRef) {
	type keyed struct {
		r  RAPI
		id EndpointRef
	}
	idx := make(map[EndpointRef]int, len(center)+len(local))
	var picked []keyed
	add := func(r RAPI) bool {
		id := EndpointRef{platformKey(r.PlatformBaseURL), r.Model}
		if i, ok := idx[id]; ok {
			if f := diffFields(picked[i].r, r); len(f) > 0 {
				rep.Conflicts = append(rep.Conflicts, Conflict{Kind: "rapi", Key: id.String(), Fields: f})
				if !centerFirst {
					picked[i].r = r
				}
			}
			return false
		}
		idx[id] = len(picked)
		picked = append(picked, keyed{r: r, id: id})
		return true
	}
	for _, r := range center {
		add(r)
	}
	for _, r := range local {
		if add(r) {
			rep.LocalOnlyRAPIs = append(rep.LocalOnlyRAPIs, EndpointRef{platformKey(r.PlatformBaseURL), r.Model})
		}
	}
	out := make([]RAPI, 0, len(picked))
	var dropped []EndpointRef
	for _, p := range picked {
		if !platOK[platformKey(p.r.PlatformBaseURL)] {
			dropped = append(dropped, p.id)
			continue
		}
		out = append(out, p.r)
	}
	return out, dropped
}

// mergeBindings 按 (endpoint, token_hash) 求并集，裁掉悬空引用。
func mergeBindings(center, local []CredentialBinding, epOK map[EndpointRef]bool, credOK map[string]bool, centerFirst bool, rep *MergeReport) ([]CredentialBinding, []BindingRef) {
	type keyed struct {
		b  CredentialBinding
		id BindingRef
	}
	ref := func(b CredentialBinding) BindingRef {
		return BindingRef{
			Endpoint: EndpointRef{platformKey(b.PlatformBaseURL), b.Model},
			Cred:     CredRef{TokenHash: strings.TrimSpace(b.TokenHash)},
		}
	}
	idx := make(map[BindingRef]int, len(center)+len(local))
	var picked []keyed
	add := func(b CredentialBinding) bool {
		id := ref(b)
		if i, ok := idx[id]; ok {
			if f := diffFields(picked[i].b, b); len(f) > 0 {
				rep.Conflicts = append(rep.Conflicts, Conflict{Kind: "binding", Key: id.Endpoint.String() + "|" + id.Cred.TokenHash, Fields: f})
				if !centerFirst {
					picked[i].b = b
				}
			}
			return false
		}
		idx[id] = len(picked)
		picked = append(picked, keyed{b: b, id: id})
		return true
	}
	for _, b := range center {
		add(b)
	}
	for _, b := range local {
		if add(b) {
			rep.LocalOnlyBindings = append(rep.LocalOnlyBindings, ref(b))
		}
	}
	out := make([]CredentialBinding, 0, len(picked))
	var dropped []BindingRef
	for _, p := range picked {
		if !epOK[p.id.Endpoint] || !credOK[p.id.Cred.TokenHash] {
			dropped = append(dropped, p.id)
			continue
		}
		out = append(out, p.b)
	}
	return out, dropped
}

// mergeLAPIsAndChains 实现 §5.2 的 lapi 合并：中心权威 + 本地保留区。
//
// 按公开名配对后比链（链签名基于自然键，改名不产生假差异）：
//   - L0 链相同          → 去重，留中心版
//   - L2 一方是另一方子链 → 留长链（中心长则用中心；本地长则保留本地）
//   - L1 同集异序 / L3 无关 → 不可比：中心占原名，本地版改名 `name#local` 保留
//   - 仅本地存在          → 保留（不下发也不删）
//   - 仅中心存在          → 下发
//
// 关键约束：同一个 lapi 的整条链只来自一侧，绝不两侧交错——落库表有
// UNIQUE(lapi_id, order_index)，交错会产生重复 order_index。
func mergeLAPIsAndChains(center, local *Bundle, epOK map[EndpointRef]bool, rep *MergeReport) ([]LAPI, []LAPIRapiOrder, []string, []string) {
	centerChains := groupChain(center.LAPIRapiOrder)
	localChains := groupChain(local.LAPIRapiOrder)

	centerLAPI := make(map[string]LAPI, len(center.LAPIs))
	for _, l := range center.LAPIs {
		centerLAPI[l.Alias] = l
	}
	localLAPI := make(map[string]LAPI, len(local.LAPIs))
	for _, l := range local.LAPIs {
		localLAPI[l.Alias] = l
	}

	aliases := make([]string, 0, len(centerLAPI)+len(localLAPI))
	for a := range centerLAPI {
		aliases = append(aliases, a)
	}
	for a := range localLAPI {
		if _, ok := centerLAPI[a]; !ok {
			aliases = append(aliases, a)
		}
	}
	sort.Strings(aliases) // 输出确定，便于回归比对

	var outLAPIs []LAPI
	var outChains []LAPIRapiOrder
	var dropped []string
	var localOnly []string

	// 已占用名字（含改名产生的 #local），用于撞名递增
	taken := make(map[string]bool, len(aliases))
	allocName := func(base string) string {
		if !taken[base] {
			taken[base] = true
			return base
		}
		for i := 2; ; i++ {
			cand := fmt.Sprintf("%s#local%d", base, i)
			if !taken[cand] {
				taken[cand] = true
				return cand
			}
		}
	}

	emit := func(l LAPI, chain []LAPIRapiOrder, alias string) {
		kept, drop := filterChain(chain, alias, epOK)
		// 改名保留时（不可比 → 本地版改名），链节点必须跟着改指向新 lapi。
		// 否则本地链仍挂在原名下：既与中心链产生重复行让 Validate 拒绝，
		// 又会把两条链交错到同一 lapi（order_index 冲突，apply 必失败）。
		for i := range kept {
			kept[i].LAPIAlias = alias
		}
		outLAPIs = append(outLAPIs, l)
		outChains = append(outChains, kept...)
		dropped = append(dropped, drop...)
	}

	for _, alias := range aliases {
		cLAPI, inCenter := centerLAPI[alias]
		lLAPI, inLocal := localLAPI[alias]

		if inCenter && !inLocal {
			taken[alias] = true
			emit(cLAPI, centerChains[alias], alias)
			continue
		}
		if inLocal && !inCenter {
			taken[alias] = true
			emit(lLAPI, localChains[alias], alias)
			localOnly = append(localOnly, alias)
			rep.LAPIs = append(rep.LAPIs, LAPIDecision{Alias: alias, Level: "local_only", Kept: "local"})
			continue
		}

		// 两端同名 → 比链（§5.2 表）
		cChain, lChain := centerChains[alias], localChains[alias]
		level := compareChains(cChain, lChain)
		switch level {
		case "L0", "L2c":
			// 链相同 / 中心更长 → 中心占原名，本地链被覆盖丢弃
			taken[alias] = true
			emit(cLAPI, cChain, alias)
			rep.LAPIs = append(rep.LAPIs, LAPIDecision{Alias: alias, Level: level, Kept: "center"})

		case "L2l":
			// 本地更长 → 留本地：本地占原名，中心短链被丢弃
			taken[alias] = true
			emit(lLAPI, lChain, alias)
			rep.LAPIs = append(rep.LAPIs, LAPIDecision{Alias: alias, Level: level, Kept: "local"})

		default: // L1 同集异序 / L3 无关 → 不可比
			// 中心占原名；本地版改名 `name#local` 保留（撞名递增）
			taken[alias] = true
			emit(cLAPI, cChain, alias)
			localName := allocName(alias)
			renamed := lLAPI
			renamed.Alias = localName
			emit(renamed, lChain, localName)
			rep.LAPIs = append(rep.LAPIs, LAPIDecision{Alias: alias, Level: level, Kept: "center", Renamed: localName})
			localOnly = append(localOnly, localName)
		}
	}

	outChains = renumberChains(outChains)
	return outLAPIs, outChains, dropped, localOnly
}

// filterChain 丢掉指向已不存在端点的链节点（端点在合并中被裁掉时会出现）。
func filterChain(chain []LAPIRapiOrder, alias string, epOK map[EndpointRef]bool) ([]LAPIRapiOrder, []string) {
	kept := make([]LAPIRapiOrder, 0, len(chain))
	var dropped []string
	for _, o := range chain {
		ep := EndpointRef{platformKey(o.RAPIPlatformBaseURL), o.RAPIModel}
		if !epOK[ep] {
			dropped = append(dropped, alias+"→"+ep.String())
			continue
		}
		kept = append(kept, o)
	}
	return kept, dropped
}

// chainNodes 取链上端点自然键的有序序列。
func chainNodes(chain []LAPIRapiOrder) []string {
	seq := append([]LAPIRapiOrder(nil), chain...)
	sort.SliceStable(seq, func(i, j int) bool { return seq[i].OrderIndex < seq[j].OrderIndex })
	out := make([]string, 0, len(seq))
	for _, o := range seq {
		out = append(out, canonicalNodeKey(o.RAPIPlatformBaseURL, o.RAPIModel))
	}
	return out
}

// compareChains 判定两条链的关系：
// L0 相同 / L1 同集异序 / L2c 本地是中心子链（中心更长）/ L2l 中心是本地子链 / L3 无关。
func compareChains(center, local []LAPIRapiOrder) string {
	if ChainSignature(center) == ChainSignature(local) {
		return "L0"
	}
	cn, ln := chainNodes(center), chainNodes(local)
	if len(cn) == len(ln) && sameMultiset(cn, ln) {
		return "L1" // 同集异序
	}
	switch {
	case isSubsequence(ln, cn):
		return "L2c"
	case isSubsequence(cn, ln):
		return "L2l"
	default:
		return "L3"
	}
}

// sameMultiset 报两个序列是否元素多重集相同（顺序可不同）。
func sameMultiset(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	cnt := make(map[string]int, len(a))
	for _, x := range a {
		cnt[x]++
	}
	for _, x := range b {
		cnt[x]--
		if cnt[x] < 0 {
			return false
		}
	}
	return true
}

// groupChain 按 lapi 分组并按 order_index 升序排（稳定）。
func groupChain(in []LAPIRapiOrder) map[string][]LAPIRapiOrder {
	out := make(map[string][]LAPIRapiOrder)
	for _, o := range in {
		out[o.LAPIAlias] = append(out[o.LAPIAlias], o)
	}
	for _, seq := range out {
		sort.SliceStable(seq, func(i, j int) bool { return seq[i].OrderIndex < seq[j].OrderIndex })
	}
	return out
}

// renumberChains 给每个 lapi 的链从 0 连续编号（保序），杜绝重复/断号。
func renumberChains(in []LAPIRapiOrder) []LAPIRapiOrder {
	byLAPI := groupChain(in)
	aliases := make([]string, 0, len(byLAPI))
	for a := range byLAPI {
		aliases = append(aliases, a)
	}
	sort.Strings(aliases)
	out := make([]LAPIRapiOrder, 0, len(in))
	for _, a := range aliases {
		for i, o := range byLAPI[a] {
			o.OrderIndex = i
			out = append(out, o)
		}
	}
	return out
}
