package bundle

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// ---- Merge（中心 ↔ 本地 定义快照合并）------------------------------------
//
// 契约基础：Bundle 不带自增 id，全部用业务键引用（platform.name、
// (platform_name, key_index)、(platform_name, alias)、lapi.alias、路由链三元组），
// 因此两端求并集是安全的——本地 id 与中心 id 无需一致。
//
// 语义（"两端同时合并"）：
//   - 只在中心有 → 保留中心（拉下来）
//   - 只在本地有 → 收进结果（由调用方推上中心）
//   - 两端都有   → 按 ConflictRule 取值，差异字段记入 report.Conflicts
//   - 合并后裁剪悬空引用（平台没了，其下 key / rapi / 路由链一并丢弃），
//     保证结果一定能通过 Validate。
//
// 明确不做：**合并语义下不产生任何删除**。某一侧删掉的行不会传染到另一侧
// （删除传播需要独立 tombstone 机制，不在此范围内）。

// ConflictRule 决定同一业务键在两端都存在且字段值不同时以哪一侧为准。
type ConflictRule int

const (
	// RuleCenterWins：中心权威，冲突保留中心值。与既有设计一致
	//（"权威：定义类=中心权威"），任一陈旧节点都不会反向覆盖全网配置。
	RuleCenterWins ConflictRule = iota
	// RuleLocalWins：管理端本地优先，冲突以本地值覆盖中心。
	RuleLocalWins
)

func (r ConflictRule) String() string {
	if r == RuleLocalWins {
		return "local_wins"
	}
	return "center_wins"
}

// PKRef 标识一条平台密钥的业务键。
type PKRef struct {
	PlatformName string `json:"platform_name"`
	KeyIndex     int    `json:"key_index"`
}

func (k PKRef) String() string { return fmt.Sprintf("(%s,%d)", k.PlatformName, k.KeyIndex) }

// RAPIKey 标识一条上游模型端点的业务键。
type RAPIKey struct {
	PlatformName string `json:"platform_name"`
	Alias        string `json:"alias"`
}

func (k RAPIKey) String() string { return fmt.Sprintf("(%s,%s)", k.PlatformName, k.Alias) }

// Conflict 记录一次"两端同键不同值"的冲突及具体差异字段。
type Conflict struct {
	Kind   string   `json:"kind"` // platform | platform_key | rapi | lapi
	Key    string   `json:"key"`
	Fields []string `json:"fields"`
}

// MergeReport 汇总一次合并结果，供面板"预览差异"展示与审计。
type MergeReport struct {
	Rule ConflictRule `json:"rule"`
	// 仅本地存在、将被补推上中心的行。
	LocalOnlyPlatforms []string  `json:"local_only_platforms"`
	LocalOnlyKeys      []PKRef   `json:"local_only_keys"`
	LocalOnlyRAPIs     []RAPIKey `json:"local_only_rapis"`
	LocalOnlyLAPIs     []string  `json:"local_only_lapis"`
	LocalOnlyOrderLAPI []string  `json:"local_only_order_lapis"`
	// 两端同键不同值。
	Conflicts []Conflict `json:"conflicts"`
	// 因引用消失而被裁剪的悬空行。
	DroppedKeys  []PKRef   `json:"dropped_keys"`
	DroppedRAPIs []RAPIKey `json:"dropped_rapis"`
	DroppedOrder []string  `json:"dropped_order"`
}

// Empty 报告是否"两端完全一致"（无新增、无冲突、无裁剪）。
func (r *MergeReport) Empty() bool {
	if r == nil {
		return true
	}
	return len(r.LocalOnlyPlatforms) == 0 && len(r.LocalOnlyKeys) == 0 &&
		len(r.LocalOnlyRAPIs) == 0 && len(r.LocalOnlyLAPIs) == 0 &&
		len(r.LocalOnlyOrderLAPI) == 0 && len(r.Conflicts) == 0 &&
		len(r.DroppedKeys) == 0 && len(r.DroppedRAPIs) == 0 && len(r.DroppedOrder) == 0
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
			continue // 未导出
		}
		if !reflect.DeepEqual(va.Field(i).Interface(), vb.Field(i).Interface()) {
			out = append(out, t.Field(i).Name)
		}
	}
	return out
}

// Merge 把中心快照与本地快照求并集。
//
// rule 只影响"两端同键不同值"的取舍；并集与裁剪行为两种规则下一致。
// 返回的 Bundle 一定通过 Validate（不合法直接返回 error，绝不交出脏结果）。
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

	// 顺序固定：中心原有行在前（保持其 sort_order 语义稳定），本地独有行追加在后。
	out.Platforms = mergePlatforms(center.Platforms, local.Platforms, centerFirst, rep)

	platOK := make(map[string]bool, len(out.Platforms))
	for _, p := range out.Platforms {
		platOK[p.Name] = true
	}

	out.PlatformKeys, rep.DroppedKeys = mergePlatformKeys(center.PlatformKeys, local.PlatformKeys, platOK, centerFirst, rep)
	keyOK := make(map[PKRef]bool, len(out.PlatformKeys))
	for _, k := range out.PlatformKeys {
		keyOK[PKRef{k.PlatformName, k.KeyIndex}] = true
	}

	out.RAPIs, rep.DroppedRAPIs = mergeRAPIs(center.RAPIs, local.RAPIs, platOK, keyOK, centerFirst, rep)
	rapiOK := make(map[RAPIKey]bool, len(out.RAPIs))
	for _, r := range out.RAPIs {
		rapiOK[RAPIKey{r.PlatformName, r.Alias}] = true
	}

	out.LAPIs = mergeLAPIs(center.LAPIs, local.LAPIs, centerFirst, rep)
	lapiOK := make(map[string]bool, len(out.LAPIs))
	for _, l := range out.LAPIs {
		lapiOK[l.Alias] = true
	}

	out.LAPIRapiOrder, rep.DroppedOrder, rep.LocalOnlyOrderLAPI =
		mergeOrder(center.LAPIRapiOrder, local.LAPIRapiOrder, lapiOK, rapiOK)

	// 结果必须自洽：脏结果绝不能交出去让 ApplyBundle 兜底。
	chk := &Envelope{SchemaVersion: SchemaVersion, Bundle: *out}
	if err := Validate(chk); err != nil {
		return nil, rep, fmt.Errorf("bundle: merge produced invalid bundle: %w", err)
	}
	return out, rep, nil
}

// mergePlatforms 按 name 求并集。
func mergePlatforms(center, local []Platform, centerFirst bool, rep *MergeReport) []Platform {
	out := make([]Platform, 0, len(center)+len(local))
	idx := make(map[string]int, len(center)+len(local))
	for _, p := range center {
		idx[p.Name] = len(out)
		out = append(out, p)
	}
	for _, p := range local {
		if i, ok := idx[p.Name]; ok {
			if f := diffFields(out[i], p); len(f) > 0 {
				rep.Conflicts = append(rep.Conflicts, Conflict{Kind: "platform", Key: p.Name, Fields: f})
				if !centerFirst {
					out[i] = p
				}
			}
			continue
		}
		idx[p.Name] = len(out)
		out = append(out, p)
		rep.LocalOnlyPlatforms = append(rep.LocalOnlyPlatforms, p.Name)
	}
	return out
}

// mergePlatformKeys 按 (platform_name, key_index) 求并集，并裁掉平台已消失的悬空密钥。
func mergePlatformKeys(center, local []PlatformKey, platOK map[string]bool, centerFirst bool, rep *MergeReport) ([]PlatformKey, []PKRef) {
	type keyed struct {
		k  PlatformKey
		id PKRef
	}
	idx := make(map[PKRef]int, len(center)+len(local))
	var picked []keyed
	add := func(k PlatformKey) {
		id := PKRef{k.PlatformName, k.KeyIndex}
		if i, ok := idx[id]; ok {
			if f := diffFields(picked[i].k, k); len(f) > 0 {
				rep.Conflicts = append(rep.Conflicts, Conflict{Kind: "platform_key", Key: id.String(), Fields: f})
				if !centerFirst {
					picked[i].k = k
				}
			}
			return
		}
		idx[id] = len(picked)
		picked = append(picked, keyed{k: k, id: id})
	}
	for _, k := range center {
		add(k)
	}
	for _, k := range local {
		before := len(picked)
		add(k)
		if len(picked) > before {
			rep.LocalOnlyKeys = append(rep.LocalOnlyKeys, PKRef{k.PlatformName, k.KeyIndex})
		}
	}

	out := make([]PlatformKey, 0, len(picked))
	var dropped []PKRef
	for _, p := range picked {
		if !platOK[p.k.PlatformName] {
			dropped = append(dropped, p.id)
			continue
		}
		out = append(out, p.k)
	}
	return out, dropped
}

// mergeRAPIs 按 (platform_name, alias) 求并集，并裁掉两类悬空：
// 平台已消失，或 key_ids 引用了该平台下不存在的 key_index。
func mergeRAPIs(center, local []RAPI, platOK map[string]bool, keyOK map[PKRef]bool, centerFirst bool, rep *MergeReport) ([]RAPI, []RAPIKey) {
	type keyed struct {
		r  RAPI
		id RAPIKey
	}
	idx := make(map[RAPIKey]int, len(center)+len(local))
	var picked []keyed
	add := func(r RAPI) {
		id := RAPIKey{r.PlatformName, r.Alias}
		if i, ok := idx[id]; ok {
			if f := diffFields(picked[i].r, r); len(f) > 0 {
				rep.Conflicts = append(rep.Conflicts, Conflict{Kind: "rapi", Key: id.String(), Fields: f})
				if !centerFirst {
					picked[i].r = r
				}
			}
			return
		}
		idx[id] = len(picked)
		picked = append(picked, keyed{r: r, id: id})
	}
	for _, r := range center {
		add(r)
	}
	for _, r := range local {
		before := len(picked)
		add(r)
		if len(picked) > before {
			rep.LocalOnlyRAPIs = append(rep.LocalOnlyRAPIs, RAPIKey{r.PlatformName, r.Alias})
		}
	}

	out := make([]RAPI, 0, len(picked))
	var dropped []RAPIKey
	for _, p := range picked {
		if !platOK[p.r.PlatformName] || !keyIDsResolvable(p.r, keyOK) {
			dropped = append(dropped, p.id)
			continue
		}
		out = append(out, p.r)
	}
	return out, dropped
}

// mergeLAPIs 按 alias 求并集。
func mergeLAPIs(center, local []LAPI, centerFirst bool, rep *MergeReport) []LAPI {
	out := make([]LAPI, 0, len(center)+len(local))
	idx := make(map[string]int, len(center)+len(local))
	for _, l := range center {
		idx[l.Alias] = len(out)
		out = append(out, l)
	}
	for _, l := range local {
		if i, ok := idx[l.Alias]; ok {
			if f := diffFields(out[i], l); len(f) > 0 {
				rep.Conflicts = append(rep.Conflicts, Conflict{Kind: "lapi", Key: l.Alias, Fields: f})
				if !centerFirst {
					out[i] = l
				}
			}
			continue
		}
		idx[l.Alias] = len(out)
		out = append(out, l)
		rep.LocalOnlyLAPIs = append(rep.LocalOnlyLAPIs, l.Alias)
	}
	return out
}

// mergeOrder 合并路由链顺序。
//
// 关键约束：**同一个 lapi 的整条链只从一侧取**，绝不两侧交错。落库表有
// UNIQUE(lapi_id, order_index)，交错会产生重复 order_index，ApplyBundle 必失败。
// 中心有该 lapi 的链就整条用中心的；只有本地有才用本地的。
func mergeOrder(center, local []LAPIRapiOrder, lapiOK map[string]bool, rapiOK map[RAPIKey]bool) ([]LAPIRapiOrder, []string, []string) {
	centerByLAPI := groupOrderByLAPI(center)
	localByLAPI := groupOrderByLAPI(local)

	aliases := make([]string, 0, len(centerByLAPI)+len(localByLAPI))
	for a := range centerByLAPI {
		aliases = append(aliases, a)
	}
	for a := range localByLAPI {
		if _, ok := centerByLAPI[a]; !ok {
			aliases = append(aliases, a)
		}
	}
	sort.Strings(aliases) // 输出确定，便于测试与审计

	var out []LAPIRapiOrder
	var dropped []string
	var localOnly []string
	for _, alias := range aliases {
		seq, fromCenter := centerByLAPI[alias], true
		if _, ok := centerByLAPI[alias]; !ok {
			seq, fromCenter = localByLAPI[alias], false
		}
		kept := make([]LAPIRapiOrder, 0, len(seq))
		for _, o := range seq {
			if lapiOK[o.LAPIAlias] && rapiOK[RAPIKey{o.RAPIPlatformName, o.RAPIAlias}] {
				kept = append(kept, o)
			} else {
				dropped = append(dropped, o.LAPIAlias+"→"+o.RAPIPlatformName+"/"+o.RAPIAlias)
			}
		}
		out = append(out, kept...)
		if !fromCenter {
			localOnly = append(localOnly, alias)
		}
	}
	return out, dropped, localOnly
}

// groupOrderByLAPI 按 lapi 分组并按 order_index 升序排（稳定）。
func groupOrderByLAPI(in []LAPIRapiOrder) map[string][]LAPIRapiOrder {
	out := make(map[string][]LAPIRapiOrder)
	for _, o := range in {
		out[o.LAPIAlias] = append(out[o.LAPIAlias], o)
	}
	for _, seq := range out {
		sort.SliceStable(seq, func(i, j int) bool { return seq[i].OrderIndex < seq[j].OrderIndex })
	}
	return out
}

// keyIDsResolvable 校验 rapi.key_ids 里的 key_index 在该平台下都存在。
// 空 = 该平台全部 key 可用（与 bundle.Validate 语义一致）。
func keyIDsResolvable(r RAPI, keyOK map[PKRef]bool) bool {
	s := strings.TrimSpace(r.KeyIDs)
	if s == "" {
		return true
	}
	for _, raw := range strings.Split(s, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		idx, err := strconv.Atoi(raw)
		if err != nil || !keyOK[PKRef{r.PlatformName, idx}] {
			return false
		}
	}
	return true
}
