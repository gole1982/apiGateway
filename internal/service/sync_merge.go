package service

// 并集合并（union merge）：中心 bundle 与本地快照按自然键取并集。
//
// 触发条件（见 syncAction）：中心版本 != 本地 last_good 且本地有未同步修改
// （dirty）。版本相同 → 不合并、不提示；本地干净 → 照旧直接拉取覆盖。
//
// 合并规则：
//   - 只出现在任一边的行 → 保留（两边取并集）。
//   - 同自然键、内容不同 → 本地胜（端侧修改是运维者的 deliberate 操作；
//     可写 key 会把并集回推中心，中心最终收敛，不丢数据；只读 key 下本地
//     保留修改并继续标脏）。
//   - 自然键口径与落库一致：platform 用 base_url 原串（不归一化，与
//     bundle.platformKey / 本地唯一索引语义对齐）；credential 用 token_hash；
//     rapi 用 (base_url, model)；lapi 用 alias；绑定用 (base_url, model,
//     token_hash)；路由链用 (lapi, base_url, model)。
//   - 合并不传播删除：任一边删掉的行会被另一边"复活"。删行请走推送
//     （可写 key 把本地状态整体发布到中心）或在中心删除后保持端侧干净再拉取。
//   - 输出顺序确定：本地原有顺序 + 中心独有行按中心顺序追加，保证重复合并
//     结果稳定（幂等），幂等性由 TestMergeBundles_PureAndIdempotent 锁死。
//
// MergeBundles 是纯函数（无 DB、无网络），可单测。

import (
	"reflect"
	"time"

	"gateway/internal/bundle"
)

// MergeConflict 记录一次"同键内容不同、按本地胜解决"的冲突。
// 展示给用户看，长度由 maxMergeConflicts 截断。
type MergeConflict struct {
	Table string `json:"table"`
	Key   string `json:"key"`
}

// MergeSummary 并集结果摘要：面板横幅 + 系统日志用。
type MergeSummary struct {
	MergedAt   string          `json:"merged_at"`
	CenterVer  int64           `json:"center_version"`
	FromCenter map[string]int  `json:"from_center"` // 中心独有、并入本地的各表行数
	KeptLocal  map[string]int  `json:"kept_local"`  // 本地独有、保留的各表行数
	Conflicts  []MergeConflict `json:"conflicts,omitempty"`
	// AddsToCenter 为 true ⟺ 并集相对中心有新增/变更（本地独有行或冲突），
	// 此时可写 key 才值得回推；否则回推只会空转 bump 中心版本号。
	AddsToCenter bool   `json:"adds_to_center"`
	PushedBack   bool   `json:"pushed_back"`
	PushError    string `json:"push_error,omitempty"`
}

// maxMergeConflicts 摘要里最多保留的冲突条目数（防超大中心刷屏）。
const maxMergeConflicts = 20

func canonTime(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.Unix()
}

// platformEqual 比较平台定义是否实质相同。LastTokenFetch 只比到秒
// （本地库 DATETIME 精度与中心 JSON 回环可能差亚秒级，直接 DeepEqual 会误报冲突）。
func platformEqual(a, b bundle.Platform) bool {
	return a.BaseURL == b.BaseURL &&
		a.Name == b.Name &&
		a.Token == b.Token &&
		canonTime(a.LastTokenFetch) == canonTime(b.LastTokenFetch) &&
		a.Enabled == b.Enabled &&
		a.Notes == b.Notes &&
		a.SupportedFormats == b.SupportedFormats &&
		a.FormatEndpoints == b.FormatEndpoints &&
		a.CustomHeaders == b.CustomHeaders &&
		a.LoginAccount == b.LoginAccount &&
		a.SortOrder == b.SortOrder
}

// credentialEqual 比较凭据定义是否实质相同（ExpiresAt 同理只比到秒）。
func credentialEqual(a, b bundle.Credential) bool {
	return a.TokenHash == b.TokenHash &&
		a.PlatformBaseURL == b.PlatformBaseURL &&
		a.SortOrder == b.SortOrder &&
		a.Token == b.Token &&
		a.Label == b.Label &&
		a.Enabled == b.Enabled &&
		canonTime(a.ExpiresAt) == canonTime(b.ExpiresAt) &&
		a.IsFree == b.IsFree
}

// MergeBundles 取 local 与 center 的并集（local 冲突胜出），返回合并包与摘要。
// 两个输入都不修改；merged 与它们不共享底层数组（append 拷贝）。
func MergeBundles(local, center bundle.Bundle) (bundle.Bundle, MergeSummary) {
	sum := MergeSummary{
		MergedAt:   time.Now().Format(time.RFC3339),
		FromCenter: map[string]int{},
		KeptLocal:  map[string]int{},
	}

	// ---- platform（键 = base_url 原串）----
	localPlats := make(map[string]bundle.Platform, len(local.Platforms))
	for _, p := range local.Platforms {
		localPlats[p.BaseURL] = p
	}
	merged := bundle.Bundle{}
	for _, p := range local.Platforms {
		merged.Platforms = append(merged.Platforms, p)
		if _, ok := indexPlatform(center.Platforms, p.BaseURL); !ok {
			sum.KeptLocal["platform"]++
			sum.AddsToCenter = true
		}
	}
	for _, p := range center.Platforms {
		if lp, ok := localPlats[p.BaseURL]; ok {
			if !platformEqual(lp, p) {
				sum.addConflict("platform", p.BaseURL)
				sum.AddsToCenter = true
			}
			continue
		}
		merged.Platforms = append(merged.Platforms, p)
		sum.FromCenter["platform"]++
	}

	// ---- credential（键 = token_hash）----
	localCreds := make(map[string]bundle.Credential, len(local.Credentials))
	for _, c := range local.Credentials {
		localCreds[c.TokenHash] = c
	}
	for _, c := range local.Credentials {
		merged.Credentials = append(merged.Credentials, c)
		if _, ok := indexCredential(center.Credentials, c.TokenHash); !ok {
			sum.KeptLocal["credential"]++
			sum.AddsToCenter = true
		}
	}
	for _, c := range center.Credentials {
		if lc, ok := localCreds[c.TokenHash]; ok {
			if !credentialEqual(lc, c) {
				sum.addConflict("credential", shortHash(c.TokenHash))
				sum.AddsToCenter = true
			}
			continue
		}
		merged.Credentials = append(merged.Credentials, c)
		sum.FromCenter["credential"]++
	}

	// ---- rapi（键 = base_url + model）----
	type epKey struct{ baseURL, model string }
	ek := func(baseURL, model string) epKey { return epKey{baseURL, model} }
	localRAPIs := make(map[epKey]bundle.RAPI, len(local.RAPIs))
	for _, r := range local.RAPIs {
		localRAPIs[ek(r.PlatformBaseURL, r.Model)] = r
	}
	for _, r := range local.RAPIs {
		merged.RAPIs = append(merged.RAPIs, r)
		if _, ok := indexRAPI(center.RAPIs, r.PlatformBaseURL, r.Model); !ok {
			sum.KeptLocal["rapi"]++
			sum.AddsToCenter = true
		}
	}
	for _, r := range center.RAPIs {
		k := ek(r.PlatformBaseURL, r.Model)
		if lr, ok := localRAPIs[k]; ok {
			if !reflect.DeepEqual(lr, r) {
				sum.addConflict("rapi", r.PlatformBaseURL+"#"+r.Model)
				sum.AddsToCenter = true
			}
			continue
		}
		merged.RAPIs = append(merged.RAPIs, r)
		sum.FromCenter["rapi"]++
	}

	// ---- lapi（键 = alias）----
	localLAPIs := make(map[string]bundle.LAPI, len(local.LAPIs))
	for _, l := range local.LAPIs {
		localLAPIs[l.Alias] = l
	}
	for _, l := range local.LAPIs {
		merged.LAPIs = append(merged.LAPIs, l)
		if _, ok := indexLAPI(center.LAPIs, l.Alias); !ok {
			sum.KeptLocal["lapi"]++
			sum.AddsToCenter = true
		}
	}
	for _, l := range center.LAPIs {
		if ll, ok := localLAPIs[l.Alias]; ok {
			if !reflect.DeepEqual(ll, l) {
				sum.addConflict("lapi", l.Alias)
				sum.AddsToCenter = true
			}
			continue
		}
		merged.LAPIs = append(merged.LAPIs, l)
		sum.FromCenter["lapi"]++
	}

	// ---- bindings（键 = base_url + model + token_hash）----
	type bindKey struct{ baseURL, model, hash string }
	bk := func(baseURL, model, hash string) bindKey { return bindKey{baseURL, model, hash} }
	localBinds := make(map[bindKey]bundle.CredentialBinding, len(local.Bindings))
	for _, bd := range local.Bindings {
		localBinds[bk(bd.PlatformBaseURL, bd.Model, bd.TokenHash)] = bd
	}
	for _, bd := range local.Bindings {
		merged.Bindings = append(merged.Bindings, bd)
		if _, ok := indexBinding(center.Bindings, bd.PlatformBaseURL, bd.Model, bd.TokenHash); !ok {
			sum.KeptLocal["endpoint_credential"]++
			sum.AddsToCenter = true
		}
	}
	for _, bd := range center.Bindings {
		k := bk(bd.PlatformBaseURL, bd.Model, bd.TokenHash)
		if lb, ok := localBinds[k]; ok {
			if !reflect.DeepEqual(lb, bd) {
				sum.addConflict("endpoint_credential", bd.PlatformBaseURL+"#"+bd.Model+"#"+shortHash(bd.TokenHash))
				sum.AddsToCenter = true
			}
			continue
		}
		merged.Bindings = append(merged.Bindings, bd)
		sum.FromCenter["endpoint_credential"]++
	}

	// ---- lapi_rapi_order（键 = lapi + base_url + model）----
	type ordKey struct{ lapi, baseURL, model string }
	okf := func(lapi, baseURL, model string) ordKey { return ordKey{lapi, baseURL, model} }
	localOrders := make(map[ordKey]bundle.LAPIRapiOrder, len(local.LAPIRapiOrder))
	for _, o := range local.LAPIRapiOrder {
		localOrders[okf(o.LAPIAlias, o.RAPIPlatformBaseURL, o.RAPIModel)] = o
	}
	for _, o := range local.LAPIRapiOrder {
		merged.LAPIRapiOrder = append(merged.LAPIRapiOrder, o)
		if _, ok := indexOrder(center.LAPIRapiOrder, o.LAPIAlias, o.RAPIPlatformBaseURL, o.RAPIModel); !ok {
			sum.KeptLocal["lapi_rapi_order"]++
			sum.AddsToCenter = true
		}
	}
	for _, o := range center.LAPIRapiOrder {
		k := okf(o.LAPIAlias, o.RAPIPlatformBaseURL, o.RAPIModel)
		if lo, ok := localOrders[k]; ok {
			if !reflect.DeepEqual(lo, o) {
				sum.addConflict("lapi_rapi_order", o.LAPIAlias+"#"+o.RAPIPlatformBaseURL+"#"+o.RAPIModel)
				sum.AddsToCenter = true
			}
			continue
		}
		merged.LAPIRapiOrder = append(merged.LAPIRapiOrder, o)
		sum.FromCenter["lapi_rapi_order"]++
	}

	return merged, sum
}

func (s *MergeSummary) addConflict(table, key string) {
	if len(s.Conflicts) < maxMergeConflicts {
		s.Conflicts = append(s.Conflicts, MergeConflict{Table: table, Key: key})
	}
}

func shortHash(h string) string {
	if len(h) <= 12 {
		return h
	}
	return h[:12]
}

func indexPlatform(ps []bundle.Platform, baseURL string) (bundle.Platform, bool) {
	for _, p := range ps {
		if p.BaseURL == baseURL {
			return p, true
		}
	}
	return bundle.Platform{}, false
}

func indexCredential(cs []bundle.Credential, hash string) (bundle.Credential, bool) {
	for _, c := range cs {
		if c.TokenHash == hash {
			return c, true
		}
	}
	return bundle.Credential{}, false
}

func indexRAPI(rs []bundle.RAPI, baseURL, model string) (bundle.RAPI, bool) {
	for _, r := range rs {
		if r.PlatformBaseURL == baseURL && r.Model == model {
			return r, true
		}
	}
	return bundle.RAPI{}, false
}

func indexLAPI(ls []bundle.LAPI, alias string) (bundle.LAPI, bool) {
	for _, l := range ls {
		if l.Alias == alias {
			return l, true
		}
	}
	return bundle.LAPI{}, false
}

func indexBinding(bs []bundle.CredentialBinding, baseURL, model, hash string) (bundle.CredentialBinding, bool) {
	for _, b := range bs {
		if b.PlatformBaseURL == baseURL && b.Model == model && b.TokenHash == hash {
			return b, true
		}
	}
	return bundle.CredentialBinding{}, false
}

func indexOrder(os []bundle.LAPIRapiOrder, lapi, baseURL, model string) (bundle.LAPIRapiOrder, bool) {
	for _, o := range os {
		if o.LAPIAlias == lapi && o.RAPIPlatformBaseURL == baseURL && o.RAPIModel == model {
			return o, true
		}
	}
	return bundle.LAPIRapiOrder{}, false
}
