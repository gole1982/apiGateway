// Package bundle 定义"中心 ↔ 代理"之间的定义快照契约。
//
// 一个 Bundle 是某次中心版本对应的全部定义类配置（platforms / platform_keys /
// rapis / lapis / lapi_rapi_order），用业务键引用（platform.name、(platform_name,
// key_index)、(platform_name, alias)、lapi.alias），不携带任何自增 id —— 这样
// 代理把它 upsert 进本地 SQLite 时无需关心本地 id 与中心是否一致。
//
// 权威：定义类=中心权威；健康态/遥测=代理本地，**不进 Bundle**。
// 设计：docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md
package bundle

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SchemaVersion 是 Bundle 契约的版本号；中心 get_bundle 与代理 Parse 双向对齐。
// 契约演进时 +1，代理 Parse 校验认识此版本才继续。
const SchemaVersion = 1

// Envelope 是中心 get_bundle RPC 的顶层返回。
// version 与中心 config_meta.version 对应；p_version 相同时中心返回空体/null
// （304 等价短路），Pull 据此跳过 Apply。
type Envelope struct {
	SchemaVersion int    `json:"schema_version"`
	Version       int64  `json:"version"`
	Bundle        Bundle `json:"bundle"`
}

// Bundle 是定义快照本体（envelope.bundle）。
type Bundle struct {
	Platforms     []Platform      `json:"platforms"`
	PlatformKeys  []PlatformKey   `json:"platform_keys"`
	RAPIs         []RAPI           `json:"rapis"`
	LAPIs         []LAPI           `json:"lapis"`
	LAPIRapiOrder []LAPIRapiOrder  `json:"lapi_rapi_order"`
}

// Platform —— 平台定义（业务键 Name）。token/login_password 为 center_key 密文。
type Platform struct {
	Name             string     `json:"name"`
	BaseURL          string     `json:"base_url"`
	Token            string     `json:"token"`            // center_key 密文
	LastTokenFetch   *time.Time `json:"last_token_fetch"` // null = 未拉取
	Enabled          bool       `json:"enabled"`
	Notes            string     `json:"notes"`
	SupportedFormats string     `json:"supported_formats"` // JSON array as TEXT，继承给子 rapi
	FormatEndpoints  string     `json:"format_endpoints"`  // JSON {format:url} as TEXT
	CustomHeaders    string     `json:"custom_headers"`    // JSON array as TEXT
	BillingAddress   string     `json:"billing_address"`
	LoginAccount     string     `json:"login_account"`
	LoginPassword    string     `json:"login_password"` // center_key 密文
	SortOrder        int        `json:"sort_order"`
}

// PlatformKey —— 平台密钥定义（业务键 (PlatformName, KeyIndex)）。token 为 center_key 密文。
type PlatformKey struct {
	PlatformName string     `json:"platform_name"`
	KeyIndex     int        `json:"key_index"`
	Token        string     `json:"token"` // center_key 密文
	Label        string     `json:"label"`
	Enabled      bool       `json:"enabled"`
	ExpiresAt    *time.Time `json:"expires_at"` // null = 永不失效
	IsFree       bool       `json:"is_free"`
}

// RAPI —— 上游模型端点定义（业务键 (PlatformName, Alias)）。
//
// KeyIDs 当前是"平台内 key_index 的 CSV"（业务键，可跨实例解析）。
// 早期中心侧曾存 platform_keys.id（自增，不可移植），管理端写入时已转成
// key_index CSV；Apply 时再解析回本地 id。见 design §4.1 / Phase 2b。
type RAPI struct {
	PlatformName     string `json:"platform_name"`
	Alias            string `json:"alias"`
	Model            string `json:"model"`
	Enabled          bool   `json:"enabled"`
	BaseCost         int    `json:"base_cost"`
	HighCost         int    `json:"high_cost"`
	RPMLimit         int    `json:"rpm_limit"`
	RPHLimit         int    `json:"rph_limit"`
	RPDLimit         int    `json:"rpd_limit"`
	TPMLimit         int    `json:"tpm_limit"`
	TPHLimit         int    `json:"tph_limit"`
	TPDLimit         int    `json:"tpd_limit"`
	TimePeriodRules  string `json:"time_period_rules"`  // JSON array as TEXT
	SupportedFormats string `json:"supported_formats"`  // JSON array as TEXT（继承平台）
	CustomHeaders    string `json:"custom_headers"`     // JSON array as TEXT
	KeyIDs           string `json:"key_ids"`            // key_index CSV（平台内业务键）
	Source           string `json:"source"`            // auto_discover | manual
	Vendor           string `json:"vendor"`
	Series           string `json:"series"`
	ModelName        string `json:"model_name"`
	Version          string `json:"version"`
	Suffix           string `json:"suffix"`
	Notes            string `json:"notes"`
	SortOrder        int    `json:"sort_order"`
}

// LAPI —— 客户端模型接口定义（业务键 Alias）。
type LAPI struct {
	Alias      string `json:"alias"`
	Notes      string `json:"notes"`
	Enabled    bool   `json:"enabled"`
	Vendor     string `json:"vendor"`
	Series     string `json:"series"`
	ModelName  string `json:"model_name"`
	Version    string `json:"version"`
	Suffix     string `json:"suffix"`
}

// LAPIRapiOrder —— 路由链顺序（业务键三元组）。
type LAPIRapiOrder struct {
	LAPIAlias        string `json:"lapi_alias"`
	RAPIPlatformName string `json:"rapi_platform_name"`
	RAPIAlias        string `json:"rapi_alias"`
	OrderIndex       int    `json:"order_index"`
}

// Parse 把中心返回的 JSON 解析成 Envelope。
// 用 DisallowUnknownFields：契约外字段当场报错，避免类型漂移被静默吞掉。
// SchemaVersion 不认识则拒。
func Parse(data []byte) (*Envelope, error) {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	var env Envelope
	if err := dec.Decode(&env); err != nil {
		return nil, fmt.Errorf("bundle: parse: %w", err)
	}
	if env.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("bundle: schema version mismatch: got %d want %d", env.SchemaVersion, SchemaVersion)
	}
	if err := Validate(&env); err != nil {
		return nil, err
	}
	return &env, nil
}

// Validate 校验 Bundle 的引用完整性（替代中心的静态 FK 在导入侧再兜一遍）。
// 任一失败即整体 abort、保留 last_good（见 design §5.4）。
func Validate(env *Envelope) error {
	if env == nil {
		return errors.New("bundle: nil envelope")
	}
	b := &env.Bundle

	// 平台名集合 + 去重
	platSet := make(map[string]struct{}, len(b.Platforms))
	for _, p := range b.Platforms {
		if strings.TrimSpace(p.Name) == "" {
			return fmt.Errorf("bundle: platform with empty name")
		}
		if _, dup := platSet[p.Name]; dup {
			return fmt.Errorf("bundle: duplicate platform name %q", p.Name)
		}
		platSet[p.Name] = struct{}{}
	}

	// 密钥：(platform_name, key_index) 指向存在平台 + 去重
	type pkKey struct{ plat string; idx int }
	pkSeen := make(map[pkKey]struct{}, len(b.PlatformKeys))
	pkByPlatform := make(map[string]map[int]struct{})
	for _, k := range b.PlatformKeys {
		if _, ok := platSet[k.PlatformName]; !ok {
			return fmt.Errorf("bundle: platform_key references unknown platform %q", k.PlatformName)
		}
		kk := pkKey{k.PlatformName, k.KeyIndex}
		if _, dup := pkSeen[kk]; dup {
			return fmt.Errorf("bundle: duplicate platform_key (%s,%d)", k.PlatformName, k.KeyIndex)
		}
		pkSeen[kk] = struct{}{}
		if pkByPlatform[k.PlatformName] == nil {
			pkByPlatform[k.PlatformName] = map[int]struct{}{}
		}
		pkByPlatform[k.PlatformName][k.KeyIndex] = struct{}{}
	}

	// rapi：(platform_name, alias) 指向存在平台 + 去重；key_ids 引用的 key_index 存在
	type rKey struct{ plat, alias string }
	rSeen := make(map[rKey]struct{}, len(b.RAPIs))
	for _, r := range b.RAPIs {
		if _, ok := platSet[r.PlatformName]; !ok {
			return fmt.Errorf("bundle: rapi references unknown platform %q", r.PlatformName)
		}
		rk := rKey{r.PlatformName, r.Alias}
		if strings.TrimSpace(r.Alias) == "" {
			return fmt.Errorf("bundle: rapi with empty alias under platform %q", r.PlatformName)
		}
		if _, dup := rSeen[rk]; dup {
			return fmt.Errorf("bundle: duplicate rapi (%s,%s)", r.PlatformName, r.Alias)
		}
		rSeen[rk] = struct{}{}
		// key_ids：逗号分隔的 key_index（平台内）。空 = 该平台所有 key 都可用。
		if s := strings.TrimSpace(r.KeyIDs); s != "" {
			allowed := pkByPlatform[r.PlatformName]
			for _, raw := range strings.Split(s, ",") {
				raw = strings.TrimSpace(raw)
				if raw == "" {
					continue
				}
				var idx int
				if _, err := fmt.Sscanf(raw, "%d", &idx); err != nil {
					return fmt.Errorf("bundle: rapi (%s,%s) key_ids has non-numeric %q", r.PlatformName, r.Alias, raw)
				}
				if _, ok := allowed[idx]; !ok {
					return fmt.Errorf("bundle: rapi (%s,%s) key_ids references unknown key_index %d", r.PlatformName, r.Alias, idx)
				}
			}
		}
	}

	// lapi：alias 去重
	lSet := make(map[string]struct{}, len(b.LAPIs))
	for _, l := range b.LAPIs {
		if strings.TrimSpace(l.Alias) == "" {
			return errors.New("bundle: lapi with empty alias")
		}
		if _, dup := lSet[l.Alias]; dup {
			return fmt.Errorf("bundle: duplicate lapi alias %q", l.Alias)
		}
		lSet[l.Alias] = struct{}{}
	}

	// lapi_rapi_order：三元组指向存在 lapi + 存在 rapi；(lapi, rapi) 不重
	type oKey struct{ lapi, p, a string }
	oSeen := make(map[oKey]struct{}, len(b.LAPIRapiOrder))
	for _, o := range b.LAPIRapiOrder {
		if _, ok := lSet[o.LAPIAlias]; !ok {
			return fmt.Errorf("bundle: lapi_rapi_order references unknown lapi %q", o.LAPIAlias)
		}
		if _, ok := rSeen[rKey{o.RAPIPlatformName, o.RAPIAlias}]; !ok {
			return fmt.Errorf("bundle: lapi_rapi_order references unknown rapi (%s,%s)", o.RAPIPlatformName, o.RAPIAlias)
		}
		ok2 := oKey{o.LAPIAlias, o.RAPIPlatformName, o.RAPIAlias}
		if _, dup := oSeen[ok2]; dup {
			return fmt.Errorf("bundle: duplicate lapi_rapi_order (%s,%s,%s)", o.LAPIAlias, o.RAPIPlatformName, o.RAPIAlias)
		}
		oSeen[ok2] = struct{}{}
	}

	return nil
}
