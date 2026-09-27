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
//
// v2（2026-09-26，自然键身份模型，见
// docs/superpowers/specs/2026-09-23-natural-key-identity-design.md §5.1）：
// 业务键全换自然键，并让绑定进契约：
//   - platform      键 = base_url（name 降级为显示名，DB 已无 UNIQUE）
//   - credential    键 = token_hash（不再是 (platform, key_index)）
//   - rapi          键 = (platform.base_url, model)（alias 降级为显示名）
//   - lapi          键 = alias（公开模型名，客户端契约，UNIQUE 保留）
//   - 绑定          取代 rapi.key_ids CSV：(endpoint 自然键, token_hash, 限额, enabled)
//
// 刻意不升版本号的旧契约（v1）已随本地自然键迁移失效：v1 用 name/alias 当业务键，
// 而本地表早已去掉这两个 UNIQUE 约束，v1 的 upsert 会在同名/同别名时静默并行。
const SchemaVersion = 2

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
	Credentials   []Credential    `json:"credentials"`
	RAPIs         []RAPI          `json:"rapis"`
	LAPIs         []LAPI          `json:"lapis"`
	LAPIRapiOrder []LAPIRapiOrder `json:"lapi_rapi_order"`
	// Bindings 取代 v1 的 rapi.key_ids CSV（自然键迁移 §3/§6.3）。
	// 空 Bindings = 该端点绑定该平台全部凭据（与旧 key_ids 为空的语义一致）。
	Bindings []CredentialBinding `json:"bindings,omitempty"`
}

// Credential —— 平台密钥定义。**业务键 = TokenHash**（全局唯一，token 内容判据）。
// token 为 center_key 密文；token_hash 是相等性判据（高熵，离线爆破不可行）。
//
// PlatformBaseURL / SortOrder 是**普通属性，不是键的一部分**：token_hash 全局唯一，
// 但凭据仍需声明归属平台（本地 credential.platform_id 是 NOT NULL 外键，且调度器按
// 平台分组轮换），以及平台内的轮换序号。v1 的 key_index 曾兼作 (平台, key_index)
// 复合键的一部分，v2 把它降级为纯属性，复合身份改由 idx_credential_token_hash 承担。
type Credential struct {
	TokenHash       string     `json:"token_hash"`
	PlatformBaseURL string     `json:"platform_base_url"` // 归属平台（引用 platform.base_url）
	SortOrder       int        `json:"sort_order"`        // 平台内轮换序号
	Token           string     `json:"token"`             // center_key 密文
	Label           string     `json:"label"`
	Enabled         bool       `json:"enabled"`
	ExpiresAt       *time.Time `json:"expires_at"` // null = 永不失效
	IsFree          bool       `json:"is_free"`
}

// CredentialBinding —— 端点↔凭据绑定（自然键 (endpoint, token_hash)）。
// 取代 v1 的 rapi.key_ids CSV；限额语义"跟凭据"（自然键迁移 §6.3）。
type CredentialBinding struct {
	PlatformBaseURL string `json:"platform_base_url"`
	Model           string `json:"model"`
	TokenHash       string `json:"token_hash"`
	RPMLimit        int    `json:"rpm_limit,omitempty"`
	RPHLimit        int    `json:"rph_limit,omitempty"`
	RPDLimit        int    `json:"rpd_limit,omitempty"`
	TPMLimit        int    `json:"tpm_limit,omitempty"`
	TPHLimit        int    `json:"tph_limit,omitempty"`
	TPDLimit        int    `json:"tpd_limit,omitempty"`
	Enabled         bool   `json:"enabled"`
}

// Platform —— 平台定义。**业务键 = BaseURL**（name 已降级为显示名，本地表无 UNIQUE）。
// token/login_password 为 center_key 密文。
type Platform struct {
	BaseURL          string     `json:"base_url"` // 业务键（归一化后）
	Name             string     `json:"name"`     // 显示名
	Token            string     `json:"token"`    // center_key 密文
	LastTokenFetch   *time.Time `json:"last_token_fetch"`
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

// RAPI —— 上游模型端点定义。
// **业务键 = (PlatformBaseURL, Model)**；Alias 仅为显示名（本地表已无 UNIQUE 约束）。
// 端点→凭据的绑定不再塞在本结构里（v1 的 key_ids CSV 已废除），
// 改由 Bundle.Bindings 按自然键 (platform_base_url, model, token_hash) 承载。
type RAPI struct {
	PlatformBaseURL  string `json:"platform_base_url"` // 业务键
	Model            string `json:"model"`             // 业务键
	Alias            string `json:"alias"`             // 显示名
	Enabled          bool   `json:"enabled"`
	BaseCost         int    `json:"base_cost"`
	HighCost         int    `json:"high_cost"`
	RPMLimit         int    `json:"rpm_limit"`
	RPHLimit         int    `json:"rph_limit"`
	RPDLimit         int    `json:"rpd_limit"`
	TPMLimit         int    `json:"tpm_limit"`
	TPHLimit         int    `json:"tph_limit"`
	TPDLimit         int    `json:"tpd_limit"`
	TimePeriodRules  string `json:"time_period_rules"` // JSON array as TEXT
	SupportedFormats string `json:"supported_formats"` // JSON array as TEXT（继承平台）
	CustomHeaders    string `json:"custom_headers"`    // JSON array as TEXT
	Source           string `json:"source"`            // auto_discover | manual
	Vendor           string `json:"vendor"`
	Series           string `json:"series"`
	ModelName        string `json:"model_name"`
	Version          string `json:"version"`
	Suffix           string `json:"suffix"`
	Notes            string `json:"notes"`
	SortOrder        int    `json:"sort_order"`
}

// LAPI —— 客户端模型接口定义（业务键 Alias = 公开模型名，客户端契约，UNIQUE 保留）。
type LAPI struct {
	Alias     string `json:"alias"`
	Notes     string `json:"notes"`
	Enabled   bool   `json:"enabled"`
	Vendor    string `json:"vendor"`
	Series    string `json:"series"`
	ModelName string `json:"model_name"`
	Version   string `json:"version"`
	Suffix    string `json:"suffix"`
}

// LAPIRapiOrder —— 路由链顺序（业务键 = (lapi 公开名, 端点自然键 base_url+model)）。
type LAPIRapiOrder struct {
	LAPIAlias           string `json:"lapi_alias"`
	RAPIPlatformBaseURL string `json:"rapi_platform_base_url"`
	RAPIModel           string `json:"rapi_model"`
	OrderIndex          int    `json:"order_index"`
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
//
// v2：全部按自然键校验——platform(base_url)、credential(token_hash)、
// rapi(base_url, model)、lapi(alias)，绑定(endpoint 自然键, token_hash)。
func Validate(env *Envelope) error {
	if env == nil {
		return errors.New("bundle: nil envelope")
	}
	b := &env.Bundle

	// 平台：base_url 归一化后作业务键，去重
	platSet := make(map[string]struct{}, len(b.Platforms))
	for _, p := range b.Platforms {
		bu := platformKey(p.BaseURL)
		if bu == "" {
			return fmt.Errorf("bundle: platform %q has empty base_url", p.Name)
		}
		if _, dup := platSet[bu]; dup {
			return fmt.Errorf("bundle: duplicate platform base_url %q", bu)
		}
		platSet[bu] = struct{}{}
	}

	// 凭据：token_hash 作业务键去重；归属平台必须存在
	credSet := make(map[string]struct{}, len(b.Credentials))
	for _, c := range b.Credentials {
		h := strings.TrimSpace(c.TokenHash)
		if h == "" {
			return fmt.Errorf("bundle: credential with empty token_hash (label=%q)", c.Label)
		}
		if _, dup := credSet[h]; dup {
			return fmt.Errorf("bundle: duplicate credential token_hash %q", h)
		}
		credSet[h] = struct{}{}
		cp := platformKey(c.PlatformBaseURL)
		if _, ok := platSet[cp]; !ok {
			return fmt.Errorf("bundle: credential %s references unknown platform base_url %q", h, c.PlatformBaseURL)
		}
	}

	// 端点：(platform.base_url, model) 作业务键
	type epKey struct{ baseURL, model string }
	epSet := make(map[epKey]struct{}, len(b.RAPIs))
	for _, r := range b.RAPIs {
		bu := platformKey(r.PlatformBaseURL)
		if _, ok := platSet[bu]; !ok {
			return fmt.Errorf("bundle: rapi %q references unknown platform base_url %q", r.Alias, r.PlatformBaseURL)
		}
		if strings.TrimSpace(r.Model) == "" {
			return fmt.Errorf("bundle: rapi with empty model under platform %q", bu)
		}
		ek := epKey{bu, r.Model}
		if _, dup := epSet[ek]; dup {
			return fmt.Errorf("bundle: duplicate rapi (%s,%s)", bu, r.Model)
		}
		epSet[ek] = struct{}{}
	}

	// 绑定：端点与 token_hash 都必须存在，(endpoint, token_hash) 不重
	type bindKey struct {
		ep        epKey
		tokenHash string
	}
	bindSeen := make(map[bindKey]struct{}, len(b.Bindings))
	for _, bd := range b.Bindings {
		ek := epKey{platformKey(bd.PlatformBaseURL), bd.Model}
		if _, ok := epSet[ek]; !ok {
			return fmt.Errorf("bundle: binding references unknown endpoint (%s,%s)", bd.PlatformBaseURL, bd.Model)
		}
		h := strings.TrimSpace(bd.TokenHash)
		if _, ok := credSet[h]; !ok {
			return fmt.Errorf("bundle: binding references unknown token_hash %q", h)
		}
		bk := bindKey{ek, h}
		if _, dup := bindSeen[bk]; dup {
			return fmt.Errorf("bundle: duplicate binding (%s,%s,%s)", bd.PlatformBaseURL, bd.Model, h)
		}
		bindSeen[bk] = struct{}{}
	}

	// lapi：alias = 公开模型名，UNIQUE 保留
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

	// 路由链：(lapi 公开名, base_url, model) 四元组去重 + 引用存在
	type oKey struct {
		lapi, baseURL, model string
	}
	oSeen := make(map[oKey]struct{}, len(b.LAPIRapiOrder))
	for _, o := range b.LAPIRapiOrder {
		if _, ok := lSet[o.LAPIAlias]; !ok {
			return fmt.Errorf("bundle: lapi_rapi_order references unknown lapi %q", o.LAPIAlias)
		}
		ek := epKey{platformKey(o.RAPIPlatformBaseURL), o.RAPIModel}
		if _, ok := epSet[ek]; !ok {
			return fmt.Errorf("bundle: lapi_rapi_order references unknown endpoint (%s,%s)", o.RAPIPlatformBaseURL, o.RAPIModel)
		}
		ok := oKey{o.LAPIAlias, ek.baseURL, ek.model}
		if _, dup := oSeen[ok]; dup {
			return fmt.Errorf("bundle: duplicate lapi_rapi_order (%s,%s,%s)", o.LAPIAlias, o.RAPIPlatformBaseURL, o.RAPIModel)
		}
		oSeen[ok] = struct{}{}
	}

	return nil
}

// platformKey 是平台的业务键。
//
// 刻意**不做归一化**：本地唯一索引建在 platform(base_url) 的**原始列**上
// （migrate_natural_keys.go：CREATE UNIQUE INDEX idx_platform_base_url
// ON platform(base_url) WHERE base_url <> ”），即库把 "https://a" 与
// "https://a/" 视为两个不同的平台。自然键设计 §6.4 原本要求"base_url 归一化后
// 建 UNIQUE"，但归一化从未在写入路径落地。
//
// 契约必须镜像库的实际唯一性语义：若这里做归一化，合并/校验会把库里本就是
// 两行的平台并成一行或判成重复——正是自然键迁移要消除的那类静默丢数据。
// 归一化若要启用，须先在 db 写入路径统一落库，再谈契约。
func platformKey(baseURL string) string { return baseURL }

// canonicalNodeKey 把端点压成 "normalize(base_url)#model"，**仅用于链签名比较**
// （设计 §4：改名不产生假差异，跨实例可直接比较）。
//
// 它不是业务键，不可用于任何合并/落库判定——落库一律用原始 base_url +
// platformKey 语义，否则会与 idx_platform_base_url 打架。
func canonicalNodeKey(baseURL, model string) string {
	s := strings.TrimSpace(baseURL)
	s = strings.TrimRight(s, "/")
	if i := strings.Index(s, "://"); i >= 0 {
		scheme, rest := s[:i], s[i+3:]
		if j := strings.IndexAny(rest, "/?#"); j >= 0 {
			s = strings.ToLower(scheme) + "://" + strings.ToLower(rest[:j]) + rest[j:]
		} else {
			s = strings.ToLower(scheme) + "://" + strings.ToLower(rest)
		}
	}
	return s + "#" + model
}
