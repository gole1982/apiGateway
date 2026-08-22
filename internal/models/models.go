package models

import (
	"encoding/json"
	"time"

	"gateway/internal/apiformat"
)

// PlatformKey represents one API key (token) belonging to a Platform.
// A platform may have multiple keys for load-spreading / failover.
type PlatformKey struct {
	ID         int64     `json:"id"`
	PlatformID int64     `json:"platform_id"`
	KeyIndex   int       `json:"key_index"` // 0-based ordering within the platform
	Token      string    `json:"token"`
	Label      string    `json:"label,omitempty"`
	Enabled    bool      `json:"enabled"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	// FailureType classifies the last observed failure: 0=none, 1=temporary(429/5xx), 2=permanent(401/402/403).
	// Set by the gateway on upstream errors; cleared on success or manual probe reset.
	FailureType   int        `json:"failure_type"`
	FailureReason string     `json:"failure_reason,omitempty"`
	FailedAt      *time.Time `json:"failed_at,omitempty"`
	// ExpiresAt is the key's validity deadline. nil/zero means never expires.
	// When now > ExpiresAt the key is treated as disabled: the startup sweep
	// sets enabled=0, and PickAvailableKey also skips it at runtime as a guard.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	// IsFree marks a key as free-tier. Within one platform, PickAvailableKey
	// prefers free keys first; among keys of the same free/paid tier it prefers
	// the one whose ExpiresAt is nearest (so quota is consumed before it lapses).
	IsFree bool `json:"is_free"`
}

// Platform represents an upstream API provider (e.g., OpenAI, Azure, DeepSeek).
// It holds connection-level info: base URL and auth token(s).
type Platform struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	BaseURL string `json:"base_url"`
	Token   string `json:"token"`
	Notes   string `json:"notes"`
	// CustomHeaders stores platform-level HTTP headers sent with every upstream request
	// for all RAPIs under this platform. Same JSON format as RAPI.CustomHeaders.
	CustomHeaders string `json:"custom_headers,omitempty"`
	// SupportedFormats is the platform-level authoritative list of API formats
	// supported by this platform's endpoint (e.g. ["openai","anthropic"]).
	// RAPIs under this platform inherit this value as a cache; the model UI no
	// longer shows per-model formats.
	SupportedFormats string `json:"supported_formats,omitempty"`
	// FormatEndpoints is a JSON object {"format":"url",...} mapping each
	// supported format to the exact upstream URL that detection proved works
	// (e.g. {"openai":"https://api.example.com/v1/chat/completions",
	//        "anthropic":"https://api.example.com/v1/messages"}).
	// The gateway prefers these URLs over BuildURL() so that aggregators with
	// non-standard paths still get correct forwarding. Empty when no detection
	// has been run; the gateway falls back to BuildURL() in that case.
	FormatEndpoints string    `json:"format_endpoints,omitempty"`
	// BillingAddress is the provider's billing console URL — shown in the UI as
	// a clickable external link (alerts can jump straight to the billing page).
	BillingAddress string `json:"billing_address,omitempty"`
	// LoginAccount is the account used to log into the provider console.
	// Returned to the UI on read (non-secret).
	LoginAccount string `json:"login_account,omitempty"`
	// LoginPassword is the provider console password. Stored encrypted
	// (AES-256-GCM, same mechanism as Token) and NEVER selected on read —
	// it is write-only: empty on read, set only via create/update payloads.
	LoginPassword string `json:"login_password,omitempty"`
	LastTokenFetch  time.Time `json:"last_token_fetch"`
	Enabled         bool      `json:"enabled"`
	Available       bool      `json:"available"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// RAPI is a specific model endpoint under a Platform.
// Cost is a property of RAPI (independent of which LAPI references it).
type RAPI struct {
	ID                int64  `json:"id"`
	Alias             string `json:"alias"`
	Model             string `json:"model"`
	Notes             string `json:"notes"`
	Vendor            string `json:"vendor"`  // 厂商 (e.g. "openai", "z.ai")
	Series            string `json:"series"`  // 系列 (e.g. "glm", "claude")
	ModelName         string `json:"model_name"` // 名 (legacy, values migrated into Suffix)
	Version           string `json:"version"` // 版本 (e.g. "5.2", "4.7")
	Suffix            string `json:"suffix"`  // 后缀 (e.g. "luna xhigh", "sonnet")
	PlatformID        int64  `json:"platform_id"`
	Enabled           bool   `json:"enabled"`
	Available         bool   `json:"available"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
	BaseCost          int    `json:"base_cost"`
	HighCost          int    `json:"high_cost"`
	RPMLimit          int    `json:"rpm_limit"`
	RPHLimit          int    `json:"rph_limit"`
	RPDLimit          int    `json:"rpd_limit"`
	TPMLimit          int    `json:"tpm_limit"`
	TPHLimit          int    `json:"tph_limit"`
	TPDLimit          int    `json:"tpd_limit"`
	SupportedFormats  string `json:"supported_formats"`           // JSON: ["openai","anthropic","gemini"]
	TimePeriodRules   string `json:"time_period_rules,omitempty"` // JSON: [{"start":"HH:MM","end":"HH:MM","cost":N}]
	// Source records how this RAPI was created: "auto_discover" (added via the
	// platform's fetch-models sync) or "manual" (hand-added). Auto-discovered
	// models may be removed by re-sync when the platform no longer lists them;
	// manual models are never auto-removed.
	Source string `json:"source"`
	// CustomHeaders stores user-defined HTTP headers sent with every upstream request.
	// Stored as JSON: [{"key":"X-My-Header","value":"fixed"},{"key":"X-Ts","value":"{{timestamp}}"}]
	// Supported variable placeholders in value: {{timestamp}} (Unix seconds), {{uuid}} (random UUID).
	CustomHeaders string `json:"custom_headers,omitempty"`
	// KeyIDs is a comma-separated whitelist of platform_keys IDs this RAPI is
	// allowed to use. Empty means "all platform keys" (the default). This is the
	// model→key capability mapping: only these keys are eligible to serve the
	// model, so key-pool health (KeysHardDead / PickAvailableKey) is judged per
	// RAPI instead of per platform.
	KeyIDs    string    `json:"key_ids,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Denormalized fields populated on read (not stored in rapi table)
	Platform *Platform `json:"platform,omitempty"`
}

// KeyModelBlock records that a specific key is not permitted to serve a
// specific model (platform-side key→model permission). The pair is skipped at
// pick time; a key blocked for one model keeps serving all others. Blocks
// expire after CapabilityBlockSec so a re-granted permission is picked up
// automatically on the next expiry window.
type KeyModelBlock struct {
	KeyID     int64     `json:"key_id"`
	RAPIID    int64     `json:"rapi_id"`
	Reason    string    `json:"reason"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

// RAPIWithPlatform is RAPI with platform fields embedded for convenience.
type RAPIWithPlatform struct {
	// RAPI fields
	ID                int64  `json:"id"`
	Alias             string `json:"alias"`
	Model             string `json:"model"`
	Notes             string `json:"notes"`
	Vendor            string `json:"vendor"`
	Series            string `json:"series"`
	ModelName         string `json:"model_name"`
	Version           string `json:"version"`
	Suffix            string `json:"suffix"`
	PlatformID        int64  `json:"platform_id"`
	Enabled           bool   `json:"enabled"`
	Available         bool   `json:"available"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
	BaseCost          int    `json:"base_cost"`
	HighCost          int    `json:"high_cost"`
	RPMLimit          int    `json:"rpm_limit"`
	RPHLimit          int    `json:"rph_limit"`
	RPDLimit          int    `json:"rpd_limit"`
	TPMLimit          int    `json:"tpm_limit"`
	TPHLimit          int    `json:"tph_limit"`
	TPDLimit          int    `json:"tpd_limit"`
	SupportedFormats  string `json:"supported_formats"`
	TimePeriodRules   string `json:"time_period_rules,omitempty"`
	CustomHeaders     string `json:"custom_headers,omitempty"`
	KeyIDs            string `json:"key_ids,omitempty"`
	Source            string `json:"source"`
	OrderIndex        int    `json:"order_index"` // from lapi_rapi_order, for sort tie-breaking

	// Platform fields (denormalized)
	PlatformName             string    `json:"platform_name"`
	PlatformCustomHeaders    string    `json:"platform_custom_headers,omitempty"`
	PlatformSupportedFormats string    `json:"platform_supported_formats,omitempty"`
	PlatformFormatEndpoints  string    `json:"platform_format_endpoints,omitempty"`
	BaseURL                  string    `json:"base_url"`
	Token                    string    `json:"token"`
	LastTokenFetch           time.Time `json:"last_token_fetch"`
	CreatedAt                time.Time `json:"created_at"`
	UpdatedAt                time.Time `json:"updated_at"`

	// Keys holds the ordered PlatformKeys for this RAPI's platform (populated on demand).
	Keys []PlatformKey `json:"keys,omitempty"`
}

// SupportsAPIFormat checks if this RAPI supports the given API format.
// Prefers the platform-level authority (PlatformSupportedFormats) so that
// minimum-conversion routing stays correct even if the per-RAPI cache
// (SupportedFormats) is stale; falls back to the RAPI-level cache when the
// platform-level field is empty (e.g. legacy rows / not joined in).
// Uses apiformat.ParseFormats for correct JSON-array parsing (Bug 8.8 fix).
func (r *RAPIWithPlatform) SupportsAPIFormat(format string) bool {
	if r.PlatformSupportedFormats != "" && r.PlatformSupportedFormats != "[]" {
		formats := apiformat.ParseFormats(r.PlatformSupportedFormats)
		if apiformat.SupportsFormat(formats, apiformat.APIFormat(format)) {
			return true
		}
		// Platform authority is non-empty and does NOT contain this format:
		// trust it as authoritative (do not fall back to a stale RAPI cache).
		return false
	}
	formats := apiformat.ParseFormats(r.SupportedFormats)
	return apiformat.SupportsFormat(formats, apiformat.APIFormat(format))
}

type LAPI struct {
	ID        int64     `json:"id"`
	Alias     string    `json:"alias"`
	Notes     string    `json:"notes"`
	Vendor    string    `json:"vendor"`  // 厂商
	Series    string    `json:"series"`  // 系列
	ModelName string    `json:"model_name"` // 名 (legacy, values migrated into Suffix)
	Version   string    `json:"version"` // 版本
	Suffix    string    `json:"suffix"`  // 后缀
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

// ComputeModelName builds the unified display name from the four naming
// components: "{vendor}/" + non-empty [series, version, suffix] joined with "-".
// Empty components are skipped entirely (no stray separators). When all four
// components are empty the remark (notes) is used as the name; when the remark
// is empty too, fallback (usually the upstream model string) is returned.
func ComputeModelName(vendor, series, version, suffix, notes, fallback string) string {
	parts := make([]string, 0, 3)
	for _, p := range []string{series, version, suffix} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	name := ""
	if vendor != "" {
		name = vendor + "/"
	}
	name += joinDash(parts)
	if name != "" {
		return name
	}
	if notes != "" {
		return notes
	}
	return fallback
}

func joinDash(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "-"
		}
		out += p
	}
	return out
}

type ProxyRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// contentPart is a single element of an OpenAI multi-modal content array.
type contentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// UnmarshalJSON allows Message.content to be either a plain string or a
// multi-modal content array (e.g. vision messages).  Array parts are
// collapsed: only "text" parts contribute their text, joined with "\n".
func (m *Message) UnmarshalJSON(data []byte) error {
	// Use an alias to avoid infinite recursion.
	type messageAlias Message
	aux := struct {
		messageAlias
		Content json.RawMessage `json:"content"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	m.Role = aux.messageAlias.Role

	if len(aux.Content) == 0 {
		return nil
	}

	// Try plain string first.
	var s string
	if err := json.Unmarshal(aux.Content, &s); err == nil {
		m.Content = s
		return nil
	}

	// Try array of content parts.
	var parts []contentPart
	if err := json.Unmarshal(aux.Content, &parts); err != nil {
		return err
	}
	text := ""
	for _, p := range parts {
		if p.Type == "text" {
			if text != "" {
				text += "\n"
			}
			text += p.Text
		}
	}
	m.Content = text
	return nil
}
