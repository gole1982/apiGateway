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
}

// Platform represents an upstream API provider (e.g., OpenAI, Azure, DeepSeek).
// It holds connection-level info: base URL and auth token(s).
type Platform struct {
	ID              int64     `json:"id"`
	Name            string    `json:"name"`
	BaseURL         string    `json:"base_url"`
	// URLAutoComplete controls whether the gateway appends the format-specific path
	// (e.g. /v1/chat/completions) to BaseURL automatically.
	// When false the BaseURL is used verbatim as the upstream endpoint.
	URLAutoComplete bool      `json:"url_auto_complete"`
	Token           string    `json:"token"`
	Notes           string    `json:"notes"`
	// CustomHeaders stores platform-level HTTP headers sent with every upstream request
	// for all RAPIs under this platform. Same JSON format as RAPI.CustomHeaders.
	CustomHeaders   string    `json:"custom_headers,omitempty"`
	LastTokenFetch  time.Time `json:"last_token_fetch"`
	Enabled         bool      `json:"enabled"`
	Available       bool      `json:"available"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// RAPI is a specific model endpoint under a Platform.
// Cost is a property of RAPI (independent of which LAPI references it).
type RAPI struct {
	ID                 int64     `json:"id"`
	Alias              string    `json:"alias"`
	Model              string    `json:"model"`
	Notes              string    `json:"notes"`
	PlatformID         int64     `json:"platform_id"`
	Enabled            bool      `json:"enabled"`
	Available          bool      `json:"available"`
	UnavailableReason  string    `json:"unavailable_reason,omitempty"`
	BaseCost        int       `json:"base_cost"`
	HighCost        int       `json:"high_cost"`
	RPMLimit        int       `json:"rpm_limit"`
	RPHLimit        int       `json:"rph_limit"`
	RPDLimit        int       `json:"rpd_limit"`
	TPMLimit        int       `json:"tpm_limit"`
	TPHLimit        int       `json:"tph_limit"`
	TPDLimit        int       `json:"tpd_limit"`
	SupportedFormats string    `json:"supported_formats"`          // JSON: ["openai","anthropic","gemini"]
	TimePeriodRules  string    `json:"time_period_rules,omitempty"` // JSON: [{"start":"HH:MM","end":"HH:MM","cost":N}]
	// CustomHeaders stores user-defined HTTP headers sent with every upstream request.
	// Stored as JSON: [{"key":"X-My-Header","value":"fixed"},{"key":"X-Ts","value":"{{timestamp}}"}]
	// Supported variable placeholders in value: {{timestamp}} (Unix seconds), {{uuid}} (random UUID).
	CustomHeaders   string    `json:"custom_headers,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`

	// Denormalized fields populated on read (not stored in rapi table)
	Platform *Platform `json:"platform,omitempty"`
}

// RAPIWithPlatform is RAPI with platform fields embedded for convenience.
type RAPIWithPlatform struct {
	// RAPI fields
	ID                int64  `json:"id"`
	Alias             string `json:"alias"`
	Model             string `json:"model"`
	Notes             string `json:"notes"`
	PlatformID        int64  `json:"platform_id"`
	Enabled           bool   `json:"enabled"`
	Available         bool   `json:"available"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
	BaseCost        int    `json:"base_cost"`
	HighCost        int    `json:"high_cost"`
	RPMLimit        int    `json:"rpm_limit"`
	RPHLimit        int    `json:"rph_limit"`
	RPDLimit        int    `json:"rpd_limit"`
	TPMLimit        int    `json:"tpm_limit"`
	TPHLimit        int    `json:"tph_limit"`
	TPDLimit        int    `json:"tpd_limit"`
	SupportedFormats string `json:"supported_formats"`
	TimePeriodRules  string `json:"time_period_rules,omitempty"`
	CustomHeaders    string `json:"custom_headers,omitempty"`
	OrderIndex       int    `json:"order_index"` // from lapi_rapi_order, for sort tie-breaking

	// Platform fields (denormalized)
	PlatformName          string    `json:"platform_name"`
	PlatformCustomHeaders string    `json:"platform_custom_headers,omitempty"`
	BaseURL               string    `json:"base_url"`
	URLAutoComplete       bool      `json:"url_auto_complete"`
	Token                 string    `json:"token"`
	LastTokenFetch        time.Time `json:"last_token_fetch"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`

	// Keys holds the ordered PlatformKeys for this RAPI's platform (populated on demand).
	Keys []PlatformKey `json:"keys,omitempty"`
}

// EffectiveURL returns the normalized full chat completions URL from a platform base URL.
func (r *RAPIWithPlatform) EffectiveURL() string {
	return NormalizeToCompletionsURL(r.BaseURL)
}

// SupportsAPIFormat checks if this RAPI supports the given API format.
// Uses apiformat.ParseFormats for correct JSON-array parsing (Bug 8.8 fix).
func (r *RAPIWithPlatform) SupportsAPIFormat(format string) bool {
	formats := apiformat.ParseFormats(r.SupportedFormats)
	return apiformat.SupportsFormat(formats, apiformat.APIFormat(format))
}

// NormalizeToCompletionsURL takes a base URL (possibly partial) and normalizes it
// to always end with /v1/chat/completions.
func NormalizeToCompletionsURL(rawURL string) string {
	// Trim trailing slashes
	u := rawURL
	for len(u) > 0 && u[len(u)-1] == '/' {
		u = u[:len(u)-1]
	}

	suffixes := []string{"/v1/chat/completions", "/v1/chat", "/v1"}
	for _, s := range suffixes {
		if len(u) >= len(s) && u[len(u)-len(s):] == s {
			// Strip the matched suffix and re-add the full one
			u = u[:len(u)-len(s)]
			break
		}
	}

	return u + "/v1/chat/completions"
}

type LAPI struct {
	ID        int64     `json:"id"`
	Alias     string    `json:"alias"`
	Notes     string    `json:"notes"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

type LAPIRAPIOrder struct {
	ID     int64 `json:"id"`
	LAPIID int64 `json:"lapi_id"`
	RAPIID int64 `json:"rapi_id"`
	Order  int   `json:"order"`
}

type LAPIWithRAPIs struct {
	LAPI   LAPI   `json:"lapi"`
	RAPIs  []RAPI `json:"rapis"`
	Orders []int  `json:"orders"`
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
