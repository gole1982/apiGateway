package models

import "time"

// Platform represents an upstream API provider (e.g., OpenAI, Azure, DeepSeek).
// It holds connection-level info: base URL, auth token, dynamic refresh settings.
type Platform struct {
	ID             int64     `json:"id"`
	Name           string    `json:"name"`
	BaseURL        string    `json:"base_url"`
	Token          string    `json:"token"`
	IsDynamic      bool      `json:"is_dynamic"`
	TokenCommand   string    `json:"token_command,omitempty"`
	LastTokenFetch time.Time `json:"last_token_fetch"`
	Enabled        bool      `json:"enabled"`
	Available      bool      `json:"available"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// RAPI is a specific model endpoint under a Platform.
// Cost is a property of RAPI (independent of which LAPI references it).
type RAPI struct {
	ID              int64     `json:"id"`
	Alias           string    `json:"alias"`
	Model           string    `json:"model"`
	PlatformID      int64     `json:"platform_id"`
	Enabled         bool      `json:"enabled"`
	Available       bool      `json:"available"`
	BaseCost        int       `json:"base_cost"`
	HighCost        int       `json:"high_cost"`
	RPMLimit        int       `json:"rpm_limit"`
	RPHLimit        int       `json:"rph_limit"`
	RPDLimit        int       `json:"rpd_limit"`
	TPMLimit        int       `json:"tpm_limit"`
	TPHLimit        int       `json:"tph_limit"`
	TPDLimit        int       `json:"tpd_limit"`
	TimePeriodRules string    `json:"time_period_rules,omitempty"` // JSON: [{"start":"HH:MM","end":"HH:MM","cost":N}]
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`

	// Denormalized fields populated on read (not stored in rapi table)
	Platform *Platform `json:"platform,omitempty"`
}

// RAPIWithPlatform is RAPI with platform fields embedded for convenience.
type RAPIWithPlatform struct {
	// RAPI fields
	ID              int64  `json:"id"`
	Alias           string `json:"alias"`
	Model           string `json:"model"`
	PlatformID      int64  `json:"platform_id"`
	Enabled         bool   `json:"enabled"`
	Available       bool   `json:"available"`
	BaseCost        int    `json:"base_cost"`
	HighCost        int    `json:"high_cost"`
	RPMLimit        int    `json:"rpm_limit"`
	RPHLimit        int    `json:"rph_limit"`
	RPDLimit        int    `json:"rpd_limit"`
	TPMLimit        int    `json:"tpm_limit"`
	TPHLimit        int    `json:"tph_limit"`
	TPDLimit        int    `json:"tpd_limit"`
	TimePeriodRules string `json:"time_period_rules,omitempty"`
	OrderIndex      int    `json:"order_index"` // from lapi_rapi_order, for sort tie-breaking

	// Platform fields (denormalized)
	PlatformName   string    `json:"platform_name"`
	BaseURL        string    `json:"base_url"`
	Token          string    `json:"token"`
	IsDynamic      bool      `json:"is_dynamic"`
	TokenCommand   string    `json:"token_command,omitempty"`
	LastTokenFetch time.Time `json:"last_token_fetch"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// EffectiveURL returns the normalized full chat completions URL from a platform base URL.
func (r *RAPIWithPlatform) EffectiveURL() string {
	return NormalizeToCompletionsURL(r.BaseURL)
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
