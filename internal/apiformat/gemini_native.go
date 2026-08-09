package apiformat

import (
	"encoding/json"
	"net/url"
	"strings"
)

// GoogleNativeListModelsPath is the list-models endpoint path used by the
// real Google Generative Language API (and Vertex-style mirrors of it).
const GoogleNativeListModelsPath = "/v1beta/models"

// IsGoogleNativeBaseURL reports whether baseURL points at the real Google
// Generative Language API (generativelanguage.googleapis.com) rather than an
// OpenAI-compatible relay that merely happens to speak the Gemini wire format.
//
// We treat any host containing "googleapis.com" with a path/hostname implying
// the generativelanguage endpoint (or a regional aiplatform / Vertex host) as
// "native", because those require x-goog-api-key (or OAuth) auth and the
// /v1beta/models list endpoint that returns models[].name instead of data[].id.
func IsGoogleNativeBaseURL(baseURL string) bool {
	if baseURL == "" {
		return false
	}
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	// generativelanguage.googleapis.com (AI Studio) and <region>-aiplatform.googleapis.com (Vertex).
	if strings.HasSuffix(host, "googleapis.com") &&
		(strings.Contains(host, "generativelanguage") || strings.Contains(host, "aiplatform")) {
		return true
	}
	return false
}

// BuildGoogleListModelsURL constructs the native Google list-models URL.
// It strips any chat-completion/message suffix from baseURL first so that a
// base configured as "https://generativelanguage.googleapis.com/v1beta" still
// works. The API key is appended as ?key= (Google accepts both ?key= and the
// x-goog-api-key header; we send both for robustness).
func BuildGoogleListModelsURL(baseURL, apiKey string) string {
	u := strings.TrimRight(baseURL, "/")
	// Strip known suffixes so we normalise to the API root, including an existing
	// /v1beta path (we re-add it) and OpenAI-style suffixes (defensive: a misconfigured
	// base URL that mixed formats).
	for _, suffix := range []string{
		"/v1/chat/completions",
		"/v1/messages",
		"/v1beta/models",
		"/v1beta",
		"/v1/chat",
		"/v1",
	} {
		if strings.HasSuffix(u, suffix) {
			u = strings.TrimSuffix(u, suffix)
			break
		}
	}
	u += GoogleNativeListModelsPath
	if apiKey != "" {
		u += "?key=" + url.QueryEscape(apiKey)
	}
	return u
}

// ParseGoogleListModelsResponse extracts model IDs from a native Google
// list-models payload. Google returns:
//
//	{"models":[{"name":"models/gemini-2.0-flash","supportedGenerationMethods":[...]}, ...]}
//
// We strip the leading "models/" prefix and only keep entries that actually
// support content generation (generateContent / streamGenerateContent), which
// filters out text/embedding-only models from the model-pick list.
func ParseGoogleListModelsResponse(raw []byte) []string {
	// Decode lazily; we don't want a hard dependency on the exact schema.
	type googleModel struct {
		Name                      string   `json:"name"`
		SupportedGenerationMethods []string `json:"supportedGenerationMethods"`
	}
	var resp struct {
		Models []googleModel `json:"models"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil
	}
	out := make([]string, 0, len(resp.Models))
	for _, m := range resp.Models {
		name := strings.TrimPrefix(m.Name, "models/")
		if name == "" {
			continue
		}
		// Only keep models that can generate content — skip embeddings, etc.
		if !supportsContentGeneration(m.SupportedGenerationMethods) {
			continue
		}
		out = append(out, name)
	}
	return out
}

func supportsContentGeneration(methods []string) bool {
	if len(methods) == 0 {
		// Older endpoints or Vertex sometimes omit the field; keep the model.
		return true
	}
	for _, method := range methods {
		switch method {
		case "generateContent", "streamGenerateContent":
			return true
		}
	}
	return false
}

// GoogleAPIKeyHeader is the header name Google uses for API-key auth.
const GoogleAPIKeyHeader = "x-goog-api-key"

// SetGoogleAuth applies Google-native auth (x-goog-api-key header) to the
// given request header set. It is the Gemini equivalent of OpenAI's
// "Authorization: Bearer" — callers should use this instead of Bearer when the
// target base URL is a real Google endpoint.
//
// req is a minimal abstraction over http.Header so this package stays free of
// a net/http import in hot paths; pass a real http.Header.
func SetGoogleAuth(req headerSetter, apiKey string) {
	req.Set(GoogleAPIKeyHeader, apiKey)
}

// headerSetter is the subset of http.Header we need, for testability.
type headerSetter interface {
	Set(key, value string)
}
