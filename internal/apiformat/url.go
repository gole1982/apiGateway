package apiformat

import (
	"fmt"
	"strings"
)

// BuildURL constructs the full endpoint URL for the given API format.
// Returns the canonical (first) candidate. Use BuildURLs to get all
// candidate URLs for probing.
func BuildURL(baseURL string, model string, format APIFormat) string {
	return BuildURLs(baseURL, model, format)[0]
}

// stripKnownSuffix removes any well-known API path suffix from baseURL so
// subsequent suffix concatenation starts from the platform root.
func stripKnownSuffix(u, model string) string {
	for _, suffix := range []string{
		"/v1/chat/completions",
		"/v1/messages",
		"/v1beta/models/" + model + ":generateContent",
		"/v1beta/models",
		"/v1beta",
		"/v1/chat",
		"/v1",
	} {
		if strings.HasSuffix(u, suffix) {
			return strings.TrimSuffix(u, suffix)
		}
	}
	return u
}

// BuildURLs returns candidate endpoint URLs for the given format, ordered
// from most canonical to most permissive. Detection should try each in order
// and treat the first success as the authoritative endpoint for that format.
// OpenAI / Anthropic each have 2 common variants observed on third-party
// aggregators; Gemini keeps a single canonical path.
func BuildURLs(baseURL string, model string, format APIFormat) []string {
	u := stripKnownSuffix(strings.TrimRight(baseURL, "/"), model)

	switch format {
	case FormatAnthropic:
		return []string{
			u + "/v1/messages",
			u + "/messages",
		}
	case FormatGemini:
		return []string{
			fmt.Sprintf("%s/v1beta/models/%s:generateContent", u, model),
		}
	default: // OpenAI
		return []string{
			u + "/v1/chat/completions",
			u + "/chat/completions",
		}
	}
}

// DetectFormatFromPath determines the client request format from the URL path.
func DetectFormatFromPath(path string) APIFormat {
	switch {
	case strings.HasPrefix(path, "/v1/messages"):
		return FormatAnthropic
	case strings.HasPrefix(path, "/v1beta/models/"):
		return FormatGemini
	default:
		return FormatOpenAI
	}
}

// EnsureGeminiStreamSSE transforms a Gemini generateContent URL into the
// streaming SSE form required by the StreamConverter.
//
// Google's :streamGenerateContent endpoint returns a buffered JSON array of
// chunks by default; only with ?alt=sse does it return proper Server-Sent
// Events (data: <json>\n\n). The StreamConverter expects SSE framing — without
// alt=sse it would parse the JSON array as plain lines, emit zero frames, and
// the client would receive nothing despite an upstream 200.
//
// This is a no-op idempotent transform: if the URL already carries the suffix
// or the query param, it is left alone.
func EnsureGeminiStreamSSE(u string) string {
	if !strings.Contains(u, ":streamGenerateContent") {
		u = strings.Replace(u, ":generateContent", ":streamGenerateContent", 1)
	}
	if !strings.Contains(u, "alt=sse") {
		sep := "?"
		if strings.Contains(u, "?") {
			sep = "&"
		}
		u += sep + "alt=sse"
	}
	return u
}

// ExtractGeminiModel extracts the model name from a Gemini-style URL path.
// e.g. /v1beta/models/gemini-2.0-flash:generateContent -> gemini-2.0-flash
func ExtractGeminiModel(path string) string {
	const prefix = "/v1beta/models/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	idx := strings.Index(rest, ":")
	if idx > 0 {
		return rest[:idx]
	}
	return rest
}
