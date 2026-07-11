package apiformat

import (
	"fmt"
	"strings"
)

// BuildURL constructs the full endpoint URL for the given API format.
func BuildURL(baseURL string, model string, format APIFormat) string {
	u := strings.TrimRight(baseURL, "/")

	// Strip any existing path suffix so we always start from the root.
	for _, suffix := range []string{
		"/v1/chat/completions",
		"/v1/messages",
		"/v1beta/models/" + model + ":generateContent",
		"/v1/chat",
		"/v1",
	} {
		if strings.HasSuffix(u, suffix) {
			u = strings.TrimSuffix(u, suffix)
			break
		}
	}

	switch format {
	case FormatAnthropic:
		return u + "/v1/messages"
	case FormatGemini:
		return fmt.Sprintf("%s/v1beta/models/%s:generateContent", u, model)
	default: // OpenAI
		return u + "/v1/chat/completions"
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
