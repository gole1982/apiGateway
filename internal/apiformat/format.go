package apiformat

import "encoding/json"

// APIFormat represents a supported API protocol format.
type APIFormat string

const (
	FormatOpenAI    APIFormat = "openai"
	FormatAnthropic APIFormat = "anthropic"
	FormatGemini    APIFormat = "gemini"
)

// AllFormats returns all supported API formats.
func AllFormats() []APIFormat {
	return []APIFormat{FormatOpenAI, FormatAnthropic, FormatGemini}
}

// ValidFormat checks if the given format string is a known API format.
func ValidFormat(f string) bool {
	switch APIFormat(f) {
	case FormatOpenAI, FormatAnthropic, FormatGemini:
		return true
	}
	return false
}

// ParseFormats parses a JSON array string like ["openai","anthropic"] into a slice.
// Returns ["openai"] if the input is empty or invalid.
func ParseFormats(jsonStr string) []APIFormat {
	if jsonStr == "" {
		return []APIFormat{FormatOpenAI}
	}
	var formats []APIFormat
	if err := json.Unmarshal([]byte(jsonStr), &formats); err != nil {
		return []APIFormat{FormatOpenAI}
	}
	if len(formats) == 0 {
		return []APIFormat{FormatOpenAI}
	}
	return formats
}

// FormatsToJSON serialises a slice of APIFormat into a JSON array string.
func FormatsToJSON(formats []APIFormat) string {
	if len(formats) == 0 {
		return `["openai"]`
	}
	b, err := json.Marshal(formats)
	if err != nil {
		return `["openai"]`
	}
	return string(b)
}

// SupportsFormat checks if the format list contains the given format.
func SupportsFormat(formats []APIFormat, f APIFormat) bool {
	for _, v := range formats {
		if v == f {
			return true
		}
	}
	return false
}
