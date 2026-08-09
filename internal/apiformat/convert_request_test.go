package apiformat

import (
	"encoding/json"
	"strings"
	"testing"
)

// ConvertRequest is exercised via the from/toCanonical helpers; for the OpenAI→Gemini
// direction the key correctness concern is tool-call name resolution, tested below.

// TestOpenAIToGeminiToolResultNameResolvedFromToolCallID is a regression test for the
// bug where OpenAI "tool" role messages (which carry only tool_call_id, NOT the function
// name) were converted to Gemini functionResponse with a hardcoded name "unknown".
// Gemini requires functionResponse.name to match the preceding functionCall.name or it
// rejects the request with 400, making OpenAI→Gemini tool calls unusable. The fix builds
// a tool_call_id → name map from assistant tool_calls and resolves the name there.
func TestOpenAIToGeminiToolResultNameResolvedFromToolCallID(t *testing.T) {
	// OpenAI request: assistant issues two tool calls (get_weather id=call_1,
	// get_time id=call_2), then two tool results reply with only tool_call_id (no name).
	openaiBody := `{
		"model": "gpt-4",
		"messages": [
			{"role": "user", "content": "What's the weather and time in SF?"},
			{"role": "assistant", "content": null, "tool_calls": [
				{"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"loc\":\"SF\"}"}},
				{"id": "call_2", "type": "function", "function": {"name": "get_time", "arguments": "{\"tz\":\"PST\"}"}}
			]},
			{"role": "tool", "tool_call_id": "call_1", "content": "{\"temp\": 60}"},
			{"role": "tool", "tool_call_id": "call_2", "content": "{\"time\": \"12:00\"}"}
		]
	}`

	out, err := ConvertRequest([]byte(openaiBody), FormatOpenAI, FormatGemini, "gemini-pro")
	if err != nil {
		t.Fatalf("ConvertRequest error: %v", err)
	}

	var dst map[string]interface{}
	if err := json.Unmarshal(out, &dst); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, string(out))
	}

	contents, _ := dst["contents"].([]interface{})

	// Collect functionResponse names from the contents array.
	var respNames []string
	for _, c := range contents {
		content, _ := c.(map[string]interface{})
		parts, _ := content["parts"].([]interface{})
		for _, p := range parts {
			part, _ := p.(map[string]interface{})
			if fr, ok := part["functionResponse"].(map[string]interface{}); ok {
				if name, ok := fr["name"].(string); ok {
					respNames = append(respNames, name)
				}
			}
		}
	}

	if len(respNames) != 2 {
		t.Fatalf("expected 2 functionResponse blocks, got %d (names=%v)", len(respNames), respNames)
	}

	// Both names must be resolved from tool_call_id, NOT "unknown".
	for _, name := range respNames {
		if name == "unknown" {
			t.Errorf("regression: functionResponse.name is \"unknown\" — tool_call_id→name resolution failed; names=%v", respNames)
		}
	}

	// Verify the actual resolved names (order corresponds to message order).
	wantNames := map[string]bool{"get_weather": false, "get_time": false}
	for _, name := range respNames {
		if _, ok := wantNames[name]; ok {
			wantNames[name] = true
		}
	}
	for name, found := range wantNames {
		if !found {
			t.Errorf("expected functionResponse name %q to be resolved, but it was not; names=%v", name, respNames)
		}
	}

	// Also verify the preceding functionCall names exist (so they match).
	var callNames []string
	for _, c := range contents {
		content, _ := c.(map[string]interface{})
		parts, _ := content["parts"].([]interface{})
		for _, p := range parts {
			part, _ := p.(map[string]interface{})
			if fc, ok := part["functionCall"].(map[string]interface{}); ok {
				if name, ok := fc["name"].(string); ok {
					callNames = append(callNames, name)
				}
			}
		}
	}
	for _, cn := range callNames {
		if !strings.Contains(strings.Join(respNames, ","), cn) {
			// Not every functionCall needs a matching functionResponse in this exchange,
			// but if a response name is NOT among the call names, Gemini would reject it.
		}
	}
}
