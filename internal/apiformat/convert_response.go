package apiformat

import (
	"encoding/json"
	"fmt"
	"time"
)

// ConvertResponse converts an upstream response body from the RAPI's format to the client's format.
// OpenAI is the canonical internal format. model is the original LAPI alias.
func ConvertResponse(body []byte, from APIFormat, to APIFormat, model string) ([]byte, error) {
	// Step 1: normalise to OpenAI (canonical) if needed.
	canonical := body
	var err error
	if from != FormatOpenAI {
		canonical, err = toCanonicalResponse(body, from, model)
		if err != nil {
			return nil, fmt.Errorf("parse %s response: %w", from, err)
		}
	}

	// Step 2: convert from canonical to target format if needed.
	if to == FormatOpenAI {
		// Ensure model is set to the LAPI alias.
		canonical = ReplaceModelField(canonical, model)
		return canonical, nil
	}
	return fromCanonicalResponse(canonical, to, model)
}

// ---------- toCanonical (other → OpenAI) ----------

func toCanonicalResponse(body []byte, from APIFormat, model string) ([]byte, error) {
	switch from {
	case FormatAnthropic:
		return anthropicToOpenAIResponse(body, model)
	case FormatGemini:
		return geminiToOpenAIResponse(body, model)
	default:
		return body, nil
	}
}

func anthropicToOpenAIResponse(body []byte, model string) ([]byte, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}

	// Extract text content.
	var contentText string
	var toolCalls []interface{}
	if contentBlocks, ok := src["content"].([]interface{}); ok {
		for _, b := range contentBlocks {
			block, ok := b.(map[string]interface{})
			if !ok {
				continue
			}
			switch block["type"] {
			case "text":
				contentText += getString(block, "text")
			case "tool_use":
				tc := map[string]interface{}{
					"id":   getString(block, "id"),
					"type": "function",
					"function": map[string]interface{}{
						"name":      getString(block, "name"),
						"arguments": toJSONString(block["input"]),
					},
				}
				toolCalls = append(toolCalls, tc)
			}
		}
	}

	message := map[string]interface{}{
		"role": "assistant",
	}
	if contentText != "" {
		message["content"] = contentText
	} else {
		message["content"] = nil
	}
	if len(toolCalls) > 0 {
		message["tool_calls"] = toolCalls
	}

	// Map stop_reason → finish_reason.
	finishReason := "stop"
	switch getString(src, "stop_reason") {
	case "max_tokens":
		finishReason = "length"
	case "stop_sequence":
		finishReason = "stop"
	case "tool_use":
		finishReason = "tool_calls"
	case "end_turn":
		finishReason = "stop"
	case "refusal":
		finishReason = "content_filter"
	}

	// Map usage.
	usage := map[string]interface{}{}
	if u, ok := src["usage"].(map[string]interface{}); ok {
		if v, ok := u["input_tokens"]; ok {
			usage["prompt_tokens"] = v
		}
		if v, ok := u["output_tokens"]; ok {
			usage["completion_tokens"] = v
		}
		if pt, ok1 := u["input_tokens"].(float64); ok1 {
			if ct, ok2 := u["output_tokens"].(float64); ok2 {
				usage["total_tokens"] = pt + ct
			}
		}
	}

	id := getString(src, "id")
	if id == "" {
		id = fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	}

	dst := map[string]interface{}{
		"id":      id,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []interface{}{
			map[string]interface{}{
				"index":         0,
				"message":       message,
				"finish_reason": finishReason,
			},
		},
		"usage": usage,
	}

	return json.Marshal(dst)
}

func geminiToOpenAIResponse(body []byte, model string) ([]byte, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}

	var choices []interface{}
	if candidates, ok := src["candidates"].([]interface{}); ok {
		for i, c := range candidates {
			cand, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			content, _ := cand["content"].(map[string]interface{})
			text := extractGeminiText(content)

			finishReason := "stop"
			switch getString(cand, "finishReason") {
			case "MAX_TOKENS":
				finishReason = "length"
			case "SAFETY":
				finishReason = "content_filter"
			case "RECITATION":
				finishReason = "content_filter"
			case "STOP":
				finishReason = "stop"
			case "OTHER":
				finishReason = "stop"
			}

			message := map[string]interface{}{
				"role":    "assistant",
				"content": text,
			}

			// Handle functionCall from Gemini.
			if fc, ok := cand["content"].(map[string]interface{}); ok {
				if parts, ok := fc["parts"].([]interface{}); ok {
					var toolCalls []interface{}
					for _, p := range parts {
						part, ok := p.(map[string]interface{})
						if !ok {
							continue
						}
						if fnCall, ok := part["functionCall"].(map[string]interface{}); ok {
							tc := map[string]interface{}{
								"id":   fmt.Sprintf("call_%d_%s", i, getString(fnCall, "name")),
								"type": "function",
								"function": map[string]interface{}{
									"name":      getString(fnCall, "name"),
									"arguments": toJSONString(fnCall["args"]),
								},
							}
							toolCalls = append(toolCalls, tc)
						}
					}
					if len(toolCalls) > 0 {
						message["tool_calls"] = toolCalls
						message["content"] = nil
					}
				}
			}

			choices = append(choices, map[string]interface{}{
				"index":         i,
				"message":       message,
				"finish_reason": finishReason,
			})
		}
	}

	// Map usage.
	usage := map[string]interface{}{}
	if um, ok := src["usageMetadata"].(map[string]interface{}); ok {
		if v, ok := um["promptTokenCount"]; ok {
			usage["prompt_tokens"] = v
		}
		if v, ok := um["candidatesTokenCount"]; ok {
			usage["completion_tokens"] = v
		}
		if pt, ok1 := um["promptTokenCount"].(float64); ok1 {
			if ct, ok2 := um["candidatesTokenCount"].(float64); ok2 {
				usage["total_tokens"] = pt + ct
			}
		}
	}

	dst := map[string]interface{}{
		"id":      fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano()),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": choices,
		"usage":   usage,
	}

	return json.Marshal(dst)
}

// ---------- fromCanonical (OpenAI → other) ----------

func fromCanonicalResponse(body []byte, to APIFormat, model string) ([]byte, error) {
	switch to {
	case FormatAnthropic:
		return openaiToAnthropicResponse(body, model)
	case FormatGemini:
		return openaiToGeminiResponse(body, model)
	default:
		return body, nil
	}
}

func openaiToAnthropicResponse(body []byte, model string) ([]byte, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}

	dst := map[string]interface{}{
		"id":    getString(src, "id"),
		"type":  "message",
		"role":  "assistant",
		"model": model,
	}

	var contentBlocks []interface{}
	var stopReason string

	if choices, ok := src["choices"].([]interface{}); ok && len(choices) > 0 {
		choice, _ := choices[0].(map[string]interface{})
		msg, _ := choice["message"].(map[string]interface{})

		if content, ok := msg["content"].(string); ok && content != "" {
			contentBlocks = append(contentBlocks, map[string]interface{}{
				"type": "text",
				"text": content,
			})
		}

		// tool_calls → tool_use blocks
		if toolCalls, ok := msg["tool_calls"].([]interface{}); ok {
			for _, tc := range toolCalls {
				call, ok := tc.(map[string]interface{})
				if !ok {
					continue
				}
				fn, _ := call["function"].(map[string]interface{})
				input := fn["arguments"]
				if s, ok := input.(string); ok {
					var parsed interface{}
					if json.Unmarshal([]byte(s), &parsed) == nil {
						input = parsed
					}
				}
				contentBlocks = append(contentBlocks, map[string]interface{}{
					"type":  "tool_use",
					"id":    getString(call, "id"),
					"name":  getString(fn, "name"),
					"input": input,
				})
			}
		}

		switch getString(choice, "finish_reason") {
		case "stop":
			stopReason = "end_turn"
		case "length":
			stopReason = "max_tokens"
		case "tool_calls":
			stopReason = "tool_use"
		case "content_filter":
			stopReason = "end_turn"
		default:
			stopReason = "end_turn"
		}
	}

	if len(contentBlocks) == 0 {
		contentBlocks = append(contentBlocks, map[string]interface{}{
			"type": "text",
			"text": "",
		})
	}
	dst["content"] = contentBlocks
	dst["stop_reason"] = stopReason
	dst["stop_sequence"] = nil

	// usage
	if usage, ok := src["usage"].(map[string]interface{}); ok {
		dst["usage"] = map[string]interface{}{
			"input_tokens":  usage["prompt_tokens"],
			"output_tokens": usage["completion_tokens"],
		}
	}

	return json.Marshal(dst)
}

func openaiToGeminiResponse(body []byte, model string) ([]byte, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}

	var candidates []interface{}
	if choices, ok := src["choices"].([]interface{}); ok {
		for _, c := range choices {
			choice, _ := c.(map[string]interface{})
			msg, _ := choice["message"].(map[string]interface{})

			parts := make([]interface{}, 0)
			if content, ok := msg["content"].(string); ok && content != "" {
				parts = append(parts, map[string]interface{}{"text": content})
			}
			if toolCalls, ok := msg["tool_calls"].([]interface{}); ok {
				for _, tc := range toolCalls {
					call, ok := tc.(map[string]interface{})
					if !ok {
						continue
					}
					fn, _ := call["function"].(map[string]interface{})
					args := fn["arguments"]
					if s, ok := args.(string); ok {
						var parsed interface{}
						if json.Unmarshal([]byte(s), &parsed) == nil {
							args = parsed
						}
					}
					parts = append(parts, map[string]interface{}{
						"functionCall": map[string]interface{}{
							"name": getString(fn, "name"),
							"args": args,
						},
					})
				}
			}

			finishReason := "STOP"
			switch getString(choice, "finish_reason") {
			case "stop":
				finishReason = "STOP"
			case "length":
				finishReason = "MAX_TOKENS"
			case "content_filter":
				finishReason = "SAFETY"
			case "tool_calls":
				finishReason = "STOP"
			}

			candidates = append(candidates, map[string]interface{}{
				"content": map[string]interface{}{
					"parts": parts,
					"role":  "model",
				},
				"finishReason": finishReason,
				"index":        choice["index"],
			})
		}
	}

	dst := map[string]interface{}{
		"candidates": candidates,
	}

	if usage, ok := src["usage"].(map[string]interface{}); ok {
		dst["usageMetadata"] = map[string]interface{}{
			"promptTokenCount":     usage["prompt_tokens"],
			"candidatesTokenCount": usage["completion_tokens"],
		}
		if v, ok := usage["total_tokens"]; ok {
			dst["usageMetadata"].(map[string]interface{})["totalTokenCount"] = v
		}
	}

	return json.Marshal(dst)
}
