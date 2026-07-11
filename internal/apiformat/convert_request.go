package apiformat

import (
	"encoding/json"
	"fmt"
)

// ConvertRequest converts a request body from one API format to another.
// OpenAI is the canonical internal format. All conversions go through OpenAI as intermediary.
// model is the target RAPI's upstream model name.
func ConvertRequest(body []byte, from APIFormat, to APIFormat, model string) ([]byte, error) {
	// Step 1: normalise to OpenAI (canonical) if needed.
	canonical := body
	var err error
	if from != FormatOpenAI {
		canonical, err = toCanonicalRequest(body, from)
		if err != nil {
			return nil, fmt.Errorf("parse %s request: %w", from, err)
		}
	}

	// Step 2: replace model in canonical body.
	canonical = replaceModelField(canonical, model)

	// Step 3: convert from canonical to target format.
	if to == FormatOpenAI {
		return canonical, nil
	}
	return fromCanonicalRequest(canonical, to, model)
}

// replaceModelField replaces the "model" field value in JSON bytes.
// The model name is normalised to lowercase to match stored RAPI aliases.
func replaceModelField(body []byte, newModel string) []byte {
	if newModel == "" {
		return body
	}
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	m["model"] = newModel
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// ---------- toCanonical (other → OpenAI) ----------

func toCanonicalRequest(body []byte, from APIFormat) ([]byte, error) {
	switch from {
	case FormatAnthropic:
		return anthropicToOpenAIRequest(body)
	case FormatGemini:
		return geminiToOpenAIRequest(body)
	default:
		return body, nil
	}
}

func anthropicToOpenAIRequest(body []byte) ([]byte, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}

	dst := map[string]interface{}{
		"model": getString(src, "model"),
	}

	// messages
	messages := make([]interface{}, 0)
	if system, ok := src["system"]; ok {
		messages = append(messages, map[string]interface{}{
			"role":    "system",
			"content": system,
		})
	}
	if msgs, ok := src["messages"].([]interface{}); ok {
		for _, m := range msgs {
			msg, ok := m.(map[string]interface{})
			if !ok {
				continue
			}
			out := map[string]interface{}{
				"role": getString(msg, "role"),
			}
			// Anthropic content can be string or array of content blocks.
			if content, ok := msg["content"].(string); ok {
				out["content"] = content
			} else if blocks, ok := msg["content"].([]interface{}); ok {
				// Simplify: concatenate text blocks.
				var text string
				for _, b := range blocks {
					block, ok := b.(map[string]interface{})
					if !ok {
						continue
					}
					if t, ok := block["text"].(string); ok {
						text += t
					}
				}
				out["content"] = text
				// Preserve tool_use blocks as tool_calls if present.
				var toolCalls []interface{}
				for _, b := range blocks {
					block, ok := b.(map[string]interface{})
					if !ok {
						continue
					}
					if block["type"] == "tool_use" {
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
				if len(toolCalls) > 0 {
					out["tool_calls"] = toolCalls
					if text == "" {
						delete(out, "content")
					}
				}
			}
			messages = append(messages, out)
		}
	}
	dst["messages"] = messages

	copyIfPresent(src, dst, "stream")
	copyIfPresent(src, dst, "temperature")
	copyIfPresent(src, dst, "top_p")
	if v, ok := src["max_tokens"]; ok {
		dst["max_tokens"] = v
	}
	if v, ok := src["stop_sequences"]; ok {
		dst["stop"] = v
	}

	// tools
	if tools, ok := src["tools"].([]interface{}); ok {
		openaiTools := make([]interface{}, 0, len(tools))
		for _, t := range tools {
			tool, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			openaiTools = append(openaiTools, map[string]interface{}{
				"type": "function",
				"function": map[string]interface{}{
					"name":        getString(tool, "name"),
					"description": getString(tool, "description"),
					"parameters":  tool["input_schema"],
				},
			})
		}
		if len(openaiTools) > 0 {
			dst["tools"] = openaiTools
		}
	}

	return json.Marshal(dst)
}

func geminiToOpenAIRequest(body []byte) ([]byte, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}

	dst := map[string]interface{}{}

	messages := make([]interface{}, 0)

	// systemInstruction
	if si, ok := src["systemInstruction"].(map[string]interface{}); ok {
		text := extractGeminiText(si)
		if text != "" {
			messages = append(messages, map[string]interface{}{
				"role":    "system",
				"content": text,
			})
		}
	}

	// contents
	if contents, ok := src["contents"].([]interface{}); ok {
		for _, c := range contents {
			content, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			role := getString(content, "role")
			if role == "model" {
				role = "assistant"
			}
			messages = append(messages, map[string]interface{}{
				"role":    role,
				"content": extractGeminiText(content),
			})
		}
	}
	dst["messages"] = messages

	// generationConfig
	if gc, ok := src["generationConfig"].(map[string]interface{}); ok {
		if v, ok := gc["temperature"]; ok {
			dst["temperature"] = v
		}
		if v, ok := gc["topP"]; ok {
			dst["top_p"] = v
		}
		if v, ok := gc["maxOutputTokens"]; ok {
			dst["max_tokens"] = v
		}
	}

	// stream hint — set by caller based on URL suffix
	if stream, ok := src["_stream"]; ok {
		dst["stream"] = stream
		delete(dst, "_stream")
	}

	// tools
	if tools, ok := src["tools"].([]interface{}); ok {
		openaiTools := make([]interface{}, 0)
		for _, t := range tools {
			tool, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			if fns, ok := tool["functionDeclarations"].([]interface{}); ok {
				for _, f := range fns {
					fn, ok := f.(map[string]interface{})
					if !ok {
						continue
					}
					openaiTools = append(openaiTools, map[string]interface{}{
						"type": "function",
						"function": map[string]interface{}{
							"name":        getString(fn, "name"),
							"description": getString(fn, "description"),
							"parameters":  fn["parameters"],
						},
					})
				}
			}
		}
		if len(openaiTools) > 0 {
			dst["tools"] = openaiTools
		}
	}

	return json.Marshal(dst)
}

// ---------- fromCanonical (OpenAI → other) ----------

func fromCanonicalRequest(body []byte, to APIFormat, model string) ([]byte, error) {
	switch to {
	case FormatAnthropic:
		return openaiToAnthropicRequest(body, model)
	case FormatGemini:
		return openaiToGeminiRequest(body, model)
	default:
		return body, nil
	}
}

func openaiToAnthropicRequest(body []byte, model string) ([]byte, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}

	dst := map[string]interface{}{
		"model": model,
	}

	// Extract system message → top-level system field.
	var messages []interface{}
	if msgs, ok := src["messages"].([]interface{}); ok {
		for _, m := range msgs {
			msg, ok := m.(map[string]interface{})
			if !ok {
				continue
			}
			if getString(msg, "role") == "system" {
				dst["system"] = msg["content"]
			} else {
				out := map[string]interface{}{
					"role": getString(msg, "role"),
				}
				// Convert content to Anthropic array format.
				if content, ok := msg["content"].(string); ok {
					out["content"] = []interface{}{
						map[string]interface{}{"type": "text", "text": content},
					}
				} else if rawContent, hasContent := msg["content"]; hasContent && rawContent != nil {
					out["content"] = rawContent
				} else {
					out["content"] = []interface{}{
						map[string]interface{}{"type": "text", "text": ""},
					}
				}
				// Handle tool_calls → tool_use content blocks.
				if toolCalls, ok := msg["tool_calls"].([]interface{}); ok {
					blocks := make([]interface{}, 0)
					if content, ok := msg["content"].(string); ok && content != "" {
						blocks = append(blocks, map[string]interface{}{"type": "text", "text": content})
					}
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
						blocks = append(blocks, map[string]interface{}{
							"type":  "tool_use",
							"id":    getString(call, "id"),
							"name":  getString(fn, "name"),
							"input": input,
						})
					}
					out["content"] = blocks
				}
				// Handle tool role (tool results).
				if getString(msg, "role") == "tool" {
					out["role"] = "user"
					out["content"] = []interface{}{
						map[string]interface{}{
							"type":      "tool_result",
							"tool_use_id": getString(msg, "tool_call_id"),
							"content":   msg["content"],
						},
					}
				}
				messages = append(messages, out)
			}
		}
	}
	dst["messages"] = messages

	// max_tokens is required in Anthropic.
	if v, ok := src["max_tokens"]; ok {
		dst["max_tokens"] = v
	} else {
		dst["max_tokens"] = 32768
	}

	copyIfPresent(src, dst, "stream")
	copyIfPresent(src, dst, "temperature")
	copyIfPresent(src, dst, "top_p")
	if v, ok := src["stop"]; ok {
		dst["stop_sequences"] = v
	}

	// tools
	if tools, ok := src["tools"].([]interface{}); ok {
		anthropicTools := make([]interface{}, 0, len(tools))
		for _, t := range tools {
			tool, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			fn, _ := tool["function"].(map[string]interface{})
			anthropicTools = append(anthropicTools, map[string]interface{}{
				"name":         getString(fn, "name"),
				"description":  getString(fn, "description"),
				"input_schema": fn["parameters"],
			})
		}
		if len(anthropicTools) > 0 {
			dst["tools"] = anthropicTools
		}
	}

	return json.Marshal(dst)
}

func openaiToGeminiRequest(body []byte, model string) ([]byte, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}

	dst := map[string]interface{}{}

	var contents []interface{}
	generationConfig := map[string]interface{}{}

	if msgs, ok := src["messages"].([]interface{}); ok {
		for _, m := range msgs {
			msg, ok := m.(map[string]interface{})
			if !ok {
				continue
			}
			role := getString(msg, "role")
			switch role {
			case "system":
				// system → systemInstruction (top-level)
				dst["systemInstruction"] = map[string]interface{}{
					"parts": []interface{}{
						map[string]interface{}{"text": msg["content"]},
					},
				}
			default:
				geminiRole := role
				if role == "assistant" {
					geminiRole = "model"
				}
				content := map[string]interface{}{
					"role": geminiRole,
					"parts": []interface{}{
						map[string]interface{}{"text": msg["content"]},
					},
				}
				contents = append(contents, content)
			}
		}
	}
	dst["contents"] = contents

	// generationConfig
	if v, ok := src["temperature"]; ok {
		generationConfig["temperature"] = v
	}
	if v, ok := src["top_p"]; ok {
		generationConfig["topP"] = v
	}
	if v, ok := src["max_tokens"]; ok {
		generationConfig["maxOutputTokens"] = v
	}
	if len(generationConfig) > 0 {
		dst["generationConfig"] = generationConfig
	}

	// tools
	if tools, ok := src["tools"].([]interface{}); ok {
		funcDecls := make([]interface{}, 0)
		for _, t := range tools {
			tool, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			fn, _ := tool["function"].(map[string]interface{})
			funcDecls = append(funcDecls, map[string]interface{}{
				"name":        getString(fn, "name"),
				"description": getString(fn, "description"),
				"parameters":  fn["parameters"],
			})
		}
		if len(funcDecls) > 0 {
			dst["tools"] = []interface{}{
				map[string]interface{}{"functionDeclarations": funcDecls},
			}
		}
	}

	return json.Marshal(dst)
}

// ---------- helpers ----------

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func extractGeminiText(m map[string]interface{}) string {
	if parts, ok := m["parts"].([]interface{}); ok {
		var text string
		for _, p := range parts {
			if part, ok := p.(map[string]interface{}); ok {
				if t, ok := part["text"].(string); ok {
					text += t
				}
			}
		}
		return text
	}
	return ""
}

func copyIfPresent(src, dst map[string]interface{}, key string) {
	if v, ok := src[key]; ok {
		dst[key] = v
	}
}

func toJSONString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
