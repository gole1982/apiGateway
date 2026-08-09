package apiformat

import (
	"encoding/json"
	"fmt"
	"strings"
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
	canonical = ReplaceModelField(canonical, model)

	// Step 3: convert from canonical to target format.
	if to == FormatOpenAI {
		return canonical, nil
	}
	return fromCanonicalRequest(canonical, to, model)
}

// ReplaceModelField replaces the "model" field value in JSON bytes via a lossless
// json decode/encode round-trip. It is preferred over byte-level substitution, which
// can corrupt user content that happens to contain a "model":"X" substring and misses
// non-standard whitespace.
func ReplaceModelField(body []byte, newModel string) []byte {
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

	// system: can be a string or an array of content blocks.
	if system, ok := src["system"]; ok {
		var systemText string
		switch s := system.(type) {
		case string:
			systemText = s
		case []interface{}:
			// Array of content blocks — extract text.
			for _, b := range s {
				block, ok := b.(map[string]interface{})
				if !ok {
					continue
				}
				if t, ok := block["text"].(string); ok {
					systemText += t
				}
			}
		}
		if systemText != "" {
			messages = append(messages, map[string]interface{}{
				"role":    "system",
				"content": systemText,
			})
		}
	}

	if msgs, ok := src["messages"].([]interface{}); ok {
		for _, m := range msgs {
			msg, ok := m.(map[string]interface{})
			if !ok {
				continue
			}
			role := getString(msg, "role")

			// Anthropic content can be string or array of content blocks.
			if content, ok := msg["content"].(string); ok {
				messages = append(messages, map[string]interface{}{
					"role":    role,
					"content": content,
				})
			} else if blocks, ok := msg["content"].([]interface{}); ok {
				// Separate blocks by type.
				var textParts []string
				var imageParts []interface{}
				var toolCalls []interface{}
				var toolResults []interface{}

				for _, b := range blocks {
					block, ok := b.(map[string]interface{})
					if !ok {
						continue
					}
					switch block["type"] {
					case "text":
						if t, ok := block["text"].(string); ok {
							textParts = append(textParts, t)
						}
					case "image":
						// Convert Anthropic image to OpenAI image_url format.
						imgPart := convertAnthropicImageToOpenAI(block)
						if imgPart != nil {
							imageParts = append(imageParts, imgPart)
						}
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
					case "tool_result":
						toolResults = append(toolResults, block)
					}
				}

				// If this message contains tool_results, emit them as separate
				// OpenAI "tool" role messages (one per result).
				if len(toolResults) > 0 {
					for _, tr := range toolResults {
						result, _ := tr.(map[string]interface{})
						var resultContent string
						switch c := result["content"].(type) {
						case string:
							resultContent = c
						case []interface{}:
							// Array of content blocks inside tool_result.
							for _, rb := range c {
								if rblock, ok := rb.(map[string]interface{}); ok {
									if t, ok := rblock["text"].(string); ok {
										resultContent += t
									}
								}
							}
						}
						messages = append(messages, map[string]interface{}{
							"role":         "tool",
							"tool_call_id": getString(result, "tool_use_id"),
							"content":      resultContent,
						})
					}
					continue
				}

				// Build the output message.
				out := map[string]interface{}{
					"role": role,
				}

				// If we have images, use OpenAI multi-modal content array.
				if len(imageParts) > 0 {
					contentArr := make([]interface{}, 0)
					if len(textParts) > 0 {
						contentArr = append(contentArr, map[string]interface{}{
							"type": "text",
							"text": strings.Join(textParts, ""),
						})
					}
					contentArr = append(contentArr, imageParts...)
					out["content"] = contentArr
				} else {
					out["content"] = strings.Join(textParts, "")
				}

				if len(toolCalls) > 0 {
					out["tool_calls"] = toolCalls
					// If content is empty string and we have tool_calls, set content to null.
					if c, ok := out["content"].(string); ok && c == "" {
						out["content"] = nil
					}
				}

				messages = append(messages, out)
			} else {
				// No content field — pass through with role only.
				messages = append(messages, map[string]interface{}{
					"role": role,
				})
			}
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

	// tool_choice
	if tc, ok := src["tool_choice"].(map[string]interface{}); ok {
		dst["tool_choice"] = convertAnthropicToolChoiceToOpenAI(tc)
	}

	return json.Marshal(dst)
}

// convertAnthropicImageToOpenAI converts an Anthropic image content block to OpenAI image_url format.
func convertAnthropicImageToOpenAI(block map[string]interface{}) map[string]interface{} {
	source, ok := block["source"].(map[string]interface{})
	if !ok {
		return nil
	}
	switch getString(source, "type") {
	case "base64":
		mediaType := getString(source, "media_type")
		data := getString(source, "data")
		if mediaType == "" {
			mediaType = "image/png"
		}
		return map[string]interface{}{
			"type": "image_url",
			"image_url": map[string]interface{}{
				"url": fmt.Sprintf("data:%s;base64,%s", mediaType, data),
			},
		}
	case "url":
		return map[string]interface{}{
			"type": "image_url",
			"image_url": map[string]interface{}{
				"url": getString(source, "url"),
			},
		}
	}
	return nil
}

// convertAnthropicToolChoiceToOpenAI maps Anthropic tool_choice to OpenAI tool_choice.
func convertAnthropicToolChoiceToOpenAI(tc map[string]interface{}) interface{} {
	switch getString(tc, "type") {
	case "auto":
		return "auto"
	case "any":
		return "required"
	case "tool":
		return map[string]interface{}{
			"type":     "function",
			"function": map[string]interface{}{"name": getString(tc, "name")},
		}
	case "none":
		return "none"
	default:
		return "auto"
	}
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

	// stream: Gemini does not carry a stream flag in the JSON body — clients request
	// streaming via the ":streamGenerateContent" URL suffix. The gateway detects that
	// suffix and injects "stream":true into the canonical (OpenAI) body BEFORE calling
	// fromCanonicalRequest, so no extraction is needed here. The "_stream" branch below
	// is kept only as a defensive guard: if a non-standard client happened to set it,
	// honour the value and remove it from the source map (the key lives in src, not dst).
	if stream, ok := src["_stream"]; ok {
		dst["stream"] = stream
		delete(src, "_stream")
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

	// toolConfig → tool_choice
	if tc, ok := src["toolConfig"].(map[string]interface{}); ok {
		if fcc, ok := tc["functionCallingConfig"].(map[string]interface{}); ok {
			dst["tool_choice"] = convertGeminiToolConfigToOpenAI(fcc)
		}
	}

	return json.Marshal(dst)
}

// convertGeminiToolConfigToOpenAI maps Gemini functionCallingConfig to OpenAI tool_choice.
func convertGeminiToolConfigToOpenAI(fcc map[string]interface{}) interface{} {
	mode := getString(fcc, "mode")
	switch mode {
	case "AUTO":
		return "auto"
	case "ANY":
		// If specific function names are allowed, use named tool_choice.
		if names, ok := fcc["allowedFunctionNames"].([]interface{}); ok && len(names) == 1 {
			if name, ok := names[0].(string); ok {
				return map[string]interface{}{
					"type":     "function",
					"function": map[string]interface{}{"name": name},
				}
			}
		}
		return "required"
	case "NONE":
		return "none"
	default:
		return "auto"
	}
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
				continue
			}

			// Handle tool role (tool results) → Anthropic user message with tool_result.
			if getString(msg, "role") == "tool" {
				messages = append(messages, map[string]interface{}{
					"role": "user",
					"content": []interface{}{
						map[string]interface{}{
							"type":        "tool_result",
							"tool_use_id": getString(msg, "tool_call_id"),
							"content":     msg["content"],
						},
					},
				})
				continue
			}

			out := map[string]interface{}{
				"role": getString(msg, "role"),
			}

			// Convert content to Anthropic array format.
			if content, ok := msg["content"].(string); ok {
				out["content"] = []interface{}{
					map[string]interface{}{"type": "text", "text": content},
				}
			} else if contentArr, ok := msg["content"].([]interface{}); ok {
				// OpenAI multi-modal content array → Anthropic content blocks.
				blocks := convertOpenAIContentArrayToAnthropic(contentArr)
				out["content"] = blocks
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
				// Preserve existing text content.
				if existing, ok := out["content"].([]interface{}); ok {
					for _, b := range existing {
						if block, ok := b.(map[string]interface{}); ok {
							if t, ok := block["text"].(string); ok && t != "" {
								blocks = append(blocks, block)
							}
						}
					}
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

			messages = append(messages, out)
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

	// tool_choice
	if tc, ok := src["tool_choice"]; ok {
		dst["tool_choice"] = convertOpenAIToolChoiceToAnthropic(tc)
	}

	return json.Marshal(dst)
}

// convertOpenAIContentArrayToAnthropic converts OpenAI multi-modal content array to Anthropic blocks.
func convertOpenAIContentArrayToAnthropic(arr []interface{}) []interface{} {
	blocks := make([]interface{}, 0, len(arr))
	for _, item := range arr {
		part, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		switch getString(part, "type") {
		case "text":
			blocks = append(blocks, map[string]interface{}{
				"type": "text",
				"text": getString(part, "text"),
			})
		case "image_url":
			imgURL, _ := part["image_url"].(map[string]interface{})
			url := getString(imgURL, "url")
			if strings.HasPrefix(url, "data:") {
				// data:image/png;base64,xxxx → Anthropic base64 source
				mediaType, data := parseDataURL(url)
				blocks = append(blocks, map[string]interface{}{
					"type": "image",
					"source": map[string]interface{}{
						"type":       "base64",
						"media_type": mediaType,
						"data":       data,
					},
				})
			} else if url != "" {
				blocks = append(blocks, map[string]interface{}{
					"type": "image",
					"source": map[string]interface{}{
						"type": "url",
						"url":  url,
					},
				})
			}
		}
	}
	if len(blocks) == 0 {
		blocks = append(blocks, map[string]interface{}{"type": "text", "text": ""})
	}
	return blocks
}

// parseDataURL parses a data URI like "data:image/png;base64,xxxx" into media type and data.
func parseDataURL(url string) (string, string) {
	// Format: data:<mediatype>;base64,<data>
	rest := strings.TrimPrefix(url, "data:")
	semiIdx := strings.Index(rest, ";base64,")
	if semiIdx < 0 {
		return "image/png", rest
	}
	return rest[:semiIdx], rest[semiIdx+8:]
}

// convertOpenAIToolChoiceToAnthropic maps OpenAI tool_choice to Anthropic tool_choice.
func convertOpenAIToolChoiceToAnthropic(tc interface{}) map[string]interface{} {
	switch v := tc.(type) {
	case string:
		switch v {
		case "auto":
			return map[string]interface{}{"type": "auto"}
		case "required":
			return map[string]interface{}{"type": "any"}
		case "none":
			return map[string]interface{}{"type": "none"}
		default:
			return map[string]interface{}{"type": "auto"}
		}
	case map[string]interface{}:
		// {"type":"function","function":{"name":"xxx"}}
		if fn, ok := v["function"].(map[string]interface{}); ok {
			return map[string]interface{}{
				"type": "tool",
				"name": getString(fn, "name"),
			}
		}
		return map[string]interface{}{"type": "auto"}
	default:
		return map[string]interface{}{"type": "auto"}
	}
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
		// Build a tool_call_id → function_name map from all assistant tool_calls so that
		// we can resolve the function name for subsequent OpenAI "tool" messages (which
		// carry only tool_call_id, not the name). Gemini's functionResponse.name MUST
		// match the preceding functionCall.name or the request is rejected with 400.
		toolCallNames := make(map[string]string)
		for _, m := range msgs {
			msg, ok := m.(map[string]interface{})
			if !ok {
				continue
			}
			if getString(msg, "role") != "assistant" {
				continue
			}
			tcs, ok := msg["tool_calls"].([]interface{})
			if !ok {
				continue
			}
			for _, tc := range tcs {
				call, ok := tc.(map[string]interface{})
				if !ok {
					continue
				}
				id := getString(call, "id")
				fnName := ""
				if fn, ok := call["function"].(map[string]interface{}); ok {
					fnName = getString(fn, "name")
				}
				if id != "" {
					toolCallNames[id] = fnName
				}
			}
		}

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
			case "tool":
				// OpenAI tool result → Gemini functionResponse in a user message.
				// Resolve the function name from the preceding assistant tool_call via
				// tool_call_id; only fall back to "unknown" if unresolvable.
				fnName := ""
				if tcID := getString(msg, "tool_call_id"); tcID != "" {
					fnName = toolCallNames[tcID]
				}
				if fnName == "" {
					fnName = getString(msg, "name")
				}
				if fnName == "" {
					fnName = "unknown"
				}
				var responseData interface{}
				if s, ok := msg["content"].(string); ok {
					// Try to parse as JSON for structured response.
					var parsed interface{}
					if json.Unmarshal([]byte(s), &parsed) == nil {
						responseData = parsed
					} else {
						responseData = map[string]interface{}{"result": s}
					}
				} else {
					responseData = msg["content"]
				}
				contents = append(contents, map[string]interface{}{
					"role": "user",
					"parts": []interface{}{
						map[string]interface{}{
							"functionResponse": map[string]interface{}{
								"name":     fnName,
								"response": responseData,
							},
						},
					},
				})
			default:
				geminiRole := role
				if role == "assistant" {
					geminiRole = "model"
				}

				parts := make([]interface{}, 0)

				// Handle content (string or array).
				if content, ok := msg["content"].(string); ok && content != "" {
					parts = append(parts, map[string]interface{}{"text": content})
				} else if contentArr, ok := msg["content"].([]interface{}); ok {
					// Multi-modal content array.
					for _, item := range contentArr {
						part, ok := item.(map[string]interface{})
						if !ok {
							continue
						}
						switch getString(part, "type") {
						case "text":
							parts = append(parts, map[string]interface{}{"text": getString(part, "text")})
						case "image_url":
							imgURL, _ := part["image_url"].(map[string]interface{})
							url := getString(imgURL, "url")
							if strings.HasPrefix(url, "data:") {
								mediaType, data := parseDataURL(url)
								parts = append(parts, map[string]interface{}{
									"inlineData": map[string]interface{}{
										"mimeType": mediaType,
										"data":     data,
									},
								})
							}
						}
					}
				}

				// Handle tool_calls → functionCall parts.
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

				if len(parts) > 0 {
					contents = append(contents, map[string]interface{}{
						"role":  geminiRole,
						"parts": parts,
					})
				}
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
	if v, ok := src["stop"]; ok {
		generationConfig["stopSequences"] = v
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

	// tool_choice → toolConfig
	if tc, ok := src["tool_choice"]; ok {
		dst["toolConfig"] = convertOpenAIToolChoiceToGemini(tc)
	}

	return json.Marshal(dst)
}

// convertOpenAIToolChoiceToGemini maps OpenAI tool_choice to Gemini toolConfig.
func convertOpenAIToolChoiceToGemini(tc interface{}) map[string]interface{} {
	switch v := tc.(type) {
	case string:
		switch v {
		case "auto":
			return map[string]interface{}{
				"functionCallingConfig": map[string]interface{}{"mode": "AUTO"},
			}
		case "required":
			return map[string]interface{}{
				"functionCallingConfig": map[string]interface{}{"mode": "ANY"},
			}
		case "none":
			return map[string]interface{}{
				"functionCallingConfig": map[string]interface{}{"mode": "NONE"},
			}
		default:
			return map[string]interface{}{
				"functionCallingConfig": map[string]interface{}{"mode": "AUTO"},
			}
		}
	case map[string]interface{}:
		if fn, ok := v["function"].(map[string]interface{}); ok {
			return map[string]interface{}{
				"functionCallingConfig": map[string]interface{}{
					"mode":                 "ANY",
					"allowedFunctionNames": []interface{}{getString(fn, "name")},
				},
			}
		}
		return map[string]interface{}{
			"functionCallingConfig": map[string]interface{}{"mode": "AUTO"},
		}
	default:
		return map[string]interface{}{
			"functionCallingConfig": map[string]interface{}{"mode": "AUTO"},
		}
	}
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
