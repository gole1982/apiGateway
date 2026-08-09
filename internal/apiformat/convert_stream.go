package apiformat

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// StreamConverter reads SSE events from one format and writes them in another.
type StreamConverter struct {
	reader       io.Reader
	writer       io.Writer
	from         APIFormat
	to           APIFormat
	model        string
	flusher      interface{ Flush() }
	requestID    string
	writtenFrames int // number of data: frames actually sent to the client

	// OpenAI→Anthropic protocol state. message_start must be emitted exactly
	// once and content_block_start must precede the first text delta; track
	// them here because the per-chunk conversion is otherwise stateless.
	anthropicMsgStarted   bool
	anthropicBlockStarted bool // text content_block is open
	anthropicBlockIndex   int  // next content_block index to assign
	anthropicToolOpen     bool // a tool_use content_block is currently open
	anthropicToolIdxMap   map[int]int // OpenAI tool_calls[i].index → Anthropic block index

	// Gemini→OpenAI state: first chunk must carry role.
	geminiFirstSent bool
}

// NewStreamConverter creates a converter for streaming SSE responses.
func NewStreamConverter(reader io.Reader, writer io.Writer, from APIFormat, to APIFormat, model string) *StreamConverter {
	return &StreamConverter{
		reader:            reader,
		writer:            writer,
		from:              from,
		to:                to,
		model:             model,
		anthropicToolIdxMap: make(map[int]int),
	}
}

// SetFlusher sets an optional flusher to call after each write.
func (sc *StreamConverter) SetFlusher(f interface{ Flush() }) {
	sc.flusher = f
}

// Run reads all SSE events, converts them, and writes to the output.
// Blocks until the input stream ends or an error occurs.
func (sc *StreamConverter) Run() error {
	scanner := bufio.NewScanner(sc.reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // up to 1 MB per line
	var eventLines []string

	for scanner.Scan() {
		line := scanner.Text()

		// SSE events are separated by blank lines.
		if line == "" {
			if len(eventLines) > 0 {
				if err := sc.processEventBlock(eventLines); err != nil {
					return err
				}
				eventLines = eventLines[:0]
			}
			continue
		}
		eventLines = append(eventLines, line)
	}

	// Process any remaining event block.
	if len(eventLines) > 0 {
		sc.processEventBlock(eventLines)
	}

	return scanner.Err()
}

func (sc *StreamConverter) processEventBlock(lines []string) error {
	var eventData string
	for _, line := range lines {
		if strings.HasPrefix(line, "data: ") {
			eventData = strings.TrimPrefix(line, "data: ")
		} else if strings.HasPrefix(line, "data:") {
			eventData = strings.TrimPrefix(line, "data:")
		}
	}

	if eventData == "" {
		return nil
	}
	if eventData == "[DONE]" {
		// OpenAI→Anthropic: [DONE] is redundant — message_stop was already emitted
		// by openaiChunkToAnthropic when finish_reason was set. Forwarding it would
		// cause the client to see "D" (the 'D' in DONE) as an invalid JSON character.
		if sc.from == FormatOpenAI && sc.to == FormatAnthropic {
			return nil
		}
		sc.writeSSE("[DONE]")
		return nil
	}

	var converted string
	var err error

	switch {
	case sc.from == FormatAnthropic && sc.to == FormatOpenAI:
		converted, err = sc.anthropicChunkToOpenAI(eventData)
	case sc.from == FormatGemini && sc.to == FormatOpenAI:
		converted, err = sc.geminiChunkToOpenAI(eventData)
	case sc.from == FormatOpenAI && sc.to == FormatAnthropic:
		converted, err = sc.openaiChunkToAnthropic(eventData)
	case sc.from == FormatOpenAI && sc.to == FormatGemini:
		converted, err = sc.openaiChunkToGemini(eventData)
	default:
		// Same format or OpenAI→OpenAI: pass through.
		converted = eventData
	}

	if err != nil {
		// Skip unparseable chunks rather than aborting the stream.
		return nil
	}
	if converted == "" {
		return nil
	}
	if converted == "[DONE]" {
		sc.writeSSE("[DONE]")
		return nil
	}

	// Multi-event sentinel: converters join multiple events with NUL bytes so that
	// each becomes its own SSE frame. Split and emit each part individually.
	if strings.IndexByte(converted, 0) >= 0 {
		for _, part := range strings.Split(converted, "\x00") {
			if part != "" {
				sc.writeSSE(part)
			}
		}
		return nil
	}

	sc.writeSSE(converted)
	return nil
}

// Written reports how many SSE data frames were sent to the client.
// A value of 0 after Run() means the upstream produced no convertible content.
func (sc *StreamConverter) Written() int { return sc.writtenFrames }

func (sc *StreamConverter) writeSSE(data string) {
	fmt.Fprintf(sc.writer, "data: %s\n\n", data)
	sc.writtenFrames++
	if sc.flusher != nil {
		sc.flusher.Flush()
	}
}

// ---------- Anthropic SSE → OpenAI SSE ----------

func (sc *StreamConverter) anthropicChunkToOpenAI(data string) (string, error) {
	var event map[string]interface{}
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		return "", err
	}

	eventType := getString(event, "type")
	id := getString(event, "message_id")
	if id == "" {
		// Try nested message.id for message_start.
		if msg, ok := event["message"].(map[string]interface{}); ok {
			id = getString(msg, "id")
		}
	}
	if id == "" {
		id = fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	}

	chunk := map[string]interface{}{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   sc.model,
	}

	switch eventType {
	case "message_start":
		chunk["choices"] = []interface{}{
			map[string]interface{}{
				"index": 0,
				"delta": map[string]interface{}{
					"role": "assistant",
				},
				"finish_reason": nil,
			},
		}

	case "content_block_start":
		// If this is a tool_use block, emit the tool call metadata (id + name).
		if cb, ok := event["content_block"].(map[string]interface{}); ok {
			if getString(cb, "type") == "tool_use" {
				// Track the Anthropic block index → OpenAI tool_call index mapping.
				blockIdx := 0
				if idx, ok := event["index"].(float64); ok {
					blockIdx = int(idx)
				}
				// The tool_call index in OpenAI is the order of tool blocks (0-based).
				// Reuse an existing mapping if this block index was seen before
				// (e.g. on a retransmitted content_block_start) so arguments from
				// subsequent deltas attach to the right tool call. Allocating a fresh
				// index every time would create gaps and mis-attach arguments.
				toolCallIdx, seen := sc.anthropicToolIdxMap[blockIdx]
				if !seen {
					toolCallIdx = len(sc.anthropicToolIdxMap)
					sc.anthropicToolIdxMap[blockIdx] = toolCallIdx
				}

				chunk["choices"] = []interface{}{
					map[string]interface{}{
						"index": 0,
						"delta": map[string]interface{}{
							"tool_calls": []interface{}{
								map[string]interface{}{
									"index": toolCallIdx,
									"id":    getString(cb, "id"),
									"type":  "function",
									"function": map[string]interface{}{
										"name":      getString(cb, "name"),
										"arguments": "",
									},
								},
							},
						},
						"finish_reason": nil,
					},
				}
			} else {
				// text block start — no OpenAI equivalent needed, skip.
				return "", nil
			}
		} else {
			return "", nil
		}

	case "content_block_delta":
		delta, _ := event["delta"].(map[string]interface{})
		deltaType := getString(delta, "type")
		if deltaType == "text_delta" {
			chunk["choices"] = []interface{}{
				map[string]interface{}{
					"index": 0,
					"delta": map[string]interface{}{
						"content": getString(delta, "text"),
					},
					"finish_reason": nil,
				},
			}
		} else if deltaType == "input_json_delta" {
			// Tool use streaming — map Anthropic block index to OpenAI tool_call index.
			blockIdx := 0
			if idx, ok := event["index"].(float64); ok {
				blockIdx = int(idx)
			}
			toolCallIdx := sc.anthropicToolIdxMap[blockIdx]

			chunk["choices"] = []interface{}{
				map[string]interface{}{
					"index": 0,
					"delta": map[string]interface{}{
						"tool_calls": []interface{}{
							map[string]interface{}{
								"index": toolCallIdx,
								"function": map[string]interface{}{
									"arguments": getString(delta, "partial_json"),
								},
							},
						},
					},
					"finish_reason": nil,
				},
			}
		} else {
			return "", nil
		}

	case "message_delta":
		delta, _ := event["delta"].(map[string]interface{})
		stopReason := getString(delta, "stop_reason")
		finishReason := "stop"
		switch stopReason {
		case "max_tokens":
			finishReason = "length"
		case "tool_use":
			finishReason = "tool_calls"
		case "end_turn":
			finishReason = "stop"
		case "refusal":
			// Mirror the non-streaming path (convert_response.go) which maps
			// refusal → content_filter. Without this the content-filter signal is
			// silently lost and the client sees a normal "stop".
			finishReason = "content_filter"
		}

		choice := map[string]interface{}{
			"index":         0,
			"delta":         map[string]interface{}{},
			"finish_reason": finishReason,
		}
		chunk["choices"] = []interface{}{choice}

		// Include usage if available.
		if usage, ok := event["usage"].(map[string]interface{}); ok {
			chunk["usage"] = map[string]interface{}{
				"completion_tokens": usage["output_tokens"],
			}
		}

	case "message_stop":
		return "[DONE]", nil

	case "content_block_stop", "ping":
		return "", nil

	default:
		return "", nil
	}

	out, err := json.Marshal(chunk)
	return string(out), err
}

// ---------- Gemini SSE → OpenAI SSE ----------

func (sc *StreamConverter) geminiChunkToOpenAI(data string) (string, error) {
	var event map[string]interface{}
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		return "", err
	}

	chunk := map[string]interface{}{
		"id":      fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano()),
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   sc.model,
	}

	var deltaContent string
	var finishReason interface{}
	var toolCalls []interface{}

	if candidates, ok := event["candidates"].([]interface{}); ok && len(candidates) > 0 {
		cand, _ := candidates[0].(map[string]interface{})

		if content, ok := cand["content"].(map[string]interface{}); ok {
			deltaContent = extractGeminiText(content)

			// Handle functionCall parts.
			if parts, ok := content["parts"].([]interface{}); ok {
				for _, p := range parts {
					part, ok := p.(map[string]interface{})
					if !ok {
						continue
					}
					if fnCall, ok := part["functionCall"].(map[string]interface{}); ok {
						tc := map[string]interface{}{
							"index": 0,
							"type":  "function",
							"function": map[string]interface{}{
								"name":      getString(fnCall, "name"),
								"arguments": toJSONString(fnCall["args"]),
							},
						}
						toolCalls = append(toolCalls, tc)
					}
				}
			}
		}

		if fr := getString(cand, "finishReason"); fr != "" && fr != "STOP" {
			switch fr {
			case "MAX_TOKENS":
				finishReason = "length"
			case "SAFETY":
				finishReason = "content_filter"
			default:
				finishReason = "stop"
			}
		} else if _, ok := cand["finishReason"]; ok {
			finishReason = "stop"
		}
	}

	delta := map[string]interface{}{}
	// First chunk must carry role for OpenAI clients.
	if !sc.geminiFirstSent {
		delta["role"] = "assistant"
		sc.geminiFirstSent = true
	}
	if deltaContent != "" {
		delta["content"] = deltaContent
	}
	if len(toolCalls) > 0 {
		delta["tool_calls"] = toolCalls
	}
	chunk["choices"] = []interface{}{
		map[string]interface{}{
			"index":         0,
			"delta":         delta,
			"finish_reason": finishReason,
		},
	}

	// Include usage if present.
	if um, ok := event["usageMetadata"].(map[string]interface{}); ok {
		chunk["usage"] = map[string]interface{}{
			"prompt_tokens":     um["promptTokenCount"],
			"completion_tokens": um["candidatesTokenCount"],
		}
	}

	out, err := json.Marshal(chunk)
	if err != nil {
		return "", err
	}

	// When this chunk carries a finish_reason (i.e. the stream is ending),
	// append a [DONE] sentinel so OpenAI clients see the stream properly
	// terminated. Use the NUL sentinel trick so processEventBlock emits two
	// separate SSE frames: the JSON chunk followed by "data: [DONE]".
	if finishReason != nil {
		return string(out) + "\x00[DONE]", nil
	}
	return string(out), nil
}

// ---------- OpenAI SSE → Anthropic SSE ----------

func (sc *StreamConverter) openaiChunkToAnthropic(data string) (string, error) {
	var chunk map[string]interface{}
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return "", err
	}

	// OpenAI chunks may not have choices.
	choices, ok := chunk["choices"].([]interface{})
	if !ok || len(choices) == 0 {
		return "", nil
	}

	choice, _ := choices[0].(map[string]interface{})
	delta, _ := choice["delta"].(map[string]interface{})
	finishReason := getString(choice, "finish_reason")

	id := getString(chunk, "id")

	// Some upstream models (e.g. DeepSeek) collapse role + content, or content +
	// finish_reason into a single chunk. Collect all events that need to be emitted
	// and join them with the NUL sentinel so processEventBlock sends each as its own
	// SSE frame, instead of the old early-return approach that silently dropped fields.
	var parts []string

	// message_start must be emitted exactly once, before any content. Standard
	// OpenAI streams only carry "role" in the first chunk, but some upstreams
	// (e.g. GLM via Nvidia) repeat it in every chunk — emitting message_start
	// each time makes strict Anthropic clients treat each delta as a brand-new
	// message and abort, so only the first character ever reaches the user.
	if role := getString(delta, "role"); role != "" && !sc.anthropicMsgStarted {
		parts = append(parts, toJSON(sc.anthropicMessageStart(id)))
		sc.anthropicMsgStarted = true
	}

	// --- Text content ---
	if content, ok := delta["content"].(string); ok && content != "" {
		// Guarantee message_start even if the upstream never sent a role chunk.
		if !sc.anthropicMsgStarted {
			parts = append(parts, toJSON(sc.anthropicMessageStart(id)))
			sc.anthropicMsgStarted = true
		}
		// Anthropic requires content_block_start before the first text delta.
		if !sc.anthropicBlockStarted {
			blockStart := map[string]interface{}{
				"type":  "content_block_start",
				"index": sc.anthropicBlockIndex,
				"content_block": map[string]interface{}{
					"type": "text",
					"text": "",
				},
			}
			parts = append(parts, toJSON(blockStart))
			sc.anthropicBlockStarted = true
		}
		// Text delta.
		block := map[string]interface{}{
			"type":  "content_block_delta",
			"index": sc.anthropicBlockIndex,
			"delta": map[string]interface{}{
				"type": "text_delta",
				"text": content,
			},
		}
		parts = append(parts, toJSON(block))
	}

	// --- Tool calls ---
	if toolCalls, ok := delta["tool_calls"].([]interface{}); ok && len(toolCalls) > 0 {
		if !sc.anthropicMsgStarted {
			parts = append(parts, toJSON(sc.anthropicMessageStart(id)))
			sc.anthropicMsgStarted = true
		}
		for _, tc := range toolCalls {
			call, ok := tc.(map[string]interface{})
			if !ok {
				continue
			}
			// OpenAI tool_calls[i].index identifies which tool call this delta belongs to.
			callIdx := 0
			if idx, ok := call["index"].(float64); ok {
				callIdx = int(idx)
			}

			fn, _ := call["function"].(map[string]interface{})
			toolID := getString(call, "id")
			toolName := ""
			if fn != nil {
				toolName = getString(fn, "name")
			}

			// If this is the first chunk for this tool call (has id + name), emit content_block_start.
			if toolID != "" && toolName != "" {
				// Close any open text block first.
				if sc.anthropicBlockStarted {
					parts = append(parts, toJSON(map[string]interface{}{
						"type": "content_block_stop", "index": sc.anthropicBlockIndex,
					}))
					sc.anthropicBlockIndex++
					sc.anthropicBlockStarted = false
				}
				// Close a previously open tool block (different index).
				if sc.anthropicToolOpen {
					if prevBlock, exists := sc.anthropicToolIdxMap[callIdx]; exists && prevBlock != sc.anthropicBlockIndex {
						parts = append(parts, toJSON(map[string]interface{}{
							"type": "content_block_stop", "index": prevBlock,
						}))
					} else if sc.anthropicToolOpen {
						parts = append(parts, toJSON(map[string]interface{}{
							"type": "content_block_stop", "index": sc.anthropicBlockIndex,
						}))
					}
					sc.anthropicToolOpen = false
				}
				// Open new tool_use block.
				blockIdx := sc.anthropicBlockIndex
				sc.anthropicToolIdxMap[callIdx] = blockIdx
				blockStart := map[string]interface{}{
					"type":  "content_block_start",
					"index": blockIdx,
					"content_block": map[string]interface{}{
						"type":  "tool_use",
						"id":    toolID,
						"name":  toolName,
						"input": map[string]interface{}{},
					},
				}
				parts = append(parts, toJSON(blockStart))
				sc.anthropicBlockIndex++
				sc.anthropicToolOpen = true
			}

			// Emit argument fragments as input_json_delta.
			if fn != nil {
				if args, ok := fn["arguments"].(string); ok && args != "" {
					blockIdx := sc.anthropicToolIdxMap[callIdx]
					block := map[string]interface{}{
						"type":  "content_block_delta",
						"index": blockIdx,
						"delta": map[string]interface{}{
							"type":         "input_json_delta",
							"partial_json": args,
						},
					}
					parts = append(parts, toJSON(block))
				}
			}
		}
	}

	// --- Finish ---
	if finishReason != "" {
		// Close any open content block (text or tool_use).
		if sc.anthropicBlockStarted {
			parts = append(parts, toJSON(map[string]interface{}{
				"type": "content_block_stop", "index": sc.anthropicBlockIndex,
			}))
			sc.anthropicBlockStarted = false
		}
		if sc.anthropicToolOpen {
			// Close the last tool block.
			lastIdx := sc.anthropicBlockIndex - 1
			if lastIdx < 0 {
				lastIdx = 0
			}
			parts = append(parts, toJSON(map[string]interface{}{
				"type": "content_block_stop", "index": lastIdx,
			}))
			sc.anthropicToolOpen = false
		}

		stopReason := "end_turn"
		switch finishReason {
		case "length":
			stopReason = "max_tokens"
		case "tool_calls":
			stopReason = "tool_use"
		case "content_filter":
			stopReason = "end_turn"
		case "stop":
			stopReason = "end_turn"
		}
		deltaEvent := map[string]interface{}{
			"type": "message_delta",
			"delta": map[string]interface{}{
				"stop_reason":   stopReason,
				"stop_sequence": nil,
			},
			"usage": map[string]interface{}{
				"output_tokens": 0,
			},
		}
		stopEvent := map[string]interface{}{
			"type": "message_stop",
		}
		parts = append(parts, toJSON(deltaEvent), toJSON(stopEvent))
	}

	return strings.Join(parts, "\x00"), nil
}

// anthropicMessageStart builds the message_start event that opens an Anthropic
// streaming message.
func (sc *StreamConverter) anthropicMessageStart(id string) map[string]interface{} {
	return map[string]interface{}{
		"type": "message_start",
		"message": map[string]interface{}{
			"id":      id,
			"type":    "message",
			"role":    "assistant",
			"model":   sc.model,
			"content": []interface{}{},
			"usage":   map[string]interface{}{"input_tokens": 0, "output_tokens": 0},
		},
	}
}

// ---------- OpenAI SSE → Gemini SSE ----------

func (sc *StreamConverter) openaiChunkToGemini(data string) (string, error) {
	var chunk map[string]interface{}
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return "", err
	}

	choices, ok := chunk["choices"].([]interface{})
	if !ok || len(choices) == 0 {
		return "", nil
	}

	choice, _ := choices[0].(map[string]interface{})
	delta, _ := choice["delta"].(map[string]interface{})
	finishReason := getString(choice, "finish_reason")

	parts := make([]interface{}, 0)
	if content, ok := delta["content"].(string); ok && content != "" {
		parts = append(parts, map[string]interface{}{"text": content})
	}

	geminiFinish := ""
	if finishReason != "" {
		switch finishReason {
		case "stop":
			geminiFinish = "STOP"
		case "length":
			geminiFinish = "MAX_TOKENS"
		case "content_filter":
			geminiFinish = "SAFETY"
		default:
			geminiFinish = "STOP"
		}
	}

	geminiChunk := map[string]interface{}{
		"candidates": []interface{}{
			map[string]interface{}{
				"content": map[string]interface{}{
					"parts": parts,
					"role":  "model",
				},
			},
		},
	}

	if geminiFinish != "" {
		geminiChunk["candidates"].([]interface{})[0].(map[string]interface{})["finishReason"] = geminiFinish
	}

	return toJSON(geminiChunk), nil
}

func toJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
