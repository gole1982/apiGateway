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
}

// NewStreamConverter creates a converter for streaming SSE responses.
func NewStreamConverter(reader io.Reader, writer io.Writer, from APIFormat, to APIFormat, model string) *StreamConverter {
	return &StreamConverter{
		reader: reader,
		writer: writer,
		from:   from,
		to:     to,
		model:  model,
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
			// Tool use streaming — emit as partial JSON.
			chunk["choices"] = []interface{}{
				map[string]interface{}{
					"index": 0,
					"delta": map[string]interface{}{
						"tool_calls": []interface{}{
							map[string]interface{}{
								"index": 0,
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

	case "content_block_start", "content_block_stop", "ping":
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

	if candidates, ok := event["candidates"].([]interface{}); ok && len(candidates) > 0 {
		cand, _ := candidates[0].(map[string]interface{})

		if content, ok := cand["content"].(map[string]interface{}); ok {
			deltaContent = extractGeminiText(content)
		}

		if fr := getString(cand, "finishReason"); fr != "" && fr != "STOP" {
			switch fr {
			case "MAX_TOKENS":
				reason := "length"
				finishReason = reason
			case "SAFETY":
				reason := "content_filter"
				finishReason = reason
			default:
				reason := "stop"
				finishReason = reason
			}
		} else if _, ok := cand["finishReason"]; ok {
			reason := "stop"
			finishReason = reason
		}
	}

	delta := map[string]interface{}{}
	if deltaContent != "" {
		delta["content"] = deltaContent
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

	if role := getString(delta, "role"); role != "" {
		// role present → emit message_start.
		startEvent := map[string]interface{}{
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
		parts = append(parts, toJSON(startEvent))
	}

	if content, ok := delta["content"].(string); ok && content != "" {
		// Text delta.
		block := map[string]interface{}{
			"type":  "content_block_delta",
			"index": 0,
			"delta": map[string]interface{}{
				"type": "text_delta",
				"text": content,
			},
		}
		parts = append(parts, toJSON(block))
	}

	if finishReason != "" {
		stopReason := "end_turn"
		switch finishReason {
		case "length":
			stopReason = "max_tokens"
		case "tool_calls":
			stopReason = "tool_use"
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
