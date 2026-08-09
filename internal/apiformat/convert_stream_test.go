package apiformat

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// ---- helpers ----------------------------------------------------------------

// collectSSEFrames parses "data: ...\n\n" lines from raw SSE output.
func collectSSEFrames(raw string) []string {
	var frames []string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "data: ") {
			frames = append(frames, strings.TrimPrefix(line, "data: "))
		}
	}
	return frames
}

func runConverter(t *testing.T, input string, from, to APIFormat) (frames []string, written int) {
	t.Helper()
	var buf bytes.Buffer
	sc := NewStreamConverter(strings.NewReader(input), &buf, from, to, "test-model")
	if err := sc.Run(); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	return collectSSEFrames(buf.String()), sc.Written()
}

// ---- [DONE] suppression (OpenAI→Anthropic) ----------------------------------

// When converting OpenAI SSE → Anthropic SSE, a raw "[DONE]" sentinel from
// the upstream must NOT be forwarded: the Anthropic protocol uses message_stop
// JSON events, not "[DONE]", and forwarding "[DONE]" causes JSON parse errors
// on the client side ("invalid character 'D'").
func TestDONESuppressedForOpenAIToAnthropic(t *testing.T) {
	// Minimal OpenAI stream: one content chunk + finish chunk + [DONE]
	input := "" +
		"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"

	frames, _ := runConverter(t, input, FormatOpenAI, FormatAnthropic)

	for _, f := range frames {
		if f == "[DONE]" {
			t.Errorf("OpenAI→Anthropic: [DONE] must not be forwarded, got frames: %v", frames)
		}
	}

	// The last JSON frame must be message_stop (not [DONE]).
	if len(frames) == 0 {
		t.Fatal("expected at least one frame")
	}
	last := frames[len(frames)-1]
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(last), &obj); err != nil {
		t.Fatalf("last frame is not valid JSON: %q — err: %v", last, err)
	}
	if obj["type"] != "message_stop" {
		t.Errorf("last frame type = %v; want message_stop", obj["type"])
	}
}

// For all OTHER conversion directions, "[DONE]" must be forwarded unchanged.
func TestDONEPassedThroughForAnthropicToOpenAI(t *testing.T) {
	// Minimal Anthropic stream ending with message_stop → [DONE] in OpenAI output.
	input := "" +
		"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n" +
		"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n" +
		"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":1}}\n\n" +
		"data: {\"type\":\"message_stop\"}\n\n"

	frames, _ := runConverter(t, input, FormatAnthropic, FormatOpenAI)

	hasDone := false
	for _, f := range frames {
		if f == "[DONE]" {
			hasDone = true
		}
	}
	if !hasDone {
		t.Errorf("Anthropic→OpenAI: expected [DONE] sentinel in output; got frames: %v", frames)
	}
}

// Pass-through (same format) must forward [DONE] unchanged.
func TestDONEPassThroughSameFormat(t *testing.T) {
	input := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
	frames, _ := runConverter(t, input, FormatOpenAI, FormatOpenAI)
	hasDone := false
	for _, f := range frames {
		if f == "[DONE]" {
			hasDone = true
		}
	}
	if !hasDone {
		t.Errorf("pass-through: expected [DONE] to be forwarded; got frames: %v", frames)
	}
}

// ---- Written() counter -------------------------------------------------------

// An empty input stream must produce Written()==0.
func TestWrittenZeroOnEmptyStream(t *testing.T) {
	var buf bytes.Buffer
	sc := NewStreamConverter(strings.NewReader(""), &buf, FormatOpenAI, FormatOpenAI, "m")
	if err := sc.Run(); err != nil {
		t.Fatal(err)
	}
	if sc.Written() != 0 {
		t.Errorf("Written() = %d; want 0 for empty input", sc.Written())
	}
}

// A stream with only event types that produce no output (ping, content_block_start, etc.)
// must also yield Written()==0.
func TestWrittenZeroOnSkippedEvents(t *testing.T) {
	// Only "ping" and content_block_start/stop — anthropicChunkToOpenAI returns "" for these.
	input := "" +
		"data: {\"type\":\"ping\"}\n\n" +
		"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n"

	_, written := runConverter(t, input, FormatAnthropic, FormatOpenAI)
	if written != 0 {
		t.Errorf("Written() = %d; want 0 when all events are skipped", written)
	}
}

// A stream with one real content delta must yield Written()>=1.
func TestWrittenPositiveOnRealContent(t *testing.T) {
	input := "" +
		"data: {\"type\":\"message_start\",\"message\":{\"id\":\"x\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"m\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n" +
		"data: {\"type\":\"message_stop\"}\n\n"

	_, written := runConverter(t, input, FormatAnthropic, FormatOpenAI)
	if written <= 0 {
		t.Errorf("Written() = %d; want >0 for stream with real content", written)
	}
}

// ---- Anthropic→OpenAI conversion correctness --------------------------------

func TestAnthropicToOpenAIContentDelta(t *testing.T) {
	input := "" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello world\"}}\n\n"

	frames, _ := runConverter(t, input, FormatAnthropic, FormatOpenAI)
	if len(frames) == 0 {
		t.Fatal("expected at least one frame")
	}
	var chunk map[string]interface{}
	if err := json.Unmarshal([]byte(frames[0]), &chunk); err != nil {
		t.Fatalf("frame is not valid JSON: %v", err)
	}
	choices, _ := chunk["choices"].([]interface{})
	if len(choices) == 0 {
		t.Fatal("no choices in chunk")
	}
	choice, _ := choices[0].(map[string]interface{})
	delta, _ := choice["delta"].(map[string]interface{})
	if delta["content"] != "hello world" {
		t.Errorf("content = %v; want %q", delta["content"], "hello world")
	}
}

func TestAnthropicToOpenAIFinishReason(t *testing.T) {
	tests := []struct {
		stopReason     string
		wantFinish     string
	}{
		{"end_turn", "stop"},
		{"max_tokens", "length"},
		{"tool_use", "tool_calls"},
		// Regression: refusal must map to content_filter, mirroring the non-streaming
		// path. Previously the streaming switch fell through and left it as "stop",
		// silently dropping the content-filter signal.
		{"refusal", "content_filter"},
	}
	for _, tt := range tests {
		input := "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"" + tt.stopReason + "\",\"stop_sequence\":null}}\n\n"
		frames, _ := runConverter(t, input, FormatAnthropic, FormatOpenAI)
		if len(frames) == 0 {
			t.Fatalf("stop_reason=%s: no frames", tt.stopReason)
		}
		var chunk map[string]interface{}
		if err := json.Unmarshal([]byte(frames[0]), &chunk); err != nil {
			t.Fatalf("stop_reason=%s: invalid JSON: %v", tt.stopReason, err)
		}
		choices, _ := chunk["choices"].([]interface{})
		if len(choices) == 0 {
			t.Fatalf("stop_reason=%s: no choices", tt.stopReason)
		}
		choice, _ := choices[0].(map[string]interface{})
		if choice["finish_reason"] != tt.wantFinish {
			t.Errorf("stop_reason=%s: finish_reason=%v; want %s", tt.stopReason, choice["finish_reason"], tt.wantFinish)
		}
	}
}

// TestAnthropicToOpenAIToolCallIndexReuseOnRetransmit is a regression test for the
// bug where a repeated content_block_start for an already-seen Anthropic block index
// would allocate a NEW OpenAI tool_call index (via len(map)), creating gaps and
// mis-attaching subsequent argument deltas to the wrong tool call. The fix reuses the
// existing index for a seen blockIdx.
func TestAnthropicToOpenAIToolCallIndexReuseOnRetransmit(t *testing.T) {
	// Construct SSE input where content_block_start for index 0 is sent twice.
	// The second must reuse tool_call index 0.
	events := []string{
		`{"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"c","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":0}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_0","name":"get_weather","input":{}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"loc\":\"SF\"}"}}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call_1","name":"get_time","input":{}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_0","name":"get_weather","input":{}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"loc\":\"SF\"}"}}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null}}`,
		`{"type":"message_stop"}`,
	}
	var sb strings.Builder
	for _, e := range events {
		sb.WriteString("data: ")
		sb.WriteString(e)
		sb.WriteString("\n\n")
	}
	frames, _ := runConverter(t, sb.String(), FormatAnthropic, FormatOpenAI)

	// Collect the tool_call indices that appear in content_block_start-derived chunks.
	// We expect indices {0, 1, 0} — i.e. the retransmitted start for block 0 reuses 0,
	// NOT a new index 2.
	var toolCallIndices []int
	for _, f := range frames {
		var chunk map[string]interface{}
		if err := json.Unmarshal([]byte(f), &chunk); err != nil {
			continue
		}
		choices, _ := chunk["choices"].([]interface{})
		if len(choices) == 0 {
			continue
		}
		choice, _ := choices[0].(map[string]interface{})
		delta, _ := choice["delta"].(map[string]interface{})
		tcs, _ := delta["tool_calls"].([]interface{})
		for _, tc := range tcs {
			call, _ := tc.(map[string]interface{})
			if idx, ok := call["index"].(float64); ok {
				toolCallIndices = append(toolCallIndices, int(idx))
			}
		}
	}
	// The retransmitted start for index 0 must produce tool_call index 0 (reused),
	// not a fresh 2.
	if len(toolCallIndices) == 0 {
		t.Fatalf("no tool_call indices found in output frames; frames=%v", frames)
	}
	// Verify no index exceeds 1 (we only have two distinct tool blocks).
	maxIdx := -1
	for _, i := range toolCallIndices {
		if i > maxIdx {
			maxIdx = i
		}
	}
	if maxIdx > 1 {
		t.Errorf("tool_call index reuse regression: got max index %d (indices=%v); retransmitted block_start allocated a fresh index instead of reusing", maxIdx, toolCallIndices)
	}
}

// ---- OpenAI→Anthropic conversion correctness --------------------------------

func TestOpenAIToAnthropicMessageStart(t *testing.T) {
	// Role chunk → message_start
	input := "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n"
	frames, _ := runConverter(t, input, FormatOpenAI, FormatAnthropic)
	if len(frames) == 0 {
		t.Fatal("expected message_start frame")
	}
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(frames[0]), &obj); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if obj["type"] != "message_start" {
		t.Errorf("type = %v; want message_start", obj["type"])
	}
}

func TestOpenAIToAnthropicContentDelta(t *testing.T) {
	// A content-only chunk (upstream never sent role) must still be framed by a
	// synthesized message_start and content_block_start before the text delta.
	input := "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n"
	frames, _ := runConverter(t, input, FormatOpenAI, FormatAnthropic)

	types := make([]string, 0, len(frames))
	var gotText string
	for _, f := range frames {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(f), &obj); err != nil {
			t.Fatalf("invalid JSON %q: %v", f, err)
		}
		ty, _ := obj["type"].(string)
		types = append(types, ty)
		if ty == "content_block_delta" {
			d, _ := obj["delta"].(map[string]interface{})
			gotText, _ = d["text"].(string)
		}
	}

	if len(types) < 3 || types[0] != "message_start" || types[1] != "content_block_start" || types[2] != "content_block_delta" {
		t.Errorf("frame sequence = %v; want [message_start content_block_start content_block_delta]", types)
	}
	if gotText != "hi" {
		t.Errorf("delta.text = %q; want %q", gotText, "hi")
	}
}

func TestOpenAIToAnthropicFinishEmitsTwoFrames(t *testing.T) {
	// finish_reason chunk → message_delta + message_stop (two frames via NUL sentinel)
	input := "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	frames, _ := runConverter(t, input, FormatOpenAI, FormatAnthropic)
	if len(frames) < 2 {
		t.Fatalf("expected 2 frames (message_delta + message_stop), got %d: %v", len(frames), frames)
	}

	types := make([]string, 0, 2)
	for _, f := range frames {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(f), &obj); err != nil {
			t.Fatalf("frame not valid JSON %q: %v", f, err)
		}
		types = append(types, obj["type"].(string))
	}

	found := map[string]bool{}
	for _, ty := range types {
		found[ty] = true
	}
	if !found["message_delta"] || !found["message_stop"] {
		t.Errorf("expected message_delta and message_stop; got types: %v", types)
	}
}

// ---- OpenAI→Anthropic: combined-field chunks (DeepSeek style) ---------------

// DeepSeek collapses role + content into one chunk. Both message_start and
// content_block_delta must be emitted — the content must NOT be dropped.
func TestOpenAIToAnthropicRoleAndContentSameChunk(t *testing.T) {
	input := "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\"," +
		"\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hello\"},\"finish_reason\":null}]}\n\n"

	frames, _ := runConverter(t, input, FormatOpenAI, FormatAnthropic)

	types := map[string]bool{}
	var contentText string
	for _, f := range frames {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(f), &obj); err != nil {
			t.Fatalf("frame not valid JSON %q: %v", f, err)
		}
		ty, _ := obj["type"].(string)
		types[ty] = true
		if ty == "content_block_delta" {
			d, _ := obj["delta"].(map[string]interface{})
			contentText, _ = d["text"].(string)
		}
	}

	if !types["message_start"] {
		t.Errorf("role+content chunk: expected message_start frame; got types=%v frames=%v", types, frames)
	}
	if !types["content_block_delta"] {
		t.Errorf("role+content chunk: expected content_block_delta frame; content was dropped; got types=%v", types)
	}
	if contentText != "hello" {
		t.Errorf("content_block_delta text = %q; want %q", contentText, "hello")
	}
}

// DeepSeek also collapses content + finish_reason into one chunk. Both
// content_block_delta and message_delta/message_stop must be emitted.
func TestOpenAIToAnthropicContentAndFinishSameChunk(t *testing.T) {
	input := "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\"," +
		"\"choices\":[{\"index\":0,\"delta\":{\"content\":\"world\"},\"finish_reason\":\"stop\"}]}\n\n"

	frames, _ := runConverter(t, input, FormatOpenAI, FormatAnthropic)

	types := map[string]bool{}
	var contentText string
	for _, f := range frames {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(f), &obj); err != nil {
			t.Fatalf("frame not valid JSON %q: %v", f, err)
		}
		ty, _ := obj["type"].(string)
		types[ty] = true
		if ty == "content_block_delta" {
			d, _ := obj["delta"].(map[string]interface{})
			contentText, _ = d["text"].(string)
		}
	}

	if !types["content_block_delta"] {
		t.Errorf("content+finish chunk: expected content_block_delta; got types=%v", types)
	}
	if contentText != "world" {
		t.Errorf("content_block_delta text = %q; want %q", contentText, "world")
	}
	if !types["message_delta"] || !types["message_stop"] {
		t.Errorf("content+finish chunk: expected message_delta and message_stop; got types=%v", types)
	}
}

// Full DeepSeek-style stream: chunk1=role+content, chunk2=content+finish.
// The client must receive all text and a proper close sequence.
func TestOpenAIToAnthropicDeepSeekStyleFullStream(t *testing.T) {
	input := "" +
		"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\"," +
		"\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"你好\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\"," +
		"\"choices\":[{\"index\":0,\"delta\":{\"content\":\"！\"},\"finish_reason\":\"stop\"}]}\n\n"

	frames, _ := runConverter(t, input, FormatOpenAI, FormatAnthropic)

	types := map[string]int{}
	var allText string
	for _, f := range frames {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(f), &obj); err != nil {
			t.Fatalf("frame not valid JSON %q: %v", f, err)
		}
		ty, _ := obj["type"].(string)
		types[ty]++
		if ty == "content_block_delta" {
			d, _ := obj["delta"].(map[string]interface{})
			allText += d["text"].(string)
		}
	}

	if allText != "你好！" {
		t.Errorf("combined text = %q; want %q", allText, "你好！")
	}
	if types["message_start"] == 0 {
		t.Errorf("missing message_start; types=%v", types)
	}
	if types["message_stop"] == 0 {
		t.Errorf("missing message_stop (stream not closed); types=%v", types)
	}
}

// GLM (via Nvidia) repeats "role" in EVERY chunk, unlike standard OpenAI which
// only sends it in the first. The converter must still emit message_start
// exactly once — a duplicate message_start makes strict Anthropic clients treat
// each delta as a new message and abort, so only the first character ("我")
// ever reached the user. Also assert the required content_block_start/stop
// framing around the text deltas.
func TestOpenAIToAnthropicRoleRepeatedEveryChunk(t *testing.T) {
	input := "" +
		"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\"," +
		"\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"我\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\"," +
		"\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"意识到\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\"," +
		"\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":\"stop\"}]}\n\n"

	frames, _ := runConverter(t, input, FormatOpenAI, FormatAnthropic)

	types := map[string]int{}
	var allText string
	firstDeltaIdx, blockStartIdx := -1, -1
	for i, f := range frames {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(f), &obj); err != nil {
			t.Fatalf("frame not valid JSON %q: %v", f, err)
		}
		ty, _ := obj["type"].(string)
		types[ty]++
		if ty == "content_block_delta" {
			if firstDeltaIdx == -1 {
				firstDeltaIdx = i
			}
			d, _ := obj["delta"].(map[string]interface{})
			allText += d["text"].(string)
		}
		if ty == "content_block_start" && blockStartIdx == -1 {
			blockStartIdx = i
		}
	}

	if types["message_start"] != 1 {
		t.Errorf("message_start emitted %d times; want exactly 1 (duplicate aborts strict clients); types=%v", types["message_start"], types)
	}
	if allText != "我意识到" {
		t.Errorf("combined text = %q; want %q (content after first chunk was dropped)", allText, "我意识到")
	}
	if blockStartIdx == -1 {
		t.Errorf("missing content_block_start before text deltas; types=%v", types)
	} else if firstDeltaIdx != -1 && blockStartIdx > firstDeltaIdx {
		t.Errorf("content_block_start (idx %d) must precede first content_block_delta (idx %d)", blockStartIdx, firstDeltaIdx)
	}
	if types["content_block_stop"] != 1 {
		t.Errorf("content_block_stop emitted %d times; want 1; types=%v", types["content_block_stop"], types)
	}
	if types["message_delta"] != 1 || types["message_stop"] != 1 {
		t.Errorf("expected exactly one message_delta and message_stop; types=%v", types)
	}
}



// ---- NUL sentinel split ------------------------------------------------------

func TestNULSentinelProducesTwoSSEFrames(t *testing.T) {
	// openaiChunkToAnthropic returns "A\x00B" which must become two separate frames.
	var buf bytes.Buffer
	sc := &StreamConverter{writer: &buf, from: FormatOpenAI, to: FormatAnthropic, model: "m"}
	sc.writeSSE("frameA")
	sc.writeSSE("frameB")
	frames := collectSSEFrames(buf.String())
	if len(frames) != 2 || frames[0] != "frameA" || frames[1] != "frameB" {
		t.Errorf("expected [frameA frameB], got %v", frames)
	}
}


// ---- OpenAI→Gemini: combined-field chunks (DeepSeek style) ------------------

// DeepSeek role+content chunk: Gemini doesn't use a role frame, so the role is
// silently dropped and only the content part is emitted.
func TestOpenAIToGeminiRoleChunkDropped(t *testing.T) {
	// A role-only chunk produces an empty parts array — Gemini has no role frame.
	input := "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\"," +
		"\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n"

	frames, _ := runConverter(t, input, FormatOpenAI, FormatGemini)
	// One Gemini chunk is emitted (with empty parts) — no panic, no loss of later content.
	if len(frames) != 1 {
		t.Errorf("expected 1 frame for role-only chunk; got %d: %v", len(frames), frames)
	}
}

// DeepSeek content+finishReason chunk: both content and finishReason must appear
// in the single emitted Gemini chunk.
func TestOpenAIToGeminiContentAndFinishSameChunk(t *testing.T) {
	input := "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\"," +
		"\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\n"

	frames, _ := runConverter(t, input, FormatOpenAI, FormatGemini)
	if len(frames) != 1 {
		t.Fatalf("expected 1 Gemini frame; got %d", len(frames))
	}
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(frames[0]), &obj); err != nil {
		t.Fatalf("frame not valid JSON: %v", err)
	}
	candidates, _ := obj["candidates"].([]interface{})
	if len(candidates) == 0 {
		t.Fatal("no candidates")
	}
	cand, _ := candidates[0].(map[string]interface{})

	// content must be present
	content, _ := cand["content"].(map[string]interface{})
	parts, _ := content["parts"].([]interface{})
	if len(parts) == 0 {
		t.Errorf("expected parts with text; got empty")
	} else {
		part, _ := parts[0].(map[string]interface{})
		if part["text"] != "hello" {
			t.Errorf("parts[0].text = %v; want %q", part["text"], "hello")
		}
	}

	// finishReason must be present
	if cand["finishReason"] != "STOP" {
		t.Errorf("finishReason = %v; want STOP", cand["finishReason"])
	}
}

// ---- Anthropic→OpenAI: event types are always separate, no combined-field risk.
// Spot-check: message_start carries no content, message_delta carries finish only.
func TestAnthropicToOpenAINoFieldCollision(t *testing.T) {
	// message_start → role chunk (no content)
	input := "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\"," +
		"\"role\":\"assistant\",\"model\":\"claude-3\",\"content\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n"
	frames, _ := runConverter(t, input, FormatAnthropic, FormatOpenAI)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame; got %d", len(frames))
	}
	var chunk map[string]interface{}
	json.Unmarshal([]byte(frames[0]), &chunk)
	choices, _ := chunk["choices"].([]interface{})
	choice, _ := choices[0].(map[string]interface{})
	delta, _ := choice["delta"].(map[string]interface{})
	// role chunk must have role, must NOT have content
	if delta["role"] != "assistant" {
		t.Errorf("delta.role = %v; want assistant", delta["role"])
	}
	if _, hasContent := delta["content"]; hasContent {
		t.Errorf("message_start chunk must not carry content; got delta=%v", delta)
	}
}


// ---- Gemini→OpenAI conversion correctness -----------------------------------

// The final Gemini chunk (finishReason="STOP") must be followed by [DONE] so
// that OpenAI clients see the stream terminated cleanly (finish=stop, not unknown).
func TestGeminiToOpenAIEmitsDONEOnFinish(t *testing.T) {
	// A minimal two-chunk Gemini stream: a content chunk then a finish chunk.
	input := "" +
		"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"}],\"role\":\"model\"}}]}\n\n" +
		"data: {\"candidates\":[{\"content\":{\"parts\":[],\"role\":\"model\"},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":1}}\n\n"

	frames, _ := runConverter(t, input, FormatGemini, FormatOpenAI)

	hasDone := false
	for _, f := range frames {
		if f == "[DONE]" {
			hasDone = true
		}
	}
	if !hasDone {
		t.Errorf("Gemini→OpenAI: expected [DONE] sentinel after finish chunk; got frames: %v", frames)
	}

	// The second-to-last frame must carry finish_reason="stop".
	if len(frames) < 2 {
		t.Fatalf("expected at least 2 frames (finish chunk + [DONE]), got %d", len(frames))
	}
	finishFrame := frames[len(frames)-2]
	var chunk map[string]interface{}
	if err := json.Unmarshal([]byte(finishFrame), &chunk); err != nil {
		t.Fatalf("finish frame not valid JSON %q: %v", finishFrame, err)
	}
	choices, _ := chunk["choices"].([]interface{})
	if len(choices) == 0 {
		t.Fatal("no choices in finish frame")
	}
	choice, _ := choices[0].(map[string]interface{})
	if choice["finish_reason"] != "stop" {
		t.Errorf("finish_reason = %v; want \"stop\"", choice["finish_reason"])
	}
}

// A Gemini content chunk without finishReason must NOT emit [DONE].
func TestGeminiToOpenAINoSpuriousDONEOnContentChunk(t *testing.T) {
	input := "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hi\"}],\"role\":\"model\"}}]}\n\n"

	frames, _ := runConverter(t, input, FormatGemini, FormatOpenAI)

	for _, f := range frames {
		if f == "[DONE]" {
			t.Errorf("Gemini→OpenAI: [DONE] must not appear for a content-only chunk; got frames: %v", frames)
		}
	}
}
