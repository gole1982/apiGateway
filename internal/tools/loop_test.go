package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- test helpers for building OpenAI responses ---

// assistantToolCallResp builds an OpenAI chat completion response whose first
// choice requests one or more tool calls.
func assistantToolCallResp(callID, name, arguments string) []byte {
	resp := map[string]interface{}{
		"choices": []map[string]interface{}{
			{
				"index": 0,
				"message": map[string]interface{}{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]interface{}{
						{
							"id":   callID,
							"type": "function",
							"function": map[string]interface{}{
								"name":      name,
								"arguments": arguments,
							},
						},
					},
				},
				"finish_reason": "tool_calls",
			},
		},
	}
	b, _ := json.Marshal(resp)
	return b
}

// assistantStopResp builds an OpenAI chat completion response that ends the loop.
func assistantStopResp(content string) []byte {
	resp := map[string]interface{}{
		"choices": []map[string]interface{}{
			{
				"index": 0,
				"message": map[string]interface{}{
					"role":    "assistant",
					"content": content,
				},
				"finish_reason": "stop",
			},
		},
	}
	b, _ := json.Marshal(resp)
	return b
}

func startMessages() []interface{} {
	return []interface{}{
		map[string]interface{}{"role": "user", "content": "hi"},
	}
}

func basicLoop(forward ForwardFunc) *Loop {
	return NewLoop(&Registry{cache: map[string]*Tool{}}, DefaultAgentConfig(), forward)
}

// --- Run scenarios ---

func TestLoopRun_NoTools_Stop(t *testing.T) {
	calls := int32(0)
	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return assistantStopResp("hello back"), nil
	}
	loop := basicLoop(forward)

	content, logs, err := loop.Run(context.Background(), startMessages(), nil, "m")
	require.NoError(t, err)
	assert.Equal(t, "hello back", content)
	assert.Empty(t, logs)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestLoopRun_ToolCallThenStop(t *testing.T) {
	// Fake tool registry: the requested tool "echo" exists and is enabled.
	reg := &Registry{cache: map[string]*Tool{
		"echo": {Name: "echo", Enabled: true, ExecutorType: "builtin", ExecutorConfig: `{"handler":"http_get"}`, TimeoutMs: 5000},
	}}

	withSSRFDisabled(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return the url argument verbatim so we can assert it round-trips.
		b, _ := json.Marshal(map[string]interface{}{"echoed": r.URL.Query().Get("v")})
		w.Write(b)
	}))
	defer srv.Close()

	var calls int32
	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return assistantToolCallResp("call_1", "echo", fmt.Sprintf(`{"url":"%s?v=hi"}`, srv.URL)), nil
		}
		return assistantStopResp("done"), nil
	}
	loop := NewLoop(reg, DefaultAgentConfig(), forward)

	content, logs, err := loop.Run(context.Background(), startMessages(), nil, "m")
	require.NoError(t, err)
	assert.Equal(t, "done", content)
	require.Len(t, logs, 1)
	assert.Equal(t, "echo", logs[0].Name)
	assert.Equal(t, 1, logs[0].Iteration)
	assert.Empty(t, logs[0].Error)
	assert.Contains(t, logs[0].Result, "hi")
	// Echo tool returned {"echoed":"hi"}; http_get returns the raw body.
	assert.Contains(t, logs[0].Result, "echoed")
}

func TestLoopRun_MaxIterations(t *testing.T) {
	// Tool always exists; forward always asks for another tool call.
	reg := &Registry{cache: map[string]*Tool{
		"loop_tool": {Name: "loop_tool", Enabled: true, ExecutorType: "builtin", ExecutorConfig: `{"handler":"http_get"}`, TimeoutMs: 5000},
	}}
	// A server the tool will hit each iteration (SSRF disabled for localhost).
	withSSRFDisabled(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		return assistantToolCallResp("c", "loop_tool", fmt.Sprintf(`{"url":%q}`, srv.URL)), nil
	}
	cfg := DefaultAgentConfig()
	cfg.MaxIterations = 2
	loop := NewLoop(reg, cfg, forward)

	_, logs, err := loop.Run(context.Background(), startMessages(), nil, "m")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max iterations")
	assert.Len(t, logs, 2) // two iterations executed before hitting the cap
}

func TestLoopRun_ForwardError(t *testing.T) {
	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		return nil, errors.New("upstream down")
	}
	loop := basicLoop(forward)
	_, logs, err := loop.Run(context.Background(), startMessages(), nil, "m")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LLM request failed")
	assert.Empty(t, logs)
}

func TestLoopRun_InvalidJSON(t *testing.T) {
	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		return []byte(`not json`), nil
	}
	loop := basicLoop(forward)
	_, _, err := loop.Run(context.Background(), startMessages(), nil, "m")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse LLM response")
}

func TestLoopRun_NoChoices(t *testing.T) {
	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		return []byte(`{"choices":[]}`), nil
	}
	loop := basicLoop(forward)
	_, _, err := loop.Run(context.Background(), startMessages(), nil, "m")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no choices")
}

func TestLoopRun_NullContentTreatedAsEmpty(t *testing.T) {
	// Some upstreams return "content": null on a tool_calls message. The loop
	// must not choke on it. First call requests a tool whose executor succeeds,
	// second call stops with non-empty content.
	reg := &Registry{cache: map[string]*Tool{
		"noop": {Name: "noop", Enabled: true, ExecutorType: "builtin", ExecutorConfig: `{"handler":"http_get"}`, TimeoutMs: 5000},
	}}
	withSSRFDisabled(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	var calls int32
	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			arg, _ := json.Marshal(map[string]string{"url": srv.URL})
			resp := map[string]interface{}{
				"choices": []map[string]interface{}{
					{
						"index": 0,
						"message": map[string]interface{}{
							"role":    "assistant",
							"content": nil,
							"tool_calls": []map[string]interface{}{
								{"id": "c", "type": "function", "function": map[string]interface{}{"name": "noop", "arguments": string(arg)}},
							},
						},
						"finish_reason": "tool_calls",
					},
				},
			}
			b, _ := json.Marshal(resp)
			return b, nil
		}
		return assistantStopResp("final"), nil
	}
	loop := NewLoop(reg, DefaultAgentConfig(), forward)

	content, logs, err := loop.Run(context.Background(), startMessages(), nil, "m")
	require.NoError(t, err)
	assert.Equal(t, "final", content)
	require.Len(t, logs, 1)
}

func TestLoopRun_TotalTimeout(t *testing.T) {
	// Forward sleeps past the loop's total timeout; the ctx deadline fires and
	// the forward surfaces it as a ctx error.
	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		select {
		case <-time.After(200 * time.Millisecond):
			return assistantStopResp("late"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	cfg := DefaultAgentConfig()
	cfg.TotalTimeout = 10 * time.Millisecond
	loop := NewLoop(&Registry{cache: map[string]*Tool{}}, cfg, forward)

	_, _, err := loop.Run(context.Background(), startMessages(), nil, "m")
	require.Error(t, err)
}

func TestLoopRun_MultipleToolCallsInOneTurn(t *testing.T) {
	// Two tool calls returned in a single assistant message are executed and
	// both results appended before the next forward.
	reg := &Registry{cache: map[string]*Tool{
		"a": {Name: "a", Enabled: true, ExecutorType: "builtin", ExecutorConfig: `{"handler":"http_get"}`, TimeoutMs: 5000},
		"b": {Name: "b", Enabled: true, ExecutorType: "builtin", ExecutorConfig: `{"handler":"http_get"}`, TimeoutMs: 5000},
	}}
	withSSRFDisabled(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`hits:` + r.URL.Path))
	}))
	defer srv.Close()

	var calls int32
	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			argA, _ := json.Marshal(map[string]string{"url": srv.URL + "/a"})
			argB, _ := json.Marshal(map[string]string{"url": srv.URL + "/b"})
			resp := map[string]interface{}{
				"choices": []map[string]interface{}{
					{
						"index": 0,
						"message": map[string]interface{}{
							"role":    "assistant",
							"content": "",
							"tool_calls": []map[string]interface{}{
								{"id": "c1", "type": "function", "function": map[string]interface{}{"name": "a", "arguments": string(argA)}},
								{"id": "c2", "type": "function", "function": map[string]interface{}{"name": "b", "arguments": string(argB)}},
							},
						},
						"finish_reason": "tool_calls",
					},
				},
			}
			b, _ := json.Marshal(resp)
			return b, nil
		}
		return assistantStopResp("merged"), nil
	}
	loop := NewLoop(reg, DefaultAgentConfig(), forward)

	content, logs, err := loop.Run(context.Background(), startMessages(), nil, "m")
	require.NoError(t, err)
	assert.Equal(t, "merged", content)
	assert.Len(t, logs, 2)
	names := map[string]bool{}
	for _, l := range logs {
		names[l.Name] = true
	}
	assert.True(t, names["a"])
	assert.True(t, names["b"])
}

func TestLoopRun_ToolNotRegistered(t *testing.T) {
	reg := &Registry{cache: map[string]*Tool{}} // empty; the requested tool is unknown
	var calls int32
	forward := func(ctx context.Context, body []byte) ([]byte, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return assistantToolCallResp("c", "ghost", `{}`), nil
		}
		return assistantStopResp("done"), nil
	}
	loop := NewLoop(reg, DefaultAgentConfig(), forward)

	content, logs, err := loop.Run(context.Background(), startMessages(), nil, "m")
	// Tool failure does not abort the loop: the error becomes a tool message
	// and the loop continues, ending on the following stop response.
	require.NoError(t, err)
	assert.Equal(t, "done", content)
	require.Len(t, logs, 1)
	assert.Contains(t, logs[0].Error, "not registered")
}

// --- extractLastAssistantContent ---

func TestExtractLastAssistantContent(t *testing.T) {
	t.Run("returns last assistant content", func(t *testing.T) {
		msgs := []interface{}{
			map[string]interface{}{"role": "user", "content": "u"},
			map[string]interface{}{"role": "assistant", "content": "a1"},
			map[string]interface{}{"role": "user", "content": "u2"},
			map[string]interface{}{"role": "assistant", "content": "a2"},
		}
		assert.Equal(t, "a2", extractLastAssistantContent(msgs))
	})
	t.Run("no assistant -> empty", func(t *testing.T) {
		msgs := []interface{}{
			map[string]interface{}{"role": "user", "content": "u"},
		}
		assert.Equal(t, "", extractLastAssistantContent(msgs))
	})
	t.Run("empty -> empty", func(t *testing.T) {
		assert.Equal(t, "", extractLastAssistantContent(nil))
	})
	t.Run("non-map entry is skipped", func(t *testing.T) {
		msgs := []interface{}{
			map[string]interface{}{"role": "assistant", "content": "x"},
			"garbage",
		}
		assert.Equal(t, "x", extractLastAssistantContent(msgs))
	})
}
