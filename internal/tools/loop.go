package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// AgentConfig controls the agent loop behaviour.
type AgentConfig struct {
	MaxIterations int           // max tool-call rounds (default 10)
	TotalTimeout  time.Duration // entire loop deadline (default 120s)
	MaxResultBytes int          // max tool result size passed back to LLM (default 8192)
}

// DefaultAgentConfig returns sensible defaults.
func DefaultAgentConfig() AgentConfig {
	return AgentConfig{
		MaxIterations:  10,
		TotalTimeout:   120 * time.Second,
		MaxResultBytes: 8192,
	}
}

// ToolCallLog records one tool execution within an agent loop.
type ToolCallLog struct {
	Name       string                 `json:"name"`
	Arguments  map[string]interface{} `json:"arguments"`
	Result     string                 `json:"result,omitempty"`
	Error      string                 `json:"error,omitempty"`
	DurationMs int                    `json:"duration_ms"`
	Iteration  int                    `json:"iteration"`
}

// ForwardFunc sends a chat completion request to the LLM (via the gateway's
// existing scheduler/routing) and returns the raw OpenAI-format response body.
// The gateway provides this closure when creating the Loop.
type ForwardFunc func(ctx context.Context, body []byte) ([]byte, error)

// Loop is the agent loop controller.
type Loop struct {
	registry *Registry
	config   AgentConfig
	forward  ForwardFunc
}

// NewLoop creates an agent loop with the given forward function.
func NewLoop(registry *Registry, config AgentConfig, forward ForwardFunc) *Loop {
	return &Loop{
		registry: registry,
		config:   config,
		forward:  forward,
	}
}

// Run executes the agent loop.
// messages: the initial messages array (OpenAI format)
// tools: OpenAI tools definition array
// model: the model name to use in requests
// Returns: final assistant content, tool call logs, error
func (l *Loop) Run(ctx context.Context, messages []interface{}, tools []map[string]interface{}, model string) (string, []ToolCallLog, error) {
	// Apply total timeout
	ctx, cancel := context.WithTimeout(ctx, l.config.TotalTimeout)
	defer cancel()

	var logs []ToolCallLog
	iterations := 0

	for {
		iterations++
		if iterations > l.config.MaxIterations {
			log.Printf("[AGENT] max iterations (%d) reached", l.config.MaxIterations)
			// Return whatever the last assistant message was
			lastContent := extractLastAssistantContent(messages)
			return lastContent, logs, fmt.Errorf("agent loop exceeded max iterations (%d)", l.config.MaxIterations)
		}

		// Build request body
		reqBody := map[string]interface{}{
			"model":    model,
			"messages": messages,
			"stream":   false,
		}
		if len(tools) > 0 {
			reqBody["tools"] = tools
		}

		bodyBytes, err := json.Marshal(reqBody)
		if err != nil {
			return "", logs, fmt.Errorf("marshal request: %w", err)
		}

		// Forward to LLM via gateway
		respBody, err := l.forward(ctx, bodyBytes)
		if err != nil {
			return "", logs, fmt.Errorf("LLM request failed (iteration %d): %w", iterations, err)
		}

		// Parse response
		var resp openAIResponse
		if err := json.Unmarshal(respBody, &resp); err != nil {
			return "", logs, fmt.Errorf("parse LLM response: %w", err)
		}

		if len(resp.Choices) == 0 {
			return "", logs, fmt.Errorf("LLM returned no choices")
		}

		choice := resp.Choices[0]

		// Check if the model wants to call tools
		if choice.FinishReason == "tool_calls" && len(choice.Message.ToolCalls) > 0 {
			// Append assistant message (with tool_calls) to messages
			assistantMsg := map[string]interface{}{
				"role":       "assistant",
				"content":    choice.Message.Content,
				"tool_calls": choice.Message.ToolCalls,
			}
			messages = append(messages, assistantMsg)

			// Execute tool calls in parallel
			type toolResult struct {
				log     ToolCallLog
				content string
				callID  string
			}
			results := make([]toolResult, len(choice.Message.ToolCalls))
			var wg sync.WaitGroup

			for i, tc := range choice.Message.ToolCalls {
				wg.Add(1)
				go func(idx int, tc openAIToolCall) {
					defer wg.Done()

					tcLog := ToolCallLog{
						Name:      tc.Function.Name,
						Iteration: iterations,
					}

					// Parse arguments
					var args map[string]interface{}
					if tc.Function.Arguments != "" {
						if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
							args = map[string]interface{}{"_raw": tc.Function.Arguments}
						}
					}
					tcLog.Arguments = args

					// Execute tool
					start := time.Now()
					result, execErr := l.executeTool(ctx, tc.Function.Name, args)
					tcLog.DurationMs = int(time.Since(start).Milliseconds())

					var toolContent string
					if execErr != nil {
						tcLog.Error = execErr.Error()
						toolContent = fmt.Sprintf("Error: %s", execErr.Error())
						log.Printf("[AGENT] tool %s failed: %v (%dms)", tc.Function.Name, execErr, tcLog.DurationMs)
					} else {
						tcLog.Result = truncate(result, 512) // log truncated
						toolContent = result
						log.Printf("[AGENT] tool %s ok (%dms)", tc.Function.Name, tcLog.DurationMs)
					}

					// Truncate result for LLM context
					if len(toolContent) > l.config.MaxResultBytes {
						toolContent = toolContent[:l.config.MaxResultBytes] + "\n...[result truncated]"
					}

					results[idx] = toolResult{log: tcLog, content: toolContent, callID: tc.ID}
				}(i, tc)
			}
			wg.Wait()

			// Append results in order
			for _, r := range results {
				logs = append(logs, r.log)
				messages = append(messages, map[string]interface{}{
					"role":         "tool",
					"tool_call_id": r.callID,
					"content":      r.content,
				})
			}

			// Continue loop — send updated messages back to LLM
			continue
		}

		// finish_reason == "stop" or no tool_calls → final response
		content := choice.Message.Content
		if content == "" && choice.Message.ContentPtr != nil {
			content = *choice.Message.ContentPtr
		}
		return content, logs, nil
	}
}

// executeTool looks up and executes a tool by name.
func (l *Loop) executeTool(ctx context.Context, name string, arguments map[string]interface{}) (string, error) {
	tool, err := l.registry.Get(name)
	if err != nil {
		return "", fmt.Errorf("tool %q not registered", name)
	}
	if !tool.Enabled {
		return "", fmt.Errorf("tool %q is disabled", name)
	}

	executor, err := NewExecutor(tool)
	if err != nil {
		return "", err
	}

	// Per-tool timeout
	toolCtx, cancel := context.WithTimeout(ctx, time.Duration(tool.TimeoutMs)*time.Millisecond)
	defer cancel()

	return executor.Execute(toolCtx, tool, arguments)
}

// extractLastAssistantContent finds the last assistant message content from messages.
func extractLastAssistantContent(messages []interface{}) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if m, ok := messages[i].(map[string]interface{}); ok {
			if m["role"] == "assistant" {
				if content, ok := m["content"].(string); ok {
					return content
				}
			}
		}
	}
	return ""
}

// --- OpenAI response parsing structs ---

type openAIResponse struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Choices []openAIChoice `json:"choices"`
	Usage   *openAIUsage   `json:"usage"`
}

type openAIChoice struct {
	Index        int             `json:"index"`
	Message      openAIMessage   `json:"message"`
	FinishReason string          `json:"finish_reason"`
}

type openAIMessage struct {
	Role       string          `json:"role"`
	Content    string          `json:"content"`
	ContentPtr *string         `json:"-"` // for null content handling
	ToolCalls  []openAIToolCall `json:"tool_calls"`
}

// UnmarshalJSON handles content being null or string.
func (m *openAIMessage) UnmarshalJSON(data []byte) error {
	type alias struct {
		Role      string           `json:"role"`
		Content   *string          `json:"content"`
		ToolCalls []openAIToolCall `json:"tool_calls"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	m.Role = a.Role
	m.ToolCalls = a.ToolCalls
	if a.Content != nil {
		m.Content = *a.Content
		m.ContentPtr = a.Content
	} else {
		m.Content = ""
	}
	return nil
}

type openAIToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function openAIFunctionCall `json:"function"`
}

type openAIFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}
