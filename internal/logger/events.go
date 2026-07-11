package logger

import "time"

type LogEvent struct {
	RequestID string                 `json:"request_id"`
	SessionID string                 `json:"session_id"`
	EventType EventType              `json:"event_type"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

type EventType int

const (
	REQUEST_RECEIVED EventType = iota
	ROUTING_DECISION
	UPSTREAM_SENT
	UPSTREAM_RESPONSE
	CLIENT_RESPONSE
	ERROR
)

func (e EventType) String() string {
	switch e {
	case REQUEST_RECEIVED:
		return "REQUEST_RECEIVED"
	case ROUTING_DECISION:
		return "ROUTING_DECISION"
	case UPSTREAM_SENT:
		return "UPSTREAM_SENT"
	case UPSTREAM_RESPONSE:
		return "UPSTREAM_RESPONSE"
	case CLIENT_RESPONSE:
		return "CLIENT_RESPONSE"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

type RequestLog struct {
	ID             string    `json:"id"`
	SessionID      string    `json:"session_id"`
	Timestamp      time.Time `json:"timestamp"`

	ClientIP       string `json:"client_ip"`
	RequestMethod  string `json:"request_method"`
	RequestPath    string `json:"request_path"`
	RequestHeaders string `json:"request_headers"`
	RequestBody    string `json:"request_body"`
	// Extracted from request body for quick filtering (no body parse on hot path).
	ReqMaxTokens   int    `json:"req_max_tokens"`

	LapiAlias      string `json:"lapi_alias"`
	MatchedRAPIs   string `json:"matched_rapis"`
	SelectedRAPI   string `json:"selected_rapi"`

	UpstreamURL     string `json:"upstream_url"`
	UpstreamHeaders string `json:"upstream_headers"`
	UpstreamBody    string `json:"upstream_body"`

	ResponseStatus  int    `json:"response_status"`
	ResponseHeaders string `json:"response_headers"`
	ResponseBody    string `json:"response_body"`
	LatencyMS       int    `json:"latency_ms"`
	TokensUsed      int    `json:"tokens_used"`
	// Extracted from upstream response (last non-null finish_reason in SSE stream).
	FinishReason    string `json:"finish_reason"`

	ErrorMessage string `json:"error_message"`
	RetryCount   int    `json:"retry_count"`
	FallbackUsed bool   `json:"fallback_used"`

	Status      string    `json:"status"`
	CompletedAt time.Time `json:"completed_at"`

	Events []LogEvent `json:"events"`
}

type Session struct {
	ID             string    `json:"id"`
	ClientIP       string    `json:"client_ip"`
	ClientPort     int       `json:"client_port"`
	StartedAt      time.Time `json:"start_time"`
	EndedAt        time.Time `json:"end_time"`
	TotalRequests  int       `json:"request_count"`
	LastRequestAt  time.Time `json:"last_request_at"`
}