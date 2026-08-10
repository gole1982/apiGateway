package logger

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type Logger struct {
	eventQueue chan LogEvent
	worker     *LogWorker
	Storage    *LogStorage
	config     LogConfig
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	// closed is set before eventQueue is closed so that concurrent RecordEvent callers
	// can early-return instead of sending on a closed channel (which always panics,
	// even under select/default — select only guards the "no receiver" case, not closed).
	closed atomic.Bool
	// droppedCount counts log events that were dropped because the event queue was full.
	droppedCount atomic.Int64
}

type LogWorker struct {
	logger  *Logger
	batch   []LogEvent
	batchMu sync.Mutex
	timer   *time.Timer
}

func NewLogger(storage *LogStorage, config LogConfig) *Logger {
	ctx, cancel := context.WithCancel(context.Background())
	logger := &Logger{
		eventQueue: make(chan LogEvent, config.QueueCapacity),
		Storage:    storage,
		config:     config,
		ctx:        ctx,
		cancel:     cancel,
	}
	logger.worker = &LogWorker{logger: logger}
	return logger
}

func (l *Logger) Start() {
	l.wg.Add(1)
	go l.worker.run()

	if l.config.CleanupInterval > 0 {
		l.wg.Add(1)
		go l.startCleanup()
	}
}

func (l *Logger) Stop() {
	// Set the closed flag FIRST so concurrent RecordEvent callers early-return before
	// we close the channel. Sending on a closed channel always panics; select/default
	// only guards the "no receiver ready" case, not "closed". There is still a tiny
	// window between the flag check and the send in RecordEvent, so RecordEvent also
	// defers a recover() as a belt-and-braces guard against that race.
	l.closed.Store(true)
	// 1) Close the queue so the worker drains remaining events and exits.
	// 2) Cancel context so the cleanup goroutine exits.
	// 3) Wait for both. Order matters: cancel alone would not drain the queue.
	close(l.eventQueue)
	l.cancel()
	l.wg.Wait()
}

func (l *Logger) RecordEvent(event LogEvent) {
	if l.closed.Load() {
		// Logger is shutting down and the queue is (about to be) closed — drop silently.
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Guard against the race between the closed.Load() check above and the send below:
	// if Stop() flips the flag and closes the channel right after our check, the send
	// would panic. recover() turns that into a silent drop, which is the correct
	// behaviour during shutdown (we must not crash the gateway for a lost log line).
	defer func() { _ = recover() }()

	select {
	case l.eventQueue <- event:
	default:
		dropEvent(l.config.QueueCapacity, &l.droppedCount)
	}
}

// DroppedEvents returns the number of log events that have been dropped because the
// event queue was full (or the logger was shutting down). Useful for observability —
// a non-zero value indicates the queue capacity should be increased or the worker is
// not keeping up.
func (l *Logger) DroppedEvents() int64 {
	return l.droppedCount.Load()
}

func (l *Logger) RecordRequestReceived(requestID, sessionID, clientIP, method, path string, headers map[string]string, body string) {
	if requestID == "" {
		requestID = NewRequestID()
	}
	event := LogEvent{
		RequestID: requestID,
		SessionID: sessionID,
		EventType: REQUEST_RECEIVED,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"client_ip":       clientIP,
			"request_method":  method,
			"request_path":    path,
			"request_headers": headers,
			"request_body":    truncateBody(body, l.config.MaxBodySizeKB),
		},
	}
	l.RecordEvent(event)
}

func (l *Logger) RecordRoutingDecision(requestID, lapiAlias string, matchedRAPIs []string) {
	matchedJSON, _ := json.Marshal(matchedRAPIs)
	event := LogEvent{
		RequestID: requestID,
		EventType: ROUTING_DECISION,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"lapi_alias":    lapiAlias,
			"matched_rapis": string(matchedJSON),
		},
	}
	l.RecordEvent(event)
}

func (l *Logger) RecordUpstreamSent(requestID, selectedRAPI, upstreamURL string, headers map[string]string, body string, retryCount int) {
	event := LogEvent{
		RequestID: requestID,
		EventType: UPSTREAM_SENT,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"selected_rapi":    selectedRAPI,
			"upstream_url":     upstreamURL,
			"upstream_headers": headers,
			"upstream_body":    truncateBody(body, l.config.MaxBodySizeKB),
			"retry_count":      retryCount,
		},
	}
	l.RecordEvent(event)
}

func (l *Logger) RecordUpstreamResponse(requestID string, statusCode int, headers map[string]string, body string, latencyMS, tokensUsed int) {
	event := LogEvent{
		RequestID: requestID,
		EventType: UPSTREAM_RESPONSE,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"response_status":  statusCode,
			"response_headers": headers,
			"response_body":    truncateBody(body, l.config.MaxBodySizeKB),
			"latency_ms":       latencyMS,
			"tokens_used":      tokensUsed,
		},
	}
	l.RecordEvent(event)
}

func (l *Logger) RecordClientResponse(requestID string, statusCode int, latencyMS int, fallbackUsed bool) {
	event := LogEvent{
		RequestID: requestID,
		EventType: CLIENT_RESPONSE,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"response_status": statusCode,
			"latency_ms":      latencyMS,
			"fallback_used":   fallbackUsed,
		},
	}
	l.RecordEvent(event)
}

func (l *Logger) RecordError(requestID, message, stage string) {
	event := LogEvent{
		RequestID: requestID,
		EventType: ERROR,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"error_message": message,
			"stage":         stage,
		},
	}
	l.RecordEvent(event)
}

func (w *LogWorker) run() {
	defer w.logger.wg.Done()
	// A panic anywhere in the worker (DB write failure, malformed payload parsing,
	// a nil deref in extractMaxTokens/extractFinishReason, etc.) would otherwise kill
	// the entire logging pipeline: the deferred wg.Done() runs, Stop() returns, but
	// subsequent RecordEvent calls would pile up forever (and, post Fix 8, panic on
	// the closed channel). Catching the panic keeps the worker alive-ish — we log it
	// and let the loop continue so at least future events have a chance to be recorded.
	defer func() {
		if r := recover(); r != nil {
			DefaultConsole().Error("logger", "[logger] worker panic recovered", "panic", fmt.Sprint(r))
		}
	}()

	w.batch = make([]LogEvent, 0, w.logger.config.BatchSize)
	w.resetTimer()

	for {
		select {
		case event, ok := <-w.logger.eventQueue:
			if !ok {
				// Queue closed: drain is complete; flush and exit.
				w.flushBatch()
				return
			}
			w.addToBatch(event)

		case <-w.timer.C:
			w.flushBatch()
			w.resetTimer()
		}
	}
}

func (w *LogWorker) resetTimer() {
	if w.timer != nil {
		w.timer.Stop()
	}
	w.timer = time.NewTimer(time.Duration(w.logger.config.BatchIntervalMS) * time.Millisecond)
}

func (w *LogWorker) addToBatch(event LogEvent) {
	w.batchMu.Lock()
	w.batch = append(w.batch, event)
	var toFlush []LogEvent
	if len(w.batch) >= w.logger.config.BatchSize {
		// Take ownership of the current batch under the lock so no events are lost.
		// Previously this used `go flushBatch()` + immediate batch reset, which raced
		// and silently dropped every batch that hit BatchSize.
		toFlush = w.batch
		w.batch = make([]LogEvent, 0, w.logger.config.BatchSize)
	}
	w.batchMu.Unlock()

	if toFlush != nil {
		w.persistBatch(toFlush)
	}
}

func (w *LogWorker) flushBatch() {
	w.batchMu.Lock()
	if len(w.batch) == 0 {
		w.batchMu.Unlock()
		return
	}

	batch := w.batch
	w.batch = make([]LogEvent, 0, w.logger.config.BatchSize)
	w.batchMu.Unlock()

	w.persistBatch(batch)
}

func (w *LogWorker) persistBatch(batch []LogEvent) {
	if w.logger.Storage == nil || len(batch) == 0 {
		return
	}

	requestLogs := make(map[string]*RequestLog)

	// First pass: accumulate all events into RequestLog structs.
	for _, event := range batch {
		log, ok := requestLogs[event.RequestID]
		if !ok {
			log = &RequestLog{
				ID:        event.RequestID,
				SessionID: event.SessionID,
				Timestamp: event.Timestamp,
				Status:    "pending",
			}
			requestLogs[event.RequestID] = log
		}
		if event.SessionID != "" && log.SessionID == "" {
			log.SessionID = event.SessionID
		}
		w.updateRequestLog(log, event)
	}

	// Second pass: upsert the parent request_log row first, then append child events.
	// This order is required because log_events has a FK referencing request_logs(id).
	// Keep status="pending" for intermediate batches (REQUEST_RECEIVED / UPSTREAM_*) so
	// the dashboard does not show incomplete requests as completed.
	for _, log := range requestLogs {
		if err := w.logger.Storage.SaveRequestLog(log); err != nil {
			saveFailedEvent(log)
			continue
		}
	}

	for _, event := range batch {
		if err := w.logger.Storage.AppendEvent(event.RequestID, &event); err != nil {
			// Child event failure is non-fatal; parent row already has the fields.
			_ = err
		}
	}
}

func (w *LogWorker) updateRequestLog(log *RequestLog, event LogEvent) {
	switch event.EventType {
	case REQUEST_RECEIVED:
		log.ClientIP = getString(event.Data, "client_ip")
		log.RequestMethod = getString(event.Data, "request_method")
		log.RequestPath = getString(event.Data, "request_path")
		log.RequestHeaders = getString(event.Data, "request_headers")
		log.RequestBody = getString(event.Data, "request_body")
		log.ReqMaxTokens = extractMaxTokens(log.RequestBody)

	case ROUTING_DECISION:
		log.LapiAlias = getString(event.Data, "lapi_alias")
		log.MatchedRAPIs = getString(event.Data, "matched_rapis")

	case UPSTREAM_SENT:
		log.SelectedRAPI = getString(event.Data, "selected_rapi")
		log.UpstreamURL = getString(event.Data, "upstream_url")
		log.UpstreamHeaders = getString(event.Data, "upstream_headers")
		log.UpstreamBody = getString(event.Data, "upstream_body")
		log.RetryCount = getInt(event.Data, "retry_count")

	case UPSTREAM_RESPONSE:
		log.ResponseStatus = getInt(event.Data, "response_status")
		log.ResponseHeaders = getString(event.Data, "response_headers")
		log.ResponseBody = getString(event.Data, "response_body")
		log.LatencyMS = getInt(event.Data, "latency_ms")
		log.TokensUsed = getInt(event.Data, "tokens_used")
		log.FinishReason = extractFinishReason(log.ResponseBody)

	case CLIENT_RESPONSE:
		log.ResponseStatus = getInt(event.Data, "response_status")
		log.LatencyMS = getInt(event.Data, "latency_ms")
		log.FallbackUsed = getBool(event.Data, "fallback_used")
		log.Status = "completed"
		log.CompletedAt = event.Timestamp

	case ERROR:
		log.ErrorMessage = getString(event.Data, "error_message")
		log.Status = "failed"
		log.CompletedAt = event.Timestamp
	}
}

func (l *Logger) startCleanup() {
	defer l.wg.Done()

	ticker := time.NewTicker(time.Duration(l.config.CleanupInterval) * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if l.Storage != nil {
				l.Storage.CleanupOldRecords(l.config.MaxAgeDays, l.config.MaxRecords)
			}
		case <-l.ctx.Done():
			return
		}
	}
}

func NewRequestID() string {
	return "req-" + time.Now().Format("20060102-150405") + "-" + randomHex(4)
}

func randomHex(n int) string {
	bytes := make([]byte, n)
	for i := range bytes {
		bytes[i] = byte(rand.Intn(256))
	}
	return hex.EncodeToString(bytes)
}

func truncateBody(body string, maxKB int) string {
	maxBytes := maxKB * 1024
	if len(body) <= maxBytes {
		return body
	}
	// Truncate on a valid UTF-8 rune boundary. Slicing at an arbitrary byte offset can
	// split a multi-byte rune (e.g. mid-Chinese-character), producing invalid UTF-8 that
	// corrupts later json.Marshal output (replacement chars or errors) and confuses the
	// string-scan extractors (extractMaxTokens/extractFinishReason). Walk back to the
	// start of the last complete rune at or before maxBytes.
	cutoff := maxBytes
	for cutoff > 0 {
		if utf8.RuneStart(body[cutoff]) {
			// body[cutoff] is the first byte of a rune; cutting here keeps runes whole.
			break
		}
		cutoff--
	}
	return body[:cutoff] + " [TRUNCATED]"
}

func getString(data map[string]interface{}, key string) string {
	if val, ok := data[key]; ok {
		if s, ok := val.(string); ok {
			return s
		}
		// headers are stored as map[string]string — serialize to JSON string.
		if b, err := json.Marshal(val); err == nil {
			return string(b)
		}
	}
	return ""
}

func getInt(data map[string]interface{}, key string) int {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case int:
			return v
		case float64:
			return int(v)
		}
	}
	return 0
}

func getBool(data map[string]interface{}, key string) bool {
	if val, ok := data[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}

// dropEvent is called when a log event cannot be enqueued (queue full). It increments
// a dropped counter so that callers (e.g. /api/status) can observe log data loss.
// The previous implementation was a no-op, which meant events were silently dropped
// with zero observability — the dashboard would show incomplete requests (stuck at
// "pending") with no indication that logging was backing up.
func dropEvent(queueSize int, counter *atomic.Int64) {
	counter.Add(1)
}

// extractMaxTokens pulls the max_tokens integer out of a JSON request body using
// a lightweight string scan — avoids a full json.Unmarshal on the hot path.
func extractMaxTokens(body string) int {
	const key = `"max_tokens"`
	idx := strings.Index(body, key)
	if idx < 0 {
		return 0
	}
	rest := strings.TrimSpace(body[idx+len(key):])
	if len(rest) == 0 || rest[0] != ':' {
		return 0
	}
	rest = strings.TrimSpace(rest[1:])
	// Parse the decimal integer that follows.
	n := 0
	found := false
	for _, c := range rest {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
			found = true
		} else if found {
			break
		}
	}
	if !found {
		return 0
	}
	return n
}

// extractFinishReason scans an SSE response body for the last non-null finish_reason value.
// The scan reads backwards through the body to find the terminal chunk efficiently.
// Returns "" if not found (e.g. streaming body was truncated before the final chunk).
func extractFinishReason(body string) string {
	const key = `"finish_reason"`
	// Walk backwards: last occurrence wins (terminal SSE chunk).
	idx := strings.LastIndex(body, key)
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(body[idx+len(key):])
	if len(rest) == 0 || rest[0] != ':' {
		return ""
	}
	rest = strings.TrimSpace(rest[1:])
	if strings.HasPrefix(rest, "null") {
		return ""
	}
	// Value is a quoted string: "stop", "length", "content_filter", etc.
	if len(rest) == 0 || rest[0] != '"' {
		return ""
	}
	end := strings.IndexByte(rest[1:], '"')
	if end < 0 {
		return ""
	}
	return rest[1 : end+1]
}

func saveFailedEvent(entry *RequestLog) {
	// This is the safety-net writer invoked when the primary DB write fails. Previously
	// every error here was silently swallowed (MkdirAll ignored, file open error dropped,
	// WriteString error dropped), so a failed SaveRequestLog was completely invisible —
	// the operator would never know the safety net itself was broken. Surface each failure
	// to the structured console logger so it at least appears in process logs.
	//
	// Each line written to logs/failed_events.log is a standalone JSON Lines record
	// matching the same schema as the console stream: {"ts":...,"event":"save_failed",
	// "request_id":...,"reason":...,"entry":{...}}. The reason field uses a small set
	// of enumerated values (mkdir_failed / open_failed / marshal_failed / write_failed)
	// so the safety-net file is grep/parse-friendly rather than a free-form text dump.
	if err := os.MkdirAll("logs", 0755); err != nil {
		DefaultConsole().Error("logger", "[logger] saveFailedEvent: MkdirAll failed", "reason", "mkdir_failed", "error", err.Error())
		return
	}
	file, err := os.OpenFile("logs/failed_events.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		DefaultConsole().Error("logger", "[logger] saveFailedEvent: open failed_events.log failed", "reason", "open_failed", "error", err.Error())
		return
	}
	defer file.Close()
	entryData, err := json.Marshal(entry)
	if err != nil {
		DefaultConsole().Error("logger", "[logger] saveFailedEvent: marshal failed", "reason", "marshal_failed", "error", err.Error())
		return
	}
	// Build the JSON Lines wrapper manually so the safety-net file is self-describing
	// even when read without the rest of the codebase (each record carries the
	// request_id at the top level for quick grep).
	wrapper, _ := json.Marshal(map[string]interface{}{
		"ts":         time.Now().UTC().Format(time.RFC3339Nano),
		"event":      "save_failed",
		"request_id": entry.ID,
		"entry":      json.RawMessage(entryData),
	})
	if _, err := file.Write(append(wrapper, '\n')); err != nil {
		DefaultConsole().Error("logger", "[logger] saveFailedEvent: write failed", "reason", "write_failed", "error", err.Error())
	}
}
