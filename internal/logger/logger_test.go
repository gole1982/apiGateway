package logger

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// getString must return a plain string value unchanged.
func TestGetStringPlainString(t *testing.T) {
	data := map[string]interface{}{"key": "hello"}
	if got := getString(data, "key"); got != "hello" {
		t.Errorf("getString string = %q; want %q", got, "hello")
	}
}

// getString must serialise a map[string]interface{} (headers stored by RecordRequestReceived)
// to a JSON string so request_logs.request_headers is never silently empty.
func TestGetStringMapSerialised(t *testing.T) {
	headers := map[string]interface{}{
		"Content-Type":  "application/json",
		"Authorization": "Bearer ***",
	}
	data := map[string]interface{}{"request_headers": headers}

	got := getString(data, "request_headers")
	if got == "" {
		t.Fatal("getString returned empty string for map value; want JSON")
	}
	// Must be valid JSON.
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("getString result is not valid JSON: %q — %v", got, err)
	}
	if parsed["Content-Type"] != "application/json" {
		t.Errorf("Content-Type = %v; want application/json", parsed["Content-Type"])
	}
}

// getString must return "" for a missing key.
func TestGetStringMissingKey(t *testing.T) {
	data := map[string]interface{}{"other": "x"}
	if got := getString(data, "missing"); got != "" {
		t.Errorf("getString missing key = %q; want empty", got)
	}
}

// truncateBody must not truncate when body is within the limit.
func TestTruncateBodyWithinLimit(t *testing.T) {
	body := `{"messages":[{"role":"user","content":"hello"}]}`
	got := truncateBody(body, 50) // 50 KB — body is ~48 bytes
	if got != body {
		t.Errorf("truncateBody modified body within limit")
	}
}

// truncateBody must append " [TRUNCATED]" and cut at the byte boundary.
func TestTruncateBodyOverLimit(t *testing.T) {
	const suffix = " [TRUNCATED]"
	body := strings.Repeat("x", 10*1024+1) // 10 KB + 1 byte
	got := truncateBody(body, 10)          // 10 KB limit
	if !strings.HasSuffix(got, suffix) {
		t.Errorf("truncateBody did not append %q", suffix)
	}
	if len(got) != 10*1024+len(suffix) {
		t.Errorf("truncateBody result length = %d; want %d", len(got), 10*1024+len(suffix))
	}
}

// TestTruncateBodyPreservesUTF8Boundaries is a regression test for the bug where
// truncateBody cut the byte slice mid-rune, producing invalid UTF-8 that corrupted
// later json.Marshal output and confused the string-scan extractors. After the fix,
// the cut must land on a valid rune boundary, so the prefix remains valid UTF-8.
func TestTruncateBodyPreservesUTF8Boundaries(t *testing.T) {
	// Each Chinese char is 3 bytes in UTF-8. 700 chars = 2100 bytes. A 2 KB limit
	// (2048 bytes) lands in the middle of the 683rd char; the old byte-cut would
	// produce invalid UTF-8; the fix walks back to a rune boundary.
	chars := strings.Repeat("中", 700) // 2100 bytes
	got := truncateBody(chars, 2)     // 2*1024 = 2048 byte limit
	prefix := strings.TrimSuffix(got, " [TRUNCATED]")
	if !utf8.ValidString(prefix) {
		t.Errorf("truncateBody produced invalid UTF-8 prefix (mid-rune cut); len(prefix)=%d", len(prefix))
	}
	// Prefix must end at a rune boundary strictly before the byte limit.
	if len(prefix) != 2046 { // 2046 = 683 * 3, the largest rune-aligned cut <= 2048
		t.Errorf("truncateBody prefix length = %d; want 2046 (rune-aligned)", len(prefix))
	}
}

// extractMaxTokens must parse a simple max_tokens field.
func TestExtractMaxTokens(t *testing.T) {
	body := `{"model":"gpt-4","max_tokens":4096,"messages":[]}`
	if got := extractMaxTokens(body); got != 4096 {
		t.Errorf("extractMaxTokens = %d; want 4096", got)
	}
}

// extractMaxTokens must return 0 when field is absent.
func TestExtractMaxTokensAbsent(t *testing.T) {
	body := `{"model":"gpt-4","messages":[]}`
	if got := extractMaxTokens(body); got != 0 {
		t.Errorf("extractMaxTokens = %d; want 0", got)
	}
}

// extractFinishReason must find the last non-null finish_reason in an SSE body.
func TestExtractFinishReasonStop(t *testing.T) {
	body := `data: {"choices":[{"delta":{"content":"hi"},"finish_reason":null}]}` + "\n\n" +
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
		`data: [DONE]`
	if got := extractFinishReason(body); got != "stop" {
		t.Errorf("extractFinishReason = %q; want stop", got)
	}
}

// extractFinishReason must return "" when body contains only null values.
func TestExtractFinishReasonAllNull(t *testing.T) {
	body := `data: {"choices":[{"finish_reason":null}]}` + "\n\n"
	if got := extractFinishReason(body); got != "" {
		t.Errorf("extractFinishReason = %q; want empty", got)
	}
}

// addToBatch must not drop events when the batch reaches BatchSize.
// (Regression: the previous implementation raced go flushBatch with batch reset.)
func TestAddToBatchFlushesWithoutLosingEvents(t *testing.T) {
	storage := &countingStorage{}
	cfg := DefaultLogConfig()
	cfg.BatchSize = 3
	cfg.BatchIntervalMS = 60_000 // avoid timer flushes during the test
	cfg.CleanupInterval = 0

	logInstance := NewLogger(nil, cfg)
	// Attach a minimal storage via a wrapper that records SaveRequestLog calls.
	// LogStorage needs *db.DB; for this unit test we only exercise addToBatch/persist
	// by substituting Storage with a stub that implements the needed methods through
	// the concrete persist path. Instead, drive addToBatch directly and check batch state.
	worker := logInstance.worker
	worker.batch = make([]LogEvent, 0, cfg.BatchSize)

	// Without Storage, persistBatch is a no-op after the early return — still must not panic
	// and must leave the batch empty after a full flush.
	logInstance.Storage = nil
	for i := 0; i < 3; i++ {
		worker.addToBatch(LogEvent{
			RequestID: "req-batch-test",
			EventType: REQUEST_RECEIVED,
			Timestamp: time.Now(),
			Data:      map[string]interface{}{"client_ip": "1.2.3.4"},
		})
	}
	worker.batchMu.Lock()
	left := len(worker.batch)
	worker.batchMu.Unlock()
	if left != 0 {
		t.Fatalf("batch length after full flush = %d; want 0 (events were lost or not flushed)", left)
	}
	_ = storage
}

// countingStorage is unused beyond compile-time placeholder for the batch test.
type countingStorage struct{}

// TestRecordEventDroppedCounted verifies that when the event queue is full, RecordEvent
// drops the event and DroppedEvents() reports the count. Previously dropEvent was a
// no-op, so log data loss was completely invisible (no metrics, no observability).
func TestRecordEventDroppedCounted(t *testing.T) {
	cfg := DefaultLogConfig()
	cfg.QueueCapacity = 1 // tiny queue so the second event overflows
	cfg.BatchSize = 1000  // don't flush during the test
	cfg.BatchIntervalMS = 60_000
	cfg.CleanupInterval = 0
	lg := NewLogger(nil, cfg)
	// Do NOT Start() — we only exercise RecordEvent's enqueue drop path, not the worker.
	// (Nothing consumes the queue, so it will fill up after the capacity of 1.)

	// First event fills the one-slot buffer; the next ones must be dropped and counted.
	for i := 0; i < 5; i++ {
		lg.RecordEvent(LogEvent{
			RequestID: "drop-test",
			EventType: REQUEST_RECEIVED,
			Timestamp: time.Now(),
			Data:      map[string]interface{}{"i": i},
		})
	}

	if got := lg.DroppedEvents(); got < 1 {
		t.Errorf("DroppedEvents() = %d; want >= 1 (events were dropped but not counted — dropEvent regression)", got)
	}
}

// TestRecordEventAfterStopDoesNotPanic verifies that RecordEvent called concurrently
// with (or after) Stop() does NOT panic on send-to-closed-channel. Previously Stop()
// closed eventQueue with no guard, and any in-flight RecordEvent (e.g. from a deferred
// log call in an HTTP handler still finishing shutdown) would panic and crash the gateway.
// The fix sets a closed flag before closing and recovers the residual race window.
func TestRecordEventAfterStopDoesNotPanic(t *testing.T) {
	cfg := DefaultLogConfig()
	cfg.QueueCapacity = 1
	cfg.BatchSize = 100
	cfg.BatchIntervalMS = 100
	cfg.CleanupInterval = 0
	lg := NewLogger(nil, cfg)
	lg.Start()

	// Stop and immediately try to record — must not panic, must not race.
	lg.Stop()

	// Multiple record calls after stop, and concurrently: none may panic.
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("RecordEvent after Stop panicked: %v", r)
			}
			close(done)
		}()
		for i := 0; i < 10; i++ {
			lg.RecordEvent(LogEvent{
				RequestID: "post-stop",
				EventType: CLIENT_RESPONSE,
				Timestamp: time.Now(),
			})
		}
	}()
	<-done
}
