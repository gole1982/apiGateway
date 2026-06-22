package scheduler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"gateway/internal/models"
)

func TestPickAvailableSelectsLowestCost(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	rapis := testRAPIs(
		models.RAPIWithPlatform{ID: 1, Alias: "expensive", BaseCost: 10},
		models.RAPIWithPlatform{ID: 2, Alias: "cheap", BaseCost: 1},
	)
	got, _, err := m.PickAvailable(1, rapis)
	if err != nil {
		t.Fatalf("PickAvailable: %v", err)
	}
	if got.ID != 2 {
		t.Fatalf("selected RAPI %d, want 2 (cheapest)", got.ID)
	}
}

func TestPickAvailableTieBreaksByOrderIndex(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	rapis := testRAPIs(
		models.RAPIWithPlatform{ID: 1, Alias: "second", BaseCost: 5, OrderIndex: 2},
		models.RAPIWithPlatform{ID: 2, Alias: "first", BaseCost: 5, OrderIndex: 1},
	)
	got, _, err := m.PickAvailable(1, rapis)
	if err != nil {
		t.Fatalf("PickAvailable: %v", err)
	}
	if got.ID != 2 {
		t.Fatalf("selected RAPI %d, want 2 (lower OrderIndex)", got.ID)
	}
}

func TestPickAvailableSkipsUnavailable(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	rapis := testRAPIs(
		models.RAPIWithPlatform{ID: 1, Alias: "cheap", BaseCost: 1},
		models.RAPIWithPlatform{ID: 2, Alias: "expensive", BaseCost: 10},
	)

	// Mark the cheapest RAPI as unavailable
	m.MarkFailure(1, time.Time{}, "test failure")

	got, _, err := m.PickAvailable(1, rapis)
	if err != nil {
		t.Fatalf("PickAvailable: %v", err)
	}
	if got.ID != 2 {
		t.Fatalf("selected RAPI %d, want 2 (1 is in cooldown)", got.ID)
	}
}

func TestAllUnavailableReturnsError(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	rapis := testRAPIs(
		models.RAPIWithPlatform{ID: 1, Alias: "only", BaseCost: 1},
	)
	m.MarkFailure(1, time.Time{}, "test failure")

	_, nextAvail, err := m.PickAvailable(1, rapis)
	if !errors.Is(err, ErrAllRAPIUnavailable) {
		t.Fatalf("err = %v, want ErrAllRAPIUnavailable", err)
	}
	if nextAvail.IsZero() {
		t.Fatal("nextAvail should be non-zero")
	}
}

func TestMarkFailureSetsCooldown(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	until := m.MarkFailure(1, time.Time{}, "rate limited")
	if !until.After(time.Now()) {
		t.Fatalf("unavailableUntil %v should be in the future", until)
	}

	m.mu.Lock()
	s := m.rapis[1]
	failures := s.consecutiveFailures
	reason := s.unavailableReason
	m.mu.Unlock()

	if failures != 1 {
		t.Fatalf("consecutiveFailures = %d, want 1", failures)
	}
	if reason != "rate limited" {
		t.Fatalf("unavailableReason = %q, want %q", reason, "rate limited")
	}
}

func TestMarkFailureExponentialBackoff(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultCooldown = 10 * time.Millisecond
	cfg.MaxCooldown = time.Second
	cfg.ExponentialBackoff = true
	m := NewManager(cfg)
	defer m.Close()

	first := m.MarkFailure(1, time.Time{}, "fail1")
	second := m.MarkFailure(1, time.Time{}, "fail2")

	gap := second.Sub(first)
	if gap < 10*time.Millisecond {
		t.Fatalf("second cooldown gap = %v, expected exponential increase", gap)
	}
}

func TestMarkFailureUsesRetryAfterHeader(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	retryAfter := time.Now().Add(5 * time.Second)
	until := m.MarkFailure(1, retryAfter, "429")
	if until.Sub(retryAfter) > time.Millisecond {
		t.Fatalf("until = %v, want close to retryAfter = %v", until, retryAfter)
	}
}

func TestMarkSuccessClearsCooldown(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	m.MarkFailure(1, time.Time{}, "fail")
	m.MarkSuccess(1)

	m.mu.Lock()
	s := m.rapis[1]
	unavail := s.unavailableUntil
	failures := s.consecutiveFailures
	m.mu.Unlock()

	if !unavail.IsZero() {
		t.Fatalf("unavailableUntil should be zero after MarkSuccess, got %v", unavail)
	}
	if failures != 0 {
		t.Fatalf("consecutiveFailures = %d, want 0 after MarkSuccess", failures)
	}
}

func TestRecordRequestUpdatesCounters(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	m.RecordRequest(1, 100)
	m.RecordRequest(1, 200)

	m.mu.Lock()
	c := counters[1]
	minReqs := c.minute.reqs
	minToks := c.minute.toks
	m.mu.Unlock()

	if minReqs != 2 {
		t.Fatalf("minute reqs = %d, want 2", minReqs)
	}
	if minToks != 300 {
		t.Fatalf("minute toks = %d, want 300", minToks)
	}
}

func TestHighCostTriggeredByThreshold(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	rapis := testRAPIs(
		models.RAPIWithPlatform{ID: 1, Alias: "normal", BaseCost: 1, HighCost: 100, RPMLimit: 2},
		models.RAPIWithPlatform{ID: 2, Alias: "backup", BaseCost: 50},
	)

	// Record 2 requests to hit the RPM limit
	m.RecordRequest(1, 10)
	m.RecordRequest(1, 10)

	// Now RAPI 1's cost should be 100 (high_cost), RAPI 2's is 50 (base_cost)
	got, _, err := m.PickAvailable(1, rapis)
	if err != nil {
		t.Fatalf("PickAvailable: %v", err)
	}
	if got.ID != 2 {
		t.Fatalf("selected RAPI %d, want 2 (RAPI 1 exceeded threshold)", got.ID)
	}
}

func TestTimePeriodCostOverride(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	// Set a time period rule that covers the current time with high cost
	now := time.Now()
	start := now.Add(-time.Hour).Format("15:04")
	end := now.Add(time.Hour).Format("15:04")
	rules := `[{"start":"` + start + `","end":"` + end + `","cost":999}]`

	rapis := testRAPIs(
		models.RAPIWithPlatform{ID: 1, Alias: "peak", BaseCost: 1, TimePeriodRules: rules},
		models.RAPIWithPlatform{ID: 2, Alias: "stable", BaseCost: 50},
	)

	got, _, err := m.PickAvailable(1, rapis)
	if err != nil {
		t.Fatalf("PickAvailable: %v", err)
	}
	if got.ID != 2 {
		t.Fatalf("selected RAPI %d, want 2 (RAPI 1 in peak period with cost 999)", got.ID)
	}
}

func TestRetryAtParsesRetryAfterHeader(t *testing.T) {
	header := http.Header{"Retry-After": []string{"5"}}
	at := RetryAt(header, time.Time{})
	if at.IsZero() {
		t.Fatal("RetryAt should return non-zero time")
	}
	if at.Before(time.Now().Add(4 * time.Second)) {
		t.Fatalf("RetryAt = %v, expected ~5 seconds from now", at)
	}
}

func TestRetryAtParsesXRateLimitReset(t *testing.T) {
	future := time.Now().Add(10 * time.Second).Unix()
	header := make(http.Header)
	header.Set("X-RateLimit-Reset", strconv.FormatInt(future, 10))
	at := RetryAt(header, time.Time{})
	if at.IsZero() {
		t.Fatal("RetryAt should parse X-RateLimit-Reset")
	}
}

func TestRetryAtReturnsFallbackOnEmpty(t *testing.T) {
	fallback := time.Now().Add(time.Minute)
	at := RetryAt(nil, fallback)
	if at != fallback {
		t.Fatalf("RetryAt = %v, want fallback %v", at, fallback)
	}
}

func TestWaitContextCancelReturnsError(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := m.Wait(ctx, 42, time.Now().Add(time.Second))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait err = %v, want context.Canceled", err)
	}
}

func TestWaitQueueFull(t *testing.T) {
	cfg := testConfig()
	cfg.QueueMaxLen = 1
	m := NewManager(cfg)
	defer m.Close()

	m.mu.Lock()
	m.queues[1] = &waitQueue{count: 1}
	m.mu.Unlock()

	err := m.Wait(context.Background(), 1, time.Now().Add(time.Millisecond))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Wait err = %v, want ErrQueueFull", err)
	}
}

func TestEstimateCost(t *testing.T) {
	req := &models.ProxyRequest{
		Messages: []models.Message{
			{Role: "user", Content: "Hello world"},
			{Role: "assistant", Content: "Hi there! How can I help you today?"},
		},
	}
	cost := EstimateCost(req)
	if cost <= 1 {
		t.Fatalf("EstimateCost = %d, expected > 1", cost)
	}
}

func TestRecoveryLoopClearsCooldown(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultCooldown = 30 * time.Millisecond
	m := NewManager(cfg)
	defer m.Close()

	m.MarkFailure(1, time.Time{}, "test")

	// Wait for recovery loop to clear the cooldown
	time.Sleep(100 * time.Millisecond)

	m.mu.Lock()
	s := m.rapis[1]
	unavail := s.unavailableUntil
	m.mu.Unlock()

	if !unavail.IsZero() && unavail.After(time.Now()) {
		t.Fatalf("recovery loop should have cleared cooldown, but unavailableUntil = %v", unavail)
	}
}

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.DefaultCooldown = 10 * time.Millisecond
	cfg.MaxCooldown = 50 * time.Millisecond
	cfg.QueueMaxLen = 10
	cfg.RequestMaxWait = 0
	return cfg
}

func testRAPIs(rapis ...models.RAPIWithPlatform) []models.RAPIWithPlatform {
	for i := range rapis {
		rapis[i].BaseURL = "https://example.com/v1"
		rapis[i].Enabled = true
		rapis[i].Available = true
	}
	return rapis
}
