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
	got, _, err := m.PickAvailable(1, rapis, "")
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
	got, _, err := m.PickAvailable(1, rapis, "")
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

	got, _, err := m.PickAvailable(1, rapis, "")
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

	_, nextAvail, err := m.PickAvailable(1, rapis, "")
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
		t.Fatalf("recoverAt %v should be in the future", until)
	}

	snap := m.Snapshot()
	var rs RAPISnapshot
	for _, s := range snap.RAPIs {
		if s.ID == 1 {
			rs = s
		}
	}
	if rs.ConsecutiveFailures != 1 {
		t.Fatalf("consecutiveFailures = %d, want 1", rs.ConsecutiveFailures)
	}
	if rs.Reason != "rate limited" {
		t.Fatalf("reason = %q, want %q", rs.Reason, "rate limited")
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

	var rs RAPISnapshot
	for _, s := range m.Snapshot().RAPIs {
		if s.ID == 1 {
			rs = s
		}
	}
	if rs.Cooling {
		t.Fatalf("RAPI should not be cooling after MarkSuccess")
	}
	if rs.ConsecutiveFailures != 0 {
		t.Fatalf("consecutiveFailures = %d, want 0 after MarkSuccess", rs.ConsecutiveFailures)
	}
}

func TestRecordRequestUpdatesCounters(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	m.RecordRequest(1, 100)
	m.RecordRequest(1, 200)

	m.mu.Lock()
	c := m.counters[1]
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
	got, _, err := m.PickAvailable(1, rapis, "")
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

	got, _, err := m.PickAvailable(1, rapis, "")
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

func TestPickAvailablePrefersFormat(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	// When OrderIndex differs, order_index always wins over format preference.
	rapis := testRAPIs(
		models.RAPIWithPlatform{ID: 1, Alias: "openai-only", BaseCost: 5, OrderIndex: 1, SupportedFormats: `["openai"]`},
		models.RAPIWithPlatform{ID: 2, Alias: "anthropic-support", BaseCost: 5, OrderIndex: 2, SupportedFormats: `["openai","anthropic"]`},
	)

	// Without format preference, should pick by order_index (ID 1 has lower OrderIndex)
	got, _, err := m.PickAvailable(1, rapis, "")
	if err != nil {
		t.Fatalf("PickAvailable: %v", err)
	}
	if got.ID != 1 {
		t.Fatalf("without preference: selected RAPI %d, want 1 (lower order_index)", got.ID)
	}

	// With anthropic preference, order_index still wins: ID 1 has lower OrderIndex even though
	// ID 2 supports anthropic natively. User-configured order is the primary criterion.
	got, _, err = m.PickAvailable(1, rapis, "anthropic")
	if err != nil {
		t.Fatalf("PickAvailable: %v", err)
	}
	if got.ID != 1 {
		t.Fatalf("with anthropic preference: selected RAPI %d, want 1 (order_index beats format preference)", got.ID)
	}

	// When OrderIndex is equal, format preference breaks the tie.
	rapisTied := testRAPIs(
		models.RAPIWithPlatform{ID: 3, Alias: "openai-only-tied", BaseCost: 5, OrderIndex: 0, SupportedFormats: `["openai"]`},
		models.RAPIWithPlatform{ID: 4, Alias: "anthropic-support-tied", BaseCost: 5, OrderIndex: 0, SupportedFormats: `["openai","anthropic"]`},
	)
	got, _, err = m.PickAvailable(1, rapisTied, "anthropic")
	if err != nil {
		t.Fatalf("PickAvailable (tied): %v", err)
	}
	if got.ID != 4 {
		t.Fatalf("with tied order_index and anthropic preference: selected RAPI %d, want 4 (supports anthropic)", got.ID)
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

	var rs RAPISnapshot
	for _, s := range m.Snapshot().RAPIs {
		if s.ID == 1 {
			rs = s
		}
	}
	if rs.Cooling && rs.RecoverAt.After(time.Now()) {
		t.Fatalf("recovery loop should have cleared cooldown, but recoverAt = %v", rs.RecoverAt)
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

func TestPickAvailableKeySkipsPermanentFailure(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	keys := []models.PlatformKey{
		{ID: 1, KeyIndex: 0, Enabled: true, FailureType: 2}, // permanent failure, skip
		{ID: 2, KeyIndex: 1, Enabled: true, FailureType: 0}, // healthy
	}

	got, _, err := m.PickAvailableKey(keys)
	if err != nil {
		t.Fatalf("PickAvailableKey: %v", err)
	}
	if got.ID != 2 {
		t.Fatalf("selected key %d, want 2 (key 1 is permanent failure)", got.ID)
	}
}

func TestPickAvailableKeyAllowsTemporaryFailure(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	// failure_type=1 (temporary) keys are NOT skipped here — they're managed by
	// the scheduler's in-memory cooldown. PickAvailableKey should return them.
	keys := []models.PlatformKey{
		{ID: 1, KeyIndex: 0, Enabled: true, FailureType: 1}, // temporary failure, not skipped
	}

	got, _, err := m.PickAvailableKey(keys)
	if err != nil {
		t.Fatalf("PickAvailableKey: %v", err)
	}
	if got.ID != 1 {
		t.Fatalf("selected key %d, want 1 (temporary failure should not be skipped)", got.ID)
	}
}

func TestPickAvailableKeyAllPermanentFails(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	keys := []models.PlatformKey{
		{ID: 1, KeyIndex: 0, Enabled: true, FailureType: 2},
		{ID: 2, KeyIndex: 1, Enabled: true, FailureType: 2},
	}

	_, _, err := m.PickAvailableKey(keys)
	if !errors.Is(err, ErrAllKeysUnavailable) {
		t.Fatalf("err = %v, want ErrAllKeysUnavailable", err)
	}
}

// 轮询策略（2026-09 评审决策）：同平台多 key 一般轮询，不再偏向免费/临期
// key —— 旧 IsFree/ExpiresAt 优先测试已随之移除。
func TestPickAvailableKeyRoundRobin(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	keys := []models.PlatformKey{
		{ID: 1, PlatformID: 10, KeyIndex: 0, Enabled: true, IsFree: false},
		{ID: 2, PlatformID: 10, KeyIndex: 1, Enabled: true, IsFree: true},
		{ID: 3, PlatformID: 10, KeyIndex: 2, Enabled: true, IsFree: true},
	}

	// 连续选择应按 KeyIndex 轮转：1 → 2 → 3 → 1 …，与免费/付费无关。
	want := []int64{1, 2, 3, 1, 2, 3}
	for i, w := range want {
		got, _, err := m.PickAvailableKey(keys)
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if got.ID != w {
			t.Fatalf("pick %d = key %d, want %d (round-robin)", i, got.ID, w)
		}
	}
}

func TestPickAvailableKeyRoundRobinPerPlatform(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	pa := []models.PlatformKey{
		{ID: 1, PlatformID: 10, KeyIndex: 0, Enabled: true},
		{ID: 2, PlatformID: 10, KeyIndex: 1, Enabled: true},
	}
	pb := []models.PlatformKey{
		{ID: 3, PlatformID: 20, KeyIndex: 0, Enabled: true},
		{ID: 4, PlatformID: 20, KeyIndex: 1, Enabled: true},
	}

	// 平台 A 轮转不应影响平台 B 的游标：B 第一次仍取自己的 #3。
	if _, _, err := m.PickAvailableKey(pa); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.PickAvailableKey(pa); err != nil {
		t.Fatal(err)
	}
	got, _, err := m.PickAvailableKey(pb)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 3 {
		t.Fatalf("selected key %d, want 3 (per-platform cursor independent)", got.ID)
	}
}

func TestPickAvailableKeyRoundRobinSkipsCooling(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	keys := []models.PlatformKey{
		{ID: 1, PlatformID: 10, KeyIndex: 0, Enabled: true},
		{ID: 2, PlatformID: 10, KeyIndex: 1, Enabled: true},
	}

	// 轮到 key 1 时它已冷却 → 跳过取 key 2；下一次轮到 key 2 但它冷却 → 回到 key 1。
	m.MarkKeyTemporaryFailure(1, time.Now().Add(time.Minute), "429")
	got, _, err := m.PickAvailableKey(keys)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 2 {
		t.Fatalf("selected key %d, want 2 (cooling key skipped in rotation)", got.ID)
	}

	m.MarkKeyTemporaryFailure(2, time.Now().Add(time.Minute), "429")
	_, next, err := m.PickAvailableKey(keys)
	if !errors.Is(err, ErrAllKeysUnavailable) {
		t.Fatalf("err = %v, want ErrAllKeysUnavailable", err)
	}
	if next.IsZero() {
		t.Fatal("nextAvail should carry the earliest key recoverAt")
	}
}

func TestPickAvailableKeySkipsExpired(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	// Runtime guard: an expired-but-still-enabled key must be skipped in favour
	// of the next candidate. This covers the window before the startup sweep
	// has persisted enabled=0 for expired keys.
	past := time.Now().Add(-1 * time.Hour)
	keys := []models.PlatformKey{
		{ID: 1, KeyIndex: 0, Enabled: true, IsFree: true, ExpiresAt: &past}, // free but expired
		{ID: 2, KeyIndex: 1, Enabled: true, IsFree: false},                  // paid, healthy
	}

	got, _, err := m.PickAvailableKey(keys)
	if err != nil {
		t.Fatalf("PickAvailableKey: %v", err)
	}
	if got.ID != 2 {
		t.Fatalf("selected key %d, want 2 (expired free key must be skipped, fall back to paid)", got.ID)
	}
}

func TestPickAvailableKeyAllExpired(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	past := time.Now().Add(-1 * time.Hour)
	keys := []models.PlatformKey{
		{ID: 1, KeyIndex: 0, Enabled: true, ExpiresAt: &past},
		{ID: 2, KeyIndex: 1, Enabled: true, ExpiresAt: &past},
	}

	_, _, err := m.PickAvailableKey(keys)
	if !errors.Is(err, ErrAllKeysUnavailable) {
		t.Fatalf("err = %v, want ErrAllKeysUnavailable for all-expired keys", err)
	}
}

// 池标准冷却（2026-09 评审决策）：全池 key 进入 429 后，以第一个进入 429 的
// key 的恢复时刻为标准对齐整池与 RAPI；RAPI 冷却期间 PickAvailable 直接选
// 链上下一节点，不再 10s 空转循环。
func TestMarkPoolExhaustedAlignsKeysAndRAPI(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	// key 1 先 429（恢复较早），key 2 后 429（恢复较晚）→ 池标准 = key 1 的时刻。
	firstRecover := time.Now().Add(2 * time.Minute)
	laterRecover := time.Now().Add(5 * time.Minute)
	m.MarkKeyTemporaryFailure(1, firstRecover, "429")
	m.MarkKeyTemporaryFailure(2, laterRecover, "429")

	rapiID := int64(101)
	got := m.MarkPoolExhausted(rapiID, []int64{1, 2}, firstRecover, "key pool exhausted")
	if !got.Equal(firstRecover) {
		t.Fatalf("MarkPoolExhausted returned %v, want %v", got, firstRecover)
	}

	// 两个 key 的恢复时刻都应对齐到池标准，RAPI 冷却也 = 池标准。
	// （经 Snapshot() 读取，与 insights 面板同一路径。）
	snap := m.Snapshot()
	keyAt := func(id int64) KeySnapshot {
		for _, ks := range snap.Keys {
			if ks.ID == id {
				return ks
			}
		}
		t.Fatalf("key %d missing from snapshot", id)
		return KeySnapshot{}
	}
	for _, kid := range []int64{1, 2} {
		if got := keyAt(kid); !got.RecoverAt.Equal(firstRecover) {
			t.Fatalf("key %d RecoverAt = %v, want aligned to %v", kid, got.RecoverAt, firstRecover)
		}
	}
	rapiSnap := func(id int64) RAPISnapshot {
		for _, rs := range snap.RAPIs {
			if rs.ID == id {
				return rs
			}
		}
		t.Fatalf("rapi %d missing from snapshot", id)
		return RAPISnapshot{}
	}(rapiID)
	if !rapiSnap.RecoverAt.Equal(firstRecover) {
		t.Fatalf("RAPI RecoverAt = %v, want %v (pool-standard cooldown)", rapiSnap.RecoverAt, firstRecover)
	}

	// 全池冷却期间 PickAvailable 应跳过该 RAPI —— 表现为调用方走链上下一节点。
	rapis := []models.RAPIWithPlatform{
		{ID: rapiID, Alias: "cooling", Enabled: true, Available: true},
		{ID: 202, Alias: "next-node", Enabled: true, Available: true},
	}
	picked, _, err := m.PickAvailable(1, rapis, "")
	if err != nil {
		t.Fatalf("PickAvailable: %v", err)
	}
	if picked.ID != 202 {
		t.Fatalf("picked rapi %d, want 202 (pool-cooled rapi skipped → next chain node)", picked.ID)
	}
}

func TestPickAvailableKeyDoesNotMutateInput(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	soon := time.Now().Add(1 * time.Hour)
	later := time.Now().Add(24 * time.Hour)
	keys := []models.PlatformKey{
		{ID: 1, KeyIndex: 0, Enabled: true, IsFree: true, ExpiresAt: &later},
		{ID: 2, KeyIndex: 1, Enabled: true, IsFree: true, ExpiresAt: &soon},
	}
	// Caller-ordered snapshot must be preserved across the internal sort.
	before := append([]models.PlatformKey(nil), keys...)

	_, _, err := m.PickAvailableKey(keys)
	if err != nil {
		t.Fatalf("PickAvailableKey: %v", err)
	}
	for i := range keys {
		if keys[i].ID != before[i].ID {
			t.Fatalf("input slice mutated at index %d: got %d, want %d", i, keys[i].ID, before[i].ID)
		}
	}
}
