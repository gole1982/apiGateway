package entity

import (
	"testing"
	"time"

	"gateway/internal/models"
)

func testConfig() CooldownConfig {
	return CooldownConfig{
		DefaultCooldown:    10 * time.Millisecond,
		TimeoutCooldown:    5 * time.Millisecond,
		MaxCooldown:        50 * time.Millisecond,
		ExponentialBackoff: true,
	}
}

// fakeStore records persistence calls for asserting side effects.
type fakeStore struct {
	rapiUnavailable []string // reasons written with available=false
	platformAvail   []bool   // available values written for platforms
	permanentKeys   []int64
	temporaryKeys   []int64
	clearedKeys     []int64
}

func (f *fakeStore) SetPlatformAvailable(id int64, available bool) error {
	f.platformAvail = append(f.platformAvail, available)
	return nil
}

func (f *fakeStore) SetRAPIUnavailable(id int64, available bool, reason string) error {
	if !available {
		f.rapiUnavailable = append(f.rapiUnavailable, reason)
	}
	return nil
}
func (f *fakeStore) MarkKeyPermanentFailure(keyID int64, reason string) error {
	f.permanentKeys = append(f.permanentKeys, keyID)
	return nil
}
func (f *fakeStore) MarkKeyTemporaryFailure(keyID int64, reason string) error {
	f.temporaryKeys = append(f.temporaryKeys, keyID)
	return nil
}
func (f *fakeStore) ClearKeyFailure(keyID int64) error {
	f.clearedKeys = append(f.clearedKeys, keyID)
	return nil
}

// --- RAPI entity ---

func TestRAPIHealthyPickable(t *testing.T) {
	r := NewRAPI(1, testConfig(), nil)
	ok, recoverAt := r.Pickable(time.Now())
	if !ok {
		t.Fatal("healthy RAPI should be pickable")
	}
	if !recoverAt.IsZero() {
		t.Fatalf("healthy RAPI recoverAt should be zero, got %v", recoverAt)
	}
}

func TestRAPISessionFailureCooling(t *testing.T) {
	r := NewRAPI(1, testConfig(), nil)
	r.OnSessionFailure("429", time.Time{}, false)
	if r.State() != StateCooling {
		t.Fatalf("state = %q, want cooling", r.State())
	}
	ok, recoverAt := r.Pickable(time.Now())
	if ok {
		t.Fatal("cooling RAPI should not be pickable")
	}
	if recoverAt.IsZero() {
		t.Fatal("cooling RAPI should have a recoverAt")
	}
}

func TestRAPISuccessRecoversFromCooling(t *testing.T) {
	r := NewRAPI(1, testConfig(), nil)
	r.OnSessionFailure("fail", time.Time{}, false)
	r.OnSuccess()
	if r.State() != StateHealthy {
		t.Fatalf("state = %q, want healthy after success", r.State())
	}
	ok, _ := r.Pickable(time.Now())
	if !ok {
		t.Fatal("RAPI should be pickable after success")
	}
}

func TestRAPICooldownExpiryRecovers(t *testing.T) {
	r := NewRAPI(1, testConfig(), nil)
	now := time.Now()
	r.OnSessionFailure("fail", time.Time{}, false)
	if ok, _ := r.Pickable(now); ok {
		t.Fatal("should not be pickable during cooldown")
	}
	// After the cooldown passes, Pickable lazily advances.
	ok, _ := r.Pickable(now.Add(100 * time.Millisecond))
	if !ok {
		t.Fatal("RAPI should recover after cooldown expiry")
	}
	if r.State() != StateHealthy {
		t.Fatalf("state = %q, want healthy after expiry", r.State())
	}
}

func TestRAPIExponentialBackoff(t *testing.T) {
	r := NewRAPI(1, testConfig(), nil)
	now := time.Now()
	r.OnSessionFailure("fail1", time.Time{}, false)
	_, first := r.Pickable(now)
	r.OnSessionFailure("fail2", time.Time{}, false)
	_, second := r.Pickable(now)
	if second.Before(first) {
		t.Fatalf("second cooldown (%v) should be after first (%v) with backoff", second, first)
	}
}

func TestRAPITimeoutDoesNotIncrementFailures(t *testing.T) {
	r := NewRAPI(1, testConfig(), nil)
	// Two timeouts should not grow consecutiveFailures.
	r.OnSessionFailure("timeout1", time.Time{}, true)
	r.OnSessionFailure("timeout2", time.Time{}, true)
	rs := r.Snapshot()
	if rs.ConsecutiveFailures != 0 {
		t.Fatalf("consecutiveFailures = %d, want 0 for timeouts", rs.ConsecutiveFailures)
	}
}

func TestRAPIInvalidateAndRevalidate(t *testing.T) {
	r := NewRAPI(1, testConfig(), nil)
	r.Invalidate()
	if r.State() != StateInvalidated {
		t.Fatalf("state = %q, want invalidated", r.State())
	}
	ok, recoverAt := r.Pickable(time.Now())
	if ok {
		t.Fatal("invalidated RAPI should not be pickable")
	}
	if !recoverAt.IsZero() {
		t.Fatalf("invalidated RAPI must not auto-recover, got recoverAt %v", recoverAt)
	}
	// Success must NOT clear invalidation.
	r.OnSuccess()
	if r.State() != StateInvalidated {
		t.Fatalf("success must not clear invalidated state, got %q", r.State())
	}
	r.Revalidate()
	if r.State() != StateHealthy {
		t.Fatalf("state = %q, want healthy after revalidate", r.State())
	}
}

func TestRAPIAllKeysHardDeadPersistsAndInvalidates(t *testing.T) {
	store := &fakeStore{}
	r := NewRAPI(1, testConfig(), store)

	first := r.OnAllKeysUnavailable(true, "所有 Key 不可用: Key 已禁用")
	if !first {
		t.Fatal("first hard-dead detection should return true")
	}
	if r.State() != StateInvalidated {
		t.Fatalf("state = %q, want invalidated", r.State())
	}
	if len(store.rapiUnavailable) != 1 || store.rapiUnavailable[0] != "所有 Key 不可用: Key 已禁用" {
		t.Fatalf("store writes = %v, want single reason write", store.rapiUnavailable)
	}

	// Second detection must not re-notify or re-persist.
	second := r.OnAllKeysUnavailable(true, "所有 Key 不可用: Key 已禁用")
	if second {
		t.Fatal("second hard-dead detection should return false (already invalidated)")
	}
	if len(store.rapiUnavailable) != 1 {
		t.Fatalf("store writes = %v, want no duplicate write", store.rapiUnavailable)
	}
}

func TestRAPIAllKeysSoftOnlyCooldowns(t *testing.T) {
	store := &fakeStore{}
	r := NewRAPI(1, testConfig(), store)

	first := r.OnAllKeysUnavailable(false, "all keys unavailable")
	if first {
		t.Fatal("soft all-keys-unavailable must not invalidate")
	}
	if r.State() != StateCooling {
		t.Fatalf("state = %q, want cooling", r.State())
	}
	if len(store.rapiUnavailable) != 0 {
		t.Fatalf("soft case must not persist RAPI unavailable, got %v", store.rapiUnavailable)
	}
}

// --- Platform entity ---

func TestPlatformDetectSuccessRestores(t *testing.T) {
	store := &fakeStore{}
	p := NewPlatform(1, false, store) // starts unavailable
	if p.State() != StateUnavailable {
		t.Fatalf("initial state = %q, want unavailable", p.State())
	}
	p.OnDetectSuccess()
	if p.State() != StateHealthy {
		t.Fatalf("state = %q, want healthy", p.State())
	}
	snap := p.Snapshot()
	if !snap.Available {
		t.Fatal("snapshot should report available=true")
	}
	if len(store.platformAvail) != 1 || store.platformAvail[0] != true {
		t.Fatalf("store writes = %v, want [true]", store.platformAvail)
	}
}

func TestPlatformDetectFailureMarksUnavailable(t *testing.T) {
	store := &fakeStore{}
	p := NewPlatform(1, true, store) // starts healthy
	p.OnDetectFailure()
	if p.State() != StateUnavailable {
		t.Fatalf("state = %q, want unavailable", p.State())
	}
	if len(store.platformAvail) != 1 || store.platformAvail[0] != false {
		t.Fatalf("store writes = %v, want [false]", store.platformAvail)
	}
}

func TestPlatformIdempotentEvents(t *testing.T) {
	store := &fakeStore{}
	p := NewPlatform(1, false, store)
	// Repeated failures stay unavailable and keep persisting (idempotent).
	p.OnDetectFailure()
	p.OnDetectFailure()
	if p.State() != StateUnavailable {
		t.Fatalf("state = %q, want unavailable", p.State())
	}
	if len(store.platformAvail) != 2 {
		t.Fatalf("store writes = %v, want 2", store.platformAvail)
	}
}

func TestIsRecoverableBillingError(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"jd credits insufficient", `{"code":"1058","message":"用户积分不足"}`, true},
		{"jd lowercase body", `{"code":"1058","message":"用户积分不足"}`, true},
		{"balance zero", `{"error":"余额不足"}`, true},
		{"billing keyword", `{"error":{"message":"billing error"}}`, true},
		{"quota exhausted", `{"error":"You have exceeded your current quota"}`, true},
		{"payment required", `{"error":"payment required"}`, true},
		{"hard ban not recoverable", `{"error":"account suspended for abuse"}`, false},
		{"empty body", ``, false},
		{"plain auth error", `{"error":"invalid api key"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRecoverableBillingError(tc.body); got != tc.want {
				t.Errorf("IsRecoverableBillingError(%q) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

func TestClassifyFailure(t *testing.T) {
	cases := map[int]FailureScope{
		401: ScopeSystem,
		402: ScopePlatform,
		403: ScopePlatform,
		409: ScopePlatform,
		423: ScopePlatform,
		451: ScopePlatform,
		400: ScopeSession,
		404: ScopeSession,
		413: ScopeSession,
		422: ScopeSession,
		429: ScopeSession,
		500: ScopeSession,
		503: ScopeSession,
	}
	for status, want := range cases {
		if got := ClassifyFailure(status); got != want {
			t.Errorf("ClassifyFailure(%d) = %v, want %v", status, got, want)
		}
	}
}

// --- Key entity ---

func TestKeyHealthyPickable(t *testing.T) {
	k := NewKey(1, testConfig(), nil)
	k.Sync(models.PlatformKey{ID: 1, Enabled: true}, time.Now())
	ok, _ := k.Pickable(time.Now())
	if !ok {
		t.Fatal("healthy key should be pickable")
	}
}

func TestKeyDisabledFromRow(t *testing.T) {
	k := NewKey(1, testConfig(), nil)
	k.Sync(models.PlatformKey{ID: 1, Enabled: false}, time.Now())
	if k.State() != StateDisabled {
		t.Fatalf("state = %q, want disabled", k.State())
	}
	ok, _ := k.Pickable(time.Now())
	if ok {
		t.Fatal("disabled key should not be pickable")
	}
}

func TestKeyExpiredFromRow(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	k := NewKey(1, testConfig(), nil)
	k.Sync(models.PlatformKey{ID: 1, Enabled: true, ExpiresAt: &past}, time.Now())
	if k.State() != StateExpired {
		t.Fatalf("state = %q, want expired", k.State())
	}
	ok, _ := k.Pickable(time.Now())
	if ok {
		t.Fatal("expired key should not be pickable")
	}
}

func TestKeyPermanentFailedFromRow(t *testing.T) {
	k := NewKey(1, testConfig(), nil)
	k.Sync(models.PlatformKey{ID: 1, Enabled: true, FailureType: 2, FailureReason: "403 denied"}, time.Now())
	if k.State() != StatePermanentFailed {
		t.Fatalf("state = %q, want permanent_failed", k.State())
	}
	ok, _ := k.Pickable(time.Now())
	if ok {
		t.Fatal("permanently failed key should not be pickable")
	}
}

func TestKeyRowHealingRecovers(t *testing.T) {
	k := NewKey(1, testConfig(), nil)
	// Key is permanently failed in DB.
	k.Sync(models.PlatformKey{ID: 1, Enabled: true, FailureType: 2}, time.Now())
	if k.State() != StatePermanentFailed {
		t.Fatalf("state = %q, want permanent_failed", k.State())
	}
	// Operator probes / re-enables → row becomes healthy → entity recovers.
	k.Sync(models.PlatformKey{ID: 1, Enabled: true, FailureType: 0}, time.Now())
	if k.State() != StateHealthy {
		t.Fatalf("state = %q, want healthy after row heals", k.State())
	}
}

func TestKeySessionFailurePersistsTemporary(t *testing.T) {
	store := &fakeStore{}
	k := NewKey(1, testConfig(), store)
	k.OnTemporaryFailure("upstream 429", time.Time{}, false)
	if k.State() != StateCooling {
		t.Fatalf("state = %q, want cooling", k.State())
	}
	if len(store.temporaryKeys) != 1 || store.temporaryKeys[0] != 1 {
		t.Fatalf("temporary failure should persist failure_type=1, got %v", store.temporaryKeys)
	}
}

func TestKeyNetworkErrorDoesNotPersist(t *testing.T) {
	store := &fakeStore{}
	k := NewKey(1, testConfig(), store)
	k.OnSessionFailure("dial timeout", time.Time{}, true)
	if k.State() != StateCooling {
		t.Fatalf("state = %q, want cooling", k.State())
	}
	if len(store.temporaryKeys) != 0 {
		t.Fatalf("network errors must not persist failure_type, got %v", store.temporaryKeys)
	}
}

func TestKeyPlatformFailurePersistsPermanent(t *testing.T) {
	store := &fakeStore{}
	k := NewKey(1, testConfig(), store)
	k.OnPlatformFailure("[平台级] upstream 403: 用户积分不足")
	if k.State() != StatePlatformFailed {
		t.Fatalf("state = %q, want platform_failed", k.State())
	}
	if len(store.permanentKeys) != 1 || store.permanentKeys[0] != 1 {
		t.Fatalf("platform failure should persist failure_type=2, got %v", store.permanentKeys)
	}
}

func TestKeySuccessClearsFailure(t *testing.T) {
	store := &fakeStore{}
	k := NewKey(1, testConfig(), store)
	k.OnPlatformFailure("403")
	k.OnSuccess()
	if k.State() != StateHealthy {
		t.Fatalf("state = %q, want healthy after success", k.State())
	}
	if len(store.clearedKeys) != 1 || store.clearedKeys[0] != 1 {
		t.Fatalf("success should clear failure, got %v", store.clearedKeys)
	}
}

func TestKeySyntheticIDNeverPersists(t *testing.T) {
	store := &fakeStore{}
	k := NewKey(-1, testConfig(), store)
	k.OnPlatformFailure("403")
	if len(store.permanentKeys) != 0 {
		t.Fatalf("synthetic keys must not persist, got %v", store.permanentKeys)
	}
	k.OnSuccess()
	if len(store.clearedKeys) != 0 {
		t.Fatalf("synthetic keys must not clear, got %v", store.clearedKeys)
	}
}

func TestKeyCooldownExpiryRecovers(t *testing.T) {
	k := NewKey(1, testConfig(), nil)
	now := time.Now()
	k.OnTemporaryFailure("429", time.Time{}, false)
	if ok, _ := k.Pickable(now); ok {
		t.Fatal("should not be pickable during cooldown")
	}
	ok, _ := k.Pickable(now.Add(100 * time.Millisecond))
	if !ok {
		t.Fatal("key should recover after cooldown expiry")
	}
	if k.State() != StateHealthy {
		t.Fatalf("state = %q, want healthy", k.State())
	}
}

func TestKeyPlatformFailureStillDeadAfterTimer(t *testing.T) {
	// A platform-failed key's in-memory cooldown lapses after MaxCooldown, but
	// the DB row still says failure_type=2. PickAvailableKey always Syncs the
	// row before Pickable, so the key is re-derived as PermanentFailed and stays
	// unusable — the row is the source of truth.
	k := NewKey(1, testConfig(), nil)
	now := time.Now()
	k.OnPlatformFailure("403")
	// Row still says permanent → Sync re-derives PermanentFailed before pick.
	k.Sync(models.PlatformKey{ID: 1, Enabled: true, FailureType: 2}, now)
	if k.State() != StatePermanentFailed {
		t.Fatalf("state = %q, want permanent_failed from row", k.State())
	}
	if ok, _ := k.Pickable(now.Add(200 * time.Millisecond)); ok {
		t.Fatal("permanently failed key should not be pickable even after cooldown")
	}
}

// --- KeysHardDead ---

func TestKeysHardDead(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	expired := func() *time.Time { t := past; return &t }
	notExpired := func() *time.Time { t := future; return &t }

	base := models.PlatformKey{ID: 1, Enabled: true, FailureType: 0}
	tests := []struct {
		name     string
		keys     []models.PlatformKey
		wantDead bool
	}{
		{"empty pool is not hard-dead", nil, false},
		{"single healthy key not dead", []models.PlatformKey{base}, false},
		{"healthy key + dead key not dead", []models.PlatformKey{
			base,
			{ID: 2, Enabled: false},
		}, false},
		{"all disabled", []models.PlatformKey{
			{ID: 1, Enabled: false},
			{ID: 2, Enabled: false},
		}, true},
		{"all permanently failed", []models.PlatformKey{
			{ID: 1, Enabled: true, FailureType: 2, FailureReason: "[平台级] upstream 403: 用户积分不足"},
			{ID: 2, Enabled: true, FailureType: 2},
		}, true},
		{"all expired", []models.PlatformKey{
			{ID: 1, Enabled: true, ExpiresAt: expired()},
		}, true},
		{"disabled + expired", []models.PlatformKey{
			{ID: 1, Enabled: false},
			{ID: 2, Enabled: true, ExpiresAt: expired()},
		}, true},
		{"cooling (temp failure) not dead", []models.PlatformKey{
			{ID: 1, Enabled: true, FailureType: 1, FailureReason: "upstream 429"},
		}, false},
		{"permanent + temp not dead", []models.PlatformKey{
			{ID: 1, Enabled: true, FailureType: 2},
			{ID: 2, Enabled: true, FailureType: 1},
		}, false},
		{"healthy with expiry future not dead", []models.PlatformKey{
			{ID: 1, Enabled: true, ExpiresAt: notExpired()},
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dead, reason := KeysHardDead(tt.keys)
			if dead != tt.wantDead {
				t.Errorf("KeysHardDead() dead = %v, want %v (reason=%q)", dead, tt.wantDead, reason)
			}
			if tt.wantDead && reason == "" {
				t.Errorf("KeysHardDead() returned empty reason for a hard-dead pool")
			}
		})
	}
}

// TestKeyBillingFailureLongCooldown verifies that a recoverable billing error
// (欠费/积分不足) applies the long BillingCooldown instead of DefaultCooldown,
// and persists failure_type=1 (temporary) — so the key stays out of the pool
// during the arrears period instead of burning a failed attempt per request,
// and auto-recovers once the account is topped up.
func TestKeyBillingFailureLongCooldown(t *testing.T) {
	cfg := testConfig()
	cfg.BillingCooldown = 30 * time.Minute
	store := &fakeStore{}
	k := NewKey(1, cfg, store)

	before := time.Now()
	k.OnBillingFailure("[平台级] upstream 403: 用户积分不足")
	if k.State() != StateCooling {
		t.Fatalf("state = %q, want cooling", k.State())
	}

	ok, recoverAt := k.Pickable(time.Now())
	if ok {
		t.Fatal("billing-failed key should not be pickable")
	}
	want := before.Add(30 * time.Minute)
	if recoverAt.Before(want.Add(-time.Second)) || recoverAt.After(want.Add(time.Second)) {
		t.Errorf("recoverAt = %v, want ≈ %v (billing cooldown, not default %v)", recoverAt, want, cfg.DefaultCooldown)
	}

	// Persists failure_type=1 (temporary), never permanent.
	if len(store.temporaryKeys) != 1 || store.temporaryKeys[0] != 1 {
		t.Fatalf("temporary writes = %v, want [1]", store.temporaryKeys)
	}
	if len(store.permanentKeys) != 0 {
		t.Fatalf("permanent writes = %v, want none", store.permanentKeys)
	}

	// Advance past the billing cooldown → auto-recovered to Healthy.
	k.Advance(before.Add(31 * time.Minute))
	if k.State() != StateHealthy {
		t.Fatalf("state after cooldown = %q, want healthy (auto-recovery)", k.State())
	}
}

func TestIsCapabilityMismatch(t *testing.T) {
	cases := []struct {
		body string
		want bool
	}{
		{`{"error":{"message":"model not found","code":"model_not_found"}}`, true},
		{`{"error":"The model 'foo' does not exist"`, true},
		{`{"error":"no such model: glm-4"`, true},
		{`{"error":{"message":"模型不存在","code":404}}`, true},
		{`{"error":"无此模型或该模型已下线"}`, true},
		{`{"error":"模型未开通，请前往控制台开通后重试"}`, true},
		{`{"error":"invalid model id"`, true},
		{`{"error":{"message":"rate limit exceeded","code":429}}`, false},
		{`{"error":"服务器繁忙，请稍后重试"}`, false},
		{`{"error":{"message":"用户积分不足","code":"1058"}}`, false}, // billing, not capability
		{``, false},
	}
	for _, c := range cases {
		if got := IsCapabilityMismatch(c.body); got != c.want {
			t.Errorf("IsCapabilityMismatch(%q) = %v, want %v", c.body, got, c.want)
		}
	}
}
