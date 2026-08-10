package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"gateway/internal/entity"
	"gateway/internal/models"
)

var (
	ErrAllRAPIUnavailable = errors.New("all rapi unavailable")
	ErrAllKeysUnavailable = errors.New("all platform keys unavailable")
	ErrQueueFull          = errors.New("lapi wait queue full")
	ErrQueueCleared       = errors.New("lapi wait queue cleared")
	ErrWaitTimeout        = errors.New("no backend available within wait timeout, all RAPIs are cooling down")
)

// Config holds scheduler configuration.
type Config struct {
	// DefaultCooldown is the base cooldown after a RAPI/key failure (exponential backoff starts here).
	DefaultCooldown time.Duration
	// TimeoutCooldown is the cooldown used when the failure was a request timeout rather than a hard
	// error (e.g. 4xx/5xx). Timeouts mean the backend is slow, not necessarily broken, so we use a
	// shorter cooldown to retry sooner.
	TimeoutCooldown    time.Duration
	MaxCooldown        time.Duration
	ExponentialBackoff bool
	// BillingCooldown is the cooldown applied to recoverable billing errors
	// (欠费/积分不足). Longer than DefaultCooldown so an out-of-credit key does
	// not burn a failed upstream attempt on every request.
	BillingCooldown time.Duration
	// CapabilityBlock is how long a key×model capability block lives after the
	// platform denied a key for a model (404 "model not found" etc.). While
	// blocked the pair is skipped; after expiry it is retried naturally so a
	// re-granted permission is picked up automatically.
	CapabilityBlock time.Duration
	QueueMaxLen     int
	RequestMaxWait  time.Duration
}

func DefaultConfig() Config {
	return Config{
		DefaultCooldown:    10 * time.Second,
		TimeoutCooldown:    5 * time.Second,
		MaxCooldown:        2 * time.Minute,
		BillingCooldown:    30 * time.Minute,
		CapabilityBlock:    24 * time.Hour,
		ExponentialBackoff: true,
		QueueMaxLen:        1024,
		RequestMaxWait:     2 * time.Minute,
	}
}

// ConfigFromAppConfig builds a scheduler Config from the application config values.
func ConfigFromAppConfig(cooldownSec, maxCooldownSec, requestMaxWaitSec, billingCooldownSec, capabilityBlockSec int) Config {
	cfg := DefaultConfig()
	if cooldownSec > 0 {
		cfg.DefaultCooldown = time.Duration(cooldownSec) * time.Second
		cfg.TimeoutCooldown = cfg.DefaultCooldown / 2
		if cfg.TimeoutCooldown < time.Second {
			cfg.TimeoutCooldown = time.Second
		}
	}
	if maxCooldownSec > 0 {
		cfg.MaxCooldown = time.Duration(maxCooldownSec) * time.Second
	}
	if billingCooldownSec > 0 {
		cfg.BillingCooldown = time.Duration(billingCooldownSec) * time.Second
	}
	if capabilityBlockSec > 0 {
		cfg.CapabilityBlock = time.Duration(capabilityBlockSec) * time.Second
	}
	if requestMaxWaitSec > 0 {
		cfg.RequestMaxWait = time.Duration(requestMaxWaitSec) * time.Second
	}
	return cfg
}

// Manager holds entity FSMs (RAPI / PlatformKey), selection, failover, and
// wait queues. All runtime state lives in the entities; this type coordinates
// picking, cooldown recovery, counters, and the per-LAPI wait queues.
type Manager struct {
	mu        sync.Mutex
	cond      *sync.Cond
	cfg       Config
	ecfg      entity.CooldownConfig
	store     entity.Store
	rapis     map[int64]*entity.RAPI
	keys      map[int64]*entity.Key
	platforms map[int64]*entity.Platform
	queues    map[int64]*waitQueue
	counters  map[int64]*rapiCounters
	stop      chan struct{}
}

// timeBucket tracks request/token counts within a fixed time window.
type timeBucket struct {
	start time.Time
	reqs  int
	toks  int
}

// rapiCounters holds minute/hour/day request and token counters for a RAPI.
type rapiCounters struct {
	minute timeBucket
	hour   timeBucket
	day    timeBucket
}

type waitQueue struct {
	count      int
	generation int64
}

// NewManager creates a new scheduler and starts the recovery loop. An optional
// Store is injected for entity persistence effects (nil in tests).
func NewManager(cfg Config, store ...entity.Store) *Manager {
	if cfg.DefaultCooldown <= 0 {
		cfg.DefaultCooldown = DefaultConfig().DefaultCooldown
	}
	if cfg.MaxCooldown <= 0 {
		cfg.MaxCooldown = DefaultConfig().MaxCooldown
	}
	if cfg.QueueMaxLen <= 0 {
		cfg.QueueMaxLen = DefaultConfig().QueueMaxLen
	}
	var st entity.Store
	if len(store) > 0 {
		st = store[0]
	}
	m := &Manager{
		cfg: cfg,
		ecfg: entity.CooldownConfig{
			DefaultCooldown:    cfg.DefaultCooldown,
			TimeoutCooldown:    cfg.TimeoutCooldown,
			MaxCooldown:        cfg.MaxCooldown,
			BillingCooldown:    cfg.BillingCooldown,
			ExponentialBackoff: cfg.ExponentialBackoff,
		},
		store:     st,
		rapis:     make(map[int64]*entity.RAPI),
		keys:      make(map[int64]*entity.Key),
		platforms: make(map[int64]*entity.Platform),
		queues:    make(map[int64]*waitQueue),
		counters:  make(map[int64]*rapiCounters),
		stop:      make(chan struct{}),
	}
	m.cond = sync.NewCond(&m.mu)
	go m.recoveryLoop()
	return m
}

func (m *Manager) Close() {
	close(m.stop)
	m.cond.Broadcast()
}

// PickAvailable sorts rapis by cost, then by order_index, then by format preference.
// preferFormat is the client's request format; RAPIs supporting it natively are preferred (as a tie-breaker).
//
// The rapis slice is a snapshot taken at the start of the request. PickAvailable re-checks
// rapi.Enabled and rapi.Available on every call so that admin changes made mid-request
// (disable, platform failure write) take effect without waiting for the next DB query.
func (m *Manager) PickAvailable(lapiID int64, rapis []models.RAPIWithPlatform, preferFormat string) (models.RAPIWithPlatform, time.Time, error) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	// Sort by computed cost ascending, then user-configured order_index ascending, then format preference
	sorted := make([]models.RAPIWithPlatform, len(rapis))
	copy(sorted, rapis)
	sort.SliceStable(sorted, func(i, j int) bool {
		ci := m.computeCostLocked(sorted[i], now)
		cj := m.computeCostLocked(sorted[j], now)
		if ci != cj {
			return ci < cj
		}
		// User-configured order takes precedence over format preference.
		if sorted[i].OrderIndex != sorted[j].OrderIndex {
			return sorted[i].OrderIndex < sorted[j].OrderIndex
		}
		// Prefer RAPIs that natively support the client's format (reduces conversion overhead).
		if preferFormat != "" {
			si := sorted[i].SupportsAPIFormat(preferFormat)
			sj := sorted[j].SupportsAPIFormat(preferFormat)
			if si != sj {
				return si
			}
		}
		return false // equal
	})

	var nextAvail time.Time
	for _, rapi := range sorted {
		// Re-check DB-level flags on every pick. The slice is a snapshot; the admin may
		// have disabled or marked unavailable this RAPI after the snapshot was taken.
		if !rapi.Enabled || !rapi.Available {
			continue
		}
		e := m.rapiEntityLocked(rapi.ID)
		ok, recoverAt := e.Pickable(now)
		if !ok {
			// Invalidated RAPIs return zero recoverAt and never auto-recover.
			if !recoverAt.IsZero() {
				nextAvail = minNonZero(nextAvail, recoverAt)
			}
			continue
		}
		return rapi, time.Time{}, nil
	}
	return models.RAPIWithPlatform{}, nextAvail, ErrAllRAPIUnavailable
}

// InvalidateRAPI freezes the RAPI entity in the Invalidated state so that
// PickAvailable skips it immediately — even within an ongoing request that
// holds a snapshot of the old DB row. Call this whenever enabled or available
// is set to false in the DB (the service layer owns that write).
func (m *Manager) InvalidateRAPI(rapiID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rapiEntityLocked(rapiID).Invalidate()
	m.cond.Broadcast()
}

// RevalidateRAPI clears the Invalidated state so that PickAvailable will
// consider the RAPI again. Call this whenever enabled or available is set to
// true in the DB (the service layer owns that write).
func (m *Manager) RevalidateRAPI(rapiID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rapiEntityLocked(rapiID).Revalidate()
	m.cond.Broadcast()
}

// MarkAllKeysUnavailable fires the RAPI-level all-keys-unavailable event:
// hard-dead pools (every key disabled/permanently-failed/expired) persist
// available=false and invalidate the RAPI; soft pools (all keys cooling) get a
// short session cooldown. Returns true when the RAPI just transitioned into
// Invalidated (first detection — the caller should notify).
func (m *Manager) MarkAllKeysUnavailable(rapiID int64, hard bool, reason string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	first := m.rapiEntityLocked(rapiID).OnAllKeysUnavailable(hard, reason)
	m.cond.Broadcast()
	return first
}

// PickAvailableKey returns the best available PlatformKey from the provided slice.
//
// Selection order (ascending priority — first match wins):
//  1. IsFree desc        — within one platform, free keys are tried before paid keys
//     so free quota is consumed first.
//  2. ExpiresAt asc      — among keys of the same free/paid tier, the key whose
//     ExpiresAt is nearest is tried first (so its remaining quota
//     is used before it lapses). Keys with no ExpiresAt (never
//     expires) sort last within their tier.
//  3. KeyIndex asc       — stable tiebreaker preserving operator-configured order.
//
// Each key entity is synced with its fresh DB row (enabled / failure_type /
// expires_at) before evaluation; dead keys (disabled, permanently failed, or
// expired) are skipped. Returns (key, retryAt, nil) on success or
// (zero, retryAt, ErrAllKeysUnavailable) when all keys are unavailable/cooling.
func (m *Manager) PickAvailableKey(keys []models.PlatformKey) (models.PlatformKey, time.Time, error) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	// Sort a copy so the caller's slice (often loaded from DB and attached to a
	// RAPI snapshot) is not mutated across requests.
	sorted := make([]models.PlatformKey, len(keys))
	copy(sorted, keys)
	sort.SliceStable(sorted, func(i, j int) bool {
		ki, kj := sorted[i], sorted[j]
		// 1. Free keys first.
		if ki.IsFree != kj.IsFree {
			return ki.IsFree
		}
		// 2. Among the same tier, nearer ExpiresAt first (nil = never = last).
		ti := keyExpiryRank(ki.ExpiresAt)
		tj := keyExpiryRank(kj.ExpiresAt)
		if ti.Before(tj) {
			return true
		}
		if ti.After(tj) {
			return false
		}
		// 3. Stable tiebreaker by operator-configured key index.
		return ki.KeyIndex < kj.KeyIndex
	})

	var nextAvail time.Time
	for _, k := range sorted {
		ke := m.keyEntityLocked(k.ID)
		ke.Sync(k, now)
		ok, recoverAt := ke.Pickable(now)
		if !ok {
			if !recoverAt.IsZero() {
				nextAvail = minNonZero(nextAvail, recoverAt)
			}
			continue
		}
		return k, time.Time{}, nil
	}
	return models.PlatformKey{}, nextAvail, ErrAllKeysUnavailable
}

// keyExpiryRank maps a key's ExpiresAt to a sort key so that nearer deadlines come
// first and "never expires" (nil/zero) sorts last. Returns the far-future sentinel
// for nil/zero so it compares greater than any real deadline.
func keyExpiryRank(t *time.Time) time.Time {
	if t == nil || t.IsZero() {
		return time.Unix(1<<62, 0) // far future — sorts after all real deadlines
	}
	return *t
}

// MarkKeyFailure puts a PlatformKey into session cooldown WITHOUT persisting
// failure_type — used for network errors (timeout / connection failures),
// matching the legacy behavior where only HTTP error responses wrote the DB.
func (m *Manager) MarkKeyFailure(keyID int64, retryAt time.Time, reason string, isTimeout ...bool) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	ke := m.keyEntityLocked(keyID)
	ke.OnSessionFailure(reason, retryAt, len(isTimeout) > 0 && isTimeout[0])
	m.cond.Broadcast()
	return ke.Snapshot().RecoverAt
}

// MarkKeyTemporaryFailure puts a PlatformKey into session cooldown AND persists
// failure_type=1 — used for HTTP session failures (429/5xx/400/404/413/422).
func (m *Manager) MarkKeyTemporaryFailure(keyID int64, retryAt time.Time, reason string, isTimeout ...bool) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	ke := m.keyEntityLocked(keyID)
	ke.OnTemporaryFailure(reason, retryAt, len(isTimeout) > 0 && isTimeout[0])
	m.cond.Broadcast()
	return ke.Snapshot().RecoverAt
}

// MarkKeyBillingFailure applies the long billing cooldown (failure_type=1) for
// a recoverable billing/quota error (欠费/积分不足). The key stays out of the
// pool for BillingCooldown instead of DefaultCooldown, so an out-of-credit key
// does not burn a failed upstream attempt on every request; it auto-recovers
// once the account is topped up.
func (m *Manager) MarkKeyBillingFailure(keyID int64, reason string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	ke := m.keyEntityLocked(keyID)
	ke.OnBillingFailure(reason)
	m.cond.Broadcast()
	return ke.Snapshot().RecoverAt
}

// MarkKeyPlatformFailure puts a PlatformKey into a long cooldown AND persists
// failure_type=2 — used when the upstream returns a quota/billing/auth error
// (401/402/403/409/423/451) for this key specifically.
func (m *Manager) MarkKeyPlatformFailure(keyID int64, reason string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	ke := m.keyEntityLocked(keyID)
	ke.OnPlatformFailure(reason)
	m.cond.Broadcast()
	return ke.Snapshot().RecoverAt
}

// RemoveKey drops a deleted key's in-memory entity so it stops participating
// in cooldown scans / recovery wakeups and stops being reported in snapshots.
func (m *Manager) RemoveKey(keyID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.keys, keyID)
}

// MarkKeySuccess clears the cooldown for a PlatformKey and persists
// failure_type=0.
func (m *Manager) MarkKeySuccess(keyID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ke := m.keyEntityLocked(keyID)
	ke.OnSuccess()
	m.cond.Broadcast()
}

// MarkSuccess clears the cooldown and resets consecutive failure count.
// Does NOT clear the Invalidated state — a disabled/unavailable RAPI must be
// re-enabled explicitly through the DB and a new query.
func (m *Manager) MarkSuccess(rapiID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e := m.rapis[rapiID]; e != nil {
		e.OnSuccess()
		m.cond.Broadcast()
	}
}

// MarkFailure sets a session-scope cooldown on the RAPI (transient failure).
// isTimeout should be true when the failure was a request timeout — timeout
// failures use TimeoutCooldown and do NOT increment consecutiveFailures.
func (m *Manager) MarkFailure(rapiID int64, retryAt time.Time, reason string, isTimeout ...bool) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.rapiEntityLocked(rapiID)
	e.OnSessionFailure(reason, retryAt, len(isTimeout) > 0 && isTimeout[0])
	m.cond.Broadcast()
	return e.Snapshot().RecoverAt
}

// MarkPlatformFailure sets a platform-scope cooldown on the RAPI (quota /
// overload). The RAPI is pinned at MaxCooldown and consecutiveFailures is NOT
// incremented.
func (m *Manager) MarkPlatformFailure(rapiID int64, reason string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.rapiEntityLocked(rapiID)
	e.OnPlatformFailure(reason)
	m.cond.Broadcast()
	return e.Snapshot().RecoverAt
}

// RecordRequest updates in-memory counters for a RAPI after a request completes.
func (m *Manager) RecordRequest(rapiID int64, tokens int) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	c := m.countersForLocked(rapiID)

	// Minute bucket
	minuteStart := now.Truncate(time.Minute)
	if c.minute.start != minuteStart {
		c.minute = timeBucket{start: minuteStart}
	}
	c.minute.reqs++
	c.minute.toks += tokens

	// Hour bucket
	hourStart := now.Truncate(time.Hour)
	if c.hour.start != hourStart {
		c.hour = timeBucket{start: hourStart}
	}
	c.hour.reqs++
	c.hour.toks += tokens

	// Day bucket (24h rolling)
	dayStart := now.Truncate(24 * time.Hour)
	if c.day.start != dayStart {
		c.day = timeBucket{start: dayStart}
	}
	c.day.reqs++
	c.day.toks += tokens
}

// Wait blocks until a RAPI might become available or the context is cancelled.
func (m *Manager) Wait(ctx context.Context, lapiID int64, until time.Time) error {
	timedByLAPI := false
	if m.cfg.RequestMaxWait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.cfg.RequestMaxWait)
		defer cancel()
		timedByLAPI = true
	}
	m.mu.Lock()
	q := m.queueForLocked(lapiID)
	if q.count >= m.cfg.QueueMaxLen {
		m.mu.Unlock()
		return ErrQueueFull
	}
	q.count++
	generation := q.generation
	defer func() {
		q.count--
		m.mu.Unlock()
	}()

	wake := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			m.cond.Broadcast()
		case <-wake:
		}
	}()
	defer close(wake)

	for {
		if q.generation != generation {
			return ErrQueueCleared
		}
		if err := ctx.Err(); err != nil {
			if timedByLAPI && errors.Is(err, context.DeadlineExceeded) {
				m.clearQueueLocked(lapiID)
				return ErrWaitTimeout
			}
			return err
		}
		now := time.Now()
		if until.IsZero() || !until.After(now) {
			return nil
		}
		timer := time.AfterFunc(until.Sub(now), func() {
			m.mu.Lock()
			m.cond.Broadcast()
			m.mu.Unlock()
		})
		m.cond.Wait()
		timer.Stop()
	}
}

// EstimateCost returns a rough token estimate from the request messages.
// Used as fallback when upstream doesn't report actual token usage.
func EstimateCost(req *models.ProxyRequest) int {
	tokens := 1
	for _, msg := range req.Messages {
		tokens += len(msg.Content)/4 + 1
	}
	return tokens
}

// RetryAt parses Retry-After or X-RateLimit-Reset headers.
func RetryAt(header http.Header, fallback time.Time) time.Time {
	if header == nil {
		return fallback
	}
	value := header.Get("Retry-After")
	if value == "" {
		value = header.Get("X-RateLimit-Reset")
	}
	if value == "" {
		return fallback
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds > 1000000000 {
			return time.Unix(int64(seconds), 0)
		}
		return time.Now().Add(time.Duration(seconds) * time.Second)
	}
	if at, err := http.ParseTime(value); err == nil {
		return at
	}
	return fallback
}

// --- internal helpers ---

func (m *Manager) rapiEntityLocked(id int64) *entity.RAPI {
	e := m.rapis[id]
	if e == nil {
		e = entity.NewRAPI(id, m.ecfg, m.store)
		m.rapis[id] = e
	}
	return e
}

func (m *Manager) keyEntityLocked(id int64) *entity.Key {
	ke := m.keys[id]
	if ke == nil {
		ke = entity.NewKey(id, m.ecfg, m.store)
		m.keys[id] = ke
	}
	return ke
}

// PlatformEntity returns the Platform FSM for the given platform ID, creating
// it lazily with the persisted availability as the initial state.
func (m *Manager) PlatformEntity(platformID int64, available bool) *entity.Platform {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.platforms[platformID]
	if p == nil {
		p = entity.NewPlatform(platformID, available, m.store)
		m.platforms[platformID] = p
	}
	return p
}

// PlatformSnapshots returns point-in-time views of every known Platform entity.
func (m *Manager) PlatformSnapshots() []entity.PlatformSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	snaps := make([]entity.PlatformSnapshot, 0, len(m.platforms))
	for _, p := range m.platforms {
		snaps = append(snaps, p.Snapshot())
	}
	return snaps
}

func (m *Manager) countersForLocked(rapiID int64) *rapiCounters {
	c := m.counters[rapiID]
	if c == nil {
		c = &rapiCounters{}
		m.counters[rapiID] = c
	}
	return c
}

func (m *Manager) queueForLocked(lapiID int64) *waitQueue {
	q := m.queues[lapiID]
	if q == nil {
		q = &waitQueue{}
		m.queues[lapiID] = q
	}
	return q
}

func (m *Manager) clearQueueLocked(lapiID int64) {
	q := m.queueForLocked(lapiID)
	q.generation++
	m.cond.Broadcast()
}

// computeCostLocked calculates the current cost for a RAPI.
// Must be called with Manager.mu held.
func (m *Manager) computeCostLocked(rapi models.RAPIWithPlatform, now time.Time) int {
	// ① Time period override
	if rapi.TimePeriodRules != "" {
		var rules []timePeriodRule
		if err := json.Unmarshal([]byte(rapi.TimePeriodRules), &rules); err == nil {
			hhmm := now.Format("15:04")
			for _, rule := range rules {
				if inTimeRange(hhmm, rule.Start, rule.End) {
					return rule.Cost
				}
			}
		}
	}

	// ② Threshold check
	c := m.countersForLocked(rapi.ID)
	if thresholdExceeded(c, rapi, now) {
		if rapi.HighCost > 0 {
			return rapi.HighCost
		}
	}

	// ③ Base cost
	if rapi.BaseCost > 0 {
		return rapi.BaseCost
	}
	return 0
}

type timePeriodRule struct {
	Start string `json:"start"`
	End   string `json:"end"`
	Cost  int    `json:"cost"`
}

func inTimeRange(current, start, end string) bool {
	if start <= end {
		// Normal range, e.g. "09:00" - "22:00"
		return current >= start && current < end
	}
	// Overnight range, e.g. "22:00" - "09:00"
	return current >= start || current < end
}

func thresholdExceeded(c *rapiCounters, rapi models.RAPIWithPlatform, now time.Time) bool {
	// Ensure buckets are current before checking
	minuteStart := now.Truncate(time.Minute)
	hourStart := now.Truncate(time.Hour)
	dayStart := now.Truncate(24 * time.Hour)

	minReqs := c.minute.reqs
	minToks := c.minute.toks
	if c.minute.start != minuteStart {
		minReqs = 0
		minToks = 0
	}
	hourReqs := c.hour.reqs
	hourToks := c.hour.toks
	if c.hour.start != hourStart {
		hourReqs = 0
		hourToks = 0
	}
	dayReqs := c.day.reqs
	dayToks := c.day.toks
	if c.day.start != dayStart {
		dayReqs = 0
		dayToks = 0
	}

	if rapi.RPMLimit > 0 && minReqs >= rapi.RPMLimit {
		return true
	}
	if rapi.RPHLimit > 0 && hourReqs >= rapi.RPHLimit {
		return true
	}
	if rapi.RPDLimit > 0 && dayReqs >= rapi.RPDLimit {
		return true
	}
	if rapi.TPMLimit > 0 && minToks >= rapi.TPMLimit {
		return true
	}
	if rapi.TPHLimit > 0 && hourToks >= rapi.TPHLimit {
		return true
	}
	if rapi.TPDLimit > 0 && dayToks >= rapi.TPDLimit {
		return true
	}
	return false
}

func (m *Manager) recoveryLoop() {
	for {
		m.mu.Lock()
		next := m.nextRecoveryLocked(time.Now())
		m.mu.Unlock()
		var timer <-chan time.Time
		if !next.IsZero() {
			timer = time.After(time.Until(next))
		}
		select {
		case <-m.stop:
			return
		case <-timer:
			m.mu.Lock()
			now := time.Now()
			for _, e := range m.rapis {
				e.Advance(now)
			}
			for _, ke := range m.keys {
				ke.Advance(now)
			}
			m.cond.Broadcast()
			m.mu.Unlock()
		case <-time.After(time.Second):
		}
	}
}

func (m *Manager) nextRecoveryLocked(now time.Time) time.Time {
	var next time.Time
	for _, e := range m.rapis {
		if wa := e.WakeAt(); wa.After(now) {
			next = minNonZero(next, wa)
		}
	}
	for _, ke := range m.keys {
		if wa := ke.WakeAt(); wa.After(now) {
			next = minNonZero(next, wa)
		}
	}
	return next
}

func minNonZero(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
