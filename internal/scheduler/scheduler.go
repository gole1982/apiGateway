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

	"gateway/internal/models"
)

var (
	ErrAllRAPIUnavailable = errors.New("all rapi unavailable")
	ErrAllKeysUnavailable = errors.New("all platform keys unavailable")
	ErrQueueFull          = errors.New("lapi wait queue full")
	ErrQueueCleared       = errors.New("lapi wait queue cleared")
	ErrWaitTimeout        = errors.New("no backend available within wait timeout, all RAPIs are cooling down")
)

// FailureScope classifies how long a RAPI or key should be considered unavailable.
//
//   - ScopeSession  — transient per-request issue (content policy, token overflow, timeout).
//     Cooldown: TimeoutCooldown or DefaultCooldown; auto-recovers via exponential backoff.
//
//   - ScopePlatform — platform-level problem (quota exhausted, overload, model deprecated).
//     Cooldown: MaxCooldown (pinned); does NOT increment consecutiveFailures so the RAPI
//     is skipped but not subject to further backoff. The gateway also writes available=false
//     to the DB so the RAPI is excluded from fresh queries until admin re-enables it.
//
// System-level disabling (user-initiated) is handled exclusively through rapi.enabled in
// the DB and never passes through the scheduler.
type FailureScope int

const (
	ScopeSession  FailureScope = iota // transient; auto-recovers
	ScopePlatform                     // platform quota/overload; persisted to DB by gateway
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
	QueueMaxLen        int
	RequestMaxWait     time.Duration
}

func DefaultConfig() Config {
	return Config{
		DefaultCooldown:    10 * time.Second,
		TimeoutCooldown:    5 * time.Second,
		MaxCooldown:        2 * time.Minute,
		ExponentialBackoff: true,
		QueueMaxLen:        1024,
		RequestMaxWait:     2 * time.Minute,
	}
}

// ConfigFromAppConfig builds a scheduler Config from the application config values.
func ConfigFromAppConfig(cooldownSec, maxCooldownSec, requestMaxWaitSec int) Config {
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
	if requestMaxWaitSec > 0 {
		cfg.RequestMaxWait = time.Duration(requestMaxWaitSec) * time.Second
	}
	return cfg
}

// Manager handles RAPI selection, failover, cooldown timers, and wait queues.
type Manager struct {
	mu       sync.Mutex
	cond     *sync.Cond
	cfg      Config
	rapis    map[int64]*rapiState
	keys     map[int64]*platformKeyState
	queues   map[int64]*waitQueue
	counters map[int64]*rapiCounters
	stop     chan struct{}
}

// rapiState holds runtime state for a single RAPI.
type rapiState struct {
	id                  int64
	unavailableUntil    time.Time
	unavailableReason   string
	consecutiveFailures int
	lastSuccess         time.Time
	lastFailure         time.Time
	invalidated         bool
}

// platformKeyState holds runtime cooldown state for a single PlatformKey.
type platformKeyState struct {
	id               int64
	unavailableUntil time.Time
	reason           string
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

// NewManager creates a new scheduler and starts the recovery loop.
func NewManager(cfg Config) *Manager {
	if cfg.DefaultCooldown <= 0 {
		cfg.DefaultCooldown = DefaultConfig().DefaultCooldown
	}
	if cfg.MaxCooldown <= 0 {
		cfg.MaxCooldown = DefaultConfig().MaxCooldown
	}
	if cfg.QueueMaxLen <= 0 {
		cfg.QueueMaxLen = DefaultConfig().QueueMaxLen
	}
	m := &Manager{
		cfg:      cfg,
		rapis:    make(map[int64]*rapiState),
		keys:     make(map[int64]*platformKeyState),
		queues:   make(map[int64]*waitQueue),
		counters: make(map[int64]*rapiCounters),
		stop:     make(chan struct{}),
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
		s := m.stateForLocked(rapi.ID)
		// Also skip if invalidated at runtime (admin disable / platform failure mid-request).
		if s.invalidated {
			continue
		}
		if s.unavailableUntil.After(now) {
			nextAvail = minNonZero(nextAvail, s.unavailableUntil)
			continue
		}
		s.clearUnavailableIfExpired(now)
		return rapi, time.Time{}, nil
	}
	return models.RAPIWithPlatform{}, nextAvail, ErrAllRAPIUnavailable
}

// InvalidateRAPI marks a RAPI's in-memory state as invalidated so that PickAvailable
// skips it immediately — even within an ongoing request that holds a snapshot of the
// old DB row. Call this whenever enabled or available is set to false in the DB.
func (m *Manager) InvalidateRAPI(rapiID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stateForLocked(rapiID)
	s.invalidated = true
	// Also pin the cooldown to "forever" so Wait() is not blocked on this RAPI.
	s.unavailableUntil = time.Now().Add(24 * time.Hour * 365)
	m.cond.Broadcast()
}

// RevalidateRAPI clears the invalidated flag so that PickAvailable will consider
// the RAPI again. Call this whenever enabled or available is set to true in the DB.
func (m *Manager) RevalidateRAPI(rapiID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stateForLocked(rapiID)
	s.invalidated = false
	s.unavailableUntil = time.Time{}
	s.unavailableReason = ""
	m.cond.Broadcast()
}

// PickAvailableKey returns the first non-cooling PlatformKey from the provided slice.
// Returns (key, retryAt, nil) on success or (zero, retryAt, ErrAllKeysUnavailable) when all
// keys are in their cooldown period.
func (m *Manager) PickAvailableKey(keys []models.PlatformKey) (models.PlatformKey, time.Time, error) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	var nextAvail time.Time
	for _, k := range keys {
		if !k.Enabled {
			continue
		}
		ks := m.keyStateForLocked(k.ID)
		if ks.unavailableUntil.After(now) {
			nextAvail = minNonZero(nextAvail, ks.unavailableUntil)
			continue
		}
		return k, time.Time{}, nil
	}
	return models.PlatformKey{}, nextAvail, ErrAllKeysUnavailable
}

// MarkKeyFailure puts a PlatformKey into cooldown (session-scope).
// isTimeout should be true when the failure was a request timeout rather than a hard error;
// in that case the shorter TimeoutCooldown is used so slow-but-alive backends recover faster.
func (m *Manager) MarkKeyFailure(keyID int64, retryAt time.Time, reason string, isTimeout ...bool) time.Time {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	ks := m.keyStateForLocked(keyID)
	until := retryAt
	if until.IsZero() || !until.After(now) {
		cooldown := m.cfg.DefaultCooldown
		if len(isTimeout) > 0 && isTimeout[0] && m.cfg.TimeoutCooldown > 0 {
			cooldown = m.cfg.TimeoutCooldown
		}
		until = now.Add(cooldown)
	}
	ks.unavailableUntil = until
	ks.reason = reason
	return until
}

// MarkKeyPlatformFailure puts a PlatformKey into a long cooldown (platform-scope).
// Used when the upstream returns a quota/billing/overload error for this key specifically.
func (m *Manager) MarkKeyPlatformFailure(keyID int64, reason string) time.Time {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	ks := m.keyStateForLocked(keyID)
	until := now.Add(m.cfg.MaxCooldown)
	ks.unavailableUntil = until
	ks.reason = reason
	return until
}

// MarkKeySuccess clears the cooldown for a PlatformKey.
func (m *Manager) MarkKeySuccess(keyID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ks := m.keys[keyID]; ks != nil {
		ks.unavailableUntil = time.Time{}
		ks.reason = ""
	}
}

// MarkSuccess clears the cooldown and resets consecutive failure count.
// Does NOT clear the invalidated flag — a disabled/unavailable RAPI must be
// re-enabled explicitly through the DB and a new query.
func (m *Manager) MarkSuccess(rapiID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.rapis[rapiID]; s != nil && !s.invalidated {
		s.lastSuccess = time.Now()
		s.consecutiveFailures = 0
		s.unavailableUntil = time.Time{}
		s.unavailableReason = ""
	}
	m.cond.Broadcast()
}

// MarkFailure sets a session-scope cooldown on the RAPI (transient failure).
// isTimeout should be true when the failure was a request timeout — timeout failures use
// TimeoutCooldown and do NOT increment consecutiveFailures to avoid inflating backoff.
func (m *Manager) MarkFailure(rapiID int64, retryAt time.Time, reason string, isTimeout ...bool) time.Time {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stateForLocked(rapiID)
	s.lastFailure = now

	timeout := len(isTimeout) > 0 && isTimeout[0]
	if !timeout {
		s.consecutiveFailures++
	}

	until := retryAt
	if until.IsZero() || !until.After(now) {
		var cooldown time.Duration
		if timeout && m.cfg.TimeoutCooldown > 0 {
			cooldown = m.cfg.TimeoutCooldown
		} else {
			cooldown = m.cfg.DefaultCooldown
			if m.cfg.ExponentialBackoff && s.consecutiveFailures > 1 {
				cooldown = cooldown << minInt(s.consecutiveFailures-1, 6)
			}
			if cooldown > m.cfg.MaxCooldown {
				cooldown = m.cfg.MaxCooldown
			}
		}
		until = now.Add(cooldown)
	}
	s.unavailableUntil = until
	s.unavailableReason = reason
	m.cond.Broadcast()
	return until
}

// MarkPlatformFailure sets a platform-scope cooldown on the RAPI (quota / overload).
// The RAPI is pinned at MaxCooldown and consecutiveFailures is NOT incremented — the
// gateway is responsible for writing available=false to the DB separately.
func (m *Manager) MarkPlatformFailure(rapiID int64, reason string) time.Time {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stateForLocked(rapiID)
	s.lastFailure = now
	// Do not increment consecutiveFailures: this is a platform condition, not a
	// transient per-request failure, so backoff arithmetic is not meaningful.
	until := now.Add(m.cfg.MaxCooldown)
	s.unavailableUntil = until
	s.unavailableReason = reason
	m.cond.Broadcast()
	return until
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

func (m *Manager) stateForLocked(id int64) *rapiState {
	s := m.rapis[id]
	if s == nil {
		s = &rapiState{id: id}
		m.rapis[id] = s
	}
	return s
}

func (m *Manager) keyStateForLocked(id int64) *platformKeyState {
	ks := m.keys[id]
	if ks == nil {
		ks = &platformKeyState{id: id}
		m.keys[id] = ks
	}
	return ks
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

func (s *rapiState) clearUnavailableIfExpired(now time.Time) {
	if !s.unavailableUntil.IsZero() && !s.unavailableUntil.After(now) {
		s.unavailableUntil = time.Time{}
		s.unavailableReason = ""
	}
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
			for _, s := range m.rapis {
				s.clearUnavailableIfExpired(now)
			}
			m.cond.Broadcast()
			m.mu.Unlock()
		case <-time.After(time.Second):
		}
	}
}

func (m *Manager) nextRecoveryLocked(now time.Time) time.Time {
	var next time.Time
	for _, s := range m.rapis {
		if s.unavailableUntil.After(now) {
			next = minNonZero(next, s.unavailableUntil)
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
