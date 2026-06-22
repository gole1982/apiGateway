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
	ErrQueueFull          = errors.New("lapi wait queue full")
	ErrQueueCleared       = errors.New("lapi wait queue cleared")
)

// Config holds scheduler configuration.
type Config struct {
	DefaultCooldown    time.Duration
	MaxCooldown        time.Duration
	ExponentialBackoff bool
	QueueMaxLen        int
	RequestMaxWait     time.Duration
}

func DefaultConfig() Config {
	return Config{
		DefaultCooldown:    5 * time.Second,
		MaxCooldown:        time.Minute,
		ExponentialBackoff: true,
		QueueMaxLen:        1024,
		RequestMaxWait:     30 * time.Second,
	}
}

// Manager handles RAPI selection, failover, cooldown timers, and wait queues.
type Manager struct {
	mu     sync.Mutex
	cond   *sync.Cond
	cfg    Config
	rapis  map[int64]*rapiState
	queues map[int64]*waitQueue
	stop   chan struct{}
}

// rapiState holds runtime state for a single RAPI.
type rapiState struct {
	id                  int64
	unavailableUntil    time.Time
	unavailableReason   string
	consecutiveFailures int
	lastSuccess         time.Time
	lastFailure         time.Time
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
		cfg:    cfg,
		rapis:  make(map[int64]*rapiState),
		queues: make(map[int64]*waitQueue),
		stop:   make(chan struct{}),
	}
	m.cond = sync.NewCond(&m.mu)
	go m.recoveryLoop()
	return m
}

func (m *Manager) Close() {
	close(m.stop)
	m.cond.Broadcast()
}

// PickAvailable sorts rapis by cost, then returns the first one whose
// unavailableUntil has expired and whose Platform is also available.
func (m *Manager) PickAvailable(lapiID int64, rapis []models.RAPIWithPlatform) (models.RAPIWithPlatform, time.Time, error) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	// Sort by computed cost ascending, then order_index ascending
	sorted := make([]models.RAPIWithPlatform, len(rapis))
	copy(sorted, rapis)
	sort.SliceStable(sorted, func(i, j int) bool {
		ci := m.computeCostLocked(sorted[i], now)
		cj := m.computeCostLocked(sorted[j], now)
		if ci != cj {
			return ci < cj
		}
		return sorted[i].OrderIndex < sorted[j].OrderIndex
	})

	var nextAvail time.Time
	for _, rapi := range sorted {
		s := m.stateForLocked(rapi.ID)
		if s.unavailableUntil.After(now) {
			nextAvail = minNonZero(nextAvail, s.unavailableUntil)
			continue
		}
		s.clearUnavailableIfExpired(now)
		return rapi, time.Time{}, nil
	}
	return models.RAPIWithPlatform{}, nextAvail, ErrAllRAPIUnavailable
}

// MarkSuccess clears the cooldown and resets consecutive failure count.
func (m *Manager) MarkSuccess(rapiID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.rapis[rapiID]; s != nil {
		s.lastSuccess = time.Now()
		s.consecutiveFailures = 0
		s.unavailableUntil = time.Time{}
		s.unavailableReason = ""
	}
	m.cond.Broadcast()
}

// MarkFailure sets a cooldown timer on the RAPI. Returns the unavailableUntil time.
func (m *Manager) MarkFailure(rapiID int64, retryAt time.Time, reason string) time.Time {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stateForLocked(rapiID)
	s.lastFailure = now
	s.consecutiveFailures++

	until := retryAt
	if until.IsZero() || !until.After(now) {
		cooldown := m.cfg.DefaultCooldown
		if m.cfg.ExponentialBackoff && s.consecutiveFailures > 1 {
			cooldown = cooldown << minInt(s.consecutiveFailures-1, 6)
		}
		if cooldown > m.cfg.MaxCooldown {
			cooldown = m.cfg.MaxCooldown
		}
		until = now.Add(cooldown)
	}
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

// rapiCounters map (protected by Manager.mu)
var counters = make(map[int64]*rapiCounters)

func (m *Manager) stateForLocked(id int64) *rapiState {
	s := m.rapis[id]
	if s == nil {
		s = &rapiState{id: id}
		m.rapis[id] = s
	}
	return s
}

func (m *Manager) countersForLocked(rapiID int64) *rapiCounters {
	c := counters[rapiID]
	if c == nil {
		c = &rapiCounters{}
		counters[rapiID] = c
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
