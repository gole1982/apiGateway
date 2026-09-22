package entity

import (
	"sync"
	"time"

	"gateway/internal/fsm"
)

// RAPI is the state machine for one upstream model endpoint (RAPI row).
//
// States:
//
//	Healthy        — serving.
//	Cooling        — session-scope cooldown (transient failure). Auto-recovers
//	                 via the TimerExpired transition when the cooldown elapses.
//	PlatformFailed — platform-scope cooldown (quota / overload / model
//	                 deprecated). Pinned at MaxCooldown; auto-recovers.
//	Invalidated    — persisted unavailable (available=false in DB) or admin
//	                 disabled. Never auto-recovers; only EventRevalidate leaves.
//
// Events map 1:1 to the old scheduler/gateway calls:
//
//	EventSuccess        — MarkSuccess
//	EventSessionFailure — MarkFailure (transient)
//	EventPlatformFailure— MarkPlatformFailure
//	EventAllKeysSoft    — handleAllKeysUnavailable (non-hard-dead pool)
//	EventAllKeysHard    — handleAllKeysUnavailable (hard-dead pool): persists
//	                      available=false + invalidates
//	EventInvalidate     — InvalidateRAPI (admin disable / platform failure write)
//	EventRevalidate     — RevalidateRAPI (admin enable / probe success)
type RAPI struct {
	mu      sync.Mutex
	id      int64
	machine *fsm.Machine
	cfg     CooldownConfig
	store   Store

	consecutiveFailures int
	lastSuccess         time.Time
	lastFailure         time.Time
	reason              string
}

// NewRAPI builds a RAPI entity in the Healthy state with its transition table
// wired up. store may be nil (tests); persistence effects are skipped then.
func NewRAPI(id int64, cfg CooldownConfig, store Store) *RAPI {
	r := &RAPI{id: id, cfg: cfg, store: store}
	r.machine = fsm.New(StateHealthy)
	r.register()
	return r
}

func (r *RAPI) register() {
	m := r.machine
	// Healthy
	m.Add(fsm.Transition{From: StateHealthy, Event: EventSessionFailure, To: StateCooling, Effect: r.onSessionFailure})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventPlatformFailure, To: StatePlatformFailed, Effect: r.onPlatformFailure})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventInvalidate, To: StateInvalidated, Effect: r.onInvalidate})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventAllKeysSoft, To: StateCooling, Effect: r.onAllKeysSoft})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventAllKeysHard, To: StateInvalidated, Effect: r.onAllKeysHard})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventSuccess, To: StateHealthy, Effect: r.onSuccess})
	// Cooling
	m.Add(fsm.Transition{From: StateCooling, Event: EventSessionFailure, To: StateCooling, Effect: r.onSessionFailure})
	m.Add(fsm.Transition{From: StateCooling, Event: EventPlatformFailure, To: StatePlatformFailed, Effect: r.onPlatformFailure})
	m.Add(fsm.Transition{From: StateCooling, Event: EventInvalidate, To: StateInvalidated, Effect: r.onInvalidate})
	m.Add(fsm.Transition{From: StateCooling, Event: EventAllKeysSoft, To: StateCooling, Effect: r.onAllKeysSoft})
	m.Add(fsm.Transition{From: StateCooling, Event: EventAllKeysHard, To: StateInvalidated, Effect: r.onAllKeysHard})
	m.Add(fsm.Transition{From: StateCooling, Event: EventSuccess, To: StateHealthy, Effect: r.onSuccess})
	m.Add(fsm.Transition{From: StateCooling, Event: fsm.TimerExpired, To: StateHealthy, Effect: r.onTimerExpired})
	// PlatformFailed
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: EventSessionFailure, To: StateCooling, Effect: r.onSessionFailure})
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: EventInvalidate, To: StateInvalidated, Effect: r.onInvalidate})
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: EventAllKeysHard, To: StateInvalidated, Effect: r.onAllKeysHard})
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: EventSuccess, To: StateHealthy, Effect: r.onSuccess})
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: fsm.TimerExpired, To: StateHealthy, Effect: r.onTimerExpired})
	// Invalidated — only Revalidate leaves; every other event is a no-op.
	m.Add(fsm.Transition{From: StateInvalidated, Event: EventRevalidate, To: StateHealthy, Effect: r.onRevalidate})
}

// --- public API (called by the scheduler / gateway) ---

func (r *RAPI) OnSuccess() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machine.Fire(EventSuccess, nil)
}

// OnSessionFailure marks a transient failure (session scope). retryAt honors a
// Retry-After / X-RateLimit-Reset header; zero means compute from backoff.
func (r *RAPI) OnSessionFailure(reason string, retryAt time.Time, isTimeout bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machine.Fire(EventSessionFailure, SessionFailurePayload{Reason: reason, RetryAt: retryAt, IsTimeout: isTimeout})
}

// OnPlatformFailure marks a platform-scope failure (quota / overload).
func (r *RAPI) OnPlatformFailure(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machine.Fire(EventPlatformFailure, SessionFailurePayload{Reason: reason})
}

// OnAllKeysUnavailable is fired when every key of this RAPI was unusable for a
// request. hard=true (every key disabled/permanently-failed/expired) persists
// available=false and invalidates the RAPI; hard=false (all keys merely
// cooling) applies a short session cooldown. Returns true when the RAPI just
// transitioned into Invalidated (first detection — the caller should notify).
func (r *RAPI) OnAllKeysUnavailable(hard bool, reason string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ev := EventAllKeysSoft
	if hard {
		ev = EventAllKeysHard
	}
	after, changed := r.machine.Fire(ev, AllKeysPayload{Hard: hard, Reason: reason})
	return changed && after == StateInvalidated
}

// Invalidate mirrors the old InvalidateRAPI: admin disable / platform failure
// write. The caller is responsible for the DB write (service layer does it
// before calling); the entity only freezes the in-memory state.
func (r *RAPI) Invalidate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machine.Fire(EventInvalidate, nil)
}

// Revalidate mirrors the old RevalidateRAPI: admin enable / probe success.
// The caller persists available=true before calling.
func (r *RAPI) Revalidate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machine.Fire(EventRevalidate, nil)
}

// Advance fires the timer (cooldown expiry) if due, recovering to Healthy.
func (r *RAPI) Advance(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machine.Advance(now)
}

// State returns the current state.
func (r *RAPI) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.machine.State()
}

// AlignCooldownTo 对齐池标准冷却：把 RAPI 的冷却结束时刻直接设为 at。
// 仅对 Cooling 状态生效；Invalidated 不靠定时器恢复，PlatformFailed/Healthy
// 不属于软池路径。由 scheduler.MarkPoolExhausted 在软池事件后调用。
func (r *RAPI) AlignCooldownTo(at time.Time) {
	if at.IsZero() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.machine.State() == StateCooling {
		r.machine.SetTimer(at)
	}
}

// WakeAt returns the scheduled cooldown expiry (zero when none).
func (r *RAPI) WakeAt() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.machine.WakeAt()
}

// Pickable reports whether the RAPI can serve now, and if not, when it might
// recover. Invalidated RAPIs never auto-recover (zero recovery time). Expired
// cooldowns are lazily advanced here, mirroring the old clearUnavailableIfExpired.
func (r *RAPI) Pickable(now time.Time) (ok bool, recoverAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch st := r.machine.State(); st {
	case StateHealthy:
		return true, time.Time{}
	case StateInvalidated:
		return false, time.Time{}
	default: // Cooling / PlatformFailed
		wa := r.machine.WakeAt()
		if wa.After(now) {
			return false, wa
		}
		r.machine.Advance(now)
		return r.machine.State() == StateHealthy, time.Time{}
	}
}

// RAPISnapshot is a point-in-time view of the entity for the dashboard.
type RAPISnapshot struct {
	ID                  int64
	State               State
	Cooling             bool
	RecoverAt           time.Time
	Reason              string
	ConsecutiveFailures int
	LastSuccess         time.Time
	LastFailure         time.Time
	Invalidated         bool
}

// Snapshot returns a point-in-time view of the entity.
func (r *RAPI) Snapshot() RAPISnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.machine.State()
	wa := r.machine.WakeAt()
	now := time.Now()
	return RAPISnapshot{
		ID:                  r.id,
		State:               st,
		Cooling:             (st == StateCooling || st == StatePlatformFailed) && wa.After(now),
		RecoverAt:           wa,
		Reason:              r.reason,
		ConsecutiveFailures: r.consecutiveFailures,
		LastSuccess:         r.lastSuccess,
		LastFailure:         r.lastFailure,
		Invalidated:         st == StateInvalidated,
	}
}

// --- transition effects (run under r.mu, outside the machine lock) ---

func (r *RAPI) onSessionFailure(ctx *fsm.Context) {
	p := ctx.Payload.(SessionFailurePayload)
	now := time.Now()
	r.lastFailure = now
	r.reason = p.Reason
	if !p.IsTimeout {
		r.consecutiveFailures++
	}

	until := p.RetryAt
	if until.IsZero() || !until.After(now) {
		cooldown := r.cfg.DefaultCooldown
		if p.IsTimeout && r.cfg.TimeoutCooldown > 0 {
			cooldown = r.cfg.TimeoutCooldown
		} else {
			if r.cfg.ExponentialBackoff && r.consecutiveFailures > 1 {
				cooldown = cooldown << minInt(r.consecutiveFailures-1, 6)
			}
			if cooldown > r.cfg.MaxCooldown {
				cooldown = r.cfg.MaxCooldown
			}
		}
		until = now.Add(cooldown)
	}
	ctx.Machine.SetTimer(until)
}

func (r *RAPI) onPlatformFailure(ctx *fsm.Context) {
	p := ctx.Payload.(SessionFailurePayload)
	now := time.Now()
	r.lastFailure = now
	r.reason = p.Reason
	// Do not increment consecutiveFailures: platform condition, not a transient
	// per-request failure, so backoff arithmetic is not meaningful.
	ctx.Machine.SetTimer(now.Add(r.cfg.MaxCooldown))
}

func (r *RAPI) onAllKeysSoft(ctx *fsm.Context) {
	p := ctx.Payload.(AllKeysPayload)
	// 池标准冷却：软池（全部 key 冷却中）时把 RAPI 的冷却对齐到池内最早
	// 的 key 恢复时刻（第一个进入 429 的 key），而不是默认短冷却 —— 否则
	// RAPI 冷却先到期、PickAvailable 又选中它、key 仍冷却、再标记，形成
	// 空转循环（表现为请求一直等待当前 key 冷却而不用链上下一节点）。
	if !p.PoolRecoverAt.IsZero() {
		now := time.Now()
		r.lastFailure = now
		r.reason = p.Reason
		ctx.Machine.SetTimer(p.PoolRecoverAt)
		return
	}
	// 无池时间信息（如能力黑名单池）退回短会话冷却。
	r.onSessionFailure(&fsm.Context{Machine: ctx.Machine, Payload: SessionFailurePayload{Reason: p.Reason}})
}

func (r *RAPI) onAllKeysHard(ctx *fsm.Context) {
	p := ctx.Payload.(AllKeysPayload)
	now := time.Now()
	r.lastFailure = now
	r.reason = p.Reason
	// No timer: Invalidated never auto-recovers.
	if r.store != nil {
		_ = r.store.SetRAPIUnavailable(r.id, false, p.Reason)
	}
}

func (r *RAPI) onInvalidate(_ *fsm.Context) {
	// Freeze in-memory state; the caller owns the DB write. Clear any pending
	// cooldown timer from a prior Cooling/PlatformFailed state — Invalidated
	// never auto-recovers.
	r.machine.SetTimer(time.Time{})
}

func (r *RAPI) onRevalidate(_ *fsm.Context) {
	r.reason = ""
	r.machine.SetTimer(time.Time{})
	// Keep consecutiveFailures/lastFailure history — matches the old
	// RevalidateRAPI, which only cleared unavailableUntil + reason.
}

func (r *RAPI) onSuccess(_ *fsm.Context) {
	now := time.Now()
	r.lastSuccess = now
	r.consecutiveFailures = 0
	r.reason = ""
	r.machine.SetTimer(time.Time{})
}

func (r *RAPI) onTimerExpired(_ *fsm.Context) {
	r.reason = ""
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
