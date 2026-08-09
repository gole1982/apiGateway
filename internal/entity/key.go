package entity

import (
	"sync"
	"time"

	"gateway/internal/fsm"
	"gateway/internal/models"
)

// Key is the state machine for one PlatformKey.
//
// States:
//
//	Healthy        — usable.
//	Cooling        — session-scope cooldown (429/5xx/timeout/network error).
//	                 Auto-recovers via TimerExpired.
//	PlatformFailed — platform-scope cooldown (quota / overload / auth). Pinned
//	                 at MaxCooldown; auto-recovers. The DB row keeps
//	                 failure_type=2 (persisted by the PlatformFailure effect)
//	                 so the key stays skipped even after the cooldown lapses.
//	PermanentFailed — failure_type=2 in DB (reconciled from the row by Sync).
//	                 Only Success / Enable leaves.
//	Disabled        — enabled=0 in DB (reconciled from the row).
//	Expired         — past ExpiresAt (reconciled from the row).
//
// Events:
//
//	EventSuccess        — MarkKeySuccess (2xx / probe reset): clears cooldown,
//	                      persists ClearKeyFailure.
//	EventSessionFailure — MarkKeyFailure (network error): cooldown only.
//	EventPlatformFailure— MarkKeyPlatformFailure (401/402/403): long cooldown +
//	                      persists MarkKeyPermanentFailure.
//	EventDisable / EventExpire / EventPermanentFailure / EventEnable — fired by
//	                      Sync from the DB row; no persistence (the row IS the
//	                      source of truth for these facts).
type Key struct {
	mu      sync.Mutex
	id      int64
	machine *fsm.Machine
	cfg     CooldownConfig
	store   Store

	reason string
}

// NewKey builds a Key entity in the Healthy state. store may be nil (tests).
func NewKey(id int64, cfg CooldownConfig, store Store) *Key {
	k := &Key{id: id, cfg: cfg, store: store}
	k.machine = fsm.New(StateHealthy)
	k.register()
	return k
}

func (k *Key) register() {
	m := k.machine
	// Healthy
	m.Add(fsm.Transition{From: StateHealthy, Event: EventSessionFailure, To: StateCooling, Effect: k.onSessionFailure})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventPlatformFailure, To: StatePlatformFailed, Effect: k.onPlatformFailure})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventDisable, To: StateDisabled, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventExpire, To: StateExpired, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventPermanentFailure, To: StatePermanentFailed, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StateHealthy, Event: EventSuccess, To: StateHealthy, Effect: k.onSuccess})
	// Cooling
	m.Add(fsm.Transition{From: StateCooling, Event: EventSessionFailure, To: StateCooling, Effect: k.onSessionFailure})
	m.Add(fsm.Transition{From: StateCooling, Event: EventPlatformFailure, To: StatePlatformFailed, Effect: k.onPlatformFailure})
	m.Add(fsm.Transition{From: StateCooling, Event: EventDisable, To: StateDisabled, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StateCooling, Event: EventExpire, To: StateExpired, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StateCooling, Event: EventPermanentFailure, To: StatePermanentFailed, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StateCooling, Event: EventSuccess, To: StateHealthy, Effect: k.onSuccess})
	m.Add(fsm.Transition{From: StateCooling, Event: fsm.TimerExpired, To: StateHealthy, Effect: k.onTimerExpired})
	// PlatformFailed
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: EventPlatformFailure, To: StatePlatformFailed, Effect: k.onPlatformFailure})
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: EventDisable, To: StateDisabled, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: EventExpire, To: StateExpired, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: EventPermanentFailure, To: StatePermanentFailed, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: EventSuccess, To: StateHealthy, Effect: k.onSuccess})
	m.Add(fsm.Transition{From: StatePlatformFailed, Event: fsm.TimerExpired, To: StateHealthy, Effect: k.onTimerExpired})
	// PermanentFailed — only Success / Enable leaves; Disable/Expire are facts.
	m.Add(fsm.Transition{From: StatePermanentFailed, Event: EventSuccess, To: StateHealthy, Effect: k.onSuccess})
	m.Add(fsm.Transition{From: StatePermanentFailed, Event: EventEnable, To: StateHealthy, Effect: k.onEnable})
	m.Add(fsm.Transition{From: StatePermanentFailed, Event: EventDisable, To: StateDisabled, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StatePermanentFailed, Event: EventExpire, To: StateExpired, Effect: k.onRowFact})
	// Disabled / Expired — only Enable leaves.
	m.Add(fsm.Transition{From: StateDisabled, Event: EventEnable, To: StateHealthy, Effect: k.onEnable})
	m.Add(fsm.Transition{From: StateDisabled, Event: EventExpire, To: StateExpired, Effect: k.onRowFact})
	m.Add(fsm.Transition{From: StateExpired, Event: EventEnable, To: StateHealthy, Effect: k.onEnable})
	m.Add(fsm.Transition{From: StateExpired, Event: EventDisable, To: StateDisabled, Effect: k.onRowFact})
}

// --- public API (called by the scheduler / gateway) ---

// Sync reconciles the entity with a fresh DB row. It fires the row-fact events
// (Disable / Expire / PermanentFailure) and recovers dead states when the row
// says the key is healthy again (operator re-enabled it or probed it). The row
// is the source of truth for enabled / failure_type / expires_at; Sync must be
// called before every Pickable evaluation.
func (k *Key) Sync(row models.PlatformKey, now time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	switch {
	case !row.Enabled:
		k.machine.Fire(EventDisable, nil)
	case row.FailureType == 2:
		k.machine.Fire(EventPermanentFailure, row.FailureReason)
	case row.ExpiresAt != nil && !row.ExpiresAt.IsZero() && now.After(*row.ExpiresAt):
		k.machine.Fire(EventExpire, nil)
	default:
		// Row says healthy — recover if we are in a dead state. A Cooling or
		// PlatformFailed key stays in its cooldown (transient, auto-recovers).
		if st := k.machine.State(); st == StateDisabled || st == StateExpired || st == StatePermanentFailed {
			k.machine.Fire(EventEnable, nil)
		}
	}
}

// OnSuccess clears the cooldown and persists ClearKeyFailure (2xx / probe).
func (k *Key) OnSuccess() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.machine.Fire(EventSuccess, nil)
}

// OnSessionFailure applies a transient cooldown without persisting — used for
// network errors, where failure_type is left untouched (matches legacy).
func (k *Key) OnSessionFailure(reason string, retryAt time.Time, isTimeout bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.machine.Fire(EventSessionFailure, SessionFailurePayload{Reason: reason, RetryAt: retryAt, IsTimeout: isTimeout})
}

// OnTemporaryFailure applies a transient cooldown AND persists failure_type=1
// — used for HTTP session failures (429/5xx/400/404/413/422).
func (k *Key) OnTemporaryFailure(reason string, retryAt time.Time, isTimeout bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.machine.Fire(EventSessionFailure, SessionFailurePayload{Reason: reason, RetryAt: retryAt, IsTimeout: isTimeout, PersistTemporary: true})
}

// OnPlatformFailure applies a long cooldown AND persists failure_type=2 —
// used for 401/402/403/409/423/451.
func (k *Key) OnPlatformFailure(reason string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.machine.Fire(EventPlatformFailure, SessionFailurePayload{Reason: reason})
}

// OnBillingFailure applies a recoverable-billing-error cooldown AND persists
// failure_type=1 (temporary). The cooldown is BillingCooldown (long, e.g. 30
// minutes) rather than DefaultCooldown, so an out-of-credit key does not burn a
// failed upstream attempt on every request; it auto-recovers once the account
// is topped up and the cooldown elapses. Used for 402/403 whose body matches
// IsRecoverableBillingError.
func (k *Key) OnBillingFailure(reason string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.machine.Fire(EventSessionFailure, SessionFailurePayload{Reason: reason, IsBilling: true, PersistTemporary: true})
}

// Advance fires the timer (cooldown expiry) if due, recovering to Healthy.
func (k *Key) Advance(now time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.machine.Advance(now)
}

// State returns the current state.
func (k *Key) State() State {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.machine.State()
}

// WakeAt returns the scheduled cooldown expiry (zero when none).
func (k *Key) WakeAt() time.Time {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.machine.WakeAt()
}

// Pickable reports whether the key can serve now, and if not, when it might
// recover. Dead states (PermanentFailed / Disabled / Expired) never
// auto-recover. Must be called after Sync with the current row.
func (k *Key) Pickable(now time.Time) (ok bool, recoverAt time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	switch st := k.machine.State(); st {
	case StateHealthy:
		return true, time.Time{}
	case StateCooling, StatePlatformFailed:
		wa := k.machine.WakeAt()
		if wa.After(now) {
			return false, wa
		}
		k.machine.Advance(now)
		return k.machine.State() == StateHealthy, time.Time{}
	default:
		return false, time.Time{}
	}
}

// KeySnapshot is a point-in-time view of the entity for the dashboard.
type KeySnapshot struct {
	ID        int64
	State     State
	Cooling   bool
	RecoverAt time.Time
	Reason    string
}

// Snapshot returns a point-in-time view of the entity.
func (k *Key) Snapshot() KeySnapshot {
	k.mu.Lock()
	defer k.mu.Unlock()
	st := k.machine.State()
	wa := k.machine.WakeAt()
	now := time.Now()
	return KeySnapshot{
		ID:        k.id,
		State:     st,
		Cooling:   (st == StateCooling || st == StatePlatformFailed) && wa.After(now),
		RecoverAt: wa,
		Reason:    k.reason,
	}
}

// --- transition effects (run under k.mu, outside the machine lock) ---

func (k *Key) onSessionFailure(ctx *fsm.Context) {
	p := ctx.Payload.(SessionFailurePayload)
	now := time.Now()
	k.reason = p.Reason

	until := p.RetryAt
	if until.IsZero() || !until.After(now) {
		cooldown := k.cfg.DefaultCooldown
		if p.IsBilling && k.cfg.BillingCooldown > 0 {
			cooldown = k.cfg.BillingCooldown
		} else if p.IsTimeout && k.cfg.TimeoutCooldown > 0 {
			cooldown = k.cfg.TimeoutCooldown
		}
		until = now.Add(cooldown)
	}
	ctx.Machine.SetTimer(until)
	if p.PersistTemporary && k.store != nil && k.id > 0 {
		_ = k.store.MarkKeyTemporaryFailure(k.id, p.Reason)
	}
}

func (k *Key) onPlatformFailure(ctx *fsm.Context) {
	p := ctx.Payload.(SessionFailurePayload)
	now := time.Now()
	k.reason = p.Reason
	ctx.Machine.SetTimer(now.Add(k.cfg.MaxCooldown))
	if k.store != nil && k.id > 0 {
		_ = k.store.MarkKeyPermanentFailure(k.id, p.Reason)
	}
}

// onRowFact fires when Sync derives a dead state from the DB row — no
// persistence needed, the row already carries the fact. Clear any pending
// cooldown timer: dead states never auto-recover.
func (k *Key) onRowFact(_ *fsm.Context) {
	k.machine.SetTimer(time.Time{})
}

func (k *Key) onEnable(_ *fsm.Context) {
	k.reason = ""
}

func (k *Key) onSuccess(_ *fsm.Context) {
	k.reason = ""
	k.machine.SetTimer(time.Time{})
	if k.store != nil && k.id > 0 {
		_ = k.store.ClearKeyFailure(k.id)
	}
}

func (k *Key) onTimerExpired(_ *fsm.Context) {
	k.reason = ""
}
