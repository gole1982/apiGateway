package entity

import (
	"sync"

	"gateway/internal/fsm"
)

// Platform is the state machine for one upstream provider (Platform row).
//
// States:
//
//	Healthy    — platform available (available=true in DB). Serving.
//	Unavailable — platform marked unavailable (available=false in DB). Only a
//	              successful DetectSuccess (probe against /v1/models or the
//	              format endpoints) restores it.
//
// Events:
//
//	EventDetectSuccess — probe succeeded: transition to Healthy + persist
//	                      available=true via the Store effect.
//	EventDetectFailure — probe failed: transition to Unavailable + persist
//	                      available=false (used by the gateway when a platform
//	                      probe or a platform-scope upstream failure is
//	                      observed). The caller owns notifying child RAPIs.
//
// Unlike RAPI/Key, Platform has no cooldown timers: availability is purely
// probe-driven and persisted, so the FSM is intentionally minimal.
type Platform struct {
	mu      sync.Mutex
	id      int64
	machine *fsm.Machine
	store   Store
}

// NewPlatform builds a Platform entity in the given initial state (Healthy
// when available=true, Unavailable otherwise). store may be nil (tests).
func NewPlatform(id int64, initialHealthy bool, store Store) *Platform {
	initial := StateUnavailable
	if initialHealthy {
		initial = StateHealthy
	}
	p := &Platform{id: id, store: store}
	p.machine = fsm.New(initial)
	p.machine.Add(fsm.Transition{From: StateUnavailable, Event: EventDetectSuccess, To: StateHealthy, Effect: p.onDetectSuccess})
	p.machine.Add(fsm.Transition{From: StateHealthy, Event: EventDetectFailure, To: StateUnavailable, Effect: p.onDetectFailure})
	// Self-loop so repeated success/failure is idempotent and still allows the
	// effect to run (e.g. re-persisting available after an external change).
	p.machine.Add(fsm.Transition{From: StateHealthy, Event: EventDetectSuccess, To: StateHealthy, Effect: p.onDetectSuccess})
	p.machine.Add(fsm.Transition{From: StateUnavailable, Event: EventDetectFailure, To: StateUnavailable, Effect: p.onDetectFailure})
	return p
}

// OnDetectSuccess is fired when a probe confirms the platform is reachable.
func (p *Platform) OnDetectSuccess() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.machine.Fire(EventDetectSuccess, nil)
}

// OnDetectFailure is fired when a probe fails or a platform-scope upstream
// failure is observed. Persists available=false.
func (p *Platform) OnDetectFailure() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.machine.Fire(EventDetectFailure, nil)
}

// State returns the current state.
func (p *Platform) State() State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.machine.State()
}

// PlatformSnapshot is a point-in-time view of the entity.
type PlatformSnapshot struct {
	ID        int64
	State     State
	Available bool
}

// Snapshot returns a point-in-time view of the entity.
func (p *Platform) Snapshot() PlatformSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.machine.State()
	return PlatformSnapshot{
		ID:        p.id,
		State:     st,
		Available: st == StateHealthy,
	}
}

// --- transition effects (run under p.mu, outside the machine lock) ---

func (p *Platform) onDetectSuccess(_ *fsm.Context) {
	if p.store != nil {
		_ = p.store.SetPlatformAvailable(p.id, true)
	}
}

func (p *Platform) onDetectFailure(_ *fsm.Context) {
	if p.store != nil {
		_ = p.store.SetPlatformAvailable(p.id, false)
	}
}
