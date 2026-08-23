// Package fsm provides a minimal, thread-safe finite state machine engine.
//
// The engine is table-driven: transitions are registered once per entity kind
// (e.g. RAPI, PlatformKey) and every state change happens through Fire. Guards
// gate a transition (e.g. "only permanent-fail when the failure is 401/403"),
// effects run after the state change and carry side effects (persistence,
// notifications). Timed transitions model cooldowns: SetTimer schedules a
// wake-up and Advance fires the reserved TimerExpired event once the time has
// passed, letting the scheduler's recovery loop drive auto-recovery.
//
// Effects run outside the machine lock (the state change is atomic; the effect
// is post-hoc), so an effect may call back into the machine — e.g. SetTimer —
// without deadlocking. This mirrors the previous scheduler design where the
// in-memory mark and the DB write were separate steps.
package fsm

import (
	"sync"
	"time"
)

// State is a node in the state machine.
type State string

// Event triggers a transition.
type Event string

// TimerExpired is the reserved event fired by Advance when a machine's timer
// has elapsed. Entities register it to model auto-recovery from cooldown
// states (Cooling -> Healthy).
const TimerExpired Event = "__timer_expired__"

// Context carries the triggering event and payload into guards and effects.
type Context struct {
	Machine *Machine
	Event   Event
	Payload any
}

// Transition describes one edge: from From, on Event, to To. When Guard is set
// it must return true for the transition to be taken. Effect, when set, runs
// after the state change (outside the machine lock).
type Transition struct {
	From   State
	Event  Event
	To     State
	Guard  func(*Context) bool
	Effect func(*Context)
}

type stateEventKey struct {
	state State
	event Event
}

// Machine is a generic, table-driven FSM. It is safe for concurrent use.
type Machine struct {
	mu        sync.Mutex
	state     State
	enteredAt time.Time
	wakeAt    time.Time // timer for timed transitions; zero means no timer
	table     []Transition
	index     map[stateEventKey][]*Transition
}

// New creates a machine in the given initial state.
func New(initial State) *Machine {
	return &Machine{
		state:     initial,
		enteredAt: time.Now(),
		index:     make(map[stateEventKey][]*Transition),
	}
}

// Add registers a transition rule. Transitions are evaluated in registration
// order; the first whose Guard passes wins.
func (m *Machine) Add(tr Transition) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.table = append(m.table, tr)
	key := stateEventKey{tr.From, tr.Event}
	m.index[key] = append(m.index[key], &m.table[len(m.table)-1])
}

// State returns the current state.
func (m *Machine) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// EnteredAt returns when the current state was entered.
func (m *Machine) EnteredAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.enteredAt
}

// SetTimer schedules a timed transition: Advance will fire TimerExpired at or
// after at. Pass the zero time to clear the timer.
func (m *Machine) SetTimer(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wakeAt = at
}

// WakeAt returns the scheduled timer time (zero when none is set).
func (m *Machine) WakeAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.wakeAt
}

// Fire attempts a transition from the current state on the given event.
// It returns the resulting state and whether a transition occurred. When no
// transition matches, the state is unchanged and Fire returns false.
//
// The matched effect (if any) runs after the lock is released, so effects may
// safely call SetTimer or other Machine methods.
func (m *Machine) Fire(event Event, payload any) (State, bool) {
	m.mu.Lock()
	to, effect, ok := m.matchLocked(event, payload)
	if !ok {
		cur := m.state
		m.mu.Unlock()
		return cur, false
	}
	m.state = to
	m.enteredAt = time.Now()
	m.mu.Unlock()
	if effect != nil {
		effect(&Context{Machine: m, Event: event, Payload: payload})
	}
	// Return the captured destination, not m.state: the lock is released and a
	// concurrent Fire may already have moved the machine on.
	return to, true
}

// Advance fires TimerExpired when the machine's timer has elapsed. Returns the
// resulting state and whether a transition occurred. No-op when no timer is
// set or it has not elapsed yet; the timer is consumed either way.
func (m *Machine) Advance(now time.Time) (State, bool) {
	m.mu.Lock()
	if m.wakeAt.IsZero() || now.Before(m.wakeAt) {
		cur := m.state
		m.mu.Unlock()
		return cur, false
	}
	m.wakeAt = time.Time{}
	to, effect, ok := m.matchLocked(TimerExpired, nil)
	if !ok {
		cur := m.state
		m.mu.Unlock()
		return cur, false
	}
	m.state = to
	m.enteredAt = time.Now()
	m.mu.Unlock()
	if effect != nil {
		effect(&Context{Machine: m, Event: TimerExpired})
	}
	// Return the captured destination, not m.state: see Fire.
	return to, true
}

// matchLocked finds the first transition from the current state on event whose
// guard passes. Must be called with m.mu held.
func (m *Machine) matchLocked(event Event, payload any) (State, func(*Context), bool) {
	key := stateEventKey{m.state, event}
	for _, tr := range m.index[key] {
		if tr.Guard != nil {
			ctx := &Context{Machine: m, Event: event, Payload: payload}
			if !tr.Guard(ctx) {
				continue
			}
		}
		return tr.To, tr.Effect, true
	}
	return m.state, nil, false
}
