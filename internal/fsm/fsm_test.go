package fsm

import (
	"sync"
	"testing"
	"time"
)

func TestBasicTransition(t *testing.T) {
	m := New(State("healthy"))
	m.Add(Transition{From: "healthy", Event: "fail", To: "cooling"})

	if got := m.State(); got != "healthy" {
		t.Fatalf("initial state = %q, want healthy", got)
	}
	to, ok := m.Fire("fail", nil)
	if !ok {
		t.Fatal("Fire(fail) did not transition")
	}
	if to != "cooling" {
		t.Fatalf("state after fail = %q, want cooling", to)
	}
	if m.State() != "cooling" {
		t.Fatalf("State() = %q, want cooling", m.State())
	}
}

func TestUndefinedTransitionIsNoop(t *testing.T) {
	m := New(State("healthy"))
	m.Add(Transition{From: "healthy", Event: "fail", To: "cooling"})

	to, ok := m.Fire("unknown", nil)
	if ok {
		t.Fatal("unknown event should not transition")
	}
	if to != "healthy" {
		t.Fatalf("state = %q, want healthy", to)
	}
}

func TestGuardBlocksTransition(t *testing.T) {
	m := New(State("healthy"))
	m.Add(Transition{From: "healthy", Event: "fail", To: "cooling", Guard: func(c *Context) bool {
		return c.Payload.(bool) // payload true = allow
	}})

	if _, ok := m.Fire("fail", false); ok {
		t.Fatal("guard=false should block")
	}
	if m.State() != "healthy" {
		t.Fatalf("state = %q, want healthy", m.State())
	}
	if _, ok := m.Fire("fail", true); !ok {
		t.Fatal("guard=true should allow")
	}
	if m.State() != "cooling" {
		t.Fatalf("state = %q, want cooling", m.State())
	}
}

func TestEffectRunsOnTransition(t *testing.T) {
	m := New(State("healthy"))
	ran := false
	m.Add(Transition{From: "healthy", Event: "fail", To: "cooling", Effect: func(c *Context) {
		ran = true
	}})

	m.Fire("fail", nil)
	if !ran {
		t.Fatal("effect did not run")
	}
}

func TestTimerAdvance(t *testing.T) {
	m := New(State("cooling"))
	m.Add(Transition{From: "cooling", Event: TimerExpired, To: "healthy"})

	m.SetTimer(time.Now().Add(time.Second))
	if _, ok := m.Advance(time.Now()); ok {
		t.Fatal("advance before timer should not transition")
	}
	if m.State() != "cooling" {
		t.Fatalf("state = %q, want cooling", m.State())
	}

	if _, ok := m.Advance(time.Now().Add(2 * time.Second)); !ok {
		t.Fatal("advance after timer should transition")
	}
	if m.State() != "healthy" {
		t.Fatalf("state = %q, want healthy", m.State())
	}
}

func TestTimerConsumedOnAdvance(t *testing.T) {
	m := New(State("cooling"))
	m.Add(Transition{From: "cooling", Event: TimerExpired, To: "healthy"})
	m.SetTimer(time.Now().Add(-time.Second))

	m.Advance(time.Now())
	if !m.WakeAt().IsZero() {
		t.Fatal("timer should be consumed after Advance")
	}
}

func TestTimerClearedBySetTimerZero(t *testing.T) {
	m := New(State("cooling"))
	m.SetTimer(time.Now().Add(time.Second))
	m.SetTimer(time.Time{})
	if !m.WakeAt().IsZero() {
		t.Fatal("timer should be cleared")
	}
}

func TestEffectMaySetTimer(t *testing.T) {
	// Effects run outside the lock, so calling SetTimer inside an effect must
	// not deadlock — the common cooldown pattern.
	m := New(State("healthy"))
	m.Add(Transition{From: "healthy", Event: "fail", To: "cooling", Effect: func(c *Context) {
		c.Machine.SetTimer(time.Now().Add(time.Minute))
	}})

	done := make(chan struct{})
	go func() {
		m.Fire("fail", nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Fire deadlocked while effect set a timer")
	}
	if m.WakeAt().IsZero() {
		t.Fatal("timer should be set by effect")
	}
}

func TestFirstMatchingGuardWins(t *testing.T) {
	m := New(State("healthy"))
	m.Add(Transition{From: "healthy", Event: "fail", To: "a", Guard: func(c *Context) bool { return false }})
	m.Add(Transition{From: "healthy", Event: "fail", To: "b", Guard: func(c *Context) bool { return true }})

	to, ok := m.Fire("fail", nil)
	if !ok || to != "b" {
		t.Fatalf("got %q ok=%v, want b", to, ok)
	}
}

func TestConcurrentFireAndAdvance(t *testing.T) {
	// Hammer one machine from many goroutines with Fire, Advance and readers.
	// Run under -race: Fire/Advance must not read m.state after unlocking, and
	// a returned state must always be one this machine actually visited.
	m := New(State("healthy"))
	m.Add(Transition{From: "healthy", Event: "fail", To: "cooling"})
	m.Add(Transition{From: "cooling", Event: "recover", To: "healthy"})
	m.Add(Transition{From: "cooling", Event: TimerExpired, To: "healthy"})

	valid := map[State]bool{"healthy": true, "cooling": true}
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				m.SetTimer(time.Now().Add(time.Millisecond))
				if _, ok := m.Fire("fail", nil); ok {
					m.Fire("recover", nil)
				}
				if to, _ := m.Advance(time.Now().Add(2 * time.Millisecond)); !valid[to] {
					t.Errorf("Advance returned unknown state %q", to)
				}
				if s := m.State(); !valid[s] {
					t.Errorf("State returned unknown state %q", s)
				}
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	close(done)
	wg.Wait()
}
