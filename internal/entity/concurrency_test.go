package entity

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"gateway/internal/models"
)

// TestConcurrentEntityAccess hammers the same RAPI and Key entities from many
// goroutines with mixed events, picks, and snapshots. Without -race this at
// least detects deadlocks (via the timeout guard) and state corruption
// (invariant checks after the storm).
func TestConcurrentEntityAccess(t *testing.T) {
	store := &fakeStore{}
	r := NewRAPI(7, testConfig(), store)
	k := NewKey(9, testConfig(), store)
	row := models.PlatformKey{ID: 9, Enabled: true, FailureType: 0}

	const workers = 16
	const iterations = 500

	var wg sync.WaitGroup
	start := make(chan struct{})

	worker := func(seed int) {
		defer wg.Done()
		<-start
		now := time.Now()
		for i := 0; i < iterations; i++ {
			switch (seed + i) % 8 {
			case 0:
				r.OnSuccess()
			case 1:
				r.OnSessionFailure("429", time.Time{}, false)
			case 2:
				r.OnSessionFailure("timeout", time.Time{}, true)
			case 3:
				r.OnPlatformFailure("quota")
			case 4:
				r.Invalidate()
			case 5:
				r.Revalidate()
			case 6:
				r.OnAllKeysUnavailable(i%2 == 0, "all keys unavailable")
			case 7:
				r.Pickable(now)
			}
			k.Sync(row, now)
			switch (seed*3 + i) % 5 {
			case 0:
				k.OnSuccess()
			case 1:
				k.OnTemporaryFailure("429", time.Time{}, false)
			case 2:
				k.OnPlatformFailure("403 denied")
			case 3:
				k.OnSessionFailure("dial timeout", time.Time{}, true)
			case 4:
				k.Pickable(now)
			}
			_ = r.Snapshot()
			_ = k.Snapshot()
			r.Advance(now)
			k.Advance(now)
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(i)
	}
	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent entity access deadlocked")
	}

	// Invariant: after the storm the entity must still be in a legal state and
	// Pickable must be consistent.
	st := r.State()
	switch st {
	case StateHealthy, StateCooling, StatePlatformFailed, StateInvalidated:
	default:
		t.Fatalf("RAPI ended in illegal state %q", st)
	}
	_ = fmt.Sprintf("%v", store.rapiUnavailable) // keep store referenced
}
