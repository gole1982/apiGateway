package scheduler

import "time"

// RAPISnapshot is a point-in-time copy of a RAPI's runtime state.
type RAPISnapshot struct {
	ID                  int64     `json:"id"`
	Cooling             bool      `json:"cooling"`
	RecoverAt           time.Time `json:"recover_at,omitempty"`
	Reason              string    `json:"reason,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	LastSuccess         time.Time `json:"last_success"`
	LastFailure         time.Time `json:"last_failure"`
	Invalidated         bool      `json:"invalidated"`
}

// KeySnapshot is a point-in-time copy of a PlatformKey's cooldown state.
type KeySnapshot struct {
	ID        int64     `json:"id"`
	Cooling   bool      `json:"cooling"`
	RecoverAt time.Time `json:"recover_at,omitempty"`
	Reason    string    `json:"reason,omitempty"`
}

// CounterSnapshot is a point-in-time copy of a RAPI's request/token counters.
type CounterSnapshot struct {
	RapiID    int64 `json:"rapi_id"`
	MinuteReq int   `json:"minute_req"`
	MinuteTok int   `json:"minute_tok"`
	HourReq   int   `json:"hour_req"`
	HourTok   int   `json:"hour_tok"`
	DayReq    int   `json:"day_req"`
	DayTok    int   `json:"day_tok"`
}

// QueueSnapshot is a point-in-time copy of a LAPI's wait queue state.
type QueueSnapshot struct {
	LapiID int64 `json:"lapi_id"`
	Count  int   `json:"count"`
}

// Snapshot is a complete point-in-time view of the scheduler's in-memory state.
type Snapshot struct {
	RAPIs    []RAPISnapshot    `json:"rapis"`
	Keys     []KeySnapshot     `json:"keys"`
	Counters []CounterSnapshot `json:"counters"`
	Queues   []QueueSnapshot   `json:"queues"`
}

// Snapshot returns a consistent snapshot of all in-memory scheduler state,
// sourced from the entity FSMs. Safe to call concurrently; acquires the lock once.
func (m *Manager) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap := Snapshot{
		RAPIs:    make([]RAPISnapshot, 0, len(m.rapis)),
		Keys:     make([]KeySnapshot, 0, len(m.keys)),
		Counters: make([]CounterSnapshot, 0, len(m.counters)),
		Queues:   make([]QueueSnapshot, 0, len(m.queues)),
	}

	for _, e := range m.rapis {
		es := e.Snapshot()
		snap.RAPIs = append(snap.RAPIs, RAPISnapshot{
			ID:                  es.ID,
			Cooling:             es.Cooling,
			RecoverAt:           es.RecoverAt,
			Reason:              es.Reason,
			ConsecutiveFailures: es.ConsecutiveFailures,
			LastSuccess:         es.LastSuccess,
			LastFailure:         es.LastFailure,
			Invalidated:         es.Invalidated,
		})
	}

	for _, ke := range m.keys {
		ks := ke.Snapshot()
		snap.Keys = append(snap.Keys, KeySnapshot{
			ID:        ks.ID,
			Cooling:   ks.Cooling,
			RecoverAt: ks.RecoverAt,
			Reason:    ks.Reason,
		})
	}

	now := time.Now()
	for rapiID, c := range m.counters {
		cs := CounterSnapshot{RapiID: rapiID}
		// Only report counts if the bucket is current; stale buckets mean 0 activity.
		minuteStart := now.Truncate(time.Minute)
		if c.minute.start == minuteStart {
			cs.MinuteReq = c.minute.reqs
			cs.MinuteTok = c.minute.toks
		}
		hourStart := now.Truncate(time.Hour)
		if c.hour.start == hourStart {
			cs.HourReq = c.hour.reqs
			cs.HourTok = c.hour.toks
		}
		dayStart := dayStartOf(now)
		if c.day.start == dayStart {
			cs.DayReq = c.day.reqs
			cs.DayTok = c.day.toks
		}
		snap.Counters = append(snap.Counters, cs)
	}

	for lapiID, q := range m.queues {
		if q.count > 0 {
			snap.Queues = append(snap.Queues, QueueSnapshot{
				LapiID: lapiID,
				Count:  q.count,
			})
		}
	}

	return snap
}
