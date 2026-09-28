package db

import (
	"testing"
	"time"
)

// newAnalyticsTestDB builds a test DB holding only request_logs -- the single
// table GetClientUsage / GetTrafficTotals read. Creating the rest of the schema
// would only bury real failures in unrelated noise.
func newAnalyticsTestDB(t *testing.T) *DB {
	t.Helper()
	db := setupTestDB(t)
	_, err := db.conn.Exec(`
		CREATE TABLE request_logs (
			id TEXT PRIMARY KEY,
			timestamp DATETIME,
			status TEXT DEFAULT 'completed',
			client_ip TEXT,
			lapi_alias TEXT,
			selected_rapi TEXT,
			response_status INTEGER,
			latency_ms INTEGER DEFAULT 0,
			tokens_used INTEGER DEFAULT 0,
			input_tokens INTEGER DEFAULT 0,
			output_tokens INTEGER DEFAULT 0,
			cached_tokens INTEGER DEFAULT 0,
			retry_count INTEGER DEFAULT 0,
			fallback_used BOOLEAN DEFAULT FALSE
		)`)
	if err != nil {
		t.Fatalf("create request_logs: %v", err)
	}
	return db
}

// TestGetClientUsageStripsPort is the whole reason GetClientUsage exists:
// client_ip holds r.RemoteAddr ("IP:port") and the port changes on every
// connection. Without stripping it one client splits into N groups and
// per-client usage is useless.
func TestGetClientUsageStripsPort(t *testing.T) {
	db := newAnalyticsTestDB(t)

	now := time.Now().Format("2006-01-02 15:04:05")
	insert := `INSERT INTO request_logs
		(id, timestamp, client_ip, lapi_alias, response_status, latency_ms,
		 input_tokens, output_tokens, cached_tokens)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	type row struct {
		id, ip, lapi    string
		status          int
		lat             int
		in, out, cached int
	}
	rows := []row{
		{"r1", "10.0.0.5:52341", "a", 200, 100, 10, 5, 2},
		{"r2", "10.0.0.5:61002", "a", 200, 300, 20, 8, 1},
		{"r3", "10.0.0.5:44881", "b", 500, 200, 5, 0, 0},
		{"r4", "10.0.0.9:33333", "a", 200, 400, 1, 1, 0},
	}
	for _, r := range rows {
		if _, err := db.conn.Exec(insert, r.id, now, r.ip, r.lapi, r.status,
			r.lat, r.in, r.out, r.cached); err != nil {
			t.Fatalf("insert %s: %v", r.id, err)
		}
	}

	got, err := db.GetClientUsage(time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("GetClientUsage: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d groups, want 2 (ports must be stripped): %+v", len(got), got)
	}

	byIP := make(map[string]int, len(got))
	for _, g := range got {
		byIP[g.ClientIP] = g.RequestCount
	}
	if byIP["10.0.0.5"] != 3 {
		t.Errorf("10.0.0.5 request count = %d, want 3", byIP["10.0.0.5"])
	}
	if byIP["10.0.0.9"] != 1 {
		t.Errorf("10.0.0.9 request count = %d, want 1", byIP["10.0.0.9"])
	}

	top := got[0]
	if top.ClientIP != "10.0.0.5" {
		t.Errorf("top group ip = %q, want 10.0.0.5", top.ClientIP)
	}
	if top.SuccessCount != 2 || top.ErrorCount != 1 {
		t.Errorf("success/error = %d/%d, want 2/1", top.SuccessCount, top.ErrorCount)
	}
	// in 10+20+5=35, out 5+8+0=13 -> total 48 (cached excluded, same as the panel)
	if top.InputTokens != 35 || top.OutputTokens != 13 || top.TotalTokens != 48 {
		t.Errorf("tokens in/out/total = %d/%d/%d, want 35/13/48",
			top.InputTokens, top.OutputTokens, top.TotalTokens)
	}
	if top.CachedTokens != 3 {
		t.Errorf("cached = %d, want 3", top.CachedTokens)
	}
	// (100+300+200)/3 = 200
	if top.AvgLatencyMs != 200 {
		t.Errorf("avg latency = %d, want 200", top.AvgLatencyMs)
	}
	if top.DistinctLapis != 2 {
		t.Errorf("distinct lapis = %d, want 2", top.DistinctLapis)
	}
	if top.LastSeen == "" {
		t.Error("last_seen is empty, want a timestamp")
	}
}

// TestGetClientUsageIPv6: Go's net package guarantees a bracketed IPv6
// RemoteAddr ("[::1]:52341"). Stripping at the FIRST colon would reduce the
// address to "[" -- a wrong cut that needs explicit coverage.
func TestGetClientUsageIPv6(t *testing.T) {
	db := newAnalyticsTestDB(t)

	now := time.Now().Format("2006-01-02 15:04:05")
	insert := `INSERT INTO request_logs
		(id, timestamp, client_ip, lapi_alias, response_status, latency_ms,
		 input_tokens, output_tokens)
		VALUES (?, ?, ?, ?, 200, 10, 1, 1)`
	for _, r := range []struct{ id, addr string }{
		{"v1", "[::1]:52341"},
		{"v2", "[2001:db8::1]:9999"},
	} {
		if _, err := db.conn.Exec(insert, r.id, now, r.addr, "a"); err != nil {
			t.Fatalf("insert %s: %v", r.id, err)
		}
	}

	got, err := db.GetClientUsage(time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("GetClientUsage: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d groups, want 2 distinct IPv6 hosts: %+v", len(got), got)
	}
	for _, g := range got {
		if g.ClientIP != "[::1]" && g.ClientIP != "[2001:db8::1]" {
			t.Errorf("client_ip = %q, want a bracket-preserved host without port", g.ClientIP)
		}
	}
}

// TestGetClientUsageWindowAndEmptyIP: rows outside the window must be dropped,
// and empty/NULL client_ip must not collapse into an "unknown" group.
func TestGetClientUsageWindowAndEmptyIP(t *testing.T) {
	db := newAnalyticsTestDB(t)

	old := time.Now().AddDate(0, 0, -10).Format("2006-01-02 15:04:05")
	recent := time.Now().Format("2006-01-02 15:04:05")
	insert := `INSERT INTO request_logs (id, timestamp, client_ip, lapi_alias, response_status, latency_ms)
		VALUES (?, ?, ?, 'a', 200, 10)`
	for _, r := range []struct {
		id, ts string
		ip     any
	}{
		{"old", old, "1.1.1.1:1111"},
		{"empty", recent, ""},
		{"null", recent, nil},
		{"ok", recent, "8.8.8.8:1"},
	} {
		if _, err := db.conn.Exec(insert, r.id, r.ts, r.ip); err != nil {
			t.Fatalf("insert %s: %v", r.id, err)
		}
	}

	got, err := db.GetClientUsage(time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("GetClientUsage: %v", err)
	}
	if len(got) != 1 || got[0].ClientIP != "8.8.8.8" || got[0].RequestCount != 1 {
		t.Errorf("got %+v, want only 8.8.8.8 with 1 request inside the window", got)
	}
}

// TestGetClientUsageLimit: limit applies, and 0 means "unset" -> default.
func TestGetClientUsageLimit(t *testing.T) {
	db := newAnalyticsTestDB(t)

	now := time.Now().Format("2006-01-02 15:04:05")
	insert := `INSERT INTO request_logs (id, timestamp, client_ip, lapi_alias, response_status, latency_ms)
		VALUES (?, ?, ?, 'a', 200, 10)`
	ips := []string{"1.1.1.1:1", "1.1.1.2:1", "1.1.1.3:1", "1.1.1.4:1", "1.1.1.5:1"}
	for i, ip := range ips {
		if _, err := db.conn.Exec(insert, string(rune('a'+i)), now, ip); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	got, err := db.GetClientUsage(time.Now().Add(-time.Hour), 2)
	if err != nil {
		t.Fatalf("GetClientUsage: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("limit=2 returned %d rows, want 2", len(got))
	}
	got, err = db.GetClientUsage(time.Now().Add(-time.Hour), 0)
	if err != nil {
		t.Fatalf("GetClientUsage(limit=0): %v", err)
	}
	if len(got) != 5 {
		t.Errorf("limit=0 returned %d rows, want all 5", len(got))
	}
}

// TestGetTrafficTotals locks the counter semantics, in particular that 3xx
// counts as neither success nor failure.
func TestGetTrafficTotals(t *testing.T) {
	db := newAnalyticsTestDB(t)

	now := time.Now().Format("2006-01-02 15:04:05")
	insert := `INSERT INTO request_logs
		(id, timestamp, response_status, retry_count, fallback_used,
		 input_tokens, output_tokens, cached_tokens)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	type row struct {
		id              string
		status          int
		retry, fallback int
		in, out, cached int
	}
	rows := []row{
		{"a", 200, 0, 0, 10, 5, 1},
		{"b", 201, 1, 1, 20, 10, 2},
		{"c", 500, 2, 0, 1, 0, 0},
		{"d", 429, 0, 1, 0, 0, 0},
		{"e", 302, 0, 0, 0, 0, 0}, // 3xx: neither success nor failure
	}
	for _, r := range rows {
		if _, err := db.conn.Exec(insert, r.id, now, r.status, r.retry, r.fallback,
			r.in, r.out, r.cached); err != nil {
			t.Fatalf("insert %s: %v", r.id, err)
		}
	}

	got, err := db.GetTrafficTotals()
	if err != nil {
		t.Fatalf("GetTrafficTotals: %v", err)
	}
	if got.TotalRequests != 5 {
		t.Errorf("total = %d, want 5", got.TotalRequests)
	}
	if got.SuccessRequests != 2 {
		t.Errorf("success = %d, want 2 (3xx must not count)", got.SuccessRequests)
	}
	if got.FailedRequests != 2 {
		t.Errorf("failed = %d, want 2 (3xx must not count)", got.FailedRequests)
	}
	if got.InputTokens != 31 || got.OutputTokens != 15 || got.CachedTokens != 3 {
		t.Errorf("tokens = %d/%d/%d, want 31/15/3",
			got.InputTokens, got.OutputTokens, got.CachedTokens)
	}
	if got.RetryTotal != 3 {
		t.Errorf("retries = %d, want 3", got.RetryTotal)
	}
	if got.FallbackTotal != 2 {
		t.Errorf("fallbacks = %d, want 2", got.FallbackTotal)
	}
}

// TestGetTrafficTotalsEmpty: an empty table must yield zeros, not an error --
// otherwise /metrics loses a whole counter group right after startup.
func TestGetTrafficTotalsEmpty(t *testing.T) {
	db := newAnalyticsTestDB(t)

	got, err := db.GetTrafficTotals()
	if err != nil {
		t.Fatalf("GetTrafficTotals on empty table: %v", err)
	}
	if got != (TrafficTotals{}) {
		t.Errorf("got %+v, want all zeros", got)
	}
}
