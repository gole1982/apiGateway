package db

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"gateway/internal/models"

	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}

	instance = &DB{conn: conn}

	_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS platform (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			base_url TEXT NOT NULL,
			token TEXT NOT NULL DEFAULT '',
			is_dynamic INTEGER NOT NULL DEFAULT 0,
			token_command TEXT,
			last_token_fetch DATETIME,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS rapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL UNIQUE,
			model TEXT NOT NULL DEFAULT '',
			platform_id INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			base_cost INTEGER NOT NULL DEFAULT 0,
			high_cost INTEGER NOT NULL DEFAULT 0,
			rpm_limit INTEGER NOT NULL DEFAULT 0,
			rph_limit INTEGER NOT NULL DEFAULT 0,
			rpd_limit INTEGER NOT NULL DEFAULT 0,
			tpm_limit INTEGER NOT NULL DEFAULT 0,
			tph_limit INTEGER NOT NULL DEFAULT 0,
			tpd_limit INTEGER NOT NULL DEFAULT 0,
			time_period_rules TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE
		);
		CREATE TABLE IF NOT EXISTS lapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL UNIQUE,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS lapi_rapi_order (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			lapi_id INTEGER NOT NULL,
			rapi_id INTEGER NOT NULL,
			order_index INTEGER NOT NULL,
			FOREIGN KEY (lapi_id) REFERENCES lapi(id) ON DELETE CASCADE,
			FOREIGN KEY (rapi_id) REFERENCES rapi(id) ON DELETE CASCADE,
			UNIQUE(lapi_id, rapi_id),
			UNIQUE(lapi_id, order_index)
		);
		CREATE TABLE IF NOT EXISTS token_cache (
			platform_id INTEGER PRIMARY KEY,
			token TEXT NOT NULL,
			expires_at DATETIME,
			fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE
		);
		CREATE TABLE IF NOT EXISTS rapi_metrics (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			rapi_id INTEGER NOT NULL,
			lapi_id INTEGER NOT NULL,
			total_requests INTEGER DEFAULT 0,
			success_requests INTEGER DEFAULT 0,
			fail_401 INTEGER DEFAULT 0,
			fail_429 INTEGER DEFAULT 0,
			fail_500 INTEGER DEFAULT 0,
			fail_other INTEGER DEFAULT 0,
			total_latency_ms INTEGER DEFAULT 0,
			token_count INTEGER DEFAULT 0,
			last_used DATETIME,
			FOREIGN KEY (rapi_id) REFERENCES rapi(id) ON DELETE CASCADE,
			FOREIGN KEY (lapi_id) REFERENCES lapi(id) ON DELETE CASCADE,
			UNIQUE(rapi_id, lapi_id)
		);
		CREATE TABLE IF NOT EXISTS request_trends (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			minute_bucket TEXT NOT NULL,
			lapi_id INTEGER NOT NULL DEFAULT 0,
			request_count INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(minute_bucket, lapi_id)
		);
		CREATE INDEX IF NOT EXISTS idx_rapi_metrics_rapi ON rapi_metrics(rapi_id);
		CREATE INDEX IF NOT EXISTS idx_rapi_metrics_lapi ON rapi_metrics(lapi_id);
		CREATE INDEX IF NOT EXISTS idx_rapi_platform ON rapi(platform_id);
		CREATE INDEX IF NOT EXISTS idx_trends_minute ON request_trends(minute_bucket);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	return instance
}

func TestPlatformCRUD(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{
		Name:    "openai",
		BaseURL: "https://api.openai.com/v1",
		Token:   "sk-test123",
	}

	// Create
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	if p.ID == 0 {
		t.Fatal("CreatePlatform did not set ID")
	}

	// Read
	got, err := db.GetPlatformByID(p.ID)
	if err != nil {
		t.Fatalf("GetPlatformByID: %v", err)
	}
	if got.Name != "openai" || got.BaseURL != "https://api.openai.com/v1" || got.Token != "sk-test123" {
		t.Errorf("GetPlatformByID returned wrong data: %+v", got)
	}

	// Update
	got.BaseURL = "https://api.openai.com/v2"
	if err := db.UpdatePlatform(got); err != nil {
		t.Fatalf("UpdatePlatform: %v", err)
	}
	got2, _ := db.GetPlatformByID(p.ID)
	if got2.BaseURL != "https://api.openai.com/v2" {
		t.Errorf("UpdatePlatform did not update base_url: %s", got2.BaseURL)
	}

	// List
	platforms, err := db.GetPlatforms()
	if err != nil {
		t.Fatalf("GetPlatforms: %v", err)
	}
	if len(platforms) != 1 {
		t.Errorf("GetPlatforms returned %d platforms, want 1", len(platforms))
	}

	// Delete
	if err := db.DeletePlatform(p.ID); err != nil {
		t.Fatalf("DeletePlatform: %v", err)
	}
	platforms, _ = db.GetPlatforms()
	if len(platforms) != 0 {
		t.Errorf("GetPlatforms after delete returned %d platforms, want 0", len(platforms))
	}
}

func TestRAPICRUD(t *testing.T) {
	db := setupTestDB(t)

	// Create platform first
	p := &models.Platform{Name: "openai", BaseURL: "https://api.openai.com/v1", Token: "sk-test"}
	db.CreatePlatform(p)

	r := &models.RAPI{
		Alias:      "gpt4-main",
		Model:      "gpt-4",
		PlatformID: p.ID,
	}

	// Create
	if err := db.CreateRAPI(r); err != nil {
		t.Fatalf("CreateRAPI: %v", err)
	}
	if r.ID == 0 {
		t.Fatal("CreateRAPI did not set ID")
	}

	// Read
	got, err := db.GetRAPIByID(r.ID)
	if err != nil {
		t.Fatalf("GetRAPIByID: %v", err)
	}
	if got.Alias != "gpt4-main" || got.Model != "gpt-4" || got.PlatformID != p.ID {
		t.Errorf("GetRAPIByID returned wrong data: %+v", got)
	}

	// Update
	got.Model = "gpt-4-turbo"
	if err := db.UpdateRAPI(&models.RAPI{ID: got.ID, Alias: got.Alias, Model: got.Model, PlatformID: got.PlatformID}); err != nil {
		t.Fatalf("UpdateRAPI: %v", err)
	}
	got2, _ := db.GetRAPIByID(r.ID)
	if got2.Model != "gpt-4-turbo" {
		t.Errorf("UpdateRAPI did not update model: %s", got2.Model)
	}

	// List
	rapis, err := db.GetRAPIs()
	if err != nil {
		t.Fatalf("GetRAPIs: %v", err)
	}
	if len(rapis) != 1 {
		t.Errorf("GetRAPIs returned %d, want 1", len(rapis))
	}

	// Delete
	if err := db.DeleteRAPI(r.ID); err != nil {
		t.Fatalf("DeleteRAPI: %v", err)
	}
	rapis, _ = db.GetRAPIs()
	if len(rapis) != 0 {
		t.Errorf("GetRAPIs after delete returned %d, want 0", len(rapis))
	}
}

func TestLAPICRUD(t *testing.T) {
	db := setupTestDB(t)

	l := &models.LAPI{Alias: "claude-sonnet"}

	// Create
	if err := db.CreateLAPI(l); err != nil {
		t.Fatalf("CreateLAPI: %v", err)
	}
	if l.ID == 0 {
		t.Fatal("CreateLAPI did not set ID")
	}

	// Read
	got, err := db.GetLAPIByAlias("claude-sonnet")
	if err != nil {
		t.Fatalf("GetLAPIByAlias: %v", err)
	}
	if got.Alias != "claude-sonnet" {
		t.Errorf("GetLAPIByAlias returned alias %q, want %q", got.Alias, "claude-sonnet")
	}

	// Update
	got.Alias = "claude-sonnet-v2"
	if err := db.UpdateLAPI(got); err != nil {
		t.Fatalf("UpdateLAPI: %v", err)
	}
	got2, _ := db.GetLAPIByAlias("claude-sonnet-v2")
	if got2 == nil || got2.Alias != "claude-sonnet-v2" {
		t.Errorf("UpdateLAPI did not update alias")
	}

	// List
	lapis, err := db.GetLAPIs()
	if err != nil {
		t.Fatalf("GetLAPIs: %v", err)
	}
	if len(lapis) != 1 {
		t.Errorf("GetLAPIs returned %d, want 1", len(lapis))
	}

	// Delete
	if err := db.DeleteLAPI(l.ID); err != nil {
		t.Fatalf("DeleteLAPI: %v", err)
	}
	lapis, _ = db.GetLAPIs()
	if len(lapis) != 0 {
		t.Errorf("GetLAPIs after delete returned %d, want 0", len(lapis))
	}
}

func TestLAPIRAPIOrder(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{Name: "openai", BaseURL: "https://api.openai.com/v1", Token: "sk-test"}
	db.CreatePlatform(p)

	r1 := &models.RAPI{Alias: "rapi-a", Model: "model-a", PlatformID: p.ID}
	r2 := &models.RAPI{Alias: "rapi-b", Model: "model-b", PlatformID: p.ID}
	db.CreateRAPI(r1)
	db.CreateRAPI(r2)

	l := &models.LAPI{Alias: "test-lapi"}
	db.CreateLAPI(l)

	// Set order
	if err := db.SetLAPIRAPIOrder(l.ID, []int64{r2.ID, r1.ID}); err != nil {
		t.Fatalf("SetLAPIRAPIOrder: %v", err)
	}

	// Get ordered RAPIs
	rapis, err := db.GetRAPIsForLAPI(l.ID)
	if err != nil {
		t.Fatalf("GetRAPIsForLAPI: %v", err)
	}
	if len(rapis) != 2 {
		t.Fatalf("GetRAPIsForLAPI returned %d, want 2", len(rapis))
	}
	if rapis[0].Alias != "rapi-b" || rapis[1].Alias != "rapi-a" {
		t.Errorf("Order wrong: got [%s, %s], want [rapi-b, rapi-a]", rapis[0].Alias, rapis[1].Alias)
	}

	// Update order
	if err := db.SetLAPIRAPIOrder(l.ID, []int64{r1.ID, r2.ID}); err != nil {
		t.Fatalf("SetLAPIRAPIOrder update: %v", err)
	}
	rapis, _ = db.GetRAPIsForLAPI(l.ID)
	if rapis[0].Alias != "rapi-a" || rapis[1].Alias != "rapi-b" {
		t.Errorf("Update order wrong: got [%s, %s], want [rapi-a, rapi-b]", rapis[0].Alias, rapis[1].Alias)
	}
}

func TestTokenCache(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{Name: "openai", BaseURL: "https://api.openai.com/v1", Token: ""}
	db.CreatePlatform(p)

	// Set cache
	if err := db.SetCachedToken(p.ID, "cached-token-123", 5*time.Minute); err != nil {
		t.Fatalf("SetCachedToken: %v", err)
	}

	// Get cache
	token, err := db.GetCachedToken(p.ID)
	if err != nil {
		t.Fatalf("GetCachedToken: %v", err)
	}
	if token != "cached-token-123" {
		t.Errorf("GetCachedToken returned %q, want %q", token, "cached-token-123")
	}

	// Expired cache
	db.SetCachedToken(p.ID, "expired-token", -1*time.Minute)
	token, err = db.GetCachedToken(p.ID)
	if err == nil && token != "" {
		t.Errorf("GetCachedToken should return empty for expired token, got %q", token)
	}
}

func TestMetrics(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{Name: "openai", BaseURL: "https://api.openai.com/v1", Token: "sk-test"}
	db.CreatePlatform(p)

	r := &models.RAPI{Alias: "gpt4", Model: "gpt-4", PlatformID: p.ID}
	db.CreateRAPI(r)

	l := &models.LAPI{Alias: "my-model"}
	db.CreateLAPI(l)

	// Record requests
	db.RecordRequest(r.ID, l.ID, 200, 150, 100)
	db.RecordRequest(r.ID, l.ID, 200, 200, 120)
	db.RecordRequest(r.ID, l.ID, 429, 50, 0)

	// Check stats
	stats, err := db.GetRAPIStats()
	if err != nil {
		t.Fatalf("GetRAPIStats: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("GetRAPIStats returned %d, want 1", len(stats))
	}
	s := stats[0]
	if s.TotalRequests != 3 {
		t.Errorf("TotalRequests = %d, want 3", s.TotalRequests)
	}
	if s.SuccessRequests != 2 {
		t.Errorf("SuccessRequests = %d, want 2", s.SuccessRequests)
	}
	if s.Fail429 != 1 {
		t.Errorf("Fail429 = %d, want 1", s.Fail429)
	}
	if s.TokenCount != 220 {
		t.Errorf("TokenCount = %d, want 220", s.TokenCount)
	}
}

func TestRequestTrends(t *testing.T) {
	db := setupTestDB(t)

	l := &models.LAPI{Alias: "trend-model"}
	db.CreateLAPI(l)

	// Record trends
	db.RecordTrend(l.ID)
	db.RecordTrend(l.ID)
	db.RecordTrend(0) // different lapi

	// Query trends
	trends, err := db.GetRequestTrends(5)
	if err != nil {
		t.Fatalf("GetRequestTrends: %v", err)
	}
	if len(trends) == 0 {
		t.Fatal("GetRequestTrends returned empty")
	}

	// Check that we have at least one trend point
	found := false
	for _, tp := range trends {
		if tp.RequestCount > 0 {
			found = true
			break
		}
	}
	if !found {
		t.Error("No trend points with request_count > 0")
	}
}
