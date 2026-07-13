package db

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"gateway/internal/crypto"
	"gateway/internal/models"

	_ "modernc.org/sqlite"
)

func initTestCrypto(t *testing.T) {
	t.Helper()
	// Point the key file to a temp directory so tests don't touch ~/.apiGateway.key.
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir) // Windows
	if err := crypto.Init(); err != nil {
		t.Fatalf("crypto.Init: %v", err)
	}
}

func setupTestDB(t *testing.T) *DB {
	t.Helper()
	initTestCrypto(t)
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
			url_auto_complete INTEGER NOT NULL DEFAULT 1,
			token TEXT NOT NULL DEFAULT '',
			last_token_fetch DATETIME,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			notes TEXT NOT NULL DEFAULT '',
			custom_headers TEXT NOT NULL DEFAULT '',
			push_secret TEXT NOT NULL DEFAULT '',
			webpage_domain TEXT NOT NULL DEFAULT '',
			is_dynamic INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS rapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			notes TEXT NOT NULL DEFAULT '',
			platform_id INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			unavailable_reason TEXT NOT NULL DEFAULT '',
			base_cost INTEGER NOT NULL DEFAULT 0,
			high_cost INTEGER NOT NULL DEFAULT 0,
			rpm_limit INTEGER NOT NULL DEFAULT 0,
			rph_limit INTEGER NOT NULL DEFAULT 0,
			rpd_limit INTEGER NOT NULL DEFAULT 0,
			tpm_limit INTEGER NOT NULL DEFAULT 0,
			tph_limit INTEGER NOT NULL DEFAULT 0,
			tpd_limit INTEGER NOT NULL DEFAULT 0,
			time_period_rules TEXT,
			supported_formats TEXT NOT NULL DEFAULT '["openai"]',
			custom_headers TEXT NOT NULL DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
			UNIQUE(platform_id, alias)
		);
		CREATE TABLE IF NOT EXISTS lapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL UNIQUE,
			notes TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1,
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
		CREATE TABLE IF NOT EXISTS platform_keys (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			platform_id INTEGER NOT NULL,
			key_index INTEGER NOT NULL DEFAULT 0,
			token TEXT NOT NULL DEFAULT '',
			label TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1,
			session_headers TEXT NOT NULL DEFAULT '',
			reusable_status INTEGER NOT NULL DEFAULT -1,
			reusable_reasons TEXT NOT NULL DEFAULT '[]',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
			UNIQUE(platform_id, key_index)
		);
		CREATE TABLE IF NOT EXISTS token_cache (
			platform_key_id INTEGER PRIMARY KEY,
			token TEXT NOT NULL,
			expires_at DATETIME,
			fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_key_id) REFERENCES platform_keys(id) ON DELETE CASCADE
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

	// Add a platform key first (new schema requires a key row).
	k := &models.PlatformKey{PlatformID: p.ID, Token: "", Label: "default", Enabled: true}
	if err := db.AddPlatformKey(k); err != nil {
		t.Fatalf("AddPlatformKey: %v", err)
	}

	// Set cache via key
	if err := db.SetCachedTokenForKey(k.ID, "cached-token-123", 5*time.Minute); err != nil {
		t.Fatalf("SetCachedTokenForKey: %v", err)
	}

	// Get cache via key
	token, err := db.GetCachedTokenForKey(k.ID)
	if err != nil {
		t.Fatalf("GetCachedTokenForKey: %v", err)
	}
	if token != "cached-token-123" {
		t.Errorf("GetCachedTokenForKey returned %q, want %q", token, "cached-token-123")
	}

	// Backward-compat wrappers
	if err := db.SetCachedToken(p.ID, "compat-token", 5*time.Minute); err != nil {
		t.Fatalf("SetCachedToken (compat): %v", err)
	}
	token, err = db.GetCachedToken(p.ID)
	if err != nil {
		t.Fatalf("GetCachedToken (compat): %v", err)
	}
	if token != "compat-token" {
		t.Errorf("GetCachedToken (compat) returned %q, want compat-token", token)
	}

	// Expired cache
	db.SetCachedTokenForKey(k.ID, "expired-token", -1*time.Minute)
	token, err = db.GetCachedTokenForKey(k.ID)
	if err == nil && token != "" {
		t.Errorf("GetCachedTokenForKey should return empty for expired token, got %q", token)
	}
}

func TestPlatformKeyCRUD(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{Name: "test-platform", BaseURL: "https://api.test.com/v1", Token: "sk-orig"}
	db.CreatePlatform(p)

	// Add keys
	k1 := &models.PlatformKey{PlatformID: p.ID, Token: "key-a", Label: "primary", Enabled: true}
	k2 := &models.PlatformKey{PlatformID: p.ID, Token: "key-b", Label: "secondary", Enabled: true}
	if err := db.AddPlatformKey(k1); err != nil {
		t.Fatalf("AddPlatformKey k1: %v", err)
	}
	if err := db.AddPlatformKey(k2); err != nil {
		t.Fatalf("AddPlatformKey k2: %v", err)
	}

	keys, err := db.GetPlatformKeys(p.ID)
	if err != nil {
		t.Fatalf("GetPlatformKeys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("GetPlatformKeys returned %d, want 2", len(keys))
	}
	if keys[0].KeyIndex != 0 || keys[1].KeyIndex != 1 {
		t.Errorf("key_index ordering wrong: %v %v", keys[0].KeyIndex, keys[1].KeyIndex)
	}

	// Update key
	k1.Token = "key-a-updated"
	if err := db.UpdatePlatformKey(k1); err != nil {
		t.Fatalf("UpdatePlatformKey: %v", err)
	}
	keys, _ = db.GetPlatformKeys(p.ID)
	if keys[0].Token != "key-a-updated" {
		t.Errorf("UpdatePlatformKey: token = %q, want key-a-updated", keys[0].Token)
	}

	// Delete first key; second should become index 0
	if err := db.DeletePlatformKey(k1.ID); err != nil {
		t.Fatalf("DeletePlatformKey: %v", err)
	}
	keys, _ = db.GetPlatformKeys(p.ID)
	if len(keys) != 1 {
		t.Fatalf("After delete, GetPlatformKeys returned %d, want 1", len(keys))
	}
	if keys[0].KeyIndex != 0 {
		t.Errorf("After delete, remaining key_index = %d, want 0", keys[0].KeyIndex)
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

// TestRAPISameAliasOnDifferentPlatforms verifies that two platforms can each have a RAPI
// with the same alias (e.g. "glm-5.2" on JD and on ZhipuAI) now that the uniqueness
// constraint is UNIQUE(platform_id, alias) rather than UNIQUE(alias).
func TestRAPISameAliasOnDifferentPlatforms(t *testing.T) {
	db := setupTestDB(t)

	p1 := &models.Platform{Name: "jd", BaseURL: "https://api.jd.com/v1", Token: "key-jd"}
	p2 := &models.Platform{Name: "zhipuai", BaseURL: "https://open.bigmodel.cn/v1", Token: "key-zp"}
	if err := db.CreatePlatform(p1); err != nil {
		t.Fatalf("CreatePlatform p1: %v", err)
	}
	if err := db.CreatePlatform(p2); err != nil {
		t.Fatalf("CreatePlatform p2: %v", err)
	}

	r1 := &models.RAPI{Alias: "glm-5.2", Model: "glm-5.2", PlatformID: p1.ID, Enabled: true, Available: true}
	r2 := &models.RAPI{Alias: "glm-5.2", Model: "glm-5.2", PlatformID: p2.ID, Enabled: true, Available: true}

	if err := db.CreateRAPI(r1); err != nil {
		t.Fatalf("CreateRAPI on platform 1: %v", err)
	}
	if err := db.CreateRAPI(r2); err != nil {
		t.Fatalf("CreateRAPI same alias on platform 2 (should succeed): %v", err)
	}
	if r1.ID == r2.ID {
		t.Errorf("both RAPIs got same ID %d", r1.ID)
	}

	// Same alias on same platform must still be rejected.
	r3 := &models.RAPI{Alias: "glm-5.2", Model: "glm-5.2-dup", PlatformID: p1.ID}
	if err := db.CreateRAPI(r3); err == nil {
		t.Error("CreateRAPI with duplicate (platform_id, alias) should have failed but did not")
	}
}

// TestWebpagePlatformIsDynamicAndIsWebpage verifies that webpage platforms persist
// is_dynamic and that RAPIs under them are marked IsWebpage for gateway routing.
func TestWebpagePlatformIsDynamicAndIsWebpage(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{
		Name:            "arena-web",
		BaseURL:         "https://arena.ai/agent",
		WebpageDomain:   "arena.ai/agent",
		URLAutoComplete: false,
		Enabled:         true,
		Available:       true,
	}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	got, err := db.GetPlatformByID(p.ID)
	if err != nil {
		t.Fatalf("GetPlatformByID: %v", err)
	}
	if !got.IsDynamic {
		t.Fatal("expected IsDynamic=true when WebpageDomain is set")
	}

	r := &models.RAPI{
		Alias:            "webpage",
		Model:            "default",
		PlatformID:       p.ID,
		Enabled:          true,
		Available:        true,
		SupportedFormats: `["openai"]`,
	}
	if err := db.CreateRAPI(r); err != nil {
		t.Fatalf("CreateRAPI: %v", err)
	}
	rapis, err := db.GetRAPIsByPlatform(p.ID)
	if err != nil {
		t.Fatalf("GetRAPIsByPlatform: %v", err)
	}
	if len(rapis) != 1 || !rapis[0].IsWebpage {
		t.Fatalf("expected IsWebpage=true for webpage platform RAPI, got %+v", rapis)
	}
}

// TestTokenEncryptionRoundtrip verifies that tokens written to the DB are stored encrypted
// and that reading them back returns the original plaintext.
func TestTokenEncryptionRoundtrip(t *testing.T) {
	db := setupTestDB(t)

	const secret = "sk-super-secret-api-key"
	p := &models.Platform{Name: "enc-test", BaseURL: "https://api.example.com/v1", Token: secret}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}

	// Verify that the raw value stored in SQLite is NOT the plaintext.
	var rawToken string
	if err := db.conn.QueryRow("SELECT token FROM platform WHERE id = ?", p.ID).Scan(&rawToken); err != nil {
		t.Fatalf("raw query: %v", err)
	}
	if rawToken == secret {
		t.Errorf("token stored as plaintext in DB; expected ciphertext")
	}
	if len(rawToken) < 4 || rawToken[:4] != "enc:" {
		t.Errorf("stored token does not have 'enc:' prefix: %q", rawToken)
	}

	// Reading back via the DB API must return the original plaintext.
	got, err := db.GetPlatformByID(p.ID)
	if err != nil {
		t.Fatalf("GetPlatformByID: %v", err)
	}
	if got.Token != secret {
		t.Errorf("GetPlatformByID returned token %q, want %q", got.Token, secret)
	}
}
