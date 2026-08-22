package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
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
			token TEXT NOT NULL DEFAULT '',
			last_token_fetch DATETIME,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			notes TEXT NOT NULL DEFAULT '',
			custom_headers TEXT NOT NULL DEFAULT '',
			supported_formats TEXT NOT NULL DEFAULT '["openai"]',
			format_endpoints TEXT NOT NULL DEFAULT '',
			sort_order INTEGER NOT NULL DEFAULT 0,
			billing_address TEXT NOT NULL DEFAULT '',
			login_account TEXT NOT NULL DEFAULT '',
			login_password TEXT NOT NULL DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS rapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			notes TEXT NOT NULL DEFAULT '',
			vendor TEXT NOT NULL DEFAULT '',
			series TEXT NOT NULL DEFAULT '',
			model_name TEXT NOT NULL DEFAULT '',
			version TEXT NOT NULL DEFAULT '',
			suffix TEXT NOT NULL DEFAULT '',
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
			key_ids TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT 'manual',
			sort_order INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
			UNIQUE(platform_id, alias)
		);
		CREATE TABLE IF NOT EXISTS lapi (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL UNIQUE,
			notes TEXT NOT NULL DEFAULT '',
			vendor TEXT NOT NULL DEFAULT '',
			series TEXT NOT NULL DEFAULT '',
			model_name TEXT NOT NULL DEFAULT '',
			version TEXT NOT NULL DEFAULT '',
			suffix TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1,
			sort_order INTEGER NOT NULL DEFAULT 0,
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
			failure_type INTEGER NOT NULL DEFAULT 0,
			failure_reason TEXT NOT NULL DEFAULT '',
			failed_at DATETIME,
			expires_at DATETIME,
			is_free INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
			UNIQUE(platform_id, key_index)
		);
		CREATE TABLE IF NOT EXISTS key_model_blocks (
			key_id INTEGER NOT NULL,
			rapi_id INTEGER NOT NULL,
			reason TEXT NOT NULL DEFAULT '',
			expires_at DATETIME,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (key_id, rapi_id)
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
		Name:           "openai",
		BaseURL:        "https://api.openai.com/v1",
		Token:          "sk-test123",
		BillingAddress: "https://console.openai.com/billing",
		LoginAccount:   "ops@example.com",
		LoginPassword:  "secret-pw-1",
	}

	// Create
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	if p.ID == 0 {
		t.Fatal("CreatePlatform did not set ID")
	}

	// Read: billing/login_account returned, login_password NEVER returned (write-only)
	got, err := db.GetPlatformByID(p.ID)
	if err != nil {
		t.Fatalf("GetPlatformByID: %v", err)
	}
	if got.Name != "openai" || got.BaseURL != "https://api.openai.com/v1" || got.Token != "sk-test123" {
		t.Errorf("GetPlatformByID returned wrong data: %+v", got)
	}
	if got.BillingAddress != "https://console.openai.com/billing" || got.LoginAccount != "ops@example.com" {
		t.Errorf("GetPlatformByID returned wrong account data: billing=%q account=%q", got.BillingAddress, got.LoginAccount)
	}
	if got.LoginPassword != "" {
		t.Errorf("GetPlatformByID leaked login_password: %q", got.LoginPassword)
	}

	// Verify the stored password is encrypted (not plaintext) in the DB.
	var storedPw string
	if err := db.conn.QueryRow(`SELECT login_password FROM platform WHERE id = ?`, p.ID).Scan(&storedPw); err != nil {
		t.Fatalf("select stored login_password: %v", err)
	}
	if storedPw == "" || storedPw == "secret-pw-1" || !strings.HasPrefix(storedPw, "enc:") {
		t.Errorf("stored login_password is not encrypted: %q", storedPw)
	}
	// And it decrypts back to the original value.
	if dec, err := crypto.Decrypt(storedPw); err != nil || dec != "secret-pw-1" {
		t.Errorf("stored login_password does not decrypt back: dec=%q err=%v", dec, err)
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

	// Update with empty login_password must KEEP the existing password.
	got2.LoginPassword = ""
	got2.LoginAccount = "ops2@example.com"
	if err := db.UpdatePlatform(got2); err != nil {
		t.Fatalf("UpdatePlatform (empty pw): %v", err)
	}
	got3, _ := db.GetPlatformByID(p.ID)
	if got3.LoginAccount != "ops2@example.com" {
		t.Errorf("UpdatePlatform did not update login_account: %q", got3.LoginAccount)
	}
	var pwAfter string
	if err := db.conn.QueryRow(`SELECT login_password FROM platform WHERE id = ?`, p.ID).Scan(&pwAfter); err != nil {
		t.Fatalf("select login_password after empty-pw update: %v", err)
	}
	if dec, err := crypto.Decrypt(pwAfter); err != nil || dec != "secret-pw-1" {
		t.Errorf("empty login_password overwrote existing password: dec=%q err=%v", dec, err)
	}

	// Update with a NEW login_password must replace it.
	got3.LoginPassword = "secret-pw-2"
	if err := db.UpdatePlatform(got3); err != nil {
		t.Fatalf("UpdatePlatform (new pw): %v", err)
	}
	var pwNew string
	if err := db.conn.QueryRow(`SELECT login_password FROM platform WHERE id = ?`, p.ID).Scan(&pwNew); err != nil {
		t.Fatalf("select login_password after new-pw update: %v", err)
	}
	if dec, err := crypto.Decrypt(pwNew); err != nil || dec != "secret-pw-2" {
		t.Errorf("new login_password not applied: dec=%q err=%v", dec, err)
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

// TestPlatformKeyExpiryAndFreePersistence verifies that ExpiresAt/IsFree survive a
// round-trip through SetPlatformKeys + GetPlatformKeys, and that DisableExpiredKeys
// flips the enabled flag for keys whose ExpiresAt has passed.
func TestPlatformKeyExpiryAndFreePersistence(t *testing.T) {
	db := setupTestDB(t)

	p := &models.Platform{Name: "test-expiry", BaseURL: "https://api.test.com/v1", Token: "sk-orig"}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}

	past := time.Now().Add(-1 * time.Hour).UTC()
	future := time.Now().Add(24 * time.Hour).UTC()

	// Mixed set: one free-unexpired, one paid-unexpired, one free-expired.
	keys := []models.PlatformKey{
		{Token: "free-future", Label: "free-future", Enabled: true, IsFree: true, ExpiresAt: &future},
		{Token: "paid-future", Label: "paid-future", Enabled: true, IsFree: false, ExpiresAt: &future},
		{Token: "free-expired", Label: "free-expired", Enabled: true, IsFree: true, ExpiresAt: &past},
	}
	if err := db.SetPlatformKeys(p.ID, keys); err != nil {
		t.Fatalf("SetPlatformKeys: %v", err)
	}

	got, err := db.GetPlatformKeys(p.ID)
	if err != nil {
		t.Fatalf("GetPlatformKeys: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetPlatformKeys returned %d, want 3", len(got))
	}
	// Find by label for stable assertions regardless of internal ordering.
	byLabel := map[string]models.PlatformKey{}
	for _, k := range got {
		byLabel[k.Label] = k
	}
	if !byLabel["free-future"].IsFree {
		t.Errorf("free-future: IsFree = false, want true")
	}
	if byLabel["paid-future"].IsFree {
		t.Errorf("paid-future: IsFree = true, want false")
	}
	if byLabel["free-future"].ExpiresAt == nil {
		t.Errorf("free-future: ExpiresAt is nil, want set")
	}

	// Auto-disable expired keys.
	n, err := db.DisableExpiredKeys()
	if err != nil {
		t.Fatalf("DisableExpiredKeys: %v", err)
	}
	if n != 1 {
		t.Fatalf("DisableExpiredKeys affected %d, want 1", n)
	}

	got, _ = db.GetPlatformKeys(p.ID)
	byLabel = map[string]models.PlatformKey{}
	for _, k := range got {
		byLabel[k.Label] = k
	}
	if byLabel["free-expired"].Enabled {
		t.Errorf("free-expired: Enabled = true after DisableExpiredKeys, want false")
	}
	if !byLabel["free-future"].Enabled {
		t.Errorf("free-future: Enabled = false, want true (not expired)")
	}
	if !byLabel["paid-future"].Enabled {
		t.Errorf("paid-future: Enabled = false, want true (not expired)")
	}

	// Idempotent: a second run should report zero affected rows.
	n2, _ := db.DisableExpiredKeys()
	if n2 != 0 {
		t.Errorf("DisableExpiredKeys second run affected %d, want 0", n2)
	}
}

func TestMarkKeyPermanentFailure(t *testing.T) {
	db := setupTestDB(t)
	p := &models.Platform{Name: "test", BaseURL: "https://api.test.com/v1", Token: "sk-orig"}
	db.CreatePlatform(p)
	k := &models.PlatformKey{PlatformID: p.ID, Token: "key-a", Label: "primary", Enabled: true}
	if err := db.AddPlatformKey(k); err != nil {
		t.Fatalf("AddPlatformKey: %v", err)
	}

	if err := db.MarkKeyPermanentFailure(k.ID, "[认证失败] upstream 401"); err != nil {
		t.Fatalf("MarkKeyPermanentFailure: %v", err)
	}

	keys, _ := db.GetPlatformKeys(p.ID)
	if len(keys) != 1 {
		t.Fatalf("GetPlatformKeys returned %d, want 1", len(keys))
	}
	if keys[0].FailureType != 2 {
		t.Errorf("FailureType = %d, want 2 (permanent)", keys[0].FailureType)
	}
	if keys[0].FailureReason != "[认证失败] upstream 401" {
		t.Errorf("FailureReason = %q, want '[认证失败] upstream 401'", keys[0].FailureReason)
	}
	if keys[0].FailedAt == nil {
		t.Errorf("FailedAt should be non-nil after permanent failure")
	}
	// enabled should NOT be changed (decoupled from failure tracking)
	if !keys[0].Enabled {
		t.Errorf("Enabled = false, want true (failure tracking decoupled from enabled)")
	}
}

func TestMarkKeyTemporaryFailure(t *testing.T) {
	db := setupTestDB(t)
	p := &models.Platform{Name: "test", BaseURL: "https://api.test.com/v1", Token: "sk-orig"}
	db.CreatePlatform(p)
	k := &models.PlatformKey{PlatformID: p.ID, Token: "key-a", Enabled: true}
	db.AddPlatformKey(k)

	if err := db.MarkKeyTemporaryFailure(k.ID, "upstream 429: rate limited"); err != nil {
		t.Fatalf("MarkKeyTemporaryFailure: %v", err)
	}

	keys, _ := db.GetPlatformKeys(p.ID)
	if keys[0].FailureType != 1 {
		t.Errorf("FailureType = %d, want 1 (temporary)", keys[0].FailureType)
	}
	if keys[0].FailureReason != "upstream 429: rate limited" {
		t.Errorf("FailureReason = %q, want 'upstream 429: rate limited'", keys[0].FailureReason)
	}
	if keys[0].FailedAt == nil {
		t.Errorf("FailedAt should be non-nil after temporary failure")
	}
}

func TestClearKeyFailure(t *testing.T) {
	db := setupTestDB(t)
	p := &models.Platform{Name: "test", BaseURL: "https://api.test.com/v1", Token: "sk-orig"}
	db.CreatePlatform(p)
	k := &models.PlatformKey{PlatformID: p.ID, Token: "key-a", Enabled: true}
	db.AddPlatformKey(k)

	db.MarkKeyPermanentFailure(k.ID, "test reason")
	if err := db.ClearKeyFailure(k.ID); err != nil {
		t.Fatalf("ClearKeyFailure: %v", err)
	}

	keys, _ := db.GetPlatformKeys(p.ID)
	if keys[0].FailureType != 0 {
		t.Errorf("FailureType = %d, want 0 (none)", keys[0].FailureType)
	}
	if keys[0].FailureReason != "" {
		t.Errorf("FailureReason = %q, want ''", keys[0].FailureReason)
	}
	if keys[0].FailedAt != nil {
		t.Errorf("FailedAt should be nil after clear")
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

// TestSeedDefaultPlatformsIdempotent verifies that seedDefaultPlatforms creates the
// Google Gemini preset exactly once with the correct initial state, and that calling
// it again (e.g. on gateway restart) does not create duplicates.
func TestSeedDefaultPlatformsIdempotent(t *testing.T) {
	db := setupTestDB(t)

	// 首次种入
	if err := db.seedDefaultPlatforms(); err != nil {
		t.Fatalf("seedDefaultPlatforms first call: %v", err)
	}
	// 重启模拟：再次种入，应被 UNIQUE(name) + INSERT OR IGNORE 忽略
	if err := db.seedDefaultPlatforms(); err != nil {
		t.Fatalf("seedDefaultPlatforms second call: %v", err)
	}

	platforms, err := db.GetPlatforms()
	if err != nil {
		t.Fatalf("GetPlatforms: %v", err)
	}

	var gemini *models.Platform
	count := 0
	for i := range platforms {
		if platforms[i].Name == "Google Gemini" {
			gemini = &platforms[i]
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 Google Gemini platform, got %d", count)
	}
	if gemini == nil {
		t.Fatal("gemini platform not found")
	}

	// 校验初始状态：enabled=true, available=false, token 空, gemini 格式
	if !gemini.Enabled {
		t.Errorf("preset should be enabled, got enabled=false")
	}
	if gemini.Available {
		t.Errorf("preset should be available=false (no key yet), got available=true")
	}
	if gemini.Token != "" {
		t.Errorf("preset token should be empty, got %q", gemini.Token)
	}
	if gemini.SupportedFormats != `["gemini"]` {
		t.Errorf("preset supported_formats = %q, want %q", gemini.SupportedFormats, `["gemini"]`)
	}
	if gemini.BaseURL != "https://generativelanguage.googleapis.com" {
		t.Errorf("preset base_url = %q", gemini.BaseURL)
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

// TestGetEnabledRAPIsIgnoresPlatformAvailable verifies that GetEnabledRAPIsForLAPI
// does NOT gate RAPI selection on platform.available. A platform may have
// available=0 (e.g. stale base_url unreachable flag) while its RAPIs are still
// individually enabled+available and should remain selectable.
func TestGetEnabledRAPIsIgnoresPlatformAvailable(t *testing.T) {
	db := setupTestDB(t)

	// Create platform with available=0 (simulates stale base_url unreachable flag).
	// Enabled=true so the p.enabled=1 gate passes; we are isolating p.available.
	p := &models.Platform{Name: "test-plat", BaseURL: "https://api.test.com/v1", Token: "sk-test", Enabled: true, Available: true}
	db.CreatePlatform(p)
	if err := db.SetPlatformAvailable(p.ID, false); err != nil {
		t.Fatalf("SetPlatformAvailable: %v", err)
	}

	// Create enabled+available RAPI under this platform
	r := &models.RAPI{Alias: "test-rapi", Model: "test-model", PlatformID: p.ID, Enabled: true, Available: true}
	if err := db.CreateRAPI(r); err != nil {
		t.Fatalf("CreateRAPI: %v", err)
	}

	// Create LAPI and link the RAPI via direct INSERT (no SetLAPIRAPIs helper exists).
	l := &models.LAPI{Alias: "test-lapi"}
	db.CreateLAPI(l)
	if _, err := db.conn.Exec("INSERT INTO lapi_rapi_order (lapi_id, rapi_id, order_index) VALUES (?, ?, 0)", l.ID, r.ID); err != nil {
		t.Fatalf("insert lapi_rapi_order: %v", err)
	}

	// Even though platform.available=0, RAPI should still be returned
	rapis, err := db.GetEnabledRAPIsForLAPI(l.ID)
	if err != nil {
		t.Fatalf("GetEnabledRAPIsForLAPI: %v", err)
	}
	if len(rapis) != 1 {
		t.Fatalf("GetEnabledRAPIsForLAPI returned %d RAPIs, want 1 (platform.available should not gate selection)", len(rapis))
	}
	if rapis[0].ID != r.ID {
		t.Errorf("returned RAPI ID = %d, want %d", rapis[0].ID, r.ID)
	}
}

// TestRAPIKeyIDsRoundTrip verifies the model→key whitelist column persists
// through Create/Get/Update and flows into every RAPI read path (including the
// LAPI chain loader used by the gateway hot path).
func TestRAPIKeyIDsRoundTrip(t *testing.T) {
	db := setupTestDB(t)
	p := &models.Platform{Name: "test", BaseURL: "https://api.test.com/v1", Token: "sk-orig", Enabled: true, Available: true}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}

	r := &models.RAPI{Alias: "m2-restricted", Model: "M2", PlatformID: p.ID, KeyIDs: "7,11", Enabled: true, Available: true}
	if err := db.CreateRAPI(r); err != nil {
		t.Fatalf("CreateRAPI: %v", err)
	}

	// Read back via GetRAPIByID.
	got, err := db.GetRAPIByID(r.ID)
	if err != nil {
		t.Fatalf("GetRAPIByID: %v", err)
	}
	if got.KeyIDs != "7,11" {
		t.Errorf("GetRAPIByID KeyIDs = %q, want \"7,11\"", got.KeyIDs)
	}

	// Read back via the chain loader (hot path).
	l := &models.LAPI{Alias: "test-lapi"}
	if err := db.CreateLAPI(l); err != nil {
		t.Fatalf("CreateLAPI: %v", err)
	}
	if _, err := db.conn.Exec("INSERT INTO lapi_rapi_order (lapi_id, rapi_id, order_index) VALUES (?, ?, 0)", l.ID, r.ID); err != nil {
		t.Fatalf("insert lapi_rapi_order: %v", err)
	}
	chain, err := db.GetEnabledRAPIsForLAPI(l.ID)
	if err != nil {
		t.Fatalf("GetEnabledRAPIsForLAPI: %v", err)
	}
	if len(chain) != 1 || chain[0].KeyIDs != "7,11" {
		t.Fatalf("chain loader KeyIDs = %+v, want [\"7,11\"]", chain)
	}

	// Update changes the whitelist.
	if err := db.UpdateRAPI(&models.RAPI{ID: r.ID, Alias: "m2-restricted", Model: "M2", PlatformID: p.ID, KeyIDs: "9"}); err != nil {
		t.Fatalf("UpdateRAPI: %v", err)
	}
	got2, _ := db.GetRAPIByID(r.ID)
	if got2.KeyIDs != "9" {
		t.Errorf("UpdateRAPI KeyIDs = %q, want \"9\"", got2.KeyIDs)
	}

	// Default is empty = all keys.
	r2 := &models.RAPI{Alias: "m1-default", Model: "M1", PlatformID: p.ID, Enabled: true, Available: true}
	if err := db.CreateRAPI(r2); err != nil {
		t.Fatalf("CreateRAPI r2: %v", err)
	}
	got3, _ := db.GetRAPIByID(r2.ID)
	if got3.KeyIDs != "" {
		t.Errorf("default KeyIDs = %q, want \"\" (all keys)", got3.KeyIDs)
	}
}

// TestDetachKeyFromRAPIs verifies that deleting a key strips it from every
// RAPI key_ids whitelist and reports the affected models, while leaving
// unrelated whitelists untouched.
func TestDetachKeyFromRAPIs(t *testing.T) {
	db := setupTestDB(t)
	p := &models.Platform{Name: "detach-test", BaseURL: "https://api.test.com/v1", Token: "sk-t", Enabled: true, Available: true}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("CreatePlatform: %v", err)
	}
	k1 := models.PlatformKey{PlatformID: p.ID, Token: "k1-token", Enabled: true}
	k2 := models.PlatformKey{PlatformID: p.ID, Token: "k2-token", Enabled: true}
	if err := db.AddPlatformKey(&k1); err != nil {
		t.Fatalf("AddPlatformKey k1: %v", err)
	}
	if err := db.AddPlatformKey(&k2); err != nil {
		t.Fatalf("AddPlatformKey k2: %v", err)
	}

	refs := []*models.RAPI{
		{Alias: "m-both", Model: "M1", PlatformID: p.ID, KeyIDs: fmt.Sprintf("%d,%d", k1.ID, k2.ID), Enabled: true, Available: true},
		{Alias: "m-only-k1", Model: "M2", PlatformID: p.ID, KeyIDs: fmt.Sprintf("%d", k1.ID), Enabled: true, Available: true},
		{Alias: "m-only-k2", Model: "M3", PlatformID: p.ID, KeyIDs: fmt.Sprintf("%d", k2.ID), Enabled: true, Available: true},
		{Alias: "m-all", Model: "M4", PlatformID: p.ID, KeyIDs: "", Enabled: true, Available: true},
	}
	for _, r := range refs {
		if err := db.CreateRAPI(r); err != nil {
			t.Fatalf("CreateRAPI %s: %v", r.Alias, err)
		}
	}

	affected, err := db.DetachKeyFromRAPIs(k1.ID)
	if err != nil {
		t.Fatalf("DetachKeyFromRAPIs: %v", err)
	}
	if len(affected) != 2 || affected[0] != "m-both" || affected[1] != "m-only-k1" {
		t.Fatalf("affected = %v, want [m-both m-only-k1]", affected)
	}

	gotBoth, _ := db.GetRAPIByID(refs[0].ID)
	if gotBoth.KeyIDs != fmt.Sprintf("%d", k2.ID) {
		t.Errorf("m-both KeyIDs = %q, want %q", gotBoth.KeyIDs, fmt.Sprintf("%d", k2.ID))
	}
	gotOnlyK1, _ := db.GetRAPIByID(refs[1].ID)
	if gotOnlyK1.KeyIDs != "" {
		t.Errorf("m-only-k1 KeyIDs = %q, want \"\" (empty = all keys)", gotOnlyK1.KeyIDs)
	}
	gotOnlyK2, _ := db.GetRAPIByID(refs[2].ID)
	if gotOnlyK2.KeyIDs != fmt.Sprintf("%d", k2.ID) {
		t.Errorf("m-only-k2 KeyIDs = %q, want %q (unrelated untouched)", gotOnlyK2.KeyIDs, fmt.Sprintf("%d", k2.ID))
	}
	gotAll, _ := db.GetRAPIByID(refs[3].ID)
	if gotAll.KeyIDs != "" {
		t.Errorf("m-all KeyIDs = %q, want \"\"", gotAll.KeyIDs)
	}

	// Re-running is a no-op (id already gone).
	if again, _ := db.DetachKeyFromRAPIs(k1.ID); len(again) != 0 {
		t.Errorf("second detach affected = %v, want empty", again)
	}
}

// TestKeyModelBlocksRoundTrip verifies Block/Get/Unblock/Clear for the
// key×model capability blacklist.
func TestKeyModelBlocksRoundTrip(t *testing.T) {
	db := setupTestDB(t)
	exp := time.Now().Add(24 * time.Hour)

	if err := db.BlockKeyForModel(1, 10, "model not found: foo", exp); err != nil {
		t.Fatalf("BlockKeyForModel: %v", err)
	}
	if err := db.BlockKeyForModel(2, 10, "模型不存在", exp); err != nil {
		t.Fatalf("BlockKeyForModel 2: %v", err)
	}

	got, err := db.GetKeyModelBlocks()
	if err != nil {
		t.Fatalf("GetKeyModelBlocks: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("blocks = %d, want 2", len(got))
	}

	// Refresh (upsert) an existing pair.
	if err := db.BlockKeyForModel(1, 10, "updated reason", exp); err != nil {
		t.Fatalf("BlockKeyForModel upsert: %v", err)
	}
	got, _ = db.GetKeyModelBlocks()
	for _, b := range got {
		if b.KeyID == 1 && b.Reason != "updated reason" {
			t.Errorf("upsert reason = %q, want %q", b.Reason, "updated reason")
		}
	}

	if err := db.UnblockKeyForModel(1, 10); err != nil {
		t.Fatalf("UnblockKeyForModel: %v", err)
	}
	got, _ = db.GetKeyModelBlocks()
	if len(got) != 1 || got[0].KeyID != 2 {
		t.Fatalf("after unblock = %+v, want only key 2", got)
	}

	if err := db.ClearKeyModelBlocksForKey(2); err != nil {
		t.Fatalf("ClearKeyModelBlocksForKey: %v", err)
	}
	got, _ = db.GetKeyModelBlocks()
	if len(got) != 0 {
		t.Fatalf("after clear = %+v, want empty", got)
	}
}

// TestGetHourlyDistributionWithBrokenTimestamps verifies the analytics
// aggregates never error on rows whose timestamp was written in Go's
// time.Time.String() format (SQLite strftime cannot parse " +0000 UTC m=+123").
// The old-format rows must be excluded; parseable rows must be counted.
func TestGetHourlyDistributionWithBrokenTimestamps(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now()

	// request_logs lives in the logger package; create a minimal copy here.
	if _, err := db.conn.Exec(`
		CREATE TABLE IF NOT EXISTS request_logs (
			id TEXT PRIMARY KEY, session_id TEXT, timestamp DATETIME,
			client_ip TEXT, request_method TEXT, request_path TEXT,
			request_headers TEXT, request_body TEXT, req_max_tokens INTEGER DEFAULT 0,
			lapi_alias TEXT, matched_rapis TEXT, selected_rapi TEXT,
			upstream_url TEXT, upstream_headers TEXT, upstream_body TEXT,
			response_status INTEGER, response_headers TEXT, response_body TEXT,
			latency_ms INTEGER, tokens_used INTEGER, finish_reason TEXT,
			error_message TEXT, retry_count INTEGER, fallback_used INTEGER,
			status TEXT, completed_at DATETIME
		)
	`); err != nil {
		t.Fatalf("create request_logs: %v", err)
	}

	// Old-format row (time.Time.String()) — strftime unparseable.
	broken := now.Format("2006-01-02 15:04:05.999999999 -0700 MST m=+999.999")
	if _, err := db.conn.Exec(`INSERT INTO request_logs (id, timestamp, lapi_alias, response_status, latency_ms) VALUES ('broken-1', ?, 'old-lapi', 200, 100)`, broken); err != nil {
		t.Fatalf("insert broken row: %v", err)
	}

	// New-format row (parseable by SQLite).
	good := now.Format("2006-01-02 15:04:05.999999999-07:00")
	if _, err := db.conn.Exec(`INSERT INTO request_logs (id, timestamp, lapi_alias, response_status, latency_ms) VALUES ('good-1', ?, 'new-lapi', 200, 200)`, good); err != nil {
		t.Fatalf("insert good row: %v", err)
	}

	buckets, err := db.GetHourlyDistribution(7)
	if err != nil {
		t.Fatalf("GetHourlyDistribution: %v", err)
	}
	foundNew := false
	foundOld := false
	for _, b := range buckets {
		if b.LapiAlias == "new-lapi" {
			foundNew = true
		}
		if b.LapiAlias == "old-lapi" {
			foundOld = true
		}
	}
	if !foundNew {
		t.Errorf("parseable row missing from buckets: %+v", buckets)
	}
	if foundOld {
		t.Errorf("unparseable old row should be excluded, got %+v", buckets)
	}

	trends, err := db.GetDailyTokenTrend(7)
	if err != nil {
		t.Fatalf("GetDailyTokenTrend: %v", err)
	}
	foundNewTrend := false
	for _, tr := range trends {
		if tr.LapiAlias == "new-lapi" {
			foundNewTrend = true
		}
	}
	if !foundNewTrend {
		t.Errorf("parseable row missing from daily trend: %+v", trends)
	}
}
