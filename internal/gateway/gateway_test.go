package gateway

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"gateway/internal/apiformat"
	"gateway/internal/entity"
	"gateway/internal/models"
)

// TestFilterKeysByKeyIDs verifies the model→key whitelist filter: empty whitelist
// keeps all keys (the default), a non-empty whitelist keeps exactly the listed
// IDs in original order, and unknown/garbage IDs are dropped silently.
func TestFilterKeysByKeyIDs(t *testing.T) {
	keys := []models.PlatformKey{
		{ID: 11, KeyIndex: 0, Token: "k1", Enabled: true},
		{ID: 22, KeyIndex: 1, Token: "k2", Enabled: true},
		{ID: 33, KeyIndex: 2, Token: "k3", Enabled: true},
	}

	// Empty whitelist = all platform keys.
	if got := filterKeysByKeyIDs(keys, ""); len(got) != 3 {
		t.Fatalf("empty whitelist: got %d keys, want 3", len(got))
	}
	if got := filterKeysByKeyIDs(keys, "   "); len(got) != 3 {
		t.Fatalf("whitespace whitelist: got %d keys, want 3", len(got))
	}

	// Whitelist keeps listed IDs in original order.
	got := filterKeysByKeyIDs(keys, "33,11")
	if len(got) != 2 || got[0].ID != 11 || got[1].ID != 33 {
		t.Fatalf("subset filter: got %+v, want [11 33] in original order", got)
	}

	// Unknown IDs and garbage are dropped silently.
	got = filterKeysByKeyIDs(keys, "11,999,abc,,42")
	if len(got) != 1 || got[0].ID != 11 {
		t.Fatalf("garbage-tolerant filter: got %+v, want [11]", got)
	}

	// No overlap → empty pool (the RAPI is then hard-dead if it has no other keys).
	if got := filterKeysByKeyIDs(keys, "777"); len(got) != 0 {
		t.Fatalf("no-overlap filter: got %d keys, want 0", len(got))
	}
}

// TestModelKeyPoolHardDead reproduces the design-B scenario: platform P has K1
// (serves M1+M2) and K2 (serves M1+M3). K1 is billed/permanently failed. The
// M2 pool (whitelist=[K1]) is then hard-dead while the M1 pool (whitelist=[K1,K2])
// still has K2 healthy — so only the affected RAPI is marked unavailable.
func TestModelKeyPoolHardDead(t *testing.T) {
	k1 := models.PlatformKey{ID: 1, KeyIndex: 0, Token: "k1", Enabled: true, FailureType: 2, FailureReason: "[平台级] upstream 403: 用户积分不足"}
	k2 := models.PlatformKey{ID: 2, KeyIndex: 1, Token: "k2", Enabled: true}
	k3 := models.PlatformKey{ID: 3, KeyIndex: 2, Token: "k3", Enabled: true}
	all := []models.PlatformKey{k1, k2, k3}

	// M1 pool: K1+K2. K2 healthy → not hard-dead.
	m1Pool := filterKeysByKeyIDs(all, "1,2")
	if dead, reason := entity.KeysHardDead(m1Pool); dead {
		t.Fatalf("M1 pool [K1,K2] should NOT be hard-dead, got reason %q", reason)
	}

	// M2 pool: only K1 (dead) → hard-dead with the underlying billing reason.
	m2Pool := filterKeysByKeyIDs(all, "1")
	dead, reason := entity.KeysHardDead(m2Pool)
	if !dead {
		t.Fatal("M2 pool [K1] with K1 permanently failed should be hard-dead")
	}
	if !strings.Contains(reason, "用户积分不足") {
		t.Errorf("reason = %q, want it to surface the billing reason", reason)
	}

	// M3 pool: only K2 (healthy) → not hard-dead.
	m3Pool := filterKeysByKeyIDs(all, "2")
	if dead, _ := entity.KeysHardDead(m3Pool); dead {
		t.Fatal("M3 pool [K2] healthy should NOT be hard-dead")
	}

	// A whitelist referencing keys that no longer exist → empty pool. Empty pool
	// is never hard-dead by contract (tryKeyForRAPI falls back to the platform
	// token in that case), so the RAPI degrades instead of disappearing.
	stalePool := filterKeysByKeyIDs(all, "777")
	if dead, _ := entity.KeysHardDead(stalePool); dead {
		t.Fatal("empty pool should NOT be hard-dead (synthetic-key fallback)")
	}
}

// TestReplaceModelField verifies the lossless model-field replacement used by the
// passthrough/fast path. Unlike the old byte-level replaceModelInBody, this uses a
// json decode/encode round-trip, so it never corrupts "model":"X" substrings that
// appear inside user content and is whitespace-agnostic.
func TestReplaceModelField(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		newModel  string
		wantModel string // expected value of the model field after replacement
	}{
		{"no space", `{"model":"gpt-4","messages":[]}`, "claude-3", "claude-3"},
		{"with space", `{"model": "gpt-4", "messages":[]}`, "claude-3", "claude-3"},
		{"already correct", `{"model":"claude-3","messages":[]}`, "claude-3", "claude-3"},
		// Regression: the old byte-level replaceModelInBody would also replace a
		// "model":"gpt-4" substring appearing INSIDE user content, corrupting it.
		// ReplaceModelField must only touch the top-level model field.
		{"model substring in content", `{"model":"gpt-4","messages":[{"role":"user","content":"the model is \"model\":\"gpt-4\" here"}]}`, "claude-3", "claude-3"},
		{"empty new model leaves body intact", `{"model":"gpt-4","messages":[]}`, "", "gpt-4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := apiformat.ReplaceModelField([]byte(tt.body), tt.newModel)
			if !strings.Contains(string(result), `"model":"`+tt.wantModel+`"`) {
				t.Errorf("result %q does not contain model=%q", string(result), tt.wantModel)
			}
		})
	}
}

// TestReplaceModelFieldDoesNotCorruptContent is a direct regression test for the
// byte-substitution bug: a user message whose text contains the literal substring
// "model":"gpt-4" must NOT be rewritten when only the top-level model field changes.
func TestReplaceModelFieldDoesNotCorruptContent(t *testing.T) {
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"do not touch \"model\":\"gpt-4\""}]}`
	result := apiformat.ReplaceModelField([]byte(body), "claude-3")
	if !strings.Contains(string(result), `"content":"do not touch \"model\":\"gpt-4\""`) {
		t.Errorf("byte-level corruption regression: user content was rewritten\nresult: %s", string(result))
	}
	if !strings.Contains(string(result), `"model":"claude-3"`) {
		t.Errorf("top-level model field was not replaced\nresult: %s", string(result))
	}
}

func TestExtractTokenUsage(t *testing.T) {
	tests := []struct {
		name     string
		headers  http.Header
		expected int
	}{
		{"openai-usage", http.Header{"Openai-Usage": []string{"150"}}, 150},
		{"x-token-usage", http.Header{"X-Token-Usage": []string{"200"}}, 200},
		{"no usage header", http.Header{}, 0},
		{"invalid number", http.Header{"Openai-Usage": []string{"abc"}}, 0},
		{"openai takes precedence", http.Header{"Openai-Usage": []string{"100"}, "X-Token-Usage": []string{"200"}}, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractTokenUsage(tt.headers)
			if got != tt.expected {
				t.Errorf("extractTokenUsage() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestAllKeysHardDead verifies the hard-dead key pool detection used to surface
// RAPIs whose keys are all unusable without operator intervention (vs. merely
// cooling down from transient 429/5xx failures, which auto-recover).
func TestAllKeysHardDead(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	expired := func() *time.Time { t := past; return &t }
	notExpired := func() *time.Time { t := future; return &t }

	base := models.PlatformKey{ID: 1, Enabled: true, FailureType: 0}
	tests := []struct {
		name     string
		keys     []models.PlatformKey
		wantDead bool
	}{
		{"empty pool is not hard-dead", nil, false},
		{"single healthy key not dead", []models.PlatformKey{base}, false},
		{"healthy key + dead key not dead", []models.PlatformKey{
			base,
			{ID: 2, Enabled: false},
		}, false},
		{"all disabled", []models.PlatformKey{
			{ID: 1, Enabled: false},
			{ID: 2, Enabled: false},
		}, true},
		{"all permanently failed", []models.PlatformKey{
			{ID: 1, Enabled: true, FailureType: 2, FailureReason: "[平台级] upstream 403: 用户积分不足"},
			{ID: 2, Enabled: true, FailureType: 2},
		}, true},
		{"all expired", []models.PlatformKey{
			{ID: 1, Enabled: true, ExpiresAt: expired()},
		}, true},
		{"disabled + expired", []models.PlatformKey{
			{ID: 1, Enabled: false},
			{ID: 2, Enabled: true, ExpiresAt: expired()},
		}, true},
		{"cooling (temp failure) not dead", []models.PlatformKey{
			{ID: 1, Enabled: true, FailureType: 1, FailureReason: "upstream 429"},
		}, false},
		{"permanent + temp not dead", []models.PlatformKey{
			{ID: 1, Enabled: true, FailureType: 2},
			{ID: 2, Enabled: true, FailureType: 1},
		}, false},
		{"healthy with expiry future not dead", []models.PlatformKey{
			{ID: 1, Enabled: true, ExpiresAt: notExpired()},
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dead, reason := entity.KeysHardDead(tt.keys)
			if dead != tt.wantDead {
				t.Errorf("allKeysHardDead() dead = %v, want %v (reason=%q)", dead, tt.wantDead, reason)
			}
			if tt.wantDead && reason == "" {
				t.Errorf("allKeysHardDead() returned empty reason for a hard-dead pool")
			}
		})
	}
}

func TestExtractUsageDetail(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		wantIn, wantOut, wantCached int
	}{
		{
			name: "openai plain json",
			body: `{"id":"x","usage":{"prompt_tokens":100,"completion_tokens":40,"prompt_tokens_details":{"cached_tokens":60}}}`,
			wantIn: 100, wantOut: 40, wantCached: 60,
		},
		{
			name: "anthropic plain json",
			body: `{"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":8,"cache_creation_input_tokens":2}}`,
			wantIn: 10, wantOut: 5, wantCached: 10,
		},
		{
			name: "gemini usageMetadata",
			body: `{"candidates":[],"usageMetadata":{"promptTokenCount":500,"candidatesTokenCount":120,"cachedContentTokenCount":300}}`,
			wantIn: 500, wantOut: 120, wantCached: 300,
		},
		{
			name: "sse stream last usage frame wins",
			body: "data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: {\"usage\":{\"prompt_tokens\":200,\"completion_tokens\":90,\"prompt_tokens_details\":{\"cached_tokens\":150}}}\n\ndata: [DONE]\n\n",
			wantIn: 200, wantOut: 90, wantCached: 150,
		},
		{
			name: "empty body",
			body: "",
			wantIn: 0, wantOut: 0, wantCached: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in, out, cached := extractUsageDetail(tt.body)
			if in != tt.wantIn || out != tt.wantOut || cached != tt.wantCached {
				t.Errorf("extractUsageDetail(%s) = (%d,%d,%d), want (%d,%d,%d)",
					tt.name, in, out, cached, tt.wantIn, tt.wantOut, tt.wantCached)
			}
		})
	}
}
