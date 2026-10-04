package db

import (
	"database/sql"
	"strings"
	"testing"

	"gateway/internal/bundle"
	"gateway/internal/crypto"

	"gateway/internal/models"
)

func countRows(t *testing.T, conn *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

func TestApplyBundle_InsertUpdateDelete(t *testing.T) {
	db := setupTestDB(t)

	// v1：1 平台 + 1 凭据 + 1 端点 + 1 lapi + 1 绑定 + 1 路由链
	// （v2 自然键：platform=base_url、credential=token_hash、rapi=(base_url, model)）
	// 中心 token 一律明文。
	credToken := "sk-key0"
	v1 := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       1,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{
				Name: "openai", BaseURL: "https://api.openai.com",
				Token: "sk-aaa", Enabled: true, SupportedFormats: `["openai"]`,
			}},
			Credentials: []bundle.Credential{{
				TokenHash: models.TokenHash(credToken), PlatformBaseURL: "https://api.openai.com", Token: credToken, Enabled: true,
			}},
			RAPIs: []bundle.RAPI{{
				PlatformBaseURL: "https://api.openai.com", Alias: "gpt-4", Model: "gpt-4", Enabled: true,
				SupportedFormats: `["openai"]`,
			}},
			LAPIs: []bundle.LAPI{{Alias: "chat", Enabled: true}},
			Bindings: []bundle.CredentialBinding{{
				PlatformBaseURL: "https://api.openai.com", Model: "gpt-4",
				TokenHash: models.TokenHash(credToken), Enabled: true,
			}},
			LAPIRapiOrder: []bundle.LAPIRapiOrder{{
				LAPIAlias: "chat", RAPIPlatformBaseURL: "https://api.openai.com",
				RAPIModel: "gpt-4", OrderIndex: 0,
			}},
		},
	}

	if err := db.ApplyBundle(v1, "http://test"); err != nil {
		t.Fatalf("apply v1: %v", err)
	}

	// sync_state
	st, err := db.GetSyncState()
	if err != nil {
		t.Fatalf("get sync_state: %v", err)
	}
	if st.CurrentVersion != 1 || st.LastGoodVersion != 1 {
		t.Fatalf("sync_state version = (%d,%d), want (1,1)", st.CurrentVersion, st.LastGoodVersion)
	}
	if st.SourceURL != "http://test" {
		t.Fatalf("sync_state source_url = %q, want http://test", st.SourceURL)
	}

	// platform 落库 + token 用本地 key 可解出明文（边界重加密生效）
	var tok string
	if err := db.conn.QueryRow(`SELECT token FROM platform WHERE name=?`, "openai").Scan(&tok); err != nil {
		t.Fatalf("read platform token: %v", err)
	}
	if dec, _ := crypto.Decrypt(tok); dec != "sk-aaa" {
		t.Fatalf("platform token decrypt = %q, want sk-aaa", dec)
	}

	// rapi.key_ids：bundle 里是 key_index "0" → 应解析成本地 credential.id
	var keyIDs string
	if err := db.conn.QueryRow(`SELECT key_ids FROM rapi WHERE alias=?`, "gpt-4").Scan(&keyIDs); err != nil {
		t.Fatalf("read rapi key_ids: %v", err)
	}
	if keyIDs != "1" { // 第一条 credential 的自增 id = 1
		t.Fatalf("rapi key_ids = %q, want \"1\" (resolved local id)", keyIDs)
	}

	// lapi_rapi_order 1 条
	if n := countRows(t, db.conn, `SELECT count(*) FROM lapi_rapi_order`); n != 1 {
		t.Fatalf("lapi_rapi_order count = %d, want 1", n)
	}

	// ---- V2：平台改名 base_url、删 key/rapi/lapi（中心只留平台本身）----
	v2 := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       2,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{
				Name: "openai", BaseURL: "https://api.openai.com/v2",
				Token: "sk-aaa2", Enabled: true, SupportedFormats: `["openai"]`,
			}},
		},
	}
	if err := db.ApplyBundle(v2, "http://test"); err != nil {
		t.Fatalf("apply v2: %v", err)
	}

	if n := countRows(t, db.conn, `SELECT count(*) FROM credential`); n != 0 {
		t.Fatalf("after v2, credential count = %d, want 0 (stale key deleted)", n)
	}
	if n := countRows(t, db.conn, `SELECT count(*) FROM rapi`); n != 0 {
		t.Fatalf("after v2, rapi count = %d, want 0", n)
	}
	if n := countRows(t, db.conn, `SELECT count(*) FROM lapi`); n != 0 {
		t.Fatalf("after v2, lapi count = %d, want 0", n)
	}
	if n := countRows(t, db.conn, `SELECT count(*) FROM lapi_rapi_order`); n != 0 {
		t.Fatalf("after v2, lapi_rapi_order count = %d, want 0", n)
	}

	var baseURL string
	if err := db.conn.QueryRow(`SELECT base_url FROM platform WHERE name=?`, "openai").Scan(&baseURL); err != nil {
		t.Fatalf("read platform base_url: %v", err)
	}
	if baseURL != "https://api.openai.com/v2" {
		t.Fatalf("base_url = %q, want v2", baseURL)
	}
	st2, _ := db.GetSyncState()
	if st2.CurrentVersion != 2 || st2.LastGoodVersion != 2 {
		t.Fatalf("sync_state after v2 = (%d,%d), want (2,2)", st2.CurrentVersion, st2.LastGoodVersion)
	}
}

// 平台登录账号随 bundle 同步：apply 落库 login_account，二次同步更新它。
func TestApplyBundle_SyncsLoginAccount(t *testing.T) {
	db := setupTestDB(t)

	v1 := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       1,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{
				Name: "openai", BaseURL: "https://api.openai.com",
				Token: "sk-aaa", Enabled: true,
				LoginAccount: "ops@example.com",
			}},
		},
	}
	if err := db.ApplyBundle(v1, "http://test"); err != nil {
		t.Fatalf("apply v1: %v", err)
	}

	var stored string
	if err := db.conn.QueryRow(`SELECT login_account FROM platform WHERE base_url=?`,
		"https://api.openai.com").Scan(&stored); err != nil {
		t.Fatalf("read login_account: %v", err)
	}
	if stored != "ops@example.com" {
		t.Fatalf("login_account = %q, want ops@example.com", stored)
	}

	v2 := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       2,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{
				Name: "openai", BaseURL: "https://api.openai.com",
				Token: "sk-aaa", Enabled: true,
				LoginAccount: "ops2@example.com",
			}},
		},
	}
	if err := db.ApplyBundle(v2, "http://test"); err != nil {
		t.Fatalf("apply v2: %v", err)
	}

	var after string
	if err := db.conn.QueryRow(`SELECT login_account FROM platform WHERE base_url=?`,
		"https://api.openai.com").Scan(&after); err != nil {
		t.Fatalf("read login_account after v2: %v", err)
	}
	if after != "ops2@example.com" {
		t.Fatalf("login_account after v2 = %q, want ops2@example.com", after)
	}
}

func TestApplyBundle_IdempotentReapply(t *testing.T) {
	db := setupTestDB(t)

	v := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       5,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{
				Name: "p1", BaseURL: "https://x", Token: "sk",
				Enabled: true, SupportedFormats: `["openai"]`,
			}},
			Credentials: []bundle.Credential{{
				TokenHash: models.TokenHash("k0"), PlatformBaseURL: "https://x", Token: "k0", Enabled: true,
			}},
		},
	}
	if err := db.ApplyBundle(v, "http://test"); err != nil {
		t.Fatalf("apply #1: %v", err)
	}
	// 重复应用同一 envelope：不应产生重复行，platform base_url 不变
	if err := db.ApplyBundle(v, "http://test"); err != nil {
		t.Fatalf("apply #2: %v", err)
	}
	if n := countRows(t, db.conn, `SELECT count(*) FROM platform WHERE name=?`, "p1"); n != 1 {
		t.Fatalf("platform count after re-apply = %d, want 1", n)
	}
	if n := countRows(t, db.conn, `SELECT count(*) FROM credential`); n != 1 {
		t.Fatalf("credential count after re-apply = %d, want 1", n)
	}
}

func TestApplyBundle_RejectsInvalidReference(t *testing.T) {
	db := setupTestDB(t)

	// v2 里凭据不直接引用平台（归属由绑定的端点决定），等价的悬空引用是
	// 绑定指向不存在的端点 → bundle.Validate 在 ApplyBundle 内拒掉。
	bad := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       1,
		Bundle: bundle.Bundle{
			Platforms:   []bundle.Platform{{Name: "p1", BaseURL: "https://x", Enabled: true, SupportedFormats: `["openai"]`}},
			Credentials: []bundle.Credential{{TokenHash: models.TokenHash("k"), PlatformBaseURL: "https://x", Token: "k", Enabled: true}},
			Bindings: []bundle.CredentialBinding{{
				PlatformBaseURL: "https://ghost", Model: "nope",
				TokenHash: models.TokenHash("k"), Enabled: true,
			}},
		},
	}
	if err := db.ApplyBundle(bad, "http://test"); err == nil {
		t.Fatal("expected validation error for dangling platform reference, got nil")
	}
	// sync_state 不应被写入（fail-open：不动现有配置）
	st, _ := db.GetSyncState()
	if st.CurrentVersion != 0 {
		t.Fatalf("sync_state version = %d, want 0 (must not advance on failure)", st.CurrentVersion)
	}
}

func TestApplyBundle_RejectsEmptyBundle(t *testing.T) {
	db := setupTestDB(t)

	// 空定义集（中心被误删一切）→ 必须拒绝，绝不落库后清空
	empty := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       9,
		Bundle:        bundle.Bundle{},
	}
	// 先塞一条数据进去
	seed := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       1,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{Name: "p1", BaseURL: "https://x", Enabled: true, SupportedFormats: `["openai"]`}},
		},
	}
	if err := db.ApplyBundle(seed, "http://test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 空 bundle：Validate 不会拦（无引用问题），但 ApplyBundle 必须拒绝以防误删一切
	if err := db.ApplyBundle(empty, "http://test"); err == nil {
		t.Fatal("expected error applying empty bundle (anti-mass-delete guard), got nil")
	}
	// 原有数据保留
	if n := countRows(t, db.conn, `SELECT count(*) FROM platform`); n != 1 {
		t.Fatalf("platform count after empty apply = %d, want 1 (preserved)", n)
	}
}

// 中心历史加密残留（enc: 前缀，center_key 已取消无法解密）→ 直接报错并指引
// 用推送改写为明文，而不是存下解不开的密文。
func TestApplyBundle_RejectsLegacyCiphertext(t *testing.T) {
	db := setupTestDB(t)

	enc := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       1,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{
				Name: "openai", BaseURL: "https://api.openai.com",
				Token: "enc:deadbeef", Enabled: true, SupportedFormats: `["openai"]`,
			}},
		},
	}
	err := db.ApplyBundle(enc, "http://test")
	if err == nil {
		t.Fatal("expected error for legacy center ciphertext, got nil")
	}
	if got := err.Error(); !strings.Contains(got, "整体覆盖推送") {
		t.Errorf("error = %q, want push-to-center guidance", got)
	}
	// 失败不得落库（fail-open：沿用本地旧配置）
	if n := countRows(t, db.conn, `SELECT count(*) FROM platform`); n != 0 {
		t.Fatalf("platform count = %d, want 0 (nothing applied on failure)", n)
	}
}
