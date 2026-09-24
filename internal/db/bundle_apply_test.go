package db

import (
	"database/sql"
	"testing"

	"gateway/internal/bundle"
	"gateway/internal/crypto"
)

// 固定中心密钥（32 字节 hex）。测试用，生产请随机生成。
const testCenterKeyHex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func mustCenterKey(t *testing.T) []byte {
	t.Helper()
	k, err := crypto.ParseKey(testCenterKeyHex)
	if err != nil {
		t.Fatalf("parse center key: %v", err)
	}
	return k
}

// encCenter 用中心密钥加密一个明文（模拟管理端写入中心库的密文形态）。
func encCenter(t *testing.T, plain string, ck []byte) string {
	t.Helper()
	c, err := crypto.EncryptWithKey(plain, ck)
	if err != nil {
		t.Fatalf("encrypt with center key: %v", err)
	}
	return c
}

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
	ck := mustCenterKey(t)

	// V1：1 平台 + 1 key + 1 rapi + 1 lapi + 1 路由链
	v1 := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       1,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{
				Name: "openai", BaseURL: "https://api.openai.com",
				Token: encCenter(t, "sk-aaa", ck), Enabled: true, SupportedFormats: `["openai"]`,
			}},
			PlatformKeys: []bundle.PlatformKey{{
				PlatformName: "openai", KeyIndex: 0, Token: encCenter(t, "sk-key0", ck), Enabled: true,
			}},
			RAPIs: []bundle.RAPI{{
				PlatformName: "openai", Alias: "gpt-4", Model: "gpt-4", Enabled: true,
				KeyIDs: "0", SupportedFormats: `["openai"]`,
			}},
			LAPIs: []bundle.LAPI{{Alias: "chat", Enabled: true}},
			LAPIRapiOrder: []bundle.LAPIRapiOrder{{
				LAPIAlias: "chat", RAPIPlatformName: "openai", RAPIAlias: "gpt-4", OrderIndex: 0,
			}},
		},
	}

	if err := db.ApplyBundle(v1, ck, "http://test"); err != nil {
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
				Token: encCenter(t, "sk-aaa2", ck), Enabled: true, SupportedFormats: `["openai"]`,
			}},
		},
	}
	if err := db.ApplyBundle(v2, ck, "http://test"); err != nil {
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

func TestApplyBundle_IdempotentReapply(t *testing.T) {
	db := setupTestDB(t)
	ck := mustCenterKey(t)

	v := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       5,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{
				Name: "p1", BaseURL: "https://x", Token: encCenter(t, "sk", ck),
				Enabled: true, SupportedFormats: `["openai"]`,
			}},
			PlatformKeys: []bundle.PlatformKey{{
				PlatformName: "p1", KeyIndex: 0, Token: encCenter(t, "k0", ck), Enabled: true,
			}},
		},
	}
	if err := db.ApplyBundle(v, ck, "http://test"); err != nil {
		t.Fatalf("apply #1: %v", err)
	}
	// 重复应用同一 envelope：不应产生重复行，platform base_url 不变
	if err := db.ApplyBundle(v, ck, "http://test"); err != nil {
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
	ck := mustCenterKey(t)

	// key 引用了不存在的平台 → bundle.Validate 在 ApplyBundle 内拒掉
	bad := &bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion,
		Version:       1,
		Bundle: bundle.Bundle{
			Platforms: []bundle.Platform{{Name: "p1", BaseURL: "https://x", Enabled: true, SupportedFormats: `["openai"]`}},
			PlatformKeys: []bundle.PlatformKey{{
				PlatformName: "ghost", KeyIndex: 0, Token: encCenter(t, "k", ck), Enabled: true,
			}},
		},
	}
	if err := db.ApplyBundle(bad, ck, "http://test"); err == nil {
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
	ck := mustCenterKey(t)

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
	if err := db.ApplyBundle(seed, ck, "http://test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 空 bundle：Validate 不会拦（无引用问题），但 ApplyBundle 必须拒绝以防误删一切
	if err := db.ApplyBundle(empty, ck, "http://test"); err == nil {
		t.Fatal("expected error applying empty bundle (anti-mass-delete guard), got nil")
	}
	// 原有数据保留
	if n := countRows(t, db.conn, `SELECT count(*) FROM platform`); n != 1 {
		t.Fatalf("platform count after empty apply = %d, want 1 (preserved)", n)
	}
}
