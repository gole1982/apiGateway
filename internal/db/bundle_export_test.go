package db

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"gateway/internal/bundle"
	"gateway/internal/models"
)

// seedLocalDefs 用本地 API（而非 bundle）建一套定义数据，
// 以确保导出器面对的是"真实写入路径产生的库"。
func seedLocalDefs(t *testing.T, db *DB) {
	t.Helper()
	p := &models.Platform{
		Name: "openai", BaseURL: "https://api.openai.com", Token: "sk-plat",
		Enabled: true, SupportedFormats: `["openai"]`,
	}
	if err := db.CreatePlatform(p); err != nil {
		t.Fatalf("create platform: %v", err)
	}
	p2 := &models.Platform{
		Name: "jd", BaseURL: "https://api.jd.com", Token: "sk-jd",
		Enabled: true, SupportedFormats: `["openai"]`,
	}
	if err := db.CreatePlatform(p2); err != nil {
		t.Fatalf("create platform 2: %v", err)
	}
	for i, tok := range []string{"sk-a", "sk-b"} {
		if err := db.AddPlatformKey(&models.PlatformKey{
			PlatformID: p.ID, KeyIndex: i, Token: tok, Label: tok, Enabled: true,
		}); err != nil {
			t.Fatalf("add key %d: %v", i, err)
		}
	}
	for _, spec := range []struct {
		alias, model, base string
	}{{"gpt-4", "gpt-4", "https://api.openai.com"}, {"gpt-4o", "gpt-4o", "https://api.openai.com"},
		{"jd-chat", "jd-chat", "https://api.jd.com"}} {
		pid := p.ID
		if spec.base != p.BaseURL {
			pid = p2.ID
		}
		if err := db.CreateRAPI(&models.RAPI{
			Alias: spec.alias, Model: spec.model, PlatformID: pid, Enabled: true,
			SupportedFormats: `["openai"]`, Source: "manual",
		}); err != nil {
			t.Fatalf("create rapi %s: %v", spec.alias, err)
		}
	}
	if err := db.CreateLAPI(&models.LAPI{Alias: "chat", Enabled: true}); err != nil {
		t.Fatalf("create lapi: %v", err)
	}
}

// 导出器必须产出通过 Validate 的自洽快照，且自然键取值与本地行一致。
func TestExportBundle_ProducesSelfConsistentV2(t *testing.T) {
	db := setupTestDB(t)
	seedLocalDefs(t, db)

	b, err := db.ExportBundle() // 中心一律明文
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if err := bundle.Validate(&bundle.Envelope{
		SchemaVersion: bundle.SchemaVersion, Bundle: *b,
	}); err != nil {
		t.Fatalf("exported bundle must validate: %v", err)
	}
	if len(b.Platforms) != 2 {
		t.Errorf("platforms = %d, want 2", len(b.Platforms))
	}
	if len(b.Credentials) != 2 {
		t.Fatalf("credentials = %d, want 2", len(b.Credentials))
	}
	// 凭据必须带出归属平台与轮换序号
	for _, c := range b.Credentials {
		if c.PlatformBaseURL == "" {
			t.Errorf("credential %s missing platform_base_url", c.TokenHash)
		}
		if c.TokenHash != models.TokenHash(c.Token) {
			t.Errorf("token_hash %s does not match token (plaintext mode)", c.TokenHash)
		}
	}
	for _, r := range b.RAPIs {
		if r.Model == "" || r.PlatformBaseURL == "" {
			t.Errorf("rapi missing natural key: %+v", r)
		}
	}
	if len(b.RAPIs) != 3 {
		t.Errorf("rapis = %d, want 3", len(b.RAPIs))
	}
}

// 往返一致性：export → ApplyBundle → export 语义必须完全相等。
// 这是"本地数据能安全穿过 v2 契约"的回归闸门。
// 中心一律明文，导出 token 即明文，可直接 DeepEqual。
func TestExportBundle_RoundTripIsStable(t *testing.T) {
	db := setupTestDB(t)
	seedLocalDefs(t, db)

	first, err := db.ExportBundle()
	if err != nil {
		t.Fatalf("export #1: %v", err)
	}
	env := &bundle.Envelope{SchemaVersion: bundle.SchemaVersion, Version: 1, Bundle: *first}
	if err := db.ApplyBundle(env, "http://test"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	second, err := db.ExportBundle()
	if err != nil {
		t.Fatalf("export #2: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		j1, _ := json.Marshal(first)
		j2, _ := json.Marshal(second)
		t.Errorf("round trip changed the bundle:\n#1=%s\n#2=%s", j1, j2)
	}
	// 中心明文模式下导出 token 必须就是明文（无 enc: 前缀）
	for _, c := range second.Credentials {
		if c.Token == "" {
			continue
		}
		if strings.HasPrefix(c.Token, "enc:") {
			t.Errorf("credential token must be center plaintext, got ciphertext")
		}
	}
}

// 同名平台 / 同显示名模型必须保持为两行（这正是 v2 要修的静默丢数据）。
func TestExportBundle_SameNamePlatformsStayDistinct(t *testing.T) {
	db := setupTestDB(t)
	for _, u := range []string{"https://a.example.com", "https://b.example.com"} {
		if err := db.CreatePlatform(&models.Platform{
			Name: "same-display-name", BaseURL: u, Token: "t", Enabled: true,
			SupportedFormats: `["openai"]`,
		}); err != nil {
			t.Fatalf("create platform %s: %v", u, err)
		}
	}
	b, err := db.ExportBundle()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(b.Platforms) != 2 {
		t.Fatalf("two same-named platforms collapsed to %d — v2 key regression", len(b.Platforms))
	}
	if b.Platforms[0].BaseURL == b.Platforms[1].BaseURL {
		t.Error("distinct base_url must remain distinct keys")
	}
}

// 绑定导出：真实绑定行必须带出（建 rapi 时已自动全量绑定），悬挂行必须跳过。
func TestExportBundle_BindingsFollowEndpointCredential(t *testing.T) {
	db := setupTestDB(t)
	seedLocalDefs(t, db)

	// 建 rapi 时 syncBindingsTx 已为"空 key_ids"展开成"平台全部凭据"的绑定，
	// 所以这里不该再插一条（会撞 UNIQUE(rapi_id, credential_id)），而是改限额。
	var n int
	if err := db.conn.QueryRow(`SELECT count(*) FROM endpoint_credential`).Scan(&n); err != nil {
		t.Fatalf("count bindings: %v", err)
	}
	if n == 0 {
		t.Fatal("expected auto-created bindings for openai rapis")
	}
	if _, err := db.conn.Exec(`UPDATE endpoint_credential SET rpm_limit=42 WHERE rowid=(
		SELECT min(rowid) FROM endpoint_credential)`); err != nil {
		t.Fatalf("update binding limit: %v", err)
	}

	b, err := db.ExportBundle()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(b.Bindings) != n {
		t.Fatalf("bindings = %d, want %d", len(b.Bindings), n)
	}
	found42 := false
	for _, bd := range b.Bindings {
		if bd.RPMLimit == 42 {
			found42 = true
		}
	}
	if !found42 {
		t.Errorf("binding limit not carried into export: %+v", b.Bindings)
	}

	// 悬挂绑定不得产出悬空引用：删掉端点，绑定应随之消失
	if _, err := db.conn.Exec(`DELETE FROM rapi WHERE id=(SELECT min(rapi_id) FROM endpoint_credential)`); err != nil {
		t.Fatalf("delete rapi: %v", err)
	}
	b2, err := db.ExportBundle()
	if err != nil {
		t.Fatalf("export with dangling binding: %v", err)
	}
	if len(b2.Bindings) >= n {
		t.Errorf("dangling bindings must be skipped, got %d want < %d", len(b2.Bindings), n)
	}
}
