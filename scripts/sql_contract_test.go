// Package scripts holds structural tests for the SQL assets in this directory.
//
// scripts/supabase_schema_v2.sql and scripts/supabase_verify_v2.sql have the
// largest blast radius in the repo (they build and wipe the production center)
// and had no test coverage at all. Every check below corresponds to a defect
// that actually occurred, or to a promise the scripts make in their own header
// comments.
package scripts

import (
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gateway/internal/bundle"
)

const (
	schemaFile = "supabase_schema_v2.sql"
	verifyFile = "supabase_verify_v2.sql"
)

func read(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// stripSQLComments removes line comments and dollar-quoted bodies so keyword
// scans don't trip over prose. Dollar-quoted bodies are blanked rather than
// deleted to keep byte offsets stable (not needed, but keeps the output sane).
var (
	lineCommentRe  = regexp.MustCompile(`--[^\n]*`)
	blockCommentRe = regexp.MustCompile(`(?s)/\*.*?\*/`)
	dollarQuoteRe  = regexp.MustCompile(`(?s)\$\$.*?\$\$`)
)

func stripSQLComments(src string) string {
	src = blockCommentRe.ReplaceAllString(src, " ")
	src = dollarQuoteRe.ReplaceAllString(src, " $$ ")
	src = lineCommentRe.ReplaceAllString(src, " ")
	return src
}

// ---------------------------------------------------------------------------
// DDL: 执行顺序
// ---------------------------------------------------------------------------

// Regression: 第 4 节曾在 CREATE TABLE config_meta 之前就 UPDATE config_meta。
// 全新 Supabase 项目上那句直接报 relation "config_meta" does not exist；
// SQL Editor 遇错中止会让 bump_version()、6 个触发器、get_version()、
// get_bundle() 全部不建 —— 中心有表但没有版本号和拉取 RPC，完全不可用。
// 而这正是两份用户文档推荐的首次部署路径。
func TestSchemaV2CreatesConfigMetaBeforeUpdatingIt(t *testing.T) {
	src := stripSQLComments(read(t, schemaFile))

	createAt := strings.Index(src, "CREATE TABLE IF NOT EXISTS config_meta")
	if createAt < 0 {
		t.Fatal("config_meta is never created; the version counter is mandatory")
	}
	// Every statement that mutates config_meta, not just the first.
	for _, m := range regexp.MustCompile(`UPDATE\s+config_meta`).FindAllStringIndex(src, -1) {
		if m[0] < createAt {
			t.Errorf("UPDATE config_meta at offset %d precedes its CREATE at %d: "+
				"a fresh database has no config_meta yet, so the script aborts there "+
				"and never creates the triggers or get_bundle()", m[0], createAt)
		}
	}
}

// Regression: 本脚本"可重复运行"只代表不会报错，不代表可以随便重跑。推送成功后
// 误跑第二遍会静默 TRUNCATE 掉刚灌进去的数据。必须有显式闸，且闸必须在
// TRUNCATE 之前生效。
func TestSchemaV2DestructiveStepIsGuardedBeforeTruncate(t *testing.T) {
	raw := read(t, schemaFile)
	truncateAt := strings.Index(raw, "TRUNCATE TABLE")
	if truncateAt < 0 {
		t.Fatal("no TRUNCATE found; expected the destructive rebuild step")
	}
	guardAt := strings.Index(raw, "apiGateway.destructive")
	if guardAt < 0 {
		t.Fatal("TRUNCATE is unguarded: re-running the script after a successful " +
			"push would silently wipe the center. Add a current_setting() gate.")
	}
	if guardAt > truncateAt {
		t.Error("the destructive gate appears after the TRUNCATE, so it cannot prevent it")
	}
	if !strings.Contains(raw, "RAISE EXCEPTION") {
		t.Error("gate does not RAISE; it should abort the script rather than warn")
	}
}

// ---------------------------------------------------------------------------
// DDL: 表集合
// ---------------------------------------------------------------------------

var v2Tables = []string{
	"platform", "credential", "rapi", "endpoint_credential", "lapi", "lapi_rapi_order",
}

func TestSchemaV2CreatesEveryContractTable(t *testing.T) {
	src := stripSQLComments(read(t, schemaFile))
	for _, tbl := range v2Tables {
		if !strings.Contains(src, "CREATE TABLE IF NOT EXISTS "+tbl+" ") &&
			!strings.Contains(src, "CREATE TABLE IF NOT EXISTS "+tbl+"(") {
			t.Errorf("DDL does not create %q", tbl)
		}
	}
}

// v1 的 platform_keys 必须被显式删掉：留着它，Go 侧（已适配 v2）不会读它，
// 但它会让人误以为迁移没做完。
func TestSchemaV2DropsLegacyPlatformKeys(t *testing.T) {
	raw := read(t, schemaFile)
	if !strings.Contains(raw, "DROP TABLE IF EXISTS platform_keys") {
		t.Error("DDL never drops platform_keys; the v1 table would linger")
	}
	// 连残留触发器也要先摘掉，否则 DROP TABLE 会被依赖挡住。
	if !strings.Contains(raw, "trg_bump_platform_keys") {
		t.Error("DDL does not mention trg_bump_platform_keys; the v1 trigger must be " +
			"dropped before the table or DROP TABLE ... CASCADE silently takes the " +
			"new schema's objects with it")
	}
}

// ---------------------------------------------------------------------------
// DDL ↔ Go 契约
// ---------------------------------------------------------------------------

// get_bundle 产出的每个键都必须能被 bundle 侧的 json tag 接住。Go 侧 Parse
// 开了 DisallowUnknownFields：多一个键整包解析失败，少一个键静默变零值。
// 两边任何一侧漂移都在这里炸掉，而不是等到代理拉不到配置。
func TestSchemaV2BundleContractMatchesGoStructs(t *testing.T) {
	raw := read(t, schemaFile)

	// Go 侧允许的键集合。
	allowed := map[string]bool{}
	addFields := func(v any) {
		tp := reflect.TypeOf(v)
		for i := 0; i < tp.NumField(); i++ {
			tag := tp.Field(i).Tag.Get("json")
			name := strings.Split(tag, ",")[0]
			if name == "" || name == "-" {
				continue
			}
			allowed[name] = true
		}
	}
	addFields(bundle.Envelope{})
	addFields(bundle.Bundle{})
	addFields(bundle.Platform{})
	addFields(bundle.Credential{})
	addFields(bundle.RAPI{})
	addFields(bundle.LAPI{})
	addFields(bundle.LAPIRapiOrder{})
	addFields(bundle.CredentialBinding{})

	// SQL 侧 jsonb_build_object 的键，形如 'foo', <expr>
	body := raw
	if i := strings.Index(body, "CREATE OR REPLACE FUNCTION get_bundle"); i >= 0 {
		body = body[i:]
	} else {
		t.Fatal("get_bundle function not found in the DDL")
	}
	keyRe := regexp.MustCompile(`'([a-z0-9_]+)'\s*,`)
	seen := map[string]bool{}
	for _, m := range keyRe.FindAllStringSubmatch(body, -1) {
		key := m[1]
		if seen[key] {
			continue
		}
		seen[key] = true
		if !allowed[key] {
			t.Errorf("get_bundle emits key %q which no bundle struct declares; "+
				"DisallowUnknownFields will reject the whole payload", key)
		}
	}

	// 反向：Go 契约里的每个键都必须在 SQL 里出现，否则代理会拿到零值。
	for key := range allowed {
		if !strings.Contains(body, "'"+key+"'") {
			t.Errorf("bundle struct declares %q but get_bundle never emits it; "+
				"agents will read it as a zero value", key)
		}
	}
}

// credentials 必须按 (base_url, sort_order) 排序发出 —— 代理侧 credential.sort_order
// 就是轮换顺序，顺序错了等于轮询顺序错了。
func TestSchemaV2OrdersCredentialsForRotation(t *testing.T) {
	raw := read(t, schemaFile)
	credIdx := strings.Index(raw, "'credentials', COALESCE(")
	if credIdx < 0 {
		t.Fatal("get_bundle does not emit a credentials array")
	}
	seg := raw[credIdx:]
	if end := strings.Index(seg, "'rapis', COALESCE("); end > 0 {
		seg = seg[:end]
	}
	if !strings.Contains(seg, "ORDER BY p.base_url, c.sort_order") {
		t.Errorf("credentials array is not ordered by (base_url, sort_order); " +
			"rotation order would be whatever Postgres happens to return")
	}
}

// ---------------------------------------------------------------------------
// 校验脚本：只读 + 与 DDL 一致
// ---------------------------------------------------------------------------

var mutatingKeywords = []string{
	"INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER", "DROP",
	"CREATE", "GRANT", "REVOKE", "DO", "CALL",
}

// 脚本自己的头注释承诺"只含 SELECT，不修改任何数据"。这条承诺没有机器可验，
// 于是就没有真正的约束 —— 而这是唯一一个要在生产中心上跑的脚本。
func TestVerifyScriptIsReadOnly(t *testing.T) {
	src := stripSQLComments(read(t, verifyFile))
	// 逐词扫描，避免把列名/别名里的子串误判成关键字。
	wordRe := regexp.MustCompile(`[A-Za-z_]+`)
	for _, kw := range mutatingKeywords {
		for _, m := range wordRe.FindAllString(src, -1) {
			if strings.EqualFold(m, kw) {
				t.Errorf("verify script contains %s — it must stay read-only; it is "+
					"the only script run against the live center after a rebuild", kw)
			}
		}
	}
	if n := strings.Count(src, "SELECT"); n < 5 {
		t.Errorf("verify script has only %d SELECTs; expected the full check suite", n)
	}
}

// 校验脚本引用的每一张表都必须在 DDL 里被创建，否则重建后校验会自己报错。
func TestVerifyScriptOnlyQueriesTablesTheSchemaCreates(t *testing.T) {
	schema := stripSQLComments(read(t, schemaFile))
	verify := stripSQLComments(read(t, verifyFile))

	// 从 DDL 里抽出被创建的表（而不是对着硬编码清单），这样新增表时这个
	// 测试仍然有效。
	created := map[string]bool{}
	createRe := regexp.MustCompile(`CREATE TABLE (?:IF NOT EXISTS )?([a-z_][a-z0-9_]*)`)
	for _, m := range createRe.FindAllStringSubmatch(schema, -1) {
		created[strings.ToLower(m[1])] = true
	}
	if len(created) == 0 {
		t.Fatal("no CREATE TABLE found in the DDL")
	}
	// Postgres 自带目录表，不在本脚本里创建。
	for _, sys := range []string{"pg_tables", "pg_indexes", "pg_class"} {
		created[sys] = true
	}

	refRe := regexp.MustCompile(`(?i)\b(?:from|join)\s+([a-z_][a-z0-9_]*)`)
	for _, m := range refRe.FindAllStringSubmatch(verify, -1) {
		tbl := strings.ToLower(m[1])
		if !created[tbl] {
			t.Errorf("verify script queries %q, which the v2 schema never creates "+
				"(schema creates: %v)", tbl, keysOf(created))
		}
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sortStrings(out)
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// 跨平台绑定在 v2 是 schema 层面允许的（credential.platform_id 只是归属平台、
// 不是身份），但会让 PickAvailableKey 的游标记到错误的平台上。v1 结构上不可能
// 出现，所以这是一条 v2 新增的、必须显式检查的不变量。
func TestVerifyScriptChecksCrossPlatformBindings(t *testing.T) {
	src := stripSQLComments(read(t, verifyFile))
	if !strings.Contains(src, "binding_cross_platform") {
		t.Error("verify script has no cross-platform binding check; a binding between " +
			"a rapi and another platform's credential silently mis-attributes the " +
			"rotation cursor")
	}
	if !strings.Contains(src, "c.platform_id <> r.platform_id") {
		t.Error("cross-platform check does not compare credential.platform_id to " +
			"rapi.platform_id")
	}
}

// 契约冒烟必须断言 v1 字段已消失：key_ids / platform_keys 一旦泄漏进 bundle，
// 代理侧 Parse 直接拒收整包。
func TestVerifyScriptRejectsV1ContractFields(t *testing.T) {
	src := stripSQLComments(read(t, verifyFile))
	for _, f := range []string{"key_ids", "platform_keys"} {
		if !strings.Contains(src, f) {
			t.Errorf("verify script no longer counts %q occurrences in the bundle; "+
				"that regression would ship silently", f)
		}
	}
}

// 期望行数写在脚本头注释里，校验脚本第 1 节也要能对上 —— 两处数字漂移会让
// 运维对着错的基线判断成败。
func TestVerifyScriptExpectedCountsAreSixTables(t *testing.T) {
	verify := stripSQLComments(read(t, verifyFile))
	sec := verify
	if i := strings.Index(verify, "-- 1."); i >= 0 {
		if j := strings.Index(verify[i:], "-- 2."); j > 0 {
			sec = verify[i : i+j]
		}
	}
	for _, tbl := range v2Tables {
		if !strings.Contains(sec, "FROM "+tbl) &&
			!strings.Contains(sec, "from "+tbl) {
			t.Errorf("section 1 (row counts) does not count %q", tbl)
		}
	}
	// 头部注释里的 6 个期望值应当都是合法数字。
	raw := read(t, verifyFile)
	for _, n := range regexp.MustCompile(`\b(\d+)\b`).FindAllStringSubmatch(raw, -1) {
		if _, err := strconv.Atoi(n[1]); err != nil {
			t.Errorf("header contains a non-numeric expected count %q", n[1])
		}
	}
}
