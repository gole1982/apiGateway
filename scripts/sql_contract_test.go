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
	// 单引号字符串字面量。权限校验用 has_table_privilege('anon','platform','UPDATE')
	// 这类调用，字面量里的 'UPDATE'/'INSERT' 是**权限名**而非 SQL 关键字，
	// 不能让只读扫描误判。剥掉字面量（保留分隔）再扫关键字。
	stringLiteralRe = regexp.MustCompile(`'(?:[^']|'')*'`)
)

func stripSQLComments(src string) string {
	src = blockCommentRe.ReplaceAllString(src, " ")
	src = dollarQuoteRe.ReplaceAllString(src, " $$ ")
	src = lineCommentRe.ReplaceAllString(src, " ")
	return src
}

// stripStringLiterals blanks single-quoted string literals (keeping the quotes so
// word boundaries stay intact) so keyword scans ignore privilege names like 'UPDATE'.
func stripStringLiterals(src string) string {
	return stringLiteralRe.ReplaceAllString(src, "''")
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
	src := stripStringLiterals(stripSQLComments(read(t, verifyFile)))
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
	// Postgres 自带目录表，不在本脚本里创建。pg_policies 是 RLS 策略的目录视图
	// （写隔离校验用），同样由 Postgres 提供。
	for _, sys := range []string{"pg_tables", "pg_indexes", "pg_class", "pg_policies"} {
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

// ---------------------------------------------------------------------------
// DDL: 第 7 节写隔离（中心未做写隔离 = publishable key 被误判成管理端）
// ---------------------------------------------------------------------------
//
// 这是整条安全链的地基：角色判定靠"无副作用写探测"，中心不做写隔离时
// publishable key 的 PATCH 会返回 204，探测就会把只读角色判成可写。
// 代码侧已加"意图 vs 事实"交叉校验兜底（prefix 是 publishable 却写成功 →
// 判 proxy 并报错），但那只把误判降级成报错，根治仍在中心侧 DDL。
// 第 7 节一旦被误删或改坏，CI 全绿而生产中心静默失去隔离 —— 故此处逐条锁死。

// 定义表全集 = 6 张契约表 + config_meta（版本号所在，同样只读）。
var writeIsolationTables = []string{
	"platform", "credential", "rapi", "endpoint_credential",
	"lapi", "lapi_rapi_order", "config_meta",
}

var writePrivileges = []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE"}

// section7 抽出第 7 节的 SQL 文本（去掉注释，只留可执行语句），供后续检查。
func section7(t *testing.T) string {
	t.Helper()
	src := stripSQLComments(read(t, schemaFile))
	marker := "ENABLE ROW LEVEL SECURITY"
	start := strings.Index(src, marker)
	if start < 0 {
		t.Fatal("schema has no ENABLE ROW LEVEL SECURITY — section 7 (write isolation) " +
			"is missing; the center would let a publishable key write")
	}
	// 回退到该行行首：第一条语句是 "ALTER TABLE platform ENABLE ..."，
	// 从 marker 起切会把 "ALTER TABLE platform" 丢掉。
	if nl := strings.LastIndexByte(src[:start], '\n'); nl >= 0 {
		start = nl + 1
	}
	// 截到「硬化可选项」为止（其余部分不属于本节）。
	end := strings.Index(src[start:], "ALTER DEFAULT PRIVILEGES")
	if end < 0 {
		end = len(src) - start
	}
	return src[start : start+end]
}

// 每张定义表都要 ENABLE ROW LEVEL SECURITY，且不能用 FORCE ——
// FORCE 会连表 owner / service_role 一起拦，管理端就写不了中心了。
func TestSchemaV2Section7EnablesRLSOnEveryDefinitionTable(t *testing.T) {
	sec := section7(t)
	for _, tbl := range writeIsolationTables {
		want := "ALTER TABLE " + tbl
		if !strings.Contains(sec, want) {
			t.Errorf("section 7 does not ENABLE ROW LEVEL SECURITY on %q", tbl)
			continue
		}
		// 该表的 ALTER 必须落在 ENABLE 之前（同一语句内）。
		i := strings.Index(sec, want)
		tail := sec[i:]
		if j := strings.Index(tail, ";"); j > 0 {
			tail = tail[:j]
		}
		if !strings.Contains(tail, "ENABLE ROW LEVEL SECURITY") {
			t.Errorf("ALTER TABLE %s does not ENABLE ROW LEVEL SECURITY", tbl)
		}
	}
	// 内联的 FORCE（无 ENABLE 前缀）才危险；注释里的说明已被 stripSQLComments 去掉。
	stripped := stripStringLiterals(sec)
	if regexp.MustCompile(`\bFORCE ROW LEVEL SECURITY`).MatchString(stripped) {
		t.Error("section 7 uses FORCE ROW LEVEL SECURITY — it blocks the table owner / " +
			"service_role too, so the management role could no longer write the center")
	}
}

// REVOKE 是 RLS 之外的第二道闸（防策略误加）：anon/authenticated 的四种写权限
// 都要撤，且 service_role 绝不能出现在被撤名单里（它靠 BYPASSRLS 保留全权）。
func TestSchemaV2Section7RevokesWritesFromAnonAndAuthenticated(t *testing.T) {
	sec := stripStringLiterals(section7(t))
	// 找出所有 REVOKE ... ; 语句。
	var revokes []string
	for _, stmt := range strings.Split(sec, ";") {
		if strings.Contains(stmt, "REVOKE") {
			revokes = append(revokes, strings.Join(strings.Fields(stmt), " "))
		}
	}
	if len(revokes) == 0 {
		t.Fatal("section 7 has no REVOKE statement — RLS alone is not enough if a policy " +
			"is ever added by mistake; a publishable key would then be able to write")
	}
	for _, kw := range writePrivileges {
		found := false
		for _, r := range revokes {
			if !strings.Contains(r, kw) {
				continue
			}
			// 只认撤到 anon/authenticated 的那条。
			fi := strings.Index(r, " FROM ")
			if fi < 0 {
				continue
			}
			grantees := r[fi+len(" FROM "):]
			if !strings.Contains(grantees, "anon") || !strings.Contains(grantees, "authenticated") {
				t.Errorf("REVOKE %s does not target both anon and authenticated: %q", kw, r)
				continue
			}
			if strings.Contains(grantees, "service_role") {
				t.Errorf("REVOKE %s also revokes from service_role: %q — the management "+
					"role must keep write access (BYPASSRLS)", kw, r)
			}
			found = true
		}
		if !found {
			t.Errorf("section 7 does not REVOKE %s from anon/authenticated", kw)
		}
	}
}

// RLS 开启后"无策略即拒绝一切"。必须给每张定义表显式放行 SELECT，
// 否则代理端（publishable key）拉取配置会被 403，数据同步直接断掉。
func TestSchemaV2Section7GrantsReadOnlyPolicyForEveryDefinitionTable(t *testing.T) {
	raw := stripSQLComments(section7(t))
	sec := strings.Join(strings.Fields(raw), " ")
	for _, tbl := range writeIsolationTables {
		want := "CREATE POLICY read_defs ON " + tbl + " FOR SELECT TO anon, authenticated"
		if !strings.Contains(sec, want) {
			t.Errorf("section 7 does not create the read-only policy on %q; with RLS "+
				"enabled and no policy, SELECT is denied and the proxy cannot pull "+
				"config (expected: %q)", tbl, want)
		}
	}
	// DROP + CREATE 才是可重入的：缺 DROP，第二次执行会因策略已存在而报错，
	// SQL Editor 遇错中止 —— 后面的 GRANT/RPC 全部不执行。
	if n := strings.Count(sec, "DROP POLICY IF EXISTS read_defs ON"); n < len(writeIsolationTables) {
		t.Errorf("section 7 has %d DROP POLICY IF EXISTS read_defs, want >= %d — without it "+
			"the script is not re-runnable", n, len(writeIsolationTables))
	}
}

// 写权限撤掉后，读 + RPC 必须还在：get_bundle/get_version 是 SECURITY INVOKER，
// 需要调用者的 SELECT 权限；缺这两个 GRANT，代理端同步全断。
func TestSchemaV2Section7KeepsReadAndRPCForAnon(t *testing.T) {
	sec := strings.Join(strings.Fields(stripSQLComments(section7(t))), " ")
	if !regexp.MustCompile(`GRANT SELECT ON .* TO anon, authenticated`).MatchString(sec) {
		t.Error("section 7 does not GRANT SELECT to anon/authenticated; read access is gone")
	}
	for _, fn := range []string{"get_version()", "get_bundle(BIGINT)"} {
		want := "GRANT EXECUTE ON FUNCTION " + fn + " TO anon, authenticated"
		if !strings.Contains(sec, want) {
			t.Errorf("section 7 does not grant EXECUTE on %s to anon/authenticated; the "+
				"proxy cannot fetch the bundle", fn)
		}
	}
}
