// centerinit 是一次性迁移工具：初始化 Supabase 中心库，并把本地 gateway.db
// 的定义类配置（platform / platform_keys / rapi / lapi / lapi_rapi_order）整体上传。
//
// 设计：docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md
//
// 用法：
//
//		go run ./cmd/centerinit -ref <项目ref> -token <PAT> [-db gateway.db] [-center-key <hex>] [-plain] [-dry-run]
//
//	  - ref        Supabase 项目 ref（xxxxx.supabase.co 里的 xxxxx）
//	  - token      Supabase Personal Access Token（账号 Settings → Access Tokens；
//	               也可用环境变量 SUPABASE_ACCESS_TOKEN 传入，避免进 shell 历史）
//	  - db         本地 SQLite 路径（默认 gateway.db），只读 5 张定义表，
//	               健康/遥测（metrics/logs 等）按设计留本地、不上传
//	  - center-key 32 字节 hex；不给则自动生成并在结束时打印（要填进各代理
//	               proxy.cfg 的 [sync].center_key）。-plain 则明文上传
//	  - dry-run    只读本地 + 生成 SQL 打印，不调 API
//
// token 边界转换：本地密文（~/.apiGateway.key AES-GCM）→ 解出明文 → 用
// center_key 重新加密后上传（与代理 Apply 的解密路径互逆）。
// rapi.key_ids 本地存 platform_keys.id CSV，上传时换算成"平台内 key_index
// CSV"（业务键），中心与代理按业务键解析。id 原样保留（含自增），引用不换算。
package main

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"gateway/internal/crypto"

	_ "modernc.org/sqlite"
)

const managementAPI = "https://api.supabase.com/v1/projects/%s/database/query"

// sqliteConn 只用到 Query（读本地定义表 + PRAGMA）。
type sqliteConn interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func openSQLite(dbPath string) (*sql.DB, error) {
	conn, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping %s: %w", dbPath, err)
	}
	return conn, nil
}

func main() {
	var (
		ref       = flag.String("ref", os.Getenv("SUPABASE_PROJECT_REF"), "Supabase 项目 ref")
		token     = flag.String("token", os.Getenv("SUPABASE_ACCESS_TOKEN"), "Supabase Personal Access Token")
		dbPath    = flag.String("db", "gateway.db", "本地 SQLite 路径")
		schemaP   = flag.String("schema", "scripts/supabase_schema.sql", "中心 schema SQL 文件")
		centerKey = flag.String("center-key", "", "32 字节 hex 中心密钥（不给则自动生成）")
		plain     = flag.Bool("plain", false, "token 明文上传（不加密，仅测试用）")
		dryRun    = flag.Bool("dry-run", false, "只读本地并打印 SQL，不调 API")
	)
	flag.Parse()

	if *ref == "" {
		fatal("缺少 -ref（Supabase 项目 ref）")
	}
	if !*dryRun && *token == "" {
		fatal("缺少 -token（或环境变量 SUPABASE_ACCESS_TOKEN）；-dry-run 可免 token")
	}

	// 1) 本地解密用 key（gateway.db 里的 token 是 ~/.apiGateway.key 密文）
	if err := crypto.Init(); err != nil {
		fatal("本地 crypto init: %v", err)
	}

	// 2) 中心密钥：自动生成 / 用户给 / 明文模式
	var ck []byte
	switch {
	case *plain:
		fmt.Println("== 明文上传模式（-plain）：中心 token 不加密 ==")
	case *centerKey != "":
		k, err := crypto.ParseKey(*centerKey)
		if err != nil {
			fatal("center-key: %v", err)
		}
		ck = k
		fmt.Println("== 使用指定 center_key ==")
	default:
		k, err := crypto.ParseKey(randomHexKey())
		if err != nil {
			fatal("generate center key: %v", err)
		}
		ck = k
		fmt.Println("== 已自动生成 center_key（结束时打印，务必填进各代理 proxy.cfg）==")
	}

	// 3) 读本地定义表
	local, err := readLocal(*dbPath)
	if err != nil {
		fatal("读本地库: %v", err)
	}
	local.printSummary()

	// 4) 转换：token 重加密 + key_ids 换 key_index
	rows := convert(local, ck, *plain)

	// 5) 生成 SQL
	schemaSQL, err := os.ReadFile(*schemaP)
	if err != nil {
		fatal("读 schema: %v", err)
	}
	schemaStmts := splitSQLStatements(string(schemaSQL))
	dataStmts := generateDataSQL(rows)

	fmt.Printf("\n== 计划 ==\nschema 语句: %d 条\ndata 语句: %d 条\n", len(schemaStmts), len(dataStmts))
	if *dryRun {
		fmt.Println("\n== DRY-RUN：data SQL 如下（未调用 API）==")
		for _, s := range dataStmts {
			fmt.Println(s)
		}
		finish(ck, rows)
		return
	}

	// 6) 调 API：先 schema，再数据，再校验
	fmt.Println("\n== 初始化 schema ==")
	for i, s := range schemaStmts {
		if err := runQuery(*ref, *token, s); err != nil {
			fatal("schema 语句 #%d 失败: %v\n语句: %s", i+1, err, firstLine(s))
		}
	}
	fmt.Println("OK")

	fmt.Println("\n== 上传定义数据 ==")
	for i, s := range dataStmts {
		if err := runQuery(*ref, *token, s); err != nil {
			fatal("data 语句 #%d 失败: %v\n语句: %s", i+1, err, firstLine(s))
		}
	}
	fmt.Println("OK")

	fmt.Println("\n== 校验中心 ==")
	checkCounts(*ref, *token, rows)
	finish(ck, rows)
}

// ---------------------------------------------------------------------------
// 本地读取
// ---------------------------------------------------------------------------

type localTables struct {
	platforms []map[string]string // 列名 → 文本值（NULL=缺失）
	keys      []map[string]string
	rapis     []map[string]string
	lapis     []map[string]string
	orders    []map[string]string
}

func (lt *localTables) printSummary() {
	fmt.Printf("\n== 本地定义表 ==\nplatform: %d\nplatform_keys: %d\nrapi: %d\nlapi: %d\nlapi_rapi_order: %d\n",
		len(lt.platforms), len(lt.keys), len(lt.rapis), len(lt.lapis), len(lt.orders))
}

// readLocal 读 5 张定义表。健康/遥测表（metrics/logs/trends/blocks/cache）不上传。
func readLocal(dbPath string) (*localTables, error) {
	conn, err := openSQLite(dbPath)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	lt := &localTables{}
	want := map[string][]string{
		"platform": {"id", "name", "base_url", "token", "last_token_fetch", "enabled",
			"notes", "supported_formats", "format_endpoints", "custom_headers",
			"billing_address", "login_account", "login_password", "sort_order",
			"created_at", "updated_at"},
		"platform_keys": {"id", "platform_id", "key_index", "token", "label", "enabled",
			"expires_at", "is_free", "created_at", "updated_at"},
		"rapi": {"id", "platform_id", "alias", "model", "enabled", "base_cost", "high_cost",
			"rpm_limit", "rph_limit", "rpd_limit", "tpm_limit", "tph_limit", "tpd_limit",
			"time_period_rules", "supported_formats", "custom_headers", "key_ids", "source",
			"vendor", "series", "model_name", "version", "suffix", "notes", "sort_order",
			"created_at", "updated_at"},
		"lapi": {"id", "alias", "notes", "enabled", "vendor", "series", "model_name",
			"version", "suffix", "created_at", "updated_at"},
		"lapi_rapi_order": {"id", "lapi_id", "rapi_id", "order_index"},
	}
	targets := map[string]*[]map[string]string{
		"platform": &lt.platforms, "platform_keys": &lt.keys, "rapi": &lt.rapis,
		"lapi": &lt.lapis, "lapi_rapi_order": &lt.orders,
	}
	for table, dst := range targets {
		cols, err := tableColumns(conn, table)
		if err != nil {
			return nil, err
		}
		if len(cols) == 0 {
			fmt.Printf("[WARN] 本地缺表 %s，跳过\n", table)
			continue
		}
		// 本地列与中心列取交集；lapi.updated_at 老库可能没有
		var sel []string
		for _, c := range want[table] {
			if cols[c] {
				sel = append(sel, c)
			}
		}
		query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(sel, ","), table)
		rrows, err := conn.Query(query)
		if err != nil {
			return nil, fmt.Errorf("select %s: %w", table, err)
		}
		for rrows.Next() {
			vals := make([]sqlNullString, len(sel))
			ptrs := make([]any, len(sel))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err = rrows.Scan(ptrs...); err != nil {
				rrows.Close()
				return nil, fmt.Errorf("scan %s: %w", table, err)
			}
			row := map[string]string{}
			for i, c := range sel {
				row[c] = vals[i].v
			}
			// 老库没有 updated_at 的表用 created_at 顶上
			if !cols["updated_at"] && cols["created_at"] {
				row["updated_at"] = row["created_at"]
			}
			*dst = append(*dst, row)
		}
		rrows.Close()
	}
	return lt, nil
}

type sqlNullString struct{ v string } // 空串表示 SQL NULL

func (n *sqlNullString) Scan(src any) error {
	switch x := src.(type) {
	case nil:
		n.v = ""
	case []byte:
		n.v = string(x)
	case string:
		n.v = x
	case int64:
		n.v = strconv.FormatInt(x, 10)
	case float64:
		n.v = strconv.FormatFloat(x, 'f', -1, 64)
	default:
		n.v = fmt.Sprintf("%v", x)
	}
	return nil
}

func tableColumns(conn sqliteConn, table string) (map[string]bool, error) {
	rows, err := conn.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var (
			cid       int
			name, typ string
			notNull   int
			dflt      any
			pk        int
		)
		if err = rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

// ---------------------------------------------------------------------------
// 转换与加密
// ---------------------------------------------------------------------------

type converted struct {
	platforms []map[string]string
	keys      []map[string]string
	rapis     []map[string]string
	lapis     []map[string]string
	orders    []map[string]string
	centerKey bool // 是否用 center_key 加密
}

// convert 做 token 边界重加密（本地 key → center_key）+ key_ids 换业务键。
func convert(lt *localTables, ck []byte, plain bool) *converted {
	out := &converted{centerKey: len(ck) > 0}

	// key_id → key_index（用于 rapi.key_ids 换算）
	keyIdxByID := map[string]string{}
	for _, k := range lt.keys {
		keyIdxByID[k["id"]] = k["key_index"]
	}

	reenc := func(cipher string, label string) string {
		if cipher == "" {
			return ""
		}
		p, err := crypto.Decrypt(cipher) // 本地 key 解密；明文（legacy）原样返回
		if err != nil {
			fatal("%s 本地解密失败: %v", label, err)
		}
		if plain || len(ck) == 0 {
			return p // 明文上传
		}
		enc, err := crypto.EncryptWithKey(p, ck)
		if err != nil {
			fatal("%s 中心加密失败: %v", label, err)
		}
		return enc
	}

	for _, p := range lt.platforms {
		p["token"] = reenc(p["token"], "platform.token("+p["name"]+")")
		p["login_password"] = reenc(p["login_password"], "platform.login_password("+p["name"]+")")
		out.platforms = append(out.platforms, p)
	}
	for _, k := range lt.keys {
		k["token"] = reenc(k["token"], "platform_keys.token("+k["platform_id"]+"/"+k["key_index"]+")")
		out.keys = append(out.keys, k)
	}
	for _, r := range lt.rapis {
		out.rapis = append(out.rapis, convertKeyIDs(r, keyIdxByID))
	}
	out.lapis = lt.lapis
	out.orders = lt.orders
	return out
}

// convertKeyIDs 把 rapi.key_ids（platform_keys.id CSV）换算成 key_index CSV。
// 本地悬空 id 丢弃并告警（中心唯一约束也不接受悬空）。
func convertKeyIDs(r map[string]string, keyIdxByID map[string]string) map[string]string {
	raw := strings.TrimSpace(r["key_ids"])
	if raw == "" {
		return r
	}
	var keep []string
	for _, id := range strings.Split(raw, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		idx, ok := keyIdxByID[id]
		if !ok {
			fmt.Printf("[WARN] rapi %s 的 key_ids 引用了不存在的 key id %s，已丢弃\n", r["alias"], id)
			continue
		}
		keep = append(keep, idx)
	}
	r["key_ids"] = strings.Join(keep, ",")
	return r
}

func randomHexKey() string {
	// crypto.ParseKey 需要 64 个 hex 字符（32 字节）
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		fatal("generate center key: %v", err)
	}
	return hex.EncodeToString(b)
}

// ---------------------------------------------------------------------------
// SQL 生成
// ---------------------------------------------------------------------------

// 中心列（definition-only）与插入顺序。
var (
	platCols  = []string{"id", "name", "base_url", "token", "last_token_fetch", "enabled", "notes", "supported_formats", "format_endpoints", "custom_headers", "billing_address", "login_account", "login_password", "sort_order", "created_at", "updated_at"}
	keyCols   = []string{"id", "platform_id", "key_index", "token", "label", "enabled", "expires_at", "is_free", "created_at", "updated_at"}
	rapiCols  = []string{"id", "platform_id", "alias", "model", "enabled", "base_cost", "high_cost", "rpm_limit", "rph_limit", "rpd_limit", "tpm_limit", "tph_limit", "tpd_limit", "time_period_rules", "supported_formats", "custom_headers", "key_ids", "source", "vendor", "series", "model_name", "version", "suffix", "notes", "sort_order", "created_at", "updated_at"}
	lapiCols  = []string{"id", "alias", "notes", "enabled", "vendor", "series", "model_name", "version", "suffix", "created_at", "updated_at"}
	orderCols = []string{"id", "lapi_id", "rapi_id", "order_index"}
)

// generateDataSQL 生成幂等的数据上传语句：清空（子→父）→ 多行 INSERT（保留 id）→ setval。
func generateDataSQL(rows *converted) []string {
	var out []string
	out = append(out,
		`DELETE FROM lapi_rapi_order;`,
		`DELETE FROM rapi;`,
		`DELETE FROM platform_keys;`,
		`DELETE FROM lapi;`,
		`DELETE FROM platform;`,
	)
	insert := func(table string, cols []string, src []map[string]string) {
		if len(src) == 0 {
			return
		}
		for start := 0; start < len(src); start += 200 { // 分批，控制单条语句大小
			end := min(start+200, len(src))
			var b strings.Builder
			fmt.Fprintf(&b, "INSERT INTO %s (%s) VALUES ", table, strings.Join(cols, ","))
			for i, row := range src[start:end] {
				if i > 0 {
					b.WriteString(",")
				}
				b.WriteString("(")
				for j, c := range cols {
					if j > 0 {
						b.WriteString(",")
					}
					b.WriteString(sqlValue(c, row[c]))
				}
				b.WriteString(")")
			}
			b.WriteString(";")
			out = append(out, b.String())
		}
	}
	insert("platform", platCols, rows.platforms)
	insert("platform_keys", keyCols, rows.keys)
	insert("rapi", rapiCols, rows.rapis)
	insert("lapi", lapiCols, rows.lapis)
	insert("lapi_rapi_order", orderCols, rows.orders)

	for _, t := range []string{"platform", "platform_keys", "rapi", "lapi", "lapi_rapi_order"} {
		out = append(out, fmt.Sprintf(
			`SELECT setval(pg_get_serial_sequence('%s','id'), COALESCE((SELECT MAX(id) FROM %s), 1));`, t, t))
	}
	return out
}

// 列类型（按列名，不按值嗅探——本地 TEXT 列里存着 '4.7'、前导零等，嗅探会错）。
var (
	intColsSet = map[string]bool{
		"id": true, "platform_id": true, "key_index": true, "order_index": true,
		"base_cost": true, "high_cost": true,
		"rpm_limit": true, "rph_limit": true, "rpd_limit": true,
		"tpm_limit": true, "tph_limit": true, "tpd_limit": true,
		"sort_order": true,
	}
	boolColsSet = map[string]bool{"enabled": true, "is_free": true}
	timeColsSet = map[string]bool{"last_token_fetch": true, "expires_at": true, "created_at": true, "updated_at": true}
)

var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999 -0700 MST", // Go time.Time.String()（旧驱动写入）
	"2006-01-02 15:04:05.999999999-07:00",     // modernc _time_format=sqlite 写入
	"2006-01-02T15:04:05.999999999Z07:00",     // RFC3339Nano
	"2006-01-02 15:04:05",                     // SQLite CURRENT_TIMESTAMP（UTC）
	"2006-01-02",
}

// normalizeTime 把本地库里的多种时间文本统一成 PostgreSQL 可解析的 RFC3339。
func normalizeTime(v string) string {
	s := strings.TrimSpace(v)
	if s == "" {
		return ""
	}
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().Format("2006-01-02T15:04:05.999999Z07:00")
		}
	}
	return s // 认不出的原样透传，让 PG 报错可见而不是静默吞掉
}

// sqlValue 按列类型生成字面量：
//   - int：原样（空→NULL）
//   - bool：0/1 → false/true
//   - time：归一化 RFC3339（空→NULL，列可空）
//   - text：加引号转义（空→”，满足中心 NOT NULL DEFAULT ”）
func sqlValue(col, v string) string {
	switch {
	case intColsSet[col]:
		if v == "" {
			return "NULL"
		}
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return "'" + esc(v) + "'" // 非法数字退化为字符串，让 PG 报错可见
		}
		return v
	case boolColsSet[col]:
		if v == "1" || v == "true" {
			return "true"
		}
		return "false"
	case timeColsSet[col]:
		if strings.TrimSpace(v) == "" {
			return "NULL"
		}
		return "'" + esc(normalizeTime(v)) + "'"
	default:
		return "'" + esc(v) + "'"
	}
}

func esc(v string) string {
	return strings.ReplaceAll(v, "'", "''")
}

// splitSQLStatements 按 ; 切语句，正确跳过 '...' 字符串、$$...$$ 函数体、
// -- 行注释、/* */ 块注释。
func splitSQLStatements(src string) []string {
	var (
		out          []string
		cur          strings.Builder
		inS, inDl    bool
		inLine, inBl bool
	)
	for i := 0; i < len(src); i++ {
		c := src[i]
		switch {
		case inLine:
			cur.WriteByte(c)
			if c == '\n' {
				inLine = false
			}
		case inBl:
			cur.WriteByte(c)
			if c == '*' && i+1 < len(src) && src[i+1] == '/' {
				cur.WriteByte('/')
				i++
				inBl = false
			}
		case !inS && !inDl && c == '-' && i+1 < len(src) && src[i+1] == '-':
			inLine = true
			cur.WriteByte(c)
		case !inS && !inDl && c == '/' && i+1 < len(src) && src[i+1] == '*':
			inBl = true
			cur.WriteByte(c)
		case !inS && !inDl && c == '\'' && i+1 < len(src) && src[i+1] == '\'':
			cur.WriteString("''")
			i++
		case c == '\'':
			inS = !inS
			cur.WriteByte(c)
		case !inS && c == '$' && i+1 < len(src) && src[i+1] == '$':
			inDl = !inDl
			cur.WriteString("$$")
			i++
		case !inS && !inDl && c == ';':
			stmt := strings.TrimSpace(cur.String())
			if stmt != "" {
				out = append(out, stmt+";")
			}
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if stmt := strings.TrimSpace(cur.String()); stmt != "" {
		out = append(out, stmt+";")
	}
	return out
}

// ---------------------------------------------------------------------------
// Supabase Management API
// ---------------------------------------------------------------------------

func runQuery(ref, token, query string) error {
	payload, _ := json.Marshal(map[string]string{"query": query})
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(managementAPI, ref), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func queryRows(ref, token, query string) ([]map[string]any, error) {
	payload, _ := json.Marshal(map[string]string{"query": query})
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(managementAPI, ref), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
	}
	var rows []map[string]any
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("parse %q: %w", string(body), err)
	}
	return rows, nil
}

func checkCounts(ref, token string, rows *converted) {
	r, err := queryRows(ref, token, `SELECT get_version() AS version,
		(SELECT count(*) FROM platform) AS platforms,
		(SELECT count(*) FROM platform_keys) AS keys,
		(SELECT count(*) FROM rapi) AS rapis,
		(SELECT count(*) FROM lapi) AS lapis,
		(SELECT count(*) FROM lapi_rapi_order) AS orders;`)
	if err != nil {
		fatal("校验查询失败: %v", err)
	}
	if len(r) == 0 {
		fatal("校验查询无返回")
	}
	fmt.Printf("中心 version=%v platforms=%v keys=%v rapis=%v lapis=%v orders=%v\n",
		r[0]["version"], r[0]["platforms"], r[0]["keys"], r[0]["rapis"], r[0]["lapis"], r[0]["orders"])
}

func finish(ck []byte, rows *converted) {
	if len(ck) > 0 {
		fmt.Printf("\n== center_key（填进各代理 proxy.cfg 的 [sync].center_key）==\n%s\n", hex.EncodeToString(ck))
	}
	want := map[string]int{"platform": len(rows.platforms), "platform_keys": len(rows.keys),
		"rapi": len(rows.rapis), "lapi": len(rows.lapis), "lapi_rapi_order": len(rows.orders)}
	_ = want
	fmt.Println("\n完成。下一步：各代理 proxy.cfg 打开 [sync] 段（见 docs/日常操作手册.md §五）。")
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[centerinit] "+format+"\n", args...)
	os.Exit(1)
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 120 {
		s = s[:120] + "…"
	}
	return s
}
