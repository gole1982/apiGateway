package db

import (
	"fmt"
	"sort"
	"time"
)

// HourBucket represents one hour's aggregated traffic across all or one LAPI.
type HourBucket struct {
	Hour         int    `json:"hour"` // 0-23
	LapiAlias    string `json:"lapi_alias"`
	RequestCount int    `json:"request_count"`
	TokenCount   int    `json:"token_count"`
	ErrorCount   int    `json:"error_count"` // response_status >= 400
	Fail429      int    `json:"fail_429"`
	AvgLatencyMs int    `json:"avg_latency_ms"`
}

// GetHourlyDistribution aggregates request_logs into 24 hourly buckets,
// optionally scoped to the last N days. Returns one row per (hour, lapi_alias).
func (db *DB) GetHourlyDistribution(days int) ([]HourBucket, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	since := time.Now().AddDate(0, 0, -days)
	rows, err := db.conn.Query(`
		SELECT CAST(strftime('%H', timestamp) AS INTEGER) as hour,
			lapi_alias,
			COUNT(*) as req_count,
			COALESCE(SUM(tokens_used), 0) as tok_count,
			SUM(CASE WHEN response_status >= 400 THEN 1 ELSE 0 END) as err_count,
			SUM(CASE WHEN response_status = 429 THEN 1 ELSE 0 END) as fail429,
			CAST(COALESCE(AVG(latency_ms), 0) AS INTEGER) as avg_lat
		FROM request_logs
		WHERE timestamp >= ?
		  -- Old rows (written before _time_format=sqlite) carry time.Time.String()
		  -- text that strftime() cannot parse; exclude them so the aggregate never
		  -- 500s on a NULL hour. New rows parse fine.
		  AND strftime('%s', timestamp) IS NOT NULL
		GROUP BY hour, lapi_alias
		ORDER BY hour, req_count DESC
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buckets []HourBucket
	for rows.Next() {
		var b HourBucket
		if err := rows.Scan(&b.Hour, &b.LapiAlias, &b.RequestCount, &b.TokenCount, &b.ErrorCount, &b.Fail429, &b.AvgLatencyMs); err != nil {
			return nil, err
		}
		buckets = append(buckets, b)
	}
	return buckets, nil
}

// FallbackStats holds fallback/retry statistics from request_logs.
type FallbackStats struct {
	LapiAlias     string  `json:"lapi_alias"`
	TotalRequests int     `json:"total_requests"`
	FallbackCount int     `json:"fallback_count"`
	RetryCount    int     `json:"retry_count"`   // sum of retry_count
	FallbackRate  float64 `json:"fallback_rate"` // fallback_count / total * 100
}

// GetFallbackStats aggregates fallback usage from request_logs within the last N hours.
// hours=0 means all time.
func (db *DB) GetFallbackStats(hours int) ([]FallbackStats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	query := `
		SELECT lapi_alias,
			COUNT(*) as total,
			SUM(CASE WHEN fallback_used = 1 THEN 1 ELSE 0 END) as fb_count,
			COALESCE(SUM(retry_count), 0) as retry_sum
		FROM request_logs
	`
	args := []interface{}{}
	if hours > 0 {
		since := time.Now().Add(-time.Duration(hours) * time.Hour)
		query += " WHERE timestamp >= ?"
		args = append(args, since)
	}
	query += " GROUP BY lapi_alias ORDER BY total DESC"

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []FallbackStats
	for rows.Next() {
		var s FallbackStats
		if err := rows.Scan(&s.LapiAlias, &s.TotalRequests, &s.FallbackCount, &s.RetryCount); err != nil {
			return nil, err
		}
		if s.TotalRequests > 0 {
			s.FallbackRate = float64(s.FallbackCount) / float64(s.TotalRequests) * 100
		}
		stats = append(stats, s)
	}
	return stats, nil
}

// DailyTrend holds one day's aggregated token and request counts.
type DailyTrend struct {
	Date         string `json:"date"` // YYYY-MM-DD
	LapiAlias    string `json:"lapi_alias"`
	RequestCount int    `json:"request_count"`
	TokenCount   int    `json:"token_count"`
}

// GetDailyTokenTrend aggregates request_logs by day and LAPI for the last N days.
func (db *DB) GetDailyTokenTrend(days int) ([]DailyTrend, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	since := time.Now().AddDate(0, 0, -days)
	rows, err := db.conn.Query(`
		SELECT strftime('%Y-%m-%d', timestamp) as day,
			lapi_alias,
			COUNT(*) as req_count,
			COALESCE(SUM(tokens_used), 0) as tok_count
		FROM request_logs
		WHERE timestamp >= ?
		  AND strftime('%s', timestamp) IS NOT NULL
		GROUP BY day, lapi_alias
		ORDER BY day, tok_count DESC
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var trends []DailyTrend
	for rows.Next() {
		var t DailyTrend
		if err := rows.Scan(&t.Date, &t.LapiAlias, &t.RequestCount, &t.TokenCount); err != nil {
			return nil, err
		}
		trends = append(trends, t)
	}
	return trends, nil
}

// TrafficTotals 是 request_logs 的全量累计聚合，供 /metrics 暴露 counter。
//
// 为什么放在 db 层而不是 service 层现拼：/metrics 每次抓取都要跑这条聚合，
// 而它是全表扫描。让 DB 层出一个明确的、可被单测覆盖的函数，比让 service
// 层内联 SQL 更可控。
type TrafficTotals struct {
	TotalRequests   int64 `json:"total_requests"`
	SuccessRequests int64 `json:"success_requests"`
	FailedRequests  int64 `json:"failed_requests"`
	InputTokens     int64 `json:"input_tokens"`
	OutputTokens    int64 `json:"output_tokens"`
	CachedTokens    int64 `json:"cached_tokens"`
	RetryTotal      int64 `json:"retry_total"`
	FallbackTotal   int64 `json:"fallback_total"`
}

// GetTrafficTotals 聚合 request_logs 的累计计数。
//
// "成功/失败"的判定用 response_status：2xx 记成功，>=400 记失败。
// 3xx 不计入任何一侧 —— 网关自身不会产生 3xx（它要么透传上游的 2xx，
// 要么转成 4xx/5xx），真出现 3xx 说明有未预期的分支，宁可漏计也不要
// 错误地把它算成成功，那样成功率会虚高、告警永远不响。
func (db *DB) GetTrafficTotals() (TrafficTotals, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var t TrafficTotals
	row := db.conn.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN response_status BETWEEN 200 AND 299 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN response_status >= 400 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(input_tokens), 0),
			COALESCE(SUM(output_tokens), 0),
			COALESCE(SUM(cached_tokens), 0),
			COALESCE(SUM(retry_count), 0),
			COALESCE(SUM(CASE WHEN fallback_used = 1 THEN 1 ELSE 0 END), 0)
		FROM request_logs
	`)
	if err := row.Scan(&t.TotalRequests, &t.SuccessRequests, &t.FailedRequests,
		&t.InputTokens, &t.OutputTokens, &t.CachedTokens,
		&t.RetryTotal, &t.FallbackTotal); err != nil {
		return TrafficTotals{}, err
	}
	return t, nil
}

// ClientUsage is one downstream client's aggregated usage over a time window.
//
// 身份口径 = client_ip。这是"没有下游认证"这一现实约束下能拿到的最细粒度
// 归属：request_logs.client_ip 来自代理层的 r.RemoteAddr，同一 IP 后的多个
// 客户端会被合并。若将来引入 per-client API key，应改为以 key 为主体、IP
// 为辅助标签 —— 那时这个结构体加一列、聚合 SQL 换掉分组键即可。
//
// 之所以现在就做：本地自部署场景下"谁在用、花了多少"是排查成本归属
// （某个客户端的循环把额度烧光）最常用的第一个问题，而当前面板只有
// 平台/Key/模型/接口四个**上游**维度，看不到下游。
type ClientUsage struct {
	ClientIP      string `json:"client_ip"`
	RequestCount  int    `json:"request_count"`
	SuccessCount  int    `json:"success_count"`
	ErrorCount    int    `json:"error_count"`
	InputTokens   int    `json:"input_tokens"`
	OutputTokens  int    `json:"output_tokens"`
	CachedTokens  int    `json:"cached_tokens"`
	TotalTokens   int    `json:"total_tokens"`
	AvgLatencyMs  int    `json:"avg_latency_ms"`
	DistinctLapis int    `json:"distinct_lapis"`
	LastSeen      string `json:"last_seen"`
}

// GetClientUsage aggregates request_logs by client_ip since the given time.
//
// client_ip 存的是 r.RemoteAddr，而 Go 的 net 包保证它形如 "IP:port" ——
// IPv4 是 "127.0.0.1:52341"，IPv6 是带方括号的 "[::1]:52341"。端口每条
// 连接都变，若按整串分组，同一个客户端会被拆成成千上万行，"每 IP 用量"
// 彻底失效。故这里先剥掉端口再分组。
//
// 剥端口的 CASE 分支顺序不可调换：
//   - 先判 ']:' —— IPv6 的地址末尾是右括号，紧跟冒号才是端口。instr 返回
//     子串首字符（']'）的 1 基下标，对 "[::1]:52341" 是 5，故
//     substr(client_ip, 1, 5) 正好取到 "[::1]"（含右括号、不含冒号）。
//     多写一个字符会得到 "[::1]:"，尾巴上留个冒号 —— 这正是
//     TestGetClientUsageIPv6 抓到的错。
//   - 再判裸 ':' —— 此时必是 IPv4:port，取第一个冒号之前的内容。
//   - 都不命中（如纯 IP、无端口的历史行）则原样保留。
func (db *DB) GetClientUsage(since time.Time, limit int) ([]ClientUsage, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if limit <= 0 {
		limit = 20
	}
	rows, err := db.conn.Query(`
		SELECT
			CASE
				WHEN instr(client_ip, ']:') > 0
					THEN substr(client_ip, 1, instr(client_ip, ']:'))
				WHEN instr(client_ip, ':') > 0
					THEN substr(client_ip, 1, instr(client_ip, ':') - 1)
				ELSE client_ip
			END AS ip,
			COUNT(*),
			COALESCE(SUM(CASE WHEN response_status BETWEEN 200 AND 299 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN response_status >= 400 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(input_tokens), 0),
			COALESCE(SUM(output_tokens), 0),
			COALESCE(SUM(cached_tokens), 0),
			CAST(COALESCE(AVG(latency_ms), 0) AS INTEGER),
			COUNT(DISTINCT lapi_alias),
			COALESCE(MAX(timestamp), '')
		FROM request_logs
		WHERE timestamp >= ?
		  AND strftime('%s', timestamp) IS NOT NULL
		  AND client_ip IS NOT NULL
		  AND client_ip != ''
		GROUP BY ip
		ORDER BY COUNT(*) DESC
		LIMIT ?
	`, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ClientUsage
	for rows.Next() {
		var u ClientUsage
		if err := rows.Scan(&u.ClientIP, &u.RequestCount, &u.SuccessCount, &u.ErrorCount,
			&u.InputTokens, &u.OutputTokens, &u.CachedTokens,
			&u.AvgLatencyMs, &u.DistinctLapis, &u.LastSeen); err != nil {
			return nil, err
		}
		u.TotalTokens = u.InputTokens + u.OutputTokens
		out = append(out, u)
	}
	return out, nil
}

// LAPIRAPIOrder is a single lapi→rapi mapping row.
type LAPIRAPIOrder struct {
	LapiID int64 `json:"lapi_id"`
	RAPIID int64 `json:"rapi_id"`
	Order  int   `json:"order"`
}

// GetAllLAPIRAPIOrders returns all lapi→rapi mappings in a single query.
func (db *DB) GetAllLAPIRAPIOrders() ([]LAPIRAPIOrder, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT lapi_id, rapi_id, order_index
		FROM lapi_rapi_order
		ORDER BY lapi_id, order_index
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []LAPIRAPIOrder
	for rows.Next() {
		var o LAPIRAPIOrder
		if err := rows.Scan(&o.LapiID, &o.RAPIID, &o.Order); err != nil {
			return nil, err
		}
		orders = append(orders, o)
	}
	return orders, nil
}

// ---------------------------------------------------------------------------
// 仪表盘指标聚合：今日/本月 × 4 维度（平台/Key/模型/接口）× 10 指标 TOP3
// ---------------------------------------------------------------------------

// MetricRow 是一个 (维度, 指标) 槽位的 TOP3 行。
type MetricRow struct {
	Name  string `json:"name"`
	Value int64  `json:"value"`
	Count int    `json:"count"` // 样本数（延迟类指标=请求数，token 类=请求数）
}

// DimensionMetrics 是一个维度（platform/key/model/interface）的全部 10 类指标。
type DimensionMetrics struct {
	TotalTokens    []MetricRow `json:"total_tokens"`     // 最多总 token
	InputTokens    []MetricRow `json:"input_tokens"`     // 最多输入 token
	CachedTokens   []MetricRow `json:"cached_tokens"`    // 最多命中缓存 token
	OutputTokens   []MetricRow `json:"output_tokens"`    // 最多输出 token
	Attempts       []MetricRow `json:"attempts"`         // 最多尝试访问
	Errors         []MetricRow `json:"errors"`           // 最多报错
	TTFTLowest     []MetricRow `json:"ttft_lowest"`      // 首 token 延迟最低
	RestLatLowest  []MetricRow `json:"rest_lat_lowest"`  // 除首 token 平均延迟最低
	TTFTHighest    []MetricRow `json:"ttft_highest"`     // 首 token 延迟最高
	RestLatHighest []MetricRow `json:"rest_lat_highest"` // 除首 token 平均延迟最高
}

// MetricsResponse 是 /api/dashboard/metrics 的响应体。
type MetricsResponse struct {
	Period    string           `json:"period"` // today | month
	Since     string           `json:"since"`
	Platform  DimensionMetrics `json:"platform"`
	Key       DimensionMetrics `json:"key"`
	Model     DimensionMetrics `json:"model"`
	Interface DimensionMetrics `json:"interface"`
	// NeedsAttention / RecentErrors 由 service 层补充（db 包不做定义类查询）。
	NeedsAttention any `json:"needs_attention,omitempty"`
	RecentErrors   any `json:"recent_errors,omitempty"`
}

// metricAggRow 承载一次分组聚合扫描的输出列。
type metricAggRow struct {
	DimID     string // 维度分组值（平台/key 是数字 id 字符串；模型/接口是别名）
	DimName   string
	Attempt   int64
	Errors    int64
	TotalTok  int64
	InTok     int64
	CachedTok int64
	OutTok    int64
	TTFTSum   int64
	TTFTN     int64
	RestSum   int64
	RestN     int64
}

// minLatencySamples 防小样本：延迟类 TOP 榜要求至少 3 个样本。
const minLatencySamples = 3

// GetDashboardMetrics 聚合 request_logs（自 since 起）到 4 维度 10 指标 TOP3。
// 时间过滤用本地时区今日/本月起点（由 service 层计算传入）。
func (db *DB) GetDashboardMetrics(since time.Time) (*MetricsResponse, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	resp := &MetricsResponse{Since: since.Format(time.RFC3339)}

	// dims 的 group 列必须来自下面的硬编码白名单（列名是 SQL 拼接进查询的，
	// 禁止接受任何外部/用户输入，否则注入风险）。
	dims := []struct {
		key   string // 维度槽位标识
		group string // SQL 分组列
	}{
		{"platform", "selected_platform_id"},
		{"key", "selected_key_id"},
		{"model", "selected_rapi"},
		{"interface", "lapi_alias"},
	}

	for _, d := range dims {
		// G202 命中的是列名拼进 SQL：d.group 只能取自上方 dims 里四个硬编码
		// 列名，没有任何用户输入参与；SQL 标识符本来就不能用 ? 占位。
		// 用 Sprintf 先组装再传参（而不是行内拼接），生成的 SQL 与原来逐字节
		// 一致 —— 行内 `//nolint` 会掉进 raw string 字面量变成 SQL 的一部分，
		// 2026-09-27 已经因此搞坏过一次这个查询。
		// G201（Sprintf 组装 SQL）同样是白名单列名，见上。
		//nolint:gosec
		query := fmt.Sprintf(`
			SELECT %s AS dim_id,
			       COALESCE(NULLIF(%s, ''), '') AS raw_dim,
			       COUNT(*) AS attempts,
			       SUM(CASE WHEN response_status >= 400 OR status = 'failed' THEN 1 ELSE 0 END) AS errors,
			       COALESCE(SUM(tokens_used), 0) AS total_tok,
			       COALESCE(SUM(input_tokens), 0) AS in_tok,
			       COALESCE(SUM(cached_tokens), 0) AS cached_tok,
			       COALESCE(SUM(output_tokens), 0) AS out_tok,
			       COALESCE(SUM(ttft_ms), 0) AS ttft_sum,
			       SUM(CASE WHEN ttft_ms > 0 THEN 1 ELSE 0 END) AS ttft_n,
			       COALESCE(SUM(rest_latency_ms), 0) AS rest_sum,
			       SUM(CASE WHEN rest_latency_ms > 0 THEN 1 ELSE 0 END) AS rest_n
			FROM request_logs
			WHERE timestamp >= ? AND %s IS NOT NULL AND %s != '' AND %s != '0'
			GROUP BY dim_id
		`, d.group, d.group, d.group, d.group, d.group) //nolint:gosec
		rows, err := db.conn.Query(query, since)
		if err != nil {
			return nil, err
		}
		var agg []metricAggRow
		for rows.Next() {
			var r metricAggRow
			if err := rows.Scan(&r.DimID, &r.DimName, &r.Attempt, &r.Errors, &r.TotalTok,
				&r.InTok, &r.CachedTok, &r.OutTok, &r.TTFTSum, &r.TTFTN, &r.RestSum, &r.RestN); err != nil {
				rows.Close()
				return nil, err
			}
			// 模型/接口的分组列本身就是名称；平台/key 的 id→名称映射由调用方补。
			agg = append(agg, r)
		}
		rows.Close()

		dm := DimensionMetrics{
			TotalTokens:    topBy(agg, func(r metricAggRow) (int64, int) { return r.TotalTok, int(r.Attempt) }),
			InputTokens:    topBy(agg, func(r metricAggRow) (int64, int) { return r.InTok, int(r.Attempt) }),
			CachedTokens:   topBy(agg, func(r metricAggRow) (int64, int) { return r.CachedTok, int(r.Attempt) }),
			OutputTokens:   topBy(agg, func(r metricAggRow) (int64, int) { return r.OutTok, int(r.Attempt) }),
			Attempts:       topBy(agg, func(r metricAggRow) (int64, int) { return r.Attempt, int(r.Attempt) }),
			Errors:         topBy(agg, func(r metricAggRow) (int64, int) { return r.Errors, int(r.Attempt) }),
			TTFTLowest:     topLat(agg, func(r metricAggRow) (int64, int64) { return r.TTFTSum, r.TTFTN }, true),
			RestLatLowest:  topLat(agg, func(r metricAggRow) (int64, int64) { return r.RestSum, r.RestN }, true),
			TTFTHighest:    topLat(agg, func(r metricAggRow) (int64, int64) { return r.TTFTSum, r.TTFTN }, false),
			RestLatHighest: topLat(agg, func(r metricAggRow) (int64, int64) { return r.RestSum, r.RestN }, false),
		}
		switch d.key {
		case "platform":
			resp.Platform = dm
		case "key":
			resp.Key = dm
		case "model":
			resp.Model = dm
		case "interface":
			resp.Interface = dm
		}
	}
	return resp, nil
}

// topLimit 是每个维度在面板上展示的名次数量。
const topLimit = 3

// topBy 取 sum/sum 类指标 TOP topLimit（值 >0 才上榜），恒为降序（最多）。
// sum 类指标（token/尝试/报错）不存在"最少"这种有意义的排序，故不带
// lowest 参数；需要最低值语义的是延迟类，走 topLat。
func topBy(rows []metricAggRow, pick func(metricAggRow) (int64, int)) []MetricRow {
	type pair struct {
		name  string
		id    string
		val   int64
		count int
	}
	ps := make([]pair, 0, len(rows))
	for _, r := range rows {
		v, c := pick(r)
		if v <= 0 {
			continue
		}
		ps = append(ps, pair{r.DimName, r.DimID, v, c})
	}
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].val != ps[j].val {
			return ps[i].val > ps[j].val
		}
		return ps[i].name < ps[j].name
	})
	out := make([]MetricRow, 0, topLimit)
	for i := 0; i < len(ps) && i < topLimit; i++ {
		out = append(out, MetricRow{Name: ps[i].name, Value: ps[i].val, Count: ps[i].count})
	}
	return out
}

// topLat 取平均延迟类指标 TOP topLimit；minSamples 防小样本，lowest=true 取最低。
func topLat(rows []metricAggRow, pick func(metricAggRow) (int64, int64), lowest bool) []MetricRow {
	type pair struct {
		name string
		id   string
		avg  float64
		n    int64
	}
	ps := make([]pair, 0, len(rows))
	for _, r := range rows {
		sum, cnt := pick(r)
		if cnt < minLatencySamples {
			continue
		}
		ps = append(ps, pair{r.DimName, r.DimID, float64(sum) / float64(cnt), cnt})
	}
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].avg != ps[j].avg {
			if lowest {
				return ps[i].avg < ps[j].avg
			}
			return ps[i].avg > ps[j].avg
		}
		return ps[i].name < ps[j].name
	})
	out := make([]MetricRow, 0, topLimit)
	for i := 0; i < len(ps) && i < topLimit; i++ {
		out = append(out, MetricRow{Name: ps[i].name, Value: int64(ps[i].avg), Count: int(ps[i].n)})
	}
	return out
}
