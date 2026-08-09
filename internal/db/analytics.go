package db

import "time"

// HourBucket represents one hour's aggregated traffic across all or one LAPI.
type HourBucket struct {
	Hour         int    `json:"hour"`          // 0-23
	LapiAlias    string `json:"lapi_alias"`
	RequestCount int    `json:"request_count"`
	TokenCount   int    `json:"token_count"`
	ErrorCount   int    `json:"error_count"`   // response_status >= 400
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
	RetryCount    int     `json:"retry_count"`     // sum of retry_count
	FallbackRate  float64 `json:"fallback_rate"`   // fallback_count / total * 100
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
	Date         string `json:"date"`          // YYYY-MM-DD
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
