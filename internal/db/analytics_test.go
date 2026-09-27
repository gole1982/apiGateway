package db

import (
	"testing"
	"time"
)

// TestGetDashboardMetrics 是 GetDashboardMetrics 里维度 SQL 的回归测试。
//
// 背景：2026-09-27 修 gosec G202 时，一版 `//nolint:gosec` 注释被写进了
// raw string 字面量内部，成了 SQL 文本的一部分，四个维度的查询全坏。
// 这个函数此前零覆盖，所以测试全绿、面板的指标页却必然 500。
// 这个测试保证：SQL 能跑通，且聚合值算得对。
func TestGetDashboardMetrics(t *testing.T) {
	db := setupTestDB(t)

	_, err := db.conn.Exec(`
		CREATE TABLE request_logs (
			id TEXT PRIMARY KEY,
			timestamp DATETIME,
			status TEXT DEFAULT 'completed',
			lapi_alias TEXT,
			selected_rapi TEXT,
			selected_key_id INTEGER DEFAULT 0,
			selected_platform_id INTEGER DEFAULT 0,
			response_status INTEGER,
			tokens_used INTEGER,
			input_tokens INTEGER DEFAULT 0,
			output_tokens INTEGER DEFAULT 0,
			cached_tokens INTEGER DEFAULT 0,
			ttft_ms INTEGER DEFAULT 0,
			rest_latency_ms INTEGER DEFAULT 0
		)`)
	if err != nil {
		t.Fatalf("create request_logs: %v", err)
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	insert := `INSERT INTO request_logs
		(id, timestamp, status, lapi_alias, selected_rapi,
		 selected_key_id, selected_platform_id, response_status,
		 tokens_used, input_tokens, output_tokens)
		VALUES (?, ?, 'completed', 'a', 'm1', 3, 7, ?, ?, ?, ?)`
	if _, err := db.conn.Exec(insert, "r1", now, 200, 100, 60, 40); err != nil {
		t.Fatalf("insert r1: %v", err)
	}
	if _, err := db.conn.Exec(insert, "r2", now, 500, 50, 30, 20); err != nil {
		t.Fatalf("insert r2: %v", err)
	}

	resp, err := db.GetDashboardMetrics(time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("GetDashboardMetrics: %v", err)
	}

	// 平台维度：id 7 合计 150 token，1 次报错。
	got := rowsByName(resp.Platform.TotalTokens)
	if got["7"] != 150 {
		t.Errorf("platform TotalTokens[7] = %d, want 150", got["7"])
	}
	if got := rowsByName(resp.Platform.Errors); got["7"] != 1 {
		t.Errorf("platform Errors[7] = %d, want 1", got["7"])
	}
	// 模型维度：名称本就是别名，不走 id 映射。
	if got := rowsByName(resp.Model.TotalTokens); got["m1"] != 150 {
		t.Errorf("model TotalTokens[m1] = %d, want 150", got["m1"])
	}
	// 接口维度：lapi_alias 直接聚合。
	if got := rowsByName(resp.Interface.Attempts); got["a"] != 2 {
		t.Errorf("interface Attempts[a] = %d, want 2", got["a"])
	}
	// key 维度：id 3。
	if got := rowsByName(resp.Key.TotalTokens); got["3"] != 150 {
		t.Errorf("key TotalTokens[3] = %d, want 150", got["3"])
	}
}

func rowsByName(rows []MetricRow) map[string]int64 {
	out := make(map[string]int64, len(rows))
	for _, r := range rows {
		out[r.Name] = r.Value
	}
	return out
}
