// catalog_state.go —— rapi 目录判定的冷却状态（防上游抖动导致 available 反复横跳）。
//
// 为什么需要冷却：路由表是逐请求从 DB 读的，available 一翻转立刻打到线上。
// 上游目录可能临时残缺（网络抖动、上游维护），若立刻恢复再失效，客户端会看到
// 模型时好时坏。所以恢复要求"连续 N 次探测确认在场"，失效则立即执行（响应快）。
//
// streak 只在"探测确认在场但当前 available=0"时累加；一旦某轮探测失败就清零。
// N 由 settings 表的 `catalog_recover_confirmations` 控制，默认 2，用户可调。
package db

import (
	"database/sql"
	"fmt"
)

// migrateCatalogState 建 rapi_catalog_state 表。幂等。
func (db *DB) migrateCatalogState() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.execMigrationDDL("migrateCatalogState/create rapi_catalog_state", `
		CREATE TABLE IF NOT EXISTS rapi_catalog_state (
			rapi_id         INTEGER PRIMARY KEY,
			recover_streak  INTEGER NOT NULL DEFAULT 0,
			last_judged_at  DATETIME
		)`)
}

// GetRecoverStreak 读某 rapi 的恢复连续确认次数。
func (db *DB) GetRecoverStreak(rapiID int64) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var n int
	if err := db.conn.QueryRow(`SELECT recover_streak FROM rapi_catalog_state WHERE rapi_id = ?`, rapiID).Scan(&n); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return n, nil
}

// SetRecoverStreak 写某 rapi 的恢复连续确认次数（0 表示清零，删除该行）。
func (db *DB) SetRecoverStreak(rapiID int64, streak int) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if streak <= 0 {
		_, err := db.conn.Exec(`DELETE FROM rapi_catalog_state WHERE rapi_id = ?`, rapiID)
		return err
	}
	_, err := db.conn.Exec(`INSERT INTO rapi_catalog_state (rapi_id, recover_streak, last_judged_at)
		VALUES (?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(rapi_id) DO UPDATE SET recover_streak = ?, last_judged_at = CURRENT_TIMESTAMP`,
		rapiID, streak, streak)
	return err
}

// GetRecoverConfirmations 读恢复冷却阈值（默认 2）。
// 放在 settings 表而非 rapi_catalog_state：它是全局策略参数，不是某个 rapi 的状态。
func (db *DB) GetRecoverConfirmations() (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var v string
	if err := db.conn.QueryRow(`SELECT value FROM settings WHERE key = 'catalog_recover_confirmations'`).Scan(&v); err != nil {
		if err == sql.ErrNoRows {
			return 2, nil // 默认值
		}
		return 2, nil // 读取失败时退回默认值，不阻断探测
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n < 1 {
		return 2, nil
	}
	return n, nil
}
