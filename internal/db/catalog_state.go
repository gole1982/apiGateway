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
	"log/slog"
	"strings"

	"gateway/internal/models"
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

// migrateNonAutoDiscoveredRAPIAvailable 修历史数据：非自动发现的模型没有"失效"态。
//
// 口径来自需求方：自定义模型只有"禁用/启用"两态，没有"失效/有效"。旧版目录同步
// 拿 /v1/models 去判它们（必然缺席 → 批量写成 available=0），旧版运行时也会在
// "全部 Key 不可用"时把任意模型持久化为不可用。这些行在面板上显示"失效"，且被
// GetEnabledRAPIsForLAPI 过滤掉，属于纯噪声。
//
// 判定用白名单（source <> 'auto_discover' 全部复位），与 models.IsAutoDiscovered
// 同口径：source 为空串的存量行按 manual 处理。
//
// 幂等：新代码不再给这些行写 available=0，首次修完之后每次启动都是空操作。
func (db *DB) migrateNonAutoDiscoveredRAPIAvailable() {
	db.mu.Lock()
	defer db.mu.Unlock()

	res, err := db.conn.Exec(`
		UPDATE rapi SET available = 1, unavailable_reason = '', updated_at = CURRENT_TIMESTAMP
		WHERE source <> 'auto_discover' AND available = 0`)
	if err != nil {
		slog.Warn("[DB] repair non-discovered RAPI availability failed", "component", "db", "error", err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		slog.Info("[DB] non-discovered RAPI availability repaired", "component", "db", "rows", n)
	}
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

// LocalRAPIIDByNaturalKey 按 (platform.base_url, rapi.model) 回查本地 rapi.id。
//
// 管理模式下 store.A() 返回中心侧 id，本地 SQLite 的自增序列是另一套；
// 健康态只能写本地表，直接用中心 id 会改错行或改不到。自然键在中心/本地一致，
// 因此统一用它在本地镜像里定位。
func (db *DB) LocalRAPIIDByNaturalKey(platformBaseURL, model string) (int64, bool, error) {
	want := models.NormalizeBaseURL(platformBaseURL)
	model = strings.TrimSpace(model)
	if want == "" || model == "" {
		return 0, false, nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT r.id, p.base_url
		FROM rapi r JOIN platform p ON p.id = r.platform_id
		WHERE r.model = ?`, model)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id   int64
			base string
		)
		if err := rows.Scan(&id, &base); err != nil {
			return 0, false, err
		}
		if models.NormalizeBaseURL(base) == want {
			return id, true, nil
		}
	}
	return 0, false, rows.Err()
}

// LocalRAPIAvailable 读本地镜像里某个 rapi 的 available 标志。
//
// 只用于判定"这次写入是否真的改变了状态"：目录同步每 30 分钟跑一轮恢复，
// 已失效的模型每轮都会被再判一次失效，若不加区分就会反复发通知。
// 它不是路由决策的输入（路由看的是 GetEnabledRAPIsForLAPI 取出的整行）。
func (db *DB) LocalRAPIAvailable(id int64) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var available int
	if err := db.conn.QueryRow(`SELECT available FROM rapi WHERE id = ?`, id).Scan(&available); err != nil {
		return false, err
	}
	return available != 0, nil
}

// LocalPlatformHasRAPIs 报告本地镜像里该平台是否已有端点记录。
// 管理模式/代理模式首次启动时，中心定义要等首轮 sync 才会落到本地；
// 目录同步在此之前探测会写下一批 probe 记录，却因本地无 rapi 可写而
// 无法应用判定，之后又因"已探测过"不再重试。调用方应先确认就绪。
func (db *DB) LocalPlatformHasRAPIs(platformBaseURL string) (bool, error) {
	want := models.NormalizeBaseURL(platformBaseURL)
	if want == "" {
		return false, nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT p.base_url FROM platform p
		WHERE EXISTS (SELECT 1 FROM rapi r WHERE r.platform_id = p.id)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var base string
		if err := rows.Scan(&base); err != nil {
			return false, err
		}
		if models.NormalizeBaseURL(base) == want {
			return true, nil
		}
	}
	return false, rows.Err()
}

// LocalUnavailableRAPIModels 返回本地镜像中该平台"启用但不可用"的**可判定**
// 模型集合（小写、去空白）。目录同步据此判断是否还需要恢复轮次。
//
// 只统计 source=auto_discover：其余来源（manual、空串等）没有失效/有效两态，
// 目录同步永远不会去恢复它们。若把它们算进来，一个这样的行只要 available=0，
// 该平台就会被永久判为"有待恢复端点"，每 30 分钟无意义地重探一次上游。
func (db *DB) LocalUnavailableRAPIModels(platformBaseURL string) (map[string]struct{}, error) {
	want := models.NormalizeBaseURL(platformBaseURL)
	out := make(map[string]struct{})
	if want == "" {
		return out, nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT r.model, p.base_url
		FROM rapi r JOIN platform p ON p.id = r.platform_id
		WHERE r.enabled = 1 AND r.available = 0 AND r.source = 'auto_discover'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			model string
			base  string
		)
		if err := rows.Scan(&model, &base); err != nil {
			return nil, err
		}
		if models.NormalizeBaseURL(base) != want {
			continue
		}
		if m := strings.ToLower(strings.TrimSpace(model)); m != "" {
			out[m] = struct{}{}
		}
	}
	return out, rows.Err()
}
