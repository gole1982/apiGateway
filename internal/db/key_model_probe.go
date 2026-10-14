// key_model_probe.go —— key×model 授权探测的**观测记录**（纯诊断，与生效配置解耦）。
//
// 要解决的问题：rapi.key_ids 的"空 = 平台全部 Key"是一条**推断规则**，而上游的
// 模型目录是否随 Key 变化（权益分档）从未被验证过。发现模型时只拿 keys[0] 去探测
// （service.go 的 fetch-models），若该 Key 权限偏低，得到的目录是残缺的；发现向导
// 会据此把"当前可用但 keys[0] 没权限"的模型判为已下线并硬删（dashboard.html 的
// importToRemove → DELETE ?sync=true）。这是一条真实的数据丢失路径。
//
// 本表记录每把 Key 实际观测到的目录，供 service.probePlatformKeys 对照现状输出
// 偏差。**硬约束：写入本表绝不能改变网关当前行为。** 网关读的是 rapi.key_ids
// （endpoint_credential 的派生投影），因此本表与 endpoint_credential 保持完全独立，
// 也不进 store 接缝 —— 它是诊断数据，不该触发 sync 脏标记、也不该进 bundle 契约。
package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// migrateKeyModelProbe 建 key_model_probe 表，并给 credential 加 models_probed_at
// （NULL = 该 Key 从未探测过，其目录状态未知）。幂等。
func (db *DB) migrateKeyModelProbe() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.execMigrationDDL("migrateKeyModelProbe/create key_model_probe", `
		CREATE TABLE IF NOT EXISTS key_model_probe (
			key_id    INTEGER NOT NULL,
			model     TEXT NOT NULL,
			probed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (key_id, model)
		)`)
	db.execMigrationDDL("migrateKeyModelProbe/index key_model_probe.model", `
		CREATE INDEX IF NOT EXISTS idx_key_model_probe_model ON key_model_probe(model)`)

	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('credential') WHERE name='models_probed_at'`)
	if row.Scan(&count) == nil && count == 0 {
		db.execMigrationDDL("migrateKeyModelProbe/credential.models_probed_at", `
			ALTER TABLE credential ADD COLUMN models_probed_at DATETIME`)
	}
}

// RecordKeyModelProbe 用一次探测结果**整体替换**该 Key 的目录记录，并盖上
// probed_at 时间戳。整体替换而非增量追加：一次成功的探测就是该 Key 目录的全量
// 快照，增量会让"上游已下架"的模型永久残留在表里。
func (db *DB) RecordKeyModelProbe(keyID int64, modelNames []string, probedAt time.Time) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM key_model_probe WHERE key_id = ?`, keyID); err != nil {
		return err
	}
	stamp := probedAt.UTC().Format(time.RFC3339)
	for _, name := range modelNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, err := tx.Exec(`INSERT INTO key_model_probe (key_id, model, probed_at) VALUES (?, ?, ?)`,
			keyID, name, stamp); err != nil {
			return fmt.Errorf("record probe key=%d model=%q: %w", keyID, name, err)
		}
	}
	if _, err := tx.Exec(`UPDATE credential SET models_probed_at = ? WHERE id = ?`, stamp, keyID); err != nil {
		return fmt.Errorf("stamp credential %d: %w", keyID, err)
	}
	return tx.Commit()
}

// GetKeyModelProbe 读回某把 Key 最近一次探测到的目录（未探测过时返回空切片）。
func (db *DB) GetKeyModelProbe(keyID int64) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.keyModelProbeLocked(keyID)
}

func (db *DB) keyModelProbeLocked(keyID int64) ([]string, error) {
	rows, err := db.conn.Query(`SELECT model FROM key_model_probe WHERE key_id = ? ORDER BY model`, keyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// KeyProbedAt 返回该 Key 最近一次成功探测的时间；ok=false 表示从未探测过
// （其对任一模型的可达性都是未知，不能据此下"没有权限"的结论）。
func (db *DB) KeyProbedAt(keyID int64) (time.Time, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var at sql.NullTime
	if err := db.conn.QueryRow(`SELECT models_probed_at FROM credential WHERE id = ?`, keyID).Scan(&at); err != nil {
		return time.Time{}, false
	}
	if !at.Valid || at.Time.IsZero() {
		return time.Time{}, false
	}
	return at.Time, true
}

// GetAllKeyModelProbes 一次性读出全部探测记录，排障/导出用。
// 返回 keyID → 目录。
func (db *DB) GetAllKeyModelProbes() (map[int64][]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`SELECT key_id, model FROM key_model_probe ORDER BY key_id, model`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int64][]string)
	for rows.Next() {
		var keyID int64
		var m string
		if err := rows.Scan(&keyID, &m); err != nil {
			return nil, err
		}
		out[keyID] = append(out[keyID], m)
	}
	return out, rows.Err()
}
