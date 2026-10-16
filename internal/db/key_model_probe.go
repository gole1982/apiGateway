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
//
// **主键为什么是 credential_token_hash 而不是 credential.id**（踩过的坑）：
// 管理端下 store.A() 返回的是中心侧 id，本地 SQLite 镜像的 credential.id 是另一套
// 自增序列（实测：中心 key_id=72/74，本地 credential.id 只有 1..38，且本地镜像
// 根本不含该平台）。用 id 做主键会让整表悬空、credential 上的标记列永远匹配不到。
// token_hash 是凭据的自然键（docs/superpowers/specs/2026-09-23 自然键身份设计），
// 中心与本地对同一明文 token 算出同一个哈希，与 id 空间无关。
package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// migrateKeyModelProbe 建 key_model_probe 表。幂等。
//
// 主键从 (key_id, model) 换成 (credential_token_hash, model)：见文件头说明。
// 旧表数据在管理端下全部悬空（key_id 指向本地不存在的行），无可抢救价值，直接重建。
func (db *DB) migrateKeyModelProbe() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.execMigrationDDL("migrateKeyModelProbe/drop legacy id-keyed table", `
		DROP TABLE IF EXISTS key_model_probe`)
	db.execMigrationDDL("migrateKeyModelProbe/create key_model_probe", `
		CREATE TABLE IF NOT EXISTS key_model_probe (
			credential_token_hash TEXT NOT NULL,
			model                 TEXT NOT NULL,
			probed_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (credential_token_hash, model)
		)`)
	db.execMigrationDDL("migrateKeyModelProbe/index key_model_probe.model", `
		CREATE INDEX IF NOT EXISTS idx_key_model_probe_model ON key_model_probe(model)`)

	// credential.models_probed_at 是主键还是 id 时代留下的标记列，随主键一起作废：
	// "是否已探测"现在由 key_model_probe 里有没有该 token_hash 的行决定，
	// 两份真相只会打架。
	var colCount int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('credential') WHERE name='models_probed_at'`)
	if row.Scan(&colCount) == nil && colCount > 0 {
		db.execMigrationDDL("migrateKeyModelProbe/drop credential.models_probed_at", `
			ALTER TABLE credential DROP COLUMN models_probed_at`)
	}
}

// RecordKeyModelProbe 用一次探测结果**整体替换**该凭据的目录记录。
// 整体替换而非增量追加：一次成功的探测就是该凭据目录的全量快照，
// 增量会让"上游已下架"的模型永久残留在表里。
//
// 上游目录里出现重复模型名是真实存在的（实测 unorouter / TokenRouter 均有），
// 一个重复项就能撞 PRIMARY KEY 回滚整个事务 —— 那会让该平台整轮探测白跑。
// 所以先按大小写不敏感去重（与 diffKeyAuthorization 的匹配口径一致，保留首个
// 原始大小写），再用 INSERT OR IGNORE 兜底。
//
// tokenHash 为空串表示该凭据没有 token（动态令牌占位行），无处可探测，直接跳过。
func (db *DB) RecordKeyModelProbe(tokenHash string, modelNames []string, probedAt time.Time) error {
	if tokenHash == "" {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	seenLV := make(map[string]string, len(modelNames))
	names := make([]string, 0, len(modelNames))
	for _, name := range modelNames {
		lv := strings.ToLower(strings.TrimSpace(name))
		if lv == "" {
			continue
		}
		if _, dup := seenLV[lv]; dup {
			continue
		}
		seenLV[lv] = name
		names = append(names, strings.TrimSpace(name))
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM key_model_probe WHERE credential_token_hash = ?`, tokenHash); err != nil {
		return err
	}
	stamp := probedAt.UTC().Format(time.RFC3339)
	for _, name := range names {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO key_model_probe (credential_token_hash, model, probed_at) VALUES (?, ?, ?)`,
			tokenHash, name, stamp); err != nil {
			return fmt.Errorf("record probe token=%s model=%q: %w", tokenHash[:8], name, err)
		}
	}
	// 空目录也要留痕：插一行 model='' 的哨兵，证明"探测发生过但上游没给目录"。
	// 没有它，空目录的探测与"从未探测"无法区分，而两者对切量闸门的含义不同
	// （前者是"无从判断"，后者是"还没做"）。哨兵在 GetKeyModelProbe 里被过滤掉，
	// 不参与任何模型判定。
	if len(names) == 0 {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO key_model_probe (credential_token_hash, model, probed_at) VALUES (?, '', ?)`,
			tokenHash, stamp); err != nil {
			return fmt.Errorf("record empty-catalog sentinel token=%s: %w", tokenHash[:8], err)
		}
	}
	return tx.Commit()
}

// GetKeyModelProbe 读回某凭据最近一次探测到的目录（无记录时返回空切片）。
// model=” 是"探测过但上游没给目录"的哨兵行，在这里被过滤掉 —— 它只用于让
// CredentialProbedAt 能区分"探测过"与"从未探测"，不参与模型判定。
func (db *DB) GetKeyModelProbe(tokenHash string) ([]string, error) {
	if tokenHash == "" {
		return []string{}, nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`SELECT model FROM key_model_probe WHERE credential_token_hash = ? AND model != '' ORDER BY model`, tokenHash)
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

// CredentialProbedAt 返回某凭据最近一次成功探测的时间；ok=false 表示从未探测过。
// "探测过但目录为空"也算探测过 —— 那是有意义的结论（上游不给目录），与"没探测"不同。
// 目录是否为空由调用方取 GetKeyModelProbe 的行数判断。
//
// 注意：modernc.org/sqlite 把 DATETIME 列以字符串返回，不能直接 Scan 进
// sql.NullTime（实测报 unsupported Scan），所以先接字符串再解析。
func (db *DB) CredentialProbedAt(tokenHash string) (time.Time, bool, error) {
	if tokenHash == "" {
		return time.Time{}, false, nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	var at sql.NullString
	if err := db.conn.QueryRow(`SELECT MAX(probed_at) FROM key_model_probe WHERE credential_token_hash = ?`, tokenHash).Scan(&at); err != nil {
		return time.Time{}, false, err
	}
	if !at.Valid || at.String == "" {
		return time.Time{}, false, nil
	}
	t, err := time.Parse(time.RFC3339, at.String)
	if err != nil {
		return time.Time{}, false, err
	}
	return t, true, nil
}

// GetAllKeyModelProbes 一次性读出全部探测记录，排障/导出用。
func (db *DB) GetAllKeyModelProbes() (map[string][]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`SELECT credential_token_hash, model FROM key_model_probe ORDER BY credential_token_hash, model`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]string)
	for rows.Next() {
		var h, m string
		if err := rows.Scan(&h, &m); err != nil {
			return nil, err
		}
		out[h] = append(out[h], m)
	}
	return out, rows.Err()
}
