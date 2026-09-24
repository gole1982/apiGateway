// migrate_natural_keys.go —— 自然键身份模型迁移（设计：docs/superpowers/specs/
// 2026-09-23-natural-key-identity-design.md §6）。
//
// 一次性把 platform_keys(key_index) / rapi.alias 身份体系切换为：
//   - credential      全局凭据表，自然键 token_hash = sha256(token)[:16]（保留原
//     platform_keys.id，key_model_blocks / token_cache 引用免换算）
//   - endpoint_credential 端点↔凭据绑定表，自然键 (rapi_id, credential_id)，
//     物化旧 rapi.key_ids 白名单语义（空=平台全部 key → 显式全绑定）
//   - rapi            去掉 alias 唯一约束（降级为显示名），身份 = (platform_id→
//     base_url, model)，部分唯一索引 WHERE model<>”
//   - platform        去掉 name 唯一约束（降级为显示名），base_url 归一化 +
//     部分唯一索引 WHERE base_url<>”
//
// 幂等可重入：platform_keys 不存在且各表已是新形态时整体跳过。
// 合并清单（dry-run）：PreviewNaturalKeyMigration 只读分析，cmd/naturalmigrate
// 用它在升级前打印将发生的合并。
package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"gateway/internal/crypto"
	"gateway/internal/models"
)

// 新表 DDL（IF NOT EXISTS：新装库与升级库同一路径）。
const credentialDDL = `
CREATE TABLE IF NOT EXISTS credential (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	platform_id INTEGER NOT NULL DEFAULT 0,
	token_hash TEXT,
	token TEXT NOT NULL DEFAULT '',
	label TEXT NOT NULL DEFAULT '',
	enabled INTEGER NOT NULL DEFAULT 1,
	failure_type INTEGER NOT NULL DEFAULT 0,
	failure_reason TEXT NOT NULL DEFAULT '',
	failed_at DATETIME,
	expires_at DATETIME,
	is_free INTEGER NOT NULL DEFAULT 0,
	sort_order INTEGER NOT NULL DEFAULT 0,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_credential_token_hash
	ON credential(token_hash) WHERE token_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_credential_platform ON credential(platform_id);
`

const endpointCredentialDDL = `
CREATE TABLE IF NOT EXISTS endpoint_credential (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	rapi_id INTEGER NOT NULL,
	credential_id INTEGER NOT NULL,
	rpm_limit INTEGER NOT NULL DEFAULT 0,
	rph_limit INTEGER NOT NULL DEFAULT 0,
	rpd_limit INTEGER NOT NULL DEFAULT 0,
	tpm_limit INTEGER NOT NULL DEFAULT 0,
	tph_limit INTEGER NOT NULL DEFAULT 0,
	tpd_limit INTEGER NOT NULL DEFAULT 0,
	enabled INTEGER NOT NULL DEFAULT 1,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY (rapi_id) REFERENCES rapi(id) ON DELETE CASCADE,
	FOREIGN KEY (credential_id) REFERENCES credential(id) ON DELETE CASCADE,
	UNIQUE(rapi_id, credential_id)
);
CREATE INDEX IF NOT EXISTS idx_endpoint_credential_cred ON endpoint_credential(credential_id);
`

// NaturalKeyReport 是迁移（或 dry-run 预览）产生的合并清单。
type NaturalKeyReport struct {
	CredentialsCreated    int               `json:"credentials_created"`
	CredentialsMerged     []CredentialMerge `json:"credentials_merged,omitempty"`
	BindingsCreated       int               `json:"bindings_created"`
	RAPIsMerged           []RAPIMerge       `json:"rapis_merged,omitempty"`
	PlatformsRenormalized int               `json:"platforms_renormalized"`
	PlatformsMerged       []PlatformMerge   `json:"platforms_merged,omitempty"`
}

// CredentialMerge：同 token_hash 的多行 platform_keys 合并为一行 credential。
type CredentialMerge struct {
	KeptID    int64  `json:"kept_id"`
	DroppedID int64  `json:"dropped_id"`
	TokenHash string `json:"token_hash"`
}

// RAPIMerge：同 (platform, model) 的重复端点合并（统计并入保留行，引用改挂）。
type RAPIMerge struct {
	PlatformID int64  `json:"platform_id"`
	KeptID     int64  `json:"kept_id"`
	DroppedID  int64  `json:"dropped_id"`
	Model      string `json:"model"`
}

// PlatformMerge：归一化后 base_url 相同的平台合并（子资源改挂保留行）。
type PlatformMerge struct {
	KeptID      int64  `json:"kept_id"`
	DroppedID   int64  `json:"dropped_id"`
	KeptName    string `json:"kept_name"`
	DroppedName string `json:"dropped_name"`
	BaseURL     string `json:"base_url"`
}

func (r *NaturalKeyReport) empty() bool {
	return r.CredentialsCreated == 0 && len(r.CredentialsMerged) == 0 &&
		r.BindingsCreated == 0 && len(r.RAPIsMerged) == 0 &&
		r.PlatformsRenormalized == 0 && len(r.PlatformsMerged) == 0
}

// migrateNaturalKeys 在 Init 迁移链末尾调用（所有历史列已就位之后）。
func (db *DB) migrateNaturalKeys() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	rep, err := MigrateNaturalKeysConn(db.conn)
	if err != nil {
		return err
	}
	if !rep.empty() {
		slog.Info("[DB] natural-key migration applied", "component", "db",
			"credentials_created", rep.CredentialsCreated,
			"credentials_merged", len(rep.CredentialsMerged),
			"bindings_created", rep.BindingsCreated,
			"rapis_merged", len(rep.RAPIsMerged),
			"platforms_renormalized", rep.PlatformsRenormalized,
			"platforms_merged", len(rep.PlatformsMerged))
		for _, m := range rep.CredentialsMerged {
			slog.Warn("[DB] credential merge (duplicate token)", "component", "db",
				"kept_id", m.KeptID, "dropped_id", m.DroppedID, "token_hash", m.TokenHash)
		}
		for _, m := range rep.RAPIsMerged {
			slog.Warn("[DB] rapi merge (duplicate platform+model)", "component", "db",
				"platform_id", m.PlatformID, "kept_id", m.KeptID, "dropped_id", m.DroppedID, "model", m.Model)
		}
		for _, m := range rep.PlatformsMerged {
			slog.Warn("[DB] platform merge (same normalized base_url)", "component", "db",
				"kept", fmt.Sprintf("%d(%s)", m.KeptID, m.KeptName),
				"dropped", fmt.Sprintf("%d(%s)", m.DroppedID, m.DroppedName), "base_url", m.BaseURL)
		}
	}
	return nil
}

// MigrateNaturalKeysConn 在任意连接上执行迁移（Init 与 cmd/naturalmigrate 共用）。
// schema 探测在事务前完成；建表、数据平移和旧表删除在同一事务内提交。
func MigrateNaturalKeysConn(conn *sql.DB) (*NaturalKeyReport, error) {
	rep := &NaturalKeyReport{}

	// 识别旧二进制在自然键终态库上重新创建的 platform_keys 残影。只删除
	// 所有行都具备 migrateAddPlatformKeys 回填指纹的表；混合/真实旧行进入
	// 下方事务做无损失冲量，而不是按表整批丢弃。
	staleKeys := false
	staleRows := 0
	if tableExists(conn, "credential") && tableExists(conn, "platform_keys") {
		stale, rows, err := staleBackfillResidue(conn)
		if err != nil {
			return rep, fmt.Errorf("inspect stale platform_keys: %w", err)
		}
		staleKeys, staleRows = stale, rows
	}

	hasKeys := tableExists(conn, "platform_keys")
	if staleKeys {
		hasKeys = false
	}
	hasCredential := tableExists(conn, "credential")
	hasEndpointCredential := tableExists(conn, "endpoint_credential")
	rapiSQL := tableSchemaSQL(conn, "rapi")
	rapiNeedsRebuild := strings.Contains(rapiSQL, "UNIQUE(platform_id, alias)") ||
		strings.Contains(rapiSQL, "alias TEXT NOT NULL UNIQUE")
	platNeedsRebuild := strings.Contains(tableSchemaSQL(conn, "platform"), "UNIQUE")
	tcNeedsRebuild := columnExists(conn, "token_cache", "platform_key_id")
	kmbNeedsRebuild := !strings.Contains(tableSchemaSQL(conn, "key_model_blocks"), "credential(")

	if !hasKeys && hasCredential && hasEndpointCredential && !staleKeys &&
		!rapiNeedsRebuild && !platNeedsRebuild && !tcNeedsRebuild && !kmbNeedsRebuild {
		return rep, nil // 已是新形态
	}

	if _, err := conn.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		return rep, err
	}
	defer conn.Exec("PRAGMA foreign_keys=ON") //nolint:errcheck // best-effort restore

	tx, err := conn.Begin()
	if err != nil {
		return rep, err
	}
	defer tx.Rollback() //nolint:errcheck // commit 后为 no-op

	// 新表的创建也纳入事务。旧实现把 DDL 放在 Begin 之前，失败后会留下空
	// credential 表，让下一次启动误判迁移已完成。
	if _, err := tx.Exec(credentialDDL + endpointCredentialDDL); err != nil {
		return rep, fmt.Errorf("create credential tables: %w", err)
	}

	if staleKeys {
		if _, err := tx.Exec(`DROP TABLE platform_keys`); err != nil {
			return rep, fmt.Errorf("drop stale platform_keys: %w", err)
		}
		slog.Warn("[DB] dropped stale platform_keys residue", "component", "db", "rows", staleRows,
			"reason", "credential 已存在且非空，platform_keys 为旧版本回填残影；其 token 仍保留在 platform.token")
	}

	// A. platform_keys → credential（保留可用原 id；冲突行按 token 映射到
	// 已有 credential 或分配新 id，并重映射所有旧引用）。
	if hasKeys {
		if err := migratePlatformKeysTx(tx, rep); err != nil {
			return rep, fmt.Errorf("platform_keys to credential: %w", err)
		}
	}

	// B. rapi (platform_id, model) 去重合并（平台合并前先来一轮）。
	merged, err := mergeDuplicateRAPIsTx(tx)
	if err != nil {
		return rep, fmt.Errorf("rapi dedupe: %w", err)
	}
	rep.RAPIsMerged = append(rep.RAPIsMerged, merged...)

	// C. platform base_url 归一化 + 合并，再跑一轮 rapi 去重（跨平台模型碰撞）。
	pmerged, renorm, err := mergePlatformsTx(tx)
	if err != nil {
		return rep, fmt.Errorf("platform merge: %w", err)
	}
	rep.PlatformsMerged = pmerged
	rep.PlatformsRenormalized = renorm
	merged, err = mergeDuplicateRAPIsTx(tx)
	if err != nil {
		return rep, fmt.Errorf("rapi dedupe after platform merge: %w", err)
	}
	rep.RAPIsMerged = append(rep.RAPIsMerged, merged...)

	// D. 表重建（去旧唯一约束 / 换 FK 指向 credential）。
	if tcNeedsRebuild {
		if _, err := tx.Exec(`
			CREATE TABLE token_cache_new (
				credential_id INTEGER PRIMARY KEY,
				token TEXT NOT NULL,
				expires_at DATETIME,
				fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
				FOREIGN KEY (credential_id) REFERENCES credential(id) ON DELETE CASCADE
			);
			INSERT OR IGNORE INTO token_cache_new (credential_id, token, expires_at, fetched_at)
				SELECT platform_key_id, token, expires_at, fetched_at FROM token_cache;
			DROP TABLE token_cache;
			ALTER TABLE token_cache_new RENAME TO token_cache;
		`); err != nil {
			return rep, fmt.Errorf("rebuild token_cache: %w", err)
		}
	}
	if kmbNeedsRebuild {
		if _, err := tx.Exec(`
			CREATE TABLE key_model_blocks_new (
				key_id INTEGER NOT NULL,
				rapi_id INTEGER NOT NULL,
				reason TEXT NOT NULL DEFAULT '',
				expires_at DATETIME,
				created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (key_id, rapi_id),
				FOREIGN KEY (key_id) REFERENCES credential(id) ON DELETE CASCADE,
				FOREIGN KEY (rapi_id) REFERENCES rapi(id) ON DELETE CASCADE
			);
			INSERT OR IGNORE INTO key_model_blocks_new (key_id, rapi_id, reason, expires_at, created_at)
				SELECT key_id, rapi_id, reason, expires_at, created_at FROM key_model_blocks;
			DROP TABLE key_model_blocks;
			ALTER TABLE key_model_blocks_new RENAME TO key_model_blocks;
		`); err != nil {
			return rep, fmt.Errorf("rebuild key_model_blocks: %w", err)
		}
	}
	if rapiNeedsRebuild {
		if err := rebuildRAPITx(tx); err != nil {
			return rep, fmt.Errorf("rebuild rapi: %w", err)
		}
	}
	if platNeedsRebuild {
		if err := rebuildPlatformTx(tx); err != nil {
			return rep, fmt.Errorf("rebuild platform: %w", err)
		}
	}

	// E. 自然键唯一索引（部分索引：空值不参与，兼容动态平台/历史空 model）。
	if _, err := tx.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS idx_platform_base_url
			ON platform(base_url) WHERE base_url <> '';
		CREATE UNIQUE INDEX IF NOT EXISTS idx_rapi_platform_model
			ON rapi(platform_id, model) WHERE model <> '';
	`); err != nil {
		return rep, fmt.Errorf("natural-key indexes: %w", err)
	}

	// F. key_ids 重建为绑定投影（credential id CSV，运行时消费方零改动）。
	if _, err := tx.Exec(`
		UPDATE rapi SET key_ids = COALESCE((
			SELECT GROUP_CONCAT(credential_id) FROM (
				SELECT credential_id FROM endpoint_credential
				WHERE rapi_id = rapi.id AND enabled = 1 ORDER BY credential_id
			)
		), '')
	`); err != nil {
		return rep, fmt.Errorf("refresh key_ids: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return rep, err
	}
	return rep, nil
}

// migratePlatformKeysTx 把 platform_keys 平移进 credential：
//   - 保留原行 id（key_model_blocks / token_cache / rapi.key_ids 里的旧引用免换算）；
//   - 同 token_hash 的多行合并为 id 最小的一行，被合并行的引用改挂到保留行；
//   - 按旧 key_ids 语义物化 endpoint_credential 绑定（空=该平台全部 key）；
//   - 最后 DROP platform_keys。
func migratePlatformKeysTx(tx *sql.Tx, rep *NaturalKeyReport) error {
	rows, err := tx.Query(`SELECT id, platform_id, key_index, token, label, enabled,
		failure_type, failure_reason, failed_at, expires_at, is_free FROM platform_keys ORDER BY id ASC`)
	if err != nil {
		return err
	}
	type pkRow struct {
		id, platformID       int64
		keyIndex             int
		encToken, label      string
		enabled, failureType int
		failureReason        string
		failedAt, expiresAt  sql.NullTime
		isFree               int
	}
	var all []pkRow
	for rows.Next() {
		var r pkRow
		if err := rows.Scan(&r.id, &r.platformID, &r.keyIndex, &r.encToken, &r.label,
			&r.enabled, &r.failureType, &r.failureReason, &r.failedAt, &r.expiresAt, &r.isFree); err != nil {
			rows.Close()
			return err
		}
		all = append(all, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// 已有 credential 也参与去重/ID 分配。混合状态（终态 credential 与旧
	// platform_keys 同时存在）不能再假设 credential 为空或旧 ID 一定可用。
	seen := make(map[string]int64, len(all)) // token_hash → 保留 credential id
	existingRows, err := tx.Query(`SELECT id, token_hash FROM credential WHERE token_hash IS NOT NULL`)
	if err != nil {
		return err
	}
	for existingRows.Next() {
		var id int64
		var hash string
		if err := existingRows.Scan(&id, &hash); err != nil {
			existingRows.Close()
			return err
		}
		seen[hash] = id
	}
	existingRows.Close()
	if err := existingRows.Err(); err != nil {
		return err
	}

	idMap := make(map[int64]int64, len(all)) // 旧 platform_keys.id → credential.id
	dupOf := make(map[int64]int64)           // 被合并旧行 id → 保留 credential.id
	for _, r := range all {
		plain, derr := crypto.Decrypt(r.encToken)
		if derr != nil {
			plain = r.encToken // 解不开就用原文算 hash（确定性，不丢行）
		}
		h := models.TokenHash(plain)
		if h != "" {
			if keep, dup := seen[h]; dup {
				idMap[r.id] = keep
				if keep != r.id {
					dupOf[r.id] = keep
					rep.CredentialsMerged = append(rep.CredentialsMerged,
						CredentialMerge{KeptID: keep, DroppedID: r.id, TokenHash: h})
				}
				continue
			}
		}
		var hashArg any
		if h != "" {
			hashArg = h
		}

		// 旧 ID 未被占用时保留它，保证 key_model_blocks / token_cache /
		// rapi.key_ids 无需换算；冲突时让 SQLite 分配新 ID，并记录映射。
		var credentialID int64
		if err := tx.QueryRow(`SELECT 1 FROM credential WHERE id = ?`, r.id).Scan(&credentialID); err == nil {
			res, err := tx.Exec(`INSERT INTO credential
				(platform_id, token_hash, token, label, enabled, failure_type, failure_reason,
				 failed_at, expires_at, is_free, sort_order)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				r.platformID, hashArg, r.encToken, r.label, r.enabled, r.failureType,
				r.failureReason, r.failedAt, r.expiresAt, r.isFree, r.keyIndex)
			if err != nil {
				return fmt.Errorf("insert credential from key %d: %w", r.id, err)
			}
			credentialID, err = res.LastInsertId()
			if err != nil {
				return fmt.Errorf("credential id from key %d: %w", r.id, err)
			}
			dupOf[r.id] = credentialID
		} else if err != sql.ErrNoRows {
			return fmt.Errorf("probe credential id %d: %w", r.id, err)
		} else {
			if _, err := tx.Exec(`INSERT INTO credential
				(id, platform_id, token_hash, token, label, enabled, failure_type, failure_reason,
				 failed_at, expires_at, is_free, sort_order)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				r.id, r.platformID, hashArg, r.encToken, r.label, r.enabled, r.failureType,
				r.failureReason, r.failedAt, r.expiresAt, r.isFree, r.keyIndex); err != nil {
				return fmt.Errorf("insert credential from key %d: %w", r.id, err)
			}
			credentialID = r.id
		}
		idMap[r.id] = credentialID
		if h != "" {
			seen[h] = credentialID
		}
		rep.CredentialsCreated++
	}

	// 被合并/改号的旧引用统一映射到最终 credential.id。token_cache 是可重建
	// 缓存，旧列不存在时直接跳过，保证迁移也可从部分终态安全重入。
	for oldID, keep := range dupOf {
		if oldID == keep {
			continue
		}
		if _, err := tx.Exec(`UPDATE OR IGNORE key_model_blocks SET key_id = ? WHERE key_id = ?`, keep, oldID); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM key_model_blocks WHERE key_id = ?`, oldID); err != nil {
			return err
		}
		if column, err := tableColumnInTx(tx, "token_cache", "platform_key_id"); err != nil {
			return err
		} else if column {
			if _, err := tx.Exec(`DELETE FROM token_cache WHERE platform_key_id = ?`, oldID); err != nil {
				return err
			}
		}
	}

	// 物化绑定：key_ids 非空 → 白名单（引用改挂后去重）；空 → 平台全部 credential。
	credByPlatform := make(map[int64][]int64)
	credRows, err := tx.Query(`SELECT id, platform_id FROM credential ORDER BY sort_order ASC, id ASC`)
	if err != nil {
		return err
	}
	for credRows.Next() {
		var cid, pid int64
		if err := credRows.Scan(&cid, &pid); err != nil {
			credRows.Close()
			return err
		}
		credByPlatform[pid] = append(credByPlatform[pid], cid)
	}
	credRows.Close()
	if err := credRows.Err(); err != nil {
		return err
	}

	rapiRows, err := tx.Query(`SELECT id, platform_id, key_ids FROM rapi`)
	if err != nil {
		return err
	}
	type rapiRef struct {
		id, platformID int64
		keyIDs         string
	}
	var rapis []rapiRef
	for rapiRows.Next() {
		var r rapiRef
		if err := rapiRows.Scan(&r.id, &r.platformID, &r.keyIDs); err != nil {
			rapiRows.Close()
			return err
		}
		rapis = append(rapis, r)
	}
	rapiRows.Close()
	if err := rapiRows.Err(); err != nil {
		return err
	}

	for _, r := range rapis {
		bound := credByPlatform[r.platformID]
		if ids := parseIDCSV(r.keyIDs); len(ids) > 0 {
			bound = nil
			seenID := map[int64]bool{}
			for _, id := range ids {
				if mapped, ok := idMap[id]; ok {
					id = mapped
				}
				if seenID[id] {
					continue
				}
				seenID[id] = true
				bound = append(bound, id)
			}
		}
		for _, cid := range bound {
			res, err := tx.Exec(`INSERT OR IGNORE INTO endpoint_credential
				(rapi_id, credential_id, enabled) VALUES (?, ?, 1)`, r.id, cid)
			if err != nil {
				return fmt.Errorf("bind rapi %d credential %d: %w", r.id, cid, err)
			}
			if n, _ := res.RowsAffected(); n > 0 {
				rep.BindingsCreated++
			}
		}
	}

	if _, err := tx.Exec(`DROP TABLE platform_keys`); err != nil {
		return fmt.Errorf("drop platform_keys: %w", err)
	}
	return nil
}

// parseIDCSV 解析逗号分隔 id 列表（容忍空白/垃圾段）。
func parseIDCSV(s string) []int64 {
	var out []int64
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var id int64
		if _, err := fmt.Sscanf(p, "%d", &id); err == nil {
			out = append(out, id)
		}
	}
	return out
}

// mergeDuplicateRAPIsTx 把同 (platform_id, model)（model 非空）的重复 rapi 合并到
// id 最小的一行：统计并入、引用改挂、绑定并入、重复行删除。循环直至无重复组
// （平台合并可能制造新的碰撞组，由调用方再跑一轮）。
func mergeDuplicateRAPIsTx(tx *sql.Tx) ([]RAPIMerge, error) {
	var out []RAPIMerge
	for {
		rows, err := tx.Query(`SELECT platform_id, model, GROUP_CONCAT(id) FROM rapi
			WHERE model <> '' GROUP BY platform_id, model HAVING COUNT(*) > 1`)
		if err != nil {
			return out, err
		}
		type group struct {
			platformID int64
			model      string
			ids        []int64
		}
		var groups []group
		for rows.Next() {
			var g group
			var csv string
			if err := rows.Scan(&g.platformID, &g.model, &csv); err != nil {
				rows.Close()
				return out, err
			}
			g.ids = parseIDCSV(csv)
			groups = append(groups, g)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return out, err
		}
		if len(groups) == 0 {
			return out, nil
		}
		for _, g := range groups {
			if len(g.ids) < 2 {
				continue
			}
			keep := g.ids[0] // rowid 序扫描 → GROUP_CONCAT 内 id 升序，最小 id 在前
			for _, dup := range g.ids[1:] {
				if err := mergeRAPIRefsTx(tx, keep, dup); err != nil {
					return out, err
				}
				out = append(out, RAPIMerge{PlatformID: g.platformID, KeptID: keep, DroppedID: dup, Model: g.model})
			}
		}
	}
}

// mergeRAPIRefsTx 把 dup rapi 的全部引用/统计并入 keep，然后删除 dup。
func mergeRAPIRefsTx(tx *sql.Tx, keep, dup int64) error {
	// 路由映射：冲突（同 lapi 已挂 keep）的行靠 OR IGNORE 跳过，残留直接删。
	if _, err := tx.Exec(`UPDATE OR IGNORE lapi_rapi_order SET rapi_id = ? WHERE rapi_id = ?`, keep, dup); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM lapi_rapi_order WHERE rapi_id = ?`, dup); err != nil {
		return err
	}
	// 统计：按 (rapi_id, lapi_id) 并入保留行；保留行没有该 lapi 的桶则整行改挂。
	mrows, err := tx.Query(`SELECT lapi_id, total_requests, success_requests,
		fail_401, fail_429, fail_500, COALESCE(fail_other, 0), total_latency_ms, token_count, last_used
		FROM rapi_metrics WHERE rapi_id = ?`, dup)
	if err != nil {
		return err
	}
	type mrow struct {
		lapiID                                 int64
		total, success, f401, f429, f500, fOth int
		latency, tokens                        int
		lastUsed                               sql.NullString
	}
	var ms []mrow
	for mrows.Next() {
		var m mrow
		if err := mrows.Scan(&m.lapiID, &m.total, &m.success, &m.f401, &m.f429, &m.f500,
			&m.fOth, &m.latency, &m.tokens, &m.lastUsed); err != nil {
			mrows.Close()
			return err
		}
		ms = append(ms, m)
	}
	mrows.Close()
	if err := mrows.Err(); err != nil {
		return err
	}
	for _, m := range ms {
		res, err := tx.Exec(`UPDATE rapi_metrics SET
			total_requests = total_requests + ?, success_requests = success_requests + ?,
			fail_401 = fail_401 + ?, fail_429 = fail_429 + ?, fail_500 = fail_500 + ?,
			fail_other = COALESCE(fail_other, 0) + ?, total_latency_ms = total_latency_ms + ?,
			token_count = token_count + ?,
			last_used = MAX(COALESCE(last_used, ''), COALESCE(?, ''))
			WHERE rapi_id = ? AND lapi_id = ?`,
			m.total, m.success, m.f401, m.f429, m.f500, m.fOth, m.latency, m.tokens,
			m.lastUsed.String, keep, m.lapiID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			if _, err := tx.Exec(`UPDATE rapi_metrics SET rapi_id = ? WHERE rapi_id = ? AND lapi_id = ?`,
				keep, dup, m.lapiID); err != nil {
				return err
			}
		}
	}
	if _, err := tx.Exec(`DELETE FROM rapi_metrics WHERE rapi_id = ?`, dup); err != nil {
		return err
	}
	// 能力封锁 / 绑定：冲突跳过，残留删除。
	for _, stmt := range [][2]string{
		{`UPDATE OR IGNORE key_model_blocks SET rapi_id = ? WHERE rapi_id = ?`, `DELETE FROM key_model_blocks WHERE rapi_id = ?`},
		{`UPDATE OR IGNORE endpoint_credential SET rapi_id = ? WHERE rapi_id = ?`, `DELETE FROM endpoint_credential WHERE rapi_id = ?`},
	} {
		if _, err := tx.Exec(stmt[0], keep, dup); err != nil {
			return err
		}
		if _, err := tx.Exec(stmt[1], dup); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`DELETE FROM rapi WHERE id = ?`, dup); err != nil {
		return err
	}
	return nil
}

// mergePlatformsTx 归一化所有 platform.base_url，并把归一化后撞车的平台合并到
// id 最小的一行（rapi/credential 改挂，重复平台删除）。空 base_url 不参与合并。
func mergePlatformsTx(tx *sql.Tx) ([]PlatformMerge, int, error) {
	var merges []PlatformMerge
	renorm := 0
	rows, err := tx.Query(`SELECT id, name, base_url FROM platform ORDER BY id ASC`)
	if err != nil {
		return nil, 0, err
	}
	type prow struct {
		id      int64
		name    string
		baseURL string
	}
	var plats []prow
	for rows.Next() {
		var p prow
		if err := rows.Scan(&p.id, &p.name, &p.baseURL); err != nil {
			rows.Close()
			return nil, 0, err
		}
		plats = append(plats, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	byNorm := make(map[string][]prow)
	for _, p := range plats {
		nb := models.NormalizeBaseURL(p.baseURL)
		if nb != p.baseURL {
			if _, err := tx.Exec(`UPDATE platform SET base_url = ? WHERE id = ?`, nb, p.id); err != nil {
				return merges, renorm, err
			}
			renorm++
		}
		if nb == "" {
			continue // 空地址平台（动态/占位）不参与合并与唯一约束
		}
		byNorm[nb] = append(byNorm[nb], prow{id: p.id, name: p.name, baseURL: nb})
	}
	for nb, group := range byNorm {
		if len(group) < 2 {
			continue
		}
		keep := group[0]
		for _, dup := range group[1:] {
			if _, err := tx.Exec(`UPDATE rapi SET platform_id = ? WHERE platform_id = ?`, keep.id, dup.id); err != nil {
				return merges, renorm, err
			}
			if _, err := tx.Exec(`UPDATE credential SET platform_id = ? WHERE platform_id = ?`, keep.id, dup.id); err != nil {
				return merges, renorm, err
			}
			if _, err := tx.Exec(`DELETE FROM platform WHERE id = ?`, dup.id); err != nil {
				return merges, renorm, err
			}
			merges = append(merges, PlatformMerge{
				KeptID: keep.id, DroppedID: dup.id,
				KeptName: keep.name, DroppedName: dup.name, BaseURL: nb,
			})
		}
	}
	return merges, renorm, nil
}

// rebuildRAPITx 重建 rapi 表：去掉 alias 上的旧唯一约束（alias 降级为显示名），
// 身份唯一性改由 idx_rapi_platform_model 部分索引承担。列清单动态取交集，
// 历史迁移加过的列不会丢（与 migrateRAPIUniqueAliasToPerPlatform 同一模式）。
func rebuildRAPITx(tx *sql.Tx) error {
	if _, err := tx.Exec(`
		CREATE TABLE rapi_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			alias TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			vendor TEXT NOT NULL DEFAULT '',
			series TEXT NOT NULL DEFAULT '',
			model_name TEXT NOT NULL DEFAULT '',
			version TEXT NOT NULL DEFAULT '',
			suffix TEXT NOT NULL DEFAULT '',
			platform_id INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			unavailable_reason TEXT NOT NULL DEFAULT '',
			base_cost INTEGER NOT NULL DEFAULT 0,
			high_cost INTEGER NOT NULL DEFAULT 0,
			rpm_limit INTEGER NOT NULL DEFAULT 0,
			rph_limit INTEGER NOT NULL DEFAULT 0,
			rpd_limit INTEGER NOT NULL DEFAULT 0,
			tpm_limit INTEGER NOT NULL DEFAULT 0,
			tph_limit INTEGER NOT NULL DEFAULT 0,
			tpd_limit INTEGER NOT NULL DEFAULT 0,
			time_period_rules TEXT DEFAULT '',
			supported_formats TEXT NOT NULL DEFAULT '["openai"]',
			custom_headers TEXT NOT NULL DEFAULT '',
			key_ids TEXT NOT NULL DEFAULT '',
			notes TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT 'manual',
			sort_order INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE
		);
	`); err != nil {
		return err
	}
	keep := []string{"id", "alias", "model", "vendor", "series", "model_name", "version", "suffix",
		"platform_id", "enabled", "available", "unavailable_reason", "base_cost", "high_cost",
		"rpm_limit", "rph_limit", "rpd_limit", "tpm_limit", "tph_limit", "tpd_limit",
		"time_period_rules", "supported_formats", "custom_headers", "key_ids", "notes",
		"source", "sort_order", "created_at", "updated_at"}
	copySQL, err := buildCopyCommonColumns(tx, "rapi", "rapi_new", keep)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(copySQL); err != nil {
		return err
	}
	_, err = tx.Exec(`
		DROP TABLE rapi;
		ALTER TABLE rapi_new RENAME TO rapi;
		CREATE INDEX IF NOT EXISTS idx_rapi_platform ON rapi(platform_id);
	`)
	return err
}

// rebuildPlatformTx 重建 platform 表：去掉 name 的旧 UNIQUE（name 降级为显示名），
// 身份唯一性由 idx_platform_base_url 部分索引承担。
func rebuildPlatformTx(tx *sql.Tx) error {
	if _, err := tx.Exec(`
		CREATE TABLE platform_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			base_url TEXT NOT NULL DEFAULT '',
			token TEXT NOT NULL DEFAULT '',
			last_token_fetch DATETIME,
			enabled INTEGER NOT NULL DEFAULT 1,
			available INTEGER NOT NULL DEFAULT 1,
			notes TEXT NOT NULL DEFAULT '',
			supported_formats TEXT NOT NULL DEFAULT '["openai"]',
			format_endpoints TEXT NOT NULL DEFAULT '',
			custom_headers TEXT NOT NULL DEFAULT '',
			billing_address TEXT NOT NULL DEFAULT '',
			login_account TEXT NOT NULL DEFAULT '',
			login_password TEXT NOT NULL DEFAULT '',
			sort_order INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
	`); err != nil {
		return err
	}
	keep := []string{"id", "name", "base_url", "token", "last_token_fetch", "enabled", "available",
		"notes", "supported_formats", "format_endpoints", "custom_headers", "billing_address",
		"login_account", "login_password", "sort_order", "created_at", "updated_at"}
	copySQL, err := buildCopyCommonColumns(tx, "platform", "platform_new", keep)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(copySQL); err != nil {
		return err
	}
	_, err = tx.Exec(`
		DROP TABLE platform;
		ALTER TABLE platform_new RENAME TO platform;
	`)
	return err
}

// ---- schema 探测小工具（迁移幂等判断用） ----

func tableExists(conn *sql.DB, name string) bool {
	var cnt int
	if err := conn.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&cnt); err != nil {
		return false
	}
	return cnt > 0
}

// staleBackfillResidue 判断 platform_keys 是否只是旧版本回填 bug 的残影，返回
// (是否残影, 行数)。判据（两条同时成立才认定，宁可保留也不误删真实数据）：
//  1. credential 表存在且非空 —— 说明自然键迁移早已成功提交；
//  2. platform_keys 非空，且每一行都恰好是 migrateAddPlatformKeys 回填的产物：
//     key_index=0、label='default'、token 等于该平台 platform.token 列的值。
//
// 混合状态不满足第二项，会进入事务按 token/ID 无损失冲量。
func staleBackfillResidue(conn *sql.DB) (bool, int, error) {
	var credRows int
	if err := conn.QueryRow(`SELECT COUNT(*) FROM credential`).Scan(&credRows); err != nil {
		return false, 0, err
	}
	if credRows == 0 {
		return false, 0, nil
	}

	var total, matching int
	if err := conn.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN pk.key_index = 0 AND pk.label = 'default'
			AND pk.token = p.token THEN 1 ELSE 0 END), 0)
		FROM platform_keys pk LEFT JOIN platform p ON p.id = pk.platform_id
	`).Scan(&total, &matching); err != nil {
		return false, 0, err
	}
	return total > 0 && total == matching, total, nil
}

func tableColumnInTx(tx *sql.Tx, table, col string) (bool, error) {
	rows, err := tx.Query(`SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if name == col {
			return true, nil
		}
	}
	return false, rows.Err()
}

func tableSchemaSQL(conn *sql.DB, name string) string {
	var s sql.NullString
	if err := conn.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&s); err != nil {
		return ""
	}
	return s.String
}

func columnExists(conn *sql.DB, table, col string) bool {
	var cnt int
	if err := conn.QueryRow(
		`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?`, table, col).Scan(&cnt); err != nil {
		return false
	}
	return cnt > 0
}

// PreviewNaturalKeyMigration 只读分析将发生的合并（dry-run 清单），不落任何写。
// 与 MigrateNaturalKeysConn 的判定规则保持一致：credential 按 token_hash、
// rapi 按"平台合并后的 (platform, model)"、platform 按归一化 base_url。
func PreviewNaturalKeyMigration(conn *sql.DB) (*NaturalKeyReport, error) {
	rep := &NaturalKeyReport{}

	// platform：归一化 + 合并预览（同时产出 id→保留 id 映射供 rapi 预览用）。
	platRows, err := conn.Query(`SELECT id, name, base_url FROM platform ORDER BY id ASC`)
	if err != nil {
		return rep, err
	}
	type prow struct {
		id      int64
		name    string
		baseURL string
	}
	var plats []prow
	for platRows.Next() {
		var p prow
		if err := platRows.Scan(&p.id, &p.name, &p.baseURL); err != nil {
			platRows.Close()
			return rep, err
		}
		plats = append(plats, p)
	}
	platRows.Close()
	if err := platRows.Err(); err != nil {
		return rep, err
	}
	platCanon := map[int64]int64{}
	byNorm := map[string][]prow{}
	for _, p := range plats {
		nb := models.NormalizeBaseURL(p.baseURL)
		if nb != p.baseURL {
			rep.PlatformsRenormalized++
		}
		if nb == "" {
			continue
		}
		byNorm[nb] = append(byNorm[nb], prow{p.id, p.name, nb})
	}
	for nb, group := range byNorm {
		for _, p := range group[1:] {
			rep.PlatformsMerged = append(rep.PlatformsMerged, PlatformMerge{
				KeptID: group[0].id, DroppedID: p.id,
				KeptName: group[0].name, DroppedName: p.name, BaseURL: nb,
			})
		}
	}
	for _, p := range plats {
		platCanon[p.id] = p.id
	}
	for _, m := range rep.PlatformsMerged {
		platCanon[m.DroppedID] = m.KeptID
	}

	// rapi：按平台合并后的 (platform, model) 预览。
	rRows, err := conn.Query(`SELECT id, platform_id, model FROM rapi WHERE model <> '' ORDER BY id ASC`)
	if err != nil {
		return rep, err
	}
	type rkey struct {
		pid   int64
		model string
	}
	rSeen := map[rkey]int64{}
	for rRows.Next() {
		var id, pid int64
		var model string
		if err := rRows.Scan(&id, &pid, &model); err != nil {
			rRows.Close()
			return rep, err
		}
		k := rkey{platCanon[pid], model}
		if keep, dup := rSeen[k]; dup {
			rep.RAPIsMerged = append(rep.RAPIsMerged,
				RAPIMerge{PlatformID: k.pid, KeptID: keep, DroppedID: id, Model: model})
			continue
		}
		rSeen[k] = id
	}
	rRows.Close()
	if err := rRows.Err(); err != nil {
		return rep, err
	}

	// credential：platform_keys 仍存在时按 token_hash 预览。
	if tableExists(conn, "platform_keys") {
		kRows, err := conn.Query(`SELECT id, token FROM platform_keys ORDER BY id ASC`)
		if err != nil {
			return rep, err
		}
		kSeen := map[string]int64{}
		for kRows.Next() {
			var id int64
			var enc string
			if err := kRows.Scan(&id, &enc); err != nil {
				kRows.Close()
				return rep, err
			}
			plain, derr := crypto.Decrypt(enc)
			if derr != nil {
				plain = enc
			}
			h := models.TokenHash(plain)
			if h == "" {
				rep.CredentialsCreated++
				continue
			}
			if keep, dup := kSeen[h]; dup {
				rep.CredentialsMerged = append(rep.CredentialsMerged,
					CredentialMerge{KeptID: keep, DroppedID: id, TokenHash: h})
				continue
			}
			kSeen[h] = id
			rep.CredentialsCreated++
		}
		kRows.Close()
		if err := kRows.Err(); err != nil {
			return rep, err
		}
	}
	return rep, nil
}
