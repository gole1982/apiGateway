// credential.go —— credential / endpoint_credential 的 CRUD 与共享助手。
//
// 自然键身份模型（docs/superpowers/specs/2026-09-23-natural-key-identity-design.md）：
// 凭据全局唯一（token_hash），端点↔凭据经 endpoint_credential 物化绑定。
// 运行时与 dashboard 仍消费 models.PlatformKey 投影（id = credential.id）与
// rapi.key_ids（绑定派生的 credential id CSV，由本文件的 refresh 函数维护），
// 因此调度器 / 网关 / 前端在 Phase 1 零改动。
package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gateway/internal/crypto"
	"gateway/internal/models"
)

// credentialSelectCols 是 credential 表的标准投影列（顺序与 scanCredentialRows 对应）。
const credentialSelectCols = `id, platform_id, sort_order, token, label, enabled,
	failure_type, failure_reason, failed_at, created_at, updated_at, expires_at, is_free`

// scanCredentialRows 把 credential 查询结果映射为 PlatformKey 投影：
// id 保留 credential.id（调度器/缓存/封锁引用不变），KeyIndex 赋值为
// 归属平台内的展示序号（0 起，按 sort_order,id 排序后的行号）。
func scanCredentialRows(rows *sql.Rows) ([]models.PlatformKey, error) {
	keys := make([]models.PlatformKey, 0)
	idxByPlatform := map[int64]int{}
	for rows.Next() {
		var k models.PlatformKey
		var encToken string
		var enabled sql.NullInt64
		var failedAt, expiresAt, created, updated sql.NullTime
		var isFree, sortOrder int
		if err := rows.Scan(&k.ID, &k.PlatformID, &sortOrder, &encToken, &k.Label, &enabled,
			&k.FailureType, &k.FailureReason, &failedAt, &created, &updated,
			&expiresAt, &isFree); err != nil {
			return nil, err
		}
		var decErr error
		if k.Token, decErr = crypto.Decrypt(encToken); decErr != nil {
			return nil, fmt.Errorf("decrypt credential %d: %w", k.ID, decErr)
		}
		k.Enabled = enabled.Int64 != 0
		k.IsFree = isFree != 0
		k.KeyIndex = idxByPlatform[k.PlatformID]
		idxByPlatform[k.PlatformID]++
		if failedAt.Valid {
			k.FailedAt = &failedAt.Time
		}
		if expiresAt.Valid {
			k.ExpiresAt = &expiresAt.Time
		}
		if created.Valid {
			k.CreatedAt = created.Time
		}
		if updated.Valid {
			k.UpdatedAt = updated.Time
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// wrapCredErr 把 token_hash 唯一冲突翻成可读中文（凭据按 token 全局唯一）。
func wrapCredErr(err error) error {
	if err != nil && strings.Contains(err.Error(), "idx_credential_token_hash") {
		return fmt.Errorf("该 token 已登记过：凭据按 token 内容全局唯一，无需重复添加")
	}
	return err
}

// tokenHashArg 计算写入用的 token_hash 绑定值：空 token → nil（NULL，不参与唯一）。
func tokenHashArg(plainToken string) any {
	if h := models.TokenHash(plainToken); h != "" {
		return h
	}
	return nil
}

// refreshKeyIDsTx 重建指定 rapi 的 key_ids 派生列（= 启用绑定的 credential id CSV）。
func refreshKeyIDsTx(tx *sql.Tx, rapiIDs ...int64) error {
	for _, id := range rapiIDs {
		if _, err := tx.Exec(`
			UPDATE rapi SET key_ids = COALESCE((
				SELECT GROUP_CONCAT(credential_id) FROM (
					SELECT credential_id FROM endpoint_credential
					WHERE rapi_id = ? AND enabled = 1 ORDER BY credential_id
				)
			), '') WHERE id = ?
		`, id, id); err != nil {
			return fmt.Errorf("refresh key_ids for rapi %d: %w", id, err)
		}
	}
	return nil
}

// refreshKeyIDsForPlatformTx 重建某平台全部 rapi 的 key_ids 派生列。
func refreshKeyIDsForPlatformTx(tx *sql.Tx, platformID int64) error {
	_, err := tx.Exec(`
		UPDATE rapi SET key_ids = COALESCE((
			SELECT GROUP_CONCAT(credential_id) FROM (
				SELECT ec.credential_id FROM endpoint_credential ec
				WHERE ec.rapi_id = rapi.id AND ec.enabled = 1 ORDER BY ec.credential_id
			)
		), '') WHERE platform_id = ?
	`, platformID)
	return err
}

// syncBindingsTx 按 key_ids CSV 语义重建某 rapi 的绑定集合并刷新派生列：
// 空串 = 归属平台全部凭据（旧默认语义）；非空 = 白名单中的 credential id，
// 但只保留本平台真实存在的 id（悬空/他平台 id 丢弃：endpoint_credential 有
// FK，且运行时本来就静默忽略未知 id）。不在集合内的旧绑定删除，新 id 以
// enabled=1 补绑。
func syncBindingsTx(tx *sql.Tx, rapiID, platformID int64, keyIDsCSV string) error {
	valid := map[int64]bool{}
	rows, err := tx.Query(`SELECT id FROM credential WHERE platform_id = ?`, platformID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		valid[id] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	want := map[int64]bool{}
	if strings.TrimSpace(keyIDsCSV) == "" {
		want = valid
	} else {
		for _, id := range parseIDCSV(keyIDsCSV) {
			if valid[id] {
				want[id] = true
			}
		}
	}

	curRows, err := tx.Query(`SELECT credential_id FROM endpoint_credential WHERE rapi_id = ?`, rapiID)
	if err != nil {
		return err
	}
	var cur []int64
	for curRows.Next() {
		var id int64
		if err := curRows.Scan(&id); err != nil {
			curRows.Close()
			return err
		}
		cur = append(cur, id)
	}
	curRows.Close()
	if err := curRows.Err(); err != nil {
		return err
	}

	for _, cid := range cur {
		if !want[cid] {
			if _, err := tx.Exec(`DELETE FROM endpoint_credential WHERE rapi_id = ? AND credential_id = ?`,
				rapiID, cid); err != nil {
				return err
			}
		}
	}
	for cid := range want {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO endpoint_credential
			(rapi_id, credential_id, enabled) VALUES (?, ?, 1)`, rapiID, cid); err != nil {
			return err
		}
	}
	return refreshKeyIDsTx(tx, rapiID)
}

// bindCredentialToPlatformRAPIsTx 把新凭据绑定到归属平台的全部端点
// （旧"key_ids 空 = 平台全部 key"语义的对偶：新增 key 默认服务全部模型）。
func bindCredentialToPlatformRAPIsTx(tx *sql.Tx, platformID, credentialID int64) error {
	_, err := tx.Exec(`
		INSERT OR IGNORE INTO endpoint_credential (rapi_id, credential_id, enabled)
		SELECT id, ?, 1 FROM rapi WHERE platform_id = ?
	`, credentialID, platformID)
	return err
}

// derivedKeyIDs 读回某 rapi 的 key_ids 派生列当前值（写入路径提交后回显用）。
func (db *DB) derivedKeyIDs(rapiID int64) string {
	var s string
	if err := db.conn.QueryRow(`SELECT key_ids FROM rapi WHERE id = ?`, rapiID).Scan(&s); err != nil {
		return ""
	}
	return s
}

// GetAllEndpointCredentials 读出全部端点↔凭据绑定（push 快照/排障用）。
func (db *DB) GetAllEndpointCredentials() ([]models.EndpointCredential, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	rows, err := db.conn.Query(`SELECT id, rapi_id, credential_id,
		rpm_limit, rph_limit, rpd_limit, tpm_limit, tph_limit, tpd_limit, enabled
		FROM endpoint_credential ORDER BY rapi_id ASC, credential_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.EndpointCredential, 0)
	for rows.Next() {
		var b models.EndpointCredential
		var enabled int
		if err := rows.Scan(&b.ID, &b.RAPIID, &b.CredentialID,
			&b.RPMLimit, &b.RPHLimit, &b.RPDLimit, &b.TPMLimit, &b.TPHLimit, &b.TPDLimit,
			&enabled); err != nil {
			return nil, err
		}
		b.Enabled = enabled != 0
		out = append(out, b)
	}
	return out, rows.Err()
}

// ============ Credential CRUD（PlatformKey 投影；签名与历史 db.DB 一致） ============

// GetPlatformKeys returns the platform's credentials as PlatformKey projections
// (id = credential.id, KeyIndex = display position), ordered by sort_order.
// Phase 1 不变式：绑定不跨归属平台，因此"平台的 key 池"= 归属平台凭据集。
func (db *DB) GetPlatformKeys(platformID int64) ([]models.PlatformKey, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT `+credentialSelectCols+`
		FROM credential WHERE platform_id = ? ORDER BY sort_order ASC, id ASC
	`, platformID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCredentialRows(rows)
}

// GetAllPlatformKeys returns every credential across all platforms.
// Used by insights/health aggregation to avoid N+1 per-platform queries.
func (db *DB) GetAllPlatformKeys() ([]models.PlatformKey, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT ` + credentialSelectCols + `
		FROM credential ORDER BY platform_id ASC, sort_order ASC, id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCredentialRows(rows)
}

// EnsureDynamicPlatformKey 保证平台存在一行空 token 占位凭据（动态令牌平台用）。
// 空 token → token_hash NULL，不参与全局唯一。返回该行 id。
func (db *DB) EnsureDynamicPlatformKey(platformID int64) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, err := db.conn.Exec(`
		INSERT INTO credential (platform_id, token_hash, token, label, enabled)
		SELECT ?, NULL, '', 'dynamic', 1
		WHERE NOT EXISTS (SELECT 1 FROM credential WHERE platform_id = ? AND token = '')
	`, platformID, platformID); err != nil {
		return 0, fmt.Errorf("ensure dynamic credential failed: %w", err)
	}

	var keyID int64
	err := db.conn.QueryRow(
		`SELECT id FROM credential WHERE platform_id = ? AND token = '' ORDER BY id ASC LIMIT 1`,
		platformID,
	).Scan(&keyID)
	return keyID, err
}

// SetPlatformKeys replaces all credentials of a platform with the provided list.
// 新增（id=0）插入并绑定平台全部端点；保留行更新；消失的 id 删除（绑定/封锁
// 随 FK 级联）。rapi.key_ids 是派生列，提交前整体重建，无需逐行换算。
func (db *DB) SetPlatformKeys(platformID int64, keys []models.PlatformKey) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	existRows, err := tx.Query(`SELECT id FROM credential WHERE platform_id = ?`, platformID)
	if err != nil {
		return err
	}
	exist := map[int64]bool{}
	for existRows.Next() {
		var id int64
		if err := existRows.Scan(&id); err != nil {
			existRows.Close()
			return err
		}
		exist[id] = true
	}
	existRows.Close()
	if err := existRows.Err(); err != nil {
		return err
	}

	keep := map[int64]bool{}
	for i := range keys {
		k := &keys[i]
		enc, err := crypto.Encrypt(k.Token)
		if err != nil {
			return fmt.Errorf("encrypt key: %w", err)
		}
		if k.ID > 0 && exist[k.ID] {
			if _, err := tx.Exec(`
				UPDATE credential SET token = ?, token_hash = ?, label = ?, enabled = ?,
				       expires_at = ?, is_free = ?, sort_order = ?, updated_at = CURRENT_TIMESTAMP
				WHERE id = ?
			`, enc, tokenHashArg(k.Token), k.Label, boolToInt(k.Enabled),
				expiresArg(k.ExpiresAt), boolToInt(k.IsFree), i, k.ID); err != nil {
				return wrapCredErr(err)
			}
			keep[k.ID] = true
		} else {
			res, err := tx.Exec(`
				INSERT INTO credential (platform_id, token, token_hash, label, enabled,
					expires_at, is_free, sort_order)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, platformID, enc, tokenHashArg(k.Token), k.Label, boolToInt(k.Enabled),
				expiresArg(k.ExpiresAt), boolToInt(k.IsFree), i)
			if err != nil {
				return wrapCredErr(err)
			}
			newID, _ := res.LastInsertId()
			keep[newID] = true
			if err := bindCredentialToPlatformRAPIsTx(tx, platformID, newID); err != nil {
				return err
			}
		}
	}

	for id := range exist {
		if !keep[id] {
			if _, err := tx.Exec(`DELETE FROM credential WHERE id = ?`, id); err != nil {
				return err
			}
		}
	}

	if err := refreshKeyIDsForPlatformTx(tx, platformID); err != nil {
		return err
	}
	return tx.Commit()
}

// AddPlatformKey appends a credential to a platform and binds it to all of the
// platform's endpoints（新 key 默认服务全部模型，与原"空 key_ids"语义一致）。
// 成功后回填 k.ID 与 k.KeyIndex（KeyIndex 为展示序号 = 当前行数）。
func (db *DB) AddPlatformKey(k *models.PlatformKey) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	enc, err := crypto.Encrypt(k.Token)
	if err != nil {
		return fmt.Errorf("encrypt key: %w", err)
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.Exec(`
		INSERT INTO credential (platform_id, token, token_hash, label, enabled,
			expires_at, is_free, sort_order)
		VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT MAX(sort_order)+1 FROM credential WHERE platform_id = ?), 0))
	`, k.PlatformID, enc, tokenHashArg(k.Token), k.Label, boolToInt(k.Enabled),
		expiresArg(k.ExpiresAt), boolToInt(k.IsFree), k.PlatformID)
	if err != nil {
		return wrapCredErr(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	if err := bindCredentialToPlatformRAPIsTx(tx, k.PlatformID, id); err != nil {
		return err
	}
	if err := refreshKeyIDsForPlatformTx(tx, k.PlatformID); err != nil {
		return err
	}
	var cnt int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM credential WHERE platform_id = ?`, k.PlatformID).Scan(&cnt); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	k.ID = id
	k.KeyIndex = cnt - 1
	return nil
}

// UpdatePlatformKey updates a credential in place（id 不变 → key_ids 无需重建）。
// token 变化会同步 token_hash；撞哈希时报中文错误。
func (db *DB) UpdatePlatformKey(k *models.PlatformKey) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	enc, err := crypto.Encrypt(k.Token)
	if err != nil {
		return fmt.Errorf("encrypt key: %w", err)
	}

	_, err = db.conn.Exec(`
		UPDATE credential SET token = ?, token_hash = ?, label = ?, enabled = ?,
		       expires_at = ?, is_free = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, enc, tokenHashArg(k.Token), k.Label, boolToInt(k.Enabled),
		expiresArg(k.ExpiresAt), boolToInt(k.IsFree), k.ID)
	return wrapCredErr(err)
}

// ============ Token Cache ============
//
// token_cache 以 credential_id 为键（旧 platform_key_id 语义平移）。当前运行时
// 无调用方（动态令牌推送特性未启用），访问器保留以维持行为兼容。

func (db *DB) GetCachedTokenForKey(credentialID int64) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var encToken string
	var expiresAt sql.NullTime

	err := db.conn.QueryRow("SELECT token, expires_at FROM token_cache WHERE credential_id = ?", credentialID).Scan(&encToken, &expiresAt)
	if err != nil {
		return "", err
	}

	if expiresAt.Valid && time.Now().After(expiresAt.Time) {
		return "", sql.ErrNoRows
	}

	return crypto.Decrypt(encToken)
}

func (db *DB) SetCachedTokenForKey(credentialID int64, token string, ttl time.Duration) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	encToken, err := crypto.Encrypt(token)
	if err != nil {
		return fmt.Errorf("encrypt cached token: %w", err)
	}

	expiresAt := time.Now().Add(ttl)
	_, err = db.conn.Exec(`
		INSERT OR REPLACE INTO token_cache (credential_id, token, expires_at, fetched_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)
	`, credentialID, encToken, expiresAt)

	return err
}

// GetCachedToken is kept for backward compatibility; it delegates to the first credential of the platform.
func (db *DB) GetCachedToken(platformID int64) (string, error) {
	db.mu.RLock()
	var keyID int64
	err := db.conn.QueryRow("SELECT id FROM credential WHERE platform_id = ? ORDER BY sort_order ASC, id ASC LIMIT 1", platformID).Scan(&keyID)
	db.mu.RUnlock()
	if err != nil {
		return "", err
	}
	return db.GetCachedTokenForKey(keyID)
}

// SetCachedToken is kept for backward compatibility; it sets the cache for the first credential.
func (db *DB) SetCachedToken(platformID int64, token string, ttl time.Duration) error {
	db.mu.RLock()
	var keyID int64
	err := db.conn.QueryRow("SELECT id FROM credential WHERE platform_id = ? ORDER BY sort_order ASC, id ASC LIMIT 1", platformID).Scan(&keyID)
	db.mu.RUnlock()
	if err != nil {
		return err
	}
	return db.SetCachedTokenForKey(keyID, token, ttl)
}

// DisableExpiredKeys sets enabled=0 on every credential whose ExpiresAt has
// passed. Returns the number disabled. Called at startup; also safe to invoke
// manually (e.g. from admin endpoints). Idempotent for already-disabled rows.
func (db *DB) DisableExpiredKeys() (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	res, err := db.conn.Exec(`
		UPDATE credential
		SET enabled = 0, updated_at = CURRENT_TIMESTAMP
		WHERE enabled = 1
		  AND expires_at IS NOT NULL
		  AND expires_at < CURRENT_TIMESTAMP
	`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ReorderKeysByGlobalOrder persists the given global key order by re-deriving each
// platform's sort_order from the positions of its credentials in ids. Keys not
// listed keep their relative order after the listed ones.
func (db *DB) ReorderKeysByGlobalOrder(ids []int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Current per-platform order (sort_order, id) as the fallback tail.
	rows, err := tx.Query(`SELECT id, platform_id FROM credential ORDER BY platform_id ASC, sort_order ASC, id ASC`)
	if err != nil {
		return err
	}
	type keyRef struct {
		id         int64
		platformID int64
	}
	var all []keyRef
	platformOf := make(map[int64]int64)
	for rows.Next() {
		var kr keyRef
		if err := rows.Scan(&kr.id, &kr.platformID); err != nil {
			rows.Close()
			return err
		}
		all = append(all, kr)
		platformOf[kr.id] = kr.platformID
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	posInReq := make(map[int64]int, len(ids))
	for i, id := range ids {
		posInReq[id] = i
	}
	// Final per-platform id sequences: requested ids first (in request order),
	// then any remaining ids in their current order.
	perPlatform := make(map[int64][]int64)
	for _, id := range ids {
		pid, ok := platformOf[id]
		if !ok {
			continue // unknown id — ignore
		}
		perPlatform[pid] = append(perPlatform[pid], id)
	}
	for _, kr := range all {
		if _, listed := posInReq[kr.id]; listed {
			continue
		}
		perPlatform[kr.platformID] = append(perPlatform[kr.platformID], kr.id)
	}

	for _, seq := range perPlatform {
		for i, id := range seq {
			if _, err := tx.Exec(`UPDATE credential SET sort_order = ? WHERE id = ?`, i, id); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// DisablePlatformKey sets enabled=false for a single credential (auth failure, e.g. 401).
func (db *DB) DisablePlatformKey(keyID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`
		UPDATE credential SET enabled = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, keyID)
	return err
}

// MarkKeyPermanentFailure sets failure_type=2 with reason and timestamp.
// Used for 401/402/403/409/423/451 — credential permanently failed, needs user action.
// Does NOT touch enabled (decoupled from user intent).
func (db *DB) MarkKeyPermanentFailure(keyID int64, reason string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`
		UPDATE credential
		SET failure_type = 2, failure_reason = ?, failed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, reason, keyID)
	return err
}

// MarkKeyTemporaryFailure sets failure_type=1 with reason and timestamp.
// Used for 429/5xx/timeout — credential temporarily failing, auto-recovering.
func (db *DB) MarkKeyTemporaryFailure(keyID int64, reason string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`
		UPDATE credential
		SET failure_type = 1, failure_reason = ?, failed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, reason, keyID)
	return err
}

// ClearKeyFailure resets failure_type=0, reason='', failed_at=NULL.
// Called on successful request, or after successful probe.
func (db *DB) ClearKeyFailure(keyID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`
		UPDATE credential
		SET failure_type = 0, failure_reason = '', failed_at = NULL, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, keyID)
	return err
}

// DetachKeyFromRAPIs 删除某凭据的全部端点绑定（key 删除前调用，防止白名单悬空），
// 返回受影响模型的 alias 列表。未知 id 为 no-op。
func (db *DB) DetachKeyFromRAPIs(keyID int64) ([]string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	rows, err := db.conn.Query(`
		SELECT r.id, r.alias FROM endpoint_credential ec
		JOIN rapi r ON r.id = ec.rapi_id
		WHERE ec.credential_id = ? ORDER BY r.id ASC
	`, keyID)
	if err != nil {
		return nil, err
	}
	var rapiIDs []int64
	var affected []string
	for rows.Next() {
		var id int64
		var alias string
		if err := rows.Scan(&id, &alias); err != nil {
			rows.Close()
			return nil, err
		}
		rapiIDs = append(rapiIDs, id)
		affected = append(affected, alias)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(rapiIDs) == 0 {
		return nil, nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM endpoint_credential WHERE credential_id = ?`, keyID); err != nil {
		return affected, err
	}
	if err := refreshKeyIDsTx(tx, rapiIDs...); err != nil {
		return affected, err
	}
	return affected, tx.Commit()
}

// DeletePlatformKey 删除一条凭据（绑定/封锁/令牌缓存 FK 级联），并重建归属平台
// 全部 rapi 的 key_ids 派生列。
func (db *DB) DeletePlatformKey(keyID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var platformID int64
	if err := tx.QueryRow(`SELECT platform_id FROM credential WHERE id = ?`, keyID).Scan(&platformID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM credential WHERE id = ?`, keyID); err != nil {
		return err
	}
	if err := refreshKeyIDsForPlatformTx(tx, platformID); err != nil {
		return err
	}
	return tx.Commit()
}
