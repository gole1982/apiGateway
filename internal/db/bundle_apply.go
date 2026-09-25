package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gateway/internal/bundle"
	"gateway/internal/crypto"
)

// sync_state：代理的同步状态（单行 id=1）。设计 §5.4：
// 校验通过才单事务切换并写 last_good_version；失败沿用 last_good 继续服务。
const syncStateDDL = `
CREATE TABLE IF NOT EXISTS sync_state (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	current_version INTEGER NOT NULL DEFAULT 0,
	last_good_version INTEGER NOT NULL DEFAULT 0,
	last_synced_at DATETIME,
	source_url TEXT NOT NULL DEFAULT ''
);`

// SyncState 是 sync_state 表的 Go 映射。
type SyncState struct {
	CurrentVersion  int64
	LastGoodVersion int64
	LastSyncedAt    time.Time
	SourceURL       string
}

// GetSyncState 返回代理当前的同步状态；从未同步过时返回零值。
func (db *DB) GetSyncState() (SyncState, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, err := db.conn.Exec(syncStateDDL); err != nil {
		return SyncState{}, fmt.Errorf("sync_state ddl: %w", err)
	}
	var (
		st         SyncState
		lastSynced sql.NullTime
		currentV   sql.NullInt64
		lastGoodV  sql.NullInt64
		source     sql.NullString
	)
	err := db.conn.QueryRow(`SELECT current_version, last_good_version, last_synced_at, source_url FROM sync_state WHERE id = 1`).
		Scan(&currentV, &lastGoodV, &lastSynced, &source)
	if errors.Is(err, sql.ErrNoRows) {
		return SyncState{}, nil
	}
	if err != nil {
		return SyncState{}, err
	}
	st.CurrentVersion = currentV.Int64
	st.LastGoodVersion = lastGoodV.Int64
	st.LastSyncedAt = lastSynced.Time
	st.SourceURL = source.String
	return st, nil
}

// ApplyBundle 把中心拉到的定义快照单事务应用进本地 SQLite。
//
// 语义（设计 §5.2/§5.3/§5.4 + 自然键迁移 §5.1）：
//   - 业务键 diff-upsert：每个同步字段覆盖成中心值；中心没有的行本地删除。
//     禁止整表 DELETE 再插——rapi_metrics / lapi_rapi_order / token_cache 对
//     rapi/lapi 是 ON DELETE CASCADE，按业务键原地 UPDATE 才能保住统计。
//   - v2 业务键：platform=base_url、credential=token_hash、rapi=(platform_id, model)、
//     lapi=alias。全部镜像本地唯一索引（idx_platform_base_url /
//     idx_credential_token_hash / idx_rapi_platform_model），故同名平台、
//     同显示名模型不再被并成一行。
//   - credential.sort_order 取 **bundle 数组下标**（中心按轮换顺序发出），
//     这是自然键契约里唯一能承载"平台内轮换序号"的位置。
//   - b.Bindings 按 (端点自然键, token_hash) 解析成本地 id CSV 后交
//     syncBindingsTx 物化 endpoint_credential，再由它刷新 key_ids 派生列；
//     某端点无绑定 = 绑定该平台全部凭据（与旧 key_ids 为空同义）。
//   - 事务内显式归零健康态：platform/rapi.available、key 失败列、key_model_blocks。
//     rapi_metrics / request_trends / request_logs 保留不清零。
//   - bundle 的 token / login_password 是 center_key 密文：解出明文后用本地
//     ~/.apiGateway.key 重新加密入库（与本地录入同路，热路径零改动）。
//   - 全部成功才 COMMIT 并写 sync_state（current=last_good=env.Version）；
//     任何一步失败整体 rollback、保留 last_good（fail-open 由调用方保证）。
//
// 调用方在 Apply 成功后需重建 scheduler 快照（service 层持有 scheduler，
// db 层不感知）。设计 §5.3。
func (db *DB) ApplyBundle(env *bundle.Envelope, centerKey []byte, sourceURL string) error {
	if env == nil {
		return errors.New("apply bundle: nil envelope")
	}
	if err := bundle.Validate(env); err != nil {
		return fmt.Errorf("apply bundle: pre-apply validate: %w", err)
	}

	// 防误删一切（设计 §5.4）：空定义集直接拒，绝不落库后清空。
	// 中心被误删 / 拉取异常返回空 bundle 时，代理必须沿用 last_good 继续服务。
	if len(env.Bundle.Platforms) == 0 {
		return errors.New("apply bundle: empty definition set rejected (anti-mass-delete guard)")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, err := db.conn.Exec(syncStateDDL); err != nil {
		return fmt.Errorf("apply bundle: sync_state ddl: %w", err)
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("apply bundle: begin: %w", err)
	}
	defer tx.Rollback() // commit 后为 no-op

	now := time.Now()
	b := &env.Bundle

	// ---- 1. platform：按 base_url upsert（v2 业务键，镜像 idx_platform_base_url）----
	platID := make(map[string]int64, len(b.Platforms))
	id2BaseURL := make(map[int64]string, len(b.Platforms))
	for _, p := range b.Platforms {
		encToken, err := reencrypt(p.Token, centerKey)
		if err != nil {
			return fmt.Errorf("platform %q token: %w", p.Name, err)
		}
		encLoginPw, err := reencrypt(p.LoginPassword, centerKey)
		if err != nil {
			return fmt.Errorf("platform %q login_password: %w", p.Name, err)
		}
		// token 为空 → available=0（与 seedDefaultPlatforms 一致：填 key 前不可用）
		avail := 1
		if encToken == "" {
			avail = 0
		}

		var id int64
		err = tx.QueryRow(`SELECT id FROM platform WHERE base_url = ?`, p.BaseURL).Scan(&id)
		switch {
		case err == nil:
			if _, err = tx.Exec(`UPDATE platform SET
					name=?, token=?, last_token_fetch=?, enabled=?, available=?,
					notes=?, supported_formats=?, format_endpoints=?, custom_headers=?,
					billing_address=?, login_account=?, login_password=?,
					sort_order=?, updated_at=?
				WHERE id=?`,
				p.Name, encToken, p.LastTokenFetch, b2i(p.Enabled), avail,
				p.Notes, p.SupportedFormats, p.FormatEndpoints, p.CustomHeaders,
				p.BillingAddress, p.LoginAccount, encLoginPw,
				p.SortOrder, now, id); err != nil {
				return fmt.Errorf("update platform %q: %w", p.Name, err)
			}
		case errors.Is(err, sql.ErrNoRows):
			res, err := tx.Exec(`INSERT INTO platform
					(name, base_url, token, last_token_fetch, enabled, available, notes,
					 supported_formats, format_endpoints, custom_headers, billing_address,
					 login_account, login_password, sort_order, created_at, updated_at)
				VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				p.Name, p.BaseURL, encToken, p.LastTokenFetch, b2i(p.Enabled), avail, p.Notes,
				p.SupportedFormats, p.FormatEndpoints, p.CustomHeaders, p.BillingAddress,
				p.LoginAccount, encLoginPw, p.SortOrder, now, now)
			if err != nil {
				return fmt.Errorf("insert platform %q: %w", p.Name, err)
			}
			if id, err = res.LastInsertId(); err != nil {
				return fmt.Errorf("insert platform %q: last id: %w", p.Name, err)
			}
		default:
			return fmt.Errorf("lookup platform %q: %w", p.Name, err)
		}
		platID[p.BaseURL] = id
		id2BaseURL[id] = p.BaseURL
	}

	// 删除中心已不存在的平台（FK CASCADE 带走其 keys/rapis/metrics）
	rows, err := tx.Query(`SELECT id FROM platform`)
	if err != nil {
		return fmt.Errorf("scan platforms: %w", err)
	}
	var stalePlats []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan platforms: %w", err)
		}
		if _, keep := platID[id2BaseURL[id]]; !keep {
			stalePlats = append(stalePlats, id)
		}
	}
	rows.Close()
	for _, id := range stalePlats {
		if _, err = tx.Exec(`DELETE FROM platform WHERE id=?`, id); err != nil {
			return fmt.Errorf("delete stale platform %d: %w", id, err)
		}
	}

	// ---- 2. credential：按 token_hash upsert（v2 业务键，镜像 idx_credential_token_hash）----
	// sort_order（平台内轮换序号）取 bundle 数组下标：自然键契约里没有 key_index，
	// 中心按轮换顺序发出 credentials，数组位置即轮换序号，行为与 v1 等价。
	credLocalID := make(map[string]int64, len(b.Credentials))
	credSeen := make(map[string]struct{}, len(b.Credentials))
	for i, k := range b.Credentials {
		hash := strings.TrimSpace(k.TokenHash)
		encToken, plainToken, err := reencryptPlain(k.Token, centerKey)
		if err != nil {
			return fmt.Errorf("credential %s token: %w", hash, err)
		}
		// 中心给的 token_hash 是判据；用本地明文重算并校验一致性，防止错配导致串号
		localHash := tokenHashArg(plainToken)
		if lh, ok := localHash.(string); ok && hash != "" && lh != hash {
			return fmt.Errorf("credential token_hash mismatch: bundle=%s local=%s（中心与本地 token 不一致）", hash, lh)
		}
		hashArg := localHash
		sortOrder := i

		var id int64
		err = tx.QueryRow(`SELECT id FROM credential WHERE token_hash = ?`, hash).Scan(&id)
		switch {
		case err == nil:
			if _, err = tx.Exec(`UPDATE credential SET
					token=?, label=?, enabled=?, sort_order=?,
					failure_type=0, failure_reason='', failed_at=NULL,
					expires_at=?, is_free=?, updated_at=?
				WHERE id=?`,
				encToken, k.Label, b2i(k.Enabled), sortOrder,
				k.ExpiresAt, b2i(k.IsFree), now, id); err != nil {
				return wrapCredErr(fmt.Errorf("update credential %s: %w", hash, err))
			}
		case errors.Is(err, sql.ErrNoRows):
			// platform_id 必填：凭据的归属平台由 center_key 无法表达时取第一个平台，
			// 其真实归属以 endpoint_credential 绑定为准（见下方绑定物化）。
			var pid int64
			if err = tx.QueryRow(`SELECT id FROM platform ORDER BY id LIMIT 1`).Scan(&pid); err != nil {
				return fmt.Errorf("credential %s: no platform to attach: %w", hash, err)
			}
			res, err := tx.Exec(`INSERT INTO credential
					(platform_id, sort_order, token_hash, token, label, enabled,
					 expires_at, is_free, created_at, updated_at)
				VALUES (?,?,?,?,?,?, ?,?,?,?)`,
				pid, sortOrder, hashArg, encToken, k.Label, b2i(k.Enabled),
				k.ExpiresAt, b2i(k.IsFree), now, now)
			if err != nil {
				return wrapCredErr(fmt.Errorf("insert credential %s: %w", hash, err))
			}
			if id, err = res.LastInsertId(); err != nil {
				return fmt.Errorf("insert credential %s: last id: %w", hash, err)
			}
		default:
			return fmt.Errorf("lookup credential %s: %w", hash, err)
		}
		credLocalID[hash] = id
		credSeen[hash] = struct{}{}
	}

	// 删除中心已不存在的凭据（FK 级联清绑定/缓存）
	var staleCreds []int64
	idRows, err := tx.Query(`SELECT id, token_hash FROM credential WHERE token_hash IS NOT NULL`)
	if err != nil {
		return fmt.Errorf("scan credentials: %w", err)
	}
	for idRows.Next() {
		var (
			id   int64
			hash sql.NullString
		)
		if err = idRows.Scan(&id, &hash); err != nil {
			idRows.Close()
			return fmt.Errorf("scan credentials: %w", err)
		}
		if _, keep := credSeen[strings.TrimSpace(hash.String)]; !keep {
			staleCreds = append(staleCreds, id)
		}
	}
	idRows.Close()
	for _, id := range staleCreds {
		if _, err = tx.Exec(`DELETE FROM credential WHERE id=?`, id); err != nil {
			return fmt.Errorf("delete stale credential %d: %w", id, err)
		}
	}

	// ---- 3. rapi：按 (platform_id, model) upsert（v2 业务键，镜像 idx_rapi_platform_model）----
	// alias 降级为显示名，随中心覆盖但**不参与查找**。
	// key_ids 是 endpoint_credential 的派生投影：不直写，upsert 后用 syncBindingsTx
	// 重建绑定集合并由它刷新派生列。
	//
	// 绑定先按端点归拢：bundle.Bindings 里的 (端点自然键, token_hash) 解析成本地
	// credential id；某端点没有任何绑定条目 = 绑定该平台全部凭据（空 CSV 语义）。
	type epKey struct{ baseURL, model string }
	bindingsByEP := make(map[epKey][]string, len(b.Bindings))
	credIDsByEP := make(map[epKey][]int64, len(b.Bindings))
	for _, bd := range b.Bindings {
		ek := epKey{bd.PlatformBaseURL, bd.Model}
		bindingsByEP[ek] = append(bindingsByEP[ek], strings.TrimSpace(bd.TokenHash))
		cid, ok := credLocalID[strings.TrimSpace(bd.TokenHash)]
		if !ok {
			return fmt.Errorf("binding (%s,%s): credential %s missing (validate should have caught)",
				bd.PlatformBaseURL, bd.Model, bd.TokenHash)
		}
		credIDsByEP[ek] = append(credIDsByEP[ek], cid)
	}

	rapiSeen := make(map[epKey]struct{}, len(b.RAPIs))
	rapiIDByEP := make(map[epKey]int64, len(b.RAPIs))
	for _, r := range b.RAPIs {
		pid, ok := platID[r.PlatformBaseURL]
		if !ok {
			return fmt.Errorf("rapi %q: platform base_url %q missing (validate should have caught)", r.Alias, r.PlatformBaseURL)
		}
		// 端点的本地 credential id CSV：无绑定条目 = 空 = 平台全部凭据
		ids := credIDsByEP[epKey{r.PlatformBaseURL, r.Model}]
		var localKeyIDs string
		if len(ids) > 0 {
			parts := make([]string, len(ids))
			for i, v := range ids {
				parts[i] = strconv.FormatInt(v, 10)
			}
			localKeyIDs = strings.Join(parts, ",")
		}

		var id int64
		err = tx.QueryRow(`SELECT id FROM rapi WHERE platform_id=? AND model=?`, pid, r.Model).Scan(&id)
		switch {
		case err == nil:
			// model 是键，不更新它；alias 随中心覆盖为最新显示名
			if _, err = tx.Exec(`UPDATE rapi SET
					alias=?, enabled=?, available=1, unavailable_reason='',
					base_cost=?, high_cost=?,
					rpm_limit=?, rph_limit=?, rpd_limit=?, tpm_limit=?, tph_limit=?, tpd_limit=?,
					time_period_rules=?, supported_formats=?, custom_headers=?,
					source=?, series=?, model_name=?, version=?, vendor=?, suffix=?,
					notes=?, sort_order=?, updated_at=?
				WHERE id=?`,
				r.Alias, b2i(r.Enabled),
				r.BaseCost, r.HighCost,
				r.RPMLimit, r.RPHLimit, r.RPDLimit, r.TPMLimit, r.TPHLimit, r.TPDLimit,
				r.TimePeriodRules, r.SupportedFormats, r.CustomHeaders,
				r.Source, r.Series, r.ModelName, r.Version, r.Vendor, r.Suffix,
				r.Notes, r.SortOrder, now, id); err != nil {
				return fmt.Errorf("update rapi (%s,%s): %w", r.PlatformBaseURL, r.Model, err)
			}
		case errors.Is(err, sql.ErrNoRows):
			res, err := tx.Exec(`INSERT INTO rapi
						(alias, model, platform_id, enabled, available, unavailable_reason,
						 base_cost, high_cost,
						 rpm_limit, rph_limit, rpd_limit, tpm_limit, tph_limit, tpd_limit,
						 time_period_rules, supported_formats, custom_headers, key_ids, source,
						 series, model_name, version, vendor, suffix, notes, sort_order,
						 created_at, updated_at)
					VALUES (?,?,?, ?,1,'',
					        ?,?,
					        ?,?,?,?,?,?,
					        ?,?,?,
					        ?,?,
					        ?,?,?,?,?,
					        ?,?,
					        ?,?)`,
				r.Alias, r.Model, pid, b2i(r.Enabled),
				r.BaseCost, r.HighCost,
				r.RPMLimit, r.RPHLimit, r.RPDLimit, r.TPMLimit, r.TPHLimit, r.TPDLimit,
				r.TimePeriodRules, r.SupportedFormats, r.CustomHeaders, localKeyIDs, r.Source,
				r.Series, r.ModelName, r.Version, r.Vendor, r.Suffix, r.Notes, r.SortOrder,
				now, now)
			if err != nil {
				return fmt.Errorf("insert rapi (%s,%s): %w", r.PlatformBaseURL, r.Model, err)
			}
			if id, err = res.LastInsertId(); err != nil {
				return fmt.Errorf("insert rapi (%s,%s): last id: %w", r.PlatformBaseURL, r.Model, err)
			}
		default:
			return fmt.Errorf("lookup rapi (%s,%s): %w", r.PlatformBaseURL, r.Model, err)
		}
		// key_ids 派生列：按解析后的本地 credential id CSV 重建绑定并刷新投影。
		if err := syncBindingsTx(tx, id, pid, localKeyIDs); err != nil {
			return fmt.Errorf("rapi (%s,%s) sync bindings: %w", r.PlatformBaseURL, r.Model, err)
		}
		ek := epKey{r.PlatformBaseURL, r.Model}
		rapiSeen[ek] = struct{}{}
		rapiIDByEP[ek] = id
	}

	// 删除中心已不存在的 rapi（FK CASCADE 带走 metrics —— rapi 本身已不在中心，可接受）
	// 反查键：(platform_id, model) → (platform.base_url, model)
	rows, err = tx.Query(`SELECT r.id, r.platform_id, r.model FROM rapi r`)
	if err != nil {
		return fmt.Errorf("scan rapis: %w", err)
	}
	var staleRapis []int64
	for rows.Next() {
		var (
			id, pid int64
			model   string
		)
		if err = rows.Scan(&id, &pid, &model); err != nil {
			rows.Close()
			return fmt.Errorf("scan rapis: %w", err)
		}
		if _, keep := rapiSeen[epKey{id2BaseURL[pid], model}]; !keep {
			staleRapis = append(staleRapis, id)
		}
	}
	rows.Close()
	for _, id := range staleRapis {
		if _, err = tx.Exec(`DELETE FROM rapi WHERE id=?`, id); err != nil {
			return fmt.Errorf("delete stale rapi %d: %w", id, err)
		}
	}

	// ---- 4. lapi：按 alias upsert ----
	lapiID := make(map[string]int64, len(b.LAPIs))
	for _, l := range b.LAPIs {
		var id int64
		err = tx.QueryRow(`SELECT id FROM lapi WHERE alias=?`, l.Alias).Scan(&id)
		switch {
		case err == nil:
			if _, err = tx.Exec(`UPDATE lapi SET notes=?, enabled=?, vendor=?, series=?,
					model_name=?, version=?, suffix=? WHERE id=?`,
				l.Notes, b2i(l.Enabled), l.Vendor, l.Series,
				l.ModelName, l.Version, l.Suffix, id); err != nil {
				return fmt.Errorf("update lapi %q: %w", l.Alias, err)
			}
		case errors.Is(err, sql.ErrNoRows):
			res, err := tx.Exec(`INSERT INTO lapi (alias, notes, enabled, vendor, series, model_name, version, suffix, created_at)
				VALUES (?,?,?,?,?,?,?,?,?)`,
				l.Alias, l.Notes, b2i(l.Enabled), l.Vendor, l.Series, l.ModelName, l.Version, l.Suffix, now)
			if err != nil {
				return fmt.Errorf("insert lapi %q: %w", l.Alias, err)
			}
			if id, err = res.LastInsertId(); err != nil {
				return fmt.Errorf("insert lapi %q: last id: %w", l.Alias, err)
			}
		default:
			return fmt.Errorf("lookup lapi %q: %w", l.Alias, err)
		}
		lapiID[l.Alias] = id
	}

	// 删除中心已不存在的 lapi（FK CASCADE 带走其 metrics —— lapi 已不在中心，可接受）
	rows, err = tx.Query(`SELECT id, alias FROM lapi`)
	if err != nil {
		return fmt.Errorf("scan lapis: %w", err)
	}
	var staleLapis []int64
	for rows.Next() {
		var id int64
		var alias string
		if err = rows.Scan(&id, &alias); err != nil {
			rows.Close()
			return fmt.Errorf("scan lapis: %w", err)
		}
		if _, keep := lapiID[alias]; !keep {
			staleLapis = append(staleLapis, id)
		}
	}
	rows.Close()
	for _, id := range staleLapis {
		if _, err = tx.Exec(`DELETE FROM lapi WHERE id=?`, id); err != nil {
			return fmt.Errorf("delete stale lapi %d: %w", id, err)
		}
	}

	// ---- 5. lapi_rapi_order：全量 swap（本表无 metrics 关联，重插安全）----
	if _, err = tx.Exec(`DELETE FROM lapi_rapi_order`); err != nil {
		return fmt.Errorf("clear lapi_rapi_order: %w", err)
	}
	for _, o := range b.LAPIRapiOrder {
		lid, ok := lapiID[o.LAPIAlias]
		if !ok {
			return fmt.Errorf("lapi_rapi_order: lapi %q missing (validate should have caught)", o.LAPIAlias)
		}
		// 反查 rapi 本地 id：端点自然键 (base_url, model) → id（映射已在本函数内建好）
		ek := epKey{o.RAPIPlatformBaseURL, o.RAPIModel}
		rpid, ok := rapiIDByEP[ek]
		if !ok {
			return fmt.Errorf("lapi_rapi_order: endpoint (%s,%s) missing (validate should have caught)",
				o.RAPIPlatformBaseURL, o.RAPIModel)
		}
		if _, err = tx.Exec(`INSERT INTO lapi_rapi_order (lapi_id, rapi_id, order_index) VALUES (?,?,?)`,
			lid, rpid, o.OrderIndex); err != nil {
			return fmt.Errorf("insert lapi_rapi_order (%s,%s,%d): %w", o.LAPIAlias, ek, o.OrderIndex, err)
		}
	}

	// ---- 6. 健康态归零（防 FSM 出错；metrics/logs 保留，设计 §5.3 定稿）----
	if _, err = tx.Exec(`UPDATE platform SET available=1`); err != nil {
		return fmt.Errorf("reset platform available: %w", err)
	}
	if _, err = tx.Exec(`UPDATE rapi SET available=1, unavailable_reason=''`); err != nil {
		return fmt.Errorf("reset rapi available: %w", err)
	}
	if _, err = tx.Exec(`UPDATE credential SET failure_type=0, failure_reason='', failed_at=NULL`); err != nil {
		return fmt.Errorf("reset credential failure: %w", err)
	}
	if _, err = tx.Exec(`DELETE FROM key_model_blocks`); err != nil {
		return fmt.Errorf("clear key_model_blocks: %w", err)
	}

	// ---- 7. sync_state + COMMIT ----
	if _, err = tx.Exec(`INSERT INTO sync_state (id, current_version, last_good_version, last_synced_at, source_url)
			VALUES (1, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET
				current_version=excluded.current_version,
				last_good_version=excluded.last_good_version,
				last_synced_at=excluded.last_synced_at,
				source_url=excluded.source_url`,
		env.Version, env.Version, now, sourceURL); err != nil {
		return fmt.Errorf("write sync_state: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("apply bundle: commit: %w", err)
	}
	return nil
}

// reencryptPlain 同 reencrypt，但额外返回明文（调用方算 token_hash 用）。
func reencryptPlain(centerCiphertext string, centerKey []byte) (enc string, plain string, err error) {
	if centerCiphertext == "" {
		return "", "", nil
	}
	if len(centerKey) == 0 && strings.HasPrefix(centerCiphertext, "enc:") {
		return "", "", errors.New("中心存储为密文但本地未配置 center_key，无法解密；" +
			"请在中心配置页填入当时的 center_key，或用「本地到中心」整体覆盖推送把中心改写为明文")
	}
	plain, err = crypto.DecryptWithKey(centerCiphertext, centerKey)
	if err != nil {
		return "", "", err
	}
	if plain == "" {
		return "", "", nil
	}
	enc, err = crypto.Encrypt(plain)
	if err != nil {
		return "", "", err
	}
	return enc, plain, nil
}

// reencrypt 把中心值转存为本地密文。空串原样返回；中心存了无 enc: 前缀的明文
// （容忍路径，含 center_key 留空的"中心明文"模式）时视作明文重新本地加密。
// centerKey 为空而中心值却带 enc: 前缀（历史加密残留）：无法解密，给出明确指引
// 而不是返回 "key not initialised" 这种误导性错误。
func reencrypt(centerCiphertext string, centerKey []byte) (string, error) {
	enc, _, err := reencryptPlain(centerCiphertext, centerKey)
	return enc, err
}

func b2i(v bool) int {
	if v {
		return 1
	}
	return 0
}
