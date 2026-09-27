package db

import (
	"database/sql"
	"fmt"
	"time"

	"gateway/internal/bundle"
	"gateway/internal/crypto"
)

// ExportBundle 把本地 SQLite 的定义类配置导出成 bundle v2 快照（本地 → 中心方向）。
//
// 这是"以本地数据初始化/重建中心"的唯一入口（生产初始化即走这条路）：
// 本地是权威源，导出结果可直接 push 到中心，也可与中心快照做 Merge。
//
// 转换要点：
//   - token / login_password 本地是 ~/.apiGateway.key 密文，这里解密成明文再用
//     centerKey 重新加密（centerKey 为空 = 中心存明文，与中心侧 enc/dec 约定一致）。
//   - 业务键全部取自然键原样值：platform.base_url、credential.token_hash、
//     rapi.(platform.base_url, model)。不做任何归一化，镜像本地唯一索引。
//   - credential 带出 platform_id→base_url 与 sort_order（平台内轮换序号）。
//   - 绑定来自 endpoint_credential 真实行；某端点若一条绑定都没有，则**不产出**
//     绑定条目（消费端按"该平台全部凭据"处理，与 v1 key_ids 为空同义）。
//   - 健康态/遥测（available、failure_*、metrics、logs）不进快照，沿用既有边界。
func (db *DB) ExportBundle(centerKey []byte) (*bundle.Bundle, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	out := &bundle.Bundle{}

	// ---- platform ----
	rows, err := db.conn.Query(`SELECT id, name, base_url, token, last_token_fetch, enabled,
		notes, supported_formats, format_endpoints, custom_headers, billing_address,
		login_account, login_password, sort_order FROM platform ORDER BY sort_order, id`)
	if err != nil {
		return nil, fmt.Errorf("export platform: %w", err)
	}
	platBaseURL := make(map[int64]string)
	for rows.Next() {
		var (
			id, enabled, sortOrder int64
			name, baseURL          string
			token                  string
			lastFetch              sql.NullTime
			notes, formats         string
			endpoints, hdrs        string
			billing, loginAcc      string
			loginPw                string
		)
		if err := rows.Scan(&id, &name, &baseURL, &token, &lastFetch, &enabled, &notes,
			&formats, &endpoints, &hdrs, &billing, &loginAcc, &loginPw, &sortOrder); err != nil {
			rows.Close()
			return nil, fmt.Errorf("export platform scan: %w", err)
		}
		ct, err := reencryptLocal(token, centerKey)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("export platform %q token: %w", name, err)
		}
		clp, err := reencryptLocal(loginPw, centerKey)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("export platform %q login_password: %w", name, err)
		}
		var ltf *time.Time
		if lastFetch.Valid {
			t := lastFetch.Time
			ltf = &t
		}
		out.Platforms = append(out.Platforms, bundle.Platform{
			BaseURL: baseURL, Name: name, Token: ct, LastTokenFetch: ltf,
			Enabled: enabled != 0, Notes: notes, SupportedFormats: formats,
			FormatEndpoints: endpoints, CustomHeaders: hdrs, BillingAddress: billing,
			LoginAccount: loginAcc, LoginPassword: clp, SortOrder: int(sortOrder),
		})
		platBaseURL[id] = baseURL
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export platform rows: %w", err)
	}

	// ---- credential（按 平台 → 轮换序号 排序，保证导出稳定）----
	rows, err = db.conn.Query(`SELECT id, platform_id, token_hash, token, label, enabled,
		expires_at, is_free, sort_order FROM credential ORDER BY platform_id, sort_order, id`)
	if err != nil {
		return nil, fmt.Errorf("export credential: %w", err)
	}
	credHash := make(map[int64]string) // credential.id → token_hash
	for rows.Next() {
		var (
			id, pid, enabled, isFree, sortOrder int64
			hash, token, label                  string
			expires                             sql.NullTime
		)
		if err := rows.Scan(&id, &pid, &hash, &token, &label, &enabled, &expires, &isFree, &sortOrder); err != nil {
			rows.Close()
			return nil, fmt.Errorf("export credential scan: %w", err)
		}
		ct, err := reencryptLocal(token, centerKey)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("export credential %s token: %w", hash, err)
		}
		var exp *time.Time
		if expires.Valid {
			t := expires.Time
			exp = &t
		}
		out.Credentials = append(out.Credentials, bundle.Credential{
			TokenHash: hash, PlatformBaseURL: platBaseURL[pid], SortOrder: int(sortOrder),
			Token: ct, Label: label, Enabled: enabled != 0, ExpiresAt: exp, IsFree: isFree != 0,
		})
		credHash[id] = hash
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export credential rows: %w", err)
	}

	// ---- rapi ----
	rows, err = db.conn.Query(`SELECT r.id, r.platform_id, r.alias, r.model, r.enabled,
		r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit,
		r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.supported_formats,
		r.custom_headers, r.source, r.vendor, r.series, r.model_name, r.version,
		r.suffix, r.notes, r.sort_order
		FROM rapi r ORDER BY r.sort_order, r.id`)
	if err != nil {
		return nil, fmt.Errorf("export rapi: %w", err)
	}
	rapiBaseURL := make(map[int64]string) // rapi.id → platform.base_url
	rapiModel := make(map[int64]string)   // rapi.id → model
	for rows.Next() {
		var (
			id, pid, enabled, sortOrder  int64
			alias, model                 string
			baseCost, highCost           int64
			rpm, rph, rpd, tpm, tph, tpd int64
			rules, formats, hdrs         string
			source, vendor, series       string
			modelName, version, suffix   string
			notes                        string
		)
		if err := rows.Scan(&id, &pid, &alias, &model, &enabled, &baseCost, &highCost,
			&rpm, &rph, &rpd, &tpm, &tph, &tpd, &rules, &formats, &hdrs, &source,
			&vendor, &series, &modelName, &version, &suffix, &notes, &sortOrder); err != nil {
			rows.Close()
			return nil, fmt.Errorf("export rapi scan: %w", err)
		}
		out.RAPIs = append(out.RAPIs, bundle.RAPI{
			PlatformBaseURL: platBaseURL[pid], Model: model, Alias: alias,
			Enabled: enabled != 0, BaseCost: int(baseCost), HighCost: int(highCost),
			RPMLimit: int(rpm), RPHLimit: int(rph), RPDLimit: int(rpd),
			TPMLimit: int(tpm), TPHLimit: int(tph), TPDLimit: int(tpd),
			TimePeriodRules: rules, SupportedFormats: formats, CustomHeaders: hdrs,
			Source: source, Vendor: vendor, Series: series, ModelName: modelName,
			Version: version, Suffix: suffix, Notes: notes, SortOrder: int(sortOrder),
		})
		rapiBaseURL[id] = platBaseURL[pid]
		rapiModel[id] = model
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export rapi rows: %w", err)
	}

	// ---- lapi ----
	rows, err = db.conn.Query(`SELECT alias, notes, enabled, vendor, series, model_name,
		version, suffix FROM lapi ORDER BY alias`)
	if err != nil {
		return nil, fmt.Errorf("export lapi: %w", err)
	}
	for rows.Next() {
		var (
			enabled            int64
			alias, notes       string
			vendor, series     string
			modelName, version string
			suffix             string
		)
		if err := rows.Scan(&alias, &notes, &enabled, &vendor, &series, &modelName, &version, &suffix); err != nil {
			rows.Close()
			return nil, fmt.Errorf("export lapi scan: %w", err)
		}
		out.LAPIs = append(out.LAPIs, bundle.LAPI{
			Alias: alias, Notes: notes, Enabled: enabled != 0, Vendor: vendor,
			Series: series, ModelName: modelName, Version: version, Suffix: suffix,
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export lapi rows: %w", err)
	}

	// ---- bindings（endpoint_credential 真实行）----
	// 只导出确实存在的绑定行；某端点一条都没有 = 该平台全部凭据（消费端按缺省处理），
	// 因此**不能**为"全绑定"显式展开成 N 行——那会让中心数据随凭据增删而抖动。
	rows, err = db.conn.Query(`SELECT ec.rapi_id, ec.credential_id, ec.rpm_limit, ec.rph_limit,
		ec.rpd_limit, ec.tpm_limit, ec.tph_limit, ec.tpd_limit, ec.enabled
		FROM endpoint_credential ec
		JOIN rapi r ON r.id = ec.rapi_id
		JOIN platform p ON p.id = r.platform_id
		JOIN credential c ON c.id = ec.credential_id
		ORDER BY ec.rapi_id, ec.credential_id`)
	if err != nil {
		return nil, fmt.Errorf("export bindings: %w", err)
	}
	for rows.Next() {
		var (
			rapiID, credID, enabled      int64
			rpm, rph, rpd, tpm, tph, tpd int64
		)
		if err := rows.Scan(&rapiID, &credID, &rpm, &rph, &rpd, &tpm, &tph, &tpd, &enabled); err != nil {
			rows.Close()
			return nil, fmt.Errorf("export binding scan: %w", err)
		}
		baseURL, okB := rapiBaseURL[rapiID]
		model, okM := rapiModel[rapiID]
		hash, okC := credHash[credID]
		if !okB || !okM || !okC {
			// 悬挂绑定（端点/凭据已被删）：跳过，不产出悬空引用
			continue
		}
		out.Bindings = append(out.Bindings, bundle.CredentialBinding{
			PlatformBaseURL: baseURL, Model: model, TokenHash: hash,
			RPMLimit: int(rpm), RPHLimit: int(rph), RPDLimit: int(rpd),
			TPMLimit: int(tpm), TPHLimit: int(tph), TPDLimit: int(tpd),
			Enabled: enabled != 0,
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export binding rows: %w", err)
	}

	// ---- lapi_rapi_order（按 lapi 分组、order_index 升序）----
	rows, err = db.conn.Query(`SELECT l.alias, p.base_url, r.model, o.order_index
		FROM lapi_rapi_order o
		JOIN lapi l ON l.id = o.lapi_id
		JOIN rapi r ON r.id = o.rapi_id
		JOIN platform p ON p.id = r.platform_id
		ORDER BY l.alias, o.order_index`)
	if err != nil {
		return nil, fmt.Errorf("export lapi_rapi_order: %w", err)
	}
	for rows.Next() {
		var (
			orderIndex            int64
			alias, baseURL, model string
		)
		if err := rows.Scan(&alias, &baseURL, &model, &orderIndex); err != nil {
			rows.Close()
			return nil, fmt.Errorf("export lapi_rapi_order scan: %w", err)
		}
		out.LAPIRapiOrder = append(out.LAPIRapiOrder, bundle.LAPIRapiOrder{
			LAPIAlias: alias, RAPIPlatformBaseURL: baseURL, RAPIModel: model,
			OrderIndex: int(orderIndex),
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export lapi_rapi_order rows: %w", err)
	}

	// 自检：导出结果必须自洽，否则宁可报错也不把脏快照推上中心。
	env := &bundle.Envelope{SchemaVersion: bundle.SchemaVersion, Bundle: *out}
	if err := bundle.Validate(env); err != nil {
		return nil, fmt.Errorf("export bundle is not self-consistent: %w", err)
	}
	return out, nil
}

// reencryptLocal 把本地密文（~/.apiGateway.key）解密后用 centerKey 重新加密，
// 供本地 → 中心方向使用（ApplyBundle 里的 reencrypt 是反方向）。
// centerKey 为空 = 中心存明文，直接透传解密结果。
func reencryptLocal(localCiphertext string, centerKey []byte) (string, error) {
	if localCiphertext == "" {
		return "", nil
	}
	plain, err := crypto.Decrypt(localCiphertext)
	if err != nil {
		return "", err
	}
	if plain == "" {
		return "", nil
	}
	if len(centerKey) == 0 {
		return plain, nil // 中心明文模式
	}
	return crypto.EncryptWithKey(plain, centerKey)
}
