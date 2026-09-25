// Package supabase 是 Store 接口的中心实现：dashboard 定义类 CRUD 经 PostgREST
// 直写 Supabase（写穿透、同步、失败即报错），本地 SQLite 仅作运行时镜像。
//
// 语义对齐（与 *db.DB 完全一致，handler 无感知）：
//   - token / login_password：读=center_key 解密为明文，写=center_key 加密落库；
//   - 凭据（v2）：中心表 credential，身份 = token_hash（由明文算），平台内轮换
//     序号 = sort_order。读出时映射回 models.PlatformKey（KeyIndex ← sort_order）；
//   - 端点↔凭据绑定（v2）：中心表 endpoint_credential，取代 v1 的 rapi.key_ids
//     CSV。store 接口对外仍是 RAPIWithPlatform.KeyIDs（credential id CSV），
//     空 = 该端点用平台全部凭据（与本地 key_ids 语义同义）；
//   - 未找到 → (nil, nil)，与本地 Scan ErrNoRows 语义一致。
//
// 已知取舍（单写者约定下可接受）：PostgREST 无跨表事务，SetPlatformKeys /
// SetLAPIRAPIOrder 为"先删后插"两步，非原子；SetPlatformSortOrder 逐行 PATCH。
// 设计：docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md §3.2
package supabase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"gateway/internal/crypto"
	"gateway/internal/db"
	"gateway/internal/models"
)

// Config 管理端连接参数（对应 proxy.cfg [management]）。
type Config struct {
	URL        string // https://xxx.supabase.co
	ServiceKey string // service_role / sb_secret_… （读写全表；勿放代理端）
	CenterKey  string // 可选：32 字节 hex，token 写中心前加密；留空 = 中心存明文
}

// Store 实现 store.Store。
type Store struct {
	url       string
	key       string
	centerKey []byte
	onChanged func() // 每次成功写之后触发（刷新本地镜像）
	http      *http.Client
}

// New 构造中心 Store。onChanged 在每次成功写后被调用（service 层用它触发
// syncOnce 立即刷新本地镜像；可为 nil）。
// CenterKey 可选（2026-09 起）：留空 = 中心库 token 存明文（中心访问安全由
// Supabase RLS/API key 负责）；填 32 字节 hex = 写中心前加密、读出时解密
// （兼容旧的 GitHub 分发威胁模型，多代理共享中心时仍建议使用）。
func New(cfg Config, onChanged func()) (*Store, error) {
	var ck []byte
	if strings.TrimSpace(cfg.CenterKey) != "" {
		k, err := crypto.ParseKey(cfg.CenterKey)
		if err != nil {
			return nil, fmt.Errorf("supabase store: %w", err)
		}
		ck = k
	}
	u := strings.TrimSuffix(strings.TrimSpace(cfg.URL), "/")
	if u == "" || strings.TrimSpace(cfg.ServiceKey) == "" {
		return nil, fmt.Errorf("supabase store: url/service_key required")
	}
	return &Store{
		url:       u,
		key:       strings.TrimSpace(cfg.ServiceKey),
		centerKey: ck,
		onChanged: onChanged,
		http:      &http.Client{Timeout: 20 * time.Second},
	}, nil
}

// ---------------------------------------------------------------------------
// PostgREST 原语
// ---------------------------------------------------------------------------

const (
	tblPlatform = "platform"
	// v2：凭据表以 token_hash 为自然键，平台内轮换序号降级为 sort_order。
	tblCred = "credential"
	tblRAPI = "rapi"
	// v2：端点↔凭据绑定表，取代 v1 的 rapi.key_ids CSV。
	tblBind  = "endpoint_credential"
	tblLAPI  = "lapi"
	tblOrder = "lapi_rapi_order"
)

// call 执行一次 PostgREST 调用。method 为 GET 时 out 解码为 JSON；
// 写方法带 Prefer: return=representation 以便读回服务端生成的 id/时间戳。
func (s *Store) call(method, table, query string, body any, out any) error {
	u := s.url + "/rest/v1/" + table
	if query != "" {
		u += "?" + query
	}
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("apikey", s.key)
	req.Header.Set("Authorization", "Bearer "+s.key)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Prefer", "return=representation")
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return wrapCredErr(fmt.Errorf("supabase %s %s: http %d: %s",
			method, table, resp.StatusCode, truncateBody(data)))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("supabase %s %s: decode: %w", method, table, err)
		}
	}
	return nil
}

func (s *Store) changed() {
	if s.onChanged != nil {
		s.onChanged()
	}
}

// wrapCredErr 把 token_hash 唯一冲突翻成可读中文，与本地 *db.DB 的
// wrapCredErr 同文案。v2 起凭据按 token 内容全局唯一（v1 允许同一 token
// 挂在多个平台），所以"换平台重复添加同一 token"从静默成功变成 409 ——
// 没有这层翻译，dashboard 上只会显示裸 PostgREST 错误。
func wrapCredErr(err error) error {
	if err != nil && strings.Contains(err.Error(), "idx_credential_token_hash") {
		return fmt.Errorf("该 token 已登记过：凭据按 token 内容全局唯一，无需重复添加")
	}
	return err
}

// ---------------------------------------------------------------------------
// token 边界 + 通用行转换
// ---------------------------------------------------------------------------

func (s *Store) dec(center string) string {
	if center == "" {
		return ""
	}
	p, err := crypto.DecryptWithKey(center, s.centerKey)
	if err != nil {
		// 密文解不开（center_key 换过/数据异常）：按明文容忍路径透传，
		// 与本地 legacy 语义一致。
		return center
	}
	return p
}

func (s *Store) enc(plain string) string {
	if plain == "" {
		return ""
	}
	c, err := crypto.EncryptWithKey(plain, s.centerKey)
	if err != nil {
		return plain // 加密失败时退回明文会让问题在读取侧暴露，而不是静默丢密钥
	}
	return c
}

func timePtr(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func nowPtr() any { return time.Now().UTC() }

func f64ToID(rows []map[string]any, key string) int64 {
	if len(rows) == 0 {
		return 0
	}
	if f, ok := rows[0][key].(float64); ok {
		return int64(f)
	}
	return 0
}

func truncateBody(b []byte) string {
	const max = 300
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "…"
}

// ---------------------------------------------------------------------------
// platform
// ---------------------------------------------------------------------------

func (s *Store) GetPlatforms() ([]models.Platform, error) {
	var rows []models.Platform
	if err := s.call(http.MethodGet, tblPlatform, "select=*&order=sort_order.asc,id.asc", nil, &rows); err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].Token = s.dec(rows[i].Token)
		rows[i].LoginPassword = s.dec(rows[i].LoginPassword)
		// 中心无健康态列（available 是代理本地 runtime 状态，schema 有意不建）：
		// 缺省置 true，否则零值 false 会让面板把所有启用平台误显为「失效」；
		// 真实健康态由面板展示层另读本地镜像（同 rapiToWithPlatform 的取舍）。
		rows[i].Available = true
	}
	return rows, nil
}

func (s *Store) GetPlatformByID(id int64) (*models.Platform, error) {
	var rows []models.Platform
	q := "select=*&id=eq." + strconv.FormatInt(id, 10) + "&limit=1"
	if err := s.call(http.MethodGet, tblPlatform, q, nil, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	rows[0].Token = s.dec(rows[0].Token)
	rows[0].LoginPassword = s.dec(rows[0].LoginPassword)
	// 同 GetPlatforms：中心无 available 列，缺省 true 防误判「失效」。
	rows[0].Available = true
	return &rows[0], nil
}

func platRow(p *models.Platform) map[string]any {
	return map[string]any{
		"name": p.Name, "base_url": p.BaseURL,
		"token":            p.Token, // 调用方已 enc
		"last_token_fetch": timePtr(p.LastTokenFetch),
		"enabled":          p.Enabled, "notes": p.Notes,
		"supported_formats": p.SupportedFormats, "format_endpoints": p.FormatEndpoints,
		"custom_headers": p.CustomHeaders, "billing_address": p.BillingAddress,
		"login_account": p.LoginAccount, "login_password": p.LoginPassword,
		"updated_at": nowPtr(), // sort_order 只经 SetPlatformSortOrder 改，模型无此字段
	}
}

func (s *Store) CreatePlatform(p *models.Platform) error {
	row := platRow(p)
	row["token"] = s.enc(p.Token)
	row["login_password"] = s.enc(p.LoginPassword)
	var out []map[string]any
	if err := s.call(http.MethodPost, tblPlatform, "", row, &out); err != nil {
		return err
	}
	p.ID = f64ToID(out, "id")
	s.changed()
	return nil
}

func (s *Store) UpdatePlatform(p *models.Platform) error {
	row := platRow(p)
	row["token"] = s.enc(p.Token)
	row["login_password"] = s.enc(p.LoginPassword)
	q := "id=eq." + strconv.FormatInt(p.ID, 10)
	if err := s.call(http.MethodPatch, tblPlatform, q, row, nil); err != nil {
		return err
	}
	s.changed()
	return nil
}

// DeletePlatform / DeletePlatformCascade：中心 FK 级联，两语义在此等价；
// 本地版的状态保护在 handler 层先行判断，不受影响。
func (s *Store) DeletePlatform(id int64) error { return s.deleteByID(tblPlatform, id) }
func (s *Store) DeletePlatformCascade(id int64) error {
	return s.deleteByID(tblPlatform, id)
}

func (s *Store) SetPlatformEnabled(id int64, enabled bool) error {
	return s.patchOne(tblPlatform, id, map[string]any{"enabled": enabled, "updated_at": nowPtr()})
}

// SetPlatformSortOrder(ids) 语义同本地：按切片顺序写 sort_order=index。
func (s *Store) SetPlatformSortOrder(ids []int64) error {
	return s.setSortOrder(tblPlatform, ids)
}

func (s *Store) UpdatePlatformFormats(platformID int64, formatsJSON string, propagateToRAPIs bool, formatEndpointsJSON string) error {
	patch := map[string]any{"supported_formats": formatsJSON, "updated_at": nowPtr()}
	if formatEndpointsJSON != "" || true { // 本地列 NOT NULL DEFAULT ''，空串也显式写
		patch["format_endpoints"] = formatEndpointsJSON
	}
	if err := s.patchOne(tblPlatform, platformID, patch); err != nil {
		return err
	}
	if propagateToRAPIs {
		q := tblRAPI + "?platform_id=eq." + strconv.FormatInt(platformID, 10)
		if err := s.call(http.MethodPatch, tblRAPI, q,
			map[string]any{"supported_formats": formatsJSON, "updated_at": nowPtr()}, nil); err != nil {
			return err
		}
	}
	s.changed()
	return nil
}

// ---------------------------------------------------------------------------
// credential
//
// v2 契约：身份 = token_hash（sha256(明文)[:16]），平台内轮换序号 = sort_order。
// 读出时映射回 store 接口的 models.PlatformKey（KeyIndex ← sort_order），
// 使上层 dashboard / gateway 代码对中心与本地两种 store 无需区分。
// ---------------------------------------------------------------------------

// credRow 是中心 credential 表的解码目标。不能用 models.PlatformKey 直接
// select=*：v2 列名是 sort_order，与 PlatformKey 的 key_index 字段名不匹配。
type credRow struct {
	ID         int64      `json:"id"`
	PlatformID int64      `json:"platform_id"`
	TokenHash  string     `json:"token_hash"`
	SortOrder  int        `json:"sort_order"`
	Token      string     `json:"token"`
	Label      string     `json:"label"`
	Enabled    bool       `json:"enabled"`
	ExpiresAt  *time.Time `json:"expires_at"`
	IsFree     bool       `json:"is_free"`
}

// toPlatformKey 转成 store 接口形态。token 在中心是 center_key 密文，
// 此处解出明文（与本地 store 行为对齐，热路径零改动）。
func (r credRow) toPlatformKey(dec func(string) string) models.PlatformKey {
	return models.PlatformKey{
		ID:         r.ID,
		PlatformID: r.PlatformID,
		KeyIndex:   r.SortOrder, // v2：轮换序号
		Token:      dec(r.Token),
		Label:      r.Label,
		Enabled:    r.Enabled,
		ExpiresAt:  r.ExpiresAt,
		IsFree:     r.IsFree,
	}
}

func (s *Store) GetPlatformKeys(platformID int64) ([]models.PlatformKey, error) {
	q := "select=*&platform_id=eq." + strconv.FormatInt(platformID, 10) + "&order=sort_order.asc"
	return s.queryCreds(q)
}

func (s *Store) GetAllPlatformKeys() ([]models.PlatformKey, error) {
	return s.queryCreds("select=*&order=platform_id.asc,sort_order.asc")
}

func (s *Store) queryCreds(q string) ([]models.PlatformKey, error) {
	var rows []credRow
	if err := s.call(http.MethodGet, tblCred, q, nil, &rows); err != nil {
		return nil, err
	}
	out := make([]models.PlatformKey, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.toPlatformKey(s.dec))
	}
	return out, nil
}

// keyRow 生成 credential 写入行。
//
// 调用契约：k.Token 必须是**明文**（所有调用点都在 s.enc 之前调用本函数），
// 因为 token_hash 要由明文算。token 字段留给调用方覆盖为密文。
// 空 token → token_hash 落 NULL 而非空串：唯一索引是
// idx_credential_token_hash ... WHERE token_hash IS NOT NULL，多条空 token
// 凭据（动态令牌占位）写空串会互相撞唯一约束。
func keyRow(k *models.PlatformKey) map[string]any {
	var hash any // nil → SQL NULL
	if h := models.TokenHash(k.Token); h != "" {
		hash = h
	}
	return map[string]any{
		"platform_id": k.PlatformID,
		"token_hash":  hash,
		"sort_order":  k.KeyIndex, // v2：平台内轮换序号
		"token":       k.Token,    // 调用方覆盖为 enc 密文
		"label":       k.Label, "enabled": k.Enabled,
		"expires_at": k.ExpiresAt, // *time.Time, nil → null
		"is_free":    k.IsFree,
		"updated_at": nowPtr(),
	}
}

func (s *Store) AddPlatformKey(k *models.PlatformKey) error {
	row := keyRow(k)
	row["token"] = s.enc(k.Token)
	var out []map[string]any
	if err := s.call(http.MethodPost, tblCred, "", row, &out); err != nil {
		return err
	}
	k.ID = f64ToID(out, "id")
	s.changed()
	return nil
}

func (s *Store) UpdatePlatformKey(k *models.PlatformKey) error {
	row := keyRow(k)
	row["token"] = s.enc(k.Token)
	if err := s.patchOne(tblCred, k.ID, row); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) DeletePlatformKey(keyID int64) error {
	if err := s.deleteByID(tblCred, keyID); err != nil {
		return err
	}
	s.changed()
	return nil
}

// SetPlatformKeys：整平台密钥列表替换。PostgREST 无跨表事务 → 先删后插两步
// （单写者约定下可接受；中途失败重试即可，幂等）。
func (s *Store) SetPlatformKeys(platformID int64, keys []models.PlatformKey) error {
	q := "platform_id=eq." + strconv.FormatInt(platformID, 10)
	if err := s.call(http.MethodDelete, tblCred, q, nil, nil); err != nil {
		return err
	}
	for i := range keys {
		row := keyRow(&keys[i])
		row["platform_id"] = platformID
		row["token"] = s.enc(keys[i].Token)
		if _, err := json.Marshal(row); err != nil {
			return err
		}
		if err := s.call(http.MethodPost, tblCred, "", row, nil); err != nil {
			return err
		}
	}
	s.changed()
	return nil
}

// DetachKeyFromRAPIs：摘掉该 credential 与所有端点的绑定，返回受影响 rapi 的
// alias（与本地语义一致）。v2 里绑定是独立表的一行，删除即摘除，无需重写 CSV。
func (s *Store) DetachKeyFromRAPIs(keyID int64) ([]string, error) {
	// 先取受影响的 rapi_id（删完就查不到了），再取 alias 用于回报。
	rows, err := s.callList(tblBind,
		"select=rapi_id&credential_id=eq."+strconv.FormatInt(keyID, 10))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	seen := map[int64]bool{}
	var rapiIDs []int64
	for _, r := range rows {
		if f, ok := r["rapi_id"].(float64); ok {
			if id := int64(f); !seen[id] {
				seen[id] = true
				rapiIDs = append(rapiIDs, id)
			}
		}
	}
	if err := s.call(http.MethodDelete, tblBind,
		"credential_id=eq."+strconv.FormatInt(keyID, 10), nil, nil); err != nil {
		return nil, err
	}

	// 一次 in.(...) 取回所有 alias，避免逐个 RTT。
	parts := make([]string, 0, len(rapiIDs))
	for _, rid := range rapiIDs {
		parts = append(parts, strconv.FormatInt(rid, 10))
	}
	aliasRows, err := s.callList(tblRAPI, "select=id,alias&id=in.("+strings.Join(parts, ",")+")")
	if err != nil {
		return nil, err
	}
	aliasByID := map[int64]string{}
	for _, r := range aliasRows {
		id, _ := r["id"].(float64)
		a, _ := r["alias"].(string)
		aliasByID[int64(id)] = a
	}
	var affected []string
	for _, rid := range rapiIDs {
		if a := aliasByID[rid]; a != "" {
			affected = append(affected, a)
		}
	}
	if len(affected) > 0 {
		s.changed()
	}
	return affected, nil
}

// ---------------------------------------------------------------------------
// rapi
// ---------------------------------------------------------------------------

// v2：端点↔凭据绑定。中心用 endpoint_credential 表，store 接口沿用
// RAPIWithPlatform.KeyIDs（credential id CSV）对外表达，两者互不外泄。
// 空 KeyIDs = 该端点不绑定任何凭据 = 用该平台全部凭据，与本地 key_ids 语义同义。

// bindingsByRAPI 一次读回全表绑定，归拢成 rapi_id → credential_id CSV。
func (s *Store) bindingsByRAPI() (map[int64]string, error) {
	rows, err := s.callList(tblBind, "select=rapi_id,credential_id")
	if err != nil {
		return nil, err
	}
	parts := map[int64][]string{}
	for _, r := range rows {
		rf, ok1 := r["rapi_id"].(float64)
		cf, ok2 := r["credential_id"].(float64)
		if !ok1 || !ok2 {
			continue
		}
		rid := int64(rf)
		parts[rid] = append(parts[rid], strconv.FormatInt(int64(cf), 10))
	}
	out := make(map[int64]string, len(parts))
	for rid, cs := range parts {
		out[rid] = strings.Join(cs, ",")
	}
	return out, nil
}

// replaceBindings 把某端点的绑定整组替换为 keyIDs（credential id CSV）。
// PostgREST 无跨表事务 → 先删后插（单写者约定下可接受，重跑幂等）。
func (s *Store) replaceBindings(rapiID int64, keyIDs string) error {
	if err := s.call(http.MethodDelete, tblBind,
		"rapi_id=eq."+strconv.FormatInt(rapiID, 10), nil, nil); err != nil {
		return err
	}
	for _, p := range strings.Split(keyIDs, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		cid, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			continue // 与 v1 mapCSV 同策略：跳过无法解析的项，不整批失败
		}
		row := map[string]any{"rapi_id": rapiID, "credential_id": cid}
		if err := s.call(http.MethodPost, tblBind, "", row, nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetRAPIs() ([]models.RAPIWithPlatform, error) {
	return s.listRAPIs("")
}

func (s *Store) GetRAPIsByPlatform(platformID int64) ([]models.RAPIWithPlatform, error) {
	return s.listRAPIs("platform_id=eq." + strconv.FormatInt(platformID, 10))
}

// listRAPIs 拉取 rapi 行 + 平台/密钥做本地语义的 JOIN 组装。
func (s *Store) listRAPIs(extraFilter string) ([]models.RAPIWithPlatform, error) {
	q := "select=*" + orderSuffix(extraFilter, "sort_order.asc,id.asc")

	// Four independent PostgREST reads. Each is a full HTTPS round trip to the
	// center, so running them concurrently takes this call from ~4 RTT to ~1 RTT.
	// Safe to fan out: all four are read-only and call/dec hold no shared
	// mutable state (only *http.Client, which is concurrency-safe).
	var (
		raws    []models.RAPI
		plats   []models.Platform
		allKeys []models.PlatformKey
		binds   map[int64]string
		rawsErr error
		platErr error
		keysErr error
		bindErr error
		wg      sync.WaitGroup
	)
	wg.Add(4)
	go func() { defer wg.Done(); rawsErr = s.call(http.MethodGet, tblRAPI, q, nil, &raws) }()
	go func() { defer wg.Done(); plats, platErr = s.GetPlatforms() }()
	go func() { defer wg.Done(); allKeys, keysErr = s.GetAllPlatformKeys() }()
	go func() { defer wg.Done(); binds, bindErr = s.bindingsByRAPI() }()
	wg.Wait()
	// Preserve the original error precedence: rapi, platforms, keys, bindings.
	if rawsErr != nil {
		return nil, rawsErr
	}
	if platErr != nil {
		return nil, platErr
	}
	if keysErr != nil {
		return nil, keysErr
	}
	if bindErr != nil {
		return nil, bindErr
	}
	platByID := map[int64]*models.Platform{}
	for i := range plats {
		platByID[plats[i].ID] = &plats[i]
	}
	keysByPlat := map[int64][]models.PlatformKey{}
	for _, k := range allKeys {
		keysByPlat[k.PlatformID] = append(keysByPlat[k.PlatformID], k)
	}

	out := make([]models.RAPIWithPlatform, 0, len(raws))
	for _, r := range raws {
		wp := rapiToWithPlatform(r)
		if p := platByID[r.PlatformID]; p != nil {
			fillPlatform(&wp, p)
			wp.Keys = keysByPlat[p.ID]
		}
		// v2：绑定来自 endpoint_credential 表（v1 是 rapi.key_ids CSV）。
		// 空 = 未绑定 = 该平台全部凭据。
		wp.KeyIDs = binds[r.ID]
		out = append(out, wp)
	}
	return out, nil
}

func orderSuffix(filter, order string) string {
	if filter != "" {
		return "&" + filter + "&order=" + order
	}
	return "&order=" + order
}

func rapiToWithPlatform(r models.RAPI) models.RAPIWithPlatform {
	return models.RAPIWithPlatform{
		ID: r.ID, Alias: r.Alias, Model: r.Model, Notes: r.Notes,
		Vendor: r.Vendor, Series: r.Series, ModelName: r.ModelName,
		Version: r.Version, Suffix: r.Suffix, PlatformID: r.PlatformID,
		Enabled: r.Enabled, Available: true, // 中心无健康态；面板展示层另读本地
		BaseCost: r.BaseCost, HighCost: r.HighCost,
		RPMLimit: r.RPMLimit, RPHLimit: r.RPHLimit, RPDLimit: r.RPDLimit,
		TPMLimit: r.TPMLimit, TPHLimit: r.TPHLimit, TPDLimit: r.TPDLimit,
		SupportedFormats: r.SupportedFormats, TimePeriodRules: r.TimePeriodRules,
		CustomHeaders: r.CustomHeaders, KeyIDs: r.KeyIDs, Source: r.Source,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}

func fillPlatform(wp *models.RAPIWithPlatform, p *models.Platform) {
	wp.PlatformName = p.Name
	wp.PlatformCustomHeaders = p.CustomHeaders
	wp.PlatformSupportedFormats = p.SupportedFormats
	wp.PlatformFormatEndpoints = p.FormatEndpoints
	wp.BaseURL = p.BaseURL
	wp.Token = p.Token
	wp.LastTokenFetch = p.LastTokenFetch
}

func (s *Store) GetRAPIByID(id int64) (*models.RAPIWithPlatform, error) {
	wp, err := s.getRAPIByIDNoBindings(id)
	if err != nil || wp == nil {
		return wp, err
	}
	// v2：绑定来自 endpoint_credential。
	if binds, err := s.bindingsByRAPI(); err == nil {
		wp.KeyIDs = binds[wp.ID]
	}
	return wp, nil
}

// getRAPIByIDNoBindings 组装单个端点（平台 + 平台凭据），**不含**绑定。
// 供 GetRAPIsForLAPI 复用：整链只拉一次绑定表，避免 N+1 次全表读。
func (s *Store) getRAPIByIDNoBindings(id int64) (*models.RAPIWithPlatform, error) {
	var raws []models.RAPI
	if err := s.call(http.MethodGet, tblRAPI,
		"select=*&id=eq."+strconv.FormatInt(id, 10)+"&limit=1", nil, &raws); err != nil {
		return nil, err
	}
	if len(raws) == 0 {
		return nil, nil
	}
	wp := rapiToWithPlatform(raws[0])
	if p, err := s.GetPlatformByID(raws[0].PlatformID); err != nil {
		return nil, err
	} else if p != nil {
		fillPlatform(&wp, p)
		wp.Keys, _ = s.GetPlatformKeys(p.ID)
	}
	return &wp, nil
}

// rapiRow 生成 rapi 写入行。v2 的 rapi 表**没有** key_ids 列（绑定在
// endpoint_credential），带上会让 PostgREST 报未知列，故此处不出现。
// 调用方需另行写绑定，见 replaceBindings。
func rapiRow(r *models.RAPI) map[string]any {
	return map[string]any{
		"platform_id": r.PlatformID, "alias": r.Alias, "model": r.Model,
		"enabled": r.Enabled, "base_cost": r.BaseCost, "high_cost": r.HighCost,
		"rpm_limit": r.RPMLimit, "rph_limit": r.RPHLimit, "rpd_limit": r.RPDLimit,
		"tpm_limit": r.TPMLimit, "tph_limit": r.TPHLimit, "tpd_limit": r.TPDLimit,
		"time_period_rules": r.TimePeriodRules, "supported_formats": r.SupportedFormats,
		"custom_headers": r.CustomHeaders, "source": r.Source,
		"vendor": r.Vendor, "series": r.Series, "model_name": r.ModelName,
		"version": r.Version, "suffix": r.Suffix, "notes": r.Notes,
		"updated_at": nowPtr(), // sort_order 只经 SetRAPISortOrder 改，模型无此字段
	}
}

func (s *Store) CreateRAPI(r *models.RAPI) error {
	var out []map[string]any
	if err := s.call(http.MethodPost, tblRAPI, "", rapiRow(r), &out); err != nil {
		return err
	}
	r.ID = f64ToID(out, "id")
	// v2：绑定独立成表。KeyIDs 已是中心 credential id（中心模式下
	// 上层拿到的就是中心 id），无需换算。
	if err := s.replaceBindings(r.ID, r.KeyIDs); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) UpdateRAPI(r *models.RAPI) error {
	if err := s.patchOne(tblRAPI, r.ID, rapiRow(r)); err != nil {
		return err
	}
	// v2：绑定独立成表，端点更新时整组替换。
	if err := s.replaceBindings(r.ID, r.KeyIDs); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) DeleteRAPI(id int64) error {
	if err := s.deleteByID(tblRAPI, id); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) DeleteRAPICascade(id int64) error { return s.DeleteRAPI(id) }

func (s *Store) SetRAPIEnabled(id int64, enabled bool) error {
	if err := s.patchOne(tblRAPI, id, map[string]any{"enabled": enabled, "updated_at": nowPtr()}); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) SetRAPISortOrder(ids []int64) error {
	if err := s.setSortOrder(tblRAPI, ids); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) UpdateRAPIFormats(id int64, formatsJSON string) error {
	if err := s.patchOne(tblRAPI, id, map[string]any{"supported_formats": formatsJSON, "updated_at": nowPtr()}); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) UpdateRAPIHeaders(id int64, headersJSON string) error {
	if err := s.patchOne(tblRAPI, id, map[string]any{"custom_headers": headersJSON, "updated_at": nowPtr()}); err != nil {
		return err
	}
	s.changed()
	return nil
}

// RAPIModelExists 报告同平台（=同 base_url）下 model 是否已存在 —— 端点身份
// 即 (base_url, model)，重复即冲突。model 为空不查（空 model 行不参与身份）。
func (s *Store) RAPIModelExists(platformID int64, model string, excludeID int64) (bool, error) {
	if strings.TrimSpace(model) == "" {
		return false, nil
	}
	q := "select=id&platform_id=eq." + strconv.FormatInt(platformID, 10) +
		"&model=eq." + url.QueryEscape(model)
	if excludeID > 0 {
		q += "&id=neq." + strconv.FormatInt(excludeID, 10)
	}
	rows, err := s.callList(tblRAPI, q)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// ---------------------------------------------------------------------------
// lapi / 路由链
// ---------------------------------------------------------------------------

func (s *Store) GetLAPIs() ([]models.LAPI, error) {
	var rows []models.LAPI
	if err := s.call(http.MethodGet, tblLAPI, "select=*&order=id.asc", nil, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *Store) GetLAPIByID(id int64) (*models.LAPI, error) {
	var rows []models.LAPI
	if err := s.call(http.MethodGet, tblLAPI, "select=*&id=eq."+strconv.FormatInt(id, 10)+"&limit=1", nil, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func (s *Store) GetLAPIByAlias(alias string) (*models.LAPI, error) {
	var rows []models.LAPI
	if err := s.call(http.MethodGet, tblLAPI, "select=*&alias=eq."+url.QueryEscape(alias)+"&limit=1", nil, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func (s *Store) FindLAPIByModelIdentity(vendor, series, version, suffix string) (*models.LAPI, error) {
	q := "select=*&vendor=eq." + url.QueryEscape(vendor) +
		"&series=eq." + url.QueryEscape(series) +
		"&version=eq." + url.QueryEscape(version) +
		"&suffix=eq." + url.QueryEscape(suffix) + "&limit=1"
	var rows []models.LAPI
	if err := s.call(http.MethodGet, tblLAPI, q, nil, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func lapiRow(l *models.LAPI) map[string]any {
	return map[string]any{
		"alias": l.Alias, "notes": l.Notes, "enabled": l.Enabled,
		"vendor": l.Vendor, "series": l.Series, "model_name": l.ModelName,
		"version": l.Version, "suffix": l.Suffix, "updated_at": nowPtr(),
	}
}

func (s *Store) CreateLAPI(u *models.LAPI) error {
	var out []map[string]any
	if err := s.call(http.MethodPost, tblLAPI, "", lapiRow(u), &out); err != nil {
		return err
	}
	u.ID = f64ToID(out, "id")
	s.changed()
	return nil
}

func (s *Store) UpdateLAPI(l *models.LAPI) error {
	if err := s.patchOne(tblLAPI, l.ID, lapiRow(l)); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) DeleteLAPI(id int64) error {
	if err := s.deleteByID(tblLAPI, id); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) SetLAPIEnabled(id int64, enabled bool) error {
	if err := s.patchOne(tblLAPI, id, map[string]any{"enabled": enabled, "updated_at": nowPtr()}); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) GetLAPIRAPIMapping(lapiID int64) ([]int64, error) {
	rows, err := s.callList(tblOrder,
		"select=rapi_id&lapi_id=eq."+strconv.FormatInt(lapiID, 10)+"&order=order_index.asc")
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		if f, ok := r["rapi_id"].(float64); ok {
			out = append(out, int64(f))
		}
	}
	return out, nil
}

func (s *Store) GetRAPIsForLAPI(lapiID int64) ([]models.RAPIWithPlatform, error) {
	ids, err := s.GetLAPIRAPIMapping(lapiID)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	byID := map[int64]*models.RAPIWithPlatform{}
	for i := range ids {
		wp, err := s.getRAPIByIDNoBindings(ids[i])
		if err != nil {
			return nil, err
		}
		if wp != nil {
			byID[wp.ID] = wp
		}
	}
	// 整链只拉一次绑定表（v2），逐个填 KeyIDs。
	if len(byID) > 0 {
		binds, err := s.bindingsByRAPI()
		if err != nil {
			return nil, err
		}
		for id, wp := range byID {
			wp.KeyIDs = binds[id]
		}
	}
	out := make([]models.RAPIWithPlatform, 0, len(ids))
	for _, id := range ids { // 保持路由链顺序
		if wp, ok := byID[id]; ok {
			out = append(out, *wp)
		}
	}
	return out, nil
}

// GetRAPIsForLAPIWithStats：中心给链路定义；统计是代理本地数据，管理端 v1
// 置零展示（链路本身完整）。如需看真实统计，看各代理自己的面板。
func (s *Store) GetRAPIsForLAPIWithStats(lapiID int64) ([]db.RAPIStat, error) {
	chain, err := s.GetRAPIsForLAPI(lapiID)
	if err != nil {
		return nil, err
	}
	out := make([]db.RAPIStat, 0, len(chain))
	for _, wp := range chain {
		out = append(out, db.RAPIStat{RapiID: wp.ID, Alias: wp.Alias})
	}
	return out, nil
}

// SetLAPIRAPIOrder：路由链整链替换（先删后插两步，单写者下可接受）。
func (s *Store) SetLAPIRAPIOrder(lapiID int64, rapiIDs []int64) error {
	q := "lapi_id=eq." + strconv.FormatInt(lapiID, 10)
	if err := s.call(http.MethodDelete, tblOrder, q, nil, nil); err != nil {
		return err
	}
	for i, rid := range rapiIDs {
		row := map[string]any{"lapi_id": lapiID, "rapi_id": rid, "order_index": i}
		if err := s.call(http.MethodPost, tblOrder, "", row, nil); err != nil {
			return err
		}
	}
	s.changed()
	return nil
}

// ---------------------------------------------------------------------------
// 本地 → 中心 整体覆盖推送（破坏性，管理模式专用）
// ---------------------------------------------------------------------------

// LocalSnapshot 是待推送的本地 SQLite 定义快照（由 service 层从 db.Get() 读取，
// token 已被本地解出为明文）。ReplaceAll 用它整体覆盖中心 6 张定义表。
type LocalSnapshot struct {
	Platforms []models.Platform
	Keys      []models.PlatformKey
	RAPIs     []models.RAPIWithPlatform
	LAPIs     []models.LAPI
	Orders    []db.LAPIRAPIOrder
}

// ReplaceAll 用本地定义整体覆盖中心 6 张定义表。流程：
//  1. DELETE 全表（子→父：order→binding→rapi→credential→lapi→platform，id=gte.0 全删）；
//  2. INSERT platform（父，不传 id），读回服务端 id 建 old→new 映射；
//  3. INSERT credential（remap platform_id；token_hash 由明文算），建 old key id→new；
//  4. INSERT rapi（remap platform_id；v2 无 key_ids 列）；
//  5. INSERT endpoint_credential（remap rapi_id / credential_id）；
//  6. INSERT lapi，建 old→new 映射；
//  7. INSERT lapi_rapi_order（remap lapi_id / rapi_id）。
//
// 不传 id（中心自增）避免序列错位（PostgREST 无法 setval）；token 走 s.enc 边界。
// 非原子：中途失败留部分状态，重跑幂等（全删全插）。返回新中心各表行数。
func (s *Store) ReplaceAll(local LocalSnapshot) (map[string]int, error) {
	// 1) DELETE 全表，子→父。
	for _, t := range []string{tblOrder, tblBind, tblRAPI, tblCred, tblLAPI, tblPlatform} {
		if err := s.call(http.MethodDelete, t, "id=gte.0", nil, nil); err != nil {
			return nil, fmt.Errorf("delete %s: %w", t, err)
		}
	}

	// 2) platform（父）。
	platMap := map[int64]int64{} // old id → new id
	for i := range local.Platforms {
		p := local.Platforms[i]
		row := platRow(&p)
		row["token"] = s.enc(p.Token)
		row["login_password"] = s.enc(p.LoginPassword)
		var out []map[string]any
		if err := s.call(http.MethodPost, tblPlatform, "", row, &out); err != nil {
			return nil, fmt.Errorf("insert platform %q: %w", p.Name, err)
		}
		platMap[p.ID] = f64ToID(out, "id")
	}

	// 3) credential：remap platform_id。token_hash 由**明文**算（keyRow 内
	//    完成），v2 凭据身份即 token_hash。空 token → NULL，不参与唯一约束。
	//    同一 token 在本地跨平台重复时中心唯一索引会拒；本地迁移
	//    （MigrateNaturalKeys）已按 token_hash 合并过，仍撞说明数据不一致，
	//    显式报错让操作者处理，而不是让 PostgREST 抛裸 409。
	keyMap := map[int64]int64{} // old key id → new credential id
	seenHash := map[string]int64{}
	for i := range local.Keys {
		k := local.Keys[i]
		row := keyRow(&k) // 用明文 k.Token 算 token_hash
		if np, ok := platMap[k.PlatformID]; ok {
			row["platform_id"] = np
		}
		if h := models.TokenHash(k.Token); h != "" {
			if prev, dup := seenHash[h]; dup {
				return nil, fmt.Errorf("insert credential: token_hash %s 重复（本地 key id %d 与 %d 同 token；"+
					"v2 凭据按 token 全局唯一，请先跑本地自然键迁移合并）", h, prev, k.ID)
			}
			seenHash[h] = k.ID
		}
		row["token"] = s.enc(k.Token)
		var out []map[string]any
		if err := s.call(http.MethodPost, tblCred, "", row, &out); err != nil {
			return nil, fmt.Errorf("insert credential platform=%d idx=%d: %w", k.PlatformID, k.KeyIndex, err)
		}
		keyMap[k.ID] = f64ToID(out, "id")
	}

	// 4) rapi：remap platform_id。v2 的 rapi 表无 key_ids 列，绑定见第 5 步。
	rapiMap := map[int64]int64{} // old id → new id
	for i := range local.RAPIs {
		wp := local.RAPIs[i]
		r := rapiCore(wp)
		row := rapiRow(&r)
		if np, ok := platMap[wp.PlatformID]; ok {
			row["platform_id"] = np
		}
		var out []map[string]any
		if err := s.call(http.MethodPost, tblRAPI, "", row, &out); err != nil {
			return nil, fmt.Errorf("insert rapi %q: %w", wp.Alias, err)
		}
		rapiMap[wp.ID] = f64ToID(out, "id")
	}

	// 5) endpoint_credential：本地 key id CSV → 中心 credential_id。
	//    原本非空、remap 后全空 = 该平台的 credential 未成功插入 → 端点会静默
	//    变成"用平台全部凭据"。这是数据错误而非悬空清理，显式报错。
	for i := range local.RAPIs {
		wp := local.RAPIs[i]
		ids := splitCSV(wp.KeyIDs)
		if len(ids) == 0 {
			continue // 空 = 不绑定 = 用平台全部凭据，与本地语义一致
		}
		credIDs := make([]int64, 0, len(ids))
		for _, old := range ids {
			if nc, ok := keyMap[old]; ok {
				credIDs = append(credIDs, nc)
			}
		}
		if len(credIDs) == 0 {
			return nil, fmt.Errorf("insert binding for rapi %q: key_ids %q 无一能映射到中心 credential",
				wp.Alias, wp.KeyIDs)
		}
		for _, cid := range credIDs {
			row := map[string]any{"rapi_id": rapiMap[wp.ID], "credential_id": cid}
			if err := s.call(http.MethodPost, tblBind, "", row, nil); err != nil {
				return nil, fmt.Errorf("insert binding rapi=%q cred=%d: %w", wp.Alias, cid, err)
			}
		}
	}

	// 6) lapi。
	lapiMap := map[int64]int64{} // old id → new id
	for i := range local.LAPIs {
		l := local.LAPIs[i]
		var out []map[string]any
		if err := s.call(http.MethodPost, tblLAPI, "", lapiRow(&l), &out); err != nil {
			return nil, fmt.Errorf("insert lapi %q: %w", l.Alias, err)
		}
		lapiMap[l.ID] = f64ToID(out, "id")
	}

	// 7) lapi_rapi_order：remap lapi_id / rapi_id；悬空引用丢弃。
	for _, o := range local.Orders {
		nl, okL := lapiMap[o.LapiID]
		nr, okR := rapiMap[o.RAPIID]
		if !okL || !okR {
			continue
		}
		row := map[string]any{"lapi_id": nl, "rapi_id": nr, "order_index": o.Order}
		if err := s.call(http.MethodPost, tblOrder, "", row, nil); err != nil {
			return nil, fmt.Errorf("insert order lapi=%d rapi=%d: %w", o.LapiID, o.RAPIID, err)
		}
	}

	s.changed()

	// 8) 读回中心各表行数（定义表小，select=id 全取后计数即可）。
	counts := map[string]int{}
	for _, pair := range []struct{ tbl, key string }{
		{tblPlatform, "platform"}, {tblCred, "credential"},
		{tblRAPI, "rapi"}, {tblBind, "endpoint_credential"},
		{tblLAPI, "lapi"}, {tblOrder, "lapi_rapi_order"},
	} {
		if rows, err := s.callList(pair.tbl, "select=id"); err == nil {
			counts[pair.key] = len(rows)
		}
	}
	return counts, nil
}

// DumpAll 读出中心 6 张定义表的全部行（select=*）。token/login_password
// 保持中心存储形态（center_key 密文），不会把明文引入备份。用于 push 覆盖前
// 自动备份中心快照（service 层存 settings.center_backup_latest）。
func (s *Store) DumpAll() (map[string][]map[string]any, error) {
	out := map[string][]map[string]any{}
	for _, t := range []string{tblPlatform, tblCred, tblRAPI, tblBind, tblLAPI, tblOrder} {
		rows, err := s.callList(t, "select=*")
		if err != nil {
			return nil, fmt.Errorf("dump %s: %w", t, err)
		}
		out[t] = rows
	}
	return out, nil
}

// rapiCore 从 RAPIWithPlatform 取出 rapiRow 需要的字段构造 models.RAPI。
func rapiCore(wp models.RAPIWithPlatform) models.RAPI {
	return models.RAPI{
		ID: wp.ID, Alias: wp.Alias, Model: wp.Model, Notes: wp.Notes,
		Vendor: wp.Vendor, Series: wp.Series, ModelName: wp.ModelName,
		Version: wp.Version, Suffix: wp.Suffix, PlatformID: wp.PlatformID,
		Enabled: wp.Enabled, BaseCost: wp.BaseCost, HighCost: wp.HighCost,
		RPMLimit: wp.RPMLimit, RPHLimit: wp.RPHLimit, RPDLimit: wp.RPDLimit,
		TPMLimit: wp.TPMLimit, TPHLimit: wp.TPHLimit, TPDLimit: wp.TPDLimit,
		SupportedFormats: wp.SupportedFormats, TimePeriodRules: wp.TimePeriodRules,
		CustomHeaders: wp.CustomHeaders, KeyIDs: wp.KeyIDs, Source: wp.Source,
	}
}

// splitCSV 解析逗号分隔的 id 串（models 里 key_ids / KeyIDs 的存储形态）。
// 空项与非数字项一律丢弃 —— 与 v1 mapCSV 的"跳过无法解析项"策略一致，
// 避免单个坏值让整批绑定写失败。
func splitCSV(csv string) []int64 {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return nil
	}
	var out []int64
	for _, p := range strings.Split(csv, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if id, err := strconv.ParseInt(p, 10, 64); err == nil {
			out = append(out, id)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

func (s *Store) callList(table, query string) ([]map[string]any, error) {
	var rows []map[string]any
	if err := s.call(http.MethodGet, table, query, nil, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *Store) patchOne(table string, id int64, row map[string]any) error {
	q := "id=eq." + strconv.FormatInt(id, 10)
	return s.call(http.MethodPatch, table, q, row, nil)
}

func (s *Store) deleteByID(table string, id int64) error {
	q := "id=eq." + strconv.FormatInt(id, 10)
	return s.call(http.MethodDelete, table, q, nil, nil)
}

func (s *Store) setSortOrder(table string, ids []int64) error {
	for i, id := range ids {
		if err := s.patchOne(table, id, map[string]any{"sort_order": i, "updated_at": nowPtr()}); err != nil {
			return err
		}
	}
	return nil
}
