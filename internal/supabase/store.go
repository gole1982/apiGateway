// Package supabase 是 Store 接口的中心实现：dashboard 定义类 CRUD 经 PostgREST
// 直写 Supabase（写穿透、同步、失败即报错），本地 SQLite 仅作运行时镜像。
//
// 语义对齐（与 *db.DB 完全一致，handler 无感知）：
//   - token / login_password：读=center_key 解密为明文，写=center_key 加密落库；
//   - rapi.key_ids：中心存"平台内 key_index CSV"（业务键，bundle 契约），
//     读时换算回 key id CSV、写时换算回 key_index CSV（dashboard 按本地语义
//     用 key id 勾选）；
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
	"time"

	"gateway/internal/crypto"
	"gateway/internal/db"
	"gateway/internal/models"
)

// Config 管理端连接参数（对应 proxy.cfg [management]）。
type Config struct {
	URL        string // https://xxx.supabase.co
	ServiceKey string // service_role / sb_secret_… （读写全表；勿放代理端）
	CenterKey  string // 32 字节 hex，token 边界加解密
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
func New(cfg Config, onChanged func()) (*Store, error) {
	ck, err := crypto.ParseKey(cfg.CenterKey)
	if err != nil {
		return nil, fmt.Errorf("supabase store: %w", err)
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
	tblKeys     = "platform_keys"
	tblRAPI     = "rapi"
	tblLAPI     = "lapi"
	tblOrder    = "lapi_rapi_order"
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
		return fmt.Errorf("supabase %s %s: http %d: %s", method, table, resp.StatusCode, truncateBody(data))
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
	return &rows[0], nil
}

func platRow(p *models.Platform) map[string]any {
	return map[string]any{
		"name": p.Name, "base_url": p.BaseURL,
		"token": p.Token, // 调用方已 enc
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
// platform_keys
// ---------------------------------------------------------------------------

func (s *Store) GetPlatformKeys(platformID int64) ([]models.PlatformKey, error) {
	q := "select=*&platform_id=eq." + strconv.FormatInt(platformID, 10) + "&order=key_index.asc"
	var rows []models.PlatformKey
	if err := s.call(http.MethodGet, tblKeys, q, nil, &rows); err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].Token = s.dec(rows[i].Token)
	}
	return rows, nil
}

func (s *Store) GetAllPlatformKeys() ([]models.PlatformKey, error) {
	var rows []models.PlatformKey
	if err := s.call(http.MethodGet, tblKeys, "select=*&order=platform_id.asc,key_index.asc", nil, &rows); err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].Token = s.dec(rows[i].Token)
	}
	return rows, nil
}

func keyRow(k *models.PlatformKey) map[string]any {
	return map[string]any{
		"platform_id": k.PlatformID, "key_index": k.KeyIndex,
		"token": k.Token, // 调用方已 enc
		"label": k.Label, "enabled": k.Enabled,
		"expires_at": k.ExpiresAt, // *time.Time, nil → null
		"is_free":    k.IsFree,
		"updated_at": nowPtr(),
	}
}

func (s *Store) AddPlatformKey(k *models.PlatformKey) error {
	row := keyRow(k)
	row["token"] = s.enc(k.Token)
	var out []map[string]any
	if err := s.call(http.MethodPost, tblKeys, "", row, &out); err != nil {
		return err
	}
	k.ID = f64ToID(out, "id")
	s.changed()
	return nil
}

func (s *Store) UpdatePlatformKey(k *models.PlatformKey) error {
	row := keyRow(k)
	row["token"] = s.enc(k.Token)
	if err := s.patchOne(tblKeys, k.ID, row); err != nil {
		return err
	}
	s.changed()
	return nil
}

func (s *Store) DeletePlatformKey(keyID int64) error {
	if err := s.deleteByID(tblKeys, keyID); err != nil {
		return err
	}
	s.changed()
	return nil
}

// SetPlatformKeys：整平台密钥列表替换。PostgREST 无跨表事务 → 先删后插两步
// （单写者约定下可接受；中途失败重试即可，幂等）。
func (s *Store) SetPlatformKeys(platformID int64, keys []models.PlatformKey) error {
	q := "platform_id=eq." + strconv.FormatInt(platformID, 10)
	if err := s.call(http.MethodDelete, tblKeys, q, nil, nil); err != nil {
		return err
	}
	for i := range keys {
		row := keyRow(&keys[i])
		row["platform_id"] = platformID
		row["token"] = s.enc(keys[i].Token)
		if _, err := json.Marshal(row); err != nil {
			return err
		}
		if err := s.call(http.MethodPost, tblKeys, "", row, nil); err != nil {
			return err
		}
	}
	s.changed()
	return nil
}

// DetachKeyFromRAPIs：从所有 rapi.key_ids（中心=key_index CSV）中摘掉该 key，
// 返回受影响 rapi 的 alias（与本地语义一致）。
func (s *Store) DetachKeyFromRAPIs(keyID int64) ([]string, error) {
	// 找到该 key 的 (platform_id, key_index)
	keys, err := s.callList(tblKeys, "select=platform_id,key_index&id=eq."+strconv.FormatInt(keyID, 10))
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	pid := int64(keys[0]["platform_id"].(float64))
	idx := int(keys[0]["key_index"].(float64))

	rapis, err := s.callList(tblRAPI, "select=id,alias,key_ids&platform_id=eq."+strconv.FormatInt(pid, 10))
	if err != nil {
		return nil, err
	}
	var affected []string
	idxStr := strconv.Itoa(idx)
	for _, r := range rapis {
		csv := r["key_ids"].(string)
		kept := removeCSVItem(csv, idxStr)
		if kept == csv {
			continue
		}
		id := int64(r["id"].(float64))
		if err := s.patchOne(tblRAPI, id, map[string]any{"key_ids": kept, "updated_at": nowPtr()}); err != nil {
			return nil, err
		}
		if a, _ := r["alias"].(string); a != "" {
			affected = append(affected, a)
		}
	}
	if len(affected) > 0 {
		s.changed()
	}
	return affected, nil
}

// removeCSVItem 从逗号分隔串里去掉一项（保持其余顺序）。
func removeCSVItem(csv, item string) string {
	parts := strings.Split(csv, ",")
	var keep []string
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" && t != item {
			keep = append(keep, t)
		}
	}
	return strings.Join(keep, ",")
}

// ---------------------------------------------------------------------------
// rapi
// ---------------------------------------------------------------------------

// keyIndexTranslator 在"中心 key_index CSV"与"dashboard 侧 key id CSV"间换算。
type keyIndexTranslator struct {
	byID    map[int64]int // key id → key_index
	byIndex map[int]int64 // key_index → key id（同平台内）
}

func (s *Store) keyTranslator(platformID int64) (*keyIndexTranslator, error) {
	keys, err := s.GetPlatformKeys(platformID)
	if err != nil {
		return nil, err
	}
	t := &keyIndexTranslator{byID: map[int64]int{}, byIndex: map[int]int64{}}
	for _, k := range keys {
		t.byID[k.ID] = k.KeyIndex
		t.byIndex[k.KeyIndex] = k.ID
	}
	return t, nil
}

// toLocalKeyIDs：中心 key_index CSV → dashboard 语义的 key id CSV。
func (t *keyIndexTranslator) toLocalKeyIDs(keyIndexCSV string) string {
	return t.mapCSV(keyIndexCSV, func(idx int) (int64, bool) {
		id, ok := t.byIndex[idx]
		return id, ok
	})
}

// toCenterKeyIDs：dashboard 的 key id CSV → 中心 key_index CSV。
func (t *keyIndexTranslator) toCenterKeyIDs(idCSV string) string {
	return t.mapCSV(idCSV, func(id int) (int64, bool) {
		idx, ok := t.byID[int64(id)]
		return int64(idx), ok
	})
}

func (t *keyIndexTranslator) mapCSV(csv string, lookup func(int) (int64, bool)) string {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return ""
	}
	var out []string
	for _, p := range strings.Split(csv, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			continue
		}
		if v, ok := lookup(n); ok {
			out = append(out, strconv.FormatInt(v, 10))
		}
	}
	return strings.Join(out, ",")
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
	var raws []models.RAPI
	if err := s.call(http.MethodGet, tblRAPI, q, nil, &raws); err != nil {
		return nil, err
	}
	plats, err := s.GetPlatforms()
	if err != nil {
		return nil, err
	}
	platByID := map[int64]*models.Platform{}
	for i := range plats {
		platByID[plats[i].ID] = &plats[i]
	}
	keysByPlat := map[int64][]models.PlatformKey{}
	allKeys, err := s.GetAllPlatformKeys()
	if err != nil {
		return nil, err
	}
	for _, k := range allKeys {
		keysByPlat[k.PlatformID] = append(keysByPlat[k.PlatformID], k)
	}

	out := make([]models.RAPIWithPlatform, 0, len(raws))
	for _, r := range raws {
		wp := rapiToWithPlatform(r)
		if p := platByID[r.PlatformID]; p != nil {
			fillPlatform(&wp, p)
			wp.Keys = keysByPlat[p.ID]
			// key_ids：中心 key_index CSV → dashboard 语义 key id CSV
			wp.KeyIDs = keyIdxToIDCSV(r.KeyIDs, keysByPlat[p.ID])
		}
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

// keyIdxToIDCSV：中心 key_index CSV → 本地语义 key id CSV（按该平台密钥表）。
func keyIdxToIDCSV(keyIndexCSV string, keys []models.PlatformKey) string {
	if strings.TrimSpace(keyIndexCSV) == "" {
		return ""
	}
	byIdx := map[int]int64{}
	for _, k := range keys {
		byIdx[k.KeyIndex] = k.ID
	}
	var out []string
	for _, p := range strings.Split(keyIndexCSV, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if n, err := strconv.Atoi(p); err == nil {
			if id, ok := byIdx[n]; ok {
				out = append(out, strconv.FormatInt(id, 10))
			}
		}
	}
	return strings.Join(out, ",")
}

func (s *Store) GetRAPIByID(id int64) (*models.RAPIWithPlatform, error) {
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
		wp.KeyIDs = keyIdxToIDCSV(raws[0].KeyIDs, wp.Keys)
	}
	return &wp, nil
}

func rapiRow(r *models.RAPI) map[string]any {
	return map[string]any{
		"platform_id": r.PlatformID, "alias": r.Alias, "model": r.Model,
		"enabled": r.Enabled, "base_cost": r.BaseCost, "high_cost": r.HighCost,
		"rpm_limit": r.RPMLimit, "rph_limit": r.RPHLimit, "rpd_limit": r.RPDLimit,
		"tpm_limit": r.TPMLimit, "tph_limit": r.TPHLimit, "tpd_limit": r.TPDLimit,
		"time_period_rules": r.TimePeriodRules, "supported_formats": r.SupportedFormats,
		"custom_headers": r.CustomHeaders, "key_ids": r.KeyIDs, "source": r.Source,
		"vendor": r.Vendor, "series": r.Series, "model_name": r.ModelName,
		"version": r.Version, "suffix": r.Suffix, "notes": r.Notes,
		"updated_at": nowPtr(), // sort_order 只经 SetRAPISortOrder 改，模型无此字段
	}
}

func (s *Store) CreateRAPI(r *models.RAPI) error {
	t, err := s.keyTranslator(r.PlatformID)
	if err != nil {
		return err
	}
	row := rapiRow(r)
	row["key_ids"] = t.toCenterKeyIDs(r.KeyIDs)
	var out []map[string]any
	if err := s.call(http.MethodPost, tblRAPI, "", row, &out); err != nil {
		return err
	}
	r.ID = f64ToID(out, "id")
	s.changed()
	return nil
}

func (s *Store) UpdateRAPI(r *models.RAPI) error {
	t, err := s.keyTranslator(r.PlatformID)
	if err != nil {
		return err
	}
	row := rapiRow(r)
	row["key_ids"] = t.toCenterKeyIDs(r.KeyIDs)
	if err := s.patchOne(tblRAPI, r.ID, row); err != nil {
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

func (s *Store) RAPIAliasExists(platformID int64, alias string, excludeID int64) (bool, error) {
	q := "select=id&platform_id=eq." + strconv.FormatInt(platformID, 10) +
		"&alias=eq." + url.QueryEscape(alias)
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
		wp, err := s.GetRAPIByID(ids[i])
		if err != nil {
			return nil, err
		}
		if wp != nil {
			byID[wp.ID] = wp
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
