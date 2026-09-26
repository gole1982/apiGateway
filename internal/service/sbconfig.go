package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gateway/internal/config"
	"gateway/internal/crypto"
	"gateway/internal/db"
	"gateway/internal/logger"
)

// Supabase 角色判定（角色由录入的 key 权限体现，而非配置文件段名）：
//   - secret key（sb_secret_…，旧称 service_role）绕过 RLS、可写 → 管理（直写中心）
//   - publishable key（sb_publishable_…，旧称 anon）受 RLS、只读   → 代理（只读拉取）
//   - 连不上 / 无 key                                              → 未连接（独立模式）
//
// 探测用无副作用写：PATCH platform?id=eq.-1（0 行受影响；401/403=只读）。
// key 在 Supabase Dashboard → Settings > API Keys 获取。

const (
	sbRoleOffline    = "offline"
	sbRoleProxy      = "proxy"
	sbRoleManagement = "management"
)

// sbConfigSettings key 常量。
const (
	sbKeyURL    = "sb_url"
	sbKeyAPI    = "sb_api_key"    // AES 加密密文
	sbKeyCenter = "sb_center_key" // AES 加密密文：center_key（token 边界加解密，管理端必填）
)

// sbConfigResponse 是 GET /api/sb-config 的响应。
type sbConfigResponse struct {
	Connected     bool           `json:"connected"`
	Role          string         `json:"role"` // offline | proxy | management
	URL           string         `json:"url"`
	KeyMask       string         `json:"key_mask"`      // 脱敏回显
	CenterKeySet  bool           `json:"center_key_set"` // center_key 是否已配置（settings 或 proxy.cfg）
	Activated     bool           `json:"activated"`      // 保存后是否已热激活（免重启）
	ActivateError string         `json:"activate_error,omitempty"`
	CenterVer     int            `json:"center_ver"`
	LocalVer      int            `json:"local_ver"`
	InSync        bool           `json:"in_sync"`
	Tables        any            `json:"tables,omitempty"`         // 中心各表行数
	LocalTables   map[string]int `json:"local_tables,omitempty"`   // 本地 SQLite 各表行数
	StartupTables map[string]int `json:"startup_tables,omitempty"` // 启动时本地行数快照
	Error         string         `json:"error,omitempty"`
}

// handleSBConfig GET 返回当前配置（脱敏）+ 角色；POST 保存 URL+key(+可选
// center_key）并即时探测 + 热激活（免重启）。
func handleSBConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		_ = json.NewEncoder(w).Encode(currentSBConfig())
	case http.MethodPost:
		var body struct {
			URL       string `json:"url"`
			Key       string `json:"key"`
			CenterKey string `json:"center_key"` // 可选；留空=沿用已保存/proxy.cfg 的值
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&body); err != nil {
			writeJSONError(w, 400, err)
			return
		}
		body.URL = normalizeSBURL(body.URL)
		body.Key = strings.TrimSpace(body.Key)
		body.CenterKey = strings.TrimSpace(body.CenterKey)
		if body.URL == "" || body.Key == "" {
			writeJSONError(w, 400, errors.New("Project ID / URL 和 key 均不能为空"))
			return
		}
		// center_key 若提供则先校验格式（32 字节 hex），避免存下一把坏钥匙。
		if body.CenterKey != "" {
			if _, err := crypto.ParseKey(body.CenterKey); err != nil {
				writeJSONError(w, 400, errors.New("center_key 须为 64 位 hex（32 字节）："+err.Error()))
				return
			}
		}
		// 加密保存 key（+可选 center_key）。settings 表为配置主源，proxy.cfg 仅回退。
		enc, err := crypto.Encrypt(body.Key)
		if err != nil {
			writeJSONError(w, 500, err)
			return
		}
		if err := db.Get().SetSetting(sbKeyURL, body.URL); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		if err := db.Get().SetSetting(sbKeyAPI, enc); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		if body.CenterKey != "" {
			encCK, err := crypto.Encrypt(body.CenterKey)
			if err != nil {
				writeJSONError(w, 500, err)
				return
			}
			if err := db.Get().SetSetting(sbKeyCenter, encCK); err != nil {
				writeJSONError(w, 500, err)
				return
			}
		}
		// 即时探测角色 + 热激活（免重启）：management→直写中心，proxy→只读同步。
		role, _, perr := probeSBRole(body.URL, body.Key)
		if perr != nil {
			logger.DefaultConsole().Warn("service", "[SB] probe failed", "error", perr.Error())
			_ = db.Get().InsertSystemLog("warn", "center", "Supabase 角色探测失败: "+perr.Error())
		} else {
			_ = db.Get().InsertSystemLog("info", "center", "Supabase 配置已保存，探测角色="+role)
		}
		resp := currentSBConfig()
		resp.Role = role
		if perr != nil {
			resp.Error = perr.Error()
		}
		if perr == nil && role != sbRoleOffline {
			ck := effectiveCenterKey(body.CenterKey)
			actRole, aerr := activateCenter(body.URL, body.Key, ck)
			if aerr != nil {
				resp.ActivateError = aerr.Error()
				_ = db.Get().InsertSystemLog("warn", "center", "热激活失败: "+aerr.Error())
			} else {
				resp.Activated = true
				resp.Role = actRole
				_ = db.Get().InsertSystemLog("info", "center", "已热激活角色="+actRole+"（免重启）")
			}
		}
		_ = json.NewEncoder(w).Encode(resp)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// loadSavedCenterKey 读取 settings 里保存的 center_key（解密后）。
func loadSavedCenterKey() (string, bool) {
	enc, _ := db.Get().GetSetting(sbKeyCenter)
	if enc == "" {
		return "", false
	}
	v, err := crypto.Decrypt(enc)
	if err != nil {
		return "", false
	}
	return v, true
}

// hasSavedCenterKey 报告 settings 是否已存 center_key（不解密校验内容）。
func hasSavedCenterKey() bool {
	_, ok := loadSavedCenterKey()
	return ok
}

// effectiveCenterKey 解析生效的 center_key：本次提交值 > settings 已存 > proxy.cfg
// [management]/[sync] 的 center_key。
func effectiveCenterKey(submitted string) string {
	if strings.TrimSpace(submitted) != "" {
		return strings.TrimSpace(submitted)
	}
	if v, ok := loadSavedCenterKey(); ok {
		return v
	}
	if c, err := config.Load(); err == nil {
		if c.Management.CenterKey != "" {
			return c.Management.CenterKey
		}
		return c.Sync.CenterKey
	}
	return ""
}

// currentSBConfig 读库组装当前配置（脱敏 key）+ 探测角色/连通/中心版本，并附
// 本地各表行数与启动快照，供中心配置页「同步状态」中心/本地对比及仪表盘卡片。
func currentSBConfig() sbConfigResponse {
	url, _ := db.Get().GetSetting(sbKeyURL)
	enc, _ := db.Get().GetSetting(sbKeyAPI)
	resp := sbConfigResponse{URL: url, LocalTables: localTableCounts(), StartupTables: startupLocalCounts}
	resp.CenterKeySet = effectiveCenterKey("") != ""
	if url == "" || enc == "" {
		resp.Role = sbRoleOffline
		return resp
	}
	key, err := crypto.Decrypt(enc)
	if err != nil {
		resp.Role = sbRoleOffline
		resp.Error = "本地 key 解密失败：" + err.Error()
		return resp
	}
	resp.KeyMask = maskKey(key)
	role, centerVer, perr := probeSBRole(url, key)
	resp.Role = role
	resp.CenterVer = centerVer
	resp.Connected = role != sbRoleOffline
	if perr != nil {
		resp.Error = perr.Error()
	}
	if role != sbRoleOffline {
		fillSBTables(&resp, url, key)
	}
	// 本地版本号 + 是否同步（中心版本 == 本地 last_good）。
	if st, e := db.Get().GetSyncState(); e == nil {
		resp.LocalVer = int(st.LastGoodVersion)
		resp.InSync = centerVer != 0 && int64(centerVer) == st.LastGoodVersion
	}
	return resp
}

// loadSavedSBConfig reads the UI-saved Supabase config from the settings table
// and returns (url, decryptedKey, true) when both sb_url and an sb_api_key are
// present and the key decrypts. Used at startup to activate management mode
// from the dashboard's 中心配置 page (without editing [management] in
// proxy.cfg), so the runtime store, sync loop, and the dashboard sync-center
// card all reflect the same connection the sb-config page already shows.
func loadSavedSBConfig() (url, key string, ok bool) {
	url, _ = db.Get().GetSetting(sbKeyURL)
	if url == "" {
		return "", "", false
	}
	enc, _ := db.Get().GetSetting(sbKeyAPI)
	if enc == "" {
		return "", "", false
	}
	k, err := crypto.Decrypt(enc)
	if err != nil {
		return "", "", false
	}
	return url, k, true
}

// normalizeSBURL 把用户输入规范化为 Supabase 项目根 URL。
// 接受三种输入：
//   - 完整 URL（含 ://）→ 去尾斜杠直接用
//   - 含点的域名（如 xxx.supabase.co，无 ://）→ 补 https://
//   - 纯 Project ref（如 abcdefghijk，无点无 ://）→ 拼 https://<ref>.supabase.co
func normalizeSBURL(input string) string {
	s := strings.TrimSpace(input)
	if s == "" {
		return ""
	}
	s = strings.TrimSuffix(s, "/")
	if strings.Contains(s, "://") {
		return s
	}
	if strings.Contains(s, ".") {
		return "https://" + s
	}
	// 纯 Project ID / ref → 拼 Supabase 默认域名。
	return "https://" + s + ".supabase.co"
}

func maskKey(k string) string {
	if len(k) <= 8 {
		return strings.Repeat("•", len(k))
	}
	return k[:4] + strings.Repeat("•", len(k)-8) + k[len(k)-4:]
}

// probeSBRole 探测连通性与写权限，顺带读回中心版本号（get_version RPC 的返回值）。
// 返回 (role, centerVersion, err)：centerVersion 仅在能解析时非 0，不阻断角色判定。
func probeSBRole(url, key string) (string, int, error) {
	client := &http.Client{Timeout: 8 * time.Second}
	// 1) 连通性：get_version RPC（同时拿中心版本号）。
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url+"/rest/v1/rpc/get_version", nil)
	req.Header.Set("apikey", key)
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return sbRoleOffline, 0, err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<14))
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return sbRoleOffline, 0, errors.New("get_version 返回 " + http.StatusText(resp.StatusCode))
	}
	centerVer := parseCenterVersion(body)
	// 2) 写权限：PATCH 不存在的 platform id（无副作用）。
	patch, _ := http.NewRequestWithContext(context.Background(), http.MethodPatch, url+"/rest/v1/platform?id=eq.-1", strings.NewReader("{}"))
	patch.Header.Set("apikey", key)
	patch.Header.Set("Authorization", "Bearer "+key)
	patch.Header.Set("Content-Type", "application/json")
	patch.Header.Set("Prefer", "return=minimal")
	pr, perr := client.Do(patch)
	if perr != nil {
		return sbRoleOffline, centerVer, perr
	}
	pr.Body.Close()
	switch {
	case pr.StatusCode == 204 || pr.StatusCode == 200:
		return sbRoleManagement, centerVer, nil
	case pr.StatusCode == 401 || pr.StatusCode == 403:
		return sbRoleProxy, centerVer, nil
	default:
		// 其他码（404 表不存在等）也视为代理级：能连但不能写。
		return sbRoleProxy, centerVer, nil
	}
}

// parseCenterVersion 解析 get_version RPC 的返回体（裸数字或 JSON 数字），失败返 0。
func parseCenterVersion(body []byte) int {
	s := strings.TrimSpace(string(body))
	if s == "" {
		return 0
	}
	// PostgREST RPC 返回裸值（如 "3"）或 JSON 数值（如 3）；都按整数解析。
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		return int(n)
	}
	return 0
}

// fillSBTables 拉取中心各表统计 + 版本对比，复用 handleSyncCenter 的数据形状。
func fillSBTables(resp *sbConfigResponse, url, key string) {
	// 复用 sync 包的 get_bundle RPC（若已配置同步则用 syncClient，否则直接调 RPC）。
	// 这里轻量调用：直接 GET 各表 count。
	client := &http.Client{Timeout: 8 * time.Second}
	tables := map[string]int{}
	// 查哪几张表以 centerDefinitionTables 为准（见该变量注释里的事故）。
	for _, t := range centerDefinitionTables {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodHead, url+"/rest/v1/"+t+"?select=id&limit=1", nil)
		req.Header.Set("apikey", key)
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Prefer", "count=exact")
		r, err := client.Do(req)
		if err != nil {
			continue
		}
		r.Body.Close()
		// PostgREST 在 Content-Range 头里给 0-0/total 形式。
		if cr := r.Header.Get("Content-Range"); cr != "" {
			if idx := strings.LastIndex(cr, "/"); idx >= 0 {
				if n, e := strconv.Atoi(cr[idx+1:]); e == nil {
					tables[t] = n
				}
			}
		}
	}
	resp.Tables = tables
}

// handleSystemLogs GET /api/logs/system?limit=N。
func handleSystemLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	logs, err := db.Get().GetSystemLogs(limit)
	if err != nil {
		writeJSONError(w, 500, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"logs": logs})
}
