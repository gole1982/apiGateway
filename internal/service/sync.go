package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gateway/internal/bundle"
	"gateway/internal/config"
	"gateway/internal/crypto"
	"gateway/internal/db"
	"gateway/internal/logger"
	"gateway/internal/store"
	"gateway/internal/supabase"
)

// sbSystemLog 记一条中心互联事件到 system_logs（best-effort，错误被吞）。
func sbSystemLog(level, msg string) {
	if d := db.Get(); d != nil {
		_ = d.InsertSystemLog(level, "center", msg)
	}
}

// 代理端中心同步（读路径）。设计 §5：
// 启动拉 1 次 + 定时轮询中心 version（短路不变） + 控制台手动刷新；
// 任何失败沿用本地 last_good 继续转发（fail-open），绝不中断业务。
//
// service 层沿用包级单例的既有模式（见 dev-guide §六）。
// 热切换（中心配置页保存后免重启激活）会改这些变量，读写一律经 syncMu 保护。
var (
	syncMu         sync.RWMutex
	syncClient     *bundle.Client
	syncCenterKey  []byte
	syncSourceURL  string
	syncConfigured bool
	syncStarted    bool            // 轮询 goroutine 是否已启动（热激活复用）
	syncStopCh     <-chan struct{} // Run 启动时记录，热激活启动轮询用
	syncPollSec    int             // 轮询间隔（热激活沿用）
	manageMode     atomic.Bool     // 管理模式：定义类写直写中心，只读守卫放行
)

// syncSnapshot 在读锁内取同步配置的当前快照，供 syncOnce / 状态接口使用——
// 热切换可能随时替换 client，直接裸读包变量存在数据竞争。
func syncSnapshot() (client *bundle.Client, centerKey []byte, sourceURL string, configured bool) {
	syncMu.RLock()
	defer syncMu.RUnlock()
	return syncClient, syncCenterKey, syncSourceURL, syncConfigured
}

// startupLocalCounts 是网关启动那一刻本地 SQLite 6 张定义表的行数快照，
// 用于仪表盘「中心连接信息」卡的「启动以来变化量」列（当前本地 − 启动快照）。
// 在 service.Run 里 db.Init 之后捕获一次；db.Get() 在 manage 模式下仍是本地
// SQLite（运行时读一律走本地，store 切换只影响定义类 CRUD 写）。
var startupLocalCounts = map[string]int{}

// startSyncLoop 初始化并启动中心同步。cfg.Sync 未配置（无 source_url）时只记录
// stopCh/间隔（供后续热激活使用）并返回，网关以纯本地模式运行；center_key 配错
// 也只禁用同步，不影响启动（fail-open）。
func startSyncLoop(stopCh <-chan struct{}, syncCfg config.Sync) {
	syncMu.Lock()
	syncStopCh = stopCh
	syncPollSec = syncCfg.PollIntervalSec
	syncMu.Unlock()
	if !syncCfg.Enabled() {
		logger.DefaultConsole().Info("service", "[SYNC] disabled (no [sync] source_url), running standalone")
		return
	}
	if err := configureSync(syncCfg); err != nil {
		logger.DefaultConsole().Error("service", "[SYNC] "+err.Error()+", sync disabled")
		return
	}
	ensureSyncLoop()
}

// configureSync 校验并热应用一份同步配置（替换 client/key/source）。调用方需已
// 确认 cfg.Enabled()。center_key 可选：留空 = 中心存明文 token；填 32 字节 hex =
// 中心存 center_key 密文。返回错误时保持旧配置不变。
func configureSync(syncCfg config.Sync) error {
	var centerKey []byte
	if strings.TrimSpace(syncCfg.CenterKey) != "" {
		k, err := crypto.ParseKey(syncCfg.CenterKey)
		if err != nil {
			return errors.New("invalid center_key (want 32-byte hex): " + err.Error())
		}
		centerKey = k
	}
	c := bundle.NewClient(syncCfg.SourceURL, syncCfg.VersionURL, syncCfg.AnonKey)
	syncMu.Lock()
	syncClient = c
	syncCenterKey = centerKey
	syncSourceURL = syncCfg.SourceURL
	syncConfigured = true
	if syncCfg.PollIntervalSec > 0 {
		syncPollSec = syncCfg.PollIntervalSec
	}
	syncMu.Unlock()
	return nil
}

// ensureSyncLoop 启动轮询 goroutine（只启动一次；热激活后 goroutine 经
// syncSnapshot 读到的已是新 client，无需重启 goroutine）。sync 未配置时调用无操作。
func ensureSyncLoop() {
	syncMu.Lock()
	if syncStarted || !syncConfigured || syncStopCh == nil {
		syncMu.Unlock()
		return
	}
	syncStarted = true
	stopCh := syncStopCh
	sec := syncPollSec
	src := syncSourceURL
	syncMu.Unlock()

	interval := time.Duration(sec) * time.Second
	if interval <= 0 {
		interval = 60 * time.Second
	}
	logger.DefaultConsole().Info("service", "[SYNC] enabled",
		"source", src, "poll_interval_sec", int(interval.Seconds()))

	go func() {
		// 启动主动拉 1 次（设计 §5.1）。失败不打断启动：沿用本地缓存继续服务。
		// 轮询走 syncOnce(false)：受 manual 拉取模式分流（manual 只记 pending 不 apply）。
		if err := syncOnce(false); err != nil {
			logger.DefaultConsole().Warn("service", "[SYNC] initial pull failed, serving from local cache",
				"error", err.Error())
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if err := syncOnce(false); err != nil {
					logger.DefaultConsole().Warn("service", "[SYNC] poll failed, keeping last-good config",
						"error", err.Error())
				}
			}
		}
	}()
}

// syncOnce 轮询中心版本号；变了才拉全量并单事务应用（version 短路，不拉全量）。
// syncOnce 轮询中心版本号。force=true 时无论拉取模式都应用（手动拉取/管理端写后拉回）；
// force=false 时按拉取模式：auto（默认）应用，manual 只记 pending_remote_version 不应用，
// 保留代理本地应急改不被覆盖。
func syncOnce(force bool) error {
	client, centerKey, sourceURL, configured := syncSnapshot()
	if !configured {
		return errors.New("sync not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	remote, err := client.PullVersion(ctx)
	if err != nil {
		sbSystemLog("warn", "中心版本号轮询失败: "+err.Error())
		return err
	}
	st, err := db.Get().GetSyncState()
	if err != nil {
		return err
	}
	if remote == st.LastGoodVersion {
		return nil // 版本没变，短路
	}
	// manual 模式且非强制：只记 pending，不 apply，保留本地应急改。
	if !force && pullMode() == "manual" {
		_ = db.Get().SetSetting("pending_remote_version", strconv.FormatInt(remote, 10))
		sbSystemLog("info", "manual 模式：中心有新版本 v"+itoa(int(remote))+"，待手动拉取")
		return nil
	}
	env, err := client.PullBundle(ctx, st.LastGoodVersion)
	if errors.Is(err, bundle.ErrNoChange) {
		return nil // 轮询间隙版本又追平（拉取时已一致）
	}
	if err != nil {
		return err
	}
	if err := db.Get().ApplyBundle(env, centerKey, sourceURL); err != nil {
		sbSystemLog("error", "应用中心配置失败: "+err.Error())
		return err
	}
	_ = db.Get().SetSetting("pending_remote_version", "") // 清 pending
	logger.DefaultConsole().Info("service", "[SYNC] applied new config",
		"version", env.Version,
		"platforms", len(env.Bundle.Platforms),
		"credentials", len(env.Bundle.Credentials),
		"rapis", len(env.Bundle.RAPIs),
		"lapis", len(env.Bundle.LAPIs),
		"bindings", len(env.Bundle.Bindings))
	sbSystemLog("info", "已应用新配置 v"+itoa(int(env.Version))+
		"（平台×"+itoa(len(env.Bundle.Platforms))+" / 凭据×"+itoa(len(env.Bundle.Credentials))+
		" / 模型×"+itoa(len(env.Bundle.RAPIs))+" / 接口×"+itoa(len(env.Bundle.LAPIs))+
		" / 绑定×"+itoa(len(env.Bundle.Bindings))+"）")
	return nil
}

// pullMode 读取拉取模式（settings 表 sync_pull_mode）：auto（默认，自动 apply）/ manual（只记 pending）。
func pullMode() string {
	m, _ := db.Get().GetSetting("sync_pull_mode")
	if m == "manual" {
		return "manual"
	}
	return "auto"
}

// localTableCounts 统计本地 SQLite 5 张定义表的当前行数。
// 一律走 db.Get()（本地），绝不走 store.A()——manage 模式下 store 是中心库，
// 用 store.A() 会把中心行数当成本地，使仪表盘「中心 vs 本地」对比失去意义。
func localTableCounts() map[string]int {
	d := db.Get()
	if d == nil {
		return map[string]int{}
	}
	out := map[string]int{}
	if ps, err := d.GetPlatforms(); err == nil {
		out["platform"] = len(ps)
	}
	if ks, err := d.GetAllPlatformKeys(); err == nil {
		// v2：本地凭据在 credential 表；计数键与中心表名一致，
		// 前端按表名取数（旧键名 platform_keys 已随旧表删除）。
		out["credential"] = len(ks)
	}
	if bs, err := d.GetAllEndpointCredentials(); err == nil {
		out["endpoint_credential"] = len(bs)
	}
	if rs, err := d.GetRAPIs(); err == nil {
		out["rapi"] = len(rs)
	}
	if ls, err := d.GetLAPIs(); err == nil {
		out["lapi"] = len(ls)
	}
	if os, err := d.GetAllLAPIRAPIOrders(); err == nil {
		out["lapi_rapi_order"] = len(os)
	}
	return out
}

// captureStartupSnapshot 在 service.Run 启动初始化后调用一次，冻结启动时本地行数。
func captureStartupSnapshot() {
	startupLocalCounts = localTableCounts()
}

// localRole 返回本地运行角色：manageMode=管理端、syncConfigured=代理端、否则未连接。
func localRole() string {
	_, _, _, configured := syncSnapshot()
	switch {
	case manageMode.Load():
		return "management"
	case configured:
		return "proxy"
	default:
		return "offline"
	}
}

// handleSyncState GET /api/sync/state —— 面板展示当前同步状态。
func handleSyncState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	st, err := db.Get().GetSyncState()
	if err != nil {
		writeJSONError(w, 500, err)
		return
	}
	var lastSynced any
	if !st.LastSyncedAt.IsZero() {
		lastSynced = st.LastSyncedAt.Format(time.RFC3339)
	}
	_, _, _, configured := syncSnapshot()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled":           configured,
		"current_version":   st.CurrentVersion,
		"last_good_version": st.LastGoodVersion,
		"last_synced_at":    lastSynced,
		"source_url":        st.SourceURL,
	})
}

// handleSyncRefresh POST /api/sync/refresh —— 控制台"手动刷新"按钮（设计 §5.1）。
// 失败显式报错、不动现有配置（fail-open）。
func handleSyncRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if _, _, _, configured := syncSnapshot(); !configured {
		writeJSONError(w, http.StatusForbidden, errors.New("sync not configured (no [sync] source_url)"))
		return
	}
	if err := syncOnce(true); err != nil {
		writeJSONError(w, 500, err)
		return
	}
	handleSyncState(w, r) // 成功后返回最新状态
}

// handleSyncCenter GET /api/sync/center —— 仪表盘的中心连接信息卡：
// 本地角色 + 连通性 + 各表统计(中心) + 本地行数 + 启动快照 + 是否同步。
// 一次 get_bundle 全量拉取（几十 KB）同时拿到连通性、中心版本、五张定义表行数；
// in_sync = 中心版本 == 本地 last_good 版本。p_version=-1 永不相等 → 中心总返回全量。
// 本地行数与启动快照一律走 db.Get()（本地 SQLite），与 store 切换无关。
//
// 数据源对齐 sb-config 页：运行时的 syncConfigured/syncClient 只在启动时由
// startSyncLoop 设置。若操作员在「中心配置」页录入 URL+key 后未重启（或 key 是
// publishable/anon 只读 → 不激活管理模式），运行时 sync 仍未配置，但 sb-config
// 页已显示"已连接"。为避免两卡互相矛盾，运行时未配置时回退读 settings 并用
// probeSBRole 探测——与 sb-config 完全同源，两张卡显示一致。
func handleSyncCenter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	client, _, _, configured := syncSnapshot()
	out := map[string]any{
		"enabled":        configured,
		"role":           localRole(),
		"local_tables":   localTableCounts(),
		"startup_tables": startupLocalCounts,
	}

	// 运行时 sync 未配置：回退到面板保存的 sb-config，与 sb-config 页同源判定。
	// 覆盖"录入后未重启"和"只读 key（代理角色）未配 [sync]"两种场景。
	if !configured {
		url, key, ok := loadSavedSBConfig()
		if !ok {
			out["connected"] = false
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		role, centerVer, perr := probeSBRole(url, key)
		out["role"] = role // 以实际探测的 key 权限为准（settings 存了什么角色就显示什么）
		out["connected"] = role != sbRoleOffline
		if perr != nil {
			out["error"] = perr.Error()
		}
		if role != sbRoleOffline {
			if st, e := db.Get().GetSyncState(); e == nil {
				out["center_version"] = centerVer
				out["local_version"] = st.LastGoodVersion
				out["in_sync"] = centerVer != 0 && int64(centerVer) == st.LastGoodVersion
				if !st.LastSyncedAt.IsZero() {
					out["last_synced_at"] = st.LastSyncedAt.Format(time.RFC3339)
				}
			}
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	env, err := client.PullBundle(ctx, -1)
	if err != nil {
		out["connected"] = false
		out["error"] = err.Error()
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	out["connected"] = true
	// 键必须用中心真实表名（credential / endpoint_credential），与
	// localTableCounts / fillSBTables 一致 —— 前端三列（中心/本地/启动快照）
	// 共用同一个表名数组取数，键系不一致就会显示"—"（2026-09-26 事故：
	// 此处曾用 bundle 风格的复数键 credentials/bindings）。
	out["tables"] = map[string]int{
		"platform":            len(env.Bundle.Platforms),
		"credential":          len(env.Bundle.Credentials),
		"rapi":                len(env.Bundle.RAPIs),
		"lapi":                len(env.Bundle.LAPIs),
		"lapi_rapi_order":     len(env.Bundle.LAPIRapiOrder),
		"endpoint_credential": len(env.Bundle.Bindings),
	}

	st, err := db.Get().GetSyncState()
	if err != nil {
		out["error"] = "local sync_state: " + err.Error()
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	out["center_version"] = env.Version
	out["local_version"] = st.LastGoodVersion
	out["in_sync"] = env.Version == st.LastGoodVersion
	if !st.LastSyncedAt.IsZero() {
		out["last_synced_at"] = st.LastSyncedAt.Format(time.RFC3339)
	}
	_ = json.NewEncoder(w).Encode(out)
}

// handleSyncDiagnostics GET /api/sync/diagnostics —— 排障：当面板已保存 sb-config
// 但运行时未激活管理模式（多为 center_key 为空、或保存后未重启）时，明确指出
// "管理端无法直写中心"的具体原因，避免静默回退本地。管理端"无法上传"看这里。
func handleSyncDiagnostics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	sbURL, _, hasSaved := loadSavedSBConfig()
	_, _, _, syncOn := syncSnapshot()
	manage := manageMode.Load()
	var cfg config.Sync
	if c, err := config.Load(); err == nil {
		cfg = c.Sync
	}
	var issues []string
	if hasSaved && !manage {
		issues = append(issues,
			"已保存面板「中心配置」但管理模式未激活：请在中心配置页重新保存一次（热激活，免重启）。")
	}
	if hasSaved && manage {
		issues = append(issues, "管理模式已由面板配置激活，可正常直写中心。")
	}
	if cfg.CenterKey == "" && !hasSavedCenterKey() {
		issues = append(issues, "center_key 未配置（可选）：中心库 token 将存明文，访问安全由 Supabase RLS/API key 负责。多代理共享中心、担心 key 泄露时建议配置。")
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"manage_mode":           manage,
		"sync_configured":       syncOn,
		"role":                  localRole(),
		"sb_saved":              hasSaved,
		"sb_url":                sbURL,
		"center_key_configured": cfg.CenterKey != "" || hasSavedCenterKey(),
		"issues":                issues,
	})
}

// handleSyncPullMode GET 返回拉取模式 + pending；POST 设置模式。
//   - auto（默认）：轮询自动 apply 中心更新（现状行为）
//   - manual：轮询只记 pending_remote_version 不 apply，保留代理本地应急改；
//     用户手动点"从中心拉取"（POST /api/sync/refresh）才 apply。
func handleSyncPullMode(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		pending, _ := db.Get().GetSetting("pending_remote_version")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":                   pullMode(),
			"pending_remote_version": pending,
			"has_pending":            pending != "",
		})
		return
	}
	if r.Method == http.MethodPost {
		var body struct {
			Mode string `json:"mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSONError(w, 400, err)
			return
		}
		if body.Mode != "auto" && body.Mode != "manual" {
			writeJSONError(w, 400, errors.New("mode must be auto or manual"))
			return
		}
		if err := db.Get().SetSetting("sync_pull_mode", body.Mode); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		sbSystemLog("info", "拉取模式切换为 "+body.Mode)
		_ = json.NewEncoder(w).Encode(map[string]any{"mode": body.Mode, "ok": true})
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

// pushConfirmToken 是服务端强制的覆盖确认串。前端 confirm() 可被绕过（直接
// POST），因此 /api/sync/push 要求 body 带 {"confirm":"OVERWRITE-CENTER"} 才执行，
// 把"破坏性操作须显式确认"从可选的前端交互变成服务端硬约束。
const pushConfirmToken = "OVERWRITE-CENTER"

// handleSyncPush POST /api/sync/push —— 管理模式「本地到中心」整体覆盖推送。
// 用本地 SQLite 定义覆盖中心 5 张定义表（破坏性）。仅 manageMode 允许（store 为
// supabase.Store），且必须带服务端确认 token。覆盖前自动把当前中心快照备份到
// settings.center_backup_latest（含 token 密文，可据此人工/脚本回滚）。成功后
// syncOnce(true) 拉回刷新本地镜像（id 对齐 + 应用版本号）。返回新中心/本地各表行数。
func handleSyncPush(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if !manageMode.Load() {
		writeJSONError(w, http.StatusForbidden, errors.New("仅管理模式可推送（本地角色须为管理端）"))
		return
	}
	// 服务端强制确认：防止绕过前端 confirm() 直接 POST 触发全表覆盖。
	var body struct {
		Confirm string `json:"confirm"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, errors.New("请求体须为 JSON，且含 {\"confirm\":\""+pushConfirmToken+"\"}"))
		return
	}
	if body.Confirm != pushConfirmToken {
		writeJSONError(w, http.StatusBadRequest,
			errors.New("缺少覆盖确认：请在请求体中带 {\"confirm\":\""+pushConfirmToken+"\"}（此操作会用本地定义整体覆盖中心，不可撤销）"))
		return
	}
	d := db.Get()
	if d == nil {
		writeJSONError(w, http.StatusInternalServerError, errors.New("db not ready"))
		return
	}
	platforms, _ := d.GetPlatforms()
	keys, _ := d.GetAllPlatformKeys()
	rapis, _ := d.GetRAPIs()
	lapis, _ := d.GetLAPIs()
	orders, _ := d.GetAllLAPIRAPIOrders()

	sup, ok := store.A().(*supabase.Store)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, errors.New("当前 store 非中心实现，无法推送"))
		return
	}

	// 覆盖前自动备份当前中心快照（best-effort）：ReplaceAll 非原子，中途失败
	// 会把中心留在部分状态；有备份即可人工/脚本恢复。备份失败只告警不阻断推送。
	if snap, berr := sup.DumpAll(); berr == nil {
		if payload, merr := json.Marshal(map[string]any{
			"backed_up_at": time.Now().Format(time.RFC3339),
			"tables":       snap,
		}); merr == nil {
			if serr := d.SetSetting("center_backup_latest", string(payload)); serr == nil {
				sbSystemLog("info", "推送前已备份中心快照（settings.center_backup_latest）")
			} else {
				sbSystemLog("warn", "中心快照备份写入失败（不阻断推送）: "+serr.Error())
			}
		}
	} else {
		sbSystemLog("warn", "中心快照备份失败（不阻断推送）: "+berr.Error())
	}

	counts, err := sup.ReplaceAll(supabase.LocalSnapshot{
		Platforms: platforms, Keys: keys, RAPIs: rapis, LAPIs: lapis, Orders: orders,
	})
	if err != nil {
		sbSystemLog("error", "本地→中心推送失败: "+err.Error())
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	sbSystemLog("info", "本地→中心推送完成：平台×"+itoa(counts["platform"])+
		" / 凭据×"+itoa(counts["credential"])+" / 模型×"+itoa(counts["rapi"])+
		" / 绑定×"+itoa(counts["endpoint_credential"])+
		" / 接口×"+itoa(counts["lapi"])+" / 路由链×"+itoa(counts["lapi_rapi_order"]))
	// 拉回刷新本地镜像：让本地 id 与中心对齐、应用中心版本号。
	_ = syncOnce(true)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":            true,
		"center_tables": counts,
		"local_tables":  localTableCounts(),
	})
}

// activateCenter 热激活中心连接（免重启）：中心配置页保存 URL+key(+可选
// center_key）后调用。按探测到的角色激活：
//   - management：supabase.Store 直写中心（store.Use）+ 同步循环（读路径保持一致）；
//     center_key 可选——留空中心存明文 token，填了则写中心前加密。
//   - proxy：仅配置只读同步循环（等效于 proxy.cfg [sync]）。
//
// proxy.cfg [management]/[sync] 降级为可选的无头启动回退；面板 settings 为主。
// 返回激活后的角色；offline/校验失败返回错误，不改变现有状态。
func activateCenter(sbURL, sbKey, centerKeyHex string) (string, error) {
	role, _, err := probeSBRole(sbURL, sbKey)
	if err != nil {
		return sbRoleOffline, err
	}
	if role == sbRoleOffline {
		return sbRoleOffline, errors.New("中心不可达")
	}

	base := strings.TrimSuffix(strings.TrimSpace(sbURL), "/")
	syncCfg := config.Sync{
		SourceURL:       base + "/rest/v1/rpc/get_bundle",
		VersionURL:      base + "/rest/v1/rpc/get_version",
		AnonKey:         sbKey,
		CenterKey:       centerKeyHex,
		PollIntervalSec: 60,
	}

	if role == sbRoleManagement {
		// center_key 可选：留空 = 中心库 token 存明文（访问安全由 Supabase 负责）。
		// 提供了则校验格式并在写中心时加密 token（enc/dec 对空 key 自动退化为明文透传）。
		sup, nerr := supabase.New(supabase.Config{
			URL:        sbURL,
			ServiceKey: sbKey,
			CenterKey:  centerKeyHex,
		}, func() {
			_ = syncOnce(true) // 写后立即拉回，本地镜像即时一致
		})
		if nerr != nil {
			return role, nerr
		}
		if cerr := configureSync(syncCfg); cerr != nil {
			return role, cerr
		}
		store.Use(sup)
		manageMode.Store(true)
		ensureSyncLoop()
		sbSystemLog("info", "管理模式已热激活（免重启）：定义类写直写中心 "+base)
		logger.DefaultConsole().Info("service", "[MGMT] management mode hot-activated", "center", base)
		return role, nil
	}

	// proxy 角色：只读同步循环。
	if cerr := configureSync(syncCfg); cerr != nil {
		return role, cerr
	}
	manageMode.Store(false)
	ensureSyncLoop()
	sbSystemLog("info", "代理模式已热激活（免重启）：只读同步 "+base)
	logger.DefaultConsole().Info("service", "[SYNC] proxy mode hot-activated", "center", base)
	return role, nil
}
