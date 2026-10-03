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
	syncOpMu       sync.Mutex      // 串行化"应用配置"类操作（拉取应用/并集合并/回推）：
	// 轮询与手动刷新/管理写后拉回并发时各干各的，会导致双份 ReplaceAll 空转
	// bump 中心版本。版本检查本身无锁，快路径不受影响。
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

// syncAction 是版本号 + 脏标记 + 拉取模式共同决定的同步动作（纯函数，可单测）。
//   - skip：版本一致，什么都不做、不提示（脏横幅由状态接口常驻展示）。
//   - pending：manual 模式且非强制，只记待拉取版本，不应用（含合并也不做），
//     保留本地应急改。
//   - merge：版本不一致且本地脏，并集合并（冲突本地胜）。
//   - pull：版本不一致且本地干净，直接拉取覆盖（本地无独有内容，并集即中心）。
func syncAction(remote, lastGood int64, dirty, manual, force bool) string {
	if remote == lastGood {
		return "skip"
	}
	if !force && manual {
		return "pending"
	}
	if dirty {
		return "merge"
	}
	return "pull"
}

// syncOnce 轮询中心版本号；变了才拉全量并单事务应用（version 短路，不拉全量）。
// force=true 时无论拉取模式都应用（手动拉取/管理端写后拉回）；
// force=false 时按拉取模式：auto（默认）应用，manual 只记 pending_remote_version 不应用。
//
// 脏本地（面板应急改）遇中心版本变化 → 并集合并而非覆盖：中心独有行并入，
// 本地独有行保留，同键冲突本地胜；并集相对中心有新增/变更且 key 可写时回推
// 中心（只读 key 则本地生效、保留脏标记待手动推送）。全程在调用方 goroutine
// （轮询/手动刷新/管理写后拉回）执行，不阻塞面板与转发。
func syncOnce(force bool) error {
	client, centerKey, sourceURL, configured := syncSnapshot()
	if !configured {
		return errors.New("sync not configured")
	}
	syncOpMu.Lock()
	defer syncOpMu.Unlock()

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
	dirty, _, _ := edgeDirtyState()
	switch syncAction(remote, st.LastGoodVersion, dirty, pullMode() == "manual", force) {
	case "skip":
		return nil // 版本没变，短路
	case "pending":
		_ = db.Get().SetSetting("pending_remote_version", strconv.FormatInt(remote, 10))
		sbSystemLog("info", "manual 模式：中心有新版本 v"+itoa(int(remote))+"，待手动拉取")
		return nil
	case "merge":
		env, err := client.PullBundle(ctx, st.LastGoodVersion)
		if errors.Is(err, bundle.ErrNoChange) {
			return nil // 轮询间隙版本又追平（拉取时已一致）
		}
		if err != nil {
			return err
		}
		return applyMerge(env, centerKey, sourceURL)
	default: // pull
		if _, err := pullApplyBundle(ctx, client, centerKey, sourceURL, st.LastGoodVersion); err != nil {
			return err
		}
		return nil
	}
}

// pullApplyBundle 拉取全量并单事务应用（本地干净时的直接覆盖路径，以及合并
// 回推后的版本对齐）。返回应用的 envelope（ErrNoChange 时返回 nil,nil）。
func pullApplyBundle(ctx context.Context, client *bundle.Client, centerKey []byte, sourceURL string, lastGood int64) (*bundle.Envelope, error) {
	env, err := client.PullBundle(ctx, lastGood)
	if errors.Is(err, bundle.ErrNoChange) {
		return nil, nil // 轮询间隙版本又追平（拉取时已一致）
	}
	if err != nil {
		return nil, err
	}
	if err := db.Get().ApplyBundle(env, centerKey, sourceURL); err != nil {
		sbSystemLog("error", "应用中心配置失败: "+err.Error())
		return nil, err
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
	return env, nil
}

// applyMerge 执行一次并集合并。调用方持有 syncOpMu。
// 流程：本地快照 ∪ 中心包 → 本地事务应用 →（并集相对中心有新增/变更且 key
// 可写）回推中心 → 拉回对齐。只读 key 时本地生效、保留脏标记。
// 合并中途的本地新修改靠脏 generation 守卫：回推/清脏前 generation 变了则
// 保留脏标记，横幅持续提示，由用户手动推送，不静默吞修改。
func applyMerge(env *bundle.Envelope, centerKey []byte, sourceURL string) error {
	gen := edgeDirtyGen.Load()
	d := db.Get()

	localSnap, err := d.ExportBundle(centerKey)
	if err != nil {
		sbSystemLog("error", "并集合并：本地快照导出失败: "+err.Error())
		return err
	}
	merged, sum := MergeBundles(*localSnap, env.Bundle)
	sum.CenterVer = env.Version
	mergedEnv := &bundle.Envelope{SchemaVersion: bundle.SchemaVersion, Version: env.Version, Bundle: merged}
	if err := d.ApplyBundle(mergedEnv, centerKey, sourceURL); err != nil {
		sbSystemLog("error", "并集合并：本地应用失败: "+err.Error())
		return err
	}
	_ = d.SetSetting("pending_remote_version", "")

	if !sum.AddsToCenter {
		// 并集与中心完全一致（脏来自已撤销的修改等）：清脏，无需回推。
		clearEdgeDirtyIfUnchanged(gen)
		saveMergeSummary(sum)
		sbSystemLog("info", mergeSummaryText(sum))
		return nil
	}
	writable, sbURL, sbKey := probeCenterWritability()
	if !writable {
		saveMergeSummary(sum)
		sbSystemLog("warn", mergeSummaryText(sum)+"；中心 key 只读，未回推（本地已生效，保留未同步标记）")
		return nil
	}
	setMerging(true)
	pushErr := pushBackMerged(sbURL, sbKey)
	setMerging(false)
	if pushErr != nil {
		sum.PushError = pushErr.Error()
		saveMergeSummary(sum)
		sbSystemLog("error", "并集合并：回推中心失败（本地已生效，可稍后手动推送）: "+pushErr.Error())
		return nil
	}
	sum.PushedBack = true
	saveMergeSummary(sum)
	sbSystemLog("info", mergeSummaryText(sum))
	if !clearEdgeDirtyIfUnchanged(gen) {
		sbSystemLog("warn", "合并回推期间本地又有新修改，保留脏标记待下次处理")
		return nil
	}
	// 回推 bump 了中心版本，拉回对齐（此时本地干净，走直接拉取分支）。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, ck, src, _ := syncSnapshot()
	st, err := db.Get().GetSyncState()
	if err != nil {
		return err
	}
	if _, err := pullApplyBundle(ctx, client, ck, src, st.LastGoodVersion); err != nil {
		sbSystemLog("warn", "合并回推后拉回对齐失败: "+err.Error())
	}
	return nil
}

// pushBackMerged 把并集回推中心（调用方持有 syncOpMu）。
// 管理模式用激活中的中心 Store；代理模式用可写 key 建一次性 Store 回推，
// 不切换运行模式、不翻 manageMode。
func pushBackMerged(sbURL, sbKey string) error {
	var sup *supabase.Store
	if manageMode.Load() {
		s, ok := store.A().(*supabase.Store)
		if !ok {
			return errors.New("当前 store 非中心实现，无法推送")
		}
		sup = s
	} else {
		t, err := transientCenterStore(sbURL, sbKey)
		if err != nil {
			return err
		}
		sup = t
	}
	counts, err := doSyncPushCore(sup)
	if err != nil {
		return err
	}
	logger.DefaultConsole().Info("service", "[SYNC] merge pushed back",
		"center_tables", counts)
	return nil
}

// saveMergeSummary 持久化最近一次并集摘要（面板横幅用，best-effort）。
func saveMergeSummary(sum MergeSummary) {
	d := db.Get()
	if d == nil {
		return
	}
	if payload, err := json.Marshal(sum); err == nil {
		_ = d.SetSetting(settingSyncLastMerge, string(payload))
	}
}

// loadMergeSummary 读最近一次并集摘要（无则返回 nil）。
func loadMergeSummary() *MergeSummary {
	d := db.Get()
	if d == nil {
		return nil
	}
	raw, _ := d.GetSetting(settingSyncLastMerge)
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var sum MergeSummary
	if err := json.Unmarshal([]byte(raw), &sum); err != nil {
		return nil
	}
	return &sum
}

// mergeCountText 把表行数 map 渲染成"平台×2/模型×3"（零值跳过，无行为空串）。
func mergeCountText(m map[string]int) string {
	names := map[string]string{
		"platform": "平台", "credential": "凭据", "rapi": "模型",
		"endpoint_credential": "绑定", "lapi": "接口", "lapi_rapi_order": "路由链",
	}
	parts := make([]string, 0, len(m))
	for _, t := range centerDefinitionTables {
		if n := m[t]; n > 0 {
			name := names[t]
			if name == "" {
				name = t
			}
			parts = append(parts, name+"×"+itoa(n))
		}
	}
	return strings.Join(parts, "/")
}

// mergeSummaryText 并集摘要一句话（系统日志 + 面板横幅共用）。
func mergeSummaryText(sum MergeSummary) string {
	msg := "并集合并完成（中心 v" + itoa(int(sum.CenterVer)) + "）"
	if s := mergeCountText(sum.FromCenter); s != "" {
		msg += "：中心新增并入本地 " + s
	}
	if s := mergeCountText(sum.KeptLocal); s != "" {
		msg += "；本地独有保留 " + s
	}
	if n := len(sum.Conflicts); n > 0 {
		msg += "；冲突 " + itoa(n) + " 项按本地胜"
	}
	if sum.PushedBack {
		msg += "；已回推中心"
	}
	return msg
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
// centerDefinitionTables 是中心 6 张定义表的规范表名。三个计数源必须用
// 同一套键，前端三列（中心/本地/启动快照）共用一个表名数组取数：
//   - fillSBTables（sb-config 卡中心列）：按此表名逐个 REST 查数；
//   - handleSyncCenter tables（sync 卡中心列）：按此表名从 bundle 取数；
//   - localTableCounters（两卡的本地列 + 启动快照）：本地 SQLite 同名表。
//
// 2026-09-26 事故：三处各写各的字面量（platform_keys 旧名、bundle 复数键
// credentials/bindings），面板密钥行显示"—"、推送 toast 显示 0，而全部单测
// 全绿 —— 因为 service 包覆盖率仅 1.8%，根本没覆盖到接线处。
// TestDefinitionTableKeysConsistent 把"三处一致"锁死，此后加表只改这一处。
var centerDefinitionTables = []string{
	"platform", "credential", "endpoint_credential",
	"rapi", "lapi", "lapi_rapi_order",
}

// bundleTableCount 按规范表名从 bundle 取行数。switch 显式列出全部 6 张表，
// 未知表名返回 -1（调用方跳过），而不是静默计 0 —— 拼错表名时 0 会伪装成
// "空表"，-1 会在面板上暴露为缺失。
func bundleTableCount(b *bundle.Bundle, table string) int {
	if b == nil {
		return -1
	}
	switch table {
	case "platform":
		return len(b.Platforms)
	case "credential":
		return len(b.Credentials)
	case "endpoint_credential":
		return len(b.Bindings)
	case "rapi":
		return len(b.RAPIs)
	case "lapi":
		return len(b.LAPIs)
	case "lapi_rapi_order":
		return len(b.LAPIRapiOrder)
	}
	return -1
}

// localTableCounter 把本地计数键与取值函数绑在一起。键集合可独立断言
// （TestDefinitionTableKeysConsistent），无需 DB；取值失败时跳过该键，
// 与旧行为一致（前端把缺失渲染为"—"）。
type localTableCounter struct {
	key   string
	count func(d *db.DB) (int, bool)
}

var localTableCounters = []localTableCounter{
	{"platform", func(d *db.DB) (int, bool) {
		ps, err := d.GetPlatforms()
		return len(ps), err == nil
	}},
	{"credential", func(d *db.DB) (int, bool) {
		ks, err := d.GetAllPlatformKeys()
		return len(ks), err == nil
	}},
	{"endpoint_credential", func(d *db.DB) (int, bool) {
		bs, err := d.GetAllEndpointCredentials()
		return len(bs), err == nil
	}},
	{"rapi", func(d *db.DB) (int, bool) {
		rs, err := d.GetRAPIs()
		return len(rs), err == nil
	}},
	{"lapi", func(d *db.DB) (int, bool) {
		ls, err := d.GetLAPIs()
		return len(ls), err == nil
	}},
	{"lapi_rapi_order", func(d *db.DB) (int, bool) {
		os, err := d.GetAllLAPIRAPIOrders()
		return len(os), err == nil
	}},
}

func localTableCounts() map[string]int {
	d := db.Get()
	if d == nil {
		return map[string]int{}
	}
	out := map[string]int{}
	for _, c := range localTableCounters {
		if n, ok := c.count(d); ok {
			out[c.key] = n
		}
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

// syncStatusExtra 状态接口公共附加字段：脏标记、可写性、合并中、最近合并摘要。
// center_writable 只在脏时才探测（平时免一次写探测往返）；无中心/离线一律 false。
// knownRole 非空时复用已探测的角色（免二次探测），空则脏时现探测。
func syncStatusExtra(knownRole string) map[string]any {
	out := map[string]any{"merging": syncMerging.Load()}
	dirty, at, reason := edgeDirtyState()
	out["dirty"] = dirty
	out["center_writable"] = false
	if dirty {
		out["dirty_at"] = at
		out["dirty_reason"] = reason
		if knownRole != "" {
			out["center_writable"] = knownRole == sbRoleManagement
		} else if writable, _, _ := probeCenterWritability(); writable {
			out["center_writable"] = true
		}
	}
	if sum := loadMergeSummary(); sum != nil {
		out["last_merge"] = sum
	}
	return out
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
	out := map[string]any{
		"enabled":           configured,
		"role":              localRole(),
		"current_version":   st.CurrentVersion,
		"last_good_version": st.LastGoodVersion,
		"last_synced_at":    lastSynced,
		"source_url":        st.SourceURL,
	}
	for k, v := range syncStatusExtra("") {
		out[k] = v
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// handleSyncRefresh POST /api/sync/refresh —— 控制台"手动刷新"按钮（设计 §5.1）。
// 异步触发后台执行（合并可能含拉取 + 本地应用 + 回推，耗时数秒到数十秒），
// 立即返回 accepted，不阻塞面板。结果经系统日志 + 状态接口（merging /
// last_merge / 脏标记）可见。失败不影响现有配置（fail-open）。
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
	go func() {
		setMerging(true)
		defer setMerging(false)
		if err := syncOnce(true); err != nil {
			sbSystemLog("warn", "手动刷新失败: "+err.Error())
		}
	}()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true})
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
			for k, v := range syncStatusExtra("") {
				out[k] = v
			}
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
				out["in_sync"] = edgeInSync(int64(centerVer), st.LastGoodVersion)
				if !st.LastSyncedAt.IsZero() {
					out["last_synced_at"] = st.LastSyncedAt.Format(time.RFC3339)
				}
			}
		}
		// 已在此分支探测过角色：复用，免二次探测。
		for k, v := range syncStatusExtra(role) {
			out[k] = v
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
	// 中心列的键与 centerDefinitionTables 同源（见该变量注释里的事故）。
	tables := map[string]int{}
	for _, t := range centerDefinitionTables {
		if n := bundleTableCount(&env.Bundle, t); n >= 0 {
			tables[t] = n
		}
	}
	out["tables"] = tables

	st, err := db.Get().GetSyncState()
	if err != nil {
		out["error"] = "local sync_state: " + err.Error()
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	out["center_version"] = env.Version
	out["local_version"] = st.LastGoodVersion
	out["in_sync"] = edgeInSync(env.Version, st.LastGoodVersion)
	for k, v := range syncStatusExtra("") {
		out[k] = v
	}
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

// doSyncPushCore 本地→中心整体覆盖推送的核心（备份 + ReplaceAll），供
// handleSyncPush（显式确认）与合并回推（pushBackMerged）共用。
// 成功后调用方负责拉回对齐（syncOnce(true)），本函数内不做，避免与外层
// syncOpMu 锁嵌套。
func doSyncPushCore(sup *supabase.Store) (map[string]int, error) {
	d := db.Get()
	if d == nil {
		return nil, errors.New("db not ready")
	}
	platforms, _ := d.GetPlatforms()
	keys, _ := d.GetAllPlatformKeys()
	rapis, _ := d.GetRAPIs()
	lapis, _ := d.GetLAPIs()
	orders, _ := d.GetAllLAPIRAPIOrders()

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
		return nil, err
	}
	sbSystemLog("info", "本地→中心推送完成：平台×"+itoa(counts["platform"])+
		" / 凭据×"+itoa(counts["credential"])+" / 模型×"+itoa(counts["rapi"])+
		" / 绑定×"+itoa(counts["endpoint_credential"])+
		" / 接口×"+itoa(counts["lapi"])+" / 路由链×"+itoa(counts["lapi_rapi_order"]))
	return counts, nil
}

// handleSyncPush POST /api/sync/push ——「本地到中心」整体覆盖推送。
// 用本地 SQLite 定义覆盖中心 6 张定义表（破坏性），且必须带服务端确认 token。
// 管理模式直接用激活中的中心 Store；代理模式下若中心 key 可写（面板存了
// secret key 但管理模式未激活等），用一次性 Store 回推，不切换运行模式。
// 只读 key 一律 403。覆盖前自动把当前中心快照备份到
// settings.center_backup_latest（含 token 密文，可据此人工/脚本回滚）。成功后
// syncOnce(true) 拉回刷新本地镜像（id 对齐 + 应用版本号）。返回新中心/本地各表行数。
func handleSyncPush(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
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
	var sup *supabase.Store
	if manageMode.Load() {
		if db.Get() == nil {
			writeJSONError(w, http.StatusInternalServerError, errors.New("db not ready"))
			return
		}
		s, ok := store.A().(*supabase.Store)
		if !ok {
			writeJSONError(w, http.StatusInternalServerError, errors.New("当前 store 非中心实现，无法推送"))
			return
		}
		sup = s
	} else {
		writable, sbURL, sbKey := probeCenterWritability()
		if !writable {
			writeJSONError(w, http.StatusForbidden, errors.New("仅可写 key 可推送（当前 key 只读或中心不可达）：请在中心配置页录入 secret key"))
			return
		}
		t, err := transientCenterStore(sbURL, sbKey)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
		sup = t
	}

	counts, err := doSyncPushCore(sup)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
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
