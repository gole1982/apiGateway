// catalog_sync.go —— 启动时统一拉取上游模型目录，自动失效/恢复 rapi。
//
// 要解决的问题：rapi.key_ids 的"空 = 平台全部 Key"是一条**推断规则**，而上游的
// 模型目录是否随 Key 变化（权益分档）从未被验证过。发现模型时只拿 keys[0] 去探测，
// 若该 Key 权限偏低，得到的目录是残缺的；发现向导会据此把"当前可用但 keys[0] 没权限"
// 的模型判为已下线并硬删（dashboard.html 的 importToRemove → DELETE ?sync=true）。
//
// 本文件在启动后异步执行一轮（最低优先级，不阻塞 ready），之后周期性重跑
// 仍有不可用端点的平台 —— 恢复要求连续 N 次确认，只跑一轮永远无法完成：
//  1. 对每个启用平台的每把启用 Key，拉取上游 /v1/models 目录
//  2. 对每个 rapi，根据各 Key 的探测状态判定失效/恢复/保持
//  3. 失效 = available=0 + unavailable_reason（**绝不删行**）
//  4. 恢复 = 连续 N 次探测确认在场 → available=1（N 默认 2，用户可调）
//  5. 状态真正翻转时发通知（一个平台一轮一条，见 notifyCatalogChanges）
//
// 判定规则（与 probe_models.go 的 diffKeyAuthorization 同一套口径）：
//
//	pool    = 探测记录确认"目录含该 model"的启用 Key
//	unknown = 未探测的启用 Key ∪ 网络级失败的 Key
//	dead    = 凭据级失败（401/402/403 非 capability）的启用 Key —— 不进任何集合
//	失效条件 = pool 空 ∧ unknown 空
//	恢复条件 = pool 非空 ∧ 连续 N 次确认
//
// pool/unknown 只统计**服务该模型的 Key**（rapi.key_ids 白名单，空=全部）：
// 模型只绑了 Key A 时，Key B 目录里有它并不代表 A 还能服务，把 B 计入 pool
// 会让这种端点永远失效不掉。
//
// **source=manual 的模型整类不参与**：用户手工录入的模型只有"启用/禁用"两态，
// 没有"失效/有效"。它们本来就不进 /v1/models，目录判定必然误伤；真实调用探测
// 会烧钱且已被明确否决。所以既不探测也不写 available。
//
// 与 probe_models.go 的分工：那个是**手动触发、只读诊断**（不改配置）；
// 这个是**启动触发、会写 available**。两者共用 key_model_probe 表和同一套判定口径。
package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"gateway/internal/db"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/notify"
	"gateway/internal/store"
)

// ---------------------------------------------------------------------------
// 判定纯函数（无 DB / 无网络依赖，直接单测）
// ---------------------------------------------------------------------------

// judgmentAction 是判定结果的动作类型。
type judgmentAction string

const (
	// judgmentInvalidate 失效：所有 Key 的目录都不含该模型，且没有未知状态的 Key。
	judgmentInvalidate judgmentAction = "invalidate"
	// judgmentRecover 恢复：至少一把 Key 的目录含该模型，且冷却已满。
	judgmentRecover judgmentAction = "recover"
	// judgmentHold 保持：有 Key 状态未知（未探测/网络失败），等下一轮。
	judgmentHold judgmentAction = "hold"
)

// keyProbeState 是一把 Key 在某一轮探测中的状态。
type keyProbeState struct {
	ID      int64    // 平台 Key 的 id（判定它是否服务某模型用，见 judgeRAPI）
	Enabled bool     // 凭据是否启用
	Dead    bool     // 凭据级失败（401/402/403 非 capability）—— 已知服务不了任何模型
	Failed  bool     // 网络级失败（超时/DNS/5xx）—— 状态未知
	Catalog []string // 探测到的上游目录（空 = 上游不发布目录，不能据此判定缺席）
}

// judgeRAPI 纯函数：根据各 Key 的探测状态，判定 rapi 应该失效/恢复/保持。
// modelNames 同时包含 Model 与 Alias：两者不是简单的大小写关系，上游目录可能
// 只出现其中一种（见 probe_models.go 的 probeHasModel）。
//
// serves 报告某把 Key 是否服务该模型（rapi.key_ids 白名单；nil = 全部服务）。
// 必须按绑定关系过滤：模型只绑 Key A 时，Key B 目录里有它不代表 A 还能服务，
// 把 B 算进 pool 会让这种端点永远失效不掉（网关只会拿 A 去请求）。
//
// 只输出有意义的动作：pool 非空就是恢复候选（是否真正恢复由冷却计数决定），
// pool 空且 unknown 空就是失效，pool 空但 unknown 非空就是保持。
func judgeRAPI(modelNames []string, keys []keyProbeState, serves func(keyID int64) bool) (judgmentAction, string) {
	pool, unknown := 0, 0
	for _, k := range keys {
		if !k.Enabled {
			continue
		}
		if serves != nil && !serves(k.ID) {
			continue // 该 Key 不服务此模型：它的目录对这个模型没有发言权
		}
		if k.Dead {
			continue // 已知死，不进 pool 也不进 unknown
		}
		// 空目录 ≠ 所有模型都缺席：上游 200 + 空 data 很常见（聚合站），
		// 这种 Key 对任何模型的存废都提供不了信息，只能算未知。
		if k.Failed || !catalogInformative(k.Catalog) {
			unknown++
			continue
		}
		if catalogHasAnyModel(k.Catalog, modelNames) {
			pool++
		}
	}

	switch {
	case pool > 0:
		return judgmentRecover, ""
	case unknown > 0:
		return judgmentHold, "有 Key 目录为空或尚未探测成功，等待下一轮"
	default:
		return judgmentInvalidate, "所有 Key 的目录均不含此模型"
	}
}

// catalogInformative 报告目录是否包含至少一个非空模型名。
// 空目录只能证明"上游没给目录"，不能证明"所有模型都缺席"。
func catalogInformative(catalog []string) bool {
	for _, m := range catalog {
		if strings.TrimSpace(m) != "" {
			return true
		}
	}
	return false
}

// catalogHasAnyModel 判断目录是否命中任一标识（Model / Alias）。
func catalogHasAnyModel(catalog, modelNames []string) bool {
	for _, name := range modelNames {
		if catalogHasModel(catalog, name) {
			return true
		}
	}
	return false
}

// catalogHasModel 大小写不敏感判断目录里是否出现该模型。
// 与 probe_models.go 的 probeHasModel 同口径（上游各站大小写不一致）。
func catalogHasModel(catalog []string, model string) bool {
	lm := strings.ToLower(strings.TrimSpace(model))
	if lm == "" {
		return false
	}
	for _, m := range catalog {
		if strings.ToLower(strings.TrimSpace(m)) == lm {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 探测调度
// ---------------------------------------------------------------------------

// catalogSyncActive 保证同一时刻只有一轮目录同步。
var catalogSyncActive sync.Mutex

// runCatalogSync 执行一轮目录同步。
//
// 目标平台 = 从未探测过的平台 ∪ 本地仍有不可用端点的平台。
// 后者让"连续 N 次确认才恢复"的冷却真正可达成；健康平台不会被反复探测。
func runCatalogSync(ctx context.Context) {
	if !catalogSyncActive.TryLock() {
		return
	}
	defer catalogSyncActive.Unlock()

	platforms, err := store.A().GetPlatforms()
	if err != nil {
		logger.DefaultConsole().Error("catalog", "[CATALOG] list platforms failed", "error", err.Error())
		return
	}

	var targets []models.Platform
	for _, p := range platforms {
		if !p.Enabled {
			continue
		}
		// 管理模式/代理模式首次启动时本地镜像可能还没同步下来：此时探测会
		// 写下"已探测"记录，却无法应用判定（本地没有对应 rapi 行），
		// 之后平台又因已探测被跳过。未就绪就留给下一轮。
		ready, err := db.Get().LocalPlatformHasRAPIs(p.BaseURL)
		if err != nil {
			logger.DefaultConsole().Warn("catalog", "[CATALOG] read local mirror failed",
				"platform", p.Name, "error", err.Error())
			continue
		}
		if !ready {
			continue
		}
		probed := platformProbed(p.ID)
		unavailableCount := 0
		if probed {
			unavailable, err := db.Get().LocalUnavailableRAPIModels(p.BaseURL)
			if err != nil {
				logger.DefaultConsole().Warn("catalog", "[CATALOG] read local health failed",
					"platform", p.Name, "error", err.Error())
				continue
			}
			unavailableCount = len(unavailable)
		}
		if !catalogSyncNeeded(probed, unavailableCount) {
			continue
		}
		targets = append(targets, p)
	}
	if len(targets) == 0 {
		return
	}

	logger.DefaultConsole().Info("catalog", "[CATALOG] sync starting",
		"platforms", len(targets), "concurrency", catalogSyncConcurrency)

	// 平台间并发，平台内 Key 顺序执行（避免打爆上游）。
	sem := make(chan struct{}, catalogSyncConcurrency)
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(p models.Platform) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := syncPlatformCatalog(ctx, p); err != nil {
				logger.DefaultConsole().Warn("catalog", "[CATALOG] platform sync failed",
					"platform", p.Name, "error", err.Error())
			}
		}(t)
	}
	wg.Wait()

	logger.DefaultConsole().Info("catalog", "[CATALOG] sync done", "platforms", len(targets))
}

// catalogSyncNeeded 决定某平台本轮是否要探测：从未探测过，或仍有待恢复端点。
func catalogSyncNeeded(probed bool, unavailableCount int) bool {
	return !probed || unavailableCount > 0
}

// catalogSyncInterval 是恢复轮次的周期。只针对仍有不可用端点的平台重探，
// 健康平台会被 catalogSyncNeeded 跳过，因此不会持续打上游。
const catalogSyncInterval = 30 * time.Minute

// startCatalogSyncLoop 启动时立即跑一轮，之后按 catalogSyncInterval 重跑。
// stopCh 关闭后退出。
func startCatalogSyncLoop(stopCh <-chan struct{}) {
	runCatalogSync(context.Background())

	ticker := time.NewTicker(catalogSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			runCatalogSync(context.Background())
		}
	}
}

// catalogSyncConcurrency 是平台间并发上限。
const catalogSyncConcurrency = 4

// platformProbed 判断某平台是否已被探测过（key_model_probe 里有任何一行）。
// 用 token_hash 前缀匹配：该平台的 Key 的 token_hash 出现在探测表里。
// 简化实现：拉全部 token_hash 去重后逐个查——平台数少，开销可接受。
func platformProbed(platformID int64) bool {
	keys, err := store.A().GetPlatformKeys(platformID)
	if err != nil {
		return false
	}
	for _, k := range keys {
		h := models.TokenHash(k.Token)
		if h == "" {
			continue
		}
		at, ok, err := db.Get().CredentialProbedAt(h)
		if err == nil && ok && !at.IsZero() {
			return true
		}
	}
	return false
}

// syncPlatformCatalog 探测单个平台的全部 Key，判定并应用失效/恢复。
// 只处理 source=auto_discover 的模型；source=manual 的模型完全没有"失效/有效"
// 两态（只有启用/禁用），既不探测也不写 available，见下方循环里的说明。
func syncPlatformCatalog(ctx context.Context, plat models.Platform) error {
	keys, err := store.A().GetPlatformKeys(plat.ID)
	if err != nil {
		return fmt.Errorf("get keys: %w", err)
	}
	rapis, err := store.A().GetRAPIsByPlatform(plat.ID)
	if err != nil {
		return fmt.Errorf("get rapis: %w", err)
	}

	// 1. 逐把 Key 探测上游目录。
	states := make([]keyProbeState, 0, len(keys))
	for i := range keys {
		k := &keys[i]
		st := keyProbeState{ID: k.ID, Enabled: k.Enabled}
		if !k.Enabled {
			states = append(states, st)
			continue
		}
		// 凭据级失败：网关已标记 permanent failure 的 Key 不进 pool 也不进 unknown。
		st.Dead = k.FailureType == 2

		names, status, ferr := discoverUpstreamModels(ctx, plat.BaseURL, k.Token)
		if ferr != nil {
			// 网络级失败（超时/DNS/5xx）→ 状态未知。
			// 凭据级失败（401/402/403）已由 st.Dead 标记，这里不再重复。
			st.Failed = true
			states = append(states, st)
			logger.DefaultConsole().Warn("catalog", "[CATALOG] key probe failed",
				"platform", plat.Name, "key_id", k.ID, "status", status)
			continue
		}

		// 落库（token_hash 主键，与 probe_models.go 共用）。
		if err := db.Get().RecordKeyModelProbe(models.TokenHash(k.Token), names, time.Now()); err != nil {
			logger.DefaultConsole().Error("catalog", "[CATALOG] persist probe failed",
				"platform", plat.Name, "key_id", k.ID, "error", err.Error())
		}
		st.Catalog = names
		states = append(states, st)
	}

	// 2. 逐 rapi 判定并应用。
	//    只处理 source=auto_discover：source=manual 是用户手工录入的模型，
	//    既不进上游 /v1/models，也没有"失效/有效"两态（只有启用/禁用）。
	//    对它做任何 available 判定都是错的 —— 目录里必然查不到它，
	//    真实调用探测会烧钱且被需求方明确否决。整类跳过，available 保持原值。
	confirmations, _ := db.Get().GetRecoverConfirmations()
	changes := make([]catalogChange, 0)
	for _, r := range rapis {
		if !r.Enabled {
			continue // 用户禁用的不自动失效
		}
		if !models.IsAutoDiscovered(r.Source) {
			continue // 自定义模型（含 source 为空的存量行）没有失效态，只有启用/禁用
		}

		// 健康态只能写本地 SQLite；store.A() 在管理模式下返回中心侧 id，
		// 与本地自增序列无关，必须按自然键回查本地 id（见 AGENTS.md）。
		localID, ok, err := db.Get().LocalRAPIIDByNaturalKey(plat.BaseURL, r.Model)
		if err != nil {
			logger.DefaultConsole().Error("catalog", "[CATALOG] resolve local rapi failed",
				"platform", plat.Name, "rapi", r.Model, "error", err.Error())
			continue
		}
		if !ok {
			logger.DefaultConsole().Warn("catalog", "[CATALOG] local rapi mirror missing, skip judgment",
				"platform", plat.Name, "rapi", r.Model)
			continue
		}

		// Model 与 Alias 任一命中即算在场（两者不是简单大小写关系）。
		// 只让"服务该模型的 Key"参与判定（key_ids 白名单，空=全部）。
		keyIDs := r.KeyIDs
		action, reason := judgeRAPI([]string{r.Model, r.Alias}, states, func(keyID int64) bool {
			return keyServesModelNow(keyID, keyIDs)
		})

		result, err := applyJudgment(localID, action, reason, confirmations)
		if err != nil {
			logger.DefaultConsole().Error("catalog", "[CATALOG] apply judgment failed",
				"rapi", r.Model, "error", err.Error())
			continue
		}
		switch result {
		case judgmentInvalidated:
			changes = append(changes, catalogChange{Model: r.Model, Action: judgmentInvalidate})
			// 冻结内存态：请求持有的 rapis 快照可能仍是旧行，只改 DB 挡不住它。
			if proxyGateway != nil {
				proxyGateway.InvalidateRAPI(localID)
			}
		case judgmentRecovered:
			changes = append(changes, catalogChange{Model: r.Model, Action: judgmentRecover})
			// 清掉 Invalidated 冻结态，否则 PickAvailable 仍会跳过它（DB 已 available=1）。
			if proxyGateway != nil {
				proxyGateway.RevalidateRAPI(localID)
			}
		}
	}
	notifyCatalogChanges(plat, changes)
	return nil
}

// catalogChange 是本轮真正发生的状态翻转（已失效 → 再判失效不算，避免每轮刷通知）。
type catalogChange struct {
	Model  string
	Action judgmentAction // 只取 judgmentInvalidate / judgmentRecover
}

// judgmentResult 是 applyJudgment 的实际效果，用于决定要不要发通知。
type judgmentResult int

const (
	judgmentNoChange judgmentResult = iota
	judgmentInvalidated
	judgmentRecovered
)

// catalogNotifyDetailRunes 是通知详情里模型名的截断长度（通知是给人看的，
// 不该被几百个模型名撑爆）。
const catalogNotifyDetailRunes = 200

// notifyCatalogChanges 把本轮的状态翻转汇总成通知：一个平台一轮一条。
// 逐模型发通知在目录抖动时会瞬间刷屏（实测一次下架几十个模型），
// 汇总后既保留"哪些模型"的细节，又只占一条未读。
//
// entityID 传 0 而不是 plat.ID：面板对 menu=models 的未读条目按 entity_id
// 当 rapi.id 回查并预填搜索框，传平台 id 会误命中同号的无关模型。
// 汇总通知本就没有"单一模型"可定位，落到模型页即可。
func notifyCatalogChanges(plat models.Platform, changes []catalogChange) {
	if len(changes) == 0 {
		return
	}
	invalidated, recovered := splitCatalogChanges(changes)

	if len(invalidated) > 0 {
		logger.DefaultConsole().Warn("catalog", "[CATALOG] models auto-invalidated",
			"platform", plat.Name, "count", len(invalidated), "models", strings.Join(invalidated, ","))
		if notifySvc != nil {
			// 这里只有 auto_discover 模型（manual 整类不参与）。逐模型的准确
			// 原因（如"所有 Key 的目录均不含此模型"）落在 rapi.unavailable_reason。
			notifySvc.PublishEvent("error", notify.MenuModels, "failure",
				fmt.Sprintf("%s：%d 个模型已自动失效", plat.Name, len(invalidated)),
				"已自动标记为不可用："+joinModelNames(invalidated, catalogNotifyDetailRunes),
				0)
		}
	}
	if len(recovered) > 0 {
		logger.DefaultConsole().Info("catalog", "[CATALOG] models recovered",
			"platform", plat.Name, "count", len(recovered), "models", strings.Join(recovered, ","))
		if notifySvc != nil {
			notifySvc.PublishEvent("info", notify.MenuModels, "update",
				fmt.Sprintf("%s：%d 个模型已恢复", plat.Name, len(recovered)),
				"已恢复可用："+joinModelNames(recovered, catalogNotifyDetailRunes),
				0)
		}
	}
}

// splitCatalogChanges 把变更按动作分成两组（保持原顺序，便于人读）。
func splitCatalogChanges(changes []catalogChange) (invalidated, recovered []string) {
	for _, c := range changes {
		switch c.Action {
		case judgmentInvalidate:
			invalidated = append(invalidated, c.Model)
		case judgmentRecover:
			recovered = append(recovered, c.Model)
		}
	}
	return invalidated, recovered
}

// joinModelNames 用顿号连接模型名，超过 maxRunes 个字符截断加省略号。
func joinModelNames(names []string, maxRunes int) string {
	s := strings.Join(names, "、")
	if maxRunes <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes]) + "…"
}

// applyJudgment 把判定结果写到 rapi.available，含恢复冷却，并回报本次是否
// 真的翻转了状态（只有翻转才该发通知）。
//
// 失效立即执行（响应快）；恢复要连续 confirmations 次确认在场才真正恢复（防抖）。
// streak 存在 rapi_catalog_state 表，跨重启保留。
func applyJudgment(rapiID int64, action judgmentAction, reason string, confirmations int) (judgmentResult, error) {
	switch action {
	case judgmentInvalidate:
		// 失效：立即执行，清零 streak。
		if err := db.Get().SetRecoverStreak(rapiID, 0); err != nil {
			return judgmentNoChange, fmt.Errorf("reset streak: %w", err)
		}
		// 先读旧值再写：恢复轮每 30 分钟重跑一次，已失效的模型会被再判一次
		// 失效，不区分就会同一批模型反复发通知。
		wasAvailable, err := db.Get().LocalRAPIAvailable(rapiID)
		if err != nil {
			return judgmentNoChange, fmt.Errorf("read availability: %w", err)
		}
		if err := db.Get().SetRAPIUnavailableWithReason(rapiID, false, reason); err != nil {
			return judgmentNoChange, err
		}
		if wasAvailable {
			return judgmentInvalidated, nil
		}
		return judgmentNoChange, nil

	case judgmentRecover:
		streak, err := db.Get().GetRecoverStreak(rapiID)
		if err != nil {
			streak = 0
		}
		streak, ready := nextRecoverStreak(streak, confirmations)
		if err := db.Get().SetRecoverStreak(rapiID, streak); err != nil {
			return judgmentNoChange, fmt.Errorf("bump streak: %w", err)
		}
		if !ready {
			// 冷却未满：保持现状（不恢复也不失效），等下一轮。
			return judgmentNoChange, nil
		}
		// 冷却已满：恢复，清零 streak。
		if err := db.Get().SetRecoverStreak(rapiID, 0); err != nil {
			return judgmentNoChange, fmt.Errorf("reset streak: %w", err)
		}
		wasAvailable, err := db.Get().LocalRAPIAvailable(rapiID)
		if err != nil {
			return judgmentNoChange, fmt.Errorf("read availability: %w", err)
		}
		if err := db.Get().SetRAPIUnavailableWithReason(rapiID, true, ""); err != nil {
			return judgmentNoChange, err
		}
		if !wasAvailable {
			return judgmentRecovered, nil
		}
		return judgmentNoChange, nil

	case judgmentHold:
		// 保持：不动 available，但清零 streak（本轮没确认在场）。
		return judgmentNoChange, db.Get().SetRecoverStreak(rapiID, 0)
	}
	return judgmentNoChange, nil
}

// nextRecoverStreak 推进恢复冷却计数，返回新 streak 与是否达到恢复阈值。
// 抽成纯函数锁定"连续 N 次确认"的语义。
func nextRecoverStreak(streak, confirmations int) (int, bool) {
	if confirmations < 1 {
		confirmations = 1
	}
	streak++
	return streak, streak >= confirmations
}
