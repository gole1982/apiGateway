// catalog_sync.go —— 启动时统一拉取上游模型目录，自动失效/恢复 rapi。
//
// 要解决的问题：rapi.key_ids 的"空 = 平台全部 Key"是一条**推断规则**，而上游的
// 模型目录是否随 Key 变化（权益分档）从未被验证过。发现模型时只拿 keys[0] 去探测，
// 若该 Key 权限偏低，得到的目录是残缺的；发现向导会据此把"当前可用但 keys[0] 没权限"
// 的模型判为已下线并硬删（dashboard.html 的 importToRemove → DELETE ?sync=true）。
//
// 本文件在启动后异步执行一轮（最低优先级，不阻塞 ready）：
//  1. 对每个启用平台的每把启用 Key，拉取上游 /v1/models 目录
//  2. 对每个 rapi，根据各 Key 的探测状态判定失效/恢复/保持
//  3. 失效 = available=0 + unavailable_reason（**绝不删行**）
//  4. 恢复 = 连续 N 次探测确认在场 → available=1（N 默认 2，用户可调）
//
// 判定规则（与 probe_models.go 的 diffKeyAuthorization 同一套口径）：
//
//	pool    = 探测记录确认"目录含该 model"的启用 Key
//	unknown = 未探测的启用 Key ∪ 网络级失败的 Key
//	dead    = 凭据级失败（401/402/403 非 capability）的启用 Key —— 不进任何集合
//	失效条件 = pool 空 ∧ unknown 空
//	恢复条件 = pool 非空 ∧ 连续 N 次确认
//
// 与 probe_models.go 的分工：那个是**手动触发、只读诊断**（不改配置）；
// 这个是**启动触发、会写 available**。两者共用 key_model_probe 表和同一套判定口径。
package service

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"gateway/internal/db"
	"gateway/internal/logger"
	"gateway/internal/models"
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
	Enabled bool     // 凭据是否启用
	Dead    bool     // 凭据级失败（401/402/403 非 capability）—— 已知服务不了任何模型
	Failed  bool     // 网络级失败（超时/DNS/5xx）—— 状态未知
	Catalog []string // 探测到的上游目录（空 = 上游不发布目录）
}

// judgeRAPI 纯函数：根据各 Key 的探测状态，判定 rapi 应该失效/恢复/保持。
//
// 只输出有意义的动作：pool 非空就是恢复候选（是否真正恢复由冷却计数决定），
// pool 空且 unknown 空就是失效，pool 空但 unknown 非空就是保持。
func judgeRAPI(model string, keys []keyProbeState) (judgmentAction, string) {
	pool, unknown := 0, 0
	for _, k := range keys {
		if !k.Enabled {
			continue
		}
		if k.Dead {
			continue // 已知死，不进 pool 也不进 unknown
		}
		if k.Failed {
			unknown++
			continue
		}
		if catalogHasModel(k.Catalog, model) {
			pool++
		}
	}

	switch {
	case pool > 0:
		return judgmentRecover, ""
	case unknown > 0:
		return judgmentHold, "有 Key 尚未探测成功，等待下一轮"
	default:
		return judgmentInvalidate, "所有 Key 的目录均不含此模型"
	}
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
// manual 模型的真实调用探测
// ---------------------------------------------------------------------------

// probeManualRAPI 用真实 API 调用探测 source=manual 的 rapi。
//
// 为什么不能用目录判定：manual 模型是用户手工录入的，**不进 /v1/models**——
// 目录里永远查不到它们，用目录判定必然得出 over-grant 的假结论。
//
// 探测方式：对每把启用的 Key 各发一个 max_tokens=1 的 chat completion，
// 任一 Key 返回 200 即视为可用。401/402/403 视为凭据级失败（dead），
// 其余错误（超时/5xx/404）视为网络级失败（状态未知）。
//
// 返回 (pool, dead, unknown)：三个计数，与目录判定的 keyProbeState 语义对齐，
// 可以直接喂给 judgeRAPI。
func probeManualRAPI(ctx context.Context, plat models.Platform, rapi models.RAPIWithPlatform, keys []models.PlatformKey) (pool, dead, unknown int) {
	client := &http.Client{Timeout: 15 * time.Second}
	for _, k := range keys {
		if !k.Enabled {
			continue
		}
		if k.FailureType == 2 {
			dead++
			continue
		}
		status := probeChatCompletion(ctx, plat.BaseURL, rapi.Model, k.Token, client)
		if status == 200 {
			pool++
		} else if status == 401 || status == 402 || status == 403 {
			dead++ // 凭据级失败
		} else {
			unknown++ // 网络级失败 → 状态未知
		}
	}
	return pool, dead, unknown
}

// probeChatCompletion 发一个 max_tokens=1 的 chat completion 探测请求。
// 返回 HTTP 状态码；网络错误返回 0。
func probeChatCompletion(ctx context.Context, baseURL, model, token string, client *http.Client) int {
	url := strings.TrimSuffix(baseURL, "/") + "/chat/completions"
	body := fmt.Sprintf(`{"model":%q,"max_tokens":1,"messages":[{"role":"user","content":"hi"}]}`, model)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// ---------------------------------------------------------------------------
// 探测调度
// ---------------------------------------------------------------------------

// catalogSyncActive 保证同一时刻只有一轮目录同步。
var catalogSyncActive sync.Mutex

// runCatalogSync 启动后异步执行一轮目录同步。
// 跳过已被探测过的平台（手工或上次自动）—— 每个平台只自动探测一次，
// 之后由用户手工触发（与 probe_models.go 的 POST 端点同一入口）。
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

	// 只处理启用且从未探测过的平台。
	var targets []models.Platform
	for _, p := range platforms {
		if !p.Enabled {
			continue
		}
		if platformProbed(p.ID) {
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
	for _, p := range targets {
		wg.Add(1)
		go func(plat models.Platform) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := syncPlatformCatalog(ctx, plat); err != nil {
				logger.DefaultConsole().Warn("catalog", "[CATALOG] platform sync failed",
					"platform", plat.Name, "error", err.Error())
			}
		}(p)
	}
	wg.Wait()

	logger.DefaultConsole().Info("catalog", "[CATALOG] sync done", "platforms", len(targets))
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
		st := keyProbeState{Enabled: k.Enabled}
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
	//    source=manual 的模型用真实调用探测（不进目录），其余用目录判定。
	confirmations, _ := db.Get().GetRecoverConfirmations()
	for _, r := range rapis {
		if !r.Enabled {
			continue // 用户禁用的不自动失效
		}

		var action judgmentAction
		var reason string
		if strings.TrimSpace(r.Source) == "manual" {
			// manual 模型：用真实 API 调用探测，不走目录判定。
			pool, dead, unknown := probeManualRAPI(ctx, plat, r, keys)
			action, reason = judgeManualRAPI(pool, dead, unknown)
		} else {
			action, reason = judgeRAPI(r.Model, states)
		}

		if err := applyJudgment(r.ID, action, reason, confirmations); err != nil {
			logger.DefaultConsole().Error("catalog", "[CATALOG] apply judgment failed",
				"rapi", r.Model, "error", err.Error())
		}
	}
	return nil
}

// judgeManualRAPI 是 manual 模型的判定入口，签名与 judgeRAPI 对齐（同一套动作），
// 只是输入是三个计数而不是 keyProbeState 切片。
func judgeManualRAPI(pool, dead, unknown int) (judgmentAction, string) {
	switch {
	case pool > 0:
		return judgmentRecover, ""
	case unknown > 0:
		return judgmentHold, "manual 模型有 Key 探测未确认，等待下一轮"
	default:
		return judgmentInvalidate, "manual 模型所有 Key 均无法调用"
	}
}

// applyJudgment 把判定结果写到 rapi.available，含恢复冷却。
//
// 失效立即执行（响应快）；恢复要连续 confirmations 次确认在场才真正恢复（防抖）。
// streak 存在 rapi_catalog_state 表，跨重启保留。
func applyJudgment(rapiID int64, action judgmentAction, reason string, confirmations int) error {
	switch action {
	case judgmentInvalidate:
		// 失效：立即执行，清零 streak。
		if err := db.Get().SetRecoverStreak(rapiID, 0); err != nil {
			return fmt.Errorf("reset streak: %w", err)
		}
		return db.Get().SetRAPIUnavailableWithReason(rapiID, false, reason)

	case judgmentRecover:
		streak, err := db.Get().GetRecoverStreak(rapiID)
		if err != nil {
			streak = 0
		}
		streak++
		if err := db.Get().SetRecoverStreak(rapiID, streak); err != nil {
			return fmt.Errorf("bump streak: %w", err)
		}
		if streak < confirmations {
			// 冷却未满：保持现状（不恢复也不失效），等下一轮。
			return nil
		}
		// 冷却已满：恢复，清零 streak。
		if err := db.Get().SetRecoverStreak(rapiID, 0); err != nil {
			return fmt.Errorf("reset streak: %w", err)
		}
		return db.Get().SetRAPIUnavailableWithReason(rapiID, true, "")

	case judgmentHold:
		// 保持：不动 available，但清零 streak（本轮没确认在场）。
		return db.Get().SetRecoverStreak(rapiID, 0)
	}
	return nil
}
