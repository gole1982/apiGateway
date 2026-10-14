// probe_models.go —— key×model 授权探测（**纯诊断，不改变任何生效行为**）。
//
// 用途：现有 rapi.key_ids 的"空 = 平台全部 Key"是一条推断规则，且发现模型时
// 只用 keys[0] 单方面探测（service.go 的 /api/platforms/fetch-models 取
// keys[0].Token）。若该 Key 权限偏低，得到的目录残缺，发现向导会据此把正常模型
// 判为已下线并硬删（dashboard.html 的 importToRemove → DELETE ?sync=true）。
//
// 本文件用每把 Key 各自探测一遍，把"现状"与"真相"逐条对照，给出三类偏差：
//   - 过度授权 over_granted：现状服务该模型，但探测目录里没有 → 上游实际会 403
//   - 授权不足 under_granted：探测目录里有，但现状不服务 → 上游实际可用却没被路由到
//   - 一致     agreed
//
// 硬约束：本文件只写 db.Get() 的 key_model_probe 表（诊断表），绝不触碰
// endpoint_credential / rapi.key_ids / supabase 中心，因此不会改变网关行为，
// 也不会触发 sync 脏标记或影响 bundle 契约。
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"gateway/internal/db"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/store"
)

// ---------------------------------------------------------------------------
// 报告结构（诊断 DTO，不进 models 包：它不属于任何持久化实体）
// ---------------------------------------------------------------------------

// probeKeyReport 是单把 Key 的探测结果与偏差。
type probeKeyReport struct {
	KeyID        int64    `json:"key_id"`
	KeyIndex     int      `json:"key_index"`
	KeyLabel     string   `json:"key_label"`
	Enabled      bool     `json:"enabled"`
	Probed       bool     `json:"probed"`
	ProbeError   string   `json:"probe_error,omitempty"`
	ProbedAt     string   `json:"probed_at,omitempty"`
	CatalogSize  int      `json:"catalog_size"`
	OverGranted  []string `json:"over_granted"`
	UnderGranted []string `json:"under_granted"`
	Agreed       []string `json:"agreed"`
}

// probePlatformReport 是整个平台的偏差汇总。
type probePlatformReport struct {
	PlatformID   int64            `json:"platform_id"`
	PlatformName string           `json:"platform_name"`
	Keys         []probeKeyReport `json:"keys"`
	UnprobedKeys int              `json:"unprobed_keys"`
	FailedKeys   int              `json:"failed_keys"`
	OverTotal    int              `json:"over_granted_total"`
	UnderTotal   int              `json:"under_granted_total"`
	// Healthy 是"切量闸门"，不是"运行正常"。
	// 它要求：无过度授权 **且** 每把可用 Key 都探测成功。
	// 只有未探测/探测失败而恰好没查出过度授权时 healthy 必须为 false ——
	// 那是"什么都不知道"，不是"没问题"。据此切量等于盲切。
	Healthy bool `json:"healthy"`
}

// ---------------------------------------------------------------------------
// 纯函数：偏差判定（无 DB / 无网络依赖，直接单测）
// ---------------------------------------------------------------------------

// keyServesModelNow 复刻网关的现状判定（gateway.go filterKeysByKeyIDs）：
// 空 key_ids = 全部 Key；非空 = 白名单；白名单全是非法片段时也退化为不限制。
// 必须逐字对齐 —— 判定基准不一致，偏差报告就没有意义。
func keyServesModelNow(keyID int64, keyIDsCSV string) bool {
	csv := strings.TrimSpace(keyIDsCSV)
	if csv == "" {
		return true
	}
	ids := parseKeyIDs(csv)
	if len(ids) == 0 {
		return true // 对应 filterKeysByKeyIDs 的 len(allowed) == 0 分支
	}
	return intIn(ids, keyID)
}

// diffKeyAuthorization 纯函数：算出一把 Key 的授权偏差。
// catalog 是该 Key 探测到的上游目录（大小写不敏感匹配 —— 模型名大小写在
// 上游各站不一致，dashboard 也是按小写做存在性判定的）。
//
// 匹配对象同时包含 Model 和 Alias：这两者**不是**简单的大小写关系，线上确实
// 存在 model=zai-org/GLM-5.3-Flash 而 alias=z.ai/glm-5.3-flash 的情况。只比
// Model 会把"上游返回了正确模型"误判成过度授权 —— 那是最危险的误报方向。
//
// 只输出有意义的偏差：既不在现状也不在真相里的模型（两处都没有）不报。
func diffKeyAuthorization(keyID int64, catalog []string, rapis []models.RAPIWithPlatform) (over, under, agreed []string) {
	inProbe := make(map[string]bool, len(catalog)*2)
	for _, m := range catalog {
		m = strings.TrimSpace(m)
		if m != "" {
			inProbe[strings.ToLower(m)] = true
		}
	}

	// 上游目录里未必带别名形式，反向也补一层：把 rapis 的 model/alias 都塞进
	// 探测集合，等价于"任一标识命中即算该上游提供此模型"。
	over, under, agreed = []string{}, []string{}, []string{}
	for _, r := range rapis {
		current := keyServesModelNow(keyID, r.KeyIDs)
		truth := probeHasModel(inProbe, r)
		switch {
		case current && !truth:
			over = append(over, r.Model)
		case !current && truth:
			under = append(under, r.Model)
		case current && truth:
			agreed = append(agreed, r.Model)
		}
	}
	return over, under, agreed
}

// probeHasModel 判断探测目录里是否出现该端点。Model 与 Alias 任一命中即可。
func probeHasModel(inProbe map[string]bool, r models.RAPIWithPlatform) bool {
	for _, name := range []string{r.Model, r.Alias} {
		n := strings.ToLower(strings.TrimSpace(name))
		if n != "" && inProbe[n] {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 探测编排
// ---------------------------------------------------------------------------

// probeActive 保证同一时刻只有一轮探测，避免并发打爆上游。
var probeActive atomic.Bool

// probePlatformKeysModels 对平台的每把可用 Key 各探测一次上游模型目录，落库并
// 逐 Key 对照现状输出偏差。探测失败（401/网络/上游 5xx）不影响其它 Key。
func probePlatformKeysModels(ctx context.Context, platformID int64) (*probePlatformReport, error) {
	if !probeActive.CompareAndSwap(false, true) {
		return nil, errors.New("已有探测在进行中，请稍后重试")
	}
	defer probeActive.Store(false)

	plat, err := store.A().GetPlatformByID(platformID)
	if err != nil || plat == nil {
		return nil, errors.New("平台不存在")
	}
	// 定义类读当前 store（管理端=中心，代理端=本地）；探测结果写本地诊断表。
	keys, err := store.A().GetPlatformKeys(platformID)
	if err != nil {
		return nil, fmt.Errorf("读取密钥失败: %w", err)
	}
	rapis, err := store.A().GetRAPIsByPlatform(platformID)
	if err != nil {
		return nil, fmt.Errorf("读取模型失败: %w", err)
	}

	report := buildProbeReport(plat, keys, rapis)

	for i := range keys {
		k := &keys[i]
		kr := &report.Keys[i]
		if !k.Enabled {
			kr.ProbeError = "密钥已禁用，未探测"
			continue
		}

		names, status, ferr := discoverUpstreamModels(ctx, plat.BaseURL, k.Token)
		if ferr != nil {
			kr.ProbeError = fmt.Sprintf("上游 %d: %v", status, ferr)
			report.FailedKeys++
			logger.DefaultConsole().Warn("probe", "[PROBE] key discovery failed",
				"platform", plat.Name, "key_id", k.ID, "status", status, "error", ferr.Error())
			continue
		}

		now := time.Now()
		if err := db.Get().RecordKeyModelProbe(k.ID, names, now); err != nil {
			// 落库失败不影响本次报告：偏差已经算出来了，只是下次读不到。
			logger.DefaultConsole().Error("probe", "[PROBE] persist probe failed",
				"key_id", k.ID, "error", err.Error())
		}

		kr.Probed = true
		kr.ProbedAt = now.Format(time.RFC3339)
		kr.CatalogSize = len(names)
		kr.OverGranted, kr.UnderGranted, kr.Agreed = diffKeyAuthorization(k.ID, names, rapis)
		report.OverTotal += len(kr.OverGranted)
		report.UnderTotal += len(kr.UnderGranted)

		logger.DefaultConsole().Info("probe", "[PROBE] key catalog",
			"platform", plat.Name, "key_id", k.ID, "catalog", len(names),
			"over_granted", len(kr.OverGranted), "under_granted", len(kr.UnderGranted))
	}

	report.Healthy = report.cutoverSafe()
	return report, nil
}

// cutoverSafe 只有在"掌握足够真相且没查出过度授权"时才为 true。
// 单看 OverTotal == 0 是错的：一把 Key 都没探测过时它同样是 0，但那是无知
// 而非无害 —— 用它当闸门会导致盲切。
func (r *probePlatformReport) cutoverSafe() bool {
	return r.OverTotal == 0 && r.UnprobedKeys == 0 && r.FailedKeys == 0
}

// buildProbeReportGrouped 只读视图：不发任何上游请求，直接用已落库的探测记录
// 重建同一份偏差报告。用于事后复查，避免每次看报告都打上游。
func buildProbeReportGrouped(plat *models.Platform, keys []models.PlatformKey, rapis []models.RAPIWithPlatform) (*probePlatformReport, error) {
	report := buildProbeReport(plat, keys, rapis)
	for i := range keys {
		k := &keys[i]
		kr := &report.Keys[i]
		if !k.Enabled {
			kr.ProbeError = "密钥已禁用，未探测"
			continue
		}
		at, ok := db.Get().KeyProbedAt(k.ID)
		if !ok {
			kr.ProbeError = "尚未探测"
			report.UnprobedKeys++
			continue
		}
		catalog, err := db.Get().GetKeyModelProbe(k.ID)
		if err != nil {
			kr.ProbeError = "读取探测记录失败: " + err.Error()
			report.FailedKeys++
			continue
		}
		kr.Probed = true
		kr.ProbedAt = at.Format(time.RFC3339)
		kr.CatalogSize = len(catalog)
		kr.OverGranted, kr.UnderGranted, kr.Agreed = diffKeyAuthorization(k.ID, catalog, rapis)
		report.OverTotal += len(kr.OverGranted)
		report.UnderTotal += len(kr.UnderGranted)
	}
	report.Healthy = report.cutoverSafe()
	return report, nil
}

// buildProbeReport 铺好骨架（逐 Key 一一对应），偏差字段由调用方填充。
func buildProbeReport(plat *models.Platform, keys []models.PlatformKey, rapis []models.RAPIWithPlatform) *probePlatformReport {
	report := &probePlatformReport{
		PlatformID:   plat.ID,
		PlatformName: plat.Name,
		Keys:         make([]probeKeyReport, 0, len(keys)),
	}
	for i := range keys {
		k := &keys[i]
		report.Keys = append(report.Keys, probeKeyReport{
			KeyID:        k.ID,
			KeyIndex:     k.KeyIndex,
			KeyLabel:     k.Label,
			Enabled:      k.Enabled,
			OverGranted:  []string{},
			UnderGranted: []string{},
			Agreed:       []string{},
		})
	}
	return report
}

// ---------------------------------------------------------------------------
// HTTP 端点
// ---------------------------------------------------------------------------

func registerProbeRoutes(mux *http.ServeMux) {
	// GET  /api/probe-keys-models?platform_id=N  读已落库的探测记录（不打上游）
	// POST /api/probe-keys-models?platform_id=N  实跑一轮探测并返回偏差报告
	mux.HandleFunc("/api/probe-keys-models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		id, ok := parseID(r.URL.Query().Get("platform_id"))
		if !ok || id <= 0 {
			writeJSONError(w, http.StatusBadRequest, errors.New("platform_id 缺失或非法"))
			return
		}

		plat, err := store.A().GetPlatformByID(id)
		if err != nil || plat == nil {
			writeJSONError(w, http.StatusNotFound, errors.New("平台不存在"))
			return
		}
		keys, err := store.A().GetPlatformKeys(id)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
		rapis, err := store.A().GetRAPIsByPlatform(id)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}

		var report *probePlatformReport
		if r.Method == http.MethodPost {
			report, err = probePlatformKeysModels(r.Context(), id)
			if err != nil {
				writeJSONError(w, http.StatusConflict, err)
				return
			}
			logger.DefaultConsole().Info("probe", "[PROBE] summary", "detail", probeSummaryLine(report))
		} else {
			report, err = buildProbeReportGrouped(plat, keys, rapis)
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError, err)
				return
			}
		}
		out, _ := json.Marshal(report)
		_, _ = w.Write(out)
	})
}

// probeSummaryLine 把报告压成一行日志，便于在大量平台日志里快速定位偏差。
func probeSummaryLine(rep *probePlatformReport) string {
	return fmt.Sprintf("platform=%s over_granted=%d under_granted=%d unprobed=%d failed=%d healthy=%v",
		rep.PlatformName, rep.OverTotal, rep.UnderTotal, rep.UnprobedKeys, rep.FailedKeys, rep.Healthy)
}
