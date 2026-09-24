package service

import (
	"encoding/json"
	"net/http"
	"time"

	"gateway/internal/db"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/store"
)

// handleDashboardMetrics GET /api/dashboard/metrics?period=today|month
// 仪表盘单屏指标：4 维度（平台/Key/模型/接口）× 10 指标 TOP3，本地时区
// 今日/本月聚合；另附"待处理"清单（失效平台、永久失败 key、失效/冷却模型、
// 无可用路由的接口）与近期错误日志。
func handleDashboardMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	period := r.URL.Query().Get("period")
	if period != "month" {
		period = "today"
	}
	now := time.Now()
	var since time.Time
	if period == "month" {
		since = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	} else {
		since = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	}

	resp, err := db.Get().GetDashboardMetrics(since)
	if err != nil {
		writeJSONError(w, 500, err)
		return
	}
	resp.Period = period

	// 平台/Key 槽位按 id 聚合，补名称（并给 key 加平台上下文便于区分）。
	platforms, _ := store.A().GetPlatforms()
	platName := make(map[int64]string, len(platforms))
	for _, p := range platforms {
		platName[p.ID] = p.Name
	}
	allKeys, _ := store.A().GetAllPlatformKeys()
	keyLabel := map[int64]string{}
	for _, k := range allKeys {
		label := k.Label
		if label == "" {
			label = "Key#" + itoa(k.KeyIndex)
		}
		if pn := platName[k.PlatformID]; pn != "" {
			label = pn + "/" + label
		}
		keyLabel[k.ID] = label
	}
	mapNames(resp.Platform, platName)
	mapNames(resp.Key, keyLabel)

	// 待处理清单：需要用户介入的定义对象（失效/永久失败/无可用路由）。
	resp.NeedsAttention = buildNeedsAttention(platforms, allKeys)

	// 近期错误日志（前 8 条：失败或有 4xx/5xx）。
	resp.RecentErrors = recentErrorLogs(8)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// mapNames 把维度行里的 id 名称替换为可读名称（无映射时保留原样）。
func mapNames(dm db.DimensionMetrics, names map[int64]string) db.DimensionMetrics {
	replace := func(rows []db.MetricRow) []db.MetricRow {
		for i := range rows {
			id, isID := parseID(rows[i].Name)
			if !isID {
				continue // 名称本就是别名（模型/接口维度），不做 id 映射
			}
			if n, ok := names[id]; ok && n != "" {
				rows[i].Name = n
			}
		}
		return rows
	}
	dm.TotalTokens = replace(dm.TotalTokens)
	dm.InputTokens = replace(dm.InputTokens)
	dm.CachedTokens = replace(dm.CachedTokens)
	dm.OutputTokens = replace(dm.OutputTokens)
	dm.Attempts = replace(dm.Attempts)
	dm.Errors = replace(dm.Errors)
	dm.TTFTLowest = replace(dm.TTFTLowest)
	dm.RestLatLowest = replace(dm.RestLatLowest)
	dm.TTFTHighest = replace(dm.TTFTHighest)
	dm.RestLatHighest = replace(dm.RestLatHighest)
	return dm
}

// parseID 解析纯数字维度名（平台/key 槽位按 id 聚合）为 int64。返回
// (值, true) 仅当 s 是一个合法的带可选负号的整数；否则 (0, false) ——
// 名称本就是别名（模型/接口维度），不做 id 映射。用 bool 而非"返回 0 表非法"
// 是为避免与真实 id=0（虽 SQLite 自增从 1 起，但不依赖该假设）撞车。
func parseID(s string) (int64, bool) {
	if s == "" || s == "-" {
		return 0, false
	}
	var v int64
	neg := false
	for i, c := range s {
		if i == 0 && c == '-' {
			neg = true
			continue
		}
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v*10 + int64(c-'0')
	}
	if neg {
		return -v, true
	}
	return v, true
}

// AttentionItem 是一条需要用户处理的定义对象信息。
type AttentionItem struct {
	Kind   string `json:"kind"`    // platform | key | model | interface
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// buildNeedsAttention 汇总失效平台 / 永久失败 key / 失效或冷却模型 / 无可用路由接口。
func buildNeedsAttention(platforms []models.Platform, allKeys []models.PlatformKey) []AttentionItem {
	var out []AttentionItem
	now := time.Now()
	for _, p := range platforms {
		if !p.Enabled {
			out = append(out, AttentionItem{Kind: "platform", Name: p.Name, Reason: "平台已禁用"})
		} else if !p.Available {
			out = append(out, AttentionItem{Kind: "platform", Name: p.Name, Reason: "平台级失效（如 403/402），需手动恢复"})
		}
	}
	platByID := map[int64]models.Platform{}
	for _, p := range platforms {
		platByID[p.ID] = p
	}
	for _, k := range allKeys {
		if k.FailureType == 2 {
			pn := ""
			if p, ok := platByID[k.PlatformID]; ok {
				pn = p.Name + "/"
			}
			name := k.Label
			if name == "" {
				name = "Key#" + itoa(k.KeyIndex)
			}
			out = append(out, AttentionItem{Kind: "key", Name: pn + name, Reason: k.FailureReason})
		} else if k.ExpiresAt != nil && !k.ExpiresAt.IsZero() && now.After(*k.ExpiresAt) {
			out = append(out, AttentionItem{Kind: "key", Name: "Key#" + itoa(k.KeyIndex), Reason: "Key 已过期"})
		}
	}
	// 冷却中的 key（scheduler 实体 Cooling 状态，带恢复时刻倒计时）。
	// 这类 key failure_type=1，不在上面永久失效分支里；之前漏掉导致面板看不到冷却。
	if proxyGateway != nil {
		snap := proxyGateway.Scheduler().Snapshot()
		keyLabelByID := map[int64]string{}
		for _, k := range allKeys {
			label := k.Label
			if label == "" {
				label = "Key#" + itoa(k.KeyIndex)
			}
			if p, ok := platByID[k.PlatformID]; ok && p.Name != "" {
				label = p.Name + "/" + label
			}
			keyLabelByID[k.ID] = label
		}
		for _, ks := range snap.Keys {
			if !ks.Cooling {
				continue
			}
			label := keyLabelByID[ks.ID]
			if label == "" {
				label = "Key#" + itoa(int(ks.ID))
			}
			reason := ks.Reason
			if !ks.RecoverAt.IsZero() {
				left := time.Until(ks.RecoverAt).Round(time.Second)
				if left < 0 {
					left = 0
				}
				reason = "冷却中，至 " + ks.RecoverAt.Format("15:04:05") + "（剩余 " + left.String() + "）"
			}
			if reason == "" {
				reason = "冷却中"
			}
			out = append(out, AttentionItem{Kind: "key", Name: label, Reason: reason})
		}
	}
	rapis, _ := store.A().GetRAPIs()
	for _, ra := range rapis {
		if !ra.Enabled {
			continue // 用户主动停用不算"待处理"
		}
		if !ra.Available {
			out = append(out, AttentionItem{Kind: "model", Name: ra.Alias, Reason: ra.UnavailableReason})
		}
	}
	// 无可用路由的接口：启用的 lapi 但链上没有任何启用的 rapi。
	lapis, _ := store.A().GetLAPIs()
	orders, _ := db.Get().GetAllLAPIRAPIOrders()
	rapiByID := map[int64]models.RAPIWithPlatform{}
	for _, ra := range rapis {
		rapiByID[ra.ID] = ra
	}
	for _, la := range lapis {
		if !la.Enabled {
			continue
		}
		hasRoute := false
		for _, o := range orders {
			if o.LapiID != la.ID {
				continue
			}
			if ra, ok := rapiByID[o.RAPIID]; ok && ra.Enabled && ra.Available {
				hasRoute = true
				break
			}
		}
		if !hasRoute {
			out = append(out, AttentionItem{Kind: "interface", Name: la.Alias, Reason: "无可用路由（链上模型全部停用/失效）"})
		}
	}
	return out
}

// recentErrorLogs 取最近的错误请求（失败状态或 4xx/5xx 响应）。
func recentErrorLogs(limit int) []map[string]any {
	if logInstance == nil || logInstance.Storage == nil {
		return nil
	}
	// 取最近 50 条再过滤，复用现有 GetRequestLogs（status=failed 只覆盖一半场景）。
	filter := logger.RequestLogFilter{Limit: 50}
	logs, err := logInstance.Storage.GetRequestLogs(filter)
	if err != nil {
		return nil
	}
	out := make([]map[string]any, 0, limit)
	for _, l := range logs {
		if l.Status != "failed" && l.ResponseStatus < 400 {
			continue
		}
		msg := l.ErrorMessage
		if msg == "" && l.ResponseBody != "" {
			msg = truncateRunes(l.ResponseBody, 160)
		}
		out = append(out, map[string]any{
			"id":         l.ID,
			"timestamp":  l.Timestamp,
			"lapi":       l.LapiAlias,
			"rapi":       l.SelectedRAPI,
			"key_id":     l.SelectedKeyID,
			"status":     l.ResponseStatus,
			"latency_ms": l.LatencyMS,
			"error":      msg,
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func truncateRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "…"
}
