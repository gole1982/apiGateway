package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"gateway/internal/db"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/prom"
	"gateway/internal/store"
)

// runtimePorts 记录本次启动实际生效的监听端口，供 /metrics 暴露。
//
// 存在包级变量而不是每次重读 config：config.Load() 读的是 exe 同目录的
// proxy.cfg，而实际端口可能已被环境/面板改过（见 savedLogLevel 那类
// "settings 覆盖 cfg" 的既有模式）。这里以 Run() 里算出的最终值为准。
var runtimePorts struct {
	proxy int
	web   int
}

// handleMetrics GET /metrics
// Prometheus 文本曝光端点。挂在**代理**端口上而不是面板端口，理由见
// 部署说明：面板端口可能只绑 127.0.0.1，而采集通常与网关同机或同网段。
//
// 无认证是刻意的（与项目整体取舍一致：自部署自用）。暴露的全是运行
// 状态与计数，不含 token / 平台凭据 / 请求体。
func handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	rt := prom.Runtime{Version: Version, ProxyPort: runtimePorts.proxy, WebPort: runtimePorts.web}
	rt.RAPIs, _ = store.A().GetRAPIs()
	rt.LAPIs, _ = store.A().GetLAPIs()
	rt.Keys, _ = store.A().GetAllPlatformKeys()
	if proxyGateway != nil {
		rt.Snap = proxyGateway.Scheduler().Snapshot()
	}
	if totals, err := db.Get().GetTrafficTotals(); err == nil {
		rt.Traffic = prom.TrafficCounters{
			TotalRequests:   totals.TotalRequests,
			SuccessRequests: totals.SuccessRequests,
			FailedRequests:  totals.FailedRequests,
			InputTokens:     totals.InputTokens,
			OutputTokens:    totals.OutputTokens,
			CachedTokens:    totals.CachedTokens,
			RetryTotal:      totals.RetryTotal,
			FallbackTotal:   totals.FallbackTotal,
		}
	}
	// 采集失败（DB 还没建表等）不阻断响应：/metrics 返回部分指标远好于
	// 整个端点 500 —— Prometheus 对抓取失败会直接告警，而"少了几个
	// counter"只会让图上缺点东西，不该升级成故障。

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	// 写失败（客户端断开）无事可做，也无法再写状态行 —— Prometheus 抓取
	// 超时是最常见的原因，记一条日志便于和抓取侧的问题对上。
	if _, err := io.WriteString(w, prom.Collect(rt)); err != nil {
		logger.DefaultConsole().Warn("service", "[METRICS] write response failed", "error", err.Error())
	}
}

// handleClientUsage GET /api/analytics/clients?period=today|month|7d&limit=N
// 下游客户端用量（按 client_ip 聚合）。见 db.ClientUsage 关于"为什么以
// IP 为身份口径"的说明。
func handleClientUsage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	now := time.Now()
	period := r.URL.Query().Get("period")
	var since time.Time
	switch period {
	case "month":
		since = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	case "7d":
		since = now.AddDate(0, 0, -7)
	case "30d":
		since = now.AddDate(0, 0, -30)
	default: // today
		period = "today"
		since = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	}

	limit := 20
	if v, ok := parseID(r.URL.Query().Get("limit")); ok && v > 0 && v <= 200 {
		limit = int(v)
	}

	rows, err := db.Get().GetClientUsage(since, limit)
	if err != nil {
		writeJSONError(w, 500, err)
		return
	}
	if rows == nil {
		rows = []db.ClientUsage{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"period":  period,
		"since":   since.Format(time.RFC3339),
		"clients": rows,
	})
}

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
//
// 就地修改：dm 传值只复制结构体，其中各维度是切片，底层数组与调用方共享，
// 所以 rows[i].Name = n 对调用方可见。因此不返回值 —— 返回的那份 dm 与
// 调用方持有的完全等价。
func mapNames(dm db.DimensionMetrics, names map[int64]string) {
	replace := func(rows []db.MetricRow) {
		for i := range rows {
			id, isID := parseID(rows[i].Name)
			if !isID {
				continue // 名称本就是别名（模型/接口维度），不做 id 映射
			}
			if n, ok := names[id]; ok && n != "" {
				rows[i].Name = n
			}
		}
	}
	replace(dm.TotalTokens)
	replace(dm.InputTokens)
	replace(dm.CachedTokens)
	replace(dm.OutputTokens)
	replace(dm.Attempts)
	replace(dm.Errors)
	replace(dm.TTFTLowest)
	replace(dm.RestLatLowest)
	replace(dm.TTFTHighest)
	replace(dm.RestLatHighest)
}

// parseID 解析纯整数字符串为 int64。返回 (值, true) 仅当 s 是一个合法的
// 带可选负号的整数；否则 (0, false)。用 bool 而非"返回 0 表非法"，是为
// 避免与真实 id=0（虽 SQLite 自增从 1 起，但不依赖该假设）撞车。
//
// 两个用途：维度名里的 id 聚合（平台/key 槽位；名称本就是别名的模型/接口
// 维度不做映射），以及所有 handler 的 id/天数类 query 参数解析。
// 后者刻意比 fmt.Sscanf 严格："12abc" 在 Sscanf 下会静默变成 12，在这里
// 是非法输入 —— 调用方按 400 处理，而不是拿着截断后的数字去查库。
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
	Kind   string `json:"kind"` // platform | key | model | interface
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
