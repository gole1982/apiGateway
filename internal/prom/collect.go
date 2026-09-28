package prom

import (
	"strconv"

	"gateway/internal/models"
	"gateway/internal/scheduler"
)

// Runtime 是 Collect 所需的全部运行时输入。由 service 层组装，避免本包
// 直接依赖 db / store（那样会把采集逻辑和持久层耦在一起，且无法单测）。
type Runtime struct {
	Version string
	// ProxyPort / WebPort 让告警规则能区分"服务没起"与"端口被改"。
	ProxyPort int
	WebPort   int
	RAPIs     []models.RAPIWithPlatform
	LAPIs     []models.LAPI
	Keys      []models.PlatformKey
	Snap      scheduler.Snapshot
	// Traffic 来自 request_logs 的累计聚合（重启不归零，可当 counter）。
	Traffic TrafficCounters
}

// TrafficCounters 是 request_logs 的一次性累计聚合。
type TrafficCounters struct {
	TotalRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	InputTokens     int64
	OutputTokens    int64
	CachedTokens    int64
	RetryTotal      int64
	FallbackTotal   int64
}

// Collect 组装一次完整的指标快照。
//
// 命名遵循 Prometheus 约定：
//   - 前缀 apigateway_ 标明来源与命名空间
//   - _total 后缀标记 counter（可被 rate() 求导）
//   - 布尔量用 0/1 而非 true/false（Prometheus 不认布尔）
//
// 所有可能出错的注册调用都被忽略（指标名是编译期常量，见本文件各处调用），
// 这样调用点保持可读。真正需要防的是"值取错了"，那由单测覆盖。
func Collect(rt Runtime) string {
	r := New()

	// ---- 进程 ----
	_ = r.Gauge("apigateway_build_info", 1, "version", rt.Version)
	_ = r.Gauge("apigateway_up", 1)
	_ = r.Gauge("apigateway_port", float64(rt.ProxyPort), "name", "proxy")
	_ = r.Gauge("apigateway_port", float64(rt.WebPort), "name", "web")

	// ---- 定义对象存量（gauge：随配置增删）----
	rapiTotal, rapiEnabled, rapiAvailable := 0, 0, 0
	for _, ra := range rt.RAPIs {
		rapiTotal++
		if ra.Enabled {
			rapiEnabled++
		}
		if ra.Enabled && ra.Available {
			rapiAvailable++
		}
	}
	_ = r.Gauge("apigateway_rapi", float64(rapiTotal), "state", "total")
	_ = r.Gauge("apigateway_rapi", float64(rapiEnabled), "state", "enabled")
	_ = r.Gauge("apigateway_rapi", float64(rapiAvailable), "state", "available")

	lapiTotal, lapiEnabled := 0, 0
	for _, la := range rt.LAPIs {
		lapiTotal++
		if la.Enabled {
			lapiEnabled++
		}
	}
	_ = r.Gauge("apigateway_lapi", float64(lapiTotal), "state", "total")
	_ = r.Gauge("apigateway_lapi", float64(lapiEnabled), "state", "enabled")

	// ---- Key 池健康 ----
	cooling := make(map[int64]bool, len(rt.Snap.Keys))
	for _, ks := range rt.Snap.Keys {
		if ks.Cooling {
			cooling[ks.ID] = true
		}
	}
	keyTotal, keyEnabled, keyCooling := 0, 0, 0
	for _, k := range rt.Keys {
		keyTotal++
		if k.Enabled {
			keyEnabled++
		}
		if cooling[k.ID] {
			keyCooling++
		}
	}
	_ = r.Gauge("apigateway_key", float64(keyTotal), "state", "total")
	_ = r.Gauge("apigateway_key", float64(keyEnabled), "state", "enabled")
	_ = r.Gauge("apigateway_key", float64(keyCooling), "state", "cooling")

	// ---- 冷却中的 RAPI 逐个打点（带 model/reason 标签，便于告警定位到具体模型）----
	aliasByID := make(map[int64]string, len(rt.RAPIs))
	for _, ra := range rt.RAPIs {
		aliasByID[ra.ID] = ra.Alias
	}
	for _, rs := range rt.Snap.RAPIs {
		if !rs.Cooling {
			continue
		}
		alias := aliasByID[rs.ID]
		if alias == "" {
			// 快照里有、但 store 里查不到（RAPI 刚被删或处于同步中间态）。
			// 仍然暴露这条样本：告警关心的是"有东西冷却了"，不能因为名字
			// 暂时取不到就静默丢掉，否则故障最需要信号的时候恰好没信号。
			alias = "unknown"
		}
		_ = r.Gauge("apigateway_rapi_cooling", 1, "model", alias, "reason", truncate(rs.Reason, 120))
	}

	// ---- 排队深度（请求等待可用 RAPI 的堆积量）----
	for _, q := range rt.Snap.Queues {
		_ = r.Gauge("apigateway_queue_depth", float64(q.Count), "lapi_id", itoa64(q.LapiID))
	}

	// ---- 流量累计量（counter，来自 request_logs，重启不归零）----
	_ = r.Counter("apigateway_requests_total", float64(rt.Traffic.TotalRequests))
	_ = r.Counter("apigateway_requests_total", float64(rt.Traffic.SuccessRequests), "result", "success")
	_ = r.Counter("apigateway_requests_total", float64(rt.Traffic.FailedRequests), "result", "failed")
	_ = r.Counter("apigateway_tokens_total", float64(rt.Traffic.InputTokens), "kind", "input")
	_ = r.Counter("apigateway_tokens_total", float64(rt.Traffic.OutputTokens), "kind", "output")
	_ = r.Counter("apigateway_tokens_total", float64(rt.Traffic.CachedTokens), "kind", "cached")
	_ = r.Counter("apigateway_retries_total", float64(rt.Traffic.RetryTotal))
	_ = r.Counter("apigateway_fallbacks_total", float64(rt.Traffic.FallbackTotal))

	return r.Gather()
}

// itoa / itoa64 是本包内的 strconv 薄封装。指标 label 的值必须是字符串，
// 而端口/计数在 Runtime 里是 int —— 这里统一转换，避免每个调用点散落
// strconv.Itoa 让 collect 主体被噪音淹没。
func itoa(n int) string     { return strconv.Itoa(n) }
func itoa64(n int64) string { return strconv.FormatInt(n, 10) }

// truncate 按 rune 截断（不是按字节）—— 按字节切会把多字节中文切成半个
// 字符，产出的 label 是乱码。上限取 120：Prometheus 对 label 值长度没有
// 硬限制，但 reason 可能是整段错误响应，无限增长会让 /metrics 变成一个
// 数 MB 的响应，而抓取超时比丢一点 reason 尾部更糟。
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	count := 0
	for i := range s {
		if count == n {
			return s[:i]
		}
		count++
	}
	return s
}
