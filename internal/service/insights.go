package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"gateway/internal/db"
	"gateway/internal/models"
	"gateway/internal/scheduler"
	"gateway/internal/store"
)

// ============ Insight Response Types ============

type InsightResponse struct {
	GeneratedAt time.Time  `json:"generated_at"`
	Health      HealthData `json:"health"`
	Efficiency  []Insight  `json:"efficiency"`
	Capacity    []Insight  `json:"capacity"`
}

type HealthData struct {
	CoolingRAPIs []CoolingRAPI `json:"cooling_rapis"`
	CoolingKeys  []CoolingKey  `json:"cooling_keys"`
	ErrorSummary []ErrorRow    `json:"error_summary"`
}

type CoolingRAPI struct {
	ID                  int64     `json:"id"`
	Alias               string    `json:"alias"`
	Reason              string    `json:"reason"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	RecoverAt           time.Time `json:"recover_at"`
	LastSuccess         time.Time `json:"last_success"`
	LastFailure         time.Time `json:"last_failure"`
	Invalidated         bool      `json:"invalidated"`
	// Platform context for hierarchical alert display / click-through navigation.
	PlatformID      int64  `json:"platform_id"`
	PlatformName    string `json:"platform_name"`
	BillingAddress  string `json:"billing_address,omitempty"`
	KeyIDs          string `json:"key_ids,omitempty"` // RAPI's key pool whitelist (empty = all platform keys)
}

type CoolingKey struct {
	ID        int64     `json:"id"`
	Reason    string    `json:"reason"`
	RecoverAt time.Time `json:"recover_at"`
	// Platform context for hierarchical alert display / click-through navigation.
	PlatformID     int64  `json:"platform_id"`
	PlatformName   string `json:"platform_name"`
	Label          string `json:"label,omitempty"`
	KeyIndex       int    `json:"key_index"`
	BillingAddress string `json:"billing_address,omitempty"`
}

type ErrorRow struct {
	RAPIID    int64   `json:"rapi_id"`
	Alias     string  `json:"alias"`
	Fail401   int     `json:"fail_401"`
	Fail429   int     `json:"fail_429"`
	Fail500   int     `json:"fail_500"`
	FailOther int     `json:"fail_other"`
	Total     int     `json:"total"`
	Rate      float64 `json:"error_rate"`
}

type Insight struct {
	Level    string                 `json:"level"`
	Category string                 `json:"category"`
	Title    string                 `json:"title"`
	Detail   string                 `json:"detail"`
	Context  map[string]interface{} `json:"context,omitempty"`
}

// rapiCfg is a lightweight config snapshot for one RAPI, used throughout insights.
type rapiCfg struct {
	Alias      string
	Model      string
	BaseCost   int
	HighCost   int
	RPMLimit   int
	RPHLimit   int
	RPDLimit   int
	TPMLimit   int
	TPHLimit   int
	TPDLimit   int
	Enabled    bool
	Available  bool
	PlatformID int64
	KeyIDs     string
}

// ============ Insight Generation ============

func generateInsights() InsightResponse {
	snap := proxyGateway.Scheduler().Snapshot()
	rapiStats, _ := db.Get().GetRAPIStats()
	rapis, _ := store.A().GetRAPIs()
	lapis, _ := store.A().GetLAPIs()
	platforms, _ := store.A().GetPlatforms()
	allKeys, _ := store.A().GetAllPlatformKeys()
	orders, _ := db.Get().GetAllLAPIRAPIOrders()
	fallbackStats, _ := db.Get().GetFallbackStats(24)
	hourlyDist, _ := db.Get().GetHourlyDistribution(7)

	// Build lookups
	rapiStatByID := make(map[int64]db.RAPIStat)
	for _, s := range rapiStats {
		rapiStatByID[s.RapiID] = s
	}
	rapiCfgByID := make(map[int64]rapiCfg)
	for _, r := range rapis {
		rapiCfgByID[r.ID] = rapiCfg{
			Alias: r.Alias, Model: r.Model, BaseCost: r.BaseCost, HighCost: r.HighCost,
			RPMLimit: r.RPMLimit, RPHLimit: r.RPHLimit, RPDLimit: r.RPDLimit,
			TPMLimit: r.TPMLimit, TPHLimit: r.TPHLimit, TPDLimit: r.TPDLimit,
			Enabled: r.Enabled, Available: r.Available, PlatformID: r.PlatformID, KeyIDs: r.KeyIDs,
		}
	}
	platformByID := make(map[int64]models.Platform)
	for _, p := range platforms {
		platformByID[p.ID] = p
	}
	keyByID := make(map[int64]models.PlatformKey)
	for _, k := range allKeys {
		keyByID[k.ID] = k
	}
	counterByID := make(map[int64]scheduler.CounterSnapshot)
	for _, c := range snap.Counters {
		counterByID[c.RapiID] = c
	}
	lapiAliasByID := make(map[int64]string)
	for _, l := range lapis {
		lapiAliasByID[l.ID] = l.Alias
	}

	// Build lapi → ordered rapi list
	lapiRAPIMap := make(map[int64][]int64)
	for _, o := range orders {
		lapiRAPIMap[o.LapiID] = append(lapiRAPIMap[o.LapiID], o.RAPIID)
	}

	health := buildHealth(snap, rapiStatByID, rapiCfgByID, platformByID, keyByID)
	efficiency := buildEfficiency(lapiRAPIMap, lapiAliasByID, rapiCfgByID, rapiStatByID, counterByID, fallbackStats)
	capacity := buildCapacity(counterByID, rapiCfgByID, rapiStatByID, snap, hourlyDist)

	return InsightResponse{
		GeneratedAt: time.Now(),
		Health:      health,
		Efficiency:  efficiency,
		Capacity:    capacity,
	}
}

func buildHealth(snap scheduler.Snapshot, rapiStatByID map[int64]db.RAPIStat, rapiCfgByID map[int64]rapiCfg,
	platformByID map[int64]models.Platform, keyByID map[int64]models.PlatformKey) HealthData {
	h := HealthData{
		CoolingRAPIs: make([]CoolingRAPI, 0),
		CoolingKeys:  make([]CoolingKey, 0),
		ErrorSummary: make([]ErrorRow, 0),
	}

	platformInfo := func(pid int64) (name, billing string) {
		if p, ok := platformByID[pid]; ok {
			return p.Name, p.BillingAddress
		}
		return "", ""
	}

	for _, rs := range snap.RAPIs {
		if !rs.Cooling && !rs.Invalidated {
			continue
		}
		alias, pid, keyIDs := "", int64(0), ""
		if cfg, ok := rapiCfgByID[rs.ID]; ok {
			alias, pid, keyIDs = cfg.Alias, cfg.PlatformID, cfg.KeyIDs
		}
		pname, billing := platformInfo(pid)
		h.CoolingRAPIs = append(h.CoolingRAPIs, CoolingRAPI{
			ID: rs.ID, Alias: alias, Reason: rs.Reason,
			ConsecutiveFailures: rs.ConsecutiveFailures,
			RecoverAt:           rs.RecoverAt, LastSuccess: rs.LastSuccess,
			LastFailure: rs.LastFailure, Invalidated: rs.Invalidated,
			PlatformID: pid, PlatformName: pname, BillingAddress: billing, KeyIDs: keyIDs,
		})
	}

	for _, ks := range snap.Keys {
		if !ks.Cooling {
			continue
		}
		key, ok := keyByID[ks.ID]
		ck := CoolingKey{
			ID: ks.ID, Reason: ks.Reason, RecoverAt: ks.RecoverAt,
		}
		if ok {
			ck.PlatformID = key.PlatformID
			ck.Label = key.Label
			ck.KeyIndex = key.KeyIndex
			ck.PlatformName, ck.BillingAddress = platformInfo(key.PlatformID)
		}
		h.CoolingKeys = append(h.CoolingKeys, ck)
	}

	for _, s := range rapiStatByID {
		totalErrors := s.Fail401 + s.Fail429 + s.Fail500 + s.FailOther
		if totalErrors == 0 {
			continue
		}
		errRate := 0.0
		if s.TotalRequests > 0 {
			errRate = float64(totalErrors) / float64(s.TotalRequests) * 100
		}
		h.ErrorSummary = append(h.ErrorSummary, ErrorRow{
			RAPIID: s.RapiID, Alias: s.Alias,
			Fail401: s.Fail401, Fail429: s.Fail429, Fail500: s.Fail500, FailOther: s.FailOther,
			Total: s.TotalRequests, Rate: errRate,
		})
	}
	sort.Slice(h.ErrorSummary, func(i, j int) bool {
		ei := h.ErrorSummary[i].Fail401 + h.ErrorSummary[i].Fail429 + h.ErrorSummary[i].Fail500 + h.ErrorSummary[i].FailOther
		ej := h.ErrorSummary[j].Fail401 + h.ErrorSummary[j].Fail429 + h.ErrorSummary[j].Fail500 + h.ErrorSummary[j].FailOther
		return ei > ej
	})

	return h
}

func buildEfficiency(
	lapiRAPIMap map[int64][]int64,
	lapiAliasByID map[int64]string,
	rapiCfgByID map[int64]rapiCfg,
	rapiStatByID map[int64]db.RAPIStat,
	counterByID map[int64]scheduler.CounterSnapshot,
	fallbackStats []db.FallbackStats,
) []Insight {
	insights := make([]Insight, 0)

	// Check 1: Route chain load imbalance
	for lapiID, rapiIDs := range lapiRAPIMap {
		if len(rapiIDs) < 2 {
			continue
		}
		primaryID := rapiIDs[0]
		primaryCfg := rapiCfgByID[primaryID]
		primaryCounter := counterByID[primaryID]

		if primaryCfg.RPHLimit <= 0 && primaryCfg.RPDLimit <= 0 {
			continue
		}

		primaryHourPct := 0.0
		if primaryCfg.RPHLimit > 0 {
			primaryHourPct = float64(primaryCounter.HourReq) / float64(primaryCfg.RPHLimit) * 100
		}
		if primaryHourPct < 70 {
			continue
		}

		for _, altID := range rapiIDs[1:] {
			altCfg := rapiCfgByID[altID]
			if !altCfg.Enabled || !altCfg.Available {
				continue
			}
			altCounter := counterByID[altID]
			altHourPct := 0.0
			if altCfg.RPHLimit > 0 {
				altHourPct = float64(altCounter.HourReq) / float64(altCfg.RPHLimit) * 100
			}
			if altHourPct >= 50 {
				continue
			}

			primaryStat := rapiStatByID[primaryID]
			altStat := rapiStatByID[altID]
			primaryErrRate := errRate(primaryStat)
			altErrRate := errRate(altStat)

			level := "info"
			if primaryCfg.BaseCost > altCfg.BaseCost || altErrRate < primaryErrRate {
				level = "warning"
			}

			lapiAlias := lapiAliasByID[lapiID]
			insights = append(insights, Insight{
				Level:    level,
				Category: "route_imbalance",
				Title:    fmt.Sprintf("路由链负载不均: %s", lapiAlias),
				Detail: fmt.Sprintf(
					"%s 当前小时已用 %.0f%% RPH 限额 (%d/%d)，而 %s 仅用 %.0f%% (%d/%d)。"+
						"考虑将 %s 提前到路由链首位以分散负载。",
					primaryCfg.Alias, primaryHourPct, primaryCounter.HourReq, primaryCfg.RPHLimit,
					altCfg.Alias, altHourPct, altCounter.HourReq, altCfg.RPHLimit,
					altCfg.Alias),
				Context: map[string]interface{}{
					"lapi_id": lapiID, "lapi_alias": lapiAlias,
					"primary_rapi": primaryCfg.Alias, "primary_hour_pct": primaryHourPct,
					"primary_used": primaryCounter.HourReq, "primary_limit": primaryCfg.RPHLimit,
					"alt_rapi": altCfg.Alias, "alt_hour_pct": altHourPct,
					"alt_used": altCounter.HourReq, "alt_limit": altCfg.RPHLimit,
					"primary_cost": primaryCfg.BaseCost, "alt_cost": altCfg.BaseCost,
					"primary_err_rate": primaryErrRate, "alt_err_rate": altErrRate,
				},
			})
			break
		}
	}

	// Check 2: High fallback rate
	for _, fs := range fallbackStats {
		if fs.FallbackRate > 20 && fs.TotalRequests > 10 {
			insights = append(insights, Insight{
				Level:    "warning",
				Category: "high_fallback",
				Title:    fmt.Sprintf("高 Fallback 率: %s", fs.LapiAlias),
				Detail: fmt.Sprintf(
					"%s 在过去 24 小时内有 %.1f%% 的请求使用了备用路径 (%d/%d 次)。"+
						"主路径可能不稳定，建议检查主路径模型的健康状态。",
					fs.LapiAlias, fs.FallbackRate, fs.FallbackCount, fs.TotalRequests),
				Context: map[string]interface{}{
					"lapi_alias": fs.LapiAlias, "fallback_rate": fs.FallbackRate,
					"fallback_count": fs.FallbackCount, "total_requests": fs.TotalRequests,
				},
			})
		}
	}

	// Check 3: Single-point LAPIs
	for lapiID, rapiIDs := range lapiRAPIMap {
		enabledCount := 0
		for _, rid := range rapiIDs {
			cfg := rapiCfgByID[rid]
			if cfg.Enabled && cfg.Available {
				enabledCount++
			}
		}
		if enabledCount <= 1 {
			lapiAlias := lapiAliasByID[lapiID]
			insights = append(insights, Insight{
				Level:    "info",
				Category: "single_point",
				Title:    fmt.Sprintf("单点故障: %s", lapiAlias),
				Detail: fmt.Sprintf(
					"%s 仅有 %d 个可用模型，没有容灾路径。建议添加备用模型到路由链。",
					lapiAlias, enabledCount),
				Context: map[string]interface{}{
					"lapi_id": lapiID, "lapi_alias": lapiAlias,
					"rapi_count": len(rapiIDs), "enabled_count": enabledCount,
				},
			})
		}
	}

	return insights
}

func buildCapacity(
	counterByID map[int64]scheduler.CounterSnapshot,
	rapiCfgByID map[int64]rapiCfg,
	rapiStatByID map[int64]db.RAPIStat,
	snap scheduler.Snapshot,
	hourlyDist []db.HourBucket,
) []Insight {
	insights := make([]Insight, 0)

	for rapiID, cfg := range rapiCfgByID {
		if !cfg.Enabled || !cfg.Available {
			continue
		}
		counter := counterByID[rapiID]

		type dimCheck struct {
			name    string
			used    int
			limit   int
			isToken bool
		}
		checks := []dimCheck{
			{"RPM", counter.MinuteReq, cfg.RPMLimit, false},
			{"RPH", counter.HourReq, cfg.RPHLimit, false},
			{"RPD", counter.DayReq, cfg.RPDLimit, false},
			{"TPM", counter.MinuteTok, cfg.TPMLimit, true},
			{"TPH", counter.HourTok, cfg.TPHLimit, true},
			{"TPD", counter.DayTok, cfg.TPDLimit, true},
		}

		for _, ck := range checks {
			if ck.limit <= 0 || ck.used <= 0 {
				continue
			}
			pct := float64(ck.used) / float64(ck.limit) * 100
			if pct < 80 {
				continue
			}

			level := "warning"
			if pct >= 95 {
				level = "critical"
			}

			stat := rapiStatByID[rapiID]
			suppressed := stat.Fail429

			detail := fmt.Sprintf(
				"%s 当前 %s 已用 %d/%d (%.0f%%)。",
				cfg.Alias, ck.name, ck.used, ck.limit, pct)
			if suppressed > 0 {
				detail += fmt.Sprintf(" 累计被限流 %d 次，实际需求量超过当前限额。", suppressed)
			}

			insights = append(insights, Insight{
				Level:    level,
				Category: "limit_pressure",
				Title:    fmt.Sprintf("%s 压力: %s", ck.name, cfg.Alias),
				Detail:   detail,
				Context: map[string]interface{}{
					"rapi_id": rapiID, "rapi_alias": cfg.Alias,
					"dimension": ck.name, "used": ck.used, "limit": ck.limit,
					"pct": pct, "suppressed": suppressed, "is_token": ck.isToken,
				},
			})
		}
	}

	for _, q := range snap.Queues {
		if q.Count > 0 {
			insights = append(insights, Insight{
				Level:    "critical",
				Category: "queue_pressure",
				Title:    "请求排队等待",
				Detail: fmt.Sprintf(
					"当前有 %d 个请求在等待模型恢复。所有可用后端可能处于冷却中。",
					q.Count),
				Context: map[string]interface{}{
					"lapi_id": q.LapiID, "queued": q.Count,
				},
			})
		}
	}

	return insights
}

// ============ API Handler ============

func handleInsights(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	resp := generateInsights()
	data, _ := json.Marshal(resp)
	w.Write(data)
}

// ============ Helpers ============

func errRate(s db.RAPIStat) float64 {
	if s.TotalRequests <= 0 {
		return 0
	}
	return float64(s.Fail401+s.Fail429+s.Fail500+s.FailOther) / float64(s.TotalRequests) * 100
}
