package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"gateway/internal/bundle"
	"gateway/internal/config"
	"gateway/internal/crypto"
	"gateway/internal/db"
	"gateway/internal/logger"
)

// 代理端中心同步（读路径）。设计 §5：
// 启动拉 1 次 + 定时轮询中心 version（短路不变） + 控制台手动刷新；
// 任何失败沿用本地 last_good 继续转发（fail-open），绝不中断业务。
//
// service 层沿用包级单例的既有模式（见 dev-guide §六）。
var (
	syncClient     *bundle.Client
	syncCenterKey  []byte
	syncSourceURL  string
	syncConfigured bool
	manageMode     bool // 管理模式：定义类写直写中心，只读守卫放行
)

// startSyncLoop 初始化并启动中心同步。cfg.Sync 未配置（无 source_url）时直接返回，
// 网关以纯本地模式运行；center_key 配错也只禁用同步，不影响启动（fail-open）。
func startSyncLoop(stopCh <-chan struct{}, syncCfg config.Sync) {
	if !syncCfg.Enabled() {
		logger.DefaultConsole().Info("service", "[SYNC] disabled (no [sync] source_url), running standalone")
		return
	}
	// center_key 可选：留空 = 中心存明文 token（手动录入里程碑，或 Phase 4 前
	// 手动在 Supabase 录入），代理 reencrypt 会把无 enc: 前缀的值当明文重新本地
	// 加密。填了 32 字节 hex = 中心存 center_key 密文（Phase 4 管理端自动加密）。
	var centerKey []byte
	if strings.TrimSpace(syncCfg.CenterKey) != "" {
		k, err := crypto.ParseKey(syncCfg.CenterKey)
		if err != nil {
			logger.DefaultConsole().Error("service", "[SYNC] invalid center_key (want 32-byte hex), sync disabled",
				"error", err.Error())
			return
		}
		centerKey = k
	}
	interval := time.Duration(syncCfg.PollIntervalSec) * time.Second
	if interval <= 0 {
		interval = 60 * time.Second
	}

	syncClient = bundle.NewClient(syncCfg.SourceURL, syncCfg.VersionURL, syncCfg.AnonKey)
	syncCenterKey = centerKey
	syncSourceURL = syncCfg.SourceURL
	syncConfigured = true

	logger.DefaultConsole().Info("service", "[SYNC] enabled",
		"source", syncCfg.SourceURL, "poll_interval_sec", int(interval.Seconds()))

	go func() {
		// 启动主动拉 1 次（设计 §5.1）。失败不打断启动：沿用本地缓存继续服务。
		if err := syncOnce(); err != nil {
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
				if err := syncOnce(); err != nil {
					logger.DefaultConsole().Warn("service", "[SYNC] poll failed, keeping last-good config",
						"error", err.Error())
				}
			}
		}
	}()
}

// syncOnce 轮询中心版本号；变了才拉全量并单事务应用（version 短路，不拉全量）。
func syncOnce() error {
	if !syncConfigured {
		return errors.New("sync not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	remote, err := syncClient.PullVersion(ctx)
	if err != nil {
		return err
	}
	st, err := db.Get().GetSyncState()
	if err != nil {
		return err
	}
	if remote == st.LastGoodVersion {
		return nil // 版本没变，短路
	}
	env, err := syncClient.PullBundle(ctx, st.LastGoodVersion)
	if errors.Is(err, bundle.ErrNoChange) {
		return nil // 轮询间隙版本又追平（拉取时已一致）
	}
	if err != nil {
		return err
	}
	if err := db.Get().ApplyBundle(env, syncCenterKey, syncSourceURL); err != nil {
		return err
	}
	logger.DefaultConsole().Info("service", "[SYNC] applied new config",
		"version", env.Version,
		"platforms", len(env.Bundle.Platforms),
		"keys", len(env.Bundle.PlatformKeys),
		"rapis", len(env.Bundle.RAPIs),
		"lapis", len(env.Bundle.LAPIs))
	return nil
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled":           syncConfigured,
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
	if !syncConfigured {
		writeJSONError(w, http.StatusForbidden, errors.New("sync not configured (no [sync] source_url)"))
		return
	}
	if err := syncOnce(); err != nil {
		writeJSONError(w, 500, err)
		return
	}
	handleSyncState(w, r) // 成功后返回最新状态
}

// withProxyReadOnlyGuard 在启用了中心同步（代理角色）时拦截定义类写操作。
// 只放行本地健康/探测类变更；定义类 CRUD 由管理端负责。设计 §3.3 / §6。
// 管理模式（manageMode）定义类写放行——那正是管理端的职责。
// 未配置同步（独立模式）时全放行，无回归。
func withProxyReadOnlyGuard(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !syncConfigured || manageMode || !isMutating(r.Method) || proxyMutationAllowed(r.URL.Path, r.Method) {
			h.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"message": "定义类配置由中心管理，代理端只读。请在管理端修改。",
				"type":    "readonly_proxy",
				"code":    "definitions_center_authoritative",
			},
		})
	})
}

func isMutating(m string) bool {
	switch m {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// proxyMutationAllowed 放行本地健康/探测动作（不落定义表的写）：
//   - sync 手动刷新、retry-unhealthy、platform/rapi 恢复探测、key×model 直测、
//     upstream 向导探测、key 探测(/probe 后缀)、key_model_blocks 清除(DELETE)
func proxyMutationAllowed(path, method string) bool {
	switch {
	case path == "/api/sync/refresh",
		path == "/api/system/retry-unhealthy",
		path == "/api/platforms/restore",
		path == "/api/rapis/restore",
		path == "/api/rapis/test",
		path == "/api/upstream/test",
		strings.HasSuffix(path, "/probe"):
		return true
	case path == "/api/key-model-blocks" && method == http.MethodDelete:
		return true
	}
	return false
}
