package service

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"gateway/internal/apiformat"
	"gateway/internal/config"
	"gateway/internal/db"
	"gateway/internal/gateway"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/notify"
	"gateway/internal/scheduler"
	"gateway/internal/store"
	"gateway/internal/supabase"
)

// ============ Security / Input Validation ============

// reUnsafeName rejects anything other than printable non-control ASCII and common Unicode.
// Bans shell metacharacters and SQL special sequences in the platform name field.
var reUnsafeName = regexp.MustCompile(`[<>"'` + "`" + `;|&$\\{}()*?\x00-\x1f]`)

// validatePlatformInput sanitizes and validates all user-supplied fields of a Platform.
// Returns a descriptive error if any field fails validation.
func validatePlatformInput(p *models.Platform) error {
	// --- Name ---
	p.Name = strings.TrimSpace(p.Name)
	if p.Name == "" {
		return errors.New("平台名称不能为空")
	}
	if utf8.RuneCountInString(p.Name) > 100 {
		return errors.New("平台名称不能超过100个字符")
	}
	if reUnsafeName.MatchString(p.Name) {
		return errors.New("平台名称包含非法字符")
	}

	// --- Notes ---
	p.Notes = strings.TrimSpace(p.Notes)
	if utf8.RuneCountInString(p.Notes) > 500 {
		return errors.New("备注描述不能超过500个字符")
	}

	// --- Base URL ---
	p.BaseURL = strings.TrimSpace(p.BaseURL)
	if p.BaseURL == "" {
		return errors.New("Base URL 不能为空")
	}
	if len(p.BaseURL) > 512 {
		return errors.New("Base URL 长度不能超过512字符")
	}
	parsed, err := url.ParseRequestURI(p.BaseURL)
	if err != nil {
		return errors.New("Base URL 格式不合法")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("Base URL 必须以 http:// 或 https:// 开头")
	}
	// Reject URLs with embedded credentials (SSRF mitigation).
	if parsed.User != nil {
		return errors.New("Base URL 不允许包含用户名/密码信息")
	}

	// --- Billing address (provider console URL, optional) ---
	p.BillingAddress = strings.TrimSpace(p.BillingAddress)
	if len(p.BillingAddress) > 512 {
		return errors.New("账单地址长度不能超过512字符")
	}
	if p.BillingAddress != "" {
		billParsed, err := url.ParseRequestURI(p.BillingAddress)
		if err != nil {
			return errors.New("账单地址格式不合法")
		}
		if billParsed.Scheme != "http" && billParsed.Scheme != "https" {
			return errors.New("账单地址必须以 http:// 或 https:// 开头")
		}
		if billParsed.User != nil {
			return errors.New("账单地址不允许包含用户名/密码信息")
		}
	}

	// --- Login account (provider console login, optional) ---
	p.LoginAccount = strings.TrimSpace(p.LoginAccount)
	if utf8.RuneCountInString(p.LoginAccount) > 200 {
		return errors.New("登录账号不能超过200个字符")
	}

	// --- Login password (provider console password, optional; kept as-is so
	// an empty value means "unchanged" in UpdatePlatform) ---
	p.LoginPassword = strings.TrimSpace(p.LoginPassword)
	if utf8.RuneCountInString(p.LoginPassword) > 512 {
		return errors.New("登录密码不能超过512个字符")
	}

	return nil
}

// recordUnread appends an in-memory unread event routed to a UI menu.
// recordUnread 记一条未读。**仅用于系统事件与用户操作的连带效应**（冷却/
// 失败/恢复/级联影响）；用户自己在面板上做的增删改不记未读——操作者刚做完
// 的事不需要再提醒他看一遍。
func recordUnread(menu, kind string, entityID int64, title, detail string) {
	if notifySvc != nil {
		notifySvc.RecordUnread(menu, kind, title, detail, entityID)
	}
}

// platformName returns a platform's display name, falling back to its id.
func platformName(pid int64) string {
	if p, err := store.A().GetPlatformByID(pid); err == nil && p != nil {
		return p.Name
	}
	return fmt.Sprintf("%d", pid)
}

// parseKeyIDs parses a comma-separated RAPI key_ids whitelist into a slice.
func parseKeyIDs(s string) []int64 {
	parts := strings.Split(s, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var id int64
		fmt.Sscanf(p, "%d", &id)
		if id > 0 {
			out = append(out, id)
		}
	}
	return out
}

// intIn reports whether id is present in ids.
func intIn(ids []int64, id int64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

type Service struct {
	stopCh    chan struct{}
	doneCh    chan struct{}
	isRunning bool
	mu        sync.Mutex
}

var (
	instance       *Service
	notifySvc      *notify.NotificationService
	proxyGateway   *gateway.ProxyGateway
	httpServer     *http.Server
	webServer      *http.Server
	logInstance    *logger.Logger
	sessionTracker *logger.SessionTracker
)

func New() *Service {
	if instance == nil {
		instance = &Service{
			stopCh: make(chan struct{}),
			doneCh: make(chan struct{}),
		}
	}
	return instance
}

func (s *Service) Run() error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return nil
	}
	s.isRunning = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.isRunning = false
		s.mu.Unlock()
	}()

	exePath, err := os.Executable()
	if err == nil {
		db.SetExePath(exePath)
	}

	if err := db.Init(); err != nil {
		logger.DefaultConsole().Error("service", "database init failed", "error", err.Error())
		return err
	}
	// 定义类持久层接缝：默认绑定本地 SQLite；管理模式（[management] 配置）
	// 在后面换成 supabase Store 直写中心。
	store.Init(db.Get())
	// 冻结启动时本地定义表行数，供仪表盘「启动以来变化量」对比。
	captureStartupSnapshot()

	cfg, err := config.Load()
	if err != nil {
		// DefaultConsole() lazily sets up a stderr-only structured logger so even
		// the pre-config-load failure lands as JSON. We keep returning the error
		// to the caller for its existing shutdown behaviour.
		logger.DefaultConsole().Error("service", "config load failed", "error", err.Error())
		return err
	}

	// Initialise the process-wide structured console logger as early as possible
	// so every subsequent line (DB init, scheduler, gateway runtime) lands as
	// JSON Lines with consistent component/level fields. Until this call, early
	// loggers fall back to the stderr-only default via DefaultConsole().
	// Structured console logger settings may be overridden from the dashboard.
	// The settings table is the runtime source of truth; proxy.cfg remains the
	// initial fallback for headless/local startup.
	if saved := savedLogLevel(cfg.LogLevel); saved != "" {
		cfg.LogLevel = saved
	}
	logger.InitConsoleLogger(logger.ConsoleOptions{
		Level:      logger.ParseLevel(cfg.LogLevel),
		EnableFile: cfg.LogFile,
		FilePath:   cfg.LogFilePath,
	})

	notifySvc = notify.NewNotificationService()

	logConfig := logger.DefaultLogConfig()
	logStorage := logger.NewLogStorage(db.Get())
	if err := logStorage.InitTables(); err != nil {
		logger.DefaultConsole().Error("service", "log storage init failed", "error", err.Error())
	}
	logInstance = logger.NewLogger(logStorage, logConfig)
	logInstance.Start()

	sessionTracker = logger.NewSessionTracker(logInstance)

	schedulerCfg := scheduler.ConfigFromAppConfig(cfg.CooldownSec, cfg.MaxCooldownSec, cfg.RequestMaxWaitSec, cfg.BillingCooldownSec, cfg.CapabilityBlockSec, cfg.KeyCursorScope)
	proxyGateway = gateway.NewProxyGatewayWithConfig(notifySvc, logInstance, sessionTracker, cfg.DialTimeoutSec, cfg.ResponseTimeoutSec, schedulerCfg)

	proxyAddr := fmt.Sprintf("0.0.0.0:%d", cfg.ProxyPort)
	proxyMux := http.NewServeMux()
	proxyMux.HandleFunc("/v1/chat/completions", proxyGateway.HandleChatCompletions)
	proxyMux.HandleFunc("/v1/messages", proxyGateway.HandleChatCompletions)
	proxyMux.HandleFunc("/v1beta/", proxyGateway.HandleChatCompletions)
	proxyMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	proxyMux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		// Bug 8.9: only expose enabled LAPIs.
		w.Header().Set("Content-Type", "application/json")
		lapis, _ := store.A().GetLAPIs()
		type modelEntry struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			Created int64  `json:"created"`
			OwnedBy string `json:"owned_by"`
		}
		data := make([]modelEntry, 0, len(lapis))
		for _, l := range lapis {
			if !l.Enabled {
				continue
			}
			data = append(data, modelEntry{
				ID:      l.Alias,
				Object:  "model",
				Created: l.CreatedAt.Unix(),
				OwnedBy: "api-gateway",
			})
		}
		resp := map[string]interface{}{"object": "list", "data": data}
		json.NewEncoder(w).Encode(resp)
	})
	// Catch-all: return JSON 404 for unregistered paths instead of Go's default plain text 404
	proxyMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		logger.DefaultConsole().Info("service", "proxy: unhandled endpoint",
			"method", r.Method, "path", r.URL.Path, "from", r.RemoteAddr)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"message": fmt.Sprintf("Unknown endpoint: %s %s", r.Method, r.URL.Path),
				"type":    "invalid_request",
				"code":    "unknown_endpoint",
			},
		})
	})
	proxyMux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		rapis, _ := store.A().GetRAPIs()
		lapis, _ := store.A().GetLAPIs()
		stats, _ := db.Get().GetRAPIStats()
		totalReq := 0
		for _, s := range stats {
			totalReq += s.TotalRequests
		}
		fmt.Fprintf(w, `{"proxy":"running","proxy_port":%d,"web_port":%d,"rapi_count":%d,"lapi_count":%d,"total_requests":%d}`, cfg.ProxyPort, cfg.WebPort, len(rapis), len(lapis), totalReq)
	})
	httpServer = &http.Server{
		Addr:         proxyAddr,
		Handler:      proxyMux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
		ConnState:    sessionTracker.OnConnState,
		// 会话 id 入 context 的唯一通道（见 logger.ConnContext 注释）。
		// 缺了它 InjectSessionID 永远 stable=false，会话级 key 轮询不生效。
		ConnContext: sessionTracker.ConnContext,
	}

	// 面板监听地址。默认 127.0.0.1（只给本机，暴露面最小）。
	//
	// 容器里必须绑 0.0.0.0：Docker 的端口映射是转发到容器网卡的，映射到容器内
	// 的 127.0.0.1 上，宿主机/局域网都访问不到面板 —— 症状是"代理 13579 正常
	// 但面板打不开"。故检测到容器（/.dockerenv 或 cgroup 提到 docker）时改绑
	// 全网卡。APIGATEWAY_WEB_HOST 可显式覆盖（想让容器内面板只走回环时用）。
	webHost := strings.TrimSpace(os.Getenv("APIGATEWAY_WEB_HOST"))
	if webHost == "" {
		webHost = "127.0.0.1"
		if isContainerEnv() {
			webHost = "0.0.0.0"
			logger.DefaultConsole().Info("service",
				"[STARTUP] container detected, dashboard binds 0.0.0.0 (override with APIGATEWAY_WEB_HOST)")
		}
	}
	webAddr := fmt.Sprintf("%s:%d", webHost, cfg.WebPort)
	webServer = &http.Server{
		Addr:        webAddr,
		Handler:     createWebHandler(),
		ReadTimeout: 30 * time.Second,
		IdleTimeout: 120 * time.Second,
	}

	errCh := make(chan error, 2)

	go func() {
		logger.DefaultConsole().Info("service", "proxy server starting", "addr", proxyAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			if strings.Contains(err.Error(), "address already in use") {
				errCh <- fmt.Errorf("Proxy端口 %s 被占用，请先停止占用该端口的程序，或修改配置文件中的proxy_port", proxyAddr)
			} else {
				errCh <- fmt.Errorf("proxy server error: %v", err)
			}
		}
	}()

	go func() {
		logger.DefaultConsole().Info("service", "web dashboard starting", "addr", webAddr)
		if err := webServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			if strings.Contains(err.Error(), "address already in use") {
				logger.DefaultConsole().Warn("service", "Web端口被占用，Web管理界面不可用，但Proxy服务仍可正常运行", "addr", webAddr)
			} else {
				logger.DefaultConsole().Error("service", "Web服务启动失败", "error", err.Error())
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			db.Get().CleanupOldTrends(120)
		}
	}()

	// 中心配置同步（读路径）+ 管理模式（写路径）。
	// 转发机配 [sync]（只读拉取）；管理机配 [management]（定义类 CRUD 直写中心，
	// 写成功后立即拉回刷新本地镜像；拉取配置由 [management] 派生）。
	syncCfg := cfg.Sync
	if cfg.Management.Configured() {
		var mgmtMu sync.Mutex
		sup, err := supabase.New(supabase.Config{
			URL:        cfg.Management.SupabaseURL,
			ServiceKey: cfg.Management.ServiceKey,
			CenterKey:  cfg.Management.CenterKey,
		}, func() {
			mgmtMu.Lock()
			defer mgmtMu.Unlock()
			_ = syncOnce(true) // 写后立即拉回（强制 apply），管理机本地镜像即时一致
		})
		if err != nil {
			logger.DefaultConsole().Error("service", "[MGMT] invalid [management] config, falling back to local store",
				"error", err.Error())
		} else {
			store.Use(sup)
			manageMode.Store(true)
			base := strings.TrimSuffix(strings.TrimSpace(cfg.Management.SupabaseURL), "/")
			syncCfg = config.Sync{
				SourceURL:       base + "/rest/v1/rpc/get_bundle",
				VersionURL:      base + "/rest/v1/rpc/get_version",
				AnonKey:         cfg.Management.ServiceKey,
				CenterKey:       cfg.Management.CenterKey,
				PollIntervalSec: cfg.Sync.PollIntervalSec,
			}
			logger.DefaultConsole().Info("service", "[MGMT] management mode: definitions write through to center",
				"center", base)
			// 面板 sb-config 也存了一份（settings 表）。若与 proxy.cfg 不一致，
			// 启动以 proxy.cfg 为准，但 sb-config 页显示的是 settings 值 → 面板
			// 与实际生效连接不符。记系统日志让操作员能发现配置漂移。
			if sbURL, _, ok := loadSavedSBConfig(); ok {
				if normalizeSBURL(sbURL) != normalizeSBURL(cfg.Management.SupabaseURL) {
					msg := "[MGMT] 中心配置不一致：proxy.cfg [management] 生效（" + base +
						"），面板「中心配置」页显示的是另一份（" + normalizeSBURL(sbURL) + "）。以 proxy.cfg 为准。"
					logger.DefaultConsole().Warn("service", msg)
					_ = db.Get().InsertSystemLog("warn", "center", msg)
				}
			}
		}
	}
	// 回退：操作员若在仪表盘「中心配置」页录入了 Supabase URL+key（存 settings
	// 表）而非编辑 proxy.cfg [management]，则据此激活管理模式，使运行时 store、
	// 同步循环与仪表盘 sync-center 卡都与 sb-config 页一致（否则卡显示未连接）。
	// center_key 同样从 settings（sb_center_key）读，proxy.cfg 仅作兜底。
	if !manageMode.Load() {
		if sbURL, sbKey, ok := loadSavedSBConfig(); ok {
			centerKeyHex := effectiveCenterKey("")
			var mgmtMu sync.Mutex
			sup, err := supabase.New(supabase.Config{
				URL:        sbURL,
				ServiceKey: sbKey,
				CenterKey:  centerKeyHex,
			}, func() {
				mgmtMu.Lock()
				defer mgmtMu.Unlock()
				_ = syncOnce(true) // 写后立即拉回，本地镜像即时一致
			})
			if err != nil {
				logger.DefaultConsole().Error("service", "[MGMT] saved sb-config invalid, falling back to local store",
					"error", err.Error())
				_ = db.Get().InsertSystemLog("warn", "center",
					"管理模式启动激活失败（回退本地）: "+err.Error()+"——请在中心配置页检查后重新保存")
			} else {
				store.Use(sup)
				manageMode.Store(true)
				base := strings.TrimSuffix(strings.TrimSpace(sbURL), "/")
				syncCfg = config.Sync{
					SourceURL:       base + "/rest/v1/rpc/get_bundle",
					VersionURL:      base + "/rest/v1/rpc/get_version",
					AnonKey:         sbKey,
					CenterKey:       centerKeyHex,
					PollIntervalSec: cfg.Sync.PollIntervalSec,
				}
				logger.DefaultConsole().Info("service", "[MGMT] management mode from saved sb-config",
					"center", base)
				msg := "[MGMT] 管理模式由面板「中心配置」页（settings 表）激活：" + base +
					"。如需固定/迁移请写入 proxy.cfg [management]（启动优先读 cfg）。"
				logger.DefaultConsole().Info("service", msg)
				_ = db.Get().InsertSystemLog("info", "center", msg)
			}
		}
	}
	startSyncLoop(s.stopCh, syncCfg)

	// Startup health recovery: probe every RAPI persisted as unavailable and
	// restore the ones that respond. Runs async so it never blocks serving.
	if cfg.RetryOnStartup {
		go func() {
			logger.DefaultConsole().Info("service", "[STARTUP] retrying unhealthy models",
				"concurrency", cfg.RetryConcurrency, "timeout_sec", cfg.RetryTimeoutSec)
			rep := proxyGateway.RecoverUnhealthyRAPIs(context.Background(), cfg.RetryConcurrency, cfg.RetryTimeoutSec)
			logger.DefaultConsole().Info("service", "[STARTUP] retry done",
				"probed", rep.Probed, "recovered", len(rep.Recovered), "failed", len(rep.Failed))
		}()
	}

	logger.DefaultConsole().Info("service", "Gateway service started successfully")

	select {
	case <-s.stopCh:
	case err := <-errCh:
		return err
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	httpServer.Shutdown(shutdownCtx)
	webServer.Shutdown(shutdownCtx)

	// Flush in-flight request logs so the last requests before shutdown are not lost.
	if logInstance != nil {
		logInstance.Stop()
	}

	close(s.doneCh)
	return nil
}

func (s *Service) Stop() error {
	s.mu.Lock()
	if !s.isRunning {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	close(s.stopCh)

	select {
	case <-s.doneCh:
	case <-time.After(10 * time.Second):
		return fmt.Errorf("shutdown timeout")
	}

	return nil
}

// discoverUpstreamModels 拉取上游模型列表并解析出模型名（OpenAI /v1/models
// 与 Google 原生接口双分支）。返回 (names, errStatus, err)：成功 err==nil；
// 失败时 errStatus 是应返回给前端的 HTTP 状态（502 本地故障 / 上游原状态码）。
// 无 DB 依赖，可单测（见 discover_test.go）。
func discoverUpstreamModels(ctx context.Context, baseURL, fetchToken string) ([]string, int, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	// Detect whether this is a real Google Generative Language API endpoint.
	// Google uses /v1beta/models?key=..., the x-goog-api-key header, and returns
	// {models:[{name:"models/..."}]} — completely different from OpenAI's
	// /v1/models + Bearer + {data:[{id}]}. Branching here is required; the OpenAI
	// path returns 401/404 against the genuine Google API.
	googleNative := apiformat.IsGoogleNativeBaseURL(baseURL)

	var modelsURL string
	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, "", nil)
	if googleNative {
		modelsURL = apiformat.BuildGoogleListModelsURL(baseURL, fetchToken)
		httpReq.Header.Set(apiformat.GoogleAPIKeyHeader, fetchToken) // Google API key auth (not Bearer).
	} else {
		// OpenAI-compatible: normalise base URL and call /v1/models.
		modelsURL = apiformat.NormalizeModelsBaseURL(baseURL) + "/v1/models"
		httpReq.Header.Set("Authorization", "Bearer "+fetchToken)
	}
	httpReq.URL, _ = url.Parse(modelsURL)
	httpReq.Header.Set("Content-Type", "application/json")

	logger.DefaultConsole().Info("service", "[FETCH] fetching models", "url", modelsURL)
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, 502, fmt.Errorf("fetch models failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, resp.StatusCode, fmt.Errorf("upstream returned %d: %s", resp.StatusCode, string(body))
	}

	var modelNames []string
	if googleNative {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, 502, fmt.Errorf("read models response failed: %v", readErr)
		}
		modelNames = apiformat.ParseGoogleListModelsResponse(bodyBytes)
		if modelNames == nil {
			return nil, 502, fmt.Errorf("parse google models response failed")
		}
	} else {
		var result struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, 502, fmt.Errorf("parse models response failed: %v", err)
		}
		modelNames = make([]string, 0, len(result.Data))
		for _, m := range result.Data {
			if m.ID != "" {
				modelNames = append(modelNames, m.ID)
			}
		}
	}
	return modelNames, 0, nil
}

// isContainerEnv 判断是否跑在容器里。两种信号任一命中即算：
//   - /.dockerenv：Docker 官方镜像（含 compose）会在根目录放这个标记文件；
//   - /proc/1/cgroup 含 "docker"：containerd / 部分编排器没有该标记文件。
//
// 用来决定面板是否绑 0.0.0.0（见 Run 里的注释）。
func isContainerEnv() bool {
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}
	if b, err := os.ReadFile("/proc/1/cgroup"); err == nil {
		s := strings.ToLower(string(b))
		if strings.Contains(s, "docker") || strings.Contains(s, "containerd") {
			return true
		}
	}
	return false
}

func createWebHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"not found"}`))
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write([]byte(dashboardHTML))
	})

	// 中心同步状态 + 手动刷新（设计 §5.1）。只读，定义类 CRUD 在代理端按需只读化。
	mux.HandleFunc("/api/sync/state", handleSyncState)
	mux.HandleFunc("/api/sync/refresh", handleSyncRefresh)
	mux.HandleFunc("/api/sync/pull-mode", handleSyncPullMode)
	// 排障：管理端无法上传/管理模式未激活的具体原因。
	mux.HandleFunc("/api/sync/diagnostics", handleSyncDiagnostics)
	// 中心连通性 + 各表统计 + 是否同步（仪表盘状态卡）。
	mux.HandleFunc("/api/sync/center", handleSyncCenter)
	// 本地→中心 整体覆盖推送（管理模式专用，破坏性，前端二次确认）。
	mux.HandleFunc("/api/sync/push", handleSyncPush)

	// RAPI endpoints
	mux.HandleFunc("/api/rapis", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			rapis, err := store.A().GetRAPIs()
			if err != nil {
				writeJSONError(w, 500, err)
				return
			}
			if rapis == nil {
				rapis = []models.RAPIWithPlatform{}
			}
			data, _ := json.Marshal(rapis)
			w.Write(data)

		case http.MethodPost:
			var rapi models.RAPI
			if err := json.NewDecoder(r.Body).Decode(&rapi); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			trimRAPINaming(&rapi)
			// Alias 是显示名（设计 §2：可随便改）——用户显式填写则尊重；
			// 留空才按统一命名规则（厂商/系列-版本-后缀 → 备注 → 上游模型名）
			// 自动生成。统一小写以兼容 LAPI 兜底匹配；Model 保留上游原始大小写。
			rapi.Alias = strings.ToLower(strings.TrimSpace(rapi.Alias))
			if rapi.Alias == "" {
				rapi.Alias = deriveRAPIAlias(&rapi)
			}
			if rapi.Alias == "" {
				writeJSONError(w, 400, fmt.Errorf("请填写名称，或至少填写厂商/系列/版本/后缀、模型备注或上游模型名之一"))
				return
			}
			// 端点身份 = (base_url, model)：同平台同 model 重复即冲突（设计 §2）。
			if exists, err := store.A().RAPIModelExists(rapi.PlatformID, rapi.Model, 0); err != nil {
				writeJSONError(w, 500, err)
				return
			} else if exists {
				writeJSONError(w, 409, fmt.Errorf("该平台下模型 %q 已存在（同 base_url + model 视为同一端点）", rapi.Model))
				return
			}
			if err := store.A().CreateRAPI(&rapi); err != nil {
				logger.DefaultConsole().Error("service", "[API] CreateRAPI failed",
					"alias", rapi.Alias, "platform_id", rapi.PlatformID, "error", err.Error())
				writeJSONError(w, 500, err)
				return
			}
			logger.DefaultConsole().Info("service", "[API] CreateRAPI success",
				"id", rapi.ID, "alias", rapi.Alias, "model", rapi.Model, "platform_id", rapi.PlatformID)
			// Auto-map: if model identity matches a LAPI, add to its routing chain
			autoMapRAPItoLAPI(&rapi)
			w.Write([]byte(`{"success":true}`))

		case http.MethodPut:
			var rapi models.RAPI
			if err := json.NewDecoder(r.Body).Decode(&rapi); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			trimRAPINaming(&rapi)
			// Alias 规则同 POST：显式填写优先，留空才自动生成。
			rapi.Alias = strings.ToLower(strings.TrimSpace(rapi.Alias))
			if rapi.Alias == "" {
				rapi.Alias = deriveRAPIAlias(&rapi)
			}
			if rapi.Alias == "" {
				writeJSONError(w, 400, fmt.Errorf("请填写名称，或至少填写厂商/系列/版本/后缀、模型备注或上游模型名之一"))
				return
			}
			// 端点身份冲突校验（excludeID 排除自身行）。
			if exists, err := store.A().RAPIModelExists(rapi.PlatformID, rapi.Model, rapi.ID); err != nil {
				writeJSONError(w, 500, err)
				return
			} else if exists {
				writeJSONError(w, 409, fmt.Errorf("该平台下模型 %q 已存在（同 base_url + model 视为同一端点）", rapi.Model))
				return
			}
			// Remember whether the model was unavailable before the edit, so a
			// key_ids whitelist change that adds a usable key can trigger an
			// automatic re-probe (previously it stayed dead until manual retry).
			wasUnavailable := false
			if prev, err := store.A().GetRAPIByID(rapi.ID); err == nil && prev != nil {
				wasUnavailable = prev.Enabled && !prev.Available
			}
			if err := store.A().UpdateRAPI(&rapi); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			if wasUnavailable {
				rep := proxyGateway.RecoverRAPIs(r.Context(), []int64{rapi.ID}, 1, 8)
				if len(rep.Recovered) > 0 {
					logger.DefaultConsole().Info("service", "[API] auto-recovered model after whitelist edit",
						"rapi_id", rapi.ID, "alias", rapi.Alias)
				}
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			force := r.URL.Query().Get("force") == "true"
			// sync=true is used by the discovery wizard when a previously
			// auto-discovered model is no longer returned by the platform. It
			// bypasses the enabled/available guard (the model may still be
			// "effective") and always cascades (removes LAPI refs + metrics).
			sync := r.URL.Query().Get("sync") == "true"
			var rapiID int64
			fmt.Sscanf(id, "%d", &rapiID)

			// State-based delete: active RAPIs cannot be deleted
			rapiInfo, err := store.A().GetRAPIByID(rapiID)
			if err != nil {
				writeJSONError(w, 404, fmt.Errorf("模型不存在"))
				return
			}
			if !sync && rapiInfo.Enabled && rapiInfo.Available {
				writeJSONError(w, 403, fmt.Errorf("生效状态的模型不能删除，请先禁用"))
				return
			}

			if force || sync {
				// Cascade delete: remove LAPI references + metrics + RAPI
				if err := store.A().DeleteRAPICascade(rapiID); err != nil {
					writeJSONError(w, 500, err)
					return
				}
				proxyGateway.InvalidateRAPI(rapiID)
			} else {
				// Non-force: only succeeds if no LAPI references exist
				if err := store.A().DeleteRAPI(rapiID); err != nil {
					writeJSONError(w, 409, err)
					return
				}
				proxyGateway.InvalidateRAPI(rapiID)
			}
			w.Write([]byte(`{"success":true}`))
		}
	})

	// Key-model direct test: POST /api/rapis/test {rapi_id, key_id, body}
	// Sends a real chat/completions request straight to the upstream platform with
	// the chosen key, bypassing the routing chain — for verifying a specific
	// key-model pairing (e.g. after billing cooldown or whitelist edits).
	mux.HandleFunc("/api/rapis/test", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			RAPIID         int64  `json:"rapi_id"`
			KeyID          int64  `json:"key_id"`
			Body           string `json:"body"`
			ClearOnSuccess bool   `json:"clear_on_success"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		rapi, err := store.A().GetRAPIByID(req.RAPIID)
		if err != nil || rapi == nil {
			writeJSONError(w, 404, fmt.Errorf("模型不存在"))
			return
		}
		platform, err := store.A().GetPlatformByID(rapi.PlatformID)
		if err != nil || platform == nil {
			writeJSONError(w, 404, fmt.Errorf("平台不存在"))
			return
		}
		// Resolve the chosen key token (key_id=0 falls back to the platform token).
		var token string
		if req.KeyID > 0 {
			keys, err := store.A().GetPlatformKeys(platform.ID)
			if err != nil {
				writeJSONError(w, 500, err)
				return
			}
			var found bool
			for i := range keys {
				if keys[i].ID == req.KeyID {
					token = keys[i].Token
					found = true
					break
				}
			}
			if !found {
				writeJSONError(w, 404, fmt.Errorf("key not found"))
				return
			}
		} else {
			token = platform.Token
		}
		if token == "" {
			writeJSONError(w, 400, fmt.Errorf("所选 Key 的 token 为空"))
			return
		}

		// Build the upstream URL from the platform base + the RAPI's model.
		var upstreamURL string
		if apiformat.IsGoogleNativeBaseURL(platform.BaseURL) {
			upstreamURL = apiformat.BuildURLs(platform.BaseURL, rapi.Model, apiformat.FormatGemini)[0]
		} else {
			upstreamURL = apiformat.BuildURLs(platform.BaseURL, rapi.Model, apiformat.FormatOpenAI)[0]
		}

		// Default minimal test body when none is provided.
		body := req.Body
		if strings.TrimSpace(body) == "" {
			body = fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"ping"}],"max_tokens":16}`, rapi.Model)
		}

		client := &http.Client{Timeout: 60 * time.Second}
		httpReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, strings.NewReader(body))
		if err != nil {
			writeJSONError(w, 500, err)
			return
		}
		if apiformat.IsGoogleNativeBaseURL(platform.BaseURL) {
			httpReq.Header.Set(apiformat.GoogleAPIKeyHeader, token)
		} else {
			httpReq.Header.Set("Authorization", "Bearer "+token)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		// Merge per-RAPI custom headers (JSON array of {key,value}).
		if rapi.CustomHeaders != "" {
			var hs []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}
			if json.Unmarshal([]byte(rapi.CustomHeaders), &hs) == nil {
				for _, h := range hs {
					if h.Key != "" {
						httpReq.Header.Set(h.Key, h.Value)
					}
				}
			}
		}

		start := time.Now()
		resp, err := client.Do(httpReq)
		if err != nil {
			writeJSONError(w, 502, fmt.Errorf("上游请求失败: %v", err))
			return
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		if len(respBody) > 60*1024 {
			respBody = respBody[:60*1024]
		}
		latency := time.Since(start).Milliseconds()

		logger.DefaultConsole().Info("service", "[RAPI-TEST] direct key-model test",
			"rapi_id", rapi.ID, "key_id", req.KeyID, "model", rapi.Model,
			"url", upstreamURL, "status", resp.StatusCode, "latency_ms", latency)

		// One-click recovery: on a 2xx response, clear the key's failure marker so
		// the scheduler stops skipping it and it re-enters the pool immediately.
		// A successful direct test also proves this key serves this model — clear
		// any key×model capability block for the pair.
		cleared := false
		if req.ClearOnSuccess && req.KeyID > 0 && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if err := clearKeyFailureState(platform.ID, req.KeyID); err == nil {
				cleared = true
			}
			proxyGateway.UnblockKeyForModel(req.KeyID, rapi.ID)
		}

		data, _ := json.Marshal(map[string]any{
			"status":     resp.StatusCode,
			"latency_ms": latency,
			"body":       string(respBody),
			"cleared":    cleared,
		})
		w.Write(data)
	})

	// Key×model capability block management (the platform revoked a key's access
	// to a model). GET lists all blocks; DELETE removes one (operator cleared it
	// or the platform re-granted access) and re-probes the model.
	mux.HandleFunc("/api/key-model-blocks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			blocks, err := db.Get().GetKeyModelBlocks()
			if err != nil {
				writeJSONError(w, 500, err)
				return
			}
			if blocks == nil {
				blocks = []models.KeyModelBlock{}
			}
			now := time.Now()
			type blockView struct {
				models.KeyModelBlock
				Expired bool `json:"expired"`
			}
			view := make([]blockView, 0, len(blocks))
			for _, b := range blocks {
				view = append(view, blockView{KeyModelBlock: b, Expired: !b.ExpiresAt.IsZero() && !b.ExpiresAt.After(now)})
			}
			json.NewEncoder(w).Encode(view)
		case http.MethodDelete:
			keyIDStr := r.URL.Query().Get("key_id")
			rapiIDStr := r.URL.Query().Get("rapi_id")
			var keyID, rapiID int64
			fmt.Sscanf(keyIDStr, "%d", &keyID)
			fmt.Sscanf(rapiIDStr, "%d", &rapiID)
			if keyID == 0 || rapiID == 0 {
				http.Error(w, `{"error":"missing key_id or rapi_id"}`, 400)
				return
			}
			proxyGateway.UnblockKeyForModel(keyID, rapiID)
			// If the model was persisted unavailable because its pool was all
			// blocked, re-probe it now that a key is unblocked.
			rep := proxyGateway.RecoverRAPIs(r.Context(), []int64{rapiID}, 1, 8)
			if len(rep.Recovered) > 0 {
				logger.DefaultConsole().Info("service", "[BLOCK] model auto-restored after unblock",
					"rapi_id", rapiID)
			}
			w.Write([]byte(`{"success":true}`))
		default:
			http.Error(w, `{"error":"method not allowed"}`, 405)
		}
	})

	// Status toggle endpoints
	mux.HandleFunc("/api/platforms/toggle", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			ID      int64 `json:"id"`
			Enabled bool  `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if err := store.A().SetPlatformEnabled(req.ID, req.Enabled); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		// Sync all RAPIs for this platform: disable+invalidate or enable+revalidate.
		rapis, _ := store.A().GetRAPIsByPlatform(req.ID)
		for _, r := range rapis {
			if !req.Enabled {
				store.A().SetRAPIEnabled(r.ID, false)
				proxyGateway.InvalidateRAPI(r.ID)
			} else {
				store.A().SetRAPIEnabled(r.ID, true)
				proxyGateway.RevalidateRAPI(r.ID)
			}
		}
		w.Write([]byte(`{"success":true}`))
	})

	mux.HandleFunc("/api/rapis/toggle", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			ID      int64 `json:"id"`
			Enabled bool  `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if err := store.A().SetRAPIEnabled(req.ID, req.Enabled); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		// Sync in-memory scheduler state with the new enabled value.
		if !req.Enabled {
			proxyGateway.InvalidateRAPI(req.ID)
		} else {
			proxyGateway.RevalidateRAPI(req.ID)
		}
		w.Write([]byte(`{"success":true}`))
	})

	// Restore a platform-failed RAPI: clears available=false and unavailable_reason.
	mux.HandleFunc("/api/rapis/restore", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			ID int64 `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		rapi, err := store.A().GetRAPIByID(req.ID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("模型不存在"))
			return
		}
		// Real probe: detect supported formats against the upstream using the same
		// credentials real requests use — the first usable platform key, not the
		// often-empty legacy platform token (the JD recovery blind spot). If any
		// format responds, the model is reachable. Only then do we clear
		// unavailable and revalidate. This avoids "recovering" a model that
		// fails again instantly.
		probeClient := &http.Client{Timeout: 15 * time.Second}
		probeCtx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		probeToken := platformKeyToken(rapi.PlatformID, rapi.Token)
		results := apiformat.DetectFormats(probeCtx, rapi.BaseURL, rapi.Model, probeToken, probeClient)
		var supported []apiformat.APIFormat
		for _, res := range results {
			if res.Supported {
				supported = append(supported, res.Format)
			}
		}
		if len(supported) == 0 {
			writeJSONError(w, 502, fmt.Errorf("连通测试失败：上游无响应或不支持任何协议"))
			return
		}
		if err := db.Get().SetRAPIUnavailableWithReason(req.ID, true, ""); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		if len(supported) > 0 {
			store.A().UpdateRAPIFormats(req.ID, apiformat.FormatsToJSON(supported))
		}
		proxyGateway.RevalidateRAPI(req.ID)
		logger.DefaultConsole().Info("service", "[API] restoreRAPI probed ok",
			"rapi_id", req.ID, "alias", rapi.Alias, "formats", fmt.Sprint(supported))
		resp, _ := json.Marshal(map[string]interface{}{
			"success": true,
			"formats": supported,
		})
		w.Write(resp)
	})

	// Probe and recover every RAPI currently persisted as unavailable.
	// Powers the dashboard "retry all unhealthy models" action.
	mux.HandleFunc("/api/system/retry-unhealthy", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		cfg, err := config.Load()
		if err != nil {
			cfg = &config.Config{RetryConcurrency: 8, RetryTimeoutSec: 15}
		}
		logger.DefaultConsole().Info("service", "[API] retry-unhealthy triggered")
		rep := proxyGateway.RecoverUnhealthyRAPIs(r.Context(), cfg.RetryConcurrency, cfg.RetryTimeoutSec)
		logger.DefaultConsole().Info("service", "[API] retry-unhealthy done",
			"probed", rep.Probed, "recovered", len(rep.Recovered), "failed", len(rep.Failed))
		json.NewEncoder(w).Encode(rep)
	})

	// Restore an invalidated platform: re-fetch /v1/models to verify connectivity.
	// On success: set available=true and revalidate all child RAPIs.
	mux.HandleFunc("/api/platforms/restore", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			ID int64 `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		platform, err := store.A().GetPlatformByID(req.ID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("platform not found"))
			return
		}

		// Build the list-models URL. Real Google Generative Language endpoints
		// require /v1beta/models + x-goog-api-key; the OpenAI /v1/models + Bearer
		// path 404s/401s there. Branch exactly like fetch-models / probePlatform.
		// Use the first usable key (skips disabled/permanently-failed/expired).
		fetchToken := platformKeyToken(platform.ID, platform.Token)

		client := &http.Client{Timeout: 15 * time.Second}
		httpReq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, "", nil)
		if apiformat.IsGoogleNativeBaseURL(platform.BaseURL) {
			modelsURL := apiformat.BuildGoogleListModelsURL(platform.BaseURL, fetchToken)
			httpReq.URL, _ = url.Parse(modelsURL)
			httpReq.Header.Set(apiformat.GoogleAPIKeyHeader, fetchToken)
		} else {
			modelsURL := apiformat.NormalizeModelsBaseURL(platform.BaseURL) + "/v1/models"
			httpReq.URL, _ = url.Parse(modelsURL)
			httpReq.Header.Set("Authorization", "Bearer "+fetchToken)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		logger.DefaultConsole().Info("service", "[RESTORE] testing platform",
			"platform_id", req.ID, "url", httpReq.URL.String())
		resp, err := client.Do(httpReq)
		if err != nil {
			writeJSONError(w, 502, fmt.Errorf("连接失败: %v", err))
			return
		}
		defer resp.Body.Close()
		io.ReadAll(resp.Body) // drain

		if resp.StatusCode != 200 {
			writeJSONError(w, 502, fmt.Errorf("平台返回 %d，恢复失败", resp.StatusCode))
			return
		}

		// Success: restore platform and all child RAPIs. The Platform entity
		// persists available=true as its transition effect; then every child
		// RAPI is cleared + revalidated.
		pe := proxyGateway.Scheduler().PlatformEntity(req.ID, false)
		pe.OnDetectSuccess()
		rapis, _ := store.A().GetRAPIsByPlatform(req.ID)
		for _, rapi := range rapis {
			db.Get().SetRAPIUnavailableWithReason(rapi.ID, true, "")
			proxyGateway.RevalidateRAPI(rapi.ID)
		}
		logger.DefaultConsole().Info("service", "[RESTORE] platform restored",
			"platform_id", req.ID, "rapis_revalidated", len(rapis))
		w.Write([]byte(`{"success":true}`))
	})

	// Custom headers endpoint: GET/PUT /api/rapis/headers?id=N
	mux.HandleFunc("/api/rapis/headers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		id := r.URL.Query().Get("id")
		var rapiID int64
		fmt.Sscanf(id, "%d", &rapiID)
		if rapiID == 0 {
			http.Error(w, `{"error":"missing id"}`, 400)
			return
		}

		switch r.Method {
		case http.MethodGet:
			rapi, err := store.A().GetRAPIByID(rapiID)
			if err != nil {
				writeJSONError(w, 404, fmt.Errorf("RAPI not found"))
				return
			}
			resp, _ := json.Marshal(map[string]string{"custom_headers": rapi.CustomHeaders})
			w.Write(resp)

		case http.MethodPut:
			var body struct {
				CustomHeaders string `json:"custom_headers"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			if err := store.A().UpdateRAPIHeaders(rapiID, body.CustomHeaders); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

		default:
			http.Error(w, `{"error":"method not allowed"}`, 405)
		}
	})

	// Format detection endpoint
	mux.HandleFunc("/api/rapis/detect-formats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			ID int64 `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		rapi, err := store.A().GetRAPIByID(req.ID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("RAPI not found"))
			return
		}
		logger.DefaultConsole().Info("service", "[DETECT] starting format detection",
			"rapi", rapi.Alias, "model", rapi.Model, "base_url", rapi.BaseURL)
		results := apiformat.DetectFormats(r.Context(), rapi.BaseURL, rapi.Model, rapi.Token, nil)
		var supported []apiformat.APIFormat
		for _, res := range results {
			if res.Supported {
				supported = append(supported, res.Format)
			}
		}
		formatsJSON := apiformat.FormatsToJSON(supported)
		if err := store.A().UpdateRAPIFormats(req.ID, formatsJSON); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		logger.DefaultConsole().Info("service", "[DETECT] formats detected",
			"rapi", rapi.Alias, "formats", formatsJSON)
		resp, _ := json.Marshal(map[string]interface{}{
			"success": true,
			"formats": supported,
			"results": results,
		})
		w.Write(resp)
	})

	// Platform endpoints
	mux.HandleFunc("/api/platforms", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			platforms, err := store.A().GetPlatforms()
			if err != nil {
				writeJSONError(w, 500, err)
				return
			}
			if platforms == nil {
				platforms = []models.Platform{}
			}
			data, _ := json.Marshal(platforms)
			w.Write(data)

		case http.MethodPost:
			var p models.Platform
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			if err := validatePlatformInput(&p); err != nil {
				writeJSONError(w, 400, err)
				return
			}
			if err := store.A().CreatePlatform(&p); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			// Never echo the plaintext login password back to the client.
			p.LoginPassword = ""
			data, _ := json.Marshal(p)
			w.Write(data)

		case http.MethodPut:
			var p models.Platform
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			if err := validatePlatformInput(&p); err != nil {
				writeJSONError(w, 400, err)
				return
			}
			if err := store.A().UpdatePlatform(&p); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			force := r.URL.Query().Get("force") == "true"
			var pID int64
			fmt.Sscanf(id, "%d", &pID)

			// State-based delete: active platforms cannot be deleted
			platform, err := store.A().GetPlatformByID(pID)
			if err != nil {
				writeJSONError(w, 404, fmt.Errorf("平台不存在"))
				return
			}
			if platform.Enabled && platform.Available {
				writeJSONError(w, 403, fmt.Errorf("生效状态的平台不能删除，请先禁用"))
				return
			}

			if force {
				// Cascade delete: platform + all child RAPIs + references + keys
				// Invalidate all child RAPIs in scheduler first
				rapis, _ := store.A().GetRAPIsByPlatform(pID)
				for _, rapi := range rapis {
					proxyGateway.InvalidateRAPI(rapi.ID)
				}
				if err := store.A().DeletePlatformCascade(pID); err != nil {
					writeJSONError(w, 500, err)
					return
				}
			} else {
				// Non-force: only succeeds if no child RAPIs exist
				if err := store.A().DeletePlatform(pID); err != nil {
					writeJSONError(w, 409, err)
					return
				}
			}
			w.Write([]byte(`{"success":true}`))
		}
	})

	// Upstream test for the unified add wizard: probes an arbitrary base_url+token
	// (with optional model) without saving anything. model empty = /v1/models
	// connectivity probe; model set = minimal chat completion against that model.
	mux.HandleFunc("/api/upstream/test", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			BaseURL string `json:"base_url"`
			Token   string `json:"token"`
			Model   string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		req.BaseURL = strings.TrimSpace(req.BaseURL)
		if req.BaseURL == "" {
			writeJSONError(w, 400, fmt.Errorf("base_url 不能为空"))
			return
		}
		if strings.TrimSpace(req.Token) == "" {
			writeJSONError(w, 400, fmt.Errorf("token 不能为空"))
			return
		}

		googleNative := apiformat.IsGoogleNativeBaseURL(req.BaseURL)
		client := &http.Client{Timeout: 30 * time.Second}
		start := time.Now()

		if strings.TrimSpace(req.Model) == "" {
			// Connectivity probe: GET /v1/models (or Google /v1beta/models).
			var modelsURL string
			hreq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, "", nil)
			if googleNative {
				modelsURL = apiformat.BuildGoogleListModelsURL(req.BaseURL, req.Token)
				hreq.Header.Set(apiformat.GoogleAPIKeyHeader, req.Token)
			} else {
				modelsURL = apiformat.NormalizeModelsBaseURL(req.BaseURL) + "/v1/models"
				hreq.Header.Set("Authorization", "Bearer "+req.Token)
			}
			hreq.URL, _ = url.Parse(modelsURL)
			resp, err := client.Do(hreq)
			if err != nil {
				writeJSONError(w, 502, fmt.Errorf("连接失败: %v", err))
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if len(body) > 8192 {
				body = body[:8192]
			}
			json.NewEncoder(w).Encode(map[string]any{
				"ok":         resp.StatusCode >= 200 && resp.StatusCode < 300,
				"status":     resp.StatusCode,
				"latency_ms": time.Since(start).Milliseconds(),
				"body":       string(body),
			})
			return
		}

		// Model test: minimal chat completion.
		format := apiformat.FormatOpenAI
		if googleNative {
			format = apiformat.FormatGemini
		}
		upstreamURL := apiformat.BuildURLs(req.BaseURL, req.Model, format)[0]
		body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"ping"}],"max_tokens":16}`, req.Model)
		hreq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, strings.NewReader(body))
		if googleNative {
			hreq.Header.Set(apiformat.GoogleAPIKeyHeader, req.Token)
		} else {
			hreq.Header.Set("Authorization", "Bearer "+req.Token)
		}
		hreq.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(hreq)
		if err != nil {
			writeJSONError(w, 502, fmt.Errorf("请求失败: %v", err))
			return
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		if len(respBody) > 8192 {
			respBody = respBody[:8192]
		}
		json.NewEncoder(w).Encode(map[string]any{
			"ok":         resp.StatusCode >= 200 && resp.StatusCode < 300,
			"status":     resp.StatusCode,
			"latency_ms": time.Since(start).Milliseconds(),
			"body":       string(respBody),
		})
	})

	// Fetch models from platform's /v1/models endpoint
	mux.HandleFunc("/api/platforms/fetch-models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			ID int64 `json:"id"`
			// 一次性发现（新增向导里的新平台，尚未入库）：直接给 base_url + token
			// 探测，不读 DB。id 与 base_url 二选一，id 优先。
			BaseURL string `json:"base_url"`
			Token   string `json:"token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		var baseURL, fetchToken, platName string
		if req.ID != 0 {
			platform, err := store.A().GetPlatformByID(req.ID)
			if err != nil {
				writeJSONError(w, 404, fmt.Errorf("platform not found"))
				return
			}
			baseURL, platName = platform.BaseURL, platform.Name
			// Task 19: Use the first (index=0) PlatformKey token instead of platform.Token.
			fetchToken = platform.Token
			if keys, err := store.A().GetPlatformKeys(platform.ID); err == nil && len(keys) > 0 {
				fetchToken = keys[0].Token
			}
		} else {
			baseURL = strings.TrimSpace(req.BaseURL)
			fetchToken = req.Token
			platName = baseURL
			if baseURL == "" || fetchToken == "" {
				writeJSONError(w, http.StatusBadRequest,
					errors.New("一次性发现需要 base_url 与 token（请在向导第①步填地址、第②步填密钥）"))
				return
			}
		}

	modelNames, errStatus, ferr := discoverUpstreamModels(r.Context(), baseURL, fetchToken)
	if ferr != nil {
		writeJSONError(w, errStatus, ferr)
		return
	}

		sort.Strings(modelNames)

		logger.DefaultConsole().Info("service", "[FETCH] models fetched",
			"count", len(modelNames), "platform", platName)
		resp2, _ := json.Marshal(map[string]interface{}{
			"success": true,
			"models":  modelNames,
		})
		w.Write(resp2)
	})

	// Batch create RAPIs + platform-level format detection
	mux.HandleFunc("/api/rapis/batch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			PlatformID int64    `json:"platform_id"`
			Models     []string `json:"models"`
			// Source: "auto_discover" (from discovery wizard) or "manual". Defaults to
			// "manual" for backward compatibility. Auto-discovered models may be pruned
			// by a later re-sync; manual models are never auto-pruned.
			Source string `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if len(req.Models) == 0 {
			writeJSONError(w, 400, fmt.Errorf("no models specified"))
			return
		}
		source := req.Source
		if source == "" {
			source = "manual"
		}
		platform, err := store.A().GetPlatformByID(req.PlatformID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("platform not found"))
			return
		}

		// Platform-level format detection (once for all RAPIs). The result becomes
		// the platform-level authority (platform.supported_formats) and is also
		// cached on each created RAPI for the gateway's routing checks. We also
		// record each format's first working URL in platform.format_endpoints so
		// the gateway can forward to the exact endpoint detection proved works
		// (important for aggregators with non-standard paths).
		logger.DefaultConsole().Info("service", "[BATCH] detecting formats for platform", "platform", platform.Name)
		// Use the first usable platform key (not platform.Token, which may be empty
		// when keys live in credential) so format detection exercises the same
		// credentials as real requests.
		detectResults := apiformat.DetectFormats(r.Context(), platform.BaseURL, req.Models[0], platformKeyToken(req.PlatformID, platform.Token), nil)
		var supportedFormats []apiformat.APIFormat
		endpoints := make(map[string]string, len(detectResults))
		for _, res := range detectResults {
			if res.Supported {
				supportedFormats = append(supportedFormats, res.Format)
				if res.URL != "" {
					endpoints[string(res.Format)] = res.URL
				}
			}
		}
		formatsJSON := apiformat.FormatsToJSON(supportedFormats)
		var endpointsJSON string
		if len(endpoints) > 0 {
			b, _ := json.Marshal(endpoints)
			endpointsJSON = string(b)
		}
		// Persist as the platform-level authority (propagate to child RAPIs below).
		if formatsJSON != "" {
			if err := store.A().UpdatePlatformFormats(req.PlatformID, formatsJSON, false, endpointsJSON); err != nil {
				logger.DefaultConsole().Error("service", "[BATCH] UpdatePlatformFormats failed",
					"platform_id", req.PlatformID, "error", err.Error())
			}
		}
		logger.DefaultConsole().Info("service", "[BATCH] platform formats detected",
			"platform", platform.Name, "formats", formatsJSON, "endpoints", endpointsJSON)

		// Create RAPIs
		created := make([]map[string]interface{}, 0)
		var errors []string
		for _, modelName := range req.Models {
			modelName = strings.TrimSpace(modelName)
			// Alias is lowercase for matching; Model preserves original casing from
			// platform. The provider's original name also becomes the model remark
			// (模型备注) so the auto-derived display name falls back to it.
			rapi := models.RAPI{
				Alias:            strings.ToLower(modelName),
				Model:            modelName,
				Notes:            modelName,
				PlatformID:       req.PlatformID,
				Enabled:          true,
				Available:        true,
				SupportedFormats: formatsJSON,
				Source:           source,
			}
			if err := store.A().CreateRAPI(&rapi); err != nil {
				errors = append(errors, fmt.Sprintf("%s: %v", modelName, err))
				logger.DefaultConsole().Error("service", "[BATCH] create RAPI failed",
					"model", modelName, "error", err.Error())
			} else {
				autoMapRAPItoLAPI(&rapi)
				created = append(created, map[string]interface{}{
					"id":     rapi.ID,
					"alias":  rapi.Alias,
					"model":  rapi.Model,
					"source": rapi.Source,
				})
			}
		}

		logger.DefaultConsole().Info("service", "[BATCH] RAPIs created",
			"count", len(created), "platform", platform.Name, "formats", formatsJSON)
		resp, _ := json.Marshal(map[string]interface{}{
			"success":        true,
			"created":        len(created),
			"failed":         len(errors),
			"errors":         errors,
			"formats":        supportedFormats,
			"detect_results": detectResults,
			"rapis":          created,
		})
		w.Write(resp)
	})

	// Platform Keys CRUD + probe endpoint: /api/platforms/{id}/keys[/{keyId}/probe]
	mux.HandleFunc("/api/platforms/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Only handle paths that look like /api/platforms/{id}/keys
		path := strings.TrimPrefix(r.URL.Path, "/api/platforms/")
		parts := strings.Split(path, "/")
		if len(parts) < 2 || parts[1] != "keys" {
			http.Error(w, `{"error":"not found"}`, 404)
			return
		}

		var platformID int64
		fmt.Sscanf(parts[0], "%d", &platformID)
		if platformID == 0 {
			http.Error(w, `{"error":"invalid platform id"}`, 400)
			return
		}

		// Sub-path: /api/platforms/{id}/keys/{keyId}/probe
		if len(parts) == 4 && parts[3] == "probe" {
			handleKeyProbe(w, r, platformID, parts[2])
			return
		}

		switch r.Method {
		case http.MethodGet:
			keys, err := store.A().GetPlatformKeys(platformID)
			if err != nil {
				writeJSONError(w, 500, err)
				return
			}
			if keys == nil {
				keys = []models.PlatformKey{}
			}
			data, _ := json.Marshal(keys)
			w.Write(data)

		case http.MethodPost:
			var k models.PlatformKey
			if err := json.NewDecoder(r.Body).Decode(&k); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			k.PlatformID = platformID
			if err := store.A().AddPlatformKey(&k); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			// A usable key was added: automatically re-probe any models of this
			// platform that were marked unavailable because all their keys were
			// dead, and restore the ones that actually respond with the new key.
			// Models whose pool is still dead stay marked unavailable.
			if strings.TrimSpace(k.Token) != "" {
				rep := proxyGateway.RecoverPlatformRAPIs(r.Context(), platformID, 4, 8)
				if len(rep.Recovered) > 0 {
					logger.DefaultConsole().Info("service", "[KEY] auto-recovered models after key add",
						"platform_id", platformID, "recovered", len(rep.Recovered))
				}
			}
			data, _ := json.Marshal(k)
			w.Write(data)

		case http.MethodPut:
			// Single-key update when ?key_id=N is given (edit label/free/expiry/enabled
			// without replacing the whole list). An empty token keeps the stored one,
			// so the client never has to echo the plaintext secret back.
			if keyIDStr := r.URL.Query().Get("key_id"); keyIDStr != "" {
				var kid int64
				fmt.Sscanf(keyIDStr, "%d", &kid)
				var k models.PlatformKey
				if err := json.NewDecoder(r.Body).Decode(&k); err != nil {
					http.Error(w, `{"error":"invalid json"}`, 400)
					return
				}
				k.ID = kid
				k.PlatformID = platformID
				if strings.TrimSpace(k.Token) == "" {
					keys, err := store.A().GetPlatformKeys(platformID)
					if err != nil {
						writeJSONError(w, 500, err)
						return
					}
					for i := range keys {
						if keys[i].ID == kid {
							k.Token = keys[i].Token
							break
						}
					}
					if k.Token == "" {
						writeJSONError(w, 404, fmt.Errorf("key not found"))
						return
					}
				}
				if err := store.A().UpdatePlatformKey(&k); err != nil {
					writeJSONError(w, 500, err)
					return
				}
				w.Write([]byte(`{"success":true}`))
				return
			}

			// Replace entire key list.
			var keys []models.PlatformKey
			if err := json.NewDecoder(r.Body).Decode(&keys); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			// Adopt stable ids and preserve failure state (failure_type/reason/failed_at)
			// across the whole-list replace: saving the platform edit form must never
			// re-id keys (that would break RAPI key_ids whitelists) nor silently clear
			// cooldowns / permanent-failure markers. Clients that send ids (current
			// dashboard) match directly; id-less clients (old cached pages, raw API
			// callers) fall back to matching by decrypted token to keep identity.
			existing, _ := store.A().GetPlatformKeys(platformID)
			byID := make(map[int64]models.PlatformKey, len(existing))
			byToken := make(map[string]models.PlatformKey, len(existing))
			for _, ek := range existing {
				byID[ek.ID] = ek
				if t := strings.TrimSpace(ek.Token); t != "" {
					byToken[t] = ek
				}
			}
			for i := range keys {
				if keys[i].ID > 0 {
					if prev, ok := byID[keys[i].ID]; ok {
						keys[i].FailureType = prev.FailureType
						keys[i].FailureReason = prev.FailureReason
						keys[i].FailedAt = prev.FailedAt
					}
					continue
				}
				// id-less key: match by token to keep identity (and failure state).
				if t := strings.TrimSpace(keys[i].Token); t != "" {
					if prev, ok := byToken[t]; ok {
						keys[i].ID = prev.ID
						keys[i].FailureType = prev.FailureType
						keys[i].FailureReason = prev.FailureReason
						keys[i].FailedAt = prev.FailedAt
					}
				}
			}
			if err := store.A().SetPlatformKeys(platformID, keys); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			// Mirrors POST: a non-empty key list restores the platform.
			hasNonEmpty := false
			for _, k := range keys {
				if strings.TrimSpace(k.Token) != "" {
					hasNonEmpty = true
					break
				}
			}
			if hasNonEmpty {
				rep := proxyGateway.RecoverPlatformRAPIs(r.Context(), platformID, 4, 8)
				if len(rep.Recovered) > 0 {
					logger.DefaultConsole().Info("service", "[KEY] auto-recovered models after key list replace",
						"platform_id", platformID, "recovered", len(rep.Recovered))
				}
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			keyID := r.URL.Query().Get("key_id")
			var kid int64
			fmt.Sscanf(keyID, "%d", &kid)
			if kid == 0 {
				http.Error(w, `{"error":"missing key_id"}`, 400)
				return
			}
			// Strip the deleted key from every RAPI key_ids whitelist first so no
			// model keeps a dangling reference (silent pool shrink). Report the
			// affected models back to the UI for the confirmation toast.
			removedFrom, err := store.A().DetachKeyFromRAPIs(kid)
			if err != nil {
				writeJSONError(w, 500, err)
				return
			}
			if err := store.A().DeletePlatformKey(kid); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			// Drop the in-memory scheduler entity (cooldown / recovery scan).
			proxyGateway.RemoveKey(kid)
			// 连带效应未读（用户操作本身不记，但删 key 导致模型失去绑定的
			// 隐性后果需要浮出）：有多少模型被摘掉了这把 key。
			if len(removedFrom) > 0 {
				detail := strings.Join(removedFrom, "、")
				if len([]rune(detail)) > 120 {
					detail = string([]rune(detail)[:120]) + "…"
				}
				recordUnread(notify.MenuModels, "cascade", 0,
					fmt.Sprintf("删除密钥连带：%d 个模型失去密钥绑定", len(removedFrom)), detail)
			}
			data, _ := json.Marshal(map[string]interface{}{
				"success":      true,
				"removed_from": removedFrom,
			})
			w.Write(data)

		default:
			http.Error(w, `{"error":"method not allowed"}`, 405)
		}
	})

	// Sort-order endpoints
	mux.HandleFunc("/api/platforms/reorder", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var ids []int64
		if err := json.NewDecoder(r.Body).Decode(&ids); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if err := store.A().SetPlatformSortOrder(ids); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		w.Write([]byte(`{"success":true}`))
	})

	mux.HandleFunc("/api/rapis/reorder", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var ids []int64
		if err := json.NewDecoder(r.Body).Decode(&ids); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if err := store.A().SetRAPISortOrder(ids); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		w.Write([]byte(`{"success":true}`))
	})

	// LAPI endpoints
	mux.HandleFunc("/api/lapis", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			lapis, err := store.A().GetLAPIs()
			if err != nil {
				writeJSONError(w, 500, err)
				return
			}
			if lapis == nil {
				lapis = []models.LAPI{}
			}
			data, _ := json.Marshal(lapis)
			w.Write(data)

		case http.MethodPost:
			var lapi models.LAPI
			if err := json.NewDecoder(r.Body).Decode(&lapi); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			// Enforce lowercase: LAPI alias must be lowercase to match incoming request model names
			lapi.Alias = strings.ToLower(strings.TrimSpace(lapi.Alias))
			if err := store.A().CreateLAPI(&lapi); err != nil {
				logger.DefaultConsole().Error("service", "[API] CreateLAPI failed",
					"alias", lapi.Alias, "error", err.Error())
				writeJSONError(w, 500, err)
				return
			}
			logger.DefaultConsole().Info("service", "[API] CreateLAPI success",
				"id", lapi.ID, "alias", lapi.Alias)
			data, _ := json.Marshal(lapi)
			w.Write(data)

		case http.MethodPut:
			var lapi models.LAPI
			if err := json.NewDecoder(r.Body).Decode(&lapi); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			// Enforce lowercase: LAPI alias must be lowercase to match incoming request model names
			lapi.Alias = strings.ToLower(strings.TrimSpace(lapi.Alias))
			if err := store.A().UpdateLAPI(&lapi); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			var lapiID int64
			fmt.Sscanf(id, "%d", &lapiID)

			// State-based delete: active LAPIs cannot be deleted
			lapiInfo, err := store.A().GetLAPIByID(lapiID)
			if err != nil {
				writeJSONError(w, 404, fmt.Errorf("路由不存在"))
				return
			}
			if lapiInfo.Enabled {
				writeJSONError(w, 403, fmt.Errorf("生效状态的路由不能删除，请先禁用"))
				return
			}

			if err := store.A().DeleteLAPI(lapiID); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))
		}
	})

	// LAPI toggle endpoint
	mux.HandleFunc("/api/lapis/toggle", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			ID      int64 `json:"id"`
			Enabled bool  `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if err := store.A().SetLAPIEnabled(req.ID, req.Enabled); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		w.Write([]byte(`{"success":true}`))
	})

	// LAPI-RAPI mapping endpoint
	mux.HandleFunc("/api/lapis/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Extract lapi ID from path: /api/lapis/{id}/rapis
		path := strings.TrimPrefix(r.URL.Path, "/api/lapis/")
		parts := strings.Split(path, "/")
		if len(parts) < 1 {
			http.Error(w, `{"error":"invalid path"}`, 400)
			return
		}

		var lapiID int64
		fmt.Sscanf(parts[0], "%d", &lapiID)

		if strings.Contains(r.URL.Path, "/rapis") {
			if r.Method == http.MethodGet {
				rapis, err := store.A().GetRAPIsForLAPI(lapiID)
				if err != nil {
					writeJSONError(w, 500, err)
					return
				}
				data, _ := json.Marshal(rapis)
				w.Write(data)
			} else if r.Method == http.MethodPut {
				var rapiIDs []int64
				if err := json.NewDecoder(r.Body).Decode(&rapiIDs); err != nil {
					http.Error(w, `{"error":"invalid json"}`, 400)
					return
				}
				if err := store.A().SetLAPIRAPIOrder(lapiID, rapiIDs); err != nil {
					writeJSONError(w, 500, err)
					return
				}
				w.Write([]byte(`{"success":true}`))
			}
		}
	})

	// Stats endpoints
	mux.HandleFunc("/api/stats/rapis", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats, err := db.Get().GetRAPIStats()
		if err != nil {
			errorResp, _ := json.Marshal(map[string]string{"error": err.Error()})
			w.WriteHeader(500)
			w.Write(errorResp)
			return
		}
		data, _ := json.Marshal(stats)
		w.Write(data)
	})

	mux.HandleFunc("/api/stats/lapis", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats, err := db.Get().GetLAPIStats()
		if err != nil {
			writeJSONError(w, 500, err)
			return
		}

		// Enrich with RAPI stats for each LAPI
		for i := range stats {
			rapis, err := store.A().GetRAPIsForLAPIWithStats(stats[i].LapiID)
			if err == nil {
				stats[i].RAPIs = rapis
			}
		}

		data, _ := json.Marshal(stats)
		w.Write(data)
	})

	// Insights endpoint — three-layer analysis: health, efficiency, capacity
	mux.HandleFunc("/api/insights", handleInsights)

	// 仪表盘单屏指标：4 维度 × 10 指标 TOP3 × 今日/本月 + 待处理 + 近期错误。
	mux.HandleFunc("/api/dashboard/metrics", handleDashboardMetrics)

	// 中心配置（Supabase）：URL + API key 加密保存 + 角色自动探测。
	mux.HandleFunc("/api/sb-config", handleSBConfig)
	// 进程日志级别：GET 查询，POST/PUT 即时切换并持久化。
	mux.HandleFunc("/api/logs/level", handleLogLevel)
	// 系统/中心互联日志（syncOnce 成败、直写成败、池冷却等）。
	mux.HandleFunc("/api/logs/system", handleSystemLogs)

	// Analytics: 24h hourly distribution for traffic chart
	mux.HandleFunc("/api/analytics/hourly", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		days := 7
		if d := r.URL.Query().Get("days"); d != "" {
			fmt.Sscanf(d, "%d", &days)
		}
		buckets, err := db.Get().GetHourlyDistribution(days)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		// Also return scheduler counters for live utilization
		snap := proxyGateway.Scheduler().Snapshot()
		resp := map[string]interface{}{
			"hourly":   buckets,
			"counters": snap.Counters,
		}
		data, _ := json.Marshal(resp)
		w.Write(data)
	})

	// Analytics: daily token trend
	mux.HandleFunc("/api/analytics/daily", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		days := 7
		if d := r.URL.Query().Get("days"); d != "" {
			fmt.Sscanf(d, "%d", &days)
		}
		trends, err := db.Get().GetDailyTokenTrend(days)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		data, _ := json.Marshal(trends)
		w.Write(data)
	})

	// 状态接口 - 用于Dashboard显示
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		rapis, _ := store.A().GetRAPIs()
		lapis, _ := store.A().GetLAPIs()
		stats, _ := db.Get().GetRAPIStats()

		// 获取刷新间隔参数（默认10秒）
		refreshInterval := 10
		if intervalStr := r.URL.Query().Get("interval"); intervalStr != "" {
			fmt.Sscanf(intervalStr, "%d", &refreshInterval)
		}

		// 计算时间窗口：当前时间往前推刷新间隔
		timeWindow := time.Now().Add(-time.Duration(refreshInterval) * time.Second)

		// 计算总请求数和时间窗口内被请求的API数量
		totalReq := 0
		rapiRequested := 0
		requestedRAPIIds := make(map[int64]bool)
		for _, s := range stats {
			totalReq += s.TotalRequests
			if s.TotalRequests > 0 {
				// 检查是否在时间窗口内被请求
				if s.LastUsed != "Never" {
					if lastUsedTime, err := parseTime(s.LastUsed); err == nil && lastUsedTime.After(timeWindow) {
						rapiRequested++
						requestedRAPIIds[s.RapiID] = true
					}
				}
			}
		}

		// 计算lapi被请求数量（通过检查关联的rapi是否在时间窗口内有请求）
		lapiRequested := 0
		for _, u := range lapis {
			rapiIds, _ := store.A().GetLAPIRAPIMapping(u.ID)
			for _, rapiId := range rapiIds {
				if requestedRAPIIds[rapiId] {
					lapiRequested++
					break
				}
			}
		}

		// 获取真实趋势数据（最近30分钟）
		trends, _ := db.Get().GetRequestTrends(30)
		trendMap := make(map[string]int)
		for _, t := range trends {
			// 格式化为 HH:MM
			if len(t.MinuteBucket) >= 16 {
				trendMap[t.MinuteBucket[11:16]] += t.RequestCount
			}
		}
		history := make([]map[string]interface{}, 0)
		now := time.Now()
		for i := 29; i >= 0; i-- {
			t := now.Add(-time.Duration(i) * time.Minute)
			key := t.Format("15:04")
			count := trendMap[key]
			history = append(history, map[string]interface{}{
				"time":  key,
				"count": count,
			})
		}

		cfg, _ := config.Load()
		fmt.Fprintf(w, `{"version":%q,"proxy_port":%d,"web_port":%d,"rapi_count":%d,"lapi_count":%d,"total_requests":%d,"rapi_requested":%d,"lapi_requested":%d,"rapi_stats":{"requested_count":%d,"configured_count":%d},"lapi_stats":{"requested_count":%d,"configured_count":%d},"history":[`, Version, cfg.ProxyPort, cfg.WebPort, len(rapis), len(lapis), totalReq, rapiRequested, lapiRequested, rapiRequested, len(rapis), lapiRequested, len(lapis))

		for i, h := range history {
			if i > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprintf(w, `{"time":"%s","count":%v}`, h["time"], h["count"])
		}
		fmt.Fprint(w, "]}")
	})

	// Notification SSE stream for browser clients
	mux.HandleFunc("/api/notify/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		id, ch := notifySvc.Subscribe()
		defer notifySvc.Unsubscribe(id)

		// Send initial keepalive.
		fmt.Fprintf(w, ": connected\n\n")
		flusher.Flush()

		// Keepalive ticker: without periodic writes, a silently-dropped TCP connection
		// (proxy/LB/browser closed without FIN) can leave this goroutine blocked on
		// r.Context().Done() for minutes. Meanwhile the subscriber stays in
		// notifySvc.subscribers forever — a memory/connection leak that grows under
		// realistic browser use. The ping probes the connection and, on write failure,
		// lets us exit and run the deferred Unsubscribe.
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case n, ok := <-ch:
				if !ok {
					return
				}
				data, _ := json.Marshal(n)
				// Check write errors: a failed write means the client is gone, so exit
				// (the deferred Unsubscribe will clean up the subscriber).
				if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
					return
				}
				flusher.Flush()
			case <-ticker.C:
				// SSE comment frame — ignored by clients but keeps the connection alive
				// and detects dead peers via write error.
				if _, err := fmt.Fprintf(w, ": ping\n\n"); err != nil {
					return
				}
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	})

	// Logs endpoints
	mux.HandleFunc("/api/logs/sessions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if logInstance == nil || logInstance.Storage == nil {
			w.Write([]byte(`{"sessions":[]}`))
			return
		}

		filter := logger.SessionFilter{
			Limit:  50,
			Offset: 0,
		}
		sessions, err := logInstance.Storage.GetSessions(filter)
		if err != nil {
			writeJSONError(w, 500, err)
			return
		}
		data, _ := json.Marshal(map[string]interface{}{"sessions": sessions})
		w.Write(data)
	})

	mux.HandleFunc("/api/logs/requests", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if logInstance == nil || logInstance.Storage == nil {
			w.Write([]byte(`{"requests":[]}`))
			return
		}

		filter := logger.RequestLogFilter{
			Limit:  50,
			Offset: 0,
		}

		if sessionID := r.URL.Query().Get("session_id"); sessionID != "" {
			filter.SessionID = sessionID
		}
		if lapi := r.URL.Query().Get("lapi"); lapi != "" {
			filter.LapiAlias = lapi
		}
		if status := r.URL.Query().Get("status"); status != "" {
			filter.Status = status
		}

		requests, err := logInstance.Storage.GetRequestLogs(filter)
		if err != nil {
			writeJSONError(w, 500, err)
			return
		}
		data, _ := json.Marshal(map[string]interface{}{"requests": requests})
		w.Write(data)
	})

	mux.HandleFunc("/api/logs/request/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if logInstance == nil || logInstance.Storage == nil {
			http.Error(w, `{"error":"logger not available"}`, 500)
			return
		}

		reqID := strings.TrimPrefix(r.URL.Path, "/api/logs/request/")
		if reqID == "" {
			http.Error(w, `{"error":"missing request id"}`, 400)
			return
		}
		reqLog, err := logInstance.Storage.GetRequestDetail(reqID)
		if err != nil {
			http.Error(w, `{"error":"request not found"}`, 404)
			return
		}
		data, _ := json.Marshal(reqLog)
		w.Write(data)
	})

	mux.HandleFunc("/api/logs/clear", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodDelete {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}

		if logInstance == nil || logInstance.Storage == nil {
			w.Write([]byte(`{"success":true}`))
			return
		}

		err := logInstance.Storage.CleanupOldRecords(0, 0)
		if err != nil {
			writeJSONError(w, 500, err)
			return
		}
		w.Write([]byte(`{"success":true}`))
	})

	// Peek token endpoint — GET /api/platforms/peek-token/{id}
	// Returns a masked preview of the current token for admin display (never the full secret).
	mux.HandleFunc("/api/platforms/peek-token/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		idStr := strings.TrimPrefix(r.URL.Path, "/api/platforms/peek-token/")
		var platformID int64
		fmt.Sscanf(idStr, "%d", &platformID)
		if platformID == 0 {
			http.Error(w, `{"error":"invalid platform id"}`, 400)
			return
		}
		platform, err := store.A().GetPlatformByID(platformID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("platform not found"))
			return
		}
		hasToken := platform.Token != ""
		preview := ""
		if hasToken {
			preview = maskToken(platform.Token)
		}
		resp := map[string]interface{}{
			"has_token": hasToken,
			"token":     preview,
		}
		data, _ := json.Marshal(resp)
		w.Write(data)
	})

	// Flat key list for the independent "密钥管理" menu: every key joined with
	// its platform name and the models it may serve (reverse of RAPI.key_ids),
	// plus the key×model capability-block state so a broken key↔model pair shows
	// in both the key and model views.
	mux.HandleFunc("/api/keys", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		// These four reads are independent of each other. Each store call can be a
		// remote round trip (the active store is Supabase in management mode), so
		// issuing them sequentially dominated this endpoint's latency.
		var (
			keys      []models.PlatformKey
			keysErr   error
			platforms []models.Platform
			rapis     []models.RAPIWithPlatform
			blocks    []models.KeyModelBlock
			wg        sync.WaitGroup
		)
		wg.Add(4)
		go func() { defer wg.Done(); keys, keysErr = store.A().GetAllPlatformKeys() }()
		go func() { defer wg.Done(); platforms, _ = store.A().GetPlatforms() }()
		go func() { defer wg.Done(); rapis, _ = store.A().GetRAPIs() }()
		go func() { defer wg.Done(); blocks, _ = db.Get().GetKeyModelBlocks() }()
		wg.Wait()
		if keysErr != nil {
			writeJSONError(w, 500, keysErr)
			return
		}
		if keys == nil {
			keys = []models.PlatformKey{}
		}
		platName := make(map[int64]string, len(platforms))
		for _, p := range platforms {
			platName[p.ID] = p.Name
		}

		// Composite struct key instead of a fmt.Sprintf'd string: avoids one
		// allocation + formatting call per key×model pair.
		type blockKey struct{ key, rapi int64 }
		blockSet := make(map[blockKey]models.KeyModelBlock, len(blocks))
		for _, b := range blocks {
			blockSet[blockKey{b.KeyID, b.RAPIID}] = b
		}

		// Bucket models by platform and parse each model's key whitelist exactly
		// once. The previous nested loop re-parsed KeyIDs for every key×model pair
		// (O(keys×models) Sscanf calls), which was the real hot spot.
		type rapiRef struct {
			rapi    models.RAPIWithPlatform
			keyIDs  []int64
			allKeys bool // empty whitelist => every key of the platform serves it
		}
		rapisByPlatform := make(map[int64][]rapiRef, len(platforms))
		for _, ra := range rapis {
			ids := parseKeyIDs(ra.KeyIDs)
			rapisByPlatform[ra.PlatformID] = append(rapisByPlatform[ra.PlatformID],
				rapiRef{rapi: ra, keyIDs: ids, allKeys: len(ids) == 0})
		}

		type keyModelView struct {
			ID      int64  `json:"id"`
			Alias   string `json:"alias"`
			Model   string `json:"model"`
			Blocked bool   `json:"blocked"`
			Reason  string `json:"reason,omitempty"`
		}
		type keyView struct {
			models.PlatformKey
			PlatformName  string         `json:"platform_name"`
			Models        []keyModelView `json:"models"`
			Cooling       bool           `json:"cooling"`
			RecoverAt     *time.Time     `json:"recover_at,omitempty"`
			RuntimeReason string         `json:"runtime_reason,omitempty"`
		}

		// The persisted failure_type is durable configuration state, while the
		// scheduler owns the live cooldown timer. Merge the latter into the
		// dashboard projection so a key that is cooling in memory is visible
		// immediately (and remains distinguishable from a permanent failure).
		runtimeKeys := make(map[int64]scheduler.KeySnapshot)
		if proxyGateway != nil {
			for _, ks := range proxyGateway.Scheduler().Snapshot().Keys {
				runtimeKeys[ks.ID] = ks
			}
		}

		out := make([]keyView, 0, len(keys))
		for _, k := range keys {
			ks, cooling := runtimeKeys[k.ID]
			cooling = cooling && ks.Cooling
			var recoverAt *time.Time
			if cooling {
				recoverAt = &ks.RecoverAt
			}
			kv := keyView{
				PlatformKey: k, PlatformName: platName[k.PlatformID], Models: []keyModelView{},
				Cooling: cooling, RecoverAt: recoverAt, RuntimeReason: ks.Reason,
			}
			if refs := rapisByPlatform[k.PlatformID]; len(refs) > 0 {
				kv.Models = make([]keyModelView, 0, len(refs))
				for _, ref := range refs {
					if !ref.allKeys && !intIn(ref.keyIDs, k.ID) {
						continue
					}
					b, blocked := blockSet[blockKey{k.ID, ref.rapi.ID}]
					mv := keyModelView{ID: ref.rapi.ID, Alias: ref.rapi.Alias, Model: ref.rapi.Model, Blocked: blocked}
					if blocked {
						mv.Reason = b.Reason
					}
					kv.Models = append(kv.Models, mv)
				}
			}
			out = append(out, kv)
		}
		json.NewEncoder(w).Encode(out)
	})

	// In-memory unread notifications (per-menu badges in the fixed top bar).
	// GET  /api/unread        → items + per-menu counts + total
	// POST /api/unread/read   → {menu: "keys"} or {all: true} marks read
	mux.HandleFunc("/api/unread", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var items []notify.UnreadItem
		var counts map[string]int
		var total int
		if notifySvc != nil {
			items = notifySvc.UnreadItems()
			counts = notifySvc.UnreadCounts()
			total = notifySvc.TotalUnread()
		}
		if items == nil {
			items = []notify.UnreadItem{}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"items": items, "counts": counts, "total": total})
	})

	mux.HandleFunc("/api/unread/read", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			Menu string `json:"menu"`
			All  bool   `json:"all"`
			ID   int64  `json:"id"` // 单条已读（点击条目/逐条关闭）
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if notifySvc != nil {
			switch {
			case req.ID != 0:
				notifySvc.MarkItemRead(req.ID)
			case req.All || req.Menu == "":
				notifySvc.MarkAllRead()
			default:
				notifySvc.MarkMenuRead(req.Menu)
			}
		}
		w.Write([]byte(`{"success":true}`))
	})

	// 代理可本地写（应急改本地 SQLite，不推中心，无害）；管理端直写中心由 store.Use(sup) 保证。
	// 历史的只读守卫已移除——代理写本地 db.DB 不影响中心权威。
	return mux
}

//go:embed dashboard.html
var dashboardHTML string

// trimRAPINaming trims whitespace from the unified naming fields.
func trimRAPINaming(rapi *models.RAPI) {
	rapi.Vendor = strings.TrimSpace(rapi.Vendor)
	rapi.Series = strings.TrimSpace(rapi.Series)
	rapi.ModelName = strings.TrimSpace(rapi.ModelName)
	rapi.Version = strings.TrimSpace(rapi.Version)
	rapi.Suffix = strings.TrimSpace(rapi.Suffix)
	rapi.Notes = strings.TrimSpace(rapi.Notes)
	rapi.Model = strings.TrimSpace(rapi.Model)
}

// deriveRAPIAlias computes the display alias from the unified naming rule:
// lower(计算名 || 模型备注 || 上游模型名). 自然键身份模型下 alias 只是显示名
// （无唯一约束，不再追加 -2/-3 去重后缀）；端点身份冲突由 handler 层用
// RAPIModelExists 按 (base_url, model) 拒绝。Returns "" when nothing names
// the model.
func deriveRAPIAlias(rapi *models.RAPI) string {
	name := models.ComputeModelName(rapi.Vendor, rapi.Series, rapi.Version, rapi.Suffix, rapi.Notes, rapi.Model)
	return strings.ToLower(strings.TrimSpace(name))
}

// autoMapRAPItoLAPI checks if a newly created RAPI's naming identity
// (vendor, series, version, suffix) matches an existing LAPI. If so, the RAPI
// is automatically appended to that LAPI's routing chain.
// Fallback: when identity fields are all empty (e.g. batch-created RAPIs), match by alias.
func autoMapRAPItoLAPI(rapi *models.RAPI) {
	var lapi *models.LAPI
	var err error

	if rapi.Vendor == "" && rapi.Series == "" && rapi.Version == "" && rapi.Suffix == "" {
		// No identity fields — fall back to alias matching.
		if rapi.Alias == "" {
			return
		}
		lapi, err = store.A().GetLAPIByAlias(rapi.Alias)
		if err != nil || lapi == nil {
			return
		}
	} else {
		lapi, err = store.A().FindLAPIByModelIdentity(rapi.Vendor, rapi.Series, rapi.Version, rapi.Suffix)
		if err != nil || lapi == nil {
			return // No matching LAPI
		}
	}
	// Get current routing chain and check for duplicates
	existing, err := store.A().GetLAPIRAPIMapping(lapi.ID)
	if err != nil {
		return
	}
	for _, id := range existing {
		if id == rapi.ID {
			return // Already in the chain
		}
	}
	// Append to the end of the chain
	existing = append(existing, rapi.ID)
	if err := store.A().SetLAPIRAPIOrder(lapi.ID, existing); err != nil {
		logger.DefaultConsole().Error("service", "[AUTO-MAP] failed to add rapi to lapi",
			"rapi_id", rapi.ID, "lapi", lapi.Alias, "error", err.Error())
		return
	}
	logger.DefaultConsole().Info("service", "[AUTO-MAP] rapi auto-added to lapi",
		"rapi", rapi.Alias, "vendor", rapi.Vendor, "series", rapi.Series,
		"version", rapi.Version, "suffix", rapi.Suffix, "lapi", lapi.Alias)
}

func writeJSONError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	errResp, _ := json.Marshal(map[string]string{"error": err.Error()})
	w.WriteHeader(status)
	w.Write(errResp)
}

// platformKeyToken returns the first usable platform key token for probing,
// preferring the gateway's shared helper (which skips disabled/permanently-
// failed/expired keys) and falling back to the platform-level token.
func platformKeyToken(platformID int64, platformToken string) string {
	if proxyGateway != nil {
		return proxyGateway.FirstUsableKey(platformID, platformToken)
	}
	return platformToken
}

// restorePlatformAvailability revalidates every child RAPI in the scheduler after a
// key is added/replaced. Previously this also flipped platform.available=true, but
// that's now decoupled: platform.available only reflects base_url reachability
// (set by detect-formats / restore endpoint), not key presence.
func restorePlatformAvailability(platformID int64) {
	plat, err := store.A().GetPlatformByID(platformID)
	if err != nil || plat == nil {
		return
	}
	if proxyGateway != nil {
		rapis, _ := store.A().GetRAPIsByPlatform(platformID)
		for _, ra := range rapis {
			// Clear persisted unavailable state (written by the gateway when all keys
			// were dead) so the RAPI re-enters GetEnabledRAPIsForLAPI immediately.
			db.Get().SetRAPIUnavailableWithReason(ra.ID, true, "")
			proxyGateway.RevalidateRAPI(ra.ID)
		}
	}
	logger.DefaultConsole().Info("service", "[KEY] RAPIs revalidated after key change",
		"platform", plat.Name, "platform_id", platformID)
}

// handleKeyProbe tests a specific platform key by calling /v1/models (or /v1beta/models
// for Google native). On success: clears key failure_type. On failure: keeps failure_type=2.
func handleKeyProbe(w http.ResponseWriter, r *http.Request, platformID int64, keyIDStr string) {
	var keyID int64
	fmt.Sscanf(keyIDStr, "%d", &keyID)
	if keyID == 0 {
		writeJSONError(w, 400, fmt.Errorf("invalid key id"))
		return
	}

	platform, err := store.A().GetPlatformByID(platformID)
	if err != nil || platform == nil {
		writeJSONError(w, 404, fmt.Errorf("platform not found"))
		return
	}

	// Find the specific key
	keys, err := store.A().GetPlatformKeys(platformID)
	if err != nil {
		writeJSONError(w, 500, err)
		return
	}
	var targetKey *models.PlatformKey
	for i := range keys {
		if keys[i].ID == keyID {
			targetKey = &keys[i]
			break
		}
	}
	if targetKey == nil {
		writeJSONError(w, 404, fmt.Errorf("key not found"))
		return
	}
	if targetKey.Token == "" {
		writeJSONError(w, 400, fmt.Errorf("key token is empty"))
		return
	}

	// Build the list-models URL (same logic as /api/platforms/restore).
	client := &http.Client{Timeout: 15 * time.Second}
	httpReq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, "", nil)
	if apiformat.IsGoogleNativeBaseURL(platform.BaseURL) {
		modelsURL := apiformat.BuildGoogleListModelsURL(platform.BaseURL, targetKey.Token)
		httpReq.URL, _ = url.Parse(modelsURL)
		httpReq.Header.Set(apiformat.GoogleAPIKeyHeader, targetKey.Token)
	} else {
		modelsURL := apiformat.NormalizeModelsBaseURL(platform.BaseURL) + "/v1/models"
		httpReq.URL, _ = url.Parse(modelsURL)
		httpReq.Header.Set("Authorization", "Bearer "+targetKey.Token)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	logger.DefaultConsole().Info("service", "[PROBE] testing key",
		"key_id", keyID, "platform", platform.Name, "url", httpReq.URL.String())
	resp, err := client.Do(httpReq)
	if err != nil {
		writeJSONError(w, 502, fmt.Errorf("连接失败: %v", err))
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if len(respBody) > 2048 {
		respBody = respBody[:2048]
	}

	if resp.StatusCode != 200 {
		// Surface the upstream body so operators can see the real rejection reason
		// (e.g. a path-allowlist message from a serverless gateway) and fix the
		// platform baseURL accordingly — instead of a generic "平台返回 502".
		hint := strings.TrimSpace(string(respBody))
		if hint == "" {
			hint = http.StatusText(resp.StatusCode)
		}
		logger.DefaultConsole().Warn("service", "[PROBE] key probe rejected",
			"key_id", keyID, "platform", platform.Name, "url", httpReq.URL.String(),
			"status", resp.StatusCode, "body", hint)
		writeJSONError(w, 502, fmt.Errorf("密钥测试失败（HTTP %d）：%s", resp.StatusCode, hint))
		return
	}

	// Success: clear failure_type + notify scheduler
	if err := clearKeyFailureState(platformID, keyID); err != nil {
		writeJSONError(w, 500, err)
		return
	}
	logger.DefaultConsole().Info("service", "[PROBE] key restored, failure_type cleared", "key_id", keyID)
	w.Write([]byte(`{"success":true}`))
}

// clearKeyFailureState resets a key's failure state (DB + scheduler) and revives
// any child RAPIs that were persisted unavailable because all their keys were
// dead. Shared by handleKeyProbe and the key-model direct test (clear_on_success).
func clearKeyFailureState(platformID, keyID int64) error {
	if err := db.Get().ClearKeyFailure(keyID); err != nil {
		return err
	}
	if proxyGateway != nil {
		proxyGateway.Scheduler().MarkKeySuccess(keyID)
	}
	// A probed-good key means the platform can serve again: revive any child RAPIs
	// that were persisted unavailable because all their keys were dead.
	restorePlatformAvailability(platformID)
	logger.DefaultConsole().Info("service", "[KEY] failure state cleared", "key_id", keyID, "platform_id", platformID)
	return nil
}

// maskToken returns a masked preview of a token for safe display.
// Shows the first 8 and last 4 characters with "***" in between.
// Short tokens (≤12 chars) are fully masked.
func maskToken(token string) string {
	if len(token) <= 12 {
		return "***"
	}
	return token[:8] + "***" + token[len(token)-4:]
}

func parseTime(tStr string) (time.Time, error) {
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999-07:00",
	}
	var lastErr error
	for _, layout := range layouts {
		if t, err := time.Parse(layout, tStr); err == nil {
			return t, nil
		} else {
			lastErr = err
		}
	}
	return time.Time{}, lastErr
}
