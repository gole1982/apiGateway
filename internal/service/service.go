package service

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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
	"gateway/internal/tools"
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

	return nil
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
		log.Printf("Database init failed: %v", err)
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		log.Printf("Config load failed: %v", err)
		return err
	}

	notifySvc = notify.NewNotificationService()

	logConfig := logger.DefaultLogConfig()
	logStorage := logger.NewLogStorage(db.Get())
	if err := logStorage.InitTables(); err != nil {
		log.Printf("Log storage init failed: %v", err)
	}
	logInstance = logger.NewLogger(logStorage, logConfig)
	logInstance.Start()

	sessionTracker = logger.NewSessionTracker(logInstance)

	schedulerCfg := scheduler.ConfigFromAppConfig(cfg.CooldownSec, cfg.MaxCooldownSec, cfg.RequestMaxWaitSec)
	proxyGateway = gateway.NewProxyGatewayWithConfig(notifySvc, logInstance, sessionTracker, cfg.DialTimeoutSec, cfg.ResponseTimeoutSec, schedulerCfg)

	// Enable agent tool-calling loop if configured
	if cfg.AgentEnabled {
		proxyGateway.SetAgentConfig(tools.AgentConfig{
			MaxIterations:  cfg.AgentMaxIterations,
			TotalTimeout:   time.Duration(cfg.AgentTimeoutSec) * time.Second,
			MaxResultBytes: 8192,
		})
		tools.SetShellWhitelist(cfg.AgentShellWhitelist)
		log.Printf("[INIT] Agent tool-calling enabled (max_iterations=%d, timeout=%ds, shell_whitelist=%v)", cfg.AgentMaxIterations, cfg.AgentTimeoutSec, cfg.AgentShellWhitelist)
	}

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
		lapis, _ := db.Get().GetLAPIs()
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
		log.Printf("proxy: unhandled %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"error":{"message":"Unknown endpoint: %s %s","type":"invalid_request","code":"unknown_endpoint"}}`, r.Method, r.URL.Path)
	})
	proxyMux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		rapis, _ := db.Get().GetRAPIs()
		lapis, _ := db.Get().GetLAPIs()
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
	}

	webAddr := fmt.Sprintf("127.0.0.1:%d", cfg.WebPort)
	webServer = &http.Server{
		Addr:        webAddr,
		Handler:     createWebHandler(),
		ReadTimeout: 30 * time.Second,
		IdleTimeout: 120 * time.Second,
	}

	errCh := make(chan error, 2)

	go func() {
		log.Printf("Proxy server starting on %s", proxyAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			if strings.Contains(err.Error(), "address already in use") {
				errCh <- fmt.Errorf("Proxy端口 %s 被占用，请先停止占用该端口的程序，或修改配置文件中的proxy_port", proxyAddr)
			} else {
				errCh <- fmt.Errorf("proxy server error: %v", err)
			}
		}
	}()

	go func() {
		log.Printf("Web dashboard starting on %s", webAddr)
		if err := webServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			if strings.Contains(err.Error(), "address already in use") {
				log.Printf("警告: Web端口 %s 被占用，Web管理界面不可用，但Proxy服务仍可正常运行", webAddr)
			} else {
				log.Printf("Web服务启动失败: %v", err)
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

	// Startup health recovery: probe every RAPI persisted as unavailable and
	// restore the ones that respond. Runs async so it never blocks serving.
	if cfg.RetryOnStartup {
		go func() {
			log.Printf("[STARTUP] retrying unhealthy models (concurrency=%d, timeout=%ds)...", cfg.RetryConcurrency, cfg.RetryTimeoutSec)
			rep := proxyGateway.RecoverUnhealthyRAPIs(context.Background(), cfg.RetryConcurrency, cfg.RetryTimeoutSec)
			log.Printf("[STARTUP] retry done: probed=%d recovered=%d failed=%d", rep.Probed, len(rep.Recovered), len(rep.Failed))
		}()
	}

	log.Println("Gateway service started successfully")

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

	// RAPI endpoints
	mux.HandleFunc("/api/rapis", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			rapis, err := db.Get().GetRAPIs()
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
			// Alias is used for LAPI matching — must be lowercase.
			// Model is the upstream model name — preserve original casing.
			rapi.Alias = strings.ToLower(strings.TrimSpace(rapi.Alias))
			rapi.Model = strings.TrimSpace(rapi.Model)
			if err := db.Get().CreateRAPI(&rapi); err != nil {
				log.Printf("[API] CreateRAPI failed: alias=%s platform_id=%d error=%v", rapi.Alias, rapi.PlatformID, err)
				writeJSONError(w, 500, err)
				return
			}
			log.Printf("[API] CreateRAPI success: id=%d alias=%s model=%s platform_id=%d", rapi.ID, rapi.Alias, rapi.Model, rapi.PlatformID)
			// Auto-map: if model identity matches a LAPI, add to its routing chain
			autoMapRAPItoLAPI(&rapi)
			w.Write([]byte(`{"success":true}`))

		case http.MethodPut:
			var rapi models.RAPI
			if err := json.NewDecoder(r.Body).Decode(&rapi); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			// Alias is used for LAPI matching — must be lowercase.
			// Model is the upstream model name — preserve original casing.
			rapi.Alias = strings.ToLower(strings.TrimSpace(rapi.Alias))
			rapi.Model = strings.TrimSpace(rapi.Model)
			if err := db.Get().UpdateRAPI(&rapi); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			force := r.URL.Query().Get("force") == "true"
			var rapiID int64
			fmt.Sscanf(id, "%d", &rapiID)

			// State-based delete: active RAPIs cannot be deleted
			rapiInfo, err := db.Get().GetRAPIByID(rapiID)
			if err != nil {
				writeJSONError(w, 404, fmt.Errorf("模型不存在"))
				return
			}
			if rapiInfo.Enabled && rapiInfo.Available {
				writeJSONError(w, 403, fmt.Errorf("生效状态的模型不能删除，请先禁用"))
				return
			}

			if force {
				// Cascade delete: remove LAPI references + metrics + RAPI
				if err := db.Get().DeleteRAPICascade(rapiID); err != nil {
					writeJSONError(w, 500, err)
					return
				}
				proxyGateway.InvalidateRAPI(rapiID)
			} else {
				// Non-force: only succeeds if no LAPI references exist
				if err := db.Get().DeleteRAPI(rapiID); err != nil {
					writeJSONError(w, 409, err)
					return
				}
				proxyGateway.InvalidateRAPI(rapiID)
			}
			w.Write([]byte(`{"success":true}`))
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
		if err := db.Get().SetPlatformEnabled(req.ID, req.Enabled); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		// Sync all RAPIs for this platform: disable+invalidate or enable+revalidate.
		rapis, _ := db.Get().GetRAPIsByPlatform(req.ID)
		for _, r := range rapis {
			if !req.Enabled {
				db.Get().SetRAPIEnabled(r.ID, false)
				proxyGateway.InvalidateRAPI(r.ID)
			} else {
				db.Get().SetRAPIEnabled(r.ID, true)
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
		if err := db.Get().SetRAPIEnabled(req.ID, req.Enabled); err != nil {
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
		if err := db.Get().SetRAPIUnavailableWithReason(req.ID, true, ""); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		proxyGateway.RevalidateRAPI(req.ID)
		log.Printf("[API] restoreRAPI id=%d", req.ID)
		w.Write([]byte(`{"success":true}`))
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
		log.Printf("[API] retry-unhealthy triggered")
		rep := proxyGateway.RecoverUnhealthyRAPIs(r.Context(), cfg.RetryConcurrency, cfg.RetryTimeoutSec)
		log.Printf("[API] retry-unhealthy done: probed=%d recovered=%d failed=%d", rep.Probed, len(rep.Recovered), len(rep.Failed))
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
		platform, err := db.Get().GetPlatformByID(req.ID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("platform not found"))
			return
		}

		// Build /v1/models URL (same logic as fetch-models)
		baseURL := platform.BaseURL
		for _, suffix := range []string{"/v1/chat/completions", "/v1/messages", "/v1/chat", "/v1"} {
			if len(baseURL) >= len(suffix) && baseURL[len(baseURL)-len(suffix):] == suffix {
				baseURL = baseURL[:len(baseURL)-len(suffix)]
				break
			}
		}
		for len(baseURL) > 0 && baseURL[len(baseURL)-1] == '/' {
			baseURL = baseURL[:len(baseURL)-1]
		}
		modelsURL := baseURL + "/v1/models"

		client := &http.Client{Timeout: 15 * time.Second}
		httpReq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, modelsURL, nil)
		fetchToken := platform.Token
		if keys, err := db.Get().GetPlatformKeys(platform.ID); err == nil && len(keys) > 0 {
			fetchToken = keys[0].Token
		}
		httpReq.Header.Set("Authorization", "Bearer "+fetchToken)
		httpReq.Header.Set("Content-Type", "application/json")

		log.Printf("[RESTORE] testing platform id=%d via %s", req.ID, modelsURL)
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

		// Success: restore platform and all child RAPIs
		if err := db.Get().SetPlatformAvailable(req.ID, true); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		rapis, _ := db.Get().GetRAPIsByPlatform(req.ID)
		for _, rapi := range rapis {
			db.Get().SetRAPIUnavailableWithReason(rapi.ID, true, "")
			proxyGateway.RevalidateRAPI(rapi.ID)
		}
		log.Printf("[RESTORE] platform id=%d restored, %d RAPIs revalidated", req.ID, len(rapis))
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
			rapi, err := db.Get().GetRAPIByID(rapiID)
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
			if err := db.Get().UpdateRAPIHeaders(rapiID, body.CustomHeaders); err != nil {
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
		rapi, err := db.Get().GetRAPIByID(req.ID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("RAPI not found"))
			return
		}
		log.Printf("[DETECT] starting format detection for rapi=%s model=%s base_url=%s", rapi.Alias, rapi.Model, rapi.BaseURL)
		results := apiformat.DetectFormats(r.Context(), rapi.BaseURL, rapi.Model, rapi.Token, nil)
		var supported []apiformat.APIFormat
		for _, res := range results {
			if res.Supported {
				supported = append(supported, res.Format)
			}
		}
		formatsJSON := apiformat.FormatsToJSON(supported)
		if err := db.Get().UpdateRAPIFormats(req.ID, formatsJSON); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		log.Printf("[DETECT] rapi=%s detected formats: %s", rapi.Alias, formatsJSON)
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
			platforms, err := db.Get().GetPlatforms()
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
			if err := db.Get().CreatePlatform(&p); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			// Webpage platforms: auto-create the canonical "webpage" RAPI (available=false until token pushed).
			if p.WebpageDomain != "" {
				if _, err := db.Get().EnsureWebpageRAPI(p.ID); err != nil {
					log.Printf("[API] EnsureWebpageRAPI failed platform_id=%d: %v", p.ID, err)
				}
			}
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
			if err := db.Get().UpdatePlatform(&p); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			// Ensure the webpage RAPI exists after update (idempotent).
			if p.WebpageDomain != "" {
				if _, err := db.Get().EnsureWebpageRAPI(p.ID); err != nil {
					log.Printf("[API] EnsureWebpageRAPI failed platform_id=%d: %v", p.ID, err)
				}
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			force := r.URL.Query().Get("force") == "true"
			var pID int64
			fmt.Sscanf(id, "%d", &pID)

			// State-based delete: active platforms cannot be deleted
			platform, err := db.Get().GetPlatformByID(pID)
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
				rapis, _ := db.Get().GetRAPIsByPlatform(pID)
				for _, rapi := range rapis {
					proxyGateway.InvalidateRAPI(rapi.ID)
				}
				if err := db.Get().DeletePlatformCascade(pID); err != nil {
					writeJSONError(w, 500, err)
					return
				}
			} else {
				// Non-force: only succeeds if no child RAPIs exist
				if err := db.Get().DeletePlatform(pID); err != nil {
					writeJSONError(w, 409, err)
					return
				}
			}
			w.Write([]byte(`{"success":true}`))
		}
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
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		platform, err := db.Get().GetPlatformByID(req.ID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("platform not found"))
			return
		}
		// Build /v1/models URL
		baseURL := platform.BaseURL
		for _, suffix := range []string{"/v1/chat/completions", "/v1/messages", "/v1/chat", "/v1"} {
			if len(baseURL) >= len(suffix) && baseURL[len(baseURL)-len(suffix):] == suffix {
				baseURL = baseURL[:len(baseURL)-len(suffix)]
				break
			}
		}
		for len(baseURL) > 0 && baseURL[len(baseURL)-1] == '/' {
			baseURL = baseURL[:len(baseURL)-1]
		}
		modelsURL := baseURL + "/v1/models"

		// Task 19: Use the first (index=0) PlatformKey token instead of platform.Token.
		client := &http.Client{Timeout: 15 * time.Second}
		httpReq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, modelsURL, nil)
		fetchToken := platform.Token
		if keys, err := db.Get().GetPlatformKeys(platform.ID); err == nil && len(keys) > 0 {
			fetchToken = keys[0].Token
		}
		httpReq.Header.Set("Authorization", "Bearer "+fetchToken)
		httpReq.Header.Set("Content-Type", "application/json")

		log.Printf("[FETCH] fetching models from %s", modelsURL)
		resp, err := client.Do(httpReq)
		if err != nil {
			writeJSONError(w, 502, fmt.Errorf("fetch models failed: %v", err))
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			writeJSONError(w, resp.StatusCode, fmt.Errorf("upstream returned %d: %s", resp.StatusCode, string(body)))
			return
		}

		var result struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			writeJSONError(w, 502, fmt.Errorf("parse models response failed: %v", err))
			return
		}

		modelNames := make([]string, 0, len(result.Data))
		for _, m := range result.Data {
			if m.ID != "" {
				modelNames = append(modelNames, m.ID)
			}
		}
		sort.Strings(modelNames)

		log.Printf("[FETCH] got %d models from platform %s", len(modelNames), platform.Name)
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
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if len(req.Models) == 0 {
			writeJSONError(w, 400, fmt.Errorf("no models specified"))
			return
		}
		platform, err := db.Get().GetPlatformByID(req.PlatformID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("platform not found"))
			return
		}

		// Platform-level format detection (once for all RAPIs)
		log.Printf("[BATCH] detecting formats for platform %s", platform.Name)
		detectResults := apiformat.DetectFormats(r.Context(), platform.BaseURL, req.Models[0], platform.Token, nil)
		var supportedFormats []apiformat.APIFormat
		for _, res := range detectResults {
			if res.Supported {
				supportedFormats = append(supportedFormats, res.Format)
			}
		}
		formatsJSON := apiformat.FormatsToJSON(supportedFormats)
		log.Printf("[BATCH] platform %s supports formats: %s", platform.Name, formatsJSON)

		// Create RAPIs
		created := make([]map[string]interface{}, 0)
		var errors []string
		for _, modelName := range req.Models {
			// Alias is lowercase for matching; Model preserves original casing from platform.
			modelName = strings.TrimSpace(modelName)
			rapi := models.RAPI{
				Alias:            strings.ToLower(modelName),
				Model:            modelName,
				PlatformID:       req.PlatformID,
				Enabled:          true,
				Available:        true,
				SupportedFormats: formatsJSON,
			}
			if err := db.Get().CreateRAPI(&rapi); err != nil {
				errors = append(errors, fmt.Sprintf("%s: %v", modelName, err))
				log.Printf("[BATCH] create RAPI failed: %s - %v", modelName, err)
			} else {
				autoMapRAPItoLAPI(&rapi)
				created = append(created, map[string]interface{}{
					"id":    rapi.ID,
					"alias": rapi.Alias,
					"model": rapi.Model,
				})
			}
		}

		log.Printf("[BATCH] created %d RAPIs for platform %s (formats: %s)", len(created), platform.Name, formatsJSON)
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

	// Platform-level format detection (apply to all existing RAPIs)
	mux.HandleFunc("/api/platforms/detect-formats", func(w http.ResponseWriter, r *http.Request) {
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
		platform, err := db.Get().GetPlatformByID(req.ID)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("platform not found"))
			return
		}
		// Get any RAPI under this platform for probing
		rapis, _ := db.Get().GetRAPIsByPlatform(req.ID)
		if len(rapis) == 0 {
			writeJSONError(w, 400, fmt.Errorf("platform has no RAPIs"))
			return
		}
		testModel := rapis[0].Model
		log.Printf("[DETECT] platform-level format detection for %s using model %s", platform.Name, testModel)
		results := apiformat.DetectFormats(r.Context(), platform.BaseURL, testModel, platform.Token, nil)
		var supported []apiformat.APIFormat
		for _, res := range results {
			if res.Supported {
				supported = append(supported, res.Format)
			}
		}
		formatsJSON := apiformat.FormatsToJSON(supported)
		// Apply to all RAPIs under this platform
		for _, rapi := range rapis {
			db.Get().UpdateRAPIFormats(rapi.ID, formatsJSON)
		}
		log.Printf("[DETECT] platform %s formats: %s (applied to %d RAPIs)", platform.Name, formatsJSON, len(rapis))
		resp, _ := json.Marshal(map[string]interface{}{
			"success": true,
			"formats": supported,
			"results": results,
			"updated": len(rapis),
		})
		w.Write(resp)
	})

	// Platform Keys CRUD endpoint: /api/platforms/{id}/keys
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

		switch r.Method {
		case http.MethodGet:
			keys, err := db.Get().GetPlatformKeys(platformID)
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
			if err := db.Get().AddPlatformKey(&k); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			data, _ := json.Marshal(k)
			w.Write(data)

		case http.MethodPut:
			// Replace entire key list.
			var keys []models.PlatformKey
			if err := json.NewDecoder(r.Body).Decode(&keys); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			if err := db.Get().SetPlatformKeys(platformID, keys); err != nil {
				writeJSONError(w, 500, err)
				return
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
			if err := db.Get().DeletePlatformKey(kid); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

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
		if err := db.Get().SetPlatformSortOrder(ids); err != nil {
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
		if err := db.Get().SetRAPISortOrder(ids); err != nil {
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
			lapis, err := db.Get().GetLAPIs()
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
			if err := db.Get().CreateLAPI(&lapi); err != nil {
				log.Printf("[API] CreateLAPI failed: alias=%s error=%v", lapi.Alias, err)
				writeJSONError(w, 500, err)
				return
			}
			log.Printf("[API] CreateLAPI success: id=%d alias=%s", lapi.ID, lapi.Alias)
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
			if err := db.Get().UpdateLAPI(&lapi); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			var lapiID int64
			fmt.Sscanf(id, "%d", &lapiID)

			// State-based delete: active LAPIs cannot be deleted
			lapiInfo, err := db.Get().GetLAPIByID(lapiID)
			if err != nil {
				writeJSONError(w, 404, fmt.Errorf("路由不存在"))
				return
			}
			if lapiInfo.Enabled {
				writeJSONError(w, 403, fmt.Errorf("生效状态的路由不能删除，请先禁用"))
				return
			}

			if err := db.Get().DeleteLAPI(lapiID); err != nil {
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
		if err := db.Get().SetLAPIEnabled(req.ID, req.Enabled); err != nil {
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
				rapis, err := db.Get().GetRAPIsForLAPI(lapiID)
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
				if err := db.Get().SetLAPIRAPIOrder(lapiID, rapiIDs); err != nil {
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
			rapis, err := db.Get().GetRAPIsForLAPIWithStats(stats[i].LapiID)
			if err == nil {
				stats[i].RAPIs = rapis
			}
		}

		data, _ := json.Marshal(stats)
		w.Write(data)
	})

	// Dashboard aggregated health endpoint
	mux.HandleFunc("/api/dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		rapis, _ := db.Get().GetRAPIs()
		lapis, _ := db.Get().GetLAPIs()
		platforms, _ := db.Get().GetPlatforms()
		rapiStats, _ := db.Get().GetRAPIStats()

		// Build stat lookup by rapi_id
		statByID := make(map[int64]db.RAPIStat)
		for _, s := range rapiStats {
			statByID[s.RapiID] = s
		}

		// Aggregate totals
		totalReq := 0
		totalSuccess := 0
		totalLatencyMs := int64(0)
		totalFail429 := 0
		totalFail401 := 0
		totalFail500 := 0
		for _, s := range rapiStats {
			totalReq += s.TotalRequests
			totalSuccess += s.SuccessRequests
			totalLatencyMs += int64(s.AvgLatencyMs * float64(s.TotalRequests))
			totalFail429 += s.Fail429
			totalFail401 += s.Fail401
			totalFail500 += s.Fail500
		}
		var overallSuccessRate float64
		var avgLatencyMs float64
		if totalReq > 0 {
			overallSuccessRate = float64(totalSuccess) / float64(totalReq) * 100
			avgLatencyMs = float64(totalLatencyMs) / float64(totalReq)
		}

		// Disabled platforms
		type platformSummary struct {
			ID      int64  `json:"id"`
			Name    string `json:"name"`
			Enabled bool   `json:"enabled"`
		}
		platSummaries := make([]platformSummary, 0, len(platforms))
		for _, p := range platforms {
			platSummaries = append(platSummaries, platformSummary{
				ID: p.ID, Name: p.Name, Enabled: p.Enabled,
			})
		}

		// Per-model health rows (only models with any activity or issues)
		type modelHealth struct {
			ID           int64   `json:"id"`
			Alias        string  `json:"alias"`
			PlatformName string  `json:"platform_name"`
			Enabled      bool    `json:"enabled"`
			Available    bool    `json:"available"`
			UnavailReason string `json:"unavail_reason,omitempty"`
			TotalReq     int     `json:"total_req"`
			SuccessRate  float64 `json:"success_rate"`
			AvgLatencyMs float64 `json:"avg_latency_ms"`
			Fail429      int     `json:"fail_429"`
			Fail401      int     `json:"fail_401"`
			Fail500      int     `json:"fail_500"`
			LastUsed     string  `json:"last_used"`
		}
		// Build platform name lookup
		platName := make(map[int64]string)
		for _, p := range platforms {
			platName[p.ID] = p.Name
		}
		models := make([]modelHealth, 0, len(rapis))
		for _, ra := range rapis {
			s := statByID[ra.ID]
			models = append(models, modelHealth{
				ID:            ra.ID,
				Alias:         ra.Alias,
				PlatformName:  platName[ra.PlatformID],
				Enabled:       ra.Enabled,
				Available:     ra.Available,
				UnavailReason: ra.UnavailableReason,
				TotalReq:      s.TotalRequests,
				SuccessRate:   s.SuccessRate,
				AvgLatencyMs:  s.AvgLatencyMs,
				Fail429:       s.Fail429,
				Fail401:       s.Fail401,
				Fail500:       s.Fail500,
				LastUsed:      s.LastUsed,
			})
		}

		resp := map[string]interface{}{
			"total_requests":      totalReq,
			"total_success":       totalSuccess,
			"overall_success_rate": overallSuccessRate,
			"avg_latency_ms":      avgLatencyMs,
			"fail_429":            totalFail429,
			"fail_401":            totalFail401,
			"fail_500":            totalFail500,
			"rapi_count":          len(rapis),
			"lapi_count":          len(lapis),
			"platform_count":      len(platforms),
			"platforms":           platSummaries,
			"models":              models,
		}
		data, _ := json.Marshal(resp)
		w.Write(data)
	})

	// Insights endpoint — three-layer analysis: health, efficiency, capacity
	mux.HandleFunc("/api/insights", handleInsights)

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
		rapis, _ := db.Get().GetRAPIs()
		lapis, _ := db.Get().GetLAPIs()
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
			rapiIds, _ := db.Get().GetLAPIRAPIMapping(u.ID)
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
		fmt.Fprintf(w, `{"proxy_port":%d,"web_port":%d,"rapi_count":%d,"lapi_count":%d,"total_requests":%d,"rapi_requested":%d,"lapi_requested":%d,"rapi_stats":{"requested_count":%d,"configured_count":%d},"lapi_stats":{"requested_count":%d,"configured_count":%d},"history":[`, cfg.ProxyPort, cfg.WebPort, len(rapis), len(lapis), totalReq, rapiRequested, lapiRequested, rapiRequested, len(rapis), lapiRequested, len(lapis))

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

	// Browser-push token endpoint — POST /api/token/push
	//
	// Allows a browser extension or local userscript to push a freshly-captured
	// token/cookie to the gateway after the user logs in manually. The gateway
	// immediately writes the new token to platform_keys (key_index=0) so that
	// the next upstream request picks it up automatically — zero manual config.
	//
	// Request body (JSON):
	//   { "platform_id": 3, "token": "sk-xxx..." }
	//
	// Optional per-platform push secret (set PushSecret on the Platform):
	//   Supply it either as the "secret" body field or the X-Push-Secret header.
	//   When PushSecret is empty (default) any local caller may push without auth.
	//
	// Response:
	//   200 {"success":true,"platform":"<name>"}
	//   400 {"error":"..."} — bad request / auth failure
	//   404 {"error":"platform not found"}
	//   500 {"error":"..."}
	mux.HandleFunc("/api/token/push", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}

		var req struct {
			// PlatformID is the numeric platform ID (legacy / manual config).
			PlatformID     int64       `json:"platform_id"`
			// Domain is the webpage_domain string (e.g. "abc.ai"). When set, the gateway
			// looks up the platform by domain so extensions never need to know the numeric ID.
			// If both are provided, platform_id takes precedence.
			Domain         string      `json:"domain"`
			Token          string      `json:"token"`
			Secret         string      `json:"secret"`
			SessionHeaders string      `json:"session_headers"`
			Reusable       *bool       `json:"reusable"`
			Reasons        []string    `json:"reasons"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		// token is required UNLESS this is a probe-only push (reusable=false with reasons,
		// used by the browser extension to report platform incompatibility without a token).
		isProbeOnly := req.Token == "" && req.Reusable != nil && !*req.Reusable && len(req.Reasons) > 0
		if req.Token == "" && !isProbeOnly {
			http.Error(w, `{"error":"token is required"}`, 400)
			return
		}

		// Resolve platform: by ID first, then by webpage domain.
		var platform *models.Platform
		var resolveErr error
		if req.PlatformID != 0 {
			platform, resolveErr = db.Get().GetPlatformByID(req.PlatformID)
		} else if req.Domain != "" {
			platform, resolveErr = db.Get().GetPlatformByWebpageDomain(req.Domain)
		} else {
			http.Error(w, `{"error":"platform_id or domain is required"}`, 400)
			return
		}
		if resolveErr != nil {
			writeJSONError(w, 404, fmt.Errorf("platform not found"))
			return
		}
		// Use the resolved platform's ID for all subsequent operations.
		req.PlatformID = platform.ID

		// Check push secret when the platform has one configured.
		if platform.PushSecret != "" {
			supplied := req.Secret
			if supplied == "" {
				supplied = r.Header.Get("X-Push-Secret")
			}
			if supplied != platform.PushSecret {
				http.Error(w, `{"error":"invalid push secret"}`, 400)
				return
			}
		}

		// Convert reusable bool pointer to int status (-1=unknown, 0=false, 1=true)
		reusableStatus := -1
		if req.Reusable != nil {
			if *req.Reusable {
				reusableStatus = 1
			} else {
				reusableStatus = 0
			}
		}
		reasonsJSON := "[]"
		if len(req.Reasons) > 0 {
			if b, jErr := json.Marshal(req.Reasons); jErr == nil {
				reasonsJSON = string(b)
			}
		}

		if isProbeOnly {
			// Probe-only: only persist reusability verdict, do not touch token or RAPI state.
			if dbErr := db.Get().UpdatePlatformReusability(req.PlatformID, reusableStatus, reasonsJSON); dbErr != nil {
				log.Printf("[PUSH] probe UpdatePlatformReusability failed platform_id=%d: %v", req.PlatformID, dbErr)
			}
		} else {
			if err := db.Get().UpdatePlatformTokenWithHeaders(req.PlatformID, req.Token, req.SessionHeaders, reusableStatus, reasonsJSON); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			// For webpage platforms: mark the webpage RAPI as ready (available=true).
			if platform.WebpageDomain != "" {
				if dbErr := db.Get().SetWebpageRAPIReady(req.PlatformID, true); dbErr != nil {
					log.Printf("[PUSH] SetWebpageRAPIReady failed platform_id=%d: %v", req.PlatformID, dbErr)
				}
				if proxyGateway != nil {
					rapis, _ := db.Get().GetRAPIsByPlatform(req.PlatformID)
					for _, ra := range rapis {
						if ra.Alias == "webpage" {
							proxyGateway.RevalidateRAPI(ra.ID)
						}
					}
				}
			}
		}

		reusableLabel := "unknown"
		if reusableStatus == 1 {
			reusableLabel = "reusable"
		} else if reusableStatus == 0 {
			reusableLabel = "not-reusable"
		}
		log.Printf("[PUSH] platform_id=%d name=%s probe=%v reusable=%s", req.PlatformID, platform.Name, isProbeOnly, reusableLabel)
		resp, _ := json.Marshal(map[string]interface{}{
			"success":  true,
			"platform": platform.Name,
		})
		w.Write(resp)
	})

	// Token status endpoint — GET /api/platforms/token-status/{id}
	// Returns has_token, valid, seconds_since_push, and reusability info for a platform.
	mux.HandleFunc("/api/platforms/token-status/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		idStr := strings.TrimPrefix(r.URL.Path, "/api/platforms/token-status/")
		var platformID int64
		fmt.Sscanf(idStr, "%d", &platformID)
		if platformID == 0 {
			http.Error(w, `{"error":"invalid platform id"}`, 400)
			return
		}
		status, err := db.Get().GetPlatformTokenStatus(platformID)
		if err != nil {
			writeJSONError(w, 500, err)
			return
		}
		data, _ := json.Marshal(status)
		w.Write(data)
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
		platform, err := db.Get().GetPlatformByID(platformID)
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

	// --- Tools (Agent Tool-Calling) CRUD ---

	mux.HandleFunc("/api/tools", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			toolsList, err := db.Get().GetAllTools()
			if err != nil {
				writeJSONError(w, 500, err)
				return
			}
			if toolsList == nil {
				toolsList = make([]db.ToolRecord, 0)
			}
			data, _ := json.Marshal(map[string]interface{}{"tools": toolsList})
			w.Write(data)

		case http.MethodPost:
			var t db.ToolRecord
			if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
				writeJSONError(w, 400, fmt.Errorf("invalid JSON: %v", err))
				return
			}
			if t.Name == "" {
				writeJSONError(w, 400, fmt.Errorf("name is required"))
				return
			}
			if t.ExecutorType == "" {
				t.ExecutorType = "http"
			}
			if t.TimeoutMs <= 0 {
				t.TimeoutMs = 30000
			}
			if t.Parameters == "" {
				t.Parameters = "{}"
			}
			if t.ExecutorConfig == "" {
				t.ExecutorConfig = "{}"
			}
			if err := db.Get().CreateTool(&t); err != nil {
				if strings.Contains(err.Error(), "UNIQUE") {
					writeJSONError(w, 409, fmt.Errorf("tool name already exists: %s", t.Name))
					return
				}
				writeJSONError(w, 500, err)
				return
			}
			tools.GetRegistry().Reload()
			w.WriteHeader(201)
			data, _ := json.Marshal(t)
			w.Write(data)

		default:
			http.Error(w, `{"error":"method not allowed"}`, 405)
		}
	})

	mux.HandleFunc("/api/tools/toggle", func(w http.ResponseWriter, r *http.Request) {
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
			writeJSONError(w, 400, err)
			return
		}
		if err := db.Get().ToggleTool(req.ID, req.Enabled); err != nil {
			writeJSONError(w, 500, err)
			return
		}
		tools.GetRegistry().Reload()
		w.Write([]byte(`{"ok":true}`))
	})

	mux.HandleFunc("/api/tools/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		idStr := strings.TrimPrefix(r.URL.Path, "/api/tools/")
		var id int64
		fmt.Sscanf(idStr, "%d", &id)
		if id == 0 {
			writeJSONError(w, 400, fmt.Errorf("invalid tool id"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			t, err := db.Get().GetToolByID(id)
			if err != nil {
				writeJSONError(w, 404, fmt.Errorf("tool not found"))
				return
			}
			data, _ := json.Marshal(t)
			w.Write(data)

		case http.MethodPut:
			var t db.ToolRecord
			if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
				writeJSONError(w, 400, err)
				return
			}
			t.ID = id
			if t.TimeoutMs <= 0 {
				t.TimeoutMs = 30000
			}
			if err := db.Get().UpdateTool(&t); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			tools.GetRegistry().Reload()
			w.Write([]byte(`{"ok":true}`))

		case http.MethodDelete:
			if err := db.Get().DeleteTool(id); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			tools.GetRegistry().Reload()
			w.Write([]byte(`{"ok":true}`))

		default:
			http.Error(w, `{"error":"method not allowed"}`, 405)
		}
	})

	// POST /api/tools/test — execute a tool with given arguments and return result
	mux.HandleFunc("/api/tools/test", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		var req struct {
			ToolName  string                 `json:"tool_name"`
			Arguments map[string]interface{} `json:"arguments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, 400, fmt.Errorf("invalid JSON: %v", err))
			return
		}
		if req.ToolName == "" {
			writeJSONError(w, 400, fmt.Errorf("tool_name is required"))
			return
		}
		tool, err := tools.GetRegistry().Get(req.ToolName)
		if err != nil {
			writeJSONError(w, 404, fmt.Errorf("tool not found: %s", req.ToolName))
			return
		}
		executor, err := tools.NewExecutor(tool)
		if err != nil {
			writeJSONError(w, 400, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(tool.TimeoutMs)*time.Millisecond)
		defer cancel()
		start := time.Now()
		result, execErr := executor.Execute(ctx, tool, req.Arguments)
		durationMs := int(time.Since(start).Milliseconds())
		resp := map[string]interface{}{
			"success":     execErr == nil,
			"result":      result,
			"duration_ms": durationMs,
		}
		if execErr != nil {
			resp["error"] = execErr.Error()
		}
		data, _ := json.Marshal(resp)
		w.Write(data)
	})

	// GET /api/tools/logs — recent tool execution logs
	mux.HandleFunc("/api/tools/logs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, 405)
			return
		}
		logs, err := db.Get().GetToolExecutionLogs(50)
		if err != nil {
			writeJSONError(w, 500, err)
			return
		}
		if logs == nil {
			logs = make([]db.ToolLogRecord, 0)
		}
		data, _ := json.Marshal(map[string]interface{}{"logs": logs})
		w.Write(data)
	})

	return mux
}

//go:embed dashboard.html
var dashboardHTML string

// autoMapRAPItoLAPI checks if a newly created RAPI's model identity (series, model_name, version)
// matches an existing LAPI. If so, the RAPI is automatically appended to that LAPI's routing chain.
func autoMapRAPItoLAPI(rapi *models.RAPI) {
	if rapi.Series == "" && rapi.ModelName == "" && rapi.Version == "" {
		return // No identity to match
	}
	lapi, err := db.Get().FindLAPIByModelIdentity(rapi.Series, rapi.ModelName, rapi.Version)
	if err != nil || lapi == nil {
		return // No matching LAPI
	}
	// Get current routing chain and check for duplicates
	existing, err := db.Get().GetLAPIRAPIMapping(lapi.ID)
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
	if err := db.Get().SetLAPIRAPIOrder(lapi.ID, existing); err != nil {
		log.Printf("[AUTO-MAP] failed to add rapi=%d to lapi=%s: %v", rapi.ID, lapi.Alias, err)
		return
	}
	log.Printf("[AUTO-MAP] rapi=%s (series=%s name=%s ver=%s) auto-added to lapi=%s",
		rapi.Alias, rapi.Series, rapi.ModelName, rapi.Version, lapi.Alias)
}

func writeJSONError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	errResp, _ := json.Marshal(map[string]string{"error": err.Error()})
	w.WriteHeader(status)
	w.Write(errResp)
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
