package service

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"gateway/internal/config"
	"gateway/internal/db"
	"gateway/internal/gateway"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/notify"
)

type WindowsService struct {
	stopCh    chan struct{}
	doneCh    chan struct{}
	isRunning bool
	mu        sync.Mutex
}

var (
	instance       *WindowsService
	notifySvc      *notify.NotificationService
	proxyGateway   *gateway.ProxyGateway
	httpServer     *http.Server
	webServer      *http.Server
	logInstance    *logger.Logger
	sessionTracker *logger.SessionTracker
)

func New() *WindowsService {
	if instance == nil {
		instance = &WindowsService{
			stopCh: make(chan struct{}),
			doneCh: make(chan struct{}),
		}
	}
	return instance
}

func (s *WindowsService) Run() error {
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

	proxyGateway = gateway.NewProxyGateway(notifySvc, logInstance, sessionTracker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxyGateway.StartBackgroundRefresh(ctx, cfg.RefreshIntervalSec)

	proxyAddr := fmt.Sprintf("0.0.0.0:%d", cfg.ProxyPort)
	proxyMux := http.NewServeMux()
	proxyMux.HandleFunc("/v1/chat/completions", proxyGateway.HandleChatCompletions)
	// 添加状态页面和统计接口
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

	webAddr := fmt.Sprintf("0.0.0.0:%d", cfg.WebPort)
	webServer = &http.Server{
		Addr:    webAddr,
		Handler: createWebHandler(),
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

	close(s.doneCh)
	return nil
}

func (s *WindowsService) Stop() error {
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
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
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
			if err := db.Get().CreateRAPI(&rapi); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodPut:
			var rapi models.RAPI
			if err := json.NewDecoder(r.Body).Decode(&rapi); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			if err := db.Get().UpdateRAPI(&rapi); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			var rapiID int64
			fmt.Sscanf(id, "%d", &rapiID)
			if err := db.Get().DeleteRAPI(rapiID); err != nil {
				writeJSONError(w, 500, err)
				return
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
		// When disabling platform, also disable all its RAPIs
		if !req.Enabled {
			rapis, _ := db.Get().GetRAPIsByPlatform(req.ID)
			for _, r := range rapis {
				db.Get().SetRAPIEnabled(r.ID, false)
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
		w.Write([]byte(`{"success":true}`))
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
			if err := db.Get().CreatePlatform(&p); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			data, _ := json.Marshal(p)
			w.Write(data)

		case http.MethodPut:
			var p models.Platform
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			if err := db.Get().UpdatePlatform(&p); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			var pID int64
			fmt.Sscanf(id, "%d", &pID)
			if err := db.Get().DeletePlatform(pID); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))
		}
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
			if err := db.Get().CreateLAPI(&lapi); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true,"alias":"` + lapi.Alias + `"}`))

		case http.MethodPut:
			var lapi models.LAPI
			if err := json.NewDecoder(r.Body).Decode(&lapi); err != nil {
				http.Error(w, `{"error":"invalid json"}`, 400)
				return
			}
			if err := db.Get().UpdateLAPI(&lapi); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			var lapiID int64
			fmt.Sscanf(id, "%d", &lapiID)
			if err := db.Get().DeleteLAPI(lapiID); err != nil {
				writeJSONError(w, 500, err)
				return
			}
			w.Write([]byte(`{"success":true}`))
		}
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
					if lastUsedTime, err := time.Parse(time.RFC3339, s.LastUsed); err == nil && lastUsedTime.After(timeWindow) {
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

		// Send initial keepalive
		fmt.Fprintf(w, ": connected\n\n")
		flusher.Flush()

		for {
			select {
			case n, ok := <-ch:
				if !ok {
					return
				}
				data, _ := json.Marshal(n)
				fmt.Fprintf(w, "data: %s\n\n", data)
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
		data, _ := json.Marshal(sessions)
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
		data, _ := json.Marshal(requests)
		w.Write(data)
	})

	mux.HandleFunc("/api/logs/request/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if logInstance == nil || logInstance.Storage == nil {
			http.Error(w, `{"error":"logger not available"}`, 500)
			return
		}

		reqID := strings.TrimPrefix(r.URL.Path, "/api/logs/request/")
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

	return mux
}

//go:embed dashboard.html
var dashboardHTML string

func writeJSONError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	errResp, _ := json.Marshal(map[string]string{"error": err.Error()})
	w.WriteHeader(status)
	w.Write(errResp)
}

func init() {
	isService, _ := isWindowsService()
	if isService {
		svcMain()
	}
}

func isWindowsService() (bool, error) {
	return false, nil
}

func svcMain() {
	service := New()
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}

func IsWindowsService() bool {
	isSvc, _ := isWindowsService()
	return isSvc
}
