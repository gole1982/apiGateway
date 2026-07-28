package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"gateway/internal/apiformat"
	"gateway/internal/db"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/notify"
	"gateway/internal/scheduler"
	"gateway/internal/tools"
)

type ProxyGateway struct {
	db             *db.DB
	httpClient     *http.Client
	notifyService  *notify.NotificationService
	log            *logger.Logger
	sessionTracker *logger.SessionTracker
	scheduler      *scheduler.Manager
	// responseTimeout is applied to non-streaming upstream requests only.
	// Streaming responses intentionally have no body deadline (client ctx is the bound).
	responseTimeout time.Duration
	// agentConfig controls the tool-calling agent loop (nil = agent disabled).
	agentConfig *tools.AgentConfig
}

// NewProxyGateway creates a gateway with default timeouts.
func NewProxyGateway(notifyService *notify.NotificationService, log *logger.Logger, sessionTracker *logger.SessionTracker) *ProxyGateway {
	return NewProxyGatewayWithConfig(notifyService, log, sessionTracker, 30, 300, scheduler.DefaultConfig())
}

// NewProxyGatewayWithConfig creates a gateway with explicit timeout and scheduler config.
// dialTimeoutSec:     TCP connect + TLS handshake deadline (applied via net.Dialer and
//
//	TLSHandshakeTimeout).
//
// responseTimeoutSec: deadline from the moment the request is sent until the response
//
//	headers are received. For streaming responses the body is read
//	indefinitely after headers arrive — we must NOT put a global
//	http.Client.Timeout on the body read or it will kill in-flight streams.
//	The per-request context (passed from the HTTP handler) already carries
//	the client's connection lifetime as an implicit deadline.
func NewProxyGatewayWithConfig(
	notifyService *notify.NotificationService,
	log *logger.Logger,
	sessionTracker *logger.SessionTracker,
	dialTimeoutSec int,
	responseTimeoutSec int,
	schedulerCfg scheduler.Config,
) *ProxyGateway {
	dialTimeout := time.Duration(dialTimeoutSec) * time.Second

	transport := &http.Transport{
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: dialTimeout,
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	respTimeout := time.Duration(responseTimeoutSec) * time.Second
	if respTimeout <= 0 {
		respTimeout = 300 * time.Second
	}

	return &ProxyGateway{
		db: db.Get(),
		// http.Client.Timeout = 0: no global deadline.
		// Dial+TLS are bounded by the Transport's DialContext and TLSHandshakeTimeout.
		// Streaming body reads are intentionally unlimited — the client connection
		// context (r.Context()) acts as the natural upper bound.
		// Non-streaming requests use responseTimeout via a per-request context.
		httpClient: &http.Client{
			Timeout:   0,
			Transport: transport,
		},
		notifyService:   notifyService,
		log:             log,
		sessionTracker:  sessionTracker,
		scheduler:       scheduler.NewManager(schedulerCfg),
		responseTimeout: respTimeout,
	}
}

// InvalidateRAPI immediately marks a RAPI as unavailable in the scheduler's in-memory
// state. Call this from management API handlers after writing enabled=false or
// available=false to the DB so that in-flight requests skip the RAPI right away.
func (g *ProxyGateway) InvalidateRAPI(rapiID int64) {
	g.scheduler.InvalidateRAPI(rapiID)
}

// RevalidateRAPI clears the invalidated flag for a RAPI so PickAvailable will
// consider it again. Call this from management API handlers after writing
// enabled=true or available=true to the DB.
func (g *ProxyGateway) RevalidateRAPI(rapiID int64) {
	g.scheduler.RevalidateRAPI(rapiID)
}

// RetryItem records the outcome of probing one previously-unavailable RAPI.
type RetryItem struct {
	ID         int64  `json:"id"`
	Alias      string `json:"alias"`
	Platform   string `json:"platform"`
	Reason     string `json:"reason,omitempty"` // for failed items: why it stayed unavailable
	DurationMs int64  `json:"duration_ms"`
}

// RetryReport is the aggregate result of a startup / on-demand health pass.
type RetryReport struct {
	Probed    int         `json:"probed"`
	Recovered []RetryItem `json:"recovered"`
	Failed    []RetryItem `json:"failed"`
}

// RecoverUnhealthyRAPIs probes every RAPI persisted as unavailable and, for
// each that responds, clears the unavailable state and revalidates it in the
// scheduler. Probes run concurrently up to `concurrency`. perProbeTimeout
// bounds each individual probe. Platform-level failures (platform.available=0)
// are recovered first via /v1/models; only their still-unavailable children are
// probed individually afterwards.
//
// The returned report is safe to surface to the dashboard.
func (g *ProxyGateway) RecoverUnhealthyRAPIs(ctx context.Context, concurrency, perProbeTimeoutSec int) *RetryReport {
	if concurrency < 1 {
		concurrency = 1
	}
	perProbeTimeout := time.Duration(perProbeTimeoutSec) * time.Second
	if perProbeTimeout <= 0 {
		perProbeTimeout = 15 * time.Second
	}

	report := &RetryReport{}
	probeClient := &http.Client{Timeout: perProbeTimeout}

	// 1) Recover platform-level failures first (mirror /api/platforms/restore).
	platforms, _ := g.db.GetPlatforms()
	var failedPlatformIDs []int64
	for _, p := range platforms {
		if !p.Enabled || p.Available {
			continue
		}
		start := time.Now()
		if ok, reason := g.probePlatform(ctx, p, probeClient); ok {
			g.db.SetPlatformAvailable(p.ID, true)
			log.Printf("[RECOVER] platform %s(id=%d) restored (%dms)", p.Name, p.ID, time.Since(start).Milliseconds())
		} else {
			failedPlatformIDs = append(failedPlatformIDs, p.ID)
			log.Printf("[RECOVER] platform %s(id=%d) still failing: %s", p.Name, p.ID, reason)
		}
	}
	failedPlatform := make(map[int64]bool, len(failedPlatformIDs))
	for _, id := range failedPlatformIDs {
		failedPlatform[id] = true
	}

	// 2) Collect RAPIs still unavailable.
	rapis, _ := g.db.GetRAPIs()
	var unhealthy []models.RAPIWithPlatform
	for _, r := range rapis {
		if r.Enabled && !r.Available && !r.IsWebpage {
			unhealthy = append(unhealthy, r)
		}
	}
	report.Probed = len(unhealthy)
	if len(unhealthy) == 0 {
		return report
	}

	// 3) Probe concurrently.
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, rapi := range unhealthy {
		wg.Add(1)
		go func(r models.RAPIWithPlatform) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			item := RetryItem{
				ID:       r.ID,
				Alias:    r.Alias,
				Platform: r.PlatformName,
			}
			start := time.Now()

			// If the owning platform itself is still failing, skip per-RAPI probe
			// — its unavailable state reflects the platform, not the model.
			if failedPlatform[r.PlatformID] {
				item.Reason = "platform unavailable"
				item.DurationMs = time.Since(start).Milliseconds()
				mu.Lock()
				report.Failed = append(report.Failed, item)
				mu.Unlock()
				return
			}

			probeCtx, cancel := context.WithTimeout(ctx, perProbeTimeout)
			defer cancel()

			results := apiformat.DetectFormats(probeCtx, r.BaseURL, r.Model, r.Token, probeClient)
			anySupported := false
			var supported []apiformat.APIFormat
			for _, res := range results {
				if res.Supported {
					anySupported = true
					supported = append(supported, res.Format)
				}
			}
			item.DurationMs = time.Since(start).Milliseconds()

			if !anySupported {
				item.Reason = "no supported format responded"
				mu.Lock()
				report.Failed = append(report.Failed, item)
				mu.Unlock()
				log.Printf("[RECOVER] rapi %s(id=%d) still failing (%dms)", r.Alias, r.ID, item.DurationMs)
				return
			}

			// Recovered: clear unavailable state, refresh formats, revalidate.
			g.db.SetRAPIUnavailableWithReason(r.ID, true, "")
			if len(supported) > 0 {
				g.db.UpdateRAPIFormats(r.ID, apiformat.FormatsToJSON(supported))
			}
			g.RevalidateRAPI(r.ID)
			mu.Lock()
			report.Recovered = append(report.Recovered, item)
			mu.Unlock()
			log.Printf("[RECOVER] rapi %s(id=%d) restored (%dms)", r.Alias, r.ID, item.DurationMs)
		}(rapi)
	}
	wg.Wait()
	return report
}

// probePlatform mirrors the connectivity check of /api/platforms/restore: it
// issues GET {baseURL}/v1/models with the platform token (or its first key) and
// reports success on HTTP 200. Returns (ok, reason).
func (g *ProxyGateway) probePlatform(ctx context.Context, p models.Platform, client *http.Client) (bool, string) {
	baseURL := p.BaseURL
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

	token := p.Token
	if keys, err := g.db.GetPlatformKeys(p.ID); err == nil && len(keys) > 0 {
		token = keys[0].Token
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, modelsURL, nil)
	if err != nil {
		return false, err.Error()
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return false, fmt.Sprintf("platform returned %d", resp.StatusCode)
	}
	return true, ""
}


// Scheduler returns the underlying scheduler Manager for read-only inspection.
func (g *ProxyGateway) Scheduler() *scheduler.Manager {
	return g.scheduler
}

// SetAgentConfig enables the agent tool-calling loop with the given configuration.
func (g *ProxyGateway) SetAgentConfig(cfg tools.AgentConfig) {
	g.agentConfig = &cfg
}

func (g *ProxyGateway) HandleChatCompletions(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	requestID := ""
	sessionID := ""
	fallbackUsed := false
	clientStatusCode := http.StatusOK

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Detect client format from URL path.
	clientFormat := apiformat.DetectFormatFromPath(r.URL.Path)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request", http.StatusBadRequest)
		return
	}

	if g.sessionTracker != nil {
		sessionID = g.sessionTracker.InjectSessionID(r)
	}

	// Bug 8.2: Generate requestID here and pass it into RecordRequestReceived.
	if g.log != nil {
		requestID = logger.NewRequestID()
		headers := make(map[string]string)
		for k, v := range r.Header {
			if len(v) > 0 {
				headers[k] = logger.SanitizeKey(v[0])
			}
		}
		g.log.RecordRequestReceived(requestID, sessionID, r.RemoteAddr, r.Method, r.URL.Path, headers, string(body))
	}

	// Bug 8.6: For Gemini, extract model from the URL path BEFORE any format conversion
	// so that we can use it during routing (not after canonicalization).
	var geminiURLModel string
	if clientFormat == apiformat.FormatGemini {
		geminiURLModel = apiformat.ExtractGeminiModel(r.URL.Path)
	}

	// Bug 8.1: Convert client body directly to OpenAI canonical format in one step.
	// (Previously was: client→OpenAI then OpenAI→target; now: client→OpenAI once, OpenAI→target once.)
	canonicalBody := body
	if clientFormat != apiformat.FormatOpenAI {
		var converted []byte
		converted, err = apiformat.ConvertRequest(body, clientFormat, apiformat.FormatOpenAI, geminiURLModel)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":{"message":"Failed to parse %s request: %s","type":"invalid_request"}}`, clientFormat, err.Error()), http.StatusBadRequest)
			if g.log != nil {
				g.log.RecordError(requestID, "Failed to parse request: "+err.Error(), "REQUEST_RECEIVED")
				g.log.RecordClientResponse(requestID, http.StatusBadRequest, int(time.Since(startTime).Milliseconds()), false)
			}
			return
		}
		canonicalBody = converted
	}

	var req models.ProxyRequest
	if err := json.Unmarshal(canonicalBody, &req); err != nil {
		http.Error(w, `{"error":{"message":"Invalid JSON","type":"invalid_request"}}`, http.StatusBadRequest)
		if g.log != nil {
			g.log.RecordError(requestID, "Invalid JSON: "+err.Error(), "REQUEST_RECEIVED")
			g.log.RecordClientResponse(requestID, http.StatusBadRequest, int(time.Since(startTime).Milliseconds()), false)
		}
		return
	}

	// Bug 8.6: Apply Gemini URL model to req.Model if not set in body.
	if clientFormat == apiformat.FormatGemini && req.Model == "" && geminiURLModel != "" {
		req.Model = geminiURLModel
		// Re-inject model into canonical body.
		canonicalBody, _ = apiformat.ConvertRequest(canonicalBody, apiformat.FormatOpenAI, apiformat.FormatOpenAI, req.Model)
	}

	// Detect stream from body or Gemini URL suffix.
	isStream := req.Stream
	if clientFormat == apiformat.FormatGemini && strings.HasSuffix(r.URL.Path, ":streamGenerateContent") {
		isStream = true
		// Gemini clients signal streaming via the URL suffix, not the JSON body. When we
		// forward to an OpenAI/Anthropic-format RAPI, the upstream needs "stream":true in
		// the body or it will return a single (non-SSE) JSON completion, which the gateway
		// then tries to stream back → corruption/hang. Inject it into the canonical body
		// so ConvertRequest propagates it to the target format.
		if !req.Stream {
			var cb map[string]interface{}
			if json.Unmarshal(canonicalBody, &cb) == nil {
				cb["stream"] = true
				if out, mErr := json.Marshal(cb); mErr == nil {
					canonicalBody = out
					req.Stream = true
				}
			}
		}
	}

	modelName := req.Model
	log.Printf("[REQ] model=%s stream=%v format=%s from=%s", modelName, isStream, clientFormat, r.RemoteAddr)
	lapi, err := g.db.GetLAPIByAlias(strings.ToLower(modelName))
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":{"message":"Unknown model: %s","type":"invalid_request"}}`, modelName), http.StatusUnauthorized)
		if g.log != nil {
			g.log.RecordRoutingDecision(requestID, modelName, nil)
			g.log.RecordError(requestID, "Unknown model: "+modelName, "ROUTING_DECISION")
			g.log.RecordClientResponse(requestID, http.StatusUnauthorized, int(time.Since(startTime).Milliseconds()), false)
		}
		return
	}

	if !lapi.Enabled {
		log.Printf("[ROUTE] model=%s is disabled", modelName)
		http.Error(w, fmt.Sprintf(`{"error":{"message":"Model %s is disabled","type":"service_unavailable"}}`, modelName), http.StatusServiceUnavailable)
		if g.log != nil {
			g.log.RecordRoutingDecision(requestID, lapi.Alias, nil)
			g.log.RecordError(requestID, "Model is disabled: "+modelName, "ROUTING_DECISION")
			g.log.RecordClientResponse(requestID, http.StatusServiceUnavailable, int(time.Since(startTime).Milliseconds()), false)
		}
		return
	}

	rapis, err := g.db.GetEnabledRAPIsForLAPI(lapi.ID)
	if err != nil || len(rapis) == 0 {
		log.Printf("[ROUTE] model=%s NO backends available", modelName)
		http.Error(w, `{"error":{"message":"No backends available for this model","type":"configuration_error"}}`, http.StatusServiceUnavailable)
		if g.log != nil {
			g.log.RecordRoutingDecision(requestID, lapi.Alias, nil)
			g.log.RecordError(requestID, "No backends available for model: "+modelName, "ROUTING_DECISION")
			g.log.RecordClientResponse(requestID, http.StatusServiceUnavailable, int(time.Since(startTime).Milliseconds()), false)
		}
		return
	}

	// Load PlatformKeys for all selected RAPIs.
	g.loadKeysForRAPIs(rapis)

	if g.log != nil {
		rapiAliases := make([]string, len(rapis))
		for i, rapi := range rapis {
			rapiAliases[i] = rapi.Alias
		}
		g.log.RecordRoutingDecision(requestID, lapi.Alias, rapiAliases)
	}

	// Agent mode: intercept requests with tools and handle the tool-call loop internally.
	if g.agentConfig != nil && !isStream && isAgentMode(canonicalBody) {
		g.handleAgentRequest(w, r, canonicalBody, &req, requestID, startTime)
		return
	}

	if isStream {
		fallbackUsed, clientStatusCode = g.handleStreamingRequest(w, r, body, canonicalBody, &req, lapi, rapis, requestID, sessionID, string(clientFormat))
	} else {
		fallbackUsed, clientStatusCode = g.handleNonStreamingRequest(w, r, body, canonicalBody, &req, lapi, rapis, requestID, sessionID, string(clientFormat))
	}

	g.db.RecordTrend(lapi.ID)

	if g.log != nil {
		latencyMS := int(time.Since(startTime).Milliseconds())
		g.log.RecordClientResponse(requestID, clientStatusCode, latencyMS, fallbackUsed)
	}
}

// loadKeysForRAPIs populates the Keys slice for each RAPI in the list.
func (g *ProxyGateway) loadKeysForRAPIs(rapis []models.RAPIWithPlatform) {
	// Batch by platform_id to avoid repeated DB calls.
	loaded := make(map[int64][]models.PlatformKey)
	for i := range rapis {
		pid := rapis[i].PlatformID
		if _, ok := loaded[pid]; !ok {
			keys, err := g.db.GetPlatformKeys(pid)
			if err != nil {
				keys = nil
			}
			loaded[pid] = keys
		}
		rapis[i].Keys = loaded[pid]
	}
}

// tryKeyForRAPI tries each PlatformKey for a given RAPI in sequence.
// Returns (resp, keyID, err). On 429/503 it marks the key failed and tries the next key.
// If all keys are cooling, it marks the RAPI failed and returns ErrAllKeysUnavailable.
func (g *ProxyGateway) tryKeyForRAPI(
	ctx context.Context,
	rapi models.RAPIWithPlatform,
	effectiveURL string,
	upstreamBody []byte,
	requestID string,
	retryCount int,
	targetFormat string,
) (*http.Response, int64, error) {
	keys := rapi.Keys
	// Fallback: if no platform_keys loaded, use legacy platform.Token as a synthetic key.
	if len(keys) == 0 {
		syntheticKey := models.PlatformKey{
			ID:         -1,
			PlatformID: rapi.PlatformID,
			KeyIndex:   0,
			Token:      rapi.Token,
			Enabled:    true,
		}
		keys = []models.PlatformKey{syntheticKey}
	}

	// Webpage RAPIs: use the captured session headers directly (full browser session replay).
	// The token is the Bearer token extracted from the browser's chat request; session_headers
	// carries the full set of headers the browser sent, allowing the gateway to impersonate
	// the browser session rather than just injecting Authorization.
	if rapi.IsWebpage {
		key, _, err := g.scheduler.PickAvailableKey(keys)
		if err != nil {
			return nil, 0, scheduler.ErrAllKeysUnavailable
		}
		resp, _, _, doErr := g.doWebpageRequest(ctx, rapi, effectiveURL, upstreamBody, key, requestID, retryCount)
		if doErr != nil {
			isTimeout := isTimeoutError(doErr)
			g.scheduler.MarkKeyFailure(key.ID, time.Time{}, doErr.Error(), isTimeout)
			return nil, 0, doErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			g.scheduler.MarkKeySuccess(key.ID)
			return resp, key.ID, nil
		}
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		log.Printf("[ERR]  webpage rapi=%s status=%d body=%s", rapi.Alias, resp.StatusCode, string(errBody))
		if g.log != nil {
			g.log.RecordError(requestID,
				fmt.Sprintf("webpage rapi=%s upstream %d: %s", rapi.Alias, resp.StatusCode, string(errBody)),
				"UPSTREAM_RESPONSE")
		}
		// On 401: session expired → mark RAPI unavailable so it shows "未就绪".
		if resp.StatusCode == http.StatusUnauthorized {
			reason := fmt.Sprintf("[会话过期] upstream 401: %s", string(errBody))
			g.db.SetRAPIUnavailableWithReason(rapi.ID, false, reason)
			g.scheduler.MarkKeyPlatformFailure(key.ID, "401 session expired")
			if g.notifyService != nil {
				g.notifyService.PublishAsync(
					fmt.Sprintf("网页平台 %s 会话已过期（401），请重新在浏览器开始对话", rapi.PlatformName),
					"网页会话过期",
				)
			}
		} else {
			retryAt := scheduler.RetryAt(resp.Header, time.Time{})
			g.scheduler.MarkKeyFailure(key.ID, retryAt, fmt.Sprintf("upstream %d: %s", resp.StatusCode, string(errBody)))
		}
		return nil, 0, fmt.Errorf("upstream %d", resp.StatusCode)
	}

	for {
		key, _, err := g.scheduler.PickAvailableKey(keys)
		if err != nil {
			// All keys for this RAPI are cooling — escalate to RAPI-level failure.
			return nil, 0, scheduler.ErrAllKeysUnavailable
		}

		token := key.Token

		resp, _, _, doErr := g.doUpstreamRequest(ctx, rapi, effectiveURL, upstreamBody, token, requestID, retryCount, targetFormat)
		if doErr != nil {
			isTimeout := isTimeoutError(doErr)
			log.Printf("[ERR]  rapi=%s key=%d do_error=%s timeout=%v", rapi.Alias, key.ID, doErr.Error(), isTimeout)
			if g.log != nil {
				g.log.RecordError(requestID, fmt.Sprintf("rapi=%s key=%d: %s", rapi.Alias, key.ID, doErr.Error()), "UPSTREAM_RESPONSE")
			}
			g.scheduler.MarkKeyFailure(key.ID, time.Time{}, doErr.Error(), isTimeout)
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			g.scheduler.MarkKeySuccess(key.ID)
			return resp, key.ID, nil
		}

		// Read and log the upstream error body for all non-2xx responses.
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		log.Printf("[ERR]  rapi=%s key=%d status=%d body=%s", rapi.Alias, key.ID, resp.StatusCode, string(errBody))
		if g.log != nil {
			g.log.RecordError(requestID,
				fmt.Sprintf("rapi=%s key=%d upstream %d: %s", rapi.Alias, key.ID, resp.StatusCode, string(errBody)),
				"UPSTREAM_RESPONSE")
		}

		switch classifyFailure(resp.StatusCode) {

		case failureSystem:
			// 401 Unauthorized: key token is invalid — disable it permanently (DB + scheduler).
			// This mirrors the failurePlatform path but at key granularity: the key itself is
			// bad, not necessarily the entire RAPI/platform.
			reason := fmt.Sprintf("[认证失败] upstream 401: %s", string(errBody))
			log.Printf("[KEY-FAIL] rapi=%s key=%d 401 unauthorized, disabling key", rapi.Alias, key.ID)
			g.scheduler.MarkKeyPlatformFailure(key.ID, "401 unauthorized")
			if key.ID > 0 { // skip synthetic keys (ID == -1)
				if dbErr := g.db.DisablePlatformKey(key.ID); dbErr != nil {
					log.Printf("[WARN] DisablePlatformKey key=%d: %v", key.ID, dbErr)
				}
			}
			if g.log != nil {
				g.log.RecordError(requestID, reason, "UPSTREAM_RESPONSE")
			}
			if g.notifyService != nil {
				g.notifyService.PublishAsync(
					fmt.Sprintf("模型 %s [%s] Key #%d 认证失败（401），已自动禁用，请检查 API Key 是否有效", rapi.Alias, rapi.PlatformName, key.KeyIndex),
					"Key 认证失败",
				)
			}
			continue

		case failurePlatform:
			// 平台级：欠费/封号/模型失效 → available=false 写 DB + 立即失效调度器状态
			reason := fmt.Sprintf("[平台级] upstream %d: %s", resp.StatusCode, string(errBody))
			log.Printf("[PLATFORM-FAIL] rapi=%s id=%d status=%d reason=%s", rapi.Alias, rapi.ID, resp.StatusCode, reason)
			if dbErr := g.db.SetRAPIUnavailableWithReason(rapi.ID, false, reason); dbErr != nil {
				log.Printf("[WARN] SetRAPIUnavailableWithReason rapi=%s: %v", rapi.Alias, dbErr)
			}
			// InvalidateRAPI ensures this RAPI is skipped in the current request's retry
			// loop immediately, without waiting for the next GetEnabledRAPIsForLAPI query.
			g.scheduler.InvalidateRAPI(rapi.ID)
			g.scheduler.MarkKeyPlatformFailure(key.ID, reason)
			if g.log != nil {
				g.log.RecordError(requestID, reason, "UPSTREAM_RESPONSE")
			}
			if g.notifyService != nil {
				g.notifyService.PublishAsync(
					fmt.Sprintf("模型 %s [%s] 平台失效（%d），已自动标记失效，请检查账号状态后点击重试恢复", rapi.Alias, rapi.PlatformName, resp.StatusCode),
					"模型平台失效",
				)
			}
			return nil, 0, fmt.Errorf("%s", reason)

		default: // failureSession
			// 会话级：限流/内容违规/请求过长/临时故障 → key 短冷却，本轮跳过
			retryAt := scheduler.RetryAt(resp.Header, time.Time{})
			g.scheduler.MarkKeyFailure(key.ID, retryAt, fmt.Sprintf("upstream %d: %s", resp.StatusCode, string(errBody)))
			continue
		}
	}
}

// handleStreamingRequest handles streaming (SSE) requests with full error absorption.
func (g *ProxyGateway) handleStreamingRequest(w http.ResponseWriter, r *http.Request, originalBody []byte, canonicalBody []byte, req *models.ProxyRequest, lapi *models.LAPI, rapis []models.RAPIWithPlatform, requestID string, sessionID string, clientFormat string) (bool, int) {
	fallbackUsed := false
	retryCount := 0
	estimatedTokens := scheduler.EstimateCost(req)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return fallbackUsed, http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Transfer-Encoding", "chunked")

	responseCommitted := false

	for {
		rapi, waitUntil, err := g.scheduler.PickAvailable(lapi.ID, rapis, clientFormat)
		if err != nil {
			if errors.Is(err, scheduler.ErrAllRAPIUnavailable) {
				if waitUntil.IsZero() {
					// All RAPIs are hard-disabled (not just cooling down) — no point waiting.
					const msg = "all RAPIs are disabled or unavailable"
					log.Printf("[FAIL] lapi=%s %s", lapi.Alias, msg)
					g.sendErrorStream(w, flusher, msg)
					if g.log != nil {
						g.log.RecordError(requestID, msg, "UPSTREAM_RESPONSE")
					}
					return fallbackUsed, http.StatusServiceUnavailable
				}
				log.Printf("[WAIT] all RAPI unavailable, waiting until %v (attempt %d)", waitUntil.Format("15:04:05"), retryCount)
				if waitErr := g.scheduler.Wait(r.Context(), lapi.ID, waitUntil); waitErr != nil {
					log.Printf("[FAIL] lapi=%s wait exhausted: %v", lapi.Alias, waitErr)
					g.sendErrorStream(w, flusher, waitErr.Error())
					if g.log != nil {
						g.log.RecordError(requestID, waitErr.Error(), "UPSTREAM_RESPONSE")
					}
					return fallbackUsed, http.StatusServiceUnavailable
				}
				continue
			}
			log.Printf("[FAIL] PickAvailable error: %v", err)
			g.sendErrorStream(w, flusher, err.Error())
			return fallbackUsed, http.StatusServiceUnavailable
		}

		// Determine the best target format for this RAPI.
		targetFormat := pickTargetFormat(rapi, clientFormat)
		var effectiveURL string
		if rapi.URLAutoComplete {
			effectiveURL = apiformat.BuildURL(rapi.BaseURL, rapi.Model, apiformat.APIFormat(targetFormat))
		} else {
			effectiveURL = strings.TrimRight(rapi.BaseURL, "/")
		}
		log.Printf("[SEND] rapi=%s model=%s format=%s url=%s attempt=%d", rapi.Alias, rapi.Model, targetFormat, effectiveURL, retryCount)
		if effectiveURL == "" {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "invalid url")
			continue
		}

		if rapi.ID != rapis[0].ID {
			fallbackUsed = true
		}

		// Bug 8.1: Convert canonical (OpenAI) body directly to target format in one step.
		// Optimization: when client format matches target format, skip the lossy
		// canonical round-trip and just replace the model field in the original body.
		// Use ReplaceModelField (lossless json decode/encode) rather than byte-level
		// substitution, which corrupts content containing "model":"X" substrings.
		var upstreamBody []byte
		var convErr error
		if targetFormat == clientFormat {
			upstreamBody = apiformat.ReplaceModelField(originalBody, rapi.Model)
		} else {
			upstreamBody, convErr = apiformat.ConvertRequest(canonicalBody, apiformat.FormatOpenAI, apiformat.APIFormat(targetFormat), rapi.Model)
		}
		if convErr != nil {
			log.Printf("[ERR] rapi=%s convert error: %v", rapi.Alias, convErr)
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "convert error: "+convErr.Error())
			continue
		}

		startTime := time.Now()
		resp, keyID, err := g.tryKeyForRAPI(r.Context(), rapi, effectiveURL, upstreamBody, requestID, retryCount, targetFormat)
		latencyMs := int(time.Since(startTime).Milliseconds())

		if err != nil {
			// Count failed request toward rate limit counters (0 tokens — unknown).
			g.scheduler.RecordRequest(rapi.ID, 0)
			if errors.Is(err, scheduler.ErrAllKeysUnavailable) {
				// All keys exhausted — mark RAPI failed too.
				g.scheduler.MarkFailure(rapi.ID, time.Time{}, "all keys unavailable")
			} else {
				isTimeout := isTimeoutError(err)
				log.Printf("[ERR]  rapi=%s error=%s latency=%dms timeout=%v", rapi.Alias, err.Error(), latencyMs, isTimeout)
				g.scheduler.MarkFailure(rapi.ID, time.Time{}, err.Error(), isTimeout)
				if g.log != nil {
					g.log.RecordError(requestID, "Model "+rapi.Alias+" ["+rapi.PlatformName+"] failed: "+err.Error(), "UPSTREAM_RESPONSE")
				}
				if g.notifyService != nil {
					g.notifyService.PublishAsync(fmt.Sprintf("模型 %s 请求失败: %v", rapi.Alias, err), "模型错误")
				}
			}
			retryCount++
			continue
		}
		_ = keyID

		// Extract token usage from response headers
		tokensUsed := extractTokenUsage(resp.Header)
		if tokensUsed <= 0 {
			tokensUsed = estimatedTokens
		}

		log.Printf("[OK]   rapi=%s status=%d latency=%dms", rapi.Alias, resp.StatusCode, latencyMs)
		g.scheduler.RecordRequest(rapi.ID, tokensUsed)
		// Bug 8.5: record to persistent DB metrics.
		g.db.RecordRequest(rapi.ID, lapi.ID, resp.StatusCode, latencyMs, tokensUsed)

		if g.notifyService != nil {
			g.notifyService.PublishAsync(fmt.Sprintf("Streaming from %s", rapi.Alias), "Active Route")
		}

		// Bug 8.1: target format is already set correctly; no intermediate OpenAI step.
		needsConversion := targetFormat != clientFormat

		// Capture up to 32 KB of the raw upstream SSE body for logging while
		// still streaming bytes to the client in real time.
		const streamLogCapBytes = 32 * 1024
		var streamLogBuf bytes.Buffer
		teeBody := io.TeeReader(resp.Body, &limitedWriter{w: &streamLogBuf, limit: streamLogCapBytes})

		if !needsConversion {
			// Bug 8.3: Read synchronously — no goroutine needed.
			buf := make([]byte, 4096)
			for {
				n, readErr := teeBody.Read(buf)
				if n > 0 {
					// Check if client has disconnected before writing.
					if r.Context().Err() != nil {
						log.Printf("[INFO] client disconnected during stream, stopping")
						resp.Body.Close()
						return fallbackUsed, http.StatusOK
					}
					responseCommitted = true
					w.Write(buf[:n])
					flusher.Flush()
				}
				if readErr != nil {
					break
				}
			}
		} else {
			// Bug 8.4: drain body on convert error is handled inside StreamConverter.Run().
			converter := apiformat.NewStreamConverter(teeBody, w, apiformat.APIFormat(targetFormat), apiformat.APIFormat(clientFormat), req.Model)
			converter.SetFlusher(flusher)
			if err := converter.Run(); err != nil {
				log.Printf("[ERR] stream convert error: %v", err)
				// Bug 8.4: drain remaining body to avoid leaking the connection.
				io.Copy(io.Discard, teeBody)
				// If the converter had already flushed one or more data: frames to the
				// client before erroring, the HTTP response is committed: we CANNOT retry
				// against another RAPI, otherwise a second stream's message_start/deltas
				// would be appended to the same response → cross-stream contamination.
				// Only retry when literally zero frames were written.
				responseCommitted = converter.Written() > 0
			} else if converter.Written() > 0 {
				// Only mark as committed when at least one SSE frame reached the client.
				// Written()==0 means the upstream sent an empty or all-skipped stream;
				// treat it as a failure so the retry loop can try the next RAPI.
				responseCommitted = true
			} else {
				log.Printf("[WARN] rapi=%s stream conversion produced 0 frames, retrying", rapi.Alias)
			}
		}

		resp.Body.Close()

		if g.log != nil {
			g.log.RecordUpstreamResponse(requestID, resp.StatusCode, respHeaders(resp), streamLogBuf.String(), latencyMs, tokensUsed)
		}

		if responseCommitted {
			g.scheduler.MarkSuccess(rapi.ID)
		} else {
			// Stream ended without any data — treat as failure.
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "empty stream")
			retryCount++
			continue
		}
		return fallbackUsed, http.StatusOK
	}
}

// handleNonStreamingRequest handles non-streaming requests with full error absorption.
func (g *ProxyGateway) handleNonStreamingRequest(w http.ResponseWriter, r *http.Request, originalBody []byte, canonicalBody []byte, req *models.ProxyRequest, lapi *models.LAPI, rapis []models.RAPIWithPlatform, requestID string, sessionID string, clientFormat string) (bool, int) {
	fallbackUsed := false
	retryCount := 0
	estimatedTokens := scheduler.EstimateCost(req)

	// Bound non-streaming upstream work so slow backends cannot hang the client forever.
	// Streaming uses the client connection context only (see handleStreamingRequest).
	reqCtx := r.Context()
	if g.responseTimeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(r.Context(), g.responseTimeout)
		defer cancel()
	}

	for {
		rapi, waitUntil, err := g.scheduler.PickAvailable(lapi.ID, rapis, clientFormat)
		if err != nil {
			if errors.Is(err, scheduler.ErrAllRAPIUnavailable) {
				if waitUntil.IsZero() {
					// All RAPIs are hard-disabled (not just cooling down) — no point waiting.
					const msg = "all RAPIs are disabled or unavailable"
					log.Printf("[FAIL] lapi=%s %s", lapi.Alias, msg)
					http.Error(w, fmt.Sprintf(`{"error":{"message":"%s","type":"service_unavailable"}}`, msg), http.StatusServiceUnavailable)
					if g.log != nil {
						g.log.RecordError(requestID, msg, "UPSTREAM_RESPONSE")
					}
					return fallbackUsed, http.StatusServiceUnavailable
				}
				log.Printf("[WAIT] all RAPI unavailable, waiting until %v (attempt %d)", waitUntil.Format("15:04:05"), retryCount)
				if waitErr := g.scheduler.Wait(reqCtx, lapi.ID, waitUntil); waitErr != nil {
					log.Printf("[FAIL] lapi=%s wait exhausted: %v", lapi.Alias, waitErr)
					http.Error(w, fmt.Sprintf(`{"error":{"message":"%s","type":"service_unavailable"}}`, waitErr.Error()), http.StatusServiceUnavailable)
					if g.log != nil {
						g.log.RecordError(requestID, waitErr.Error(), "UPSTREAM_RESPONSE")
					}
					return fallbackUsed, http.StatusServiceUnavailable
				}
				continue
			}
			log.Printf("[FAIL] PickAvailable error: %v", err)
			http.Error(w, fmt.Sprintf(`{"error":{"message":"%s","type":"service_unavailable"}}`, err.Error()), http.StatusServiceUnavailable)
			return fallbackUsed, http.StatusServiceUnavailable
		}

		// Determine the best target format for this RAPI.
		targetFormat := pickTargetFormat(rapi, clientFormat)
		var effectiveURL string
		if rapi.URLAutoComplete {
			effectiveURL = apiformat.BuildURL(rapi.BaseURL, rapi.Model, apiformat.APIFormat(targetFormat))
		} else {
			effectiveURL = strings.TrimRight(rapi.BaseURL, "/")
		}
		log.Printf("[SEND] rapi=%s model=%s format=%s url=%s attempt=%d", rapi.Alias, rapi.Model, targetFormat, effectiveURL, retryCount)
		if effectiveURL == "" {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "invalid url")
			continue
		}

		if rapi.ID != rapis[0].ID {
			fallbackUsed = true
		}

		// Bug 8.1: Convert canonical (OpenAI) body directly to target format.
		// Optimization: when client format matches target format, skip the lossy
		// canonical round-trip and just replace the model field in the original body.
		// Use ReplaceModelField (lossless json decode/encode) rather than byte-level
		// substitution, which corrupts content containing "model":"X" substrings.
		var upstreamBody []byte
		var convErr error
		if targetFormat == clientFormat {
			upstreamBody = apiformat.ReplaceModelField(originalBody, rapi.Model)
		} else {
			upstreamBody, convErr = apiformat.ConvertRequest(canonicalBody, apiformat.FormatOpenAI, apiformat.APIFormat(targetFormat), rapi.Model)
		}
		if convErr != nil {
			log.Printf("[ERR] rapi=%s convert error: %v", rapi.Alias, convErr)
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "convert error: "+convErr.Error())
			continue
		}

		startTime := time.Now()
		resp, keyID, err := g.tryKeyForRAPI(reqCtx, rapi, effectiveURL, upstreamBody, requestID, retryCount, targetFormat)
		latencyMs := int(time.Since(startTime).Milliseconds())

		if err != nil {
			// Count failed request toward rate limit counters (0 tokens — unknown).
			g.scheduler.RecordRequest(rapi.ID, 0)
			if errors.Is(err, scheduler.ErrAllKeysUnavailable) {
				g.scheduler.MarkFailure(rapi.ID, time.Time{}, "all keys unavailable")
			} else {
				isTimeout := isTimeoutError(err)
				log.Printf("[ERR]  rapi=%s error=%s latency=%dms timeout=%v", rapi.Alias, err.Error(), latencyMs, isTimeout)
				g.scheduler.MarkFailure(rapi.ID, time.Time{}, err.Error(), isTimeout)
				if g.log != nil {
					g.log.RecordError(requestID, "Model "+rapi.Alias+" ["+rapi.PlatformName+"] failed: "+err.Error(), "UPSTREAM_RESPONSE")
				}
			}
			retryCount++
			continue
		}
		_ = keyID

		tokensUsed := extractTokenUsage(resp.Header)
		if tokensUsed <= 0 {
			tokensUsed = estimatedTokens
		}

		// Success path
		log.Printf("[OK]   rapi=%s status=%d latency=%dms", rapi.Alias, resp.StatusCode, latencyMs)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		g.scheduler.RecordRequest(rapi.ID, tokensUsed)
		g.scheduler.MarkSuccess(rapi.ID)
		// Bug 8.5: record to persistent DB metrics.
		g.db.RecordRequest(rapi.ID, lapi.ID, resp.StatusCode, latencyMs, tokensUsed)

		if g.log != nil {
			g.log.RecordUpstreamResponse(requestID, resp.StatusCode, respHeaders(resp), string(respBody), latencyMs, tokensUsed)
		}

		// Convert response back to client format if needed.
		if targetFormat != clientFormat {
			converted, convErr := apiformat.ConvertResponse(respBody, apiformat.APIFormat(targetFormat), apiformat.APIFormat(clientFormat), req.Model)
			if convErr != nil {
				log.Printf("[ERR] response convert error: %v, sending raw", convErr)
				w.Header().Set("Content-Type", "application/json")
				w.Write(respBody)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.Write(converted)
			}
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write(respBody)
		}
		return fallbackUsed, resp.StatusCode
	}
}

// customHeader holds one user-defined header entry as stored in the JSON array.
type customHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// expandHeaderValue replaces supported placeholders in a header value.
// Supported: {{timestamp}} → current Unix seconds, {{uuid}} → new random UUID v4.
func expandHeaderValue(v string) string {
	if strings.Contains(v, "{{timestamp}}") {
		v = strings.ReplaceAll(v, "{{timestamp}}", strconv.FormatInt(time.Now().Unix(), 10))
	}
	if strings.Contains(v, "{{uuid}}") {
		v = strings.ReplaceAll(v, "{{uuid}}", uuid.NewString())
	}
	return v
}

// doUpstreamRequest builds and executes an upstream HTTP request with format-appropriate headers.
// Timeouts are governed entirely by the Transport-level dial/TLS deadlines set in
// NewProxyGatewayWithConfig — we do NOT set http.Client.Timeout or a per-request context
// deadline here, because either would kill streaming response bodies mid-flight.
// The caller's ctx carries the client-connection lifetime as an implicit upper bound.
func (g *ProxyGateway) doUpstreamRequest(ctx context.Context, rapi models.RAPIWithPlatform, url string, body []byte, token string, requestID string, retryCount int, targetFormat string) (*http.Response, int, []byte, error) {
	upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, 0, nil, err
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	// Disable transparent gzip decompression by Go's Transport.
	// If we allow gzip, Transport buffers the entire compressed body before decompressing,
	// which breaks streaming (SSE) responses — the client receives nothing until the
	// upstream closes the connection. Requesting identity encoding ensures raw bytes flow
	// straight through to the caller without any buffering.
	upstreamReq.Header.Set("Accept-Encoding", "identity")

	// Set format-specific auth headers.
	switch targetFormat {
	case "anthropic":
		upstreamReq.Header.Set("x-api-key", token)
		upstreamReq.Header.Set("anthropic-version", "2023-06-01")
	default: // openai, gemini, etc.
		upstreamReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	// Apply custom headers: platform-level first, then RAPI-level (RAPI can override platform).
	for _, src := range []string{rapi.PlatformCustomHeaders, rapi.CustomHeaders} {
		if src == "" {
			continue
		}
		var headers []customHeader
		if jsonErr := json.Unmarshal([]byte(src), &headers); jsonErr == nil {
			for _, h := range headers {
				if strings.TrimSpace(h.Key) == "" {
					continue
				}
				upstreamReq.Header.Set(h.Key, expandHeaderValue(h.Value))
			}
		}
	}

	if g.log != nil {
		headers := make(map[string]string)
		for k, v := range upstreamReq.Header {
			if len(v) > 0 {
				headers[k] = logger.SanitizeKey(v[0])
			}
		}
		g.log.RecordUpstreamSent(requestID, fmt.Sprintf("%s [%s]", rapi.Alias, rapi.PlatformName), url, headers, string(body), retryCount)
	}

	resp, err := g.httpClient.Do(upstreamReq)
	if err != nil {
		return nil, 0, nil, err
	}
	return resp, resp.StatusCode, nil, nil
}

// doWebpageRequest builds and executes an upstream HTTP request in "browser replay" mode.
// Instead of a clean API call with just Authorization injected, it replays the full set of
// browser session headers that were captured and pushed by the browser extension. This allows
// the gateway to impersonate the user's active browser session against the platform's chat API.
//
// key.SessionHeaders is a JSON object: {"Authorization":"Bearer sk-...","Cookie":"...","User-Agent":"..."}.
// The body is the gateway-built request body (converted to OpenAI or target format).
// The URL is the platform's configured base_url (webpage platforms set url_auto_complete=false
// so the stored URL is the verbatim chat API endpoint captured from the browser).
func (g *ProxyGateway) doWebpageRequest(ctx context.Context, rapi models.RAPIWithPlatform, url string, body []byte, key models.PlatformKey, requestID string, retryCount int) (*http.Response, int, []byte, error) {
	upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, 0, nil, err
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("Accept-Encoding", "identity")

	// Apply captured browser session headers, overriding defaults where applicable.
	// This replays the full browser context: cookies, user-agent, origin, referer, etc.
	if key.SessionHeaders != "" {
		var sessionHdrs map[string]string
		if jsonErr := json.Unmarshal([]byte(key.SessionHeaders), &sessionHdrs); jsonErr == nil {
			for k, v := range sessionHdrs {
				if strings.TrimSpace(k) != "" {
					upstreamReq.Header.Set(k, v)
				}
			}
		}
	} else if key.Token != "" {
		// Fallback: if session headers were not captured, use the token as a plain Bearer header.
		upstreamReq.Header.Set("Authorization", "Bearer "+key.Token)
	}

	// Apply platform-level custom headers last (can override session headers if needed).
	if rapi.PlatformCustomHeaders != "" {
		var headers []customHeader
		if jsonErr := json.Unmarshal([]byte(rapi.PlatformCustomHeaders), &headers); jsonErr == nil {
			for _, h := range headers {
				if strings.TrimSpace(h.Key) != "" {
					upstreamReq.Header.Set(h.Key, expandHeaderValue(h.Value))
				}
			}
		}
	}

	if g.log != nil {
		hdrs := make(map[string]string)
		for k, v := range upstreamReq.Header {
			if len(v) > 0 {
				hdrs[k] = logger.SanitizeKey(v[0])
			}
		}
		g.log.RecordUpstreamSent(requestID, fmt.Sprintf("%s [%s] (webpage)", rapi.Alias, rapi.PlatformName), url, hdrs, string(body), retryCount)
	}

	resp, err := g.httpClient.Do(upstreamReq)
	if err != nil {
		return nil, 0, nil, err
	}
	return resp, resp.StatusCode, nil, nil
}

func (g *ProxyGateway) sendErrorStream(w http.ResponseWriter, flusher http.Flusher, message string) {
	errorResp, _ := json.Marshal(map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    "proxy_error",
			"code":    "failover_exhausted",
		},
	})
	// The handler has already set Content-Type: text/event-stream, so the error MUST be
	// framed as an SSE data: event. A bare JSON object (no data: prefix, no \n\n) is not
	// parseable by SSE clients — e.g. Anthropic clients expect event:/data: framing and
	// would raise "invalid character '{'" on the raw JSON.
	if _, err := fmt.Fprintf(w, "data: %s\n\n", errorResp); err != nil {
		log.Printf("[WARN] sendErrorStream: write failed: %v", err)
		return
	}
	flusher.Flush()
}

func extractTokenUsage(header http.Header) int {
	if usage := header.Get("Openai-Usage"); usage != "" {
		if tokens, err := strconv.Atoi(usage); err == nil {
			return tokens
		}
	}
	if tokens := header.Get("X-Token-Usage"); tokens != "" {
		if n, err := strconv.Atoi(tokens); err == nil {
			return n
		}
	}
	return 0
}

// pickTargetFormat determines the best format to use when forwarding to a RAPI.
// Prefers the client's format if the RAPI supports it natively; otherwise picks the first supported format.
func pickTargetFormat(rapi models.RAPIWithPlatform, clientFormat string) string {
	// If RAPI natively supports the client's format, use it (zero conversion overhead).
	if rapi.SupportsAPIFormat(clientFormat) {
		return clientFormat
	}
	// Otherwise pick the first supported format from the RAPI's list.
	for _, f := range []string{"openai", "anthropic", "gemini"} {
		if rapi.SupportsAPIFormat(f) {
			return f
		}
	}
	return "openai" // fallback
}

// respHeaders returns a flat string map of the first value for each response header.
func respHeaders(resp *http.Response) map[string]string {
	headers := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}
	return headers
}

// isTimeoutError reports whether err represents a network timeout (dial timeout, TLS timeout,
// or HTTP client response timeout). Timeout errors mean the backend is reachable but slow;
// they should trigger a shorter cooldown than hard errors like connection refused or 4xx/5xx.
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	// context.DeadlineExceeded covers http.Client.Timeout and context-based deadlines.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// net.Error with Timeout() == true covers dial timeout, TLS handshake timeout, etc.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

// failureLevel classifies the scope of an upstream HTTP error.
type failureLevel int

const (
	failureSession  failureLevel = iota // 会话级：本轮跳过，自动冷却恢复
	failureSystem                       // 系统级：token 失效，尝试刷新
	failurePlatform                     // 平台级：账号问题，写 DB unavailable
)

// classifyFailure maps an upstream HTTP status code to one of three failure scopes:
//
// 系统级 (failureSystem) — token / auth errors that may be fixable by refreshing:
//   - 401 Unauthorized
//
// 平台级 (failurePlatform) — account-level problems requiring admin intervention:
//   - 402 Payment Required  (quota exhausted / billing)
//   - 403 Forbidden         (account banned / no permission)
//   - 409 Conflict          (account state conflict)
//   - 423 Locked            (account locked)
//   - 451 Unavailable For Legal Reasons
//
// 会话级 (failureSession) — everything else (request-level or transient infra):
//   - 400 Bad Request, 404 Not Found, 413 Payload Too Large
//   - 422 Unprocessable Entity (content policy — per-request)
//   - 429 Too Many Requests (rate limit — transient)
//   - 5xx (upstream infra — transient)
func classifyFailure(statusCode int) failureLevel {
	switch statusCode {
	case http.StatusUnauthorized: // 401
		return failureSystem
	case http.StatusPaymentRequired,          // 402
		http.StatusForbidden,                 // 403
		http.StatusConflict,                  // 409
		http.StatusLocked,                    // 423
		http.StatusUnavailableForLegalReasons: // 451
		return failurePlatform
	default:
		return failureSession
	}
}

// limitedWriter wraps a bytes.Buffer and stops writing once the byte cap is reached.
// Used to capture a bounded snapshot of a streaming response body for logging.
type limitedWriter struct {
	w     *bytes.Buffer
	limit int
}

func (lw *limitedWriter) Write(p []byte) (int, error) {
	remaining := lw.limit - lw.w.Len()
	if remaining <= 0 {
		return len(p), nil // silently discard; pretend we wrote it all
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	return lw.w.Write(p)
}

// isAgentMode determines whether the request should enter the agent tool-calling loop.
// Returns true if body contains "agent_mode": true, OR contains a non-empty "tools" array
// and "agent_mode" is not explicitly false.
func isAgentMode(body []byte) bool {
	var probe struct {
		AgentMode *bool                    `json:"agent_mode"`
		Tools     []map[string]interface{} `json:"tools"`
		ToolNames []string                 `json:"tool_names"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return false
	}
	// Explicit agent_mode: true
	if probe.AgentMode != nil && *probe.AgentMode {
		return true
	}
	// Explicit agent_mode: false → never enter agent loop
	if probe.AgentMode != nil && !*probe.AgentMode {
		return false
	}
	// Implicit: has tools or tool_names → default to agent mode
	return len(probe.Tools) > 0 || len(probe.ToolNames) > 0
}

// handleAgentRequest runs the agent tool-calling loop and writes the final response.
func (g *ProxyGateway) handleAgentRequest(w http.ResponseWriter, r *http.Request, canonicalBody []byte, req *models.ProxyRequest, requestID string, startTime time.Time) {
	// Parse tools from the request body
	var bodyMap map[string]interface{}
	if err := json.Unmarshal(canonicalBody, &bodyMap); err != nil {
		http.Error(w, `{"error":{"message":"Invalid request body","type":"invalid_request"}}`, http.StatusBadRequest)
		return
	}

	// Collect tools: inline definitions + referenced tool_names from registry
	registry := tools.GetRegistry()
	var toolsList []map[string]interface{}

	// Inline tools from body
	if inlineTools, ok := bodyMap["tools"].([]interface{}); ok {
		for _, t := range inlineTools {
			if tm, ok := t.(map[string]interface{}); ok {
				toolsList = append(toolsList, tm)
			}
		}
	}

	// Referenced tool_names from registry
	if toolNames, ok := bodyMap["tool_names"].([]interface{}); ok {
		var names []string
		for _, n := range toolNames {
			if s, ok := n.(string); ok {
				names = append(names, s)
			}
		}
		if len(names) > 0 {
			registered, err := registry.ToOpenAITools(names)
			if err != nil {
				http.Error(w, fmt.Sprintf(`{"error":{"message":"%s","type":"invalid_request"}}`, err.Error()), http.StatusBadRequest)
				return
			}
			toolsList = append(toolsList, registered...)
		}
	}

	if len(toolsList) == 0 {
		// No tools resolved — fall through to normal proxy (shouldn't happen if isAgentMode passed)
		http.Error(w, `{"error":{"message":"No tools available for agent mode","type":"invalid_request"}}`, http.StatusBadRequest)
		return
	}

	// Extract messages
	messages, _ := bodyMap["messages"].([]interface{})
	if len(messages) == 0 {
		http.Error(w, `{"error":{"message":"messages is required","type":"invalid_request"}}`, http.StatusBadRequest)
		return
	}

	// Create agent loop with ForwardInternal as the forward function
	loop := tools.NewLoop(registry, *g.agentConfig, g.ForwardInternal)

	log.Printf("[AGENT] starting agent loop: model=%s tools=%d requestID=%s", req.Model, len(toolsList), requestID)

	content, toolLogs, err := loop.Run(r.Context(), messages, toolsList, req.Model)

	// Log tool executions to DB
	for _, tl := range toolLogs {
		argsJSON, _ := json.Marshal(tl.Arguments)
		g.db.LogToolExecution(requestID, tl.Name, string(argsJSON), tl.Result, tl.Error, tl.DurationMs, tl.Iteration)
	}

	if err != nil && content == "" {
		log.Printf("[AGENT] loop failed: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":{"message":"Agent loop failed: %s","type":"agent_error"}}`, err.Error()), http.StatusBadGateway)
		if g.log != nil {
			g.log.RecordError(requestID, "Agent loop failed: "+err.Error(), "AGENT_LOOP")
			g.log.RecordClientResponse(requestID, http.StatusBadGateway, int(time.Since(startTime).Milliseconds()), false)
		}
		return
	}

	// Build standard OpenAI chat completion response
	respID := "chatcmpl-agent-" + requestID[:8]
	agentMeta := map[string]interface{}{
		"iterations": countIterations(toolLogs),
		"tool_calls": toolLogs,
	}
	if err != nil {
		agentMeta["error"] = err.Error()
	}

	response := map[string]interface{}{
		"id":      respID,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   req.Model,
		"choices": []map[string]interface{}{
			{
				"index": 0,
				"message": map[string]interface{}{
					"role":    "assistant",
					"content": content,
				},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]interface{}{
			"prompt_tokens":     0,
			"completion_tokens": 0,
			"total_tokens":      0,
		},
		"_agent_meta": agentMeta,
	}

	respBytes, _ := json.Marshal(response)
	w.Header().Set("Content-Type", "application/json")
	w.Write(respBytes)

	log.Printf("[AGENT] completed: model=%s iterations=%d tools_called=%d latency=%dms",
		req.Model, countIterations(toolLogs), len(toolLogs), time.Since(startTime).Milliseconds())

	if g.log != nil {
		g.log.RecordClientResponse(requestID, http.StatusOK, int(time.Since(startTime).Milliseconds()), false)
	}
}

// countIterations returns the max iteration number from tool logs.
func countIterations(logs []tools.ToolCallLog) int {
	max := 0
	for _, l := range logs {
		if l.Iteration > max {
			max = l.Iteration
		}
	}
	return max
}

// ForwardInternal sends a chat completion request through the normal scheduler/routing
// pipeline and returns the raw OpenAI-format response body. Used by the agent loop
// for internal LLM calls (no HTTP ResponseWriter involved).
func (g *ProxyGateway) ForwardInternal(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("parse internal request: %w", err)
	}

	lapi, err := g.db.GetLAPIByAlias(strings.ToLower(req.Model))
	if err != nil {
		return nil, fmt.Errorf("unknown model: %s", req.Model)
	}
	if !lapi.Enabled {
		return nil, fmt.Errorf("model %s is disabled", req.Model)
	}

	rapis, err := g.db.GetEnabledRAPIsForLAPI(lapi.ID)
	if err != nil || len(rapis) == 0 {
		return nil, fmt.Errorf("no backends available for model %s", req.Model)
	}
	g.loadKeysForRAPIs(rapis)

	// Try up to len(rapis) times (failover)
	var lastErr error
	for attempt := 0; attempt < len(rapis)+2; attempt++ {
		rapi, waitUntil, err := g.scheduler.PickAvailable(lapi.ID, rapis, "openai")
		if err != nil {
			if errors.Is(err, scheduler.ErrAllRAPIUnavailable) && !waitUntil.IsZero() {
				if waitErr := g.scheduler.Wait(ctx, lapi.ID, waitUntil); waitErr != nil {
					return nil, fmt.Errorf("all backends unavailable: %w", waitErr)
				}
				continue
			}
			return nil, fmt.Errorf("no available backend: %w", err)
		}

		targetFormat := pickTargetFormat(rapi, "openai")
		var effectiveURL string
		if rapi.URLAutoComplete {
			effectiveURL = apiformat.BuildURL(rapi.BaseURL, rapi.Model, apiformat.APIFormat(targetFormat))
		} else {
			effectiveURL = strings.TrimRight(rapi.BaseURL, "/")
		}
		if effectiveURL == "" {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "invalid url")
			continue
		}

		// Convert body to target format
		var upstreamBody []byte
		if targetFormat == "openai" {
			upstreamBody = apiformat.ReplaceModelField(body, rapi.Model)
		} else {
			upstreamBody, err = apiformat.ConvertRequest(body, apiformat.FormatOpenAI, apiformat.APIFormat(targetFormat), rapi.Model)
			if err != nil {
				g.scheduler.MarkFailure(rapi.ID, time.Time{}, "convert error: "+err.Error())
				continue
			}
		}

		resp, _, err := g.tryKeyForRAPI(ctx, rapi, effectiveURL, upstreamBody, "agent-internal", attempt, targetFormat)
		if err != nil {
			g.scheduler.RecordRequest(rapi.ID, 0)
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, err.Error(), isTimeoutError(err))
			lastErr = err
			continue
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		g.scheduler.RecordRequest(rapi.ID, 0)
		g.scheduler.MarkSuccess(rapi.ID)

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("upstream returned %d: %s", resp.StatusCode, string(respBody[:min(len(respBody), 256)]))
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, lastErr.Error())
			continue
		}

		// Convert response back to OpenAI format if needed
		if targetFormat != "openai" {
			converted, convErr := apiformat.ConvertResponse(respBody, apiformat.APIFormat(targetFormat), apiformat.FormatOpenAI, req.Model)
			if convErr != nil {
				return respBody, nil // return raw if conversion fails
			}
			return converted, nil
		}
		return respBody, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("all backends exhausted for model %s", req.Model)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
