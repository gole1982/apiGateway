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
	"time"

	"github.com/google/uuid"

	"gateway/internal/apiformat"
	"gateway/internal/db"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/notify"
	"gateway/internal/scheduler"
)

type ProxyGateway struct {
	db             *db.DB
	httpClient     *http.Client
	notifyService  *notify.NotificationService
	log            *logger.Logger
	sessionTracker *logger.SessionTracker
	scheduler      *scheduler.Manager
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
	return &ProxyGateway{
		db: db.Get(),
		// http.Client.Timeout = 0: no global deadline.
		// Dial+TLS are bounded by the Transport's DialContext and TLSHandshakeTimeout.
		// Streaming body reads are intentionally unlimited — the client connection
		// context (r.Context()) acts as the natural upper bound.
		httpClient: &http.Client{
			Timeout:   0,
			Transport: transport,
		},
		notifyService:  notifyService,
		log:            log,
		sessionTracker: sessionTracker,
		scheduler:      scheduler.NewManager(schedulerCfg),
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

	if isStream {
		fallbackUsed, clientStatusCode = g.handleStreamingRequest(w, r, canonicalBody, &req, lapi, rapis, requestID, sessionID, string(clientFormat))
	} else {
		fallbackUsed, clientStatusCode = g.handleNonStreamingRequest(w, r, canonicalBody, &req, lapi, rapis, requestID, sessionID, string(clientFormat))
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
			g.notifyService.PublishAsync(
				fmt.Sprintf("RAPI %s [%s] Key #%d 认证失败（401），已自动禁用，请检查 API Key 是否有效", rapi.Alias, rapi.PlatformName, key.KeyIndex),
				"Key 认证失败",
			)
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
			g.notifyService.PublishAsync(
				fmt.Sprintf("RAPI %s [%s] 平台失效（%d），已自动下线，请检查账号状态后手动恢复", rapi.Alias, rapi.PlatformName, resp.StatusCode),
				"RAPI 平台失效",
			)
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
func (g *ProxyGateway) handleStreamingRequest(w http.ResponseWriter, r *http.Request, canonicalBody []byte, req *models.ProxyRequest, lapi *models.LAPI, rapis []models.RAPIWithPlatform, requestID string, sessionID string, clientFormat string) (bool, int) {
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
		upstreamBody, convErr := apiformat.ConvertRequest(canonicalBody, apiformat.FormatOpenAI, apiformat.APIFormat(targetFormat), rapi.Model)
		if convErr != nil {
			log.Printf("[ERR] rapi=%s convert error: %v", rapi.Alias, convErr)
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "convert error: "+convErr.Error())
			continue
		}

		startTime := time.Now()
		resp, keyID, err := g.tryKeyForRAPI(r.Context(), rapi, effectiveURL, upstreamBody, requestID, retryCount, targetFormat)
		latencyMs := int(time.Since(startTime).Milliseconds())

		if err != nil {
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
				g.notifyService.PublishAsync(fmt.Sprintf("RAPI %s failed: %v", rapi.Alias, err), "RAPI Error")
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

		g.notifyService.PublishAsync(fmt.Sprintf("Streaming from %s", rapi.Alias), "Active Route")

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
func (g *ProxyGateway) handleNonStreamingRequest(w http.ResponseWriter, r *http.Request, canonicalBody []byte, req *models.ProxyRequest, lapi *models.LAPI, rapis []models.RAPIWithPlatform, requestID string, sessionID string, clientFormat string) (bool, int) {
	fallbackUsed := false
	retryCount := 0
	estimatedTokens := scheduler.EstimateCost(req)

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
				if waitErr := g.scheduler.Wait(r.Context(), lapi.ID, waitUntil); waitErr != nil {
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
		upstreamBody, convErr := apiformat.ConvertRequest(canonicalBody, apiformat.FormatOpenAI, apiformat.APIFormat(targetFormat), rapi.Model)
		if convErr != nil {
			log.Printf("[ERR] rapi=%s convert error: %v", rapi.Alias, convErr)
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "convert error: "+convErr.Error())
			continue
		}

		startTime := time.Now()
		resp, keyID, err := g.tryKeyForRAPI(r.Context(), rapi, effectiveURL, upstreamBody, requestID, retryCount, targetFormat)
		latencyMs := int(time.Since(startTime).Milliseconds())

		if err != nil {
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

func (g *ProxyGateway) sendErrorStream(w http.ResponseWriter, flusher http.Flusher, message string) {
	errorResp, _ := json.Marshal(map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    "proxy_error",
			"code":    "failover_exhausted",
		},
	})
	w.Write(errorResp)
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

func replaceModelInBody(body []byte, oldModel, newModel string) ([]byte, error) {
	oldPattern := fmt.Sprintf(`"model":"%s"`, oldModel)
	newPattern := fmt.Sprintf(`"model":"%s"`, newModel)
	result := bytes.ReplaceAll(body, []byte(oldPattern), []byte(newPattern))

	oldPatternSpace := fmt.Sprintf(`"model": "%s"`, oldModel)
	newPatternSpace := fmt.Sprintf(`"model": "%s"`, newModel)
	result = bytes.ReplaceAll(result, []byte(oldPatternSpace), []byte(newPatternSpace))

	return result, nil
}

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
