package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"gateway/internal/db"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/notify"
	"gateway/internal/scheduler"
	"gateway/internal/tokenrefresher"
)

type ProxyGateway struct {
	db             *db.DB
	tokenProvider  *tokenrefresher.DynamicTokenProvider
	httpClient     *http.Client
	notifyService  *notify.NotificationService
	log            *logger.Logger
	sessionTracker *logger.SessionTracker
	scheduler      *scheduler.Manager
}

func NewProxyGateway(notifyService *notify.NotificationService, log *logger.Logger, sessionTracker *logger.SessionTracker) *ProxyGateway {
	return &ProxyGateway{
		db:            db.Get(),
		tokenProvider: tokenrefresher.NewDynamicTokenProvider(),
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		},
		notifyService:  notifyService,
		log:            log,
		sessionTracker: sessionTracker,
		scheduler:      scheduler.NewManager(scheduler.DefaultConfig()),
	}
}

func (g *ProxyGateway) StartBackgroundRefresh(ctx context.Context, intervalSec int) {
	platforms, err := g.db.GetPlatforms()
	if err != nil {
		return
	}

	for _, platform := range platforms {
		if platform.IsDynamic && platform.TokenCommand != "" {
			g.tokenProvider.RegisterCommandForPlatform(platform.ID, platform.TokenCommand)
			interval := time.Duration(intervalSec) * time.Second
			if interval <= 0 {
				interval = 10 * time.Minute
			}

			g.tokenProvider.StartBackgroundRefreshForPlatform(ctx, platform.ID, interval, func(platformID int64, newToken string) {
				g.db.UpdatePlatformToken(platformID, newToken)
				g.notifyService.PublishAsync(fmt.Sprintf("Token refreshed for platform %d", platformID), "Token Update")
			})
		}
	}
}

func (g *ProxyGateway) HandleChatCompletions(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	requestID := ""
	sessionID := ""
	fallbackUsed := false

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request", http.StatusBadRequest)
		return
	}

	if g.sessionTracker != nil {
		sessionID = g.sessionTracker.InjectSessionID(r)
	}

	if g.log != nil {
		requestID = logger.NewRequestID()
		headers := make(map[string]string)
		for k, v := range r.Header {
			if len(v) > 0 {
				headers[k] = logger.SanitizeKey(v[0])
			}
		}
		g.log.RecordRequestReceived(sessionID, r.RemoteAddr, r.Method, r.URL.Path, headers, string(body))
	}

	var req models.ProxyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":{"message":"Invalid JSON","type":"invalid_request"}}`, http.StatusBadRequest)
		if g.log != nil {
			g.log.RecordError(requestID, "Invalid JSON: "+err.Error(), "REQUEST_RECEIVED")
		}
		return
	}

	modelName := req.Model
	lapi, err := g.db.GetLAPIByAlias(modelName)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":{"message":"Unknown model: %s","type":"invalid_request"}}`, modelName), http.StatusUnauthorized)
		if g.log != nil {
			g.log.RecordError(requestID, "Unknown model: "+modelName, "ROUTING_DECISION")
		}
		return
	}

	rapis, err := g.db.GetEnabledRAPIsForLAPI(lapi.ID)
	if err != nil || len(rapis) == 0 {
		http.Error(w, `{"error":{"message":"No backends available for this model","type":"configuration_error"}}`, http.StatusServiceUnavailable)
		if g.log != nil {
			g.log.RecordError(requestID, "No backends available for model: "+modelName, "ROUTING_DECISION")
		}
		return
	}

	if g.log != nil {
		rapiAliases := make([]string, len(rapis))
		for i, rapi := range rapis {
			rapiAliases[i] = rapi.Alias
		}
		g.log.RecordRoutingDecision(requestID, lapi.Alias, rapiAliases)
	}

	if req.Stream {
		fallbackUsed = g.handleStreamingRequest(w, r, body, &req, lapi, rapis, requestID, sessionID)
	} else {
		fallbackUsed = g.handleNonStreamingRequest(w, r, body, &req, lapi, rapis, requestID, sessionID)
	}

	g.db.RecordTrend(lapi.ID)

	if g.log != nil {
		latencyMS := int(time.Since(startTime).Milliseconds())
		g.log.RecordClientResponse(requestID, http.StatusOK, latencyMS, fallbackUsed)
	}
}

// handleStreamingRequest handles streaming (SSE) requests with full error absorption.
func (g *ProxyGateway) handleStreamingRequest(w http.ResponseWriter, r *http.Request, body []byte, req *models.ProxyRequest, lapi *models.LAPI, rapis []models.RAPIWithPlatform, requestID string, sessionID string) bool {
	fallbackUsed := false
	retryCount := 0
	estimatedTokens := scheduler.EstimateCost(req)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return fallbackUsed
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Transfer-Encoding", "chunked")

	responseCommitted := false

	for {
		rapi, waitUntil, err := g.scheduler.PickAvailable(lapi.ID, rapis)
		if err != nil {
			if errors.Is(err, scheduler.ErrAllRAPIUnavailable) {
				if waitErr := g.scheduler.Wait(r.Context(), lapi.ID, waitUntil); waitErr != nil {
					g.sendErrorStream(w, flusher, waitErr.Error())
					return fallbackUsed
				}
				continue
			}
			g.sendErrorStream(w, flusher, err.Error())
			return fallbackUsed
		}

		effectiveURL := models.NormalizeToCompletionsURL(rapi.BaseURL)
		if effectiveURL == "" {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "invalid url")
			continue
		}

		if rapi.ID != rapis[0].ID {
			fallbackUsed = true
		}

		upstreamBody, _ := replaceModelInBody(body, req.Model, rapi.Model)
		startTime := time.Now()
		token, err := g.tokenProvider.FetchTokenForPlatform(r.Context(), rapi.PlatformID, rapi.Token, rapi.IsDynamic, rapi.TokenCommand)
		if err != nil {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "token fetch failed")
			if g.log != nil {
				g.log.RecordError(requestID, "Failed to get token for "+rapi.Alias+": "+err.Error(), "UPSTREAM_SENT")
			}
			continue
		}

		resp, _, _, err := g.doUpstreamRequest(r.Context(), rapi, effectiveURL, upstreamBody, token, requestID, retryCount)
		latencyMs := int(time.Since(startTime).Milliseconds())

		if err != nil {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, err.Error())
			if g.log != nil {
				g.log.RecordError(requestID, "RAPI "+rapi.Alias+" failed: "+err.Error(), "UPSTREAM_RESPONSE")
			}
			g.notifyService.PublishAsync(fmt.Sprintf("RAPI %s failed: %v", rapi.Alias, err), "RAPI Error")
			continue
		}

		// Extract token usage from response headers
		tokensUsed := extractTokenUsage(resp.Header)
		if tokensUsed <= 0 {
			tokensUsed = estimatedTokens
		}

		// Success path
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			g.scheduler.RecordRequest(rapi.ID, tokensUsed)

			if g.log != nil {
				g.log.RecordUpstreamResponse(requestID, resp.StatusCode, respHeaders(resp), "", latencyMs, tokensUsed)
			}

			g.notifyService.PublishAsync(fmt.Sprintf("Streaming from %s", rapi.Alias), "Active Route")

			streamChan := make(chan []byte, 100)
			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				defer wg.Done()
				defer close(streamChan)
				buf := make([]byte, 4096)
				for {
					n, err := resp.Body.Read(buf)
					if n > 0 {
						data := make([]byte, n)
						copy(data, buf[:n])
						select {
						case streamChan <- data:
						case <-r.Context().Done():
							return
						}
					}
					if err != nil {
						if err != io.EOF {
							select {
							case streamChan <- []byte(fmt.Sprintf("[ERROR] Read error: %v\n", err)):
							case <-r.Context().Done():
							}
						}
						return
					}
				}
			}()

			for data := range streamChan {
				if strings.HasPrefix(string(data), "[ERROR]") {
					continue
				}
				responseCommitted = true
				w.Write(data)
				flusher.Flush()
			}

			resp.Body.Close()
			if responseCommitted {
				g.scheduler.MarkSuccess(rapi.ID)
			} else {
				// Stream ended without any data — treat as failure
				g.scheduler.MarkFailure(rapi.ID, time.Time{}, "empty stream")
				continue
			}
			return fallbackUsed
		}

		// 401: refresh token and retry same RAPI once
		if resp.StatusCode == http.StatusUnauthorized {
			resp.Body.Close()
			if g.log != nil {
				g.log.RecordError(requestID, "RAPI "+rapi.Alias+" returned 401, refreshing token", "UPSTREAM_RESPONSE")
			}
			newToken, refreshErr := g.tokenProvider.RefreshPlatformOnDemand(r.Context(), rapi.PlatformID, rapi.TokenCommand)
			if refreshErr == nil && newToken != "" {
				// Retry same RAPI with new token
				upstreamBody2, _ := replaceModelInBody(body, req.Model, rapi.Model)
				resp2, _, _, err2 := g.doUpstreamRequest(r.Context(), rapi, effectiveURL, upstreamBody2, newToken, requestID, retryCount)
				if err2 == nil && resp2.StatusCode >= 200 && resp2.StatusCode < 300 {
					tokensUsed2 := extractTokenUsage(resp2.Header)
					if tokensUsed2 <= 0 {
						tokensUsed2 = estimatedTokens
					}
					g.scheduler.RecordRequest(rapi.ID, tokensUsed2)
					g.scheduler.MarkSuccess(rapi.ID)
					g.notifyService.PublishAsync(fmt.Sprintf("Streaming from %s (after token refresh)", rapi.Alias), "Token Refresh")

					streamChan := make(chan []byte, 100)
					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer close(streamChan)
						buf := make([]byte, 4096)
						for {
							n, err := resp2.Body.Read(buf)
							if n > 0 {
								data := make([]byte, n)
								copy(data, buf[:n])
								select {
								case streamChan <- data:
								case <-r.Context().Done():
									return
								}
							}
							if err != nil {
								return
							}
						}
					}()
					for data := range streamChan {
						responseCommitted = true
						w.Write(data)
						flusher.Flush()
					}
					resp2.Body.Close()
					if responseCommitted {
						return fallbackUsed
					}
					// Empty stream after refresh — fall through to failover
				} else if resp2 != nil {
					resp2.Body.Close()
				}
			}
			// Token refresh failed or retry still failed — fall through to failover
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "401 unauthorized")
			continue
		}

		// All other non-2xx: absorb error, mark failure, try next RAPI
		resp.Body.Close()
		g.scheduler.RecordRequest(rapi.ID, tokensUsed)
		retryAt := scheduler.RetryAt(resp.Header, time.Time{})
		reason := fmt.Sprintf("upstream %d", resp.StatusCode)
		g.scheduler.MarkFailure(rapi.ID, retryAt, reason)

		if g.log != nil {
			g.log.RecordError(requestID, fmt.Sprintf("RAPI %s returned %d", rapi.Alias, resp.StatusCode), "UPSTREAM_RESPONSE")
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
			g.notifyService.PublishAsync(fmt.Sprintf("RAPI %s returned %d, failing over", rapi.Alias, resp.StatusCode), "Failover")
		}
		retryCount++
		continue
	}
}

// handleNonStreamingRequest handles non-streaming requests with full error absorption.
func (g *ProxyGateway) handleNonStreamingRequest(w http.ResponseWriter, r *http.Request, body []byte, req *models.ProxyRequest, lapi *models.LAPI, rapis []models.RAPIWithPlatform, requestID string, sessionID string) bool {
	fallbackUsed := false
	retryCount := 0
	estimatedTokens := scheduler.EstimateCost(req)

	for {
		rapi, waitUntil, err := g.scheduler.PickAvailable(lapi.ID, rapis)
		if err != nil {
			if errors.Is(err, scheduler.ErrAllRAPIUnavailable) {
				if waitErr := g.scheduler.Wait(r.Context(), lapi.ID, waitUntil); waitErr != nil {
					http.Error(w, waitErr.Error(), http.StatusServiceUnavailable)
					return fallbackUsed
				}
				continue
			}
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return fallbackUsed
		}

		effectiveURL := models.NormalizeToCompletionsURL(rapi.BaseURL)
		if effectiveURL == "" {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "invalid url")
			continue
		}

		if rapi.ID != rapis[0].ID {
			fallbackUsed = true
		}

		upstreamBody, _ := replaceModelInBody(body, req.Model, rapi.Model)
		startTime := time.Now()
		token, err := g.tokenProvider.FetchTokenForPlatform(r.Context(), rapi.PlatformID, rapi.Token, rapi.IsDynamic, rapi.TokenCommand)
		if err != nil {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "token fetch failed")
			if g.log != nil {
				g.log.RecordError(requestID, "Failed to get token for "+rapi.Alias+": "+err.Error(), "UPSTREAM_SENT")
			}
			continue
		}

		resp, _, _, err := g.doUpstreamRequest(r.Context(), rapi, effectiveURL, upstreamBody, token, requestID, retryCount)
		latencyMs := int(time.Since(startTime).Milliseconds())

		if err != nil {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, err.Error())
			if g.log != nil {
				g.log.RecordError(requestID, "RAPI "+rapi.Alias+" failed: "+err.Error(), "UPSTREAM_RESPONSE")
			}
			continue
		}

		tokensUsed := extractTokenUsage(resp.Header)
		if tokensUsed <= 0 {
			tokensUsed = estimatedTokens
		}

		// Success path
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			g.scheduler.RecordRequest(rapi.ID, tokensUsed)
			g.scheduler.MarkSuccess(rapi.ID)

			if g.log != nil {
				g.log.RecordUpstreamResponse(requestID, resp.StatusCode, respHeaders(resp), string(respBody), latencyMs, tokensUsed)
			}

			w.Header().Set("Content-Type", "application/json")
			w.Write(respBody)
			return fallbackUsed
		}

		// 401: refresh token and retry same RAPI once
		if resp.StatusCode == http.StatusUnauthorized {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if g.log != nil {
				g.log.RecordError(requestID, "RAPI "+rapi.Alias+" returned 401, refreshing token", "UPSTREAM_RESPONSE")
			}
			newToken, refreshErr := g.tokenProvider.RefreshPlatformOnDemand(r.Context(), rapi.PlatformID, rapi.TokenCommand)
			if refreshErr == nil && newToken != "" {
				upstreamBody2, _ := replaceModelInBody(body, req.Model, rapi.Model)
				resp2, _, _, err2 := g.doUpstreamRequest(r.Context(), rapi, effectiveURL, upstreamBody2, newToken, requestID, retryCount)
				if err2 == nil && resp2.StatusCode >= 200 && resp2.StatusCode < 300 {
					respBody2, _ := io.ReadAll(resp2.Body)
					resp2.Body.Close()
					tokensUsed2 := extractTokenUsage(resp2.Header)
					if tokensUsed2 <= 0 {
						tokensUsed2 = estimatedTokens
					}
					g.scheduler.RecordRequest(rapi.ID, tokensUsed2)
					g.scheduler.MarkSuccess(rapi.ID)

					if g.log != nil {
						g.log.RecordUpstreamResponse(requestID, resp2.StatusCode, respHeaders(resp2), string(respBody2), latencyMs, tokensUsed2)
					}

					w.Header().Set("Content-Type", "application/json")
					w.Write(respBody2)
					return fallbackUsed
				} else if resp2 != nil {
					resp2.Body.Close()
				}
			}
			g.scheduler.RecordRequest(rapi.ID, tokensUsed)
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "401 unauthorized")
			continue
		}

		// All other non-2xx: absorb error, mark failure, try next RAPI
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		g.scheduler.RecordRequest(rapi.ID, tokensUsed)
		retryAt := scheduler.RetryAt(resp.Header, time.Time{})
		reason := fmt.Sprintf("upstream %d", resp.StatusCode)
		g.scheduler.MarkFailure(rapi.ID, retryAt, reason)

		if g.log != nil {
			g.log.RecordError(requestID, fmt.Sprintf("RAPI %s returned %d", rapi.Alias, resp.StatusCode), "UPSTREAM_RESPONSE")
		}
		retryCount++
		continue
	}
}

// doUpstreamRequest builds and executes an upstream HTTP request.
// Returns the response, status code, body (on error), and any transport error.
func (g *ProxyGateway) doUpstreamRequest(ctx context.Context, rapi models.RAPIWithPlatform, url string, body []byte, token string, requestID string, retryCount int) (*http.Response, int, []byte, error) {
	upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, 0, nil, err
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	if g.log != nil {
		headers := make(map[string]string)
		for k, v := range upstreamReq.Header {
			if len(v) > 0 {
				headers[k] = logger.SanitizeKey(v[0])
			}
		}
		g.log.RecordUpstreamSent(requestID, rapi.Alias, url, headers, string(body), retryCount)
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
