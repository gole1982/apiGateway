package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"gateway/internal/apiformat"
	"gateway/internal/db"
	"gateway/internal/entity"
	"gateway/internal/logger"
	"gateway/internal/models"
	"gateway/internal/notify"
	"gateway/internal/scheduler"
)

// keyModelBlock is the in-memory view of a key×model capability block: the
// platform denied this key for this model. Expired blocks are ignored so the
// pair is retried naturally and re-blocked on the next denial.
type keyModelBlock struct {
	reason    string
	expiresAt time.Time
}

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

	// key×model capability blacklist (platform revoked a key's access to a
	// model). keyID → rapiID → block. Skipped at pick time; a key blocked for
	// one model keeps serving all others.
	blockMu          sync.RWMutex
	blocks           map[int64]map[int64]keyModelBlock
	capabilityBlockT time.Duration

	// recoveryRunning prevents overlapping background recovery scans. Recovery
	// is best-effort maintenance; callers should continue using normal failover
	// while another scan is in progress.
	recoveryRunning atomic.Bool
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

	gw := &ProxyGateway{
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
		scheduler:       scheduler.NewManager(schedulerCfg, &dbStore{db: db.Get()}),
		responseTimeout: respTimeout,
	}
	gw.loadKeyModelBlocks(schedulerCfg.CapabilityBlock)
	return gw
}

// dbStore implements entity.Store on top of the DB so entity transition
// effects persist state as a side effect of FSM transitions.
type dbStore struct {
	db *db.DB
}

func (s *dbStore) SetPlatformAvailable(id int64, available bool) error {
	return s.db.SetPlatformAvailable(id, available)
}

func (s *dbStore) SetRAPIUnavailable(id int64, available bool, reason string) error {
	return s.db.SetRAPIUnavailableWithReason(id, available, reason)
}

func (s *dbStore) MarkKeyPermanentFailure(keyID int64, reason string) error {
	return s.db.MarkKeyPermanentFailure(keyID, reason)
}

func (s *dbStore) MarkKeyTemporaryFailure(keyID int64, reason string) error {
	return s.db.MarkKeyTemporaryFailure(keyID, reason)
}

func (s *dbStore) ClearKeyFailure(keyID int64) error {
	return s.db.ClearKeyFailure(keyID)
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
	if !g.recoveryRunning.CompareAndSwap(false, true) {
		return &RetryReport{}
	}
	defer g.recoveryRunning.Store(false)

	concurrency, perProbeTimeout := normalizeProbe(concurrency, perProbeTimeoutSec)

	report := &RetryReport{}
	probeClient := &http.Client{Timeout: perProbeTimeout}

	// 1) Recover platform-level failures first (mirror /api/platforms/restore).
	// The probe outcome drives the Platform entity FSM, which persists
	// platform.available as a transition effect.
	platforms, _ := g.db.GetPlatforms()
	var failedPlatformIDs []int64
	for _, p := range platforms {
		if !p.Enabled || p.Available {
			continue
		}
		start := time.Now()
		pe := g.scheduler.PlatformEntity(p.ID, p.Available)
		if ok, reason := g.probePlatform(ctx, p, probeClient); ok {
			pe.OnDetectSuccess()
			logger.DefaultConsole().Info("gateway", "[RECOVER] platform restored",
				"name", p.Name, "platform_id", p.ID, "duration_ms", time.Since(start).Milliseconds())
		} else {
			failedPlatformIDs = append(failedPlatformIDs, p.ID)
			pe.OnDetectFailure()
			logger.DefaultConsole().Warn("gateway", "[RECOVER] platform still failing",
				"name", p.Name, "platform_id", p.ID, "reason", reason)
		}
	}
	failedPlatform := make(map[int64]bool, len(failedPlatformIDs))
	for _, id := range failedPlatformIDs {
		failedPlatform[id] = true
	}

	// 2) Collect RAPIs still unavailable, attaching their (model-filtered) key
	// pools so the probe exercises the same credentials real requests would use
	// — a key outside the RAPI's key_ids whitelist must not revive it.
	rapis, _ := g.db.GetRAPIs()
	var unhealthy []models.RAPIWithPlatform
	for _, r := range rapis {
		if r.Enabled && !r.Available {
			unhealthy = append(unhealthy, r)
		}
	}
	g.recoverRAPIs(ctx, unhealthy, failedPlatform, concurrency, probeClient, perProbeTimeout, report)
	return report
}

// normalizeProbe clamps the concurrency and per-probe timeout used by the
// recovery entry points.
func normalizeProbe(concurrency, perProbeTimeoutSec int) (int, time.Duration) {
	if concurrency < 1 {
		concurrency = 1
	}
	perProbeTimeout := time.Duration(perProbeTimeoutSec) * time.Second
	if perProbeTimeout <= 0 {
		perProbeTimeout = 15 * time.Second
	}
	return concurrency, perProbeTimeout
}

// RecoverPlatformRAPIs probes the RAPIs of a single platform that are persisted
// as unavailable — used after a key change so models previously marked
// unavailable because all their keys were dead are re-tested with the (new) key
// pool and restored only if they actually respond. RAPIs whose pool is still
// dead stay marked unavailable.
func (g *ProxyGateway) RecoverPlatformRAPIs(ctx context.Context, platformID int64, concurrency, perProbeTimeoutSec int) *RetryReport {
	if !g.recoveryRunning.CompareAndSwap(false, true) {
		return &RetryReport{}
	}
	defer g.recoveryRunning.Store(false)

	concurrency, perProbeTimeout := normalizeProbe(concurrency, perProbeTimeoutSec)
	report := &RetryReport{}
	probeClient := &http.Client{Timeout: perProbeTimeout}
	rapis, _ := g.db.GetRAPIs()
	var unhealthy []models.RAPIWithPlatform
	for _, r := range rapis {
		if r.PlatformID == platformID && r.Enabled && !r.Available {
			unhealthy = append(unhealthy, r)
		}
	}
	g.recoverRAPIs(ctx, unhealthy, nil, concurrency, probeClient, perProbeTimeout, report)
	return report
}

// RecoverRAPIs probes the given RAPIs (only those currently enabled but
// unavailable) and restores the ones that respond with a supported format.
// Used e.g. after a model's key_ids whitelist gains a usable key.
func (g *ProxyGateway) RecoverRAPIs(ctx context.Context, rapiIDs []int64, concurrency, perProbeTimeoutSec int) *RetryReport {
	if !g.recoveryRunning.CompareAndSwap(false, true) {
		return &RetryReport{}
	}
	defer g.recoveryRunning.Store(false)

	concurrency, perProbeTimeout := normalizeProbe(concurrency, perProbeTimeoutSec)
	report := &RetryReport{}
	probeClient := &http.Client{Timeout: perProbeTimeout}
	want := make(map[int64]bool, len(rapiIDs))
	for _, id := range rapiIDs {
		want[id] = true
	}
	rapis, _ := g.db.GetRAPIs()
	var unhealthy []models.RAPIWithPlatform
	for _, r := range rapis {
		if want[r.ID] && r.Enabled && !r.Available {
			unhealthy = append(unhealthy, r)
		}
	}
	g.recoverRAPIs(ctx, unhealthy, nil, concurrency, probeClient, perProbeTimeout, report)
	return report
}

// recoverRAPIs probes each RAPI in unhealthy concurrently and restores those
// that answer with a supported format (clears persisted unavailable state +
// revalidates the scheduler entity). failedPlatform marks platforms whose
// base_url is still down — those RAPIs are reported as failed without probing.
func (g *ProxyGateway) recoverRAPIs(ctx context.Context, unhealthy []models.RAPIWithPlatform, failedPlatform map[int64]bool, concurrency int, probeClient *http.Client, perProbeTimeout time.Duration, report *RetryReport) {
	g.loadKeysForRAPIs(unhealthy)
	report.Probed = len(unhealthy)
	if len(unhealthy) == 0 {
		return
	}

	// Probe concurrently.
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

			// Probe with the same credentials real requests use: the first usable
			// key from the RAPI's own (key_ids-filtered) pool, falling back to the
			// first usable platform key, not the (often empty) legacy platform
			// token — the JD recovery blind spot where r.Token was empty and keys
			// lived in platform_keys.
			probeToken := g.firstUsableFromKeys(r.Keys)
			if probeToken == "" {
				probeToken = g.firstUsableKey(r.PlatformID, r.Token)
			}
			results := apiformat.DetectFormats(probeCtx, r.BaseURL, r.Model, probeToken, probeClient)
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
				logger.DefaultConsole().Warn("gateway", "[RECOVER] rapi still failing",
					"rapi", r.Alias, "rapi_id", r.ID, "duration_ms", item.DurationMs)
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
			logger.DefaultConsole().Info("gateway", "[RECOVER] rapi restored",
				"rapi", r.Alias, "rapi_id", r.ID, "duration_ms", item.DurationMs)
		}(rapi)
	}
	wg.Wait()
}

// firstUsableFromKeys returns the token of the first enabled, not-permanently-
// failed, not-expired key in the given (already model-filtered) pool, or "" if
// none is usable. Uses the shared entity.KeyRowUsable predicate so probes
// exercise the same credentials real requests use.
func (g *ProxyGateway) firstUsableFromKeys(keys []models.PlatformKey) string {
	now := time.Now()
	for _, k := range keys {
		if entity.KeyRowUsable(k, now) {
			return k.Token
		}
	}
	return ""
}

// firstUsableKey returns the token of the first enabled, not-permanently-failed,
// not-expired PlatformKey for the platform — using the shared entity.KeyRowUsable
// predicate so probes exercise the same credentials real requests use. Falls back
// to the legacy platform-level token when no usable key exists (matching the old
// behavior for token-based platforms like Google Gemini's platform token).
func (g *ProxyGateway) firstUsableKey(platformID int64, platformToken string) string {
	keys, err := g.db.GetPlatformKeys(platformID)
	if err == nil && len(keys) > 0 {
		now := time.Now()
		for _, k := range keys {
			if entity.KeyRowUsable(k, now) {
				return k.Token
			}
		}
	}
	return platformToken
}

// probePlatform mirrors the connectivity check of /api/platforms/restore: it
// issues GET {baseURL}/v1/models with the first usable platform key (or the
// platform token as fallback) and reports success on HTTP 200.
// Returns (ok, reason).
func (g *ProxyGateway) probePlatform(ctx context.Context, p models.Platform, client *http.Client) (bool, string) {
	token := g.firstUsableKey(p.ID, p.Token)
	var req *http.Request
	var err error
	if apiformat.IsGoogleNativeBaseURL(p.BaseURL) {
		// Native Google Generative Language API: list models via /v1beta/models?key=
		// with x-goog-api-key auth. The OpenAI path (/v1/models + Bearer) 404s/401s here.
		modelsURL := apiformat.BuildGoogleListModelsURL(p.BaseURL, token)
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, modelsURL, nil)
		if err != nil {
			return false, err.Error()
		}
		req.Header.Set(apiformat.GoogleAPIKeyHeader, token)
	} else {
		// OpenAI-compatible platforms.
		modelsURL := apiformat.NormalizeModelsBaseURL(p.BaseURL) + "/v1/models"
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, modelsURL, nil)
		if err != nil {
			return false, err.Error()
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
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

// RemoveKey notifies the scheduler that a PlatformKey was deleted, dropping
// its in-memory entity (cooldown timer / recovery scan participation) and any
// key×model capability blocks referencing it.
func (g *ProxyGateway) RemoveKey(keyID int64) {
	g.scheduler.RemoveKey(keyID)
	g.clearBlocksForKey(keyID)
}

// loadKeyModelBlocks hydrates the in-memory key×model block cache from the DB
// (skipping already-expired entries) and records the block TTL.
func (g *ProxyGateway) loadKeyModelBlocks(ttl time.Duration) {
	g.blockMu.Lock()
	defer g.blockMu.Unlock()
	g.capabilityBlockT = ttl
	if g.capabilityBlockT <= 0 {
		g.capabilityBlockT = 24 * time.Hour
	}
	g.blocks = make(map[int64]map[int64]keyModelBlock)
	rows, err := g.db.GetKeyModelBlocks()
	if err != nil {
		logger.DefaultConsole().Warn("gateway", "[BLOCK] failed to load key×model blocks", "error", err.Error())
		return
	}
	now := time.Now()
	for _, b := range rows {
		if !b.ExpiresAt.IsZero() && b.ExpiresAt.Before(now) {
			continue
		}
		if g.blocks[b.KeyID] == nil {
			g.blocks[b.KeyID] = make(map[int64]keyModelBlock)
		}
		g.blocks[b.KeyID][b.RAPIID] = keyModelBlock{reason: b.Reason, expiresAt: b.ExpiresAt}
	}
}

// blockKeyForModel records a key×model capability block in memory and persists
// it with a fresh TTL. The pair is skipped for this model until it expires.
func (g *ProxyGateway) blockKeyForModel(keyID, rapiID int64, reason string) {
	exp := time.Now().Add(g.capabilityBlockT)
	g.blockMu.Lock()
	if g.blocks[keyID] == nil {
		g.blocks[keyID] = make(map[int64]keyModelBlock)
	}
	g.blocks[keyID][rapiID] = keyModelBlock{reason: reason, expiresAt: exp}
	g.blockMu.Unlock()
	if err := g.db.BlockKeyForModel(keyID, rapiID, reason, exp); err != nil {
		logger.DefaultConsole().Warn("gateway", "[BLOCK] persist key×model block failed",
			"key_id", keyID, "rapi_id", rapiID, "error", err.Error())
	}
}

// UnblockKeyForModel removes a key×model capability block (a request/probe
// succeeded for the pair, or the operator cleared it).
func (g *ProxyGateway) UnblockKeyForModel(keyID, rapiID int64) {
	g.blockMu.Lock()
	if m := g.blocks[keyID]; m != nil {
		delete(m, rapiID)
		if len(m) == 0 {
			delete(g.blocks, keyID)
		}
	}
	g.blockMu.Unlock()
	if err := g.db.UnblockKeyForModel(keyID, rapiID); err != nil {
		logger.DefaultConsole().Warn("gateway", "[BLOCK] remove key×model block failed",
			"key_id", keyID, "rapi_id", rapiID, "error", err.Error())
	}
}

// isBlocked reports whether (key, model) is currently capability-blocked and
// returns the recorded reason. Expired blocks are ignored (natural retry) and
// lazily purged from memory so stale entries do not accumulate.
func (g *ProxyGateway) isBlocked(keyID, rapiID int64) (string, bool) {
	g.blockMu.RLock()
	m := g.blocks[keyID]
	if m == nil {
		g.blockMu.RUnlock()
		return "", false
	}
	b, ok := m[rapiID]
	if !ok {
		g.blockMu.RUnlock()
		return "", false
	}
	if !b.expiresAt.IsZero() && !b.expiresAt.After(time.Now()) {
		g.blockMu.RUnlock()
		// Upgrade to a write lock and purge the expired entry.
		g.blockMu.Lock()
		if mm := g.blocks[keyID]; mm != nil {
			delete(mm, rapiID)
			if len(mm) == 0 {
				delete(g.blocks, keyID)
			}
		}
		g.blockMu.Unlock()
		return "", false
	}
	g.blockMu.RUnlock()
	return b.reason, true
}

// filterBlockedKeys drops keys that are capability-blocked for rapiID so they
// are never tried for that model (each attempt would burn a 404). The synthetic
// platform-token key (ID -1) is never blocked.
func (g *ProxyGateway) filterBlockedKeys(keys []models.PlatformKey, rapiID int64) []models.PlatformKey {
	out := make([]models.PlatformKey, 0, len(keys))
	for _, k := range keys {
		if _, blocked := g.isBlocked(k.ID, rapiID); blocked {
			continue
		}
		out = append(out, k)
	}
	return out
}

// blockedReasons returns the recorded reasons of a RAPI's pool keys that are
// capability-blocked for it (used to explain why the pool is dead).
func (g *ProxyGateway) blockedReasons(keys []models.PlatformKey, rapiID int64) []string {
	var reasons []string
	for _, k := range keys {
		if r, blocked := g.isBlocked(k.ID, rapiID); blocked {
			reasons = append(reasons, fmt.Sprintf("%s（%s）", k.Label, r))
		}
	}
	return reasons
}

// clearBlocksForKey drops every capability block referencing a key (deleted).
func (g *ProxyGateway) clearBlocksForKey(keyID int64) {
	g.blockMu.Lock()
	delete(g.blocks, keyID)
	g.blockMu.Unlock()
	if err := g.db.ClearKeyModelBlocksForKey(keyID); err != nil {
		logger.DefaultConsole().Warn("gateway", "[BLOCK] clear key×model blocks failed",
			"key_id", keyID, "error", err.Error())
	}
}

// FirstUsableKey returns the token of the first usable PlatformKey for the
// platform, falling back to the platform-level token. Shared by probe paths so
// recovery exercises the same credentials as real requests.
func (g *ProxyGateway) FirstUsableKey(platformID int64, platformToken string) string {
	return g.firstUsableKey(platformID, platformToken)
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
			writeGatewayJSONError(w, http.StatusBadRequest, fmt.Sprintf("Failed to parse %s request: %s", clientFormat, err.Error()), "invalid_request")
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
		// Re-inject model into canonical body. Use ReplaceModelField (lossless
		// JSON decode/encode) rather than a full OpenAI→OpenAI conversion round-trip.
		canonicalBody = apiformat.ReplaceModelField(canonicalBody, req.Model)
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
	// Attach request_id to every console log line emitted within this request
	// lifetime so the operator can correlate the structured stderr stream with
	// the SQLite request_logs row sharing the same id.
	reqLog := logger.DefaultConsole().With("request_id", requestID)
	reqLog.Info("gateway", "[REQ] incoming request",
		"model", modelName, "stream", isStream, "format", string(clientFormat), "from", r.RemoteAddr)
	lapi, err := g.db.GetLAPIByAlias(strings.ToLower(modelName))
	if err != nil {
		reqLog.Warn("gateway", "[ROUTE] unknown model", "model", modelName)
		writeGatewayJSONError(w, http.StatusUnauthorized, "Unknown model: "+modelName, "invalid_request")
		if g.log != nil {
			g.log.RecordRoutingDecision(requestID, modelName, nil)
			g.log.RecordError(requestID, "Unknown model: "+modelName, "ROUTING_DECISION")
			g.log.RecordClientResponse(requestID, http.StatusUnauthorized, int(time.Since(startTime).Milliseconds()), false)
		}
		return
	}

	if !lapi.Enabled {
		reqLog.Warn("gateway", "[ROUTE] model is disabled", "model", modelName)
		writeGatewayJSONError(w, http.StatusServiceUnavailable, "Model "+modelName+" is disabled", "service_unavailable")
		if g.log != nil {
			g.log.RecordRoutingDecision(requestID, lapi.Alias, nil)
			g.log.RecordError(requestID, "Model is disabled: "+modelName, "ROUTING_DECISION")
			g.log.RecordClientResponse(requestID, http.StatusServiceUnavailable, int(time.Since(startTime).Milliseconds()), false)
		}
		return
	}

	rapis, err := g.db.GetEnabledRAPIsForLAPI(lapi.ID)
	if err != nil || len(rapis) == 0 {
		reqLog.Error("gateway", "[ROUTE] no backends available", "model", modelName)
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
// Each RAPI's Keys is the platform key pool filtered by its key_ids whitelist
// (empty = all platform keys), so PickAvailableKey / KeysHardDead judge the
// per-model key pool instead of the whole platform pool.
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
		rapis[i].Keys = filterKeysByKeyIDs(loaded[pid], rapis[i].KeyIDs)
	}
}

// filterKeysByKeyIDs returns only the keys whose ID is listed in keyIDsCSV
// (comma-separated platform_keys IDs), preserving the original order. An empty
// whitelist means "all keys allowed" (the default). Unknown IDs are dropped
// silently — the DB row is authoritative for key existence.
func filterKeysByKeyIDs(keys []models.PlatformKey, keyIDsCSV string) []models.PlatformKey {
	csv := strings.TrimSpace(keyIDsCSV)
	if csv == "" {
		return keys
	}
	allowed := make(map[int64]bool)
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if id, err := strconv.ParseInt(part, 10, 64); err == nil {
			allowed[id] = true
		}
	}
	if len(allowed) == 0 {
		return keys
	}
	filtered := make([]models.PlatformKey, 0, len(keys))
	for _, k := range keys {
		if allowed[k.ID] {
			filtered = append(filtered, k)
		}
	}
	return filtered
}

// tryKeyForRAPI tries each PlatformKey for a given RAPI in sequence (round-robin).
// Returns (resp, keyID, err). On 429/503 it marks the key failed and tries the next key.
// If all keys are cooling, it aligns the whole pool to the pool-standard cooldown
// (the earliest key recoverAt), cools the RAPI to the same moment and returns
// ErrAllKeysUnavailable — the caller then moves to the next RAPI in the chain
// instead of waiting on this key pool.
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
			KeyIndex:   0,
			PlatformID: rapi.PlatformID,
			Token:      rapi.Token,
			Enabled:    true,
		}
		keys = []models.PlatformKey{syntheticKey}
	}

	for {
		// Re-filter each pass: a key denied on the previous iteration must not
		// be retried within this request (each attempt would burn a 404). The
		// synthetic platform-token key (ID -1) is never blocked.
		keys = g.filterBlockedKeys(keys, rapi.ID)
		if len(keys) == 0 {
			// Every key is capability-blocked for this model — escalate so the
			// caller surfaces the block reason (handleAllKeysUnavailable) and
			// persists the model as unavailable instead of spinning on 404s.
			return nil, 0, scheduler.ErrAllKeysUnavailable
		}
		key, keyNextAvail, err := g.scheduler.PickAvailableKey(keys)
		if err != nil {
			// All keys for this RAPI are cooling — align the pool to the pool
			// standard (earliest key recoverAt) so the RAPI cooldown matches,
			// then escalate; the caller proceeds to the next chain node
			// instead of spinning on this pool.
			g.markPoolExhausted(rapi, keys, keyNextAvail, requestID)
			return nil, 0, scheduler.ErrAllKeysUnavailable
		}

		token := key.Token
		// Scoped logger so every upstream attempt line carries request_id plus the
		// key/rapi context the operator needs to diagnose per-key failures.
		keyLog := logger.DefaultConsole().With("request_id", requestID).With("rapi", rapi.Alias, "key_id", key.ID)

		resp, _, doErr := g.doUpstreamRequest(ctx, rapi, effectiveURL, upstreamBody, token, requestID, retryCount, targetFormat, key.ID)
		if doErr != nil {
			isTimeout := isTimeoutError(doErr)
			keyLog.Error("gateway", "[ERR] upstream request failed",
				"do_error", doErr.Error(), "timeout", isTimeout)
			if g.log != nil {
				g.log.RecordError(requestID, fmt.Sprintf("rapi=%s key=%d: %s", rapi.Alias, key.ID, doErr.Error()), "UPSTREAM_RESPONSE")
			}
			g.scheduler.MarkKeyFailure(key.ID, time.Time{}, doErr.Error(), isTimeout)
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Entity effect persists ClearKeyFailure (failure_type=0).
			g.scheduler.MarkKeySuccess(key.ID)
			// A successful request proves the key serves this model — clear any
			// stale capability block (e.g. the platform re-granted access).
			if key.ID > 0 {
				g.UnblockKeyForModel(key.ID, rapi.ID)
			}
			return resp, key.ID, nil
		}

		// Read and log the upstream error body for all non-2xx responses.
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		keyLog.Error("gateway", "[ERR] upstream non-2xx",
			"status", resp.StatusCode, "body", string(errBody))
		if g.log != nil {
			g.log.RecordError(requestID,
				fmt.Sprintf("rapi=%s key=%d upstream %d: %s", rapi.Alias, key.ID, resp.StatusCode, string(errBody)),
				"UPSTREAM_RESPONSE")
		}

		switch entity.ClassifyFailure(resp.StatusCode) {

		case entity.ScopeSystem:
			// 401 Unauthorized: key token is invalid — mark permanent failure
			// (entity persists failure_type=2 via the Store effect). Does NOT
			// touch enabled (decoupled from user intent). The Key entity stays
			// in PermanentFailed (reconciled from the row) so PickAvailableKey
			// skips it.
			reason := fmt.Sprintf("[认证失败] upstream 401: %s", string(errBody))
			keyLog.Error("gateway", "[KEY-FAIL] 401 unauthorized, marking permanent failure", "status", resp.StatusCode)
			g.scheduler.MarkKeyPlatformFailure(key.ID, reason)
			if g.log != nil {
				g.log.RecordError(requestID, reason, "UPSTREAM_RESPONSE")
			}
			if g.notifyService != nil {
				g.notifyService.PublishEvent(
					"error", notify.MenuKeys, "failure",
					fmt.Sprintf("Key #%d 认证失败（401），已标记永久失效", key.KeyIndex),
					fmt.Sprintf("模型 %s [%s]", rapi.Alias, rapi.PlatformName),
					key.ID,
				)
			}
			continue

		case entity.ScopePlatform:
			// 平台级：欠费/封号/模型失效。可恢复计费错误（积分不足/余额不足/
			// 欠费，如京东 code 1058）降级为临时失败——短冷却 + failure_type=1，
			// 充值后自动回归；真正的封号/硬禁才标记永久失败。continue 让同 RAPI
			// 的其他 key 继续尝试，而非整体降级。
			reason := fmt.Sprintf("[平台级] upstream %d: %s", resp.StatusCode, string(errBody))
			if entity.IsRecoverableBillingError(string(errBody)) {
				// 长冷却（BillingCooldown，默认 30 分钟）：欠费期间不每笔请求都
				// 白打一次该 key，充值后冷却到期自动回归。
				keyLog.Warn("gateway", "[KEY-RECOVER] billing error treated as temporary",
					"status", resp.StatusCode, "reason", reason)
				recoverAt := g.scheduler.MarkKeyBillingFailure(key.ID, reason)
				if g.notifyService != nil {
					g.notifyService.PublishEvent("error", notify.MenuKeys, "cooling",
						fmt.Sprintf("Key #%d 计费错误冷却（%s）", key.KeyIndex, rapi.PlatformName),
						reason, key.ID)
					g.notifyService.RecordUnread(notify.MenuKeys, "cooling",
						fmt.Sprintf("Key #%d 冷却中（计费，至 %s）", key.KeyIndex, recoverAt.Format("15:04:05")),
						reason, key.ID)
				}
				if g.log != nil {
					g.log.RecordError(requestID, reason+" (可恢复计费错误，已按临时失败处理)", "UPSTREAM_RESPONSE")
				}
				continue
			}
			// 403 with a "model not found" body = the key lost access to THIS
			// model (capability), not an account-level problem — block the pair
			// so the key keeps serving its other models.
			if resp.StatusCode == 403 && entity.IsCapabilityMismatch(string(errBody)) {
				keyLog.Warn("gateway", "[KEY-BLOCK] capability mismatch, blocking key×model",
					"status", resp.StatusCode, "reason", reason)
				g.blockKeyForModel(key.ID, rapi.ID, reason)
				if g.notifyService != nil {
					g.notifyService.PublishEvent("error", notify.MenuKeys, "failure",
						fmt.Sprintf("Key #%d 无权访问模型 %s，已加入能力黑名单", key.KeyIndex, rapi.Alias),
						reason, key.ID)
					g.notifyService.RecordUnread(notify.MenuModels, "failure",
						fmt.Sprintf("模型 %s 对 Key #%d 失效", rapi.Alias, key.KeyIndex), reason, rapi.ID)
				}
				if g.log != nil {
					g.log.RecordError(requestID, reason+"（该 Key 无权访问此模型，已加入能力黑名单）", "UPSTREAM_RESPONSE")
				}
				continue
			}
			keyLog.Error("gateway", "[KEY-FAIL] platform-level failure, marking permanent",
				"status", resp.StatusCode)
			g.scheduler.MarkKeyPlatformFailure(key.ID, reason)
			if g.log != nil {
				g.log.RecordError(requestID, reason, "UPSTREAM_RESPONSE")
			}
			if g.notifyService != nil {
				g.notifyService.PublishEvent(
					"error", notify.MenuKeys, "failure",
					fmt.Sprintf("Key #%d 平台受限（%d），已标记永久失效", key.KeyIndex, resp.StatusCode),
					fmt.Sprintf("模型 %s [%s]", rapi.Alias, rapi.PlatformName),
					key.ID,
				)
			}
			continue

		default: // ScopeSession
			// 会话级：限流/内容违规/请求过长/临时故障 → key 短冷却 + 标记临时失败
			// (entity persists failure_type=1)。
			reason := fmt.Sprintf("upstream %d: %s", resp.StatusCode, string(errBody))
			// 400/404/422 with a "model not found" body = the key lost access to
			// THIS model (capability mismatch) — block the pair instead of
			// cooling the whole key, so it keeps serving its other models.
			if (resp.StatusCode == 400 || resp.StatusCode == 404 || resp.StatusCode == 422) &&
				entity.IsCapabilityMismatch(string(errBody)) {
				keyLog.Warn("gateway", "[KEY-BLOCK] capability mismatch, blocking key×model",
					"status", resp.StatusCode, "reason", reason)
				g.blockKeyForModel(key.ID, rapi.ID, reason)
				if g.notifyService != nil {
					g.notifyService.PublishEvent("error", notify.MenuKeys, "failure",
						fmt.Sprintf("Key #%d 无权访问模型 %s，已加入能力黑名单", key.KeyIndex, rapi.Alias),
						reason, key.ID)
					g.notifyService.RecordUnread(notify.MenuModels, "failure",
						fmt.Sprintf("模型 %s 对 Key #%d 失效", rapi.Alias, key.KeyIndex), reason, rapi.ID)
				}
				if g.log != nil {
					g.log.RecordError(requestID, reason+"（该 Key 无权访问此模型，已加入能力黑名单）", "UPSTREAM_RESPONSE")
				}
				continue
			}
			retryAt := scheduler.RetryAt(resp.Header, time.Time{})
			recoverAt := g.scheduler.MarkKeyTemporaryFailure(key.ID, retryAt, reason)
			// 429/5xx 冷却通知（限流类最有价值；capability/计费已在上文单独通知）。
			if g.notifyService != nil && (resp.StatusCode == 429 || resp.StatusCode >= 500) {
				g.notifyService.PublishEvent("error", notify.MenuKeys, "cooling",
					fmt.Sprintf("Key #%d 冷却（%s，%d）", key.KeyIndex, rapi.PlatformName, resp.StatusCode),
					reason, key.ID)
				g.notifyService.RecordUnread(notify.MenuKeys, "cooling",
					fmt.Sprintf("Key #%d 冷却中（至 %s）", key.KeyIndex, recoverAt.Format("15:04:05")),
					reason, key.ID)
			}
			continue
		}
	}
}

// markPoolExhausted 对"全部 key 冷却中"的软池执行池标准冷却：
// 以池内最早恢复时刻（第一个进入 429 的 key 的恢复时刻）为标准，对齐池内
// 所有 key 与 RAPI 的冷却，并写入请求日志，让日志页可见"池耗尽→冷却至
// HH:MM:SS→转下一节点"。poolRecoverAt 为零（无冷却信息）时退回旧短冷却。
func (g *ProxyGateway) markPoolExhausted(rapi models.RAPIWithPlatform, keys []models.PlatformKey, poolRecoverAt time.Time, requestID string) {
	ids := make([]int64, 0, len(keys))
	for _, k := range keys {
		if k.ID > 0 {
			ids = append(ids, k.ID)
		}
	}
	if poolRecoverAt.IsZero() {
		// 无池时间信息（例如池里全是能力黑名单外的死 key）→ 旧路径。
		g.handleAllKeysUnavailable(rapi, requestID, logger.DefaultConsole().With("request_id", requestID))
		return
	}
	g.scheduler.MarkPoolExhausted(rapi.ID, ids, poolRecoverAt, "key pool exhausted (all cooling)")
	if g.log != nil {
		g.log.RecordError(requestID,
			fmt.Sprintf("rapi=%s [%s] 全部 Key 冷却中，池标准冷却至 %s，转下一节点",
				rapi.Alias, rapi.PlatformName, poolRecoverAt.Format("15:04:05")),
			"UPSTREAM_RESPONSE")
	}
}

// handleAllKeysUnavailable surfaces a RAPI whose platform key pool was entirely
// unusable for the request. Transient cooldowns (429/5xx) keep the existing short
// cooldown behaviour. Hard-dead key pools — every key disabled, permanently failed
// (401/402/403), or expired — are persisted as RAPI unavailable with the underlying
// key reason, so GetEnabledRAPIsForLAPI drops the dead link and the dashboard shows
// why the RAPI stopped serving instead of a healthy-looking model that silently
// never fires. The RAPI is restored automatically when the operator adds/updates a
// working key (restorePlatformAvailability) or probes a key successfully
// (handleKeyProbe).
func (g *ProxyGateway) handleAllKeysUnavailable(rapi models.RAPIWithPlatform, requestID string, reqLog *logger.ConsoleLogger) {
	// Capability-blocked keys are dead for THIS model even though the key row
	// looks healthy — drop them before the hard-dead judgment so a pool whose
	// keys are all blocked is surfaced as unavailable with the real reason.
	blocked := g.blockedReasons(rapi.Keys, rapi.ID)
	pool := g.filterBlockedKeys(rapi.Keys, rapi.ID)
	hardDead, deadReason := entity.KeysHardDead(pool)
	reason := "all keys unavailable"
	switch {
	case len(blocked) > 0 && len(pool) == 0:
		hardDead = true
		reason = "所有 Key 无权访问此模型: " + strings.Join(blocked, "、")
	case hardDead:
		reason = "所有 Key 不可用: " + deadReason
	}

	reqLog.Error("gateway", "[KEY-FAIL] all keys unavailable, RAPI skipped",
		"hard_dead", hardDead, "reason", reason)
	if g.log != nil {
		g.log.RecordError(requestID, fmt.Sprintf("rapi=%s [%s] %s", rapi.Alias, rapi.PlatformName, reason), "UPSTREAM_RESPONSE")
	}

	// Fire the RAPI-level entity event. Hard-dead pools (all keys disabled /
	// permanently failed / expired) persist available=false and invalidate the
	// RAPI — once persisted, GetEnabledRAPIsForLAPI filters it out, and the
	// entity returns true exactly on first detection. Soft pools (all keys
	// cooling) get a short session cooldown.
	first := g.scheduler.MarkAllKeysUnavailable(rapi.ID, hardDead, reason)
	if first && g.notifyService != nil {
		g.notifyService.PublishEvent(
			"error", notify.MenuModels, "failure",
			fmt.Sprintf("模型 %s 所有 Key 不可用，已标记模型不可用", rapi.Alias),
			reason,
			rapi.ID,
		)
	}
}

// handleStreamingRequest handles streaming (SSE) requests with full error absorption.
func (g *ProxyGateway) handleStreamingRequest(w http.ResponseWriter, r *http.Request, originalBody []byte, canonicalBody []byte, req *models.ProxyRequest, lapi *models.LAPI, rapis []models.RAPIWithPlatform, requestID string, sessionID string, clientFormat string) (bool, int) {
	fallbackUsed := false
	retryCount := 0
	estimatedTokens := scheduler.EstimateCost(req)
	// request_id flows through every console line emitted within this request so
	// the operator can correlate streaming attempts with the request_logs row.
	reqLog := logger.DefaultConsole().With("request_id", requestID)

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
					reqLog.Error("gateway", "[FAIL] "+msg, "lapi", lapi.Alias)
					g.sendErrorStream(w, flusher, msg)
					if g.log != nil {
						g.log.RecordError(requestID, msg, "UPSTREAM_RESPONSE")
					}
					return fallbackUsed, http.StatusServiceUnavailable
				}
				reqLog.Info("gateway", "[WAIT] all RAPI unavailable",
					"wait_until", waitUntil.Format("15:04:05"), "attempt", retryCount)
				if waitErr := g.scheduler.Wait(r.Context(), lapi.ID, waitUntil); waitErr != nil {
					reqLog.Error("gateway", "[FAIL] wait exhausted", "lapi", lapi.Alias, "error", waitErr.Error())
					g.sendErrorStream(w, flusher, waitErr.Error())
					if g.log != nil {
						g.log.RecordError(requestID, waitErr.Error(), "UPSTREAM_RESPONSE")
					}
					return fallbackUsed, http.StatusServiceUnavailable
				}
				continue
			}
			reqLog.Error("gateway", "[FAIL] PickAvailable error", "error", err.Error())
			g.sendErrorStream(w, flusher, err.Error())
			return fallbackUsed, http.StatusServiceUnavailable
		}

		// Determine the best target format for this RAPI.
		targetFormat := pickTargetFormat(rapi, clientFormat)
		effectiveURL := resolveUpstreamURL(rapi, targetFormat)
		// Per-iteration rapi context for downstream log lines.
		rl := reqLog.With("rapi", rapi.Alias, "model", rapi.Model, "format", targetFormat, "url", effectiveURL, "attempt", retryCount)
		rl.Info("gateway", "[SEND] forwarding upstream")
		if effectiveURL == "" {
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "invalid url")
			continue
		}

		if rapi.ID != rapis[0].ID {
			fallbackUsed = true
		}

		// Streaming to a real Google Gemini endpoint must use the
		// :streamGenerateContent suffix WITH ?alt=sse. Two failure modes if
		// either is missing: (1) :generateContent returns a single buffered
		// JSON object — not a stream; (2) :streamGenerateContent without
		// alt=sse returns a buffered JSON array of chunks — still not SSE
		// (data: <json>\n\n) frames. In both cases the StreamConverter would
		// emit zero frames, the client would see nothing despite an upstream
		// 200, and the gateway would retry forever treating it as "empty stream".
		if targetFormat == string(apiformat.FormatGemini) && apiformat.IsGoogleNativeBaseURL(rapi.BaseURL) {
			effectiveURL = apiformat.EnsureGeminiStreamSSE(effectiveURL)
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
			rl.Error("gateway", "[ERR] convert error", "error", convErr.Error())
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "convert error: "+convErr.Error())
			continue
		}

		startTime := time.Now()
		// keyID 现在经 tryKeyForRAPI → doUpstreamRequest → RecordUpstreamSent 落库
		// （request_logs.selected_key_id，指标 key 维度埋点）。
		resp, _, err := g.tryKeyForRAPI(r.Context(), rapi, effectiveURL, upstreamBody, requestID, retryCount, targetFormat)
		latencyMs := int(time.Since(startTime).Milliseconds())

		if err != nil {
			// Do not count failed attempts toward rate-limit/cost counters:
			// a failure should not push the RAPI into HighCost or consume its
			// RPM/RPH/RPD budget (only successful requests reflect real load).
			if errors.Is(err, scheduler.ErrAllKeysUnavailable) {
				// All keys exhausted — surface the dead link instead of skipping silently.
				g.handleAllKeysUnavailable(rapi, requestID, rl)
			} else {
				isTimeout := isTimeoutError(err)
				rl.Error("gateway", "[ERR] upstream attempt failed",
					"error", err.Error(), "latency_ms", latencyMs, "timeout", isTimeout)
				g.scheduler.MarkFailure(rapi.ID, time.Time{}, err.Error(), isTimeout)
				if g.log != nil {
					g.log.RecordError(requestID, "Model "+rapi.Alias+" ["+rapi.PlatformName+"] failed: "+err.Error(), "UPSTREAM_RESPONSE")
				}
				if g.notifyService != nil {
					g.notifyService.PublishAsync("error", fmt.Sprintf("模型 %s 请求失败: %v", rapi.Alias, err), "模型错误")
				}
			}
			retryCount++
			continue
		}
		// 每请求实际使用的 key / 平台埋点（指标 key 维度）。

		// Extract token usage from response headers
		tokensUsed := extractTokenUsage(resp.Header)
		if tokensUsed <= 0 {
			tokensUsed = estimatedTokens
		}

		rl.Info("gateway", "[OK] upstream responded",
			"status", resp.StatusCode, "latency_ms", latencyMs)
		g.scheduler.RecordRequest(rapi.ID, tokensUsed)
		// Bug 8.5: record to persistent DB metrics.
		g.db.RecordRequest(rapi.ID, lapi.ID, resp.StatusCode, latencyMs, tokensUsed)

		if g.notifyService != nil {
			g.notifyService.PublishAsync("info", fmt.Sprintf("Streaming from %s", rapi.Alias), "Active Route")
		}

		// Bug 8.1: target format is already set correctly; no intermediate OpenAI step.
		needsConversion := targetFormat != clientFormat

		// Capture up to 32 KB of the raw upstream SSE body for logging while
		// still streaming bytes to the client in real time. firstByteAt 记录首帧
		// 写出时刻，用于 TTFT（首 token 延迟）。
		const streamLogCapBytes = 32 * 1024
		var streamLogBuf bytes.Buffer
		var firstFrameAt time.Time
		teeBody := io.TeeReader(resp.Body, &firstFrameRecorder{firstSeen: &firstFrameAt, next: &limitedWriter{w: &streamLogBuf, limit: streamLogCapBytes}})

		if !needsConversion {
			// Bug 8.3: Read synchronously — no goroutine needed.
			buf := make([]byte, 4096)
			for {
				n, readErr := teeBody.Read(buf)
				if n > 0 {
					// Check if client has disconnected before writing.
					if r.Context().Err() != nil {
						reqLog.Info("gateway", "[INFO] client disconnected during stream, stopping")
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
				rl.Error("gateway", "[ERR] stream convert error", "error", err.Error())
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
				rl.Warn("gateway", "[WARN] stream conversion produced 0 frames, retrying")
			}
		}

		resp.Body.Close()

		if g.log != nil {
			// TTFT：流式首帧延迟；除首 token 延迟 = 总延迟 - TTFT。
			// token 明细从 32KB 日志缓冲解析（三协议，最后 usage 帧生效）。
			ttftMs := latencyMs
			if !firstFrameAt.IsZero() {
				ttftMs = int(firstFrameAt.Sub(startTime).Milliseconds())
			}
			restMs := latencyMs - ttftMs
			if restMs < 0 {
				restMs = 0
			}
			in, out, cached := extractUsageDetail(streamLogBuf.String())
			g.log.RecordUpstreamResponseDetail(requestID, resp.StatusCode, respHeaders(resp), streamLogBuf.String(),
				latencyMs, ttftMs, tokensUsed, in, out, cached)
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
	// request_id propagated to every console line so non-streaming attempts correlate
	// with the SQLite request_logs row just like the streaming path.
	reqLog := logger.DefaultConsole().With("request_id", requestID)

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
					reqLog.Error("gateway", "[FAIL] "+msg, "lapi", lapi.Alias)
					writeGatewayJSONError(w, http.StatusServiceUnavailable, msg, "service_unavailable")
					if g.log != nil {
						g.log.RecordError(requestID, msg, "UPSTREAM_RESPONSE")
					}
					return fallbackUsed, http.StatusServiceUnavailable
				}
				reqLog.Info("gateway", "[WAIT] all RAPI unavailable",
					"wait_until", waitUntil.Format("15:04:05"), "attempt", retryCount)
				if waitErr := g.scheduler.Wait(reqCtx, lapi.ID, waitUntil); waitErr != nil {
					reqLog.Error("gateway", "[FAIL] wait exhausted", "lapi", lapi.Alias, "error", waitErr.Error())
					writeGatewayJSONError(w, http.StatusServiceUnavailable, waitErr.Error(), "service_unavailable")
					if g.log != nil {
						g.log.RecordError(requestID, waitErr.Error(), "UPSTREAM_RESPONSE")
					}
					return fallbackUsed, http.StatusServiceUnavailable
				}
				continue
			}
			reqLog.Error("gateway", "[FAIL] PickAvailable error", "error", err.Error())
			writeGatewayJSONError(w, http.StatusServiceUnavailable, err.Error(), "service_unavailable")
			return fallbackUsed, http.StatusServiceUnavailable
		}

		// Determine the best target format for this RAPI.
		targetFormat := pickTargetFormat(rapi, clientFormat)
		effectiveURL := resolveUpstreamURL(rapi, targetFormat)
		rl := reqLog.With("rapi", rapi.Alias, "model", rapi.Model, "format", targetFormat, "url", effectiveURL, "attempt", retryCount)
		rl.Info("gateway", "[SEND] forwarding upstream")
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
			rl.Error("gateway", "[ERR] convert error", "error", convErr.Error())
			g.scheduler.MarkFailure(rapi.ID, time.Time{}, "convert error: "+convErr.Error())
			continue
		}

		startTime := time.Now()
		// keyID 现在经 tryKeyForRAPI → doUpstreamRequest → RecordUpstreamSent 落库
		// （request_logs.selected_key_id，指标 key 维度埋点）。
		resp, _, err := g.tryKeyForRAPI(reqCtx, rapi, effectiveURL, upstreamBody, requestID, retryCount, targetFormat)
		latencyMs := int(time.Since(startTime).Milliseconds())

		if err != nil {
			// Do not count failed attempts toward rate-limit/cost counters
			// (see streaming path): only successful requests reflect real load.
			if errors.Is(err, scheduler.ErrAllKeysUnavailable) {
				// All keys exhausted — surface the dead link instead of skipping silently.
				g.handleAllKeysUnavailable(rapi, requestID, rl)
			} else {
				isTimeout := isTimeoutError(err)
				rl.Error("gateway", "[ERR] upstream attempt failed",
					"error", err.Error(), "latency_ms", latencyMs, "timeout", isTimeout)
				g.scheduler.MarkFailure(rapi.ID, time.Time{}, err.Error(), isTimeout)
				if g.log != nil {
					g.log.RecordError(requestID, "Model "+rapi.Alias+" ["+rapi.PlatformName+"] failed: "+err.Error(), "UPSTREAM_RESPONSE")
				}
			}
			retryCount++
			continue
		}
		// 每请求实际使用的 key / 平台埋点（指标 key 维度）。

		tokensUsed := extractTokenUsage(resp.Header)
		if tokensUsed <= 0 {
			tokensUsed = estimatedTokens
		}

		// Success path
		rl.Info("gateway", "[OK] upstream responded",
			"status", resp.StatusCode, "latency_ms", latencyMs)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		g.scheduler.RecordRequest(rapi.ID, tokensUsed)
		g.scheduler.MarkSuccess(rapi.ID)
		// Bug 8.5: record to persistent DB metrics.
		g.db.RecordRequest(rapi.ID, lapi.ID, resp.StatusCode, latencyMs, tokensUsed)

		if g.log != nil {
			// 非流式：TTFT = 总延迟（整包一次返回），除首 token 延迟 = 0。
			// token 明细从响应体解析（三协议）。
			in, out, cached := extractUsageDetail(string(respBody))
			g.log.RecordUpstreamResponseDetail(requestID, resp.StatusCode, respHeaders(resp), string(respBody),
				latencyMs, latencyMs, tokensUsed, in, out, cached)
		}

		// Convert response back to client format if needed.
		if targetFormat != clientFormat {
			converted, convErr := apiformat.ConvertResponse(respBody, apiformat.APIFormat(targetFormat), apiformat.APIFormat(clientFormat), req.Model)
			if convErr != nil {
				rl.Error("gateway", "[ERR] response convert error, sending raw", "error", convErr.Error())
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
func (g *ProxyGateway) doUpstreamRequest(ctx context.Context, rapi models.RAPIWithPlatform, url string, body []byte, token string, requestID string, retryCount int, targetFormat string, keyID int64) (*http.Response, int, error) {
	upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, 0, err
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
	case "gemini":
		// Real Google endpoints (generativelanguage/aiplatform .googleapis.com) require
		// x-goog-api-key; an OpenAI-style Bearer is rejected. OpenAI-compatible Gemini
		// relays keep using Bearer. Decide by the platform base URL.
		if apiformat.IsGoogleNativeBaseURL(rapi.BaseURL) {
			upstreamReq.Header.Set(apiformat.GoogleAPIKeyHeader, token)
		} else {
			upstreamReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		}
	default: // openai, etc.
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
		g.log.RecordUpstreamSent(requestID, fmt.Sprintf("%s [%s]", rapi.Alias, rapi.PlatformName), url, headers, string(body), retryCount, keyID, rapi.PlatformID)
	}

	resp, err := g.httpClient.Do(upstreamReq)
	if err != nil {
		return nil, 0, err
	}
	return resp, resp.StatusCode, nil
}

func writeGatewayJSONError(w http.ResponseWriter, status int, message, errorType string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    errorType,
		},
	})
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
		logger.DefaultConsole().Warn("gateway", "[WARN] sendErrorStream: write failed", "error", err.Error())
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

// extractUsageDetail parses the token usage breakdown (input / output / cached)
// from an upstream response body. It handles all three native protocols and
// both plain-JSON and SSE-framed bodies (the last usage frame wins):
//
//   - openai:    usage.prompt_tokens / completion_tokens /
//     usage.prompt_tokens_details.cached_tokens
//   - anthropic: usage.input_tokens / output_tokens /
//     cache_read_input_tokens (+ cache_creation_input_tokens counts as
//     cached too — it is a cache write, reported on the cached line)
//   - gemini:    usageMetadata.promptTokenCount / candidatesTokenCount /
//     cachedContentTokenCount
//
// Zero fields mean "not reported"; callers fall back to the header total /
// estimate. The body is already truncated to the log cap, so this never sees
// unbounded payloads.
func extractUsageDetail(body string) (in, out, cached int) {
	if body == "" {
		return 0, 0, 0
	}
	// Streaming: scan SSE frames for the last object carrying usage info.
	if strings.Contains(body, "data:") {
		for _, line := range strings.Split(body, "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "" || payload == "[DONE]" {
				continue
			}
			var probe map[string]json.RawMessage
			if json.Unmarshal([]byte(payload), &probe) != nil {
				continue
			}
			if i, o, c := usageFromObject(probe); i > 0 || o > 0 || c > 0 {
				in, out, cached = i, o, c
			}
		}
		if in > 0 || out > 0 || cached > 0 {
			return in, out, cached
		}
	}
	var probe map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &probe) != nil {
		return 0, 0, 0
	}
	return usageFromObject(probe)
}

// usageFromObject extracts the token breakdown from one decoded JSON object,
// trying all three protocol shapes. Later matches only fill still-zero fields,
// so a hybrid body cannot lose information.
func usageFromObject(obj map[string]json.RawMessage) (in, out, cached int) {
	// openai / anthropic share the "usage" object key.
	if u, ok := obj["usage"]; ok {
		var um map[string]json.RawMessage
		if json.Unmarshal(u, &um) == nil {
			// openai: prompt_tokens / completion_tokens / prompt_tokens_details{cached_tokens}
			in = rawInt(um["prompt_tokens"])
			out = rawInt(um["completion_tokens"])
			if d, ok := um["prompt_tokens_details"]; ok {
				var dm map[string]json.RawMessage
				if json.Unmarshal(d, &dm) == nil {
					cached = rawInt(dm["cached_tokens"])
				}
			}
			// anthropic: input_tokens / output_tokens / cache_read_input_tokens
			// (+ cache_creation_input_tokens counts as cached — cache write).
			if in == 0 {
				in = rawInt(um["input_tokens"])
			}
			if out == 0 {
				out = rawInt(um["output_tokens"])
			}
			if cached == 0 {
				cached = rawInt(um["cache_read_input_tokens"]) + rawInt(um["cache_creation_input_tokens"])
			}
		}
	}
	// gemini: usageMetadata{promptTokenCount, candidatesTokenCount, cachedContentTokenCount}
	if um, ok := obj["usageMetadata"]; ok {
		var gm map[string]json.RawMessage
		if json.Unmarshal(um, &gm) == nil {
			if in == 0 {
				in = rawInt(gm["promptTokenCount"])
			}
			if out == 0 {
				out = rawInt(gm["candidatesTokenCount"])
			}
			if cached == 0 {
				cached = rawInt(gm["cachedContentTokenCount"])
			}
		}
	}
	return in, out, cached
}

// rawInt decodes a JSON number field; missing / non-numeric → 0.
func rawInt(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		return 0
	}
	return int(f)
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

// resolveUpstreamURL returns the upstream URL to use when forwarding to rapi
// in the given target format. API platforms prefer the exact URL that detection
// proved works (from platform.format_endpoints) — important for aggregators with
// non-standard paths like /messages instead of /v1/messages — and fall back to
// BuildURL() when no recorded endpoint exists (legacy rows / undetected platforms).
func resolveUpstreamURL(rapi models.RAPIWithPlatform, targetFormat string) string {
	if rapi.PlatformFormatEndpoints != "" && rapi.PlatformFormatEndpoints != "{}" {
		var endpoints map[string]string
		if err := json.Unmarshal([]byte(rapi.PlatformFormatEndpoints), &endpoints); err == nil {
			if u, ok := endpoints[targetFormat]; ok && u != "" {
				return u
			}
		}
	}
	return apiformat.BuildURL(rapi.BaseURL, rapi.Model, apiformat.APIFormat(targetFormat))
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

// Failure classification moved to entity.ClassifyFailure: the entity owns the
// mapping from HTTP status to failure scope so the FSM and the gateway agree.

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

// firstFrameRecorder wraps a writer and stamps firstSeen with the time the
// first non-empty chunk flowed through — the streaming TTFT anchor. It
// forwards every write to next unchanged.
type firstFrameRecorder struct {
	firstSeen *time.Time
	next     io.Writer
}

func (f *firstFrameRecorder) Write(p []byte) (int, error) {
	if len(p) > 0 && f.firstSeen != nil && f.firstSeen.IsZero() {
		*f.firstSeen = time.Now()
	}
	return f.next.Write(p)
}
