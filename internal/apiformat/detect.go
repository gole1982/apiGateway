package apiformat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DetectResult holds the result of a single format probe.
type DetectResult struct {
	Format    APIFormat `json:"format"`
	Supported bool      `json:"supported"`
	Reason    string    `json:"reason,omitempty"`
	// URL is the first candidate URL that responded successfully for this
	// format. Empty when Supported is false. Callers should persist this URL
	// so the gateway can forward to the exact endpoint that detection proved
	// works (avoiding mismatches on aggregators with non-standard paths).
	URL string `json:"url,omitempty"`
}

// DetectFormats probes the upstream API to determine which formats it supports.
// It sends a minimal test request in each format concurrently. For each format
// it tries the candidate URLs returned by BuildURLs in order and records the
// first one that responds successfully.
func DetectFormats(ctx context.Context, baseURL string, model string, token string, httpClient *http.Client) []DetectResult {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}

	results := make([]DetectResult, 0, 3)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, format := range AllFormats() {
		wg.Add(1)
		go func(f APIFormat) {
			defer wg.Done()
			result := probeFormat(ctx, httpClient, baseURL, model, token, f)
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(format)
	}

	wg.Wait()
	return results
}

// DetectFormatsJSON is like DetectFormats but returns a JSON array string for DB storage.
func DetectFormatsJSON(ctx context.Context, baseURL string, model string, token string, httpClient *http.Client) string {
	results := DetectFormats(ctx, baseURL, model, token, httpClient)
	var supported []APIFormat
	for _, r := range results {
		if r.Supported {
			supported = append(supported, r.Format)
		}
	}
	if len(supported) == 0 {
		return `["openai"]`
	}
	return FormatsToJSON(supported)
}

// DetectFormatsEndpoints returns a JSON object {"format":"url",...} mapping
// each supported format to the first candidate URL that detection proved
// works. Suitable for storing in platform.format_endpoints. Returns "" when
// no format has a recorded URL.
func DetectFormatsEndpoints(ctx context.Context, baseURL string, model string, token string, httpClient *http.Client) string {
	results := DetectFormats(ctx, baseURL, model, token, httpClient)
	endpoints := make(map[string]string, len(results))
	for _, r := range results {
		if r.Supported && r.URL != "" {
			endpoints[string(r.Format)] = r.URL
		}
	}
	if len(endpoints) == 0 {
		return ""
	}
	b, _ := json.Marshal(endpoints)
	return string(b)
}

func probeFormat(ctx context.Context, client *http.Client, baseURL string, model string, token string, format APIFormat) DetectResult {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Build a minimal probe request (body is the same for all candidate URLs).
	body, contentType, headers := buildProbeRequest(format, model)

	// Try each candidate URL in order; the first one that proves the endpoint
	// exists wins. We keep going on 404/400/405 (path-not-found / not-accepted)
	// and stop on any other status (endpoint exists) or transport error that is
	// not a 404-equivalent.
	candidates := BuildURLs(baseURL, model, format)
	var lastReason string
	for _, url := range candidates {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
		if err != nil {
			lastReason = "build request: " + err.Error()
			continue
		}

		req.Header.Set("Content-Type", contentType)
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		// Apply authentication token based on format so probes are authenticated.
		if token != "" {
			switch format {
			case FormatAnthropic:
				req.Header.Set("x-api-key", token)
			case FormatGemini:
				// Real Google endpoints require x-goog-api-key; OpenAI-compatible
				// Gemini relays use Bearer. Decide by the probe base URL.
				if IsGoogleNativeBaseURL(baseURL) {
					req.Header.Set(GoogleAPIKeyHeader, token)
				} else {
					req.Header.Set("Authorization", "Bearer "+token)
				}
			default: // OpenAI, etc.
				req.Header.Set("Authorization", "Bearer "+token)
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				// Timeout on this candidate — give up on remaining candidates
				// (they share the same per-format deadline).
				return DetectResult{Format: format, Supported: false, Reason: "timeout"}
			}
			lastReason = "request error: " + err.Error()
			continue
		}

		status := resp.StatusCode
		resp.Body.Close()

		// Determine support based on status code.
		switch {
		case status >= 200 && status < 300:
			slog.Info("[DETECT] format supported", "component", "apiformat",
				"format", string(format), "status", status, "url", url)
			return DetectResult{Format: format, Supported: true, URL: url}

		case status == 401 || status == 403:
			// Auth error but endpoint exists — format is supported (token may be wrong for this format).
			slog.Info("[DETECT] endpoint exists but auth failed", "component", "apiformat",
				"format", string(format), "status", status, "url", url)
			return DetectResult{Format: format, Supported: true, URL: url, Reason: fmt.Sprintf("auth error %d (endpoint exists)", status)}

		case status == 404:
			// Endpoint not found at this candidate — try the next one.
			lastReason = "endpoint not found (404)"
			continue

		case status == 405:
			// Method not allowed — endpoint exists but POST not accepted; not supported here.
			lastReason = "method not allowed (405)"
			continue

		case status == 400:
			// Bad Request — the endpoint exists but rejected our probe body.
			// This typically means the path exists but the format is not supported.
			slog.Info("[DETECT] format not supported (400 bad request)", "component", "apiformat",
				"format", string(format), "url", url)
			lastReason = "bad request (format not supported)"
			continue

		case status >= 500 && status < 600:
			// Server error — endpoint exists but is currently erroring; treat as supported.
			slog.Info("[DETECT] endpoint exists but server error", "component", "apiformat",
				"format", string(format), "status", status, "url", url)
			return DetectResult{Format: format, Supported: true, URL: url, Reason: fmt.Sprintf("server error %d (endpoint exists)", status)}

		default:
			// Other 4xx (429 rate limit, etc.) — endpoint exists.
			slog.Info("[DETECT] probe returned status", "component", "apiformat",
				"format", string(format), "status", status, "url", url)
			return DetectResult{Format: format, Supported: true, URL: url, Reason: fmt.Sprintf("error %d (endpoint exists)", status)}
		}
	}

	return DetectResult{Format: format, Supported: false, Reason: lastReason}
}

func buildProbeRequest(format APIFormat, model string) (body []byte, contentType string, headers map[string]string) {
	headers = make(map[string]string)

	switch format {
	case FormatAnthropic:
		headers["x-api-key"] = "" // Will be overridden by actual token.
		headers["anthropic-version"] = "2023-06-01"
		contentType = "application/json"
		body, _ = json.Marshal(map[string]interface{}{
			"model":      model,
			"max_tokens": 5,
			"messages": []interface{}{
				map[string]interface{}{
					"role":    "user",
					"content": "hi",
				},
			},
		})

	case FormatGemini:
		contentType = "application/json"
		body, _ = json.Marshal(map[string]interface{}{
			"contents": []interface{}{
				map[string]interface{}{
					"parts": []interface{}{
						map[string]interface{}{"text": "hi"},
					},
				},
			},
			"generationConfig": map[string]interface{}{
				"maxOutputTokens": 5,
			},
		})

	default: // OpenAI
		contentType = "application/json"
		body, _ = json.Marshal(map[string]interface{}{
			"model":      model,
			"max_tokens": 5,
			"messages": []interface{}{
				map[string]interface{}{
					"role":    "user",
					"content": "hi",
				},
			},
		})
	}

	return body, contentType, headers
}
