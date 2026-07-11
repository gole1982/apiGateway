package apiformat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
}

// DetectFormats probes the upstream API to determine which formats it supports.
// It sends a minimal test request in each format concurrently.
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

func probeFormat(ctx context.Context, client *http.Client, baseURL string, model string, token string, format APIFormat) DetectResult {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	url := BuildURL(baseURL, model, format)

	// Build a minimal probe request.
	body, contentType, headers := buildProbeRequest(format, model)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return DetectResult{Format: format, Supported: false, Reason: "build request: " + err.Error()}
	}

	req.Header.Set("Content-Type", contentType)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return DetectResult{Format: format, Supported: false, Reason: "timeout"}
		}
		return DetectResult{Format: format, Supported: false, Reason: "request error: " + err.Error()}
	}
	defer resp.Body.Close()

	// Determine support based on status code.
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		// Success: format is supported.
		log.Printf("[DETECT] %s format supported (status %d) url=%s", format, resp.StatusCode, url)
		return DetectResult{Format: format, Supported: true}

	case resp.StatusCode == 401 || resp.StatusCode == 403:
		// Auth error but endpoint exists — format is supported (token may be wrong for this format).
		log.Printf("[DETECT] %s endpoint exists but auth failed (status %d) url=%s", format, resp.StatusCode, url)
		return DetectResult{Format: format, Supported: true, Reason: fmt.Sprintf("auth error %d (endpoint exists)", resp.StatusCode)}

	case resp.StatusCode == 404:
		// Endpoint not found — format not supported.
		return DetectResult{Format: format, Supported: false, Reason: "endpoint not found (404)"}

	case resp.StatusCode == 405:
		// Method not allowed — endpoint exists but POST not accepted, likely not supported.
		return DetectResult{Format: format, Supported: false, Reason: "method not allowed (405)"}

	case resp.StatusCode == 400:
		// Bad Request — the endpoint exists but rejected our probe body (format not supported).
		log.Printf("[DETECT] %s format not supported (400 bad request) url=%s", format, url)
		return DetectResult{Format: format, Supported: false, Reason: "bad request (format not supported)"}

	case resp.StatusCode >= 500 && resp.StatusCode < 600:
		// Server error — endpoint exists but is currently erroring; treat as supported.
		log.Printf("[DETECT] %s endpoint exists but server error (status %d) url=%s", format, resp.StatusCode, url)
		return DetectResult{Format: format, Supported: true, Reason: fmt.Sprintf("server error %d (endpoint exists)", resp.StatusCode)}

	default:
		// Other 4xx (429 rate limit, etc.) — endpoint exists.
		log.Printf("[DETECT] %s probe returned status %d url=%s", format, resp.StatusCode, url)
		return DetectResult{Format: format, Supported: true, Reason: fmt.Sprintf("error %d (endpoint exists)", resp.StatusCode)}
	}
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
