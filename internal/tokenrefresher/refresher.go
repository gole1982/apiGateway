package tokenrefresher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"gateway/internal/db"
)

var (
	ErrTokenFetchFailed = errors.New("failed to fetch token")
	ErrInvalidCommand   = errors.New("invalid token command")
)

type TokenProvider interface {
	FetchTokenForPlatform(ctx context.Context, platformID int64, token string, isDynamic bool, tokenCommand string) (string, error)
}

type DynamicTokenProvider struct {
	mu       sync.RWMutex
	cache    map[int64]*tokenEntry
	commands map[int64]string
}

type tokenEntry struct {
	token     string
	fetchedAt time.Time
	expiresAt time.Time
}

type RefreshCallback func(rapiID int64, newToken string)

func NewDynamicTokenProvider() *DynamicTokenProvider {
	return &DynamicTokenProvider{
		cache:    make(map[int64]*tokenEntry),
		commands: make(map[int64]string),
	}
}

func (p *DynamicTokenProvider) RegisterCommand(rapiID int64, command string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commands[rapiID] = command
}

func (p *DynamicTokenProvider) FetchTokenForPlatform(ctx context.Context, platformID int64, token string, isDynamic bool, tokenCommand string) (string, error) {
	if platformID == 0 {
		return "", errors.New("invalid platform ID")
	}

	// Check memory cache first
	p.mu.RLock()
	if entry, ok := p.cache[platformID]; ok {
		if time.Now().Before(entry.expiresAt) {
			cachedToken := entry.token
			p.mu.RUnlock()
			return cachedToken, nil
		}
	}
	p.mu.RUnlock()

	// Try to get from database cache
	database := db.Get()
	cachedToken, err := database.GetCachedToken(platformID)
	if err == nil && cachedToken != "" {
		p.mu.Lock()
		p.cache[platformID] = &tokenEntry{
			token:     cachedToken,
			fetchedAt: time.Now(),
			expiresAt: time.Now().Add(5 * time.Minute),
		}
		p.mu.Unlock()
		return cachedToken, nil
	}

	// Fetch new token
	var newToken string
	err = nil

	if token != "" {
		newToken = token
	} else if isDynamic && tokenCommand != "" {
		newToken, err = p.executeExternalCommand(ctx, tokenCommand)
	} else {
		return "", ErrInvalidCommand
	}

	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrTokenFetchFailed, err)
	}

	// Update cache and database
	p.mu.Lock()
	p.cache[platformID] = &tokenEntry{
		token:     newToken,
		fetchedAt: time.Now(),
		expiresAt: time.Now().Add(5 * time.Minute),
	}
	p.mu.Unlock()

	database.SetCachedToken(platformID, newToken, 5*time.Minute)
	database.UpdatePlatformToken(platformID, newToken)

	return newToken, nil
}

func (p *DynamicTokenProvider) executeExternalCommand(ctx context.Context, command string) (string, error) {
	var cmd *exec.Cmd

	// Handle different command formats
	if len(command) > 2 && command[0] == '"' && command[len(command)-1] == '"' {
		// Quoted command: "C:\path\to\program.exe" "arg1" "arg2"
		args := parseCommandArgs(command[1 : len(command)-1])
		if len(args) == 0 {
			return "", ErrInvalidCommand
		}
		cmd = exec.CommandContext(ctx, args[0], args[1:]...)
	} else {
		// Simple command
		cmd = exec.CommandContext(ctx, "cmd", "/c", command)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("command failed: %v, stderr: %s", err, stderr.String())
	}

	token := trimTokenOutput(stdout.String())
	if token == "" {
		return "", errors.New("empty token output")
	}

	return token, nil
}

func parseCommandArgs(command string) []string {
	var args []string
	var current bytes.Buffer
	inQuote := false

	for i := 0; i < len(command); i++ {
		c := command[i]
		switch {
		case c == '"':
			inQuote = !inQuote
		case c == ' ' && !inQuote:
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(c)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}

func trimTokenOutput(output string) string {
	result := ""
	for _, line := range splitLines(output) {
		line = trimWhitespace(line)
		if line == "" || isJSON(line) {
			continue
		}
		result = line
		break
	}
	return result
}

func splitLines(s string) []string {
	var lines []string
	var current bytes.Buffer

	for _, c := range s {
		if c == '\n' {
			lines = append(lines, current.String())
			current.Reset()
		} else if c != '\r' {
			current.WriteRune(c)
		}
	}

	if current.Len() > 0 {
		lines = append(lines, current.String())
	}

	return lines
}

func trimWhitespace(s string) string {
	start := 0
	end := len(s)

	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}

	return s[start:end]
}

func isJSON(s string) bool {
	s = trimWhitespace(s)
	return len(s) > 0 && s[0] == '{'
}

func (p *DynamicTokenProvider) RegisterCommandForPlatform(platformID int64, command string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commands[platformID] = command
}

func (p *DynamicTokenProvider) StartBackgroundRefreshForPlatform(ctx context.Context, platformID int64, interval time.Duration, onRefresh func(platformID int64, newToken string)) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.mu.RLock()
				tokenCommand := p.commands[platformID]
				p.mu.RUnlock()

				if tokenCommand == "" {
					continue
				}

				newToken, err := p.executeExternalCommand(ctx, tokenCommand)
				if err != nil {
					continue
				}

				p.mu.Lock()
				p.cache[platformID] = &tokenEntry{
					token:     newToken,
					fetchedAt: time.Now(),
					expiresAt: time.Now().Add(5 * time.Minute),
				}
				p.mu.Unlock()

				database := db.Get()
				database.SetCachedToken(platformID, newToken, 5*time.Minute)
				database.UpdatePlatformToken(platformID, newToken)

				if onRefresh != nil {
					onRefresh(platformID, newToken)
				}
			}
		}
	}()
}

func (p *DynamicTokenProvider) RefreshPlatformOnDemand(ctx context.Context, platformID int64, tokenCommand string) (string, error) {
	if tokenCommand == "" {
		return "", ErrInvalidCommand
	}
	return p.executeExternalCommand(ctx, tokenCommand)
}

func (p *DynamicTokenProvider) InvalidatePlatformCache(platformID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cache, platformID)
}

// HTTPTokenProvider fetches token from a REST endpoint
type HTTPTokenProvider struct {
	endpoint string
	method   string
	headers  map[string]string
	body     string
}

func NewHTTPTokenProvider(endpoint, method string, headers map[string]string, body string) *HTTPTokenProvider {
	return &HTTPTokenProvider{
		endpoint: endpoint,
		method:   method,
		headers:  headers,
		body:     body,
	}
}

func (h *HTTPTokenProvider) Fetch(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, h.method, h.endpoint, bytes.NewBufferString(h.body))
	if err != nil {
		return "", err
	}

	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	// Try to extract token from common JSON paths
	if token, ok := result["token"].(string); ok {
		return token, nil
	}
	if data, ok := result["data"].(map[string]interface{}); ok {
		if token, ok := data["token"].(string); ok {
			return token, nil
		}
	}

	return "", errors.New("token not found in response")
}
