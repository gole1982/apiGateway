package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"
)

// shellWhitelist holds the allowed command names for shell executor.
// Set via SetShellWhitelist from config at startup.
var shellWhitelist = []string{"python", "node", "curl"}

// ssrfCheck validates a URL against SSRF rules. It is a package-level variable
// so tests can inject a permissive validator when pointing at httptest servers
// (which bind to 127.0.0.1 and would otherwise be blocked). In production it
// defaults to validateURL.
var ssrfCheck = validateURL

// SetShellWhitelist configures the allowed commands for the shell executor.
func SetShellWhitelist(wl []string) {
	if len(wl) > 0 {
		shellWhitelist = wl
	}
}

// Executor executes a tool with given arguments and returns the result string.
type Executor interface {
	Execute(ctx context.Context, tool *Tool, arguments map[string]interface{}) (string, error)
}

// NewExecutor creates the appropriate executor based on tool type.
func NewExecutor(tool *Tool) (Executor, error) {
	switch tool.ExecutorType {
	case "http":
		return &HTTPExecutor{
			client: &http.Client{
				Timeout: time.Duration(tool.TimeoutMs) * time.Millisecond,
				Transport: &http.Transport{
					DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
				},
			},
		}, nil
	case "shell":
		return &ShellExecutor{whitelist: shellWhitelist}, nil
	case "builtin":
		return &BuiltinExecutor{
			client: &http.Client{
				Timeout: time.Duration(tool.TimeoutMs) * time.Millisecond,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown executor type: %s", tool.ExecutorType)
	}
}

// --- HTTP Executor ---

// HTTPExecutorConfig is the JSON config for http-type tools.
type HTTPExecutorConfig struct {
	Method       string            `json:"method"`
	URL          string            `json:"url"`
	Headers      map[string]string `json:"headers"`
	BodyTemplate string            `json:"body_template"` // Go template, variables from arguments
	ResultPath   string            `json:"result_path"`   // dot-separated path to extract from response JSON
}

type HTTPExecutor struct {
	client *http.Client
}

func (e *HTTPExecutor) Execute(ctx context.Context, tool *Tool, arguments map[string]interface{}) (string, error) {
	var cfg HTTPExecutorConfig
	if err := json.Unmarshal([]byte(tool.ExecutorConfig), &cfg); err != nil {
		return "", fmt.Errorf("invalid http executor config: %w", err)
	}

	if cfg.URL == "" {
		return "", fmt.Errorf("http executor: url is required")
	}
	if cfg.Method == "" {
		cfg.Method = "POST"
	}

	// Render URL template (supports {{.url}} style)
	renderedURL, err := renderTemplate(cfg.URL, arguments)
	if err != nil {
		return "", fmt.Errorf("url template render error: %w", err)
	}

	// SSRF protection: block private/internal IP ranges
	if err := ssrfCheck(renderedURL); err != nil {
		return "", err
	}

	// Render body template with arguments
	var bodyReader io.Reader
	if cfg.BodyTemplate != "" {
		rendered, err := renderTemplate(cfg.BodyTemplate, arguments)
		if err != nil {
			return "", fmt.Errorf("body template render error: %w", err)
		}
		bodyReader = bytes.NewBufferString(rendered)
	} else if len(arguments) > 0 {
		// Default: send arguments as JSON body
		bodyBytes, _ := json.Marshal(arguments)
		bodyReader = bytes.NewBuffer(bodyBytes)
	}

	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(cfg.Method), renderedURL, bodyReader)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range cfg.Headers {
		// Support template variables in header values
		rendered, err := renderTemplate(v, arguments)
		if err != nil {
			req.Header.Set(k, v)
		} else {
			req.Header.Set(k, rendered)
		}
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB max
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(respBody), 512))
	}

	// Extract result by path if specified
	result := string(respBody)
	if cfg.ResultPath != "" {
		extracted, err := extractJSONPath(respBody, cfg.ResultPath)
		if err != nil {
			// If path extraction fails, return full body
			result = string(respBody)
		} else {
			result = extracted
		}
	}

	return truncate(result, 8192), nil
}

// --- Shell Executor ---

// ShellExecutorConfig is the JSON config for shell-type tools.
type ShellExecutorConfig struct {
	Command string            `json:"command"` // must be in whitelist
	Args    []string          `json:"args"`    // Go templates rendered with arguments
	Workdir string            `json:"workdir"` // optional working directory
	Env     map[string]string `json:"env"`     // optional extra environment variables
}

type ShellExecutor struct {
	whitelist []string
}

func (e *ShellExecutor) Execute(ctx context.Context, tool *Tool, arguments map[string]interface{}) (string, error) {
	var cfg ShellExecutorConfig
	if err := json.Unmarshal([]byte(tool.ExecutorConfig), &cfg); err != nil {
		return "", fmt.Errorf("invalid shell executor config: %w", err)
	}

	if cfg.Command == "" {
		return "", fmt.Errorf("shell executor: command is required")
	}

	// Whitelist check: only the base name of the command is checked
	baseName := filepath.Base(cfg.Command)
	if !e.isAllowed(baseName) {
		return "", fmt.Errorf("command %q is not in the shell whitelist (allowed: %s)", baseName, strings.Join(e.whitelist, ", "))
	}

	// Render args templates
	renderedArgs := make([]string, len(cfg.Args))
	for i, argTmpl := range cfg.Args {
		rendered, err := renderTemplate(argTmpl, arguments)
		if err != nil {
			return "", fmt.Errorf("arg[%d] template render error: %w", i, err)
		}
		renderedArgs[i] = rendered
	}

	// Execute directly via exec.CommandContext (no shell interpretation)
	cmd := exec.CommandContext(ctx, cfg.Command, renderedArgs...)
	if cfg.Workdir != "" {
		cmd.Dir = cfg.Workdir
	}
	if len(cfg.Env) > 0 {
		cmd.Env = cmd.Environ()
		for k, v := range cfg.Env {
			rendered, err := renderTemplate(v, arguments)
			if err == nil {
				cmd.Env = append(cmd.Env, k+"="+rendered)
			} else {
				cmd.Env = append(cmd.Env, k+"="+v)
			}
		}
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := err.Error()
		if stderr.Len() > 0 {
			errMsg += "\nstderr: " + truncate(stderr.String(), 1024)
		}
		return "", fmt.Errorf("command failed: %s", errMsg)
	}

	output := stdout.String()
	if output == "" && stderr.Len() > 0 {
		output = stderr.String()
	}
	return truncate(output, 8192), nil
}

func (e *ShellExecutor) isAllowed(name string) bool {
	for _, allowed := range e.whitelist {
		if strings.EqualFold(name, allowed) {
			return true
		}
		// Also match with common extensions (e.g. "python" matches "python.exe"),
		// case-insensitive on the suffix so "NODE.EXE" matches as well.
		if strings.HasSuffix(strings.ToLower(name), ".exe") &&
			strings.EqualFold(strings.TrimSuffix(strings.ToLower(name), ".exe"), allowed) {
			return true
		}
	}
	return false
}

// --- Builtin Executor ---

// BuiltinExecutorConfig is the JSON config for builtin-type tools.
type BuiltinExecutorConfig struct {
	Handler string                 `json:"handler"` // handler name
	Options map[string]interface{} `json:"options"` // handler-specific options
}

// BuiltinHandler is a function that implements a builtin tool.
type BuiltinHandler func(ctx context.Context, args map[string]interface{}, options map[string]interface{}, client *http.Client) (string, error)

// builtinHandlers maps handler names to implementations.
var builtinHandlers = map[string]BuiltinHandler{
	"http_get": builtinHTTPGet,
}

type BuiltinExecutor struct {
	client *http.Client
}

func (e *BuiltinExecutor) Execute(ctx context.Context, tool *Tool, arguments map[string]interface{}) (string, error) {
	var cfg BuiltinExecutorConfig
	if err := json.Unmarshal([]byte(tool.ExecutorConfig), &cfg); err != nil {
		return "", fmt.Errorf("invalid builtin executor config: %w", err)
	}

	handler, ok := builtinHandlers[cfg.Handler]
	if !ok {
		return "", fmt.Errorf("unknown builtin handler: %q (available: %s)", cfg.Handler, availableHandlers())
	}

	return handler(ctx, arguments, cfg.Options, e.client)
}

// builtinHTTPGet fetches a URL and returns the response body as text.
func builtinHTTPGet(ctx context.Context, args map[string]interface{}, options map[string]interface{}, client *http.Client) (string, error) {
	urlStr, ok := args["url"].(string)
	if !ok || urlStr == "" {
		return "", fmt.Errorf("http_get: 'url' argument is required")
	}

	if err := ssrfCheck(urlStr); err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return "", fmt.Errorf("http_get: create request: %w", err)
	}
	req.Header.Set("User-Agent", "APIGateway-Agent/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("http_get: request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("http_get: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("http_get: http %d: %s", resp.StatusCode, truncate(string(body), 256))
	}

	return truncate(string(body), 8192), nil
}

func availableHandlers() string {
	names := make([]string, 0, len(builtinHandlers))
	for k := range builtinHandlers {
		names = append(names, k)
	}
	return strings.Join(names, ", ")
}

// --- Helpers ---

// renderTemplate renders a Go text/template with the given data map.
func renderTemplate(tmpl string, data map[string]interface{}) (string, error) {
	t, err := template.New("tool").Option("missingkey=zero").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// extractJSONPath extracts a value from JSON by dot-separated path.
// e.g. "data.results" → obj["data"]["results"]
func extractJSONPath(data []byte, path string) (string, error) {
	var obj interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return "", err
	}

	parts := strings.Split(path, ".")
	current := obj
	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("path %q: not an object at %q", path, part)
		}
		current, ok = m[part]
		if !ok {
			return "", fmt.Errorf("path %q: key %q not found", path, part)
		}
	}

	// Marshal the extracted value back to JSON string
	out, err := json.Marshal(current)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// validateURL blocks requests to private/internal IP ranges (SSRF protection).
func validateURL(rawURL string) error {
	// Only allow http/https
	if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		return fmt.Errorf("only http/https URLs are allowed")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	host := u.Hostname() // strips the port and unbrackets IPv6 literals, e.g. "::1"

	// Block localhost
	if host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "0.0.0.0" {
		return fmt.Errorf("access to localhost is blocked")
	}

	// Try to parse as IP and check private ranges
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("access to private/internal networks is blocked")
		}
	}

	return nil
}

// truncate limits a string to maxLen bytes.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "...[truncated]"
}
