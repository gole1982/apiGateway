package tools

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers ---

func TestTruncate(t *testing.T) {
	cases := []struct {
		name  string
		input string
		max   int
		want  string
	}{
		{"short", "abc", 10, "abc"},
		{"exact", "abc", 3, "abc"},
		{"overflow", "abcdefghij", 4, "abcd...[truncated]"},
		{"empty", "", 10, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := truncate(c.input, c.max)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestRenderTemplate(t *testing.T) {
	t.Run("basic substitution", func(t *testing.T) {
		out, err := renderTemplate(`{"query": "{{.query}}"}`, map[string]interface{}{"query": "hello"})
		require.NoError(t, err)
		assert.Equal(t, `{"query": "hello"}`, out)
	})
	t.Run("missing key -> no value marker", func(t *testing.T) {
		// With missingkey=zero and map[string]interface{}, a missing key renders
		// as the literal "<no value>" (the default text format of a nil
		// interface). This documents the actual behaviour the executor relies on.
		out, err := renderTemplate(`{{.missing}}`, map[string]interface{}{})
		require.NoError(t, err)
		assert.Equal(t, "<no value>", out)
	})
	t.Run("invalid template -> error", func(t *testing.T) {
		_, err := renderTemplate(`{{ .broken`, map[string]interface{}{})
		assert.Error(t, err)
	})
	t.Run("nested map access", func(t *testing.T) {
		out, err := renderTemplate(`{{.user.name}}`, map[string]interface{}{
			"user": map[string]interface{}{"name": "alice"},
		})
		require.NoError(t, err)
		assert.Equal(t, "alice", out)
	})
}

func TestExtractJSONPath(t *testing.T) {
	t.Run("nested path", func(t *testing.T) {
		data := []byte(`{"data":{"results":[1,2,3]}}`)
		out, err := extractJSONPath(data, "data.results")
		require.NoError(t, err)
		assert.Equal(t, "[1,2,3]", out)
	})
	t.Run("top-level key", func(t *testing.T) {
		data := []byte(`{"ok":true,"n":5}`)
		out, err := extractJSONPath(data, "ok")
		require.NoError(t, err)
		assert.Equal(t, "true", out)
	})
	t.Run("missing key -> error", func(t *testing.T) {
		data := []byte(`{"a":1}`)
		_, err := extractJSONPath(data, "b")
		assert.Error(t, err)
	})
	t.Run("path through non-object -> error", func(t *testing.T) {
		data := []byte(`{"a":[1,2]}`)
		_, err := extractJSONPath(data, "a.b")
		assert.Error(t, err)
	})
	t.Run("invalid JSON -> error", func(t *testing.T) {
		_, err := extractJSONPath([]byte(`not json`), "a")
		assert.Error(t, err)
	})
}

func TestValidateURL(t *testing.T) {
	blocked := []string{
		"http://localhost/x",
		"http://127.0.0.1/x",
		"http://[::1]/x",
		"http://0.0.0.0/x",
		"http://10.0.0.1/x",
		"http://192.168.1.1/x",
		"http://172.16.0.1/x",
		"http://172.31.255.255/x",
		"ftp://example.com/x", // non-http scheme
		"file:///etc/passwd",
	}
	for _, u := range blocked {
		t.Run("blocked: "+u, func(t *testing.T) {
			err := validateURL(u)
			assert.Error(t, err, "%s should be blocked", u)
		})
	}

	allowed := []string{
		"http://example.com/x",
		"https://api.example.com:8443/v1",
		"https://8.8.8.8/dns", // public IP
	}
	for _, u := range allowed {
		t.Run("allowed: "+u, func(t *testing.T) {
			err := validateURL(u)
			assert.NoError(t, err, "%s should be allowed", u)
		})
	}

	t.Run("172.32 is public (not private range)", func(t *testing.T) {
		// 172.32.0.0 is outside the 172.16/12 private range
		err := validateURL("http://172.32.0.1/x")
		assert.NoError(t, err)
	})
}

// --- NewExecutor factory ---

func TestNewExecutor(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		e, err := NewExecutor(&Tool{ExecutorType: "http", TimeoutMs: 1000})
		require.NoError(t, err)
		_, ok := e.(*HTTPExecutor)
		assert.True(t, ok)
	})
	t.Run("shell", func(t *testing.T) {
		e, err := NewExecutor(&Tool{ExecutorType: "shell"})
		require.NoError(t, err)
		_, ok := e.(*ShellExecutor)
		assert.True(t, ok)
	})
	t.Run("builtin", func(t *testing.T) {
		e, err := NewExecutor(&Tool{ExecutorType: "builtin", TimeoutMs: 1000})
		require.NoError(t, err)
		_, ok := e.(*BuiltinExecutor)
		assert.True(t, ok)
	})
	t.Run("unknown -> error", func(t *testing.T) {
		_, err := NewExecutor(&Tool{ExecutorType: "nope"})
		assert.Error(t, err)
	})
}

// --- HTTPExecutor ---

// withSSRFDisabled swaps the package-level ssrfCheck with a permissive
// validator for the duration of the test, so httptest servers (which bind to
// 127.0.0.1) can be exercised. SSRF validation logic itself is covered
// directly by TestValidateURL.
func withSSRFDisabled(t *testing.T) {
	t.Helper()
	prev := ssrfCheck
	ssrfCheck = func(string) error { return nil }
	t.Cleanup(func() { ssrfCheck = prev })
}

func newHTTPTool(t *testing.T, url, bodyTmpl, resultPath string) *Tool {
	t.Helper()
	cfg := map[string]interface{}{"method": "POST", "url": url}
	if bodyTmpl != "" {
		cfg["body_template"] = bodyTmpl
	}
	if resultPath != "" {
		cfg["result_path"] = resultPath
	}
	cfgBytes, _ := json.Marshal(cfg)
	return &Tool{
		ExecutorType:   "http",
		ExecutorConfig: string(cfgBytes),
		TimeoutMs:     10000,
		Enabled:       true,
	}
}

func TestHTTPExecutor_BasicPOST(t *testing.T) {
	var gotBody string
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	withSSRFDisabled(t)

	tool := newHTTPTool(t, srv.URL, `{"query":"{{.query}}"}`, "")
	tool.ExecutorConfig = mustReMarshal(t, map[string]interface{}{
		"method":        "POST",
		"url":           srv.URL,
		"body_template": `{"query":"{{.query}}"}`,
		"headers":       map[string]string{"Authorization": "Bearer {{.token}}"},
	})
	exec, err := NewExecutor(tool)
	require.NoError(t, err)

	out, err := exec.Execute(context.Background(), tool, map[string]interface{}{
		"query": "hello",
		"token": "sekret",
	})
	require.NoError(t, err)
	assert.Equal(t, `{"ok":true}`, out)
	assert.Equal(t, `{"query":"hello"}`, gotBody)
	assert.Equal(t, "Bearer sekret", gotAuth)
}

func TestHTTPExecutor_ResultPath(t *testing.T) {
	withSSRFDisabled(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"results":[1,2,3]}}`))
	}))
	defer srv.Close()

	tool := newHTTPTool(t, srv.URL, "", "data.results")
	exec, err := NewExecutor(tool)
	require.NoError(t, err)

	out, err := exec.Execute(context.Background(), tool, map[string]interface{}{"q": 1})
	require.NoError(t, err)
	assert.Equal(t, "[1,2,3]", out)
}

func TestHTTPExecutor_DefaultBodyIsArgumentsJSON(t *testing.T) {
	withSSRFDisabled(t)
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	// No body_template, but args present -> executor marshals args as JSON body
	tool := newHTTPTool(t, srv.URL, "", "")
	exec, err := NewExecutor(tool)
	require.NoError(t, err)

	_, err = exec.Execute(context.Background(), tool, map[string]interface{}{"q": "x", "n": 2})
	require.NoError(t, err)

	var got map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(gotBody), &got))
	assert.Equal(t, "x", got["q"])
	assert.EqualValues(t, 2, got["n"])
}

func TestHTTPExecutor_4xxError(t *testing.T) {
	withSSRFDisabled(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"err":"bad"}`, http.StatusBadRequest)
	}))
	defer srv.Close()

	tool := newHTTPTool(t, srv.URL, "", "")
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}

func TestHTTPExecutor_SSRFBlocked(t *testing.T) {
	// URL points to loopback -> rejected before any request
	tool := newHTTPTool(t, "http://127.0.0.1:9/", "", "")
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "blocked")
}

func TestHTTPExecutor_LongResultTruncated(t *testing.T) {
	withSSRFDisabled(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 2x the truncate limit (8192 bytes)
		w.Write([]byte(strings.Repeat("a", 2*8192)))
	}))
	defer srv.Close()

	tool := newHTTPTool(t, srv.URL, "", "")
	exec, _ := NewExecutor(tool)
	out, err := exec.Execute(context.Background(), tool, nil)
	require.NoError(t, err)
	assert.Len(t, out, 8192+len("...[truncated]"))
	assert.True(t, strings.HasSuffix(out, "...[truncated]"))
}

func TestHTTPExecutor_InvalidConfig(t *testing.T) {
	tool := &Tool{ExecutorType: "http", ExecutorConfig: `not json`}
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid http executor config")
}

func TestHTTPExecutor_MissingURL(t *testing.T) {
	tool := &Tool{ExecutorType: "http", ExecutorConfig: `{}`}
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "url is required")
}

// --- ShellExecutor ---

func TestShellExecutor_IsAllowed(t *testing.T) {
	e := &ShellExecutor{whitelist: []string{"python", "node"}}
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"exact", "python", true},
		{"case-insensitive", "Python", true},
		{"exe suffix", "python.exe", true},
		{"EXE suffix uppercase", "NODE.EXE", true},
		{"not in whitelist", "rm", false},
		{"empty", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, e.isAllowed(c.in))
		})
	}
}

func TestShellExecutor_RejectedCommand(t *testing.T) {
	// "evilrm" is not in the whitelist -> rejected before execution
	tool := &Tool{
		ExecutorType:   "shell",
		ExecutorConfig: `{"command":"evilrm","args":["-rf","/"]}`,
		TimeoutMs:      5000,
	}
	exec, err := NewExecutor(tool)
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in the shell whitelist")
}

func TestShellExecutor_RunAllowedCommand(t *testing.T) {
	// `go env GOOS` works cross-platform since the test binary runs under the
	// same toolchain that built it; `go` is guaranteed on PATH in `go test`.
	prev := shellWhitelist
	SetShellWhitelist([]string{"go"})
	t.Cleanup(func() { shellWhitelist = prev })

	tool := &Tool{
		ExecutorType:   "shell",
		ExecutorConfig: `{"command":"go","args":["env","GOOS"]}`,
		TimeoutMs:      15000,
	}
	exec, err := NewExecutor(tool)
	require.NoError(t, err)
	out, err := exec.Execute(context.Background(), tool, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, strings.TrimSpace(out), "go env GOOS should produce output")
}

func TestShellExecutor_CommandFailure(t *testing.T) {
	prev := shellWhitelist
	SetShellWhitelist([]string{"go"})
	t.Cleanup(func() { shellWhitelist = prev })

	// `go nonexistent-flag` exits non-zero
	tool := &Tool{
		ExecutorType:   "shell",
		ExecutorConfig: `{"command":"go","args":["--badly-broken-flag"]}`,
		TimeoutMs:      15000,
	}
	exec, err := NewExecutor(tool)
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "command failed")
}

func TestShellExecutor_TemplateArgs(t *testing.T) {
	prev := shellWhitelist
	SetShellWhitelist([]string{"go"})
	t.Cleanup(func() { shellWhitelist = prev })

	// Arg is a template rendered from arguments
	tool := &Tool{
		ExecutorType:   "shell",
		ExecutorConfig: `{"command":"go","args":["env","{{.VAR}}"]}`,
		TimeoutMs:      15000,
	}
	exec, err := NewExecutor(tool)
	require.NoError(t, err)
	out, err := exec.Execute(context.Background(), tool, map[string]interface{}{"VAR": "GOOS"})
	require.NoError(t, err)
	assert.NotEmpty(t, strings.TrimSpace(out))
}

func TestShellExecutor_InvalidConfig(t *testing.T) {
	tool := &Tool{ExecutorType: "shell", ExecutorConfig: `not json`}
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid shell executor config")
}

func TestShellExecutor_MissingCommand(t *testing.T) {
	tool := &Tool{ExecutorType: "shell", ExecutorConfig: `{}`}
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "command is required")
}

// --- BuiltinExecutor ---

func TestBuiltinHTTPGet_OK(t *testing.T) {
	withSSRFDisabled(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`hello world`))
	}))
	defer srv.Close()

	tool := &Tool{
		ExecutorType:   "builtin",
		ExecutorConfig: `{"handler":"http_get"}`,
		TimeoutMs:      10000,
	}
	exec, err := NewExecutor(tool)
	require.NoError(t, err)
	out, err := exec.Execute(context.Background(), tool, map[string]interface{}{"url": srv.URL})
	require.NoError(t, err)
	assert.Equal(t, "hello world", out)
}

func TestBuiltinHTTPGet_MissingURL(t *testing.T) {
	tool := &Tool{ExecutorType: "builtin", ExecutorConfig: `{"handler":"http_get"}`}
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, map[string]interface{}{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "url")
}

func TestBuiltinHTTPGet_Non2xx(t *testing.T) {
	withSSRFDisabled(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	tool := &Tool{ExecutorType: "builtin", ExecutorConfig: `{"handler":"http_get"}`}
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, map[string]interface{}{"url": srv.URL})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestBuiltinHTTPGet_SSRFBlocked(t *testing.T) {
	tool := &Tool{ExecutorType: "builtin", ExecutorConfig: `{"handler":"http_get"}`}
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, map[string]interface{}{"url": "http://127.0.0.1:9/"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "blocked")
}

func TestBuiltinExecutor_UnknownHandler(t *testing.T) {
	tool := &Tool{ExecutorType: "builtin", ExecutorConfig: `{"handler":"does_not_exist"}`}
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown builtin handler")
}

func TestBuiltinExecutor_InvalidConfig(t *testing.T) {
	tool := &Tool{ExecutorType: "builtin", ExecutorConfig: `not json`}
	exec, _ := NewExecutor(tool)
	_, err := exec.Execute(context.Background(), tool, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid builtin executor config")
}

func TestAvailableHandlers(t *testing.T) {
	// http_get is the only builtin handler registered by default
	got := availableHandlers()
	assert.Contains(t, got, "http_get")
}

// --- helper for tests ---

func mustReMarshal(t *testing.T, m map[string]interface{}) string {
	t.Helper()
	b, err := json.Marshal(m)
	require.NoError(t, err)
	return string(b)
}
