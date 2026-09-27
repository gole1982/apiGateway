package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "proxy.cfg")
	if err := os.WriteFile(p, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

// 缺失文件 → 全默认，与 Load 的旧回退行为一致（容器无挂载可启动）。
func TestLoadFromMissingFileReturnsDefaults(t *testing.T) {
	c, err := LoadFrom(filepath.Join(t.TempDir(), "no-such-file.cfg"))
	if err != nil {
		t.Fatalf("LoadFrom missing: %v", err)
	}
	if c.ProxyPort != 13579 || c.WebPort != 24680 {
		t.Errorf("ports = %d/%d, want 13579/24680", c.ProxyPort, c.WebPort)
	}
	if c.CooldownSec != 10 || c.MaxCooldownSec != 120 || c.BillingCooldownSec != 1800 {
		t.Errorf("cooldowns = %d/%d/%d, want 10/120/1800",
			c.CooldownSec, c.MaxCooldownSec, c.BillingCooldownSec)
	}
	if c.CapabilityBlockSec != 86400 || c.RequestMaxWaitSec != 120 {
		t.Errorf("capability/request-max-wait = %d/%d, want 86400/120",
			c.CapabilityBlockSec, c.RequestMaxWaitSec)
	}
	if c.KeyCursorScope != "session" {
		t.Errorf("KeyCursorScope = %q, want session (default)", c.KeyCursorScope)
	}
	if c.RetryOnStartup != true || c.RetryConcurrency != 8 || c.RetryTimeoutSec != 15 {
		t.Errorf("health retry = %v/%d/%d, want true/8/15",
			c.RetryOnStartup, c.RetryConcurrency, c.RetryTimeoutSec)
	}
	if c.Sync.PollIntervalSec != 60 {
		t.Errorf("poll interval = %d, want 60", c.Sync.PollIntervalSec)
	}
	if c.Sync.Enabled() {
		t.Error("sync should be disabled without source_url")
	}
}

// 全量自定义：每个段都解析，且类型正确。
func TestLoadFromCustomFile(t *testing.T) {
	p := writeCfg(t, `
proxy_port = 43210
web_port = 43211
dial_timeout_sec = 5
response_timeout_sec = 60
cooldown_sec = 7
max_cooldown_sec = 70
billing_cooldown_sec = 700
capability_block_sec = 7000
request_max_wait_sec = 70
key_cursor_scope = platform
[health]
retry_on_startup = false
retry_concurrency = 4
retry_timeout_sec = 9
[log]
level = debug
file = true
file_path = logs/x.log
[sync]
source_url = https://example.supabase.co/rest/v1/rpc/get_bundle
version_url = https://example.supabase.co/rest/v1/rpc/get_version
anon_key = anon-key
center_key = abcdef
poll_interval_sec = 30
[management]
supabase_url = https://example.supabase.co
service_key = secret-key
center_key = 123456
`)
	c, err := LoadFrom(p)
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if c.ProxyPort != 43210 || c.WebPort != 43211 {
		t.Errorf("ports = %d/%d, want 43210/43211", c.ProxyPort, c.WebPort)
	}
	if c.CooldownSec != 7 || c.MaxCooldownSec != 70 || c.BillingCooldownSec != 700 {
		t.Errorf("cooldowns wrong: %+v", c)
	}
	if c.CapabilityBlockSec != 7000 || c.RequestMaxWaitSec != 70 {
		t.Errorf("capability/request-max-wait = %d/%d, want 7000/70",
			c.CapabilityBlockSec, c.RequestMaxWaitSec)
	}
	if c.KeyCursorScope != "platform" {
		t.Errorf("KeyCursorScope = %q, want platform", c.KeyCursorScope)
	}
	if c.RetryOnStartup || c.RetryConcurrency != 4 || c.RetryTimeoutSec != 9 {
		t.Errorf("health wrong: %+v", c)
	}
	if c.LogLevel != "debug" || !c.LogFile || c.LogFilePath != "logs/x.log" {
		t.Errorf("log wrong: %+v", c)
	}
	if !c.Sync.Enabled() || c.Sync.AnonKey != "anon-key" || c.Sync.PollIntervalSec != 30 {
		t.Errorf("sync wrong: %+v", c.Sync)
	}
	if !c.Management.Configured() || c.Management.ServiceKey != "secret-key" {
		t.Errorf("management wrong: %+v", c.Management)
	}
}

// key_cursor_scope 缺失 → session（与 scheduler 默认一致）。
// 非法值原样保留，由 scheduler.ConfigFromAppConfig 归一化为 session。
func TestLoadFromCursorScopePassthrough(t *testing.T) {
	c, err := LoadFrom(writeCfg(t, "proxy_port = 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.KeyCursorScope != "session" {
		t.Errorf("default = %q, want session", c.KeyCursorScope)
	}
	c2, err := LoadFrom(writeCfg(t, "key_cursor_scope = typo-value\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c2.KeyCursorScope != "typo-value" {
		t.Errorf("custom value should pass through, got %q", c2.KeyCursorScope)
	}
}
