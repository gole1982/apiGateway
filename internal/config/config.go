package config

import (
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/ini.v1"
)

type Config struct {
	ProxyPort int
	WebPort   int

	// HTTP client timeouts
	// DialTimeoutSec is the TCP connect + TLS handshake deadline (default 30s).
	// ResponseTimeoutSec is the total deadline for a complete upstream response (default 300s).
	// For slow free APIs the first token can take 30-60 s; 300 s gives plenty of headroom.
	DialTimeoutSec     int
	ResponseTimeoutSec int

	// Scheduler behaviour
	// CooldownSec is the base cooldown after the first RAPI/key failure (default 10s).
	// MaxCooldownSec caps the exponential backoff (default 120s).
	// RequestMaxWaitSec is how long a request queues waiting for a free RAPI before giving up (default 120s).
	CooldownSec        int
	MaxCooldownSec     int
	RequestMaxWaitSec  int

	// Agent tool-calling loop
	AgentEnabled        bool
	AgentMaxIterations  int
	AgentTimeoutSec     int
	AgentShellWhitelist []string // allowed shell commands (e.g. python, node, curl)

	// Startup health recovery
	// RetryOnStartup, when true, makes the gateway probe every RAPI that is
	// persisted as unavailable (available=0) once the servers are up, and
	// restore the ones that respond. RetryConcurrency bounds parallelism,
	// RetryTimeoutSec is the per-RAPI probe deadline.
	RetryOnStartup   bool
	RetryConcurrency int
	RetryTimeoutSec  int
}

func Load() (*Config, error) {
	exePath, err := os.Executable()
	if err != nil {
		return nil, err
	}
	exeDir := filepath.Dir(exePath)
	cfgPath := filepath.Join(exeDir, "proxy.cfg")

	cfg, err := ini.Load(cfgPath)
	if err != nil {
		return &Config{
			ProxyPort:          13579,
			WebPort:            24680,
			DialTimeoutSec:     30,
			ResponseTimeoutSec: 300,
			CooldownSec:        10,
			MaxCooldownSec:     120,
			RequestMaxWaitSec:  120,
			AgentEnabled:       true,
			AgentMaxIterations: 10,
			AgentTimeoutSec:    120,
			AgentShellWhitelist: []string{"python", "node", "curl"},
			RetryOnStartup:     true,
			RetryConcurrency:   8,
			RetryTimeoutSec:    15,
		}, nil
	}

	// Parse shell whitelist (comma-separated)
	shellWL := cfg.Section("agent").Key("shell_whitelist").MustString("python,node,curl")
	var whitelist []string
	for _, s := range strings.Split(shellWL, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			whitelist = append(whitelist, s)
		}
	}

	return &Config{
		ProxyPort:      cfg.Section("").Key("proxy_port").MustInt(13579),
		WebPort:        cfg.Section("").Key("web_port").MustInt(24680),
		DialTimeoutSec: cfg.Section("").Key("dial_timeout_sec").MustInt(30),
		ResponseTimeoutSec: cfg.Section("").Key("response_timeout_sec").MustInt(300),
		CooldownSec:        cfg.Section("").Key("cooldown_sec").MustInt(10),
		MaxCooldownSec:     cfg.Section("").Key("max_cooldown_sec").MustInt(120),
		RequestMaxWaitSec:  cfg.Section("").Key("request_max_wait_sec").MustInt(120),
		AgentEnabled:        cfg.Section("agent").Key("enabled").MustBool(true),
		AgentMaxIterations:  cfg.Section("agent").Key("max_iterations").MustInt(10),
		AgentTimeoutSec:     cfg.Section("agent").Key("total_timeout_sec").MustInt(120),
		AgentShellWhitelist: whitelist,
		RetryOnStartup:      cfg.Section("health").Key("retry_on_startup").MustBool(true),
		RetryConcurrency:    cfg.Section("health").Key("retry_concurrency").MustInt(8),
		RetryTimeoutSec:     cfg.Section("health").Key("retry_timeout_sec").MustInt(15),
	}, nil
}
