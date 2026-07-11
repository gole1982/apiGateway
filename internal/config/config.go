package config

import (
	"os"
	"path/filepath"

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
		}, nil
	}

	return &Config{
		ProxyPort:      cfg.Section("").Key("proxy_port").MustInt(13579),
		WebPort:        cfg.Section("").Key("web_port").MustInt(24680),
		DialTimeoutSec: cfg.Section("").Key("dial_timeout_sec").MustInt(30),
		ResponseTimeoutSec: cfg.Section("").Key("response_timeout_sec").MustInt(300),
		CooldownSec:        cfg.Section("").Key("cooldown_sec").MustInt(10),
		MaxCooldownSec:     cfg.Section("").Key("max_cooldown_sec").MustInt(120),
		RequestMaxWaitSec:  cfg.Section("").Key("request_max_wait_sec").MustInt(120),
	}, nil
}
