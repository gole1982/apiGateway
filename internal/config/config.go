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
	// BillingCooldownSec is the cooldown applied to recoverable billing errors
	// (欠费/积分不足), default 1800 (30 min). Longer than CooldownSec so an
	// out-of-credit key does not burn a failed upstream attempt on every request.
	CooldownSec        int
	MaxCooldownSec     int
	BillingCooldownSec int
	// CapabilityBlockSec is how long a key×model capability block lives after
	// the platform denied a key for a model (404 "model not found" etc.),
	// default 86400 (24 h). While blocked the pair is skipped; after expiry it
	// is retried automatically so a re-granted permission is picked up.
	CapabilityBlockSec int
	RequestMaxWaitSec  int

	// Startup health recovery
	// RetryOnStartup, when true, makes the gateway probe every RAPI that is
	// persisted as unavailable (available=0) once the servers are up, and
	// restore the ones that respond. RetryConcurrency bounds parallelism,
	// RetryTimeoutSec is the per-RAPI probe deadline.
	RetryOnStartup   bool
	RetryConcurrency int
	RetryTimeoutSec  int

	// Structured console log settings.
	// LogLevel is one of "debug"/"info"/"warn"/"error" (default "info"). Maps to
	// the stdlib slog level that gates the JSON stderr stream.
	// LogFile, when true, additionally tees the JSON stream to LogFilePath
	// (default "logs/gateway.log") so operators can collect rotated files
	// without scraping stderr.
	LogLevel    string
	LogFile     bool
	LogFilePath string

	// Sync（代理端读路径，对应 [sync] 段）。配置了 source_url 才启用中心同步；
	// 否则网关以纯本地模式运行。设计：docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md
	Sync Sync
}

// Sync 描述代理端从中心 Supabase 拉取定义快照的参数。
//
//   - SourceURL / VersionURL：Supabase PostgREST 的 get_bundle / get_version RPC 全 URL
//   - AnonKey：Supabase anon 公钥（只读，可放代理端）
//   - CenterKey：32 字节 hex，token 边界解密用（管理端写入中心时用同一把加密）
//   - PollIntervalSec：代理轮询中心版本号的间隔（默认 60s）
type Sync struct {
	SourceURL       string
	VersionURL      string
	AnonKey         string
	CenterKey       string
	PollIntervalSec int
}

// Enabled 表示是否启用中心同步（配了 source_url 即启用）。
func (s Sync) Enabled() bool {
	return strings.TrimSpace(s.SourceURL) != ""
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
			ResponseTimeoutSec: 300, CooldownSec: 10,
			MaxCooldownSec:     120,
			BillingCooldownSec: 1800,
			CapabilityBlockSec: 86400,
			RequestMaxWaitSec:  120,
			RetryOnStartup:     true,
			RetryConcurrency:   8,
			RetryTimeoutSec:    15,
			LogLevel:           "info",
			LogFile:            false,
			LogFilePath:        "logs/gateway.log",
			Sync:               Sync{PollIntervalSec: 60},
		}, nil
	}

	return &Config{
		ProxyPort:          cfg.Section("").Key("proxy_port").MustInt(13579),
		WebPort:            cfg.Section("").Key("web_port").MustInt(24680),
		DialTimeoutSec:     cfg.Section("").Key("dial_timeout_sec").MustInt(30),
		ResponseTimeoutSec: cfg.Section("").Key("response_timeout_sec").MustInt(300),
		CooldownSec:        cfg.Section("").Key("cooldown_sec").MustInt(10),
		MaxCooldownSec:     cfg.Section("").Key("max_cooldown_sec").MustInt(120),
		BillingCooldownSec: cfg.Section("").Key("billing_cooldown_sec").MustInt(1800),
		CapabilityBlockSec: cfg.Section("").Key("capability_block_sec").MustInt(86400),
		RequestMaxWaitSec:  cfg.Section("").Key("request_max_wait_sec").MustInt(120),
		RetryOnStartup:     cfg.Section("health").Key("retry_on_startup").MustBool(true),
		RetryConcurrency:   cfg.Section("health").Key("retry_concurrency").MustInt(8),
		RetryTimeoutSec:    cfg.Section("health").Key("retry_timeout_sec").MustInt(15),
		LogLevel:           cfg.Section("log").Key("level").MustString("info"),
		LogFile:            cfg.Section("log").Key("file").MustBool(false),
		LogFilePath:        cfg.Section("log").Key("file_path").MustString("logs/gateway.log"),
		Sync: Sync{
			SourceURL:       cfg.Section("sync").Key("source_url").MustString(""),
			VersionURL:      cfg.Section("sync").Key("version_url").MustString(""),
			AnonKey:         cfg.Section("sync").Key("anon_key").MustString(""),
			CenterKey:       cfg.Section("sync").Key("center_key").MustString(""),
			PollIntervalSec: cfg.Section("sync").Key("poll_interval_sec").MustInt(60),
		},
	}, nil
}
