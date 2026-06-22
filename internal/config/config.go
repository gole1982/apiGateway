package config

import (
	"os"
	"path/filepath"

	"gopkg.in/ini.v1"
)

type Config struct {
	ProxyPort          int
	WebPort            int
	RefreshIntervalSec int
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
			RefreshIntervalSec: 600,
		}, nil
	}

	return &Config{
		ProxyPort:          cfg.Section("").Key("proxy_port").MustInt(13579),
		WebPort:            cfg.Section("").Key("web_port").MustInt(24680),
		RefreshIntervalSec: cfg.Section("").Key("refresh_interval_sec").MustInt(600),
	}, nil
}
