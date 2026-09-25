package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"gateway/internal/db"
	"gateway/internal/logger"
)

const logLevelSettingKey = "log_level"

var validLogLevels = map[string]struct{}{
	"debug": {},
	"info":  {},
	"warn":  {},
	"error": {},
}

func normalizeLogLevel(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "warning" {
		value = "warn"
	}
	if value == "err" {
		value = "error"
	}
	_, ok := validLogLevels[value]
	return value, ok
}

func savedLogLevel(fallback string) string {
	if db.Get() == nil {
		return fallback
	}
	if value, err := db.Get().GetSetting(logLevelSettingKey); err == nil {
		if normalized, ok := normalizeLogLevel(value); ok {
			return normalized
		}
	}
	return fallback
}

func handleLogLevel(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"level":  logger.LevelName(logger.CurrentLevel()),
			"levels": []string{"debug", "info", "warn", "error"},
		})
	case http.MethodPost, http.MethodPut:
		var body struct {
			Level string `json:"level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSONError(w, http.StatusBadRequest, err)
			return
		}
		level, ok := normalizeLogLevel(body.Level)
		if !ok {
			writeJSONError(w, http.StatusBadRequest, errors.New("日志级别必须是 debug、info、warn 或 error"))
			return
		}
		if err := db.Get().SetSetting(logLevelSettingKey, level); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
		logger.SetLogLevel(logger.ParseLevel(level))
		_ = json.NewEncoder(w).Encode(map[string]string{"level": level})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
	}
}
