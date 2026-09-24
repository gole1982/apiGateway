package db

import (
	"database/sql"
	"time"
)

// GetSetting 读取一条设置；不存在返回空串、无错误。
func (db *DB) GetSetting(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var v string
	err := db.conn.QueryRow(`SELECT value FROM settings WHERE key = ?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

// SetSetting 写入一条设置（upsert）。
func (db *DB) SetSetting(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
		key, value, time.Now())
	return err
}

// SystemLogRow 是一条系统/中心互联日志。
type SystemLogRow struct {
	ID        int64     `json:"id"`
	Level     string    `json:"level"`
	Category  string    `json:"category"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

// InsertSystemLog 写入一条系统日志（best-effort：调用方已 recover/忽略错误）。
func (db *DB) InsertSystemLog(level, category, message string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`INSERT INTO system_logs (level, category, message, created_at) VALUES (?, ?, ?, ?)`,
		level, category, message, time.Now())
	return err
}

// GetSystemLogs 取最近 N 条系统日志（按时间倒序）。
func (db *DB) GetSystemLogs(limit int) ([]SystemLogRow, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.conn.Query(`SELECT id, level, category, message, created_at FROM system_logs ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SystemLogRow
	for rows.Next() {
		var r SystemLogRow
		if err := rows.Scan(&r.ID, &r.Level, &r.Category, &r.Message, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}
