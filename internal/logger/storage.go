package logger

import (
	"database/sql"
	"time"

	"gateway/internal/db"
)

type LogStorage struct {
	db *db.DB
}

func NewLogStorage(db *db.DB) *LogStorage {
	return &LogStorage{db: db}
}

func (s *LogStorage) InitTables() error {
	_, err := s.db.Conn().Exec(`
		CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY,
			client_ip TEXT,
			client_port INTEGER,
			started_at DATETIME,
			ended_at DATETIME,
			total_requests INTEGER DEFAULT 0,
			last_request_at DATETIME
		);

		CREATE TABLE IF NOT EXISTS request_logs (
			id TEXT PRIMARY KEY,
			session_id TEXT,
			timestamp DATETIME,
			client_ip TEXT,
			request_method TEXT,
			request_path TEXT,
			request_headers TEXT,
			request_body TEXT,
			lapi_alias TEXT,
			matched_rapis TEXT,
			selected_rapi TEXT,
			upstream_url TEXT,
			upstream_headers TEXT,
			upstream_body TEXT,
			response_status INTEGER,
			response_headers TEXT,
			response_body TEXT,
			latency_ms INTEGER,
			tokens_used INTEGER,
			error_message TEXT,
			retry_count INTEGER DEFAULT 0,
			fallback_used BOOLEAN DEFAULT FALSE,
			status TEXT DEFAULT 'pending',
			completed_at DATETIME
		);

		CREATE TABLE IF NOT EXISTS log_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			request_id TEXT,
			event_type TEXT,
			timestamp DATETIME,
			data TEXT,
			FOREIGN KEY (request_id) REFERENCES request_logs(id)
		);

		CREATE INDEX IF NOT EXISTS idx_sessions_client ON sessions(client_ip);
		CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at);
		CREATE INDEX IF NOT EXISTS idx_logs_session ON request_logs(session_id);
		CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON request_logs(timestamp);
		CREATE INDEX IF NOT EXISTS idx_logs_lapi ON request_logs(lapi_alias);
		CREATE INDEX IF NOT EXISTS idx_logs_rapi ON request_logs(selected_rapi);
		CREATE INDEX IF NOT EXISTS idx_logs_status ON request_logs(status);
	`)
	return err
}

func (s *LogStorage) SaveSession(session *Session) error {
	_, err := s.db.Conn().Exec(`
		INSERT OR REPLACE INTO sessions (id, client_ip, client_port, started_at, ended_at, total_requests, last_request_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, session.ID, session.ClientIP, session.ClientPort, session.StartedAt, session.EndedAt, session.TotalRequests, session.LastRequestAt)
	return err
}

func (s *LogStorage) UpdateSessionRequestCount(sessionID string) error {
	_, err := s.db.Conn().Exec(`
		UPDATE sessions SET 
			total_requests = total_requests + 1,
			last_request_at = ?
		WHERE id = ?
	`, time.Now(), sessionID)
	return err
}

func (s *LogStorage) EndSession(sessionID string) error {
	_, err := s.db.Conn().Exec(`
		UPDATE sessions SET ended_at = ? WHERE id = ?
	`, time.Now(), sessionID)
	return err
}

func (s *LogStorage) SaveRequestLog(log *RequestLog) error {
	_, err := s.db.Conn().Exec(`
		INSERT OR REPLACE INTO request_logs (
			id, session_id, timestamp, client_ip, request_method, request_path,
			request_headers, request_body, lapi_alias, matched_rapis, selected_rapi,
			upstream_url, upstream_headers, upstream_body, response_status,
			response_headers, response_body, latency_ms, tokens_used, error_message,
			retry_count, fallback_used, status, completed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		log.ID, log.SessionID, log.Timestamp, log.ClientIP, log.RequestMethod, log.RequestPath,
		log.RequestHeaders, log.RequestBody, log.LapiAlias, log.MatchedRAPIs, log.SelectedRAPI,
		log.UpstreamURL, log.UpstreamHeaders, log.UpstreamBody, log.ResponseStatus,
		log.ResponseHeaders, log.ResponseBody, log.LatencyMS, log.TokensUsed, log.ErrorMessage,
		log.RetryCount, log.FallbackUsed, log.Status, log.CompletedAt,
	)
	return err
}

func (s *LogStorage) AppendEvent(requestID string, event *LogEvent) error {
	eventType := event.EventType.String()
	data := ""
	_, err := s.db.Conn().Exec(`
		INSERT INTO log_events (request_id, event_type, timestamp, data)
		VALUES (?, ?, ?, ?)
	`, requestID, eventType, event.Timestamp, data)
	return err
}

type SessionFilter struct {
	ClientIP     string
	StartTime    time.Time
	EndTime      time.Time
	Limit        int
	Offset       int
}

func (s *LogStorage) GetSessions(filter SessionFilter) ([]Session, error) {
	query := `SELECT id, client_ip, client_port, started_at, ended_at, total_requests, last_request_at FROM sessions WHERE 1=1`
	args := []interface{}{}

	if filter.ClientIP != "" {
		query += ` AND client_ip = ?`
		args = append(args, filter.ClientIP)
	}
	if !filter.StartTime.IsZero() {
		query += ` AND started_at >= ?`
		args = append(args, filter.StartTime)
	}
	if !filter.EndTime.IsZero() {
		query += ` AND started_at <= ?`
		args = append(args, filter.EndTime)
	}

	query += ` ORDER BY started_at DESC LIMIT ? OFFSET ?`
	args = append(args, filter.Limit, filter.Offset)

	rows, err := s.db.Conn().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessions := make([]Session, 0)
	for rows.Next() {
		var session Session
		var endedAt, lastRequestAt sql.NullTime
		err := rows.Scan(&session.ID, &session.ClientIP, &session.ClientPort, &session.StartedAt, &endedAt, &session.TotalRequests, &lastRequestAt)
		if err != nil {
			return nil, err
		}
		if endedAt.Valid {
			session.EndedAt = endedAt.Time
		}
		if lastRequestAt.Valid {
			session.LastRequestAt = lastRequestAt.Time
		}
		sessions = append(sessions, session)
	}
	return sessions, nil
}

type RequestLogFilter struct {
	SessionID    string
	RequestID    string
	LapiAlias    string
	SelectedRAPI string
	Status       string
	StartTime    time.Time
	EndTime      time.Time
	Limit        int
	Offset       int
}

func (s *LogStorage) GetRequestLogs(filter RequestLogFilter) ([]RequestLog, error) {
	query := `SELECT id, session_id, timestamp, client_ip, request_method, request_path,
		request_headers, request_body, lapi_alias, matched_rapis, selected_rapi,
		upstream_url, upstream_headers, upstream_body, response_status,
		response_headers, response_body, latency_ms, tokens_used, error_message,
		retry_count, fallback_used, status, completed_at FROM request_logs WHERE 1=1`
	args := []interface{}{}

	if filter.SessionID != "" {
		query += ` AND session_id = ?`
		args = append(args, filter.SessionID)
	}
	if filter.RequestID != "" {
		query += ` AND id = ?`
		args = append(args, filter.RequestID)
	}
	if filter.LapiAlias != "" {
		query += ` AND lapi_alias = ?`
		args = append(args, filter.LapiAlias)
	}
	if filter.SelectedRAPI != "" {
		query += ` AND selected_rapi = ?`
		args = append(args, filter.SelectedRAPI)
	}
	if filter.Status != "" {
		query += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if !filter.StartTime.IsZero() {
		query += ` AND timestamp >= ?`
		args = append(args, filter.StartTime)
	}
	if !filter.EndTime.IsZero() {
		query += ` AND timestamp <= ?`
		args = append(args, filter.EndTime)
	}

	query += ` ORDER BY timestamp DESC LIMIT ? OFFSET ?`
	args = append(args, filter.Limit, filter.Offset)

	rows, err := s.db.Conn().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	logs := make([]RequestLog, 0)
	for rows.Next() {
		var log RequestLog
		var completedAt sql.NullTime
		err := rows.Scan(
			&log.ID, &log.SessionID, &log.Timestamp, &log.ClientIP, &log.RequestMethod, &log.RequestPath,
			&log.RequestHeaders, &log.RequestBody, &log.LapiAlias, &log.MatchedRAPIs, &log.SelectedRAPI,
			&log.UpstreamURL, &log.UpstreamHeaders, &log.UpstreamBody, &log.ResponseStatus,
			&log.ResponseHeaders, &log.ResponseBody, &log.LatencyMS, &log.TokensUsed, &log.ErrorMessage,
			&log.RetryCount, &log.FallbackUsed, &log.Status, &completedAt,
		)
		if err != nil {
			return nil, err
		}
		if completedAt.Valid {
			log.CompletedAt = completedAt.Time
		}
		logs = append(logs, log)
	}
	return logs, nil
}

func (s *LogStorage) GetRequestDetail(requestID string) (*RequestLog, error) {
	var log RequestLog
	var completedAt sql.NullTime

	err := s.db.Conn().QueryRow(`
		SELECT id, session_id, timestamp, client_ip, request_method, request_path,
			request_headers, request_body, lapi_alias, matched_rapis, selected_rapi,
			upstream_url, upstream_headers, upstream_body, response_status,
			response_headers, response_body, latency_ms, tokens_used, error_message,
			retry_count, fallback_used, status, completed_at
		FROM request_logs WHERE id = ?
	`, requestID).Scan(
		&log.ID, &log.SessionID, &log.Timestamp, &log.ClientIP, &log.RequestMethod, &log.RequestPath,
		&log.RequestHeaders, &log.RequestBody, &log.LapiAlias, &log.MatchedRAPIs, &log.SelectedRAPI,
		&log.UpstreamURL, &log.UpstreamHeaders, &log.UpstreamBody, &log.ResponseStatus,
		&log.ResponseHeaders, &log.ResponseBody, &log.LatencyMS, &log.TokensUsed, &log.ErrorMessage,
		&log.RetryCount, &log.FallbackUsed, &log.Status, &completedAt,
	)
	if err != nil {
		return nil, err
	}
	if completedAt.Valid {
		log.CompletedAt = completedAt.Time
	}

	// Query events
	log.Events = make([]LogEvent, 0)
	rows, err := s.db.Conn().Query(`
		SELECT event_type, timestamp, data FROM log_events WHERE request_id = ? ORDER BY timestamp
	`, requestID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var event LogEvent
			var eventTypeStr string
			var dataStr sql.NullString
			err := rows.Scan(&eventTypeStr, &event.Timestamp, &dataStr)
			if err != nil {
				continue
			}
			event.RequestID = requestID
			event.EventType = ParseEventType(eventTypeStr)
			if dataStr.Valid && dataStr.String != "" {
				event.Data = map[string]interface{}{"raw": dataStr.String}
			}
			log.Events = append(log.Events, event)
		}
	}

	return &log, nil
}

func ParseEventType(s string) EventType {
	switch s {
	case "REQUEST_RECEIVED":
		return REQUEST_RECEIVED
	case "ROUTING_DECISION":
		return ROUTING_DECISION
	case "UPSTREAM_SENT":
		return UPSTREAM_SENT
	case "UPSTREAM_RESPONSE":
		return UPSTREAM_RESPONSE
	case "CLIENT_RESPONSE":
		return CLIENT_RESPONSE
	case "ERROR":
		return ERROR
	default:
		return -1
	}
}

func (s *LogStorage) CleanupOldRecords(maxAgeDays int, maxRecords int) error {
	tx, err := s.db.Conn().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	cutoffTime := time.Now().Add(-time.Duration(maxAgeDays) * 24 * time.Hour)
	_, err = tx.Exec(`DELETE FROM log_events WHERE request_id IN (SELECT id FROM request_logs WHERE timestamp < ?)`, cutoffTime)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DELETE FROM request_logs WHERE timestamp < ?`, cutoffTime)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DELETE FROM sessions WHERE ended_at < ? AND total_requests = 0`, cutoffTime)
	if err != nil {
		return err
	}

	if maxRecords > 0 {
		_, err = tx.Exec(`DELETE FROM request_logs WHERE id NOT IN (SELECT id FROM request_logs ORDER BY timestamp DESC LIMIT ?)`, maxRecords)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *LogStorage) GetSessionCount() (int, error) {
	var count int
	err := s.db.Conn().QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&count)
	return count, err
}

func (s *LogStorage) GetRequestLogCount() (int, error) {
	var count int
	err := s.db.Conn().QueryRow(`SELECT COUNT(*) FROM request_logs`).Scan(&count)
	return count, err
}