package logger

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

type SessionTracker struct {
	sessions map[net.Conn]*Session
	mu       sync.RWMutex
	logger   *Logger
}

type contextKey string

const sessionIDKey contextKey = "sessionID"

func NewSessionTracker(logger *Logger) *SessionTracker {
	return &SessionTracker{
		sessions: make(map[net.Conn]*Session),
		logger:   logger,
	}
}

func (t *SessionTracker) OnConnState(conn net.Conn, state http.ConnState) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch state {
	case http.StateNew:
		clientIP, clientPort := parseConnAddr(conn.RemoteAddr())
		session := &Session{
			ID:            uuid.New().String(),
			ClientIP:      clientIP,
			ClientPort:    clientPort,
			StartedAt:     time.Now(),
			TotalRequests: 0,
		}
		t.sessions[conn] = session
		if t.logger != nil && t.logger.Storage != nil {
			go t.logger.Storage.SaveSession(session)
		}

	case http.StateClosed, http.StateHijacked:
		if session, ok := t.sessions[conn]; ok {
			session.EndedAt = time.Now()
			if t.logger != nil && t.logger.Storage != nil {
				go t.logger.Storage.EndSession(session.ID)
			}
			delete(t.sessions, conn)
		}
	}
}

func (t *SessionTracker) GetSessionID(r *http.Request) string {
	if val := r.Context().Value(sessionIDKey); val != nil {
		return val.(string)
	}
	return generateFallbackSessionID(r)
}

// InjectSessionID stamps a session id onto the request context and returns
// (sessionID, stable).
//
// stable=true 意味着该 id 由 OnConnState 在 StateNew 时铸造、绑定一条
// 长连接 —— 同一连接上的后续请求会拿到同一个 id。
//
// stable=false 意味着这是 generateFallbackSessionID 现造的 UUID
// （RemoteAddr + 纳秒时间戳），**每个请求都是全新值**。调用方绝不能把它
// 当作会话身份长期持有（见 scheduler.PickAvailableKey 的游标作用域）：
// 拿它做 key 会让每张游标都从 0 开始，等于把全部流量固定到排序最靠前的
// 那一个 key 上。
func (t *SessionTracker) InjectSessionID(r *http.Request) (sessionID string, stable bool) {
	sessionID = t.GetSessionID(r)
	ctx := context.WithValue(r.Context(), sessionIDKey, sessionID)
	*r = *r.WithContext(ctx)

	// Increment request count for this session (best-effort, not from conn lookup)
	// Must use Lock (not RLock) because we mutate TotalRequests and LastRequestAt.
	t.mu.Lock()
	for _, session := range t.sessions {
		if session.ID == sessionID {
			session.TotalRequests++
			session.LastRequestAt = time.Now()
			stable = true
			if t.logger != nil && t.logger.Storage != nil {
				go t.logger.Storage.UpdateSessionRequestCount(session.ID)
			}
			break
		}
	}
	t.mu.Unlock()

	return sessionID, stable
}

func parseConnAddr(addr net.Addr) (string, int) {
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return addr.String(), 0
	}
	return tcpAddr.IP.String(), tcpAddr.Port
}

func generateFallbackSessionID(r *http.Request) string {
	clientIP := r.RemoteAddr
	now := time.Now().UnixNano()
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(clientIP+string(rune(now)))).String()
}
