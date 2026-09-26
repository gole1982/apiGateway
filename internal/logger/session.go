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

// ensureSessionLocked 返回该连接的会话，不存在即创建（与 StateNew 等价）。
// 调用方必须持有 t.mu（写锁）。
func (t *SessionTracker) ensureSessionLocked(conn net.Conn) *Session {
	if s, ok := t.sessions[conn]; ok {
		return s
	}
	clientIP, clientPort := parseConnAddr(conn.RemoteAddr())
	s := &Session{
		ID:            uuid.New().String(),
		ClientIP:      clientIP,
		ClientPort:    clientPort,
		StartedAt:     time.Now(),
		TotalRequests: 0,
	}
	t.sessions[conn] = s
	if t.logger != nil && t.logger.Storage != nil {
		go t.logger.Storage.SaveSession(s)
	}
	return s
}

func (t *SessionTracker) OnConnState(conn net.Conn, state http.ConnState) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch state {
	case http.StateNew:
		t.ensureSessionLocked(conn)

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

// ConnContext 供 http.Server.ConnContext：在首个请求到达前就把该连接的会话 id
// 注入请求 context，后续 InjectSessionID 读到即 stable=true。
//
// 为什么必须有它：只靠 OnConnState 建表是不够的 —— 表以 net.Conn 为键，
// 而 handler 拿到的是 *http.Request；不经 ConnContext 搭桥，InjectSessionID
// 永远只能拿到现造的 fallback id（stable 恒为 false），会话级 key 轮询在
// 生产上形同虚设（2026-09-26 事故：scheduler 单测全绿，特性实际未生效）。
//
// 创建兜底（不存在即建）使实现不依赖 StateNew 与 ConnContext 的触发顺序。
func (t *SessionTracker) ConnContext(ctx context.Context, conn net.Conn) context.Context {
	if t == nil {
		return ctx
	}
	t.mu.Lock()
	s := t.ensureSessionLocked(conn)
	t.mu.Unlock()
	return context.WithValue(ctx, sessionIDKey, s.ID)
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
// stable=true 意味着该 id 来自连接表（经 http.Server.ConnContext 在首个
// 请求前注入，见 ConnContext）—— 同一长连接上的后续请求拿到同一个 id。
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

// generateFallbackSessionID 为未跟踪连接现造会话 id。必须是真随机：
// 旧实现是 clientIP + 纳秒时间戳的确定性 SHA1 —— Windows 时钟粒度粗时两次
// 调用拿到同一 UnixNano 即撞车；且 string(rune(now)) 把 int64 截成单个码点，
// 熵所剩无几。调用方（InjectSessionID stable=false 路径）依赖"每次全新"。
func generateFallbackSessionID(r *http.Request) string {
	return uuid.New().String()
}
