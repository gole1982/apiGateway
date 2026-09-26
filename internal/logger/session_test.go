package logger

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// SessionTracker 按 TCP 连接标识会话：StateNew 铸造 UUID，
// Closed/Hijacked 删除。InjectSessionID 返回 stable=true 当且仅当
// 该 id 来自连接表 —— 调度器靠这个决定游标作用域（见 scheduler）：
// 每请求现造的 fallback id 绝不能当会话身份，否则全部流量钉死在首 key。
// 本文件锁死该判定。

func testTracker() *SessionTracker {
	return NewSessionTracker(nil) // Storage nil：只测内存行为
}

func testConn(t *testing.T, addr string) net.Conn {
	t.Helper()
	c1, c2 := net.Pipe()
	t.Cleanup(func() { c1.Close(); c2.Close() })
	if addr != "" {
		// net.Pipe 的 RemoteAddr 不可定制；用可定制地址的 fake 覆盖测试。
		return &addrConn{Conn: c1, addr: addr}
	}
	return c1
}

type addrConn struct {
	net.Conn
	addr string
}

func (c *addrConn) RemoteAddr() net.Addr {
	if h, _, err := net.SplitHostPort(c.addr); err == nil {
		if ip := net.ParseIP(h); ip != nil {
			return &net.TCPAddr{IP: ip}
		}
	}
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}
}

func newReqWithConn(t *testing.T, conn net.Conn) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.RemoteAddr = conn.RemoteAddr().String()
	return r
}

// 真实链路：server 先调 ConnContext（首个请求前）注入会话 id，
// handler 再调 InjectSessionID —— 两次拿到同一个 id，且 stable=true。
func TestInjectSessionIDStableOnKnownConn(t *testing.T) {
	tr := testTracker()
	conn := testConn(t, "10.0.0.9:54321")
	tr.OnConnState(conn, http.StateNew)

	r1 := newReqWithConn(t, conn)
	r1 = r1.WithContext(tr.ConnContext(r1.Context(), conn))
	id1, stable1 := tr.InjectSessionID(r1)
	r2 := newReqWithConn(t, conn)
	r2 = r2.WithContext(tr.ConnContext(r2.Context(), conn))
	id2, stable2 := tr.InjectSessionID(r2)

	if !stable1 || !stable2 {
		t.Errorf("stable = %v/%v, want true/true for a tracked connection", stable1, stable2)
	}
	if id1 == "" || id1 != id2 {
		t.Errorf("ids = %q/%q, want equal non-empty for one connection", id1, id2)
	}
	// 同一连接上 TotalRequests 累加（best-effort 计数）。
	tr.mu.RLock()
	var total int
	for _, s := range tr.sessions {
		if s.ID == id1 {
			total = s.TotalRequests
		}
	}
	tr.mu.RUnlock()
	if total != 2 {
		t.Errorf("TotalRequests = %d, want 2", total)
	}
}

// 连接关闭后会话摘除，再注入即 fallback。
func TestSessionRemovedOnConnClose(t *testing.T) {
	tr := testTracker()
	conn := testConn(t, "10.0.0.9:54322")
	tr.OnConnState(conn, http.StateNew)
	r := newReqWithConn(t, conn)
	id, _ := tr.InjectSessionID(r)

	tr.OnConnState(conn, http.StateClosed)
	tr.mu.RLock()
	n := len(tr.sessions)
	tr.mu.RUnlock()
	if n != 0 {
		t.Errorf("sessions = %d after close, want 0", n)
	}

	// 同一连接对象已不在表里：再次注入走 fallback，stable=false。
	r2 := newReqWithConn(t, conn)
	id2, stable := tr.InjectSessionID(r2)
	if stable {
		t.Error("stable = true after the session was removed, want false")
	}
	if id2 == id {
		t.Errorf("fallback id %q equals the removed session id; fallback must be fresh", id2)
	}
}

// 未跟踪的连接（短连接/直连）：每次注入都是全新 id，stable=false。
// 这是"退回平台级游标"决策的前提 —— 若 fallback 可复用，会话级轮询
// 会把全部流量钉在排序首位。
func TestInjectSessionIDUnstableOffTable(t *testing.T) {
	tr := testTracker()
	conn := testConn(t, "10.0.0.9:54323") // 故意不调 OnConnState

	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		r := newReqWithConn(t, conn)
		id, stable := tr.InjectSessionID(r)
		if stable {
			t.Fatalf("request %d: stable = true for an untracked connection", i)
		}
		if id == "" {
			t.Fatalf("request %d: empty fallback id", i)
		}
		if seen[id] {
			t.Fatalf("fallback id %q repeated; must be unique per request", id)
		}
		seen[id] = true
	}
}

// GetSessionID：上下文里有就直接用（网关已注入的路径）。
func TestGetSessionIDFromContext(t *testing.T) {
	tr := testTracker()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r2 := r.WithContext(context.WithValue(r.Context(), sessionIDKey, "fixed-id"))
	_ = r
	if got := tr.GetSessionID(r2); got != "fixed-id" {
		t.Errorf("GetSessionID = %q, want fixed-id", got)
	}
}

// ConnContext：已知连接注入其会话 id；未知连接先建表再注入
// （不依赖 StateNew 与 ConnContext 的触发顺序）；nil tracker 透传。
func TestConnContext(t *testing.T) {
	tr := testTracker()
	conn := testConn(t, "10.0.0.9:54330")

	// 未调 OnConnState：建表兜底。
	r := newReqWithConn(t, conn)
	ctx := tr.ConnContext(r.Context(), conn)
	id, ok := ctx.Value(sessionIDKey).(string)
	if !ok || id == "" {
		t.Fatalf("ConnContext did not inject a session id")
	}
	tr.mu.RLock()
	_, inTable := tr.sessions[conn]
	tr.mu.RUnlock()
	if !inTable {
		t.Error("ConnContext did not create the missing session")
	}

	// 同一连接再次调用：同一 id（幂等）。
	ctx2 := tr.ConnContext(r.Context(), conn)
	if id2, _ := ctx2.Value(sessionIDKey).(string); id2 != id {
		t.Errorf("second ConnContext = %q, want %q", id2, id)
	}

	// 注入后的请求走 InjectSessionID 即 stable。
	r3 := newReqWithConn(t, conn).WithContext(ctx)
	if got, stable := tr.InjectSessionID(r3); !stable || got != id {
		t.Errorf("InjectSessionID after ConnContext = (%q,%v), want (%q,true)", got, stable, id)
	}

	var nilTracker *SessionTracker
	if out := nilTracker.ConnContext(r.Context(), conn); out != r.Context() {
		t.Error("nil tracker must pass the context through unchanged")
	}
}

// parseConnAddr：TCP 地址拆 IP/端口；非 TCP 原样返回。
func TestParseConnAddr(t *testing.T) {
	ip, port := parseConnAddr(&net.TCPAddr{IP: net.ParseIP("192.168.1.7"), Port: 8080})
	if ip != "192.168.1.7" || port != 8080 {
		t.Errorf("got %s/%d, want 192.168.1.7/8080", ip, port)
	}
}

func TestFallbackIDsLookLikeUUIDs(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.RemoteAddr = "1.2.3.4:5678"
	a := generateFallbackSessionID(r)
	b := generateFallbackSessionID(r)
	if a == b {
		t.Error("two fallbacks in the same nanosecond window collided; expected unique")
	}
	// UUID 文本形态：36 字符，4 横杠。
	if len(a) != 36 || strings.Count(a, "-") != 4 {
		t.Errorf("fallback %q does not look like a UUID", a)
	}
}
