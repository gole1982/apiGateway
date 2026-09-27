package scheduler

import (
	"fmt"
	"testing"
	"time"

	"gateway/internal/models"
)

// mkKeys 造 n 个同平台 key，KeyIndex = 0..n-1（轮询顺序）。
func mkKeys(pid int64, n int) []models.PlatformKey {
	out := make([]models.PlatformKey, n)
	for i := range out {
		out[i] = models.PlatformKey{
			ID:         pid*100 + int64(i) + 1,
			PlatformID: pid,
			KeyIndex:   i,
			Token:      fmt.Sprintf("sk-%d-%d", pid, i),
			Enabled:    true,
		}
	}
	return out
}

// 同一会话的连续两轮必须落在不同 key 上 —— 这是会话级游标存在的全部理由。
// 平台级游标做不到这点：同平台别的流量可以把游标推进任意步数，使本会话的
// 下一轮又落回同一个 key。
func TestPickAvailableKeySessionScopeNeverRepeatsWithinSession(t *testing.T) {
	m := NewManager(DefaultConfig())
	defer m.Close()
	keys := mkKeys(1, 3)

	seen := map[int64]int{}
	var order []int64
	for i := 0; i < 9; i++ { // 走满池 3 轮
		k, _, err := m.PickAvailableKey(keys, "sess-A")
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		order = append(order, k.ID)
		seen[k.ID]++
	}
	// 每个 key 恰好 3 次，且严格不重复相邻
	for id, n := range seen {
		if n != 3 {
			t.Errorf("key %d used %d times, want 3 (even rotation)", id, n)
		}
	}
	for i := 1; i < len(order); i++ {
		if order[i] == order[i-1] {
			t.Errorf("consecutive turns reused key %d at i=%d: %v", order[i], i, order)
		}
	}
	t.Logf("session rotation order: %v", order)
}

// 用户的目标：会话级轮询把池耗尽墙钟时间从 len(pool)*Ts*nSessions
// 压到 len(pool)*Ts。3 key × 2 会话 → 2 倍提速。这里用"两个会话各自
// 独立走完池"来刻画：第二次会话扫描不必等第一会话把共享游标绕完。
func TestPickAvailableKeySessionScopeSessionsAdvanceIndependently(t *testing.T) {
	m := NewManager(DefaultConfig())
	defer m.Close()
	keys := mkKeys(1, 3)

	first := map[int64]bool{}
	for i := 0; i < 3; i++ {
		k, _, err := m.PickAvailableKey(keys, "sess-1")
		if err != nil {
			t.Fatalf("sess-1 pick %d: %v", i, err)
		}
		first[k.ID] = true
	}
	// 第二个会话从池头开始，不受第一个会话已经走过 3 步的影响
	for i := 0; i < 3; i++ {
		k, _, err := m.PickAvailableKey(keys, "sess-2")
		if err != nil {
			t.Fatalf("sess-2 pick %d: %v", i, err)
		}
		if !first[k.ID] {
			t.Errorf("sess-2 picked key %d outside pool head window %v", k.ID, first)
		}
	}
}

// 每请求一次性的 session id（无长连接的客户端）绝不能被当成会话身份，
// 否则每张游标都从 0 起算 → 全部流量钉死在 sorted[0]。
// gateway 侧靠 InjectSessionID 的 stable=false 过滤掉；这里守 scheduler
// 侧的兜底：空串必须退回平台级游标。
func TestPickAvailableKeyEmptySessionFallsBackToPlatformCursor(t *testing.T) {
	m := NewManager(DefaultConfig())
	defer m.Close()
	keys := mkKeys(1, 3)

	// 模拟两个"每请求一个新 id"的客户端：各自只请求一次。
	// 若按 id 建游标，两者都会拿到 sorted[0]。
	k1, _, err := m.PickAvailableKey(keys, "")
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	k2, _, err := m.PickAvailableKey(keys, "")
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	if k1.ID == k2.ID {
		t.Errorf("platform-scope fallback pinned both requests to key %d", k1.ID)
	}
}

// 显式 platform 模式：即使传入稳定 session id，也必须走平台级游标
// （保留旧行为 / 关闭开关的路径）。
func TestPickAvailableKeyPlatformScopeIgnoresSession(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeyCursorScope = CursorScopePlatform
	m := NewManager(cfg)
	defer m.Close()
	keys := mkKeys(1, 3)

	var order []int64
	for i := 0; i < 6; i++ {
		k, _, err := m.PickAvailableKey(keys, fmt.Sprintf("sess-%d", i))
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		order = append(order, k.ID)
	}
	// 6 次调用、3 个 key 的平台级轮询 → 每个 key 恰好 2 次
	counts := map[int64]int{}
	for _, id := range order {
		counts[id]++
	}
	for id, n := range counts {
		if n != 2 {
			t.Errorf("key %d used %d times, want 2 (platform rotation): %v", id, n, order)
		}
	}
}

// 不同平台的会话游标互不干扰。
func TestPickAvailableKeySessionScopePerPlatformIndependent(t *testing.T) {
	m := NewManager(DefaultConfig())
	defer m.Close()
	pa, pb := mkKeys(1, 3), mkKeys(2, 3)

	for i := 0; i < 3; i++ {
		if _, _, err := m.PickAvailableKey(pa, "s"); err != nil {
			t.Fatalf("pa pick %d: %v", i, err)
		}
	}
	k, _, err := m.PickAvailableKey(pb, "s")
	if err != nil {
		t.Fatalf("pb pick: %v", err)
	}
	if k.PlatformID != 2 || k.KeyIndex != 0 {
		t.Errorf("pb first pick = platform %d idx %d, want 2/0 (own cursor)", k.PlatformID, k.KeyIndex)
	}
}

// 会话游标会随会话 churn 增长，必须被淘汰；平台级游标常驻。
func TestSessionCursorsEvictedButPlatformCursorKept(t *testing.T) {
	m := NewManager(DefaultConfig())
	defer m.Close()
	keys := mkKeys(1, 2)

	// 先建一个平台级游标（空 session），它必须常驻、不被淘汰
	if _, _, err := m.PickAvailableKey(keys, ""); err != nil {
		t.Fatalf("platform pick: %v", err)
	}
	for i := 0; i < cursorSweepThreshold+50; i++ {
		if _, _, err := m.PickAvailableKey(keys, fmt.Sprintf("churn-%d", i)); err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
	}
	m.mu.Lock()
	n := len(m.keyCursors)
	m.mu.Unlock()
	if n > cursorSweepThreshold+50 {
		t.Errorf("cursor map grew to %d without eviction", n)
	}

	// 把所有会话游标的时间戳推到 TTL 之前，触发淘汰
	m.mu.Lock()
	old := time.Now().Add(-2 * sessionCursorTTL)
	for k, c := range m.keyCursors {
		if k.sessionID != "" {
			c.lastUsed = old
		}
	}
	// 平台级游标必须不受影响
	_, hadPlatform := m.keyCursors[cursorKey{platformID: 1}]
	m.mu.Unlock()
	if !hadPlatform {
		t.Fatal("expected a platform-scoped cursor to exist")
	}

	// 新建一个游标即触发清扫
	if _, _, err := m.PickAvailableKey(keys, "fresh"); err != nil {
		t.Fatalf("fresh pick: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.keyCursors {
		if k.sessionID != "fresh" && k.sessionID != "" {
			t.Errorf("stale session cursor %q survived eviction", k.sessionID)
		}
	}
	if _, ok := m.keyCursors[cursorKey{platformID: 1}]; !ok {
		t.Error("platform cursor was evicted; it must persist")
	}
}

// TTL 压不住时（一个窗口内海量不同会话）按最久未使用淘汰到硬上限。
func TestSessionCursorsCappedUnderHardLimit(t *testing.T) {
	m := NewManager(DefaultConfig())
	defer m.Close()
	keys := mkKeys(1, 2)

	m.mu.Lock()
	// 直接灌入超过硬上限的"全部刚用过"的会话游标，TTL 阶段清不掉任何东西
	now := time.Now()
	for i := 0; i < maxSessionCursors+500; i++ {
		m.keyCursors[cursorKey{platformID: 1, sessionID: fmt.Sprintf("s%d", i)}] =
			&keyCursor{n: uint64(i), lastUsed: now}
	}
	m.mu.Unlock()

	if _, _, err := m.PickAvailableKey(keys, "trigger"); err != nil {
		t.Fatalf("pick: %v", err)
	}
	m.mu.Lock()
	n := len(m.keyCursors)
	m.mu.Unlock()
	if n > maxSessionCursors {
		t.Errorf("cursor map = %d, want <= %d", n, maxSessionCursors)
	}
}
