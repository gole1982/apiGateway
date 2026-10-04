package service

import (
	"errors"
	"testing"

	"gateway/internal/models"
	"gateway/internal/store"
)

// ---------------------------------------------------------------------------
// edgeStore：面板定义写经 Store 接缝标脏；读操作与失败写不标；
// 管理模式 / 离线模式不标（markEdgeDirty 内判定）。
// db.Get() 在单测里为 nil，hook 直接换成记录器，与 settings 解耦。
// ---------------------------------------------------------------------------

// hookStore 只实现被测方法，其余内嵌 nil 接口（Store 接口本身由
// *edgeStore 的编译期断言保证）。
type hookStore struct {
	store.Store
	fail    error
	creates int
	gets    int
}

func (h *hookStore) CreatePlatform(p *models.Platform) error {
	h.creates++
	return h.fail
}

func (h *hookStore) GetPlatforms() ([]models.Platform, error) {
	h.gets++
	return nil, nil
}

func withDirtyHook(t *testing.T, fn func(calls *[]string)) {
	t.Helper()
	oldHook := onEdgeDirty
	oldManage := manageMode.Load()
	var calls []string
	onEdgeDirty = func(reason string) { calls = append(calls, reason) }
	manageMode.Store(false)
	t.Cleanup(func() {
		onEdgeDirty = oldHook
		manageMode.Store(oldManage)
	})
	fn(&calls)
}

// 有中心 + 非管理模式 → 成功写透过底层并标脏（hook 收到原因）。
func TestEdgeStoreMarksDirtyWhenCenterKnown(t *testing.T) {
	withDirtyHook(t, func(calls *[]string) {
		syncMu.Lock()
		syncConfigured = true
		syncMu.Unlock()
		t.Cleanup(func() {
			syncMu.Lock()
			syncConfigured = false
			syncMu.Unlock()
		})
		inner := &hookStore{}
		s := &edgeStore{Store: inner}
		if err := s.CreatePlatform(&models.Platform{}); err != nil {
			t.Fatal(err)
		}
		if inner.creates != 1 {
			t.Fatalf("creates = %d, want 1 (passthrough)", inner.creates)
		}
		if len(*calls) != 1 || (*calls)[0] != "新增平台" {
			t.Errorf("hook calls = %v, want [新增平台]", *calls)
		}
	})
}

// 离线（未配同步、无保存 key）→ 不标，避免离线模式误报。
func TestEdgeStoreOfflineDoesNotMark(t *testing.T) {
	withDirtyHook(t, func(calls *[]string) {
		s := &edgeStore{Store: &hookStore{}}
		if err := s.CreatePlatform(&models.Platform{}); err != nil {
			t.Fatal(err)
		}
		if len(*calls) != 0 {
			t.Errorf("offline mode must not mark dirty, got %v", *calls)
		}
	})
}

// 读操作不标脏（只统计调用，不经过 mark）。
func TestEdgeStoreReadDoesNotMark(t *testing.T) {
	withDirtyHook(t, func(calls *[]string) {
		inner := &hookStore{}
		s := &edgeStore{Store: inner}
		if _, err := s.GetPlatforms(); err != nil {
			t.Fatal(err)
		}
		if inner.gets != 1 {
			t.Fatalf("gets = %d, want 1 (passthrough)", inner.gets)
		}
		if len(*calls) != 0 {
			t.Errorf("read must not mark dirty, got %v", *calls)
		}
	})
}

// 失败写不标脏，且原错误透传。
func TestEdgeStoreFailedWriteDoesNotMark(t *testing.T) {
	withDirtyHook(t, func(calls *[]string) {
		s := &edgeStore{Store: &hookStore{fail: errors.New("boom")}}
		if err := s.CreatePlatform(&models.Platform{}); err == nil {
			t.Fatal("want error passthrough")
		}
		if len(*calls) != 0 {
			t.Errorf("failed write must not mark dirty, got %v", *calls)
		}
	})
}

// 标脏总闸四象限。
func TestShouldMarkDirty(t *testing.T) {
	if shouldMarkDirty(true, true) {
		t.Error("manage mode must not mark (writes go straight to center)")
	}
	if shouldMarkDirty(true, false) {
		t.Error("manage+offline must not mark")
	}
	if shouldMarkDirty(false, false) {
		t.Error("offline must not mark (no center to diverge from)")
	}
	if !shouldMarkDirty(false, true) {
		t.Error("proxy with center must mark")
	}
}

// edgeStore 必须完整实现 store.Store（管理模式替换、测试注入都依赖此断言）。
func TestEdgeStoreImplementsStore(t *testing.T) {
	var _ store.Store = (*edgeStore)(nil)
}

// 版本一致判定：0 版本永假。
func TestVersionsInSync(t *testing.T) {
	if versionsInSync(0, 0) {
		t.Error("version 0 must never count as in-sync")
	}
	if !versionsInSync(7, 7) {
		t.Error("7==7 must be in-sync at version level")
	}
	if versionsInSync(7, 8) {
		t.Error("7!=8 must not be in-sync")
	}
}

// edgeInSync 叠加脏标记：db 为 nil 时 edgeDirtyState 恒干净，
// 故这里只能验证「版本」那一维，脏叠加由 TestShouldMarkDirty + 单测钩子覆盖。
func TestEdgeInSyncCleanDB(t *testing.T) {
	if !edgeInSync(7, 7) {
		t.Error("clean + equal versions must be in-sync")
	}
	if edgeInSync(7, 8) {
		t.Error("different versions must not be in-sync")
	}
}
