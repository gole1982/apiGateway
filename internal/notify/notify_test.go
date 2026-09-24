package notify

import "testing"

// 同状态合并：同一 (menu,kind,entityID) 重复上报应合并为一条，计数递增、
// 保留首现时刻、取最新 detail，且复发后 ID 更新（已读后重新浮出水面）。
func TestRecordUnreadMergesSameState(t *testing.T) {
	s := NewNotificationService()
	s.RecordUnread(MenuKeys, "cooling", "Key #3 冷却中", "429 rate limit", 42)
	s.RecordUnread(MenuKeys, "cooling", "Key #3 冷却中", "429 rate limit", 42)
	s.RecordUnread(MenuKeys, "cooling", "Key #3 冷却中", "quota exhausted", 42)

	items := s.UnreadItems()
	if len(items) != 1 {
		t.Fatalf("merged items = %d, want 1", len(items))
	}
	it := items[0]
	if it.Count != 3 {
		t.Errorf("Count = %d, want 3", it.Count)
	}
	if it.Detail != "quota exhausted" {
		t.Errorf("Detail = %q, want latest %q", it.Detail, "quota exhausted")
	}
	if it.FirstAt.IsZero() || !it.FirstAt.Before(it.CreatedAt) && !it.FirstAt.Equal(it.CreatedAt) {
		t.Errorf("FirstAt should be <= CreatedAt, got %v vs %v", it.FirstAt, it.CreatedAt)
	}

	// 已读后复发 → 重新成为未读（新 ID 超过水位线）。
	s.MarkMenuRead(MenuKeys)
	if n := s.TotalUnread(); n != 0 {
		t.Fatalf("unread after mark read = %d, want 0", n)
	}
	s.RecordUnread(MenuKeys, "cooling", "Key #3 冷却中", "429 again", 42)
	if n := s.TotalUnread(); n != 1 {
		t.Fatalf("unread after recurrence = %d, want 1", n)
	}
	// 计数延续（旧条目只是被水位线隐藏、仍在 feed 里）：第 4 次冷却。
	if got := s.UnreadItems()[0].Count; got != 4 {
		t.Errorf("Count after recurrence = %d, want 4（复发延续累计计数）", got)
	}
}

// 不同实体/不同 kind 不合并。
func TestRecordUnreadNoMergeDistinct(t *testing.T) {
	s := NewNotificationService()
	s.RecordUnread(MenuKeys, "cooling", "Key #3 冷却中", "", 42)
	s.RecordUnread(MenuKeys, "cooling", "Key #4 冷却中", "", 43)
	s.RecordUnread(MenuKeys, "failure", "Key #3 永久失败", "", 42)
	if n := len(s.UnreadItems()); n != 3 {
		t.Fatalf("items = %d, want 3 (distinct entity/kind not merged)", n)
	}
}

// 单条已读：只移除该条，不影响同菜单其他条目。
func TestMarkItemRead(t *testing.T) {
	s := NewNotificationService()
	s.RecordUnread(MenuKeys, "cooling", "Key #3 冷却中", "", 42)
	s.RecordUnread(MenuKeys, "cooling", "Key #4 冷却中", "", 43)
	items := s.UnreadItems()
	s.MarkItemRead(items[0].ID)
	rest := s.UnreadItems()
	if len(rest) != 1 || rest[0].ID == items[0].ID {
		t.Fatalf("after MarkItemRead: %+v, want the other item only", rest)
	}
	if n := s.TotalUnread(); n != 1 {
		t.Fatalf("total = %d, want 1", n)
	}
}
