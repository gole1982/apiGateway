package notify

import (
	"log/slog"
	"sort"
	"sync"
	"time"
)

// NotificationService is a simple in-process pub/sub event bus.
// No TCP ports, no external dependencies.
// - Gateway calls Publish() to broadcast events
// - Subscribers (SSE endpoints, in-process handlers) receive them
type NotificationService struct {
	mu          sync.RWMutex
	subscribers map[string]chan Notification

	// In-memory unread feed. seq is a global monotonic id; readWatermark maps a
	// menu to the highest item id the user has marked read. An item is unread
	// when item.ID > readWatermark[item.Menu].
	seq           int64
	unread        []UnreadItem
	readWatermark map[string]int64
}

type Notification struct {
	Title   string `json:"title"`
	Message string `json:"message"`
	// Level classifies severity for client-side routing:
	// "info" (logged only), "warning", or "error" (surfaced in the toast box).
	Level string `json:"level,omitempty"`
}

// Unread menus — navigation targets the dashboard routes these to.
const (
	MenuPlatforms  = "platforms"
	MenuKeys       = "keys"
	MenuModels     = "models"
	MenuInterfaces = "interfaces"
)

// UnreadItem is one in-memory unread event, routed to a UI menu. Not persisted:
// unread state is intentionally transient (lost on restart) to keep maintenance
// cost low, as agreed.
type UnreadItem struct {
	ID        int64     `json:"id"`
	Menu      string    `json:"menu"`
	Kind      string    `json:"kind"` // create | update | delete | failure
	EntityID  int64     `json:"entity_id,omitempty"`
	Title     string    `json:"title"`
	Detail    string    `json:"detail,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// maxUnreadItems caps the in-memory feed so a runaway stream of events can not
// grow the slice unboundedly.
const maxUnreadItems = 200

func NewNotificationService() *NotificationService {
	return &NotificationService{
		subscribers:   make(map[string]chan Notification),
		readWatermark: make(map[string]int64),
	}
}

// Subscribe returns a channel that receives notifications.
// Caller must call Unsubscribe when done.
func (s *NotificationService) Subscribe() (string, <-chan Notification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := nextID()
	ch := make(chan Notification, 10)
	s.subscribers[id] = ch
	return id, ch
}

// Unsubscribe removes a subscriber by ID.
func (s *NotificationService) Unsubscribe(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.subscribers[id]; ok {
		close(ch)
		delete(s.subscribers, id)
	}
}

// Publish sends a notification to all subscribers.
func (s *NotificationService) Publish(level, title, message string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := Notification{Title: title, Message: message, Level: level}
	for _, ch := range s.subscribers {
		select {
		case ch <- n:
		default:
			// drop if subscriber is too slow
		}
	}
	switch level {
	case "error":
		slog.Error("NOTIFY: "+title+" — "+message, "component", "notify", "title", title, "message", message)
	case "warning":
		slog.Warn("NOTIFY: "+title+" — "+message, "component", "notify", "title", title, "message", message)
	default:
		slog.Info("NOTIFY: "+title+" — "+message, "component", "notify", "title", title, "message", message)
	}
}

// PublishAsync fires a notification in a goroutine (non-blocking).
func (s *NotificationService) PublishAsync(level, title, message string) {
	go s.Publish(level, title, message)
}

// RecordUnread appends an in-memory unread event routed to a menu, without
// broadcasting a toast (used for entity create/update/delete events).
func (s *NotificationService) RecordUnread(menu, kind, title, detail string, entityID int64) {
	if menu == "" {
		menu = MenuPlatforms
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	s.unread = append(s.unread, UnreadItem{
		ID:        s.seq,
		Menu:      menu,
		Kind:      kind,
		EntityID:  entityID,
		Title:     title,
		Detail:    detail,
		CreatedAt: time.Now(),
	})
	if len(s.unread) > maxUnreadItems {
		s.unread = s.unread[len(s.unread)-maxUnreadItems:]
	}
}

// PublishEvent records an unread item AND broadcasts a toast to subscribers.
// Used for runtime failures (key/model gone bad) so the operator gets both the
// live toast and a persistent-in-session unread badge.
func (s *NotificationService) PublishEvent(level, menu, kind, title, detail string, entityID int64) {
	s.RecordUnread(menu, kind, title, detail, entityID)
	s.Publish(level, title, detail)
}

// UnreadItems returns every unread item across menus, newest first.
func (s *NotificationService) UnreadItems() []UnreadItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]UnreadItem, 0, len(s.unread))
	for _, it := range s.unread {
		if it.ID > s.readWatermark[it.Menu] {
			out = append(out, it)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	return out
}

// UnreadCounts returns the unread count per menu (menus with zero are omitted).
func (s *NotificationService) UnreadCounts() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := make(map[string]int)
	for _, it := range s.unread {
		if it.ID > s.readWatermark[it.Menu] {
			m[it.Menu]++
		}
	}
	return m
}

// TotalUnread returns the total unread count across all menus.
func (s *NotificationService) TotalUnread() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, it := range s.unread {
		if it.ID > s.readWatermark[it.Menu] {
			n++
		}
	}
	return n
}

// MarkMenuRead marks every unread item in a menu as read.
func (s *NotificationService) MarkMenuRead(menu string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readWatermark == nil {
		s.readWatermark = make(map[string]int64)
	}
	s.readWatermark[menu] = s.seq
}

// MarkAllRead marks every unread item in every menu as read.
func (s *NotificationService) MarkAllRead() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readWatermark == nil {
		s.readWatermark = make(map[string]int64)
	}
	for _, it := range s.unread {
		s.readWatermark[it.Menu] = s.seq
	}
}

var idCounter int

func nextID() string {
	idCounter++
	return "sub-" + itoa(idCounter)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
