package notify

import (
	"log"
	"sync"
)

// NotificationService is a simple in-process pub/sub event bus.
// No TCP ports, no external dependencies.
// - Gateway calls Publish() to broadcast events
// - Subscribers (SSE endpoints, in-process handlers) receive them
type NotificationService struct {
	mu          sync.RWMutex
	subscribers map[string]chan Notification
}

type Notification struct {
	Title   string `json:"title"`
	Message string `json:"message"`
}

func NewNotificationService() *NotificationService {
	return &NotificationService{
		subscribers: make(map[string]chan Notification),
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
func (s *NotificationService) Publish(title, message string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := Notification{Title: title, Message: message}
	for _, ch := range s.subscribers {
		select {
		case ch <- n:
		default:
			// drop if subscriber is too slow
		}
	}
	log.Printf("NOTIFY: %s — %s", title, message)
}

// PublishAsync fires a notification in a goroutine (non-blocking).
func (s *NotificationService) PublishAsync(title, message string) {
	go s.Publish(title, message)
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
