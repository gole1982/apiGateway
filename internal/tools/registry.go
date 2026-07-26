package tools

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gateway/internal/db"
)

// Tool represents a registered tool definition.
type Tool struct {
	ID             int64     `json:"id"`
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	Parameters     string    `json:"parameters"`      // JSON Schema (OpenAI function params format)
	ExecutorType   string    `json:"executor_type"`   // http | shell | builtin
	ExecutorConfig string    `json:"executor_config"` // executor-specific JSON config
	Enabled        bool      `json:"enabled"`
	TimeoutMs      int       `json:"timeout_ms"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Registry manages tool definitions with DB persistence and in-memory cache.
type Registry struct {
	mu    sync.RWMutex
	cache map[string]*Tool // name → tool
}

var (
	registryInstance *Registry
	registryOnce     sync.Once
)

// GetRegistry returns the singleton tool registry.
func GetRegistry() *Registry {
	registryOnce.Do(func() {
		registryInstance = &Registry{
			cache: make(map[string]*Tool),
		}
		registryInstance.Reload()
	})
	return registryInstance
}

// Reload refreshes the in-memory cache from the database.
func (r *Registry) Reload() {
	records, err := db.Get().GetAllTools()
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache = make(map[string]*Tool, len(records))
	for i := range records {
		rec := &records[i]
		r.cache[rec.Name] = &Tool{
			ID:             rec.ID,
			Name:           rec.Name,
			Description:    rec.Description,
			Parameters:     rec.Parameters,
			ExecutorType:   rec.ExecutorType,
			ExecutorConfig: rec.ExecutorConfig,
			Enabled:        rec.Enabled,
			TimeoutMs:      rec.TimeoutMs,
		}
	}
}

// Get returns a tool by name (from cache).
func (r *Registry) Get(name string) (*Tool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.cache[name]
	if !ok {
		return nil, fmt.Errorf("tool not found: %s", name)
	}
	return t, nil
}

// ListEnabled returns all enabled tools.
func (r *Registry) ListEnabled() []Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []Tool
	for _, t := range r.cache {
		if t.Enabled {
			result = append(result, *t)
		}
	}
	return result
}

// ToOpenAITools converts named tools to OpenAI function-calling tools format.
// If names is empty, all enabled tools are included.
func (r *Registry) ToOpenAITools(names []string) ([]map[string]interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []map[string]interface{}

	if len(names) == 0 {
		// All enabled tools
		for _, t := range r.cache {
			if !t.Enabled {
				continue
			}
			entry, err := toolToOpenAI(t)
			if err != nil {
				continue
			}
			result = append(result, entry)
		}
		return result, nil
	}

	for _, name := range names {
		t, ok := r.cache[name]
		if !ok {
			return nil, fmt.Errorf("tool not found: %s", name)
		}
		if !t.Enabled {
			return nil, fmt.Errorf("tool is disabled: %s", name)
		}
		entry, err := toolToOpenAI(t)
		if err != nil {
			return nil, fmt.Errorf("tool %s: %w", name, err)
		}
		result = append(result, entry)
	}
	return result, nil
}

// toolToOpenAI converts a Tool to OpenAI tools array element format:
// {"type": "function", "function": {"name": ..., "description": ..., "parameters": ...}}
func toolToOpenAI(t *Tool) (map[string]interface{}, error) {
	var params interface{}
	if t.Parameters != "" && t.Parameters != "{}" {
		if err := json.Unmarshal([]byte(t.Parameters), &params); err != nil {
			return nil, fmt.Errorf("invalid parameters JSON: %w", err)
		}
	} else {
		params = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
	}

	return map[string]interface{}{
		"type": "function",
		"function": map[string]interface{}{
			"name":        t.Name,
			"description": t.Description,
			"parameters":  params,
		},
	}, nil
}
