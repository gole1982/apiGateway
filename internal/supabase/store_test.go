package supabase

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 中心 platform 表按设计不含 available 列（健康态是代理本地 runtime 状态）。
// 回归：GetPlatforms/GetPlatformByID 必须把 Available 缺省置 true，否则
// 管理模式下面板会把所有启用平台误显为「失效」（2026-09-23 事故）。
func TestPlatformsDefaultAvailableWhenColumnAbsent(t *testing.T) {
	// 模拟 PostgREST：行里刻意没有 "available" 键（与线上中心表一致）。
	rows := []map[string]any{
		{"id": 1, "name": "P1", "base_url": "https://a.example/v1", "enabled": true, "token": ""},
		{"id": 2, "name": "P2", "base_url": "https://b.example/v1", "enabled": false, "token": ""},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/platform") {
			http.Error(w, `{"error":"unexpected table"}`, 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.RawQuery, "id=eq.2"):
			_ = json.NewEncoder(w).Encode(rows[1:2])
		case strings.Contains(r.URL.RawQuery, "id=eq.999"):
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			_ = json.NewEncoder(w).Encode(rows)
		}
	}))
	defer srv.Close()

	s, err := New(Config{URL: srv.URL, ServiceKey: "test-key"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	plats, err := s.GetPlatforms()
	if err != nil {
		t.Fatalf("GetPlatforms: %v", err)
	}
	if len(plats) != 2 {
		t.Fatalf("got %d platforms, want 2", len(plats))
	}
	for _, p := range plats {
		if !p.Available {
			t.Errorf("platform %q: Available=false, want true (center has no health column)", p.Name)
		}
	}
	if !plats[0].Enabled || plats[1].Enabled {
		t.Errorf("enabled mapping wrong: got %v/%v, want true/false", plats[0].Enabled, plats[1].Enabled)
	}

	p, err := s.GetPlatformByID(2)
	if err != nil {
		t.Fatalf("GetPlatformByID: %v", err)
	}
	if p == nil {
		t.Fatal("GetPlatformByID returned nil")
	}
	if !p.Available {
		t.Error("GetPlatformByID: Available=false, want true")
	}

	// 不存在的 id → (nil, nil)，与本地 ErrNoRows 语义一致。
	missing, err := s.GetPlatformByID(999)
	if err != nil {
		t.Fatalf("GetPlatformByID(999): %v", err)
	}
	if missing != nil {
		t.Errorf("GetPlatformByID(999) = %+v, want nil", missing)
	}
}
