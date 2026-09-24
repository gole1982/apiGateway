package db

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"gateway/internal/bundle"
	"gateway/internal/crypto"
)

// TestSyncPullApplyE2E 用本地 HTTP 桩模拟中心 Supabase 的 get_version / get_bundle
// RPC，端到端验证代理读路径：PullVersion → 版本变了 → PullBundle → ApplyBundle →
// 本地 SQLite 落库。center_key 留空 = 中心存明文 token，代理 reencrypt 本地重加密。
func TestSyncPullApplyE2E(t *testing.T) {
	db := setupTestDB(t)

	// 中心要返回的 bundle（明文 token，模拟手动录入里程碑）
	const remoteVersion int64 = 2
	envelopeJSON, err := json.Marshal(map[string]any{
		"schema_version": bundle.SchemaVersion,
		"version":        remoteVersion,
		"bundle": map[string]any{
			"platforms": []map[string]any{{
				"name": "openai", "base_url": "https://api.openai.com",
				"token": "sk-test-plain", "enabled": true, "supported_formats": `["openai"]`,
			}},
			"platform_keys": []map[string]any{{
				"platform_name": "openai", "key_index": 0, "token": "sk-key-plain", "enabled": true,
			}},
			"rapis": []map[string]any{{
				"platform_name": "openai", "alias": "gpt-4", "model": "gpt-4", "enabled": true,
				"key_ids": "0", "supported_formats": `["openai"]`,
			}},
			"lapis": []map[string]any{{"alias": "chat", "enabled": true}},
			"lapi_rapi_order": []map[string]any{{
				"lapi_alias": "chat", "rapi_platform_name": "openai", "rapi_alias": "gpt-4", "order_index": 0,
			}},
		},
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/rest/v1/rpc/get_version":
			_, _ = w.Write([]byte(strconv.FormatInt(remoteVersion, 10)))
		case "/rest/v1/rpc/get_bundle":
			pv, _ := strconv.ParseInt(r.URL.Query().Get("p_version"), 10, 64)
			if pv == remoteVersion {
				_, _ = w.Write([]byte("null")) // 304 等价短路
				return
			}
			_, _ = w.Write(envelopeJSON)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := bundle.NewClient(srv.URL+"/rest/v1/rpc/get_bundle", srv.URL+"/rest/v1/rpc/get_version", "")

	// 1) 轮询版本号
	ver, err := client.PullVersion(context.Background())
	if err != nil {
		t.Fatalf("pull version: %v", err)
	}
	if ver != remoteVersion {
		t.Fatalf("version = %d, want %d", ver, remoteVersion)
	}

	// 2) 版本变了 → 拉全量
	env, err := client.PullBundle(context.Background(), 0)
	if err != nil {
		t.Fatalf("pull bundle: %v", err)
	}
	if env == nil {
		t.Fatal("pull bundle returned nil envelope")
	}
	if env.Version != remoteVersion {
		t.Fatalf("envelope version = %d, want %d", env.Version, remoteVersion)
	}

	// 3) 应用（center_key=nil = 明文模式）
	if err := db.ApplyBundle(env, nil, srv.URL); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// 4) 本地库校验
	var tok string
	if err := db.conn.QueryRow(`SELECT token FROM platform WHERE name=?`, "openai").Scan(&tok); err != nil {
		t.Fatalf("read platform token: %v", err)
	}
	if dec, _ := crypto.Decrypt(tok); dec != "sk-test-plain" {
		t.Fatalf("platform token = %q (decrypted), want sk-test-plain", dec)
	}
	if n := countRows(t, db.conn, `SELECT count(*) FROM rapi`); n != 1 {
		t.Fatalf("rapi count = %d, want 1", n)
	}
	if n := countRows(t, db.conn, `SELECT count(*) FROM lapi_rapi_order`); n != 1 {
		t.Fatalf("lapi_rapi_order count = %d, want 1", n)
	}

	// 5) sync_state 推进到 remoteVersion
	st, _ := db.GetSyncState()
	if st.LastGoodVersion != remoteVersion {
		t.Fatalf("sync_state last_good = %d, want %d", st.LastGoodVersion, remoteVersion)
	}

	// 6) 再拉一次（版本没变）→ ErrNoChange，不重拉
	if _, err := client.PullBundle(context.Background(), remoteVersion); err != bundle.ErrNoChange {
		t.Fatalf("re-pull with same version: got %v, want ErrNoChange", err)
	}
}
