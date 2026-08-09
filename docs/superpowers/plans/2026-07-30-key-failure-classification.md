# Key 失败分类与归因下沉 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将平台 key 失败状态从散落在 platform.available / rapi.available 下沉到 platform_keys 表，增加临时/永久分类标签与探测恢复端点，并修复 Google "No backends available" bug。

**Architecture:** 在 `platform_keys` 表新增 `failure_type`/`failure_reason`/`failed_at` 三列，网关 401/402/403 路径改为标记 key 而非禁用 key 或标记 RAPI，调度器按 `failure_type=2` 跳过 key，新增 `POST /api/platforms/{id}/keys/{keyId}/probe` 端点供 UI 重置。同时从 `GetEnabledRAPIsForLAPI` 查询去掉 `p.available = 1` 条件解耦平台可达性与 RAPI 选择。

**Tech Stack:** Go 1.x, SQLite (modernc.org/sqlite), Vue 3 (CDN), HTML/CSS/JS

**Spec:** [docs/superpowers/specs/2026-07-30-key-failure-classification-design.md](../specs/2026-07-30-key-failure-classification-design.md)

---

## 文件结构

| 文件 | 责任 | 操作 |
|------|------|------|
| `internal/models/models.go` | PlatformKey 结构体定义 | 修改：加 3 个字段 |
| `internal/db/db.go` | DB schema + 查询 + 新方法 | 修改：迁移、GetPlatformKeys、3 个新方法 |
| `internal/db/db_test.go` | DB 测试 + 测试 schema | 修改：测试 schema 加列、新增 4 个测试 |
| `internal/scheduler/scheduler.go` | 调度器 key 过滤 | 修改：PickAvailableKey 加过滤 |
| `internal/scheduler/scheduler_test.go` | 调度器测试 | 修改：新增 2 个测试 |
| `internal/gateway/gateway.go` | 失败归因处理 | 修改：L593-661 重写 |
| `internal/service/service.go` | restore 简化 + probe 端点 | 修改：L2203-2220 简化、新增 probe handler |
| `internal/service/dashboard.html` | UI 密钥管理弹窗 | 修改：L2658-2680 加徽标 + 按钮 |

---

## Task 1: 数据模型 — 迁移 + 结构体

**Files:**
- Modify: `internal/models/models.go:12-26`
- Modify: `internal/db/db.go:84-100` (CREATE TABLE)
- Modify: `internal/db/db.go:2407-2414` (迁移函数)
- Modify: `internal/db/db.go:245` (注册迁移)
- Modify: `internal/db/db_test.go:110-124` (测试 schema)

- [ ] **Step 1: 更新 `models.PlatformKey` 结构体**

在 `internal/models/models.go` 的 `PlatformKey` 结构体末尾（`UpdatedAt` 字段后）增加 3 个字段：

```go
type PlatformKey struct {
	ID         int64  `json:"id"`
	PlatformID int64  `json:"platform_id"`
	KeyIndex   int    `json:"key_index"`
	Token      string `json:"token"`
	Label      string `json:"label,omitempty"`
	Enabled    bool   `json:"enabled"`
	SessionHeaders string    `json:"session_headers,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	// FailureType classifies the last observed failure: 0=none, 1=temporary(429/5xx), 2=permanent(401/402/403).
	// Set by the gateway on upstream errors; cleared on success or manual probe reset.
	FailureType   int        `json:"failure_type"`
	FailureReason string     `json:"failure_reason,omitempty"`
	FailedAt      *time.Time `json:"failed_at,omitempty"`
}
```

- [ ] **Step 2: 更新 CREATE TABLE 语句**

在 `internal/db/db.go` L84-100 的 `platform_keys` CREATE TABLE 中，在 `reusable_reasons` 行后、`created_at` 行前加入 3 列：

```sql
CREATE TABLE IF NOT EXISTS platform_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform_id INTEGER NOT NULL,
    key_index INTEGER NOT NULL DEFAULT 0,
    token TEXT NOT NULL DEFAULT '',
    label TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    session_headers TEXT NOT NULL DEFAULT '',
    reusable_status INTEGER NOT NULL DEFAULT -1,
    reusable_reasons TEXT NOT NULL DEFAULT '[]',
    failure_type INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT NOT NULL DEFAULT '',
    failed_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
    UNIQUE(platform_id, key_index)
);
```

- [ ] **Step 3: 新增迁移函数**

在 `internal/db/db.go` 的 `migrateAddPlatformKeyReusabilityColumns` 函数（L2407-2414）后面新增：

```go
func (db *DB) migrateAddPlatformKeyFailureColumns() {
	var count int
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('platform_keys') WHERE name='failure_type'`)
	if row.Scan(&count) == nil && count == 0 {
		db.conn.Exec(`ALTER TABLE platform_keys ADD COLUMN failure_type INTEGER NOT NULL DEFAULT 0`)
		db.conn.Exec(`ALTER TABLE platform_keys ADD COLUMN failure_reason TEXT NOT NULL DEFAULT ''`)
		db.conn.Exec(`ALTER TABLE platform_keys ADD COLUMN failed_at DATETIME`)
	}
}
```

- [ ] **Step 4: 注册迁移**

在 `internal/db/db.go` L245（`migrateAddPlatformKeyReusabilityColumns()` 调用后）增加一行：

```go
	instance.migrateAddPlatformKeyReusabilityColumns()
	instance.migrateAddPlatformKeyFailureColumns()   // <-- 新增
```

- [ ] **Step 5: 更新测试 schema**

在 `internal/db/db_test.go` L110-124 的测试 `platform_keys` CREATE TABLE 中，同步加入 3 列（位置与 db.go 一致）：

```go
CREATE TABLE IF NOT EXISTS platform_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform_id INTEGER NOT NULL,
    key_index INTEGER NOT NULL DEFAULT 0,
    token TEXT NOT NULL DEFAULT '',
    label TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    session_headers TEXT NOT NULL DEFAULT '',
    reusable_status INTEGER NOT NULL DEFAULT -1,
    reusable_reasons TEXT NOT NULL DEFAULT '[]',
    failure_type INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT NOT NULL DEFAULT '',
    failed_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (platform_id) REFERENCES platform(id) ON DELETE CASCADE,
    UNIQUE(platform_id, key_index)
);
```

- [ ] **Step 6: 验证编译**

Run: `go build ./...`
Expected: 编译通过（无错误）

- [ ] **Step 7: 提交**

```bash
git add internal/models/models.go internal/db/db.go internal/db/db_test.go
git commit -m "feat(db): add failure_type/reason/failed_at columns to platform_keys"
```

---

## Task 2: DB 方法 + 查询更新

**Files:**
- Modify: `internal/db/db.go:1771-1809` (GetPlatformKeys 查询)
- Modify: `internal/db/db.go:1911-1919` (新增方法插入点)
- Modify: `internal/db/db_test.go` (新增测试)

- [ ] **Step 1: 写失败测试 — TestMarkKeyPermanentFailure**

在 `internal/db/db_test.go` 的 `TestPlatformKeyCRUD` 函数后新增：

```go
func TestMarkKeyPermanentFailure(t *testing.T) {
	db := setupTestDB(t)
	p := &models.Platform{Name: "test", BaseURL: "https://api.test.com/v1", Token: "sk-orig"}
	db.CreatePlatform(p)
	k := &models.PlatformKey{PlatformID: p.ID, Token: "key-a", Label: "primary", Enabled: true}
	if err := db.AddPlatformKey(k); err != nil {
		t.Fatalf("AddPlatformKey: %v", err)
	}

	if err := db.MarkKeyPermanentFailure(k.ID, "[认证失败] upstream 401"); err != nil {
		t.Fatalf("MarkKeyPermanentFailure: %v", err)
	}

	keys, _ := db.GetPlatformKeys(p.ID)
	if len(keys) != 1 {
		t.Fatalf("GetPlatformKeys returned %d, want 1", len(keys))
	}
	if keys[0].FailureType != 2 {
		t.Errorf("FailureType = %d, want 2 (permanent)", keys[0].FailureType)
	}
	if keys[0].FailureReason != "[认证失败] upstream 401" {
		t.Errorf("FailureReason = %q, want '[认证失败] upstream 401'", keys[0].FailureReason)
	}
	if keys[0].FailedAt == nil {
		t.Errorf("FailedAt should be non-nil after permanent failure")
	}
	// enabled should NOT be changed (decoupled from failure tracking)
	if !keys[0].Enabled {
		t.Errorf("Enabled = false, want true (failure tracking decoupled from enabled)")
	}
}
```

- [ ] **Step 2: 写失败测试 — TestMarkKeyTemporaryFailure**

紧接上文新增：

```go
func TestMarkKeyTemporaryFailure(t *testing.T) {
	db := setupTestDB(t)
	p := &models.Platform{Name: "test", BaseURL: "https://api.test.com/v1", Token: "sk-orig"}
	db.CreatePlatform(p)
	k := &models.PlatformKey{PlatformID: p.ID, Token: "key-a", Enabled: true}
	db.AddPlatformKey(k)

	if err := db.MarkKeyTemporaryFailure(k.ID, "upstream 429: rate limited"); err != nil {
		t.Fatalf("MarkKeyTemporaryFailure: %v", err)
	}

	keys, _ := db.GetPlatformKeys(p.ID)
	if keys[0].FailureType != 1 {
		t.Errorf("FailureType = %d, want 1 (temporary)", keys[0].FailureType)
	}
	if keys[0].FailureReason != "upstream 429: rate limited" {
		t.Errorf("FailureReason = %q, want 'upstream 429: rate limited'", keys[0].FailureReason)
	}
	if keys[0].FailedAt == nil {
		t.Errorf("FailedAt should be non-nil after temporary failure")
	}
}
```

- [ ] **Step 3: 写失败测试 — TestClearKeyFailure**

紧接上文新增：

```go
func TestClearKeyFailure(t *testing.T) {
	db := setupTestDB(t)
	p := &models.Platform{Name: "test", BaseURL: "https://api.test.com/v1", Token: "sk-orig"}
	db.CreatePlatform(p)
	k := &models.PlatformKey{PlatformID: p.ID, Token: "key-a", Enabled: true}
	db.AddPlatformKey(k)

	db.MarkKeyPermanentFailure(k.ID, "test reason")
	if err := db.ClearKeyFailure(k.ID); err != nil {
		t.Fatalf("ClearKeyFailure: %v", err)
	}

	keys, _ := db.GetPlatformKeys(p.ID)
	if keys[0].FailureType != 0 {
		t.Errorf("FailureType = %d, want 0 (none)", keys[0].FailureType)
	}
	if keys[0].FailureReason != "" {
		t.Errorf("FailureReason = %q, want ''", keys[0].FailureReason)
	}
	if keys[0].FailedAt != nil {
		t.Errorf("FailedAt should be nil after clear")
	}
}
```

- [ ] **Step 4: 运行测试验证失败**

Run: `go test ./internal/db/ -run "TestMarkKeyPermanentFailure|TestMarkKeyTemporaryFailure|TestClearKeyFailure" -v`
Expected: FAIL — 方法未定义

- [ ] **Step 5: 实现 3 个 DB 方法**

在 `internal/db/db.go` 的 `DisablePlatformKey` 函数（L1911-1919）后面新增：

```go
// MarkKeyPermanentFailure sets failure_type=2 with reason and timestamp.
// Used for 401/402/403/409/423/451 — key is permanently failed, needs user action.
// Does NOT touch enabled (decoupled from user intent).
func (db *DB) MarkKeyPermanentFailure(keyID int64, reason string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`
		UPDATE platform_keys
		SET failure_type = 2, failure_reason = ?, failed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, reason, keyID)
	return err
}

// MarkKeyTemporaryFailure sets failure_type=1 with reason and timestamp.
// Used for 429/5xx/timeout — key is temporarily failing, auto-recovering.
func (db *DB) MarkKeyTemporaryFailure(keyID int64, reason string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`
		UPDATE platform_keys
		SET failure_type = 1, failure_reason = ?, failed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, reason, keyID)
	return err
}

// ClearKeyFailure resets failure_type=0, reason='', failed_at=NULL.
// Called on successful request, or after successful probe.
func (db *DB) ClearKeyFailure(keyID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.conn.Exec(`
		UPDATE platform_keys
		SET failure_type = 0, failure_reason = '', failed_at = NULL, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, keyID)
	return err
}
```

- [ ] **Step 6: 更新 GetPlatformKeys 查询包含新字段**

在 `internal/db/db.go` L1775-1809，修改 `GetPlatformKeys` 的 SQL 和 Scan：

```go
func (db *DB) GetPlatformKeys(platformID int64) ([]models.PlatformKey, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rows, err := db.conn.Query(`
		SELECT id, platform_id, key_index, token, label, enabled, session_headers,
		       failure_type, failure_reason, failed_at, created_at, updated_at
		FROM platform_keys WHERE platform_id = ? ORDER BY key_index ASC
	`, platformID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make([]models.PlatformKey, 0)
	for rows.Next() {
		var k models.PlatformKey
		var encToken string
		var enabled sql.NullInt64
		var sessionHeaders sql.NullString
		var failedAt sql.NullTime
		var created, updated sql.NullTime
		if err := rows.Scan(&k.ID, &k.PlatformID, &k.KeyIndex, &encToken, &k.Label, &enabled, &sessionHeaders,
			&k.FailureType, &k.FailureReason, &failedAt, &created, &updated); err != nil {
			return nil, err
		}
		var decErr error
		if k.Token, decErr = crypto.Decrypt(encToken); decErr != nil {
			return nil, fmt.Errorf("decrypt platform_key %d: %w", k.ID, decErr)
		}
		k.Enabled = enabled.Int64 != 0
		k.SessionHeaders = sessionHeaders.String
		if failedAt.Valid {
			k.FailedAt = &failedAt.Time
		}
		if created.Valid {
			k.CreatedAt = created.Time
		}
		if updated.Valid {
			k.UpdatedAt = updated.Time
		}
		keys = append(keys, k)
	}
	return keys, nil
}
```

- [ ] **Step 7: 运行测试验证通过**

Run: `go test ./internal/db/ -run "TestMarkKeyPermanentFailure|TestMarkKeyTemporaryFailure|TestClearKeyFailure|TestPlatformKeyCRUD" -v`
Expected: PASS

- [ ] **Step 8: 提交**

```bash
git add internal/db/db.go internal/db/db_test.go
git commit -m "feat(db): add MarkKeyPermanentFailure/TemporaryFailure/ClearKeyFailure methods"
```

---

## Task 3: Bug 修复 — GetEnabledRAPIsForLAPI 解耦平台可用性

**Files:**
- Modify: `internal/db/db.go:1369-1386`

- [ ] **Step 1: 写失败测试 — TestGetEnabledRAPIsIgnoresPlatformAvailable**

在 `internal/db/db_test.go` 末尾新增：

```go
func TestGetEnabledRAPIsIgnoresPlatformAvailable(t *testing.T) {
	db := setupTestDB(t)

	// Create platform with available=0 (simulates stale base_url unreachable flag)
	p := &models.Platform{Name: "test-plat", BaseURL: "https://api.test.com/v1", Token: "sk-test"}
	db.CreatePlatform(p)
	if err := db.SetPlatformAvailable(p.ID, false); err != nil {
		t.Fatalf("SetPlatformAvailable: %v", err)
	}

	// Create enabled+available RAPI under this platform
	r := &models.RAPI{Alias: "test-rapi", Model: "test-model", PlatformID: p.ID, Enabled: true, Available: true}
	if err := db.CreateRAPI(r); err != nil {
		t.Fatalf("CreateRAPI: %v", err)
	}

	// Create LAPI and link
	l := &models.LAPI{Alias: "test-lapi"}
	db.CreateLAPI(l)
	if err := db.SetLAPIRAPIs(l.ID, []int64{r.ID}); err != nil {
		t.Fatalf("SetLAPIRAPIs: %v", err)
	}

	// Even though platform.available=0, RAPI should still be returned
	rapis, err := db.GetEnabledRAPIsForLAPI(l.ID)
	if err != nil {
		t.Fatalf("GetEnabledRAPIsForLAPI: %v", err)
	}
	if len(rapis) != 1 {
		t.Fatalf("GetEnabledRAPIsForLAPI returned %d RAPIs, want 1 (platform.available should not gate selection)", len(rapis))
	}
	if rapis[0].ID != r.ID {
		t.Errorf("returned RAPI ID = %d, want %d", rapis[0].ID, r.ID)
	}
}
```

- [ ] **Step 2: 检查测试中是否需要 SetLAPIRAPIs 辅助**

Run: `grep -n "func.*SetLAPIRAPIs" internal/db/db.go`
确认方法存在；如不存在，检查 `internal/db/db_test.go` 是否有等价方法。若无，使用以下方式直接插入 lapi_rapi_order 行：

如果 `SetLAPIRAPIs` 不存在，在测试中直接插入：

```go
	db.conn.Exec("INSERT INTO lapi_rapi_order (lapi_id, rapi_id, order_index) VALUES (?, ?, 0)", l.ID, r.ID)
```

- [ ] **Step 3: 运行测试验证失败**

Run: `go test ./internal/db/ -run TestGetEnabledRAPIsIgnoresPlatformAvailable -v`
Expected: FAIL — `len(rapis) = 0, want 1`

- [ ] **Step 4: 修复查询**

在 `internal/db/db.go` L1383，将 `AND p.available = 1` 从 WHERE 子句移除：

```go
func (db *DB) GetEnabledRAPIsForLAPI(lapiID int64) ([]models.RAPIWithPlatform, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.queryRAPIsWithPlatform(`
		SELECT r.id, r.alias, r.model, r.platform_id, r.enabled, r.available, r.unavailable_reason,
		       r.base_cost, r.high_cost, r.rpm_limit, r.rph_limit, r.rpd_limit, r.tpm_limit, r.tph_limit, r.tpd_limit, r.time_period_rules, r.created_at, r.updated_at,
		       p.name, p.base_url, p.token, p.last_token_fetch,
		       o.order_index, r.supported_formats, r.custom_headers, p.url_auto_complete, r.notes, p.custom_headers,
		       COALESCE(p.is_dynamic, 0), COALESCE(p.webpage_domain, '')
		FROM rapi r
		LEFT JOIN platform p ON r.platform_id = p.id
		INNER JOIN lapi_rapi_order o ON r.id = o.rapi_id
		WHERE o.lapi_id = ? AND r.enabled = 1 AND r.available = 1 AND p.enabled = 1
		ORDER BY o.order_index ASC
	`, lapiID)
}
```

- [ ] **Step 5: 运行测试验证通过**

Run: `go test ./internal/db/ -run TestGetEnabledRAPIsIgnoresPlatformAvailable -v`
Expected: PASS

- [ ] **Step 6: 运行全量 DB 测试确保无回归**

Run: `go test ./internal/db/ -v`
Expected: 所有测试通过

- [ ] **Step 7: 提交**

```bash
git add internal/db/db.go internal/db/db_test.go
git commit -m "fix(db): decouple platform.available from RAPI selection in GetEnabledRAPIsForLAPI"
```

---

## Task 4: 调度器 key 过滤

**Files:**
- Modify: `internal/scheduler/scheduler.go:250-267`
- Modify: `internal/scheduler/scheduler_test.go`

- [ ] **Step 1: 写失败测试 — TestPickAvailableKeySkipsPermanentFailure**

在 `internal/scheduler/scheduler_test.go` 末尾新增：

```go
func TestPickAvailableKeySkipsPermanentFailure(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	keys := []models.PlatformKey{
		{ID: 1, KeyIndex: 0, Enabled: true, FailureType: 2},  // permanent failure, skip
		{ID: 2, KeyIndex: 1, Enabled: true, FailureType: 0},  // healthy
	}

	got, _, err := m.PickAvailableKey(keys)
	if err != nil {
		t.Fatalf("PickAvailableKey: %v", err)
	}
	if got.ID != 2 {
		t.Fatalf("selected key %d, want 2 (key 1 is permanent failure)", got.ID)
	}
}

func TestPickAvailableKeyAllowsTemporaryFailure(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	// failure_type=1 (temporary) keys are NOT skipped here — they're managed by
	// the scheduler's in-memory cooldown. PickAvailableKey should return them.
	keys := []models.PlatformKey{
		{ID: 1, KeyIndex: 0, Enabled: true, FailureType: 1},  // temporary failure, not skipped
	}

	got, _, err := m.PickAvailableKey(keys)
	if err != nil {
		t.Fatalf("PickAvailableKey: %v", err)
	}
	if got.ID != 1 {
		t.Fatalf("selected key %d, want 1 (temporary failure should not be skipped)", got.ID)
	}
}

func TestPickAvailableKeyAllPermanentFails(t *testing.T) {
	m := NewManager(testConfig())
	defer m.Close()

	keys := []models.PlatformKey{
		{ID: 1, KeyIndex: 0, Enabled: true, FailureType: 2},
		{ID: 2, KeyIndex: 1, Enabled: true, FailureType: 2},
	}

	_, _, err := m.PickAvailableKey(keys)
	if !errors.Is(err, ErrAllKeysUnavailable) {
		t.Fatalf("err = %v, want ErrAllKeysUnavailable", err)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `go test ./internal/scheduler/ -run "TestPickAvailableKey" -v`
Expected: FAIL — `got.ID = 1, want 2`（因为 FailureType 未被过滤）

- [ ] **Step 3: 修改 PickAvailableKey 加过滤**

在 `internal/scheduler/scheduler.go` L256-258，将 `if !k.Enabled` 改为同时检查 `FailureType`：

```go
func (m *Manager) PickAvailableKey(keys []models.PlatformKey) (models.PlatformKey, time.Time, error) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	var nextAvail time.Time
	for _, k := range keys {
		if !k.Enabled || k.FailureType == 2 {
			continue
		}
		ks := m.keyStateForLocked(k.ID)
		if ks.unavailableUntil.After(now) {
			nextAvail = minNonZero(nextAvail, ks.unavailableUntil)
			continue
		}
		return k, time.Time{}, nil
	}
	return models.PlatformKey{}, nextAvail, ErrAllKeysUnavailable
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `go test ./internal/scheduler/ -run "TestPickAvailableKey" -v`
Expected: PASS

- [ ] **Step 5: 运行全量调度器测试确保无回归**

Run: `go test ./internal/scheduler/ -v`
Expected: 所有测试通过

- [ ] **Step 6: 提交**

```bash
git add internal/scheduler/scheduler.go internal/scheduler/scheduler_test.go
git commit -m "feat(scheduler): skip keys with failure_type=2 in PickAvailableKey"
```

---

## Task 5: 简化 restorePlatformAvailability

**Files:**
- Modify: `internal/service/service.go:2203-2220`

- [ ] **Step 1: 简化 restorePlatformAvailability**

在 `internal/service/service.go` L2198-2220，移除 `SetPlatformAvailable` 调用，保留 `RevalidateRAPI`：

```go
// restorePlatformAvailability revalidates every child RAPI in the scheduler after a
// key is added/replaced. Previously this also flipped platform.available=true, but
// that's now decoupled: platform.available only reflects base_url reachability
// (set by detect-formats / restore endpoint), not key presence.
func restorePlatformAvailability(platformID int64) {
	plat, err := db.Get().GetPlatformByID(platformID)
	if err != nil || plat == nil {
		return
	}
	if proxyGateway != nil {
		rapis, _ := db.Get().GetRAPIsByPlatform(platformID)
		for _, ra := range rapis {
			proxyGateway.RevalidateRAPI(ra.ID)
		}
	}
	log.Printf("[KEY] platform %s(id=%d) RAPIs revalidated after key change", plat.Name, platformID)
}
```

- [ ] **Step 2: 验证编译**

Run: `go build ./...`
Expected: 编译通过

- [ ] **Step 3: 提交**

```bash
git add internal/service/service.go
git commit -m "refactor(service): simplify restorePlatformAvailability — no longer sets platform.available"
```

---

## Task 6: 重写网关失败归因

**Files:**
- Modify: `internal/gateway/gateway.go:593-661`

- [ ] **Step 1: 重写 2xx 成功路径 — 加 ClearKeyFailure**

在 `internal/gateway/gateway.go` L593-596，2xx 成功路径加 `ClearKeyFailure`：

```go
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		g.scheduler.MarkKeySuccess(key.ID)
		if key.ID > 0 {
			if dbErr := g.db.ClearKeyFailure(key.ID); dbErr != nil {
				log.Printf("[WARN] ClearKeyFailure key=%d: %v", key.ID, dbErr)
			}
		}
		return resp, key.ID, nil
	}
```

- [ ] **Step 2: 重写 401 (failureSystem) 路径**

在 `internal/gateway/gateway.go` L610-632，替换 `DisablePlatformKey` 为 `MarkKeyPermanentFailure`：

```go
	case failureSystem:
		// 401 Unauthorized: key token is invalid — mark permanent failure (DB + scheduler).
		// Does NOT touch enabled (decoupled from user intent). Scheduler skips
		// failure_type=2 keys via PickAvailableKey.
		reason := fmt.Sprintf("[认证失败] upstream 401: %s", string(errBody))
		log.Printf("[KEY-FAIL] rapi=%s key=%d 401 unauthorized, marking permanent failure", rapi.Alias, key.ID)
		g.scheduler.MarkKeyPlatformFailure(key.ID, "401 unauthorized")
		if key.ID > 0 {
			if dbErr := g.db.MarkKeyPermanentFailure(key.ID, reason); dbErr != nil {
				log.Printf("[WARN] MarkKeyPermanentFailure key=%d: %v", key.ID, dbErr)
			}
		}
		if g.log != nil {
			g.log.RecordError(requestID, reason, "UPSTREAM_RESPONSE")
		}
		if g.notifyService != nil {
			g.notifyService.PublishAsync(
				fmt.Sprintf("模型 %s [%s] Key #%d 认证失败（401），已标记永久失效，请处理后点击重置状态", rapi.Alias, rapi.PlatformName, key.KeyIndex),
				"Key 认证失败",
			)
		}
		continue
```

- [ ] **Step 3: 重写 402/403 (failurePlatform) 路径 — 改 return 为 continue**

在 `internal/gateway/gateway.go` L634-654，替换 `SetRAPIUnavailableWithReason` + `InvalidateRAPI` + `return` 为 `MarkKeyPermanentFailure` + `continue`：

```go
	case failurePlatform:
		// 平台级：欠费/封号/模型失效 → 标记 key 永久失败（不碰 RAPI.available）
		// 改为 continue 让同 RAPI 的其他 key 继续尝试，而非整体降级。
		reason := fmt.Sprintf("[平台级] upstream %d: %s", resp.StatusCode, string(errBody))
		log.Printf("[KEY-FAIL] rapi=%s key=%d status=%d, marking permanent failure", rapi.Alias, key.ID, resp.StatusCode)
		g.scheduler.MarkKeyPlatformFailure(key.ID, reason)
		if key.ID > 0 {
			if dbErr := g.db.MarkKeyPermanentFailure(key.ID, reason); dbErr != nil {
				log.Printf("[WARN] MarkKeyPermanentFailure key=%d: %v", key.ID, dbErr)
			}
		}
		if g.log != nil {
			g.log.RecordError(requestID, reason, "UPSTREAM_RESPONSE")
		}
		if g.notifyService != nil {
			g.notifyService.PublishAsync(
				fmt.Sprintf("模型 %s [%s] Key #%d 平台受限（%d），已标记永久失效，请处理后点击重置状态", rapi.Alias, rapi.PlatformName, key.KeyIndex, resp.StatusCode),
				"Key 平台受限",
			)
		}
		continue
```

- [ ] **Step 4: 重写 429/5xx (failureSession) 路径 — 加 MarkKeyTemporaryFailure**

在 `internal/gateway/gateway.go` L656-660，加 `MarkKeyTemporaryFailure`：

```go
	default: // failureSession
		// 会话级：限流/内容违规/请求过长/临时故障 → key 短冷却 + 标记临时失败
		retryAt := scheduler.RetryAt(resp.Header, time.Time{})
		reason := fmt.Sprintf("upstream %d: %s", resp.StatusCode, string(errBody))
		g.scheduler.MarkKeyFailure(key.ID, retryAt, reason)
		if key.ID > 0 {
			if dbErr := g.db.MarkKeyTemporaryFailure(key.ID, reason); dbErr != nil {
				log.Printf("[WARN] MarkKeyTemporaryFailure key=%d: %v", key.ID, dbErr)
			}
		}
		continue
```

- [ ] **Step 5: 验证编译**

Run: `go build ./...`
Expected: 编译通过

- [ ] **Step 6: 运行现有网关测试确保无回归**

Run: `go test ./internal/gateway/ -v`
Expected: 所有测试通过

- [ ] **Step 7: 提交**

```bash
git add internal/gateway/gateway.go
git commit -m "feat(gateway): rewrite failure attribution — mark key-level failure_type instead of disabling key or marking RAPI"
```

---

## Task 7: 新增 probe 端点

**Files:**
- Modify: `internal/service/service.go:1042-1052` (路由扩展)
- Modify: `internal/service/service.go` (新增 handler)

- [ ] **Step 1: 扩展 /api/platforms/ 路由解析支持 probe 子路径**

在 `internal/service/service.go` L1042-1052 的 `/api/platforms/` handler 中，扩展路径解析以支持 `/api/platforms/{id}/keys/{keyId}/probe`：

```go
	// Platform Keys CRUD + probe endpoint: /api/platforms/{id}/keys[/{keyId}/probe]
	mux.HandleFunc("/api/platforms/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Only handle paths that look like /api/platforms/{id}/keys...
		path := strings.TrimPrefix(r.URL.Path, "/api/platforms/")
		parts := strings.Split(path, "/")
		if len(parts) < 2 || parts[1] != "keys" {
			http.Error(w, `{"error":"not found"}`, 404)
			return
		}

		var platformID int64
		fmt.Sscanf(parts[0], "%d", &platformID)
		if platformID == 0 {
			http.Error(w, `{"error":"invalid platform id"}`, 400)
			return
		}

		// Sub-path: /api/platforms/{id}/keys/{keyId}/probe
		if len(parts) == 4 && parts[3] == "probe" {
			handleKeyProbe(w, r, platformID, parts[2])
			return
		}
```

后续的 `switch r.Method` 保持不变（处理 GET/POST/PUT/DELETE for /api/platforms/{id}/keys）。

- [ ] **Step 2: 实现 handleKeyProbe 函数**

在 `internal/service/service.go` 的 `restorePlatformAvailability` 函数后面新增：

```go
// handleKeyProbe tests a specific platform key by calling /v1/models (or /v1beta/models
// for Google native). On success: clears key failure_type. On failure: keeps failure_type=2.
func handleKeyProbe(w http.ResponseWriter, r *http.Request, platformID int64, keyIDStr string) {
	var keyID int64
	fmt.Sscanf(keyIDStr, "%d", &keyID)
	if keyID == 0 {
		writeJSONError(w, 400, fmt.Errorf("invalid key id"))
		return
	}

	platform, err := db.Get().GetPlatformByID(platformID)
	if err != nil || platform == nil {
		writeJSONError(w, 404, fmt.Errorf("platform not found"))
		return
	}

	// Find the specific key
	keys, err := db.Get().GetPlatformKeys(platformID)
	if err != nil {
		writeJSONError(w, 500, err)
		return
	}
	var targetKey *models.PlatformKey
	for i := range keys {
		if keys[i].ID == keyID {
			targetKey = &keys[i]
			break
		}
	}
	if targetKey == nil {
		writeJSONError(w, 404, fmt.Errorf("key not found"))
		return
	}
	if targetKey.Token == "" {
		writeJSONError(w, 400, fmt.Errorf("key token is empty"))
		return
	}

	// Build the list-models URL (same logic as /api/platforms/restore).
	client := &http.Client{Timeout: 15 * time.Second}
	httpReq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, "", nil)
	if apiformat.IsGoogleNativeBaseURL(platform.BaseURL) {
		modelsURL := apiformat.BuildGoogleListModelsURL(platform.BaseURL, targetKey.Token)
		httpReq.URL, _ = url.Parse(modelsURL)
		httpReq.Header.Set(apiformat.GoogleAPIKeyHeader, targetKey.Token)
	} else {
		baseURL := platform.BaseURL
		for _, suffix := range []string{"/v1/chat/completions", "/v1/messages", "/v1beta/models", "/v1beta", "/v1/chat", "/v1"} {
			if len(baseURL) >= len(suffix) && baseURL[len(baseURL)-len(suffix):] == suffix {
				baseURL = baseURL[:len(baseURL)-len(suffix)]
				break
			}
		}
		for len(baseURL) > 0 && baseURL[len(baseURL)-1] == '/' {
			baseURL = baseURL[:len(baseURL)-1]
		}
		modelsURL := baseURL + "/v1/models"
		httpReq.URL, _ = url.Parse(modelsURL)
		httpReq.Header.Set("Authorization", "Bearer "+targetKey.Token)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	log.Printf("[PROBE] testing key id=%d platform=%s via %s", keyID, platform.Name, httpReq.URL.String())
	resp, err := client.Do(httpReq)
	if err != nil {
		writeJSONError(w, 502, fmt.Errorf("连接失败: %v", err))
		return
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		writeJSONError(w, 502, fmt.Errorf("平台返回 %d，重置失败", resp.StatusCode))
		return
	}

	// Success: clear failure_type + notify scheduler
	if err := db.Get().ClearKeyFailure(keyID); err != nil {
		writeJSONError(w, 500, err)
		return
	}
	if proxyGateway != nil {
		proxyGateway.Scheduler().MarkKeySuccess(keyID)
	}
	log.Printf("[PROBE] key id=%d restored, failure_type cleared", keyID)
	w.Write([]byte(`{"success":true}`))
}
```

- [ ] **Step 3: 确认 import**

检查 `internal/service/service.go` 顶部是否已 import：
- `"gateway/internal/models"` — 用于 `models.PlatformKey`
- `"io"` — 用于 `io.ReadAll`
- `"net/url"` — 用于 `url.Parse`

若缺失则补充。

- [ ] **Step 4: 验证编译**

Run: `go build ./...`
Expected: 编译通过

- [ ] **Step 5: 提交**

```bash
git add internal/service/service.go
git commit -m "feat(service): add POST /api/platforms/{id}/keys/{keyId}/probe endpoint"
```

---

## Task 8: UI 变更 — 密钥状态徽标 + 重置按钮

**Files:**
- Modify: `internal/service/dashboard.html:2658-2680`

- [ ] **Step 1: 在 key 行加状态徽标 + 重置按钮**

在 `internal/service/dashboard.html` L2658-2680，将现有 key 行模板替换为：

```html
<div v-for="k in keysModal.keys" :key="k.id" class="key-row" :class="{ disabled: !k.enabled }">
    <div class="key-index-dot">{{ k.key_index }}</div>
    <div style="min-width:0;flex:1;">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <div class="key-token-display" :title="k.token">{{ maskToken(k.token) }}</div>
            <div class="key-label-tag">{{ k.label || '未命名' }}</div>
            <!-- 失败状态徽标 -->
            <span v-if="k.failure_type === 2"
                  :title="k.failure_reason"
                  style="display:inline-flex;align-items:center;gap:3px;font-size:0.7rem;padding:1px 8px;border-radius:12px;font-weight:600;background:rgba(239,68,68,0.1);color:var(--color-danger);border:1px solid rgba(239,68,68,0.3);">
                🔴 永久失效·需处理
            </span>
            <span v-else-if="k.failure_type === 1"
                  :title="k.failure_reason"
                  style="display:inline-flex;align-items:center;gap:3px;font-size:0.7rem;padding:1px 8px;border-radius:12px;font-weight:600;background:rgba(245,158,11,0.1);color:var(--color-warning);border:1px solid rgba(245,158,11,0.3);">
                🟡 临时冷却
            </span>
            <span v-else
                  style="display:inline-flex;align-items:center;gap:3px;font-size:0.7rem;padding:1px 8px;border-radius:12px;font-weight:600;background:rgba(34,197,94,0.1);color:var(--color-success);border:1px solid rgba(34,197,94,0.3);">
                🟢 正常
            </span>
        </div>
        <div v-if="k.failure_reason" style="font-size:0.72rem;color:var(--text-muted);margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
            {{ k.failure_reason }}
        </div>
    </div>
    <div class="key-actions">
        <label class="checkbox-container" style="margin:0;gap:6px;font-size:0.8rem;" :title="k.enabled ? '点击禁用' : '点击启用'">
            <input type="checkbox" :checked="k.enabled" @change="toggleKey(k)">
            <span :style="{ color: k.enabled ? 'var(--color-success)' : 'var(--text-muted)' }">
                {{ k.enabled ? '启用' : '禁用' }}
            </span>
        </label>
    </div>
    <div class="key-actions">
        <button v-if="k.failure_type === 2"
                class="btn btn-secondary btn-sm"
                @click="probeKey(k)"
                title="重置失败状态（会发探测请求验证 key 是否恢复）"
                style="white-space:nowrap;">
            重置状态
        </button>
        <button class="btn btn-secondary btn-sm btn-icon" @click="navigator.clipboard.writeText(k.token).then(()=>showToast('已复制','密钥已复制到剪贴板','success'))" title="复制密钥">
            <svg style="width:13px;height:13px;" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
        </button>
        <button class="btn btn-danger btn-sm btn-icon" @click="deleteKey(k)" title="删除此 Key">
            <svg style="width:13px;height:13px;" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
        </button>
    </div>
</div>
```

- [ ] **Step 2: 新增 probeKey 方法**

在 `internal/service/dashboard.html` 的 Vue methods 中（`toggleKey` 方法附近，约 L3830），新增 `probeKey` 方法：

```javascript
async probeKey(k) {
    if (!confirm(`确定重置 Key #${k.key_index} 的失败状态吗？系统会发送探测请求验证 key 是否恢复。`)) return;
    try {
        const r = await fetch(`/api/platforms/${k.platform_id}/keys/${k.id}/probe`, {
            method: 'POST',
        });
        if (r.ok) {
            showToast('成功', 'Key 已恢复正常，失败状态已清除', 'success');
            // 刷新 key 列表
            const kr = await fetch(`/api/platforms/${k.platform_id}/keys`);
            keysModal.keys = kr.ok ? await kr.json() : [];
            if (keysModal.platform) keysModal.platform._keys = [...keysModal.keys];
        } else {
            const err = await r.json().catch(() => ({}));
            showToast('重置失败', err.error || `平台返回 ${r.status}`, 'error');
        }
    } catch (e) {
        showToast('错误', '请求失败: ' + e.message, 'error');
    }
},
```

- [ ] **Step 3: 验证编译 + 启动**

Run: `go build ./...`
Expected: 编译通过

- [ ] **Step 4: 提交**

```bash
git add internal/service/dashboard.html
git commit -m "feat(ui): add key failure status badge + reset button in keys modal"
```

---

## Task 9: 集成验证

**Files:** 无（仅验证）

- [ ] **Step 1: go build 全量编译**

Run: `go build ./...`
Expected: 编译通过

- [ ] **Step 2: go vet 静态检查**

Run: `go vet ./...`
Expected: 无警告

- [ ] **Step 3: go test 全量测试**

Run: `go test ./...`
Expected: 所有测试通过

- [ ] **Step 4: 启动网关验证**

Run: `go run .` (后台启动)
Expected: 网关正常启动，无 panic

- [ ] **Step 5: 验证 Bug 修复 — Google RAPI 不再被 platform.available 过滤**

```bash
# 假设 Google 平台 available=0，但有 RAPI + key
curl http://localhost:8080/v1/chat/completions -H "Content-Type: application/json" -d '{"model":"gemini-2.5-flash","messages":[{"role":"user","content":"hi"}]}'
```
Expected: 不再返回 "No backends available"；如果 key 有效，应返回正常响应；如果 key 无效，应返回 401 而非 "No backends"

- [ ] **Step 6: 验证 UI 状态徽标**

打开 dashboard → 平台 → 密钥管理，确认：
- 正常 key 显示 🟢 正常
- 401 失败 key 显示 🔴 永久失效·需处理 + "重置状态"按钮
- 429 冷却 key 显示 🟡 临时冷却

- [ ] **Step 7: 验证重置状态按钮**

点击 🔴 key 的"重置状态"按钮：
- 探测成功 → 徽标变 🟢
- 探测失败 → toast 显示错误，徽标保持 🔴

- [ ] **Step 8: 最终提交（如有遗漏修复）**

如无遗漏，计划完成。

---

## 自审清单

- [x] **Spec 覆盖**：所有 11 个 spec 章节都有对应 Task
  - §2 数据模型 → Task 1
  - §3 失败归因 → Task 6
  - §4 Bug 修复 → Task 3 + Task 5
  - §5 调度器过滤 → Task 4
  - §6 DB 方法 → Task 2
  - §7 探测端点 → Task 7
  - §8 UI → Task 8
  - §9 恢复机制 → Task 6 (自动) + Task 7 (手动)
  - §10 迁移 → Task 1
  - §11 测试 → 各 Task 内嵌
- [x] **无占位符**：所有步骤含完整代码
- [x] **类型一致性**：`FailureType` (int) / `FailureReason` (string) / `FailedAt` (*time.Time) 全局一致
- [x] **方法名一致**：`MarkKeyPermanentFailure` / `MarkKeyTemporaryFailure` / `ClearKeyFailure` 在 spec、plan、代码中一致
