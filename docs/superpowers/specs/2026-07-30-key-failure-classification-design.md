# Key 失败分类与归因下沉设计

- 日期：2026-07-30
- 状态：已批准（待实现）
- 范围：`internal/db`、`internal/gateway`、`internal/scheduler`、`internal/service`、`internal/models`、`internal/service/dashboard.html`

## 1. 背景与动机

### 1.1 当前架构问题

平台 key 失效时，失败状态散落在 3 个层级，且归因错位：

| 错误码 | 当前归因位置 | 问题 |
|--------|------------|------|
| 401 | `platform_keys.enabled=0`（自动禁用） | 缺结构化分类标签；`enabled` 字段同时承担"用户意图"和"系统观察"两个职责 |
| 402/403/409/423/451 | `rapi.available=0` + `unavailable_reason` | 错误归因到 RAPI/模型，实际是具体 key 的账号问题 |
| 429/5xx/timeout | 调度器内存冷却 | 重启后丢失上下文，无法在 UI 查询历史失败原因 |

### 1.2 Bug：Google "No backends available for model: gemini-3.6-flash"

根因在 [db.go:1383](`GetEnabledRAPIsForLAPI`) 的查询条件 `AND p.available = 1`。Google 平台在历史事件（未添加 key、或历史 403）后 `platform.available=0`，导致所有 Gemini RAPI 被整体排除，即使后续添加了有效 key 也不会恢复。

这印证了核心架构问题：**`platform.available` 字段被滥用**——既表示 base_url 可达，又被 `restorePlatformAvailability` 当成 "key 已添加" 的标记，还间接 gate 了 RAPI 选择。

### 1.3 设计原则

**平台问题和 key 问题解耦。** 平台本身只有 2 个状态：
1. `base_url` 是否可达
2. 支持的协议（`supported_formats`）

key 失败状态完全下沉到 `platform_keys` 表，不污染 platform / RAPI 层。

## 2. 数据模型变更

### 2.1 `platform_keys` 表新增 3 列

```sql
ALTER TABLE platform_keys ADD COLUMN failure_type INTEGER NOT NULL DEFAULT 0;
-- 0=none, 1=temporary(429/5xx/timeout), 2=permanent(401/402/403/409/423/451)
ALTER TABLE platform_keys ADD COLUMN failure_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE platform_keys ADD COLUMN failed_at DATETIME;
```

迁移用 `pragma_table_info` 幂等检查（与现有 `reusable_status` 迁移模式一致，见 [db.go:2409-2412](L2409-L2412)）。

### 2.2 `models.PlatformKey` 结构体

```go
type PlatformKey struct {
    // ... 现有字段 ...
    FailureType    int        `json:"failure_type"`     // 0=none, 1=temporary, 2=permanent
    FailureReason  string     `json:"failure_reason"`
    FailedAt       *time.Time `json:"failed_at,omitempty"`
}
```

`needs_user_action` 不单独存字段，从 `failure_type == 2` 推导（当前 1:1，避免冗余）。

### 2.3 字段语义对照表（解耦后）

| 字段 | 含义 | 由谁设置 |
|------|------|---------|
| `platform.enabled` | 用户是否启用此平台 | 用户 |
| `platform.available` | base_url 是否可达 | detect-formats / restore 端点 |
| `rapi.enabled` | 用户是否启用此 RAPI | 用户 |
| `rapi.available` | 模型是否可用（如模型下架） | 手动 / 模型级探测 |
| `platform_keys.enabled` | 用户是否启用此 key | 用户（纯用户意图） |
| `platform_keys.failure_type` | 系统观察到的失败类型 | 网关（自动）/ probe（手动重置） |

## 3. 失败归因重写（gateway.go L609-661）

### 3.1 新行为对照表

| 错误码 | 旧行为 | 新行为 |
|--------|--------|--------|
| 401 (failureSystem) | `DisablePlatformKey` (enabled=0) | **不再碰 enabled**，调用 `MarkKeyPermanentFailure(keyID, "[认证失败] upstream 401: ...")` + `continue` |
| 402/403/409/423/451 (failurePlatform) | `SetRAPIUnavailableWithReason` + `InvalidateRAPI` + `return error` | **不再碰 RAPI**，调用 `MarkKeyPermanentFailure(keyID, "[平台级] upstream 403: ...")` + `continue`（尝试下一个 key） |
| 429/5xx/timeout (failureSession) | 调度器内存冷却 | 调度器冷却 + `MarkKeyTemporaryFailure(keyID, reason)` + `continue` |
| 2xx 成功 | `MarkKeySuccess` | `MarkKeySuccess` + `ClearKeyFailure(keyID)` |

### 3.2 核心变化

1. **401 不再 auto-disable**：`enabled` 纯用户控制，调度器通过 `failure_type=2` 跳过该 key（等效于旧 `enabled=0` 的效果，但解耦）
2. **402/403 从 `return error` 改为 `continue`**：让同 RAPI 的其他 key 继续尝试，避免单 key 失败拖垮整个 RAPI
3. **成功路径清空失败状态**：确保临时/永久失败标签在 key 恢复正常后自动清除（仅对临时失败有意义；永久失败需用户手动重置）

### 3.3 调度器调用保留说明

现有的调度器内存调用**全部保留不变**，本次只改 DB 持久化和控制流：

| 错误码 | 调度器调用（保留） | DB 调用（新增/变更） | 控制流（变更） |
|--------|------------------|---------------------|---------------|
| 401 | `MarkKeyPlatformFailure` (长冷却) | `MarkKeyPermanentFailure` (新增) | `continue`（不变） |
| 402/403 | `MarkKeyPlatformFailure` (长冷却) | `MarkKeyPermanentFailure` (新增，替代 `SetRAPIUnavailableWithReason`) | `continue`（从 `return` 改） |
| 429/5xx | `MarkKeyFailure` (短冷却) | `MarkKeyTemporaryFailure` (新增) | `continue`（不变） |
| 2xx | `MarkKeySuccess` | `ClearKeyFailure` (新增) | `return`（不变） |

同时移除 401 路径的 `DisablePlatformKey` 调用，以及 402/403 路径的 `SetRAPIUnavailableWithReason` + `InvalidateRAPI` 调用。

### 3.4 通知文案调整

- 401：`"Key #X 认证失败（401），已标记永久失效，请处理后点击重置状态"`
- 402/403：`"Key #X 平台受限（403），已标记永久失效，请处理后点击重置状态"`
- 429：不发送通知（临时失败，自动恢复）

## 4. Bug 修复：解耦平台可用性

### 4.1 `GetEnabledRAPIsForLAPI` 查询

[db.go:1383](L1369-L1386) 去掉 `AND p.available = 1`：

```sql
-- 旧
WHERE o.lapi_id = ? AND r.enabled = 1 AND r.available = 1 AND p.enabled = 1 AND p.available = 1
-- 新
WHERE o.lapi_id = ? AND r.enabled = 1 AND r.available = 1 AND p.enabled = 1
```

### 4.2 `platform.available` 语义收窄

- **仅反映 base_url 可达性**，由 detect-formats / restore 端点设置
- key 添加/失效不再触碰 `platform.available`
- `restorePlatformAvailability`（service.go L2203-2220）简化：
  - **移除**：`SetPlatformAvailable(platformID, true)` 调用（不再因添加 key 而改 platform.available）
  - **保留**：`RevalidateRAPI` 调用（仍需通知调度器"此平台的 RAPI 现在有 key 可用了"）
  - 函数仍由添加 key 的 POST/PUT handler 调用，但只做 RAPI revalidation

### 4.3 `rapi.available` 语义收窄

- **仅用于模型级问题**（如模型下架、手动禁用）
- HTTP 错误（401/402/403/etc）不再触碰 `rapi.available`
- `SetRAPIUnavailableWithReason` 仍可用，但不再从 HTTP 错误路径调用

## 5. 调度器 key 过滤

### 5.1 `PickAvailableKey` 过滤条件

[scheduler.go:257](L250-L267) 增加过滤：

```go
if !k.Enabled || k.FailureType == 2 {
    continue
}
```

- `failure_type=2`（永久失败）：跳过（等效于旧 `enabled=0`）
- `failure_type=1`（临时）：不在此处过滤，由调度器内存冷却管理
- `failure_type=0`（正常）：可选用

### 5.2 内存冷却与持久化的关系

- **临时失败**：调度器内存冷却（重启丢失）+ DB `failure_type=1`（持久化，用于 UI 显示）
- 重启后：`failure_type=1` 的 key 会被重试（因为没有内存冷却），若仍失败则重新标记
- **永久失败**：DB `failure_type=2` 持久化，重启后仍跳过

## 6. 新增 DB 方法

```go
// MarkKeyPermanentFailure sets failure_type=2 with reason and timestamp.
// Used for 401/402/403/409/423/451 — key is permanently failed, needs user action.
func (db *DB) MarkKeyPermanentFailure(keyID int64, reason string) error

// MarkKeyTemporaryFailure sets failure_type=1 with reason and timestamp.
// Used for 429/5xx/timeout — key is temporarily failing, auto-recovering.
func (db *DB) MarkKeyTemporaryFailure(keyID int64, reason string) error

// ClearKeyFailure resets failure_type=0, reason='', failed_at=NULL.
// Called on successful request, or after successful probe.
func (db *DB) ClearKeyFailure(keyID int64) error
```

实现模式参考现有 `DisablePlatformKey`（db.go L1911-1919）。

## 7. 新增探测端点

### 7.1 端点定义

```
POST /api/platforms/{platformID}/keys/{keyID}/probe
```

### 7.2 处理流程

复用现有 restore 端点的探测逻辑（[service.go:582-625](L561-L639)），但下沉到 key 粒度：

1. 取指定 key 的 token（解密）
2. 根据 `platform.base_url` 判断 Google 原生还是 OpenAI 兼容
3. 发 `GET /v1/models`（OpenAI 兼容）或 `GET /v1beta/models?key=`（Google 原生）
4. **200** → `ClearKeyFailure(keyID)` + 调度器 `MarkKeySuccess(keyID)` + 返回 `{"success":true}`
5. **非 200** → 保留 `failure_type=2` + 返回 `{"error":"平台返回 401，重置失败"}` 详情

### 7.3 路由注册

在现有 `/api/platforms/` handler（service.go L1043）中扩展路径解析，支持 `/api/platforms/{id}/keys/{keyId}/probe` 子路径。

## 8. UI 变更（dashboard.html 密钥管理弹窗）

### 8.1 Key 行状态徽标

在 [dashboard.html L2658-2680](L2658-L2680) 的 key 行增加状态徽标：

```
[#0] sk-xxxx***xxxx  [🟢 正常]          [✓启用] [📋] [🗑]
[#1] sk-yyyy***yyyy  [🔴 永久失效·需处理] [重置状态] [✓启用] [📋] [🗑]
                         ↑ tooltip: "[认证失败] upstream 401: ..."
[#2] sk-zzzz***zzzz  [🟡 临时冷却]      [✓启用] [📋] [🗑]
                         ↑ tooltip: "upstream 429: ..."
```

- `failure_type=0`：绿色"正常"徽标
- `failure_type=1`：黄色"临时冷却"徽标，tooltip 显示 `failure_reason`
- `failure_type=2`：红色"永久失效·需处理"徽标 + "重置状态"按钮

### 8.2 "重置状态"按钮行为

1. 点击后调用 `POST /api/platforms/{platformID}/keys/{keyID}/probe`
2. 成功：刷新 key 列表，徽标变绿
3. 失败：toast 显示错误详情，徽标保持红色

### 8.3 现有 `enabled` toggle 不变

`enabled` 仍是纯用户意图控制（启用/禁用），与 `failure_type` 解耦。

## 9. 恢复机制

| 失败类型 | 恢复方式 | 触发条件 |
|---------|---------|---------|
| 临时 (failure_type=1) | 自动清空 | 下次请求成功时 `ClearKeyFailure` |
| 永久 (failure_type=2) | 手动重置 + 探测验证 | 用户点"重置状态" → probe 端点验证成功 |

## 10. 迁移策略

- 新列带默认值，老数据 `failure_type=0`（视为"未测试/正常"）
- 现有 `enabled=0`（被 401 自动禁用）的 key **不自动迁移**——用户可手动重新启用 + 点"重置状态"探测
- 迁移用 `pragma_table_info` 幂等检查，与现有 `reusable_status` 迁移模式一致

## 11. 测试策略

### 11.1 DB 层测试（db_test.go）

- `TestMarkKeyPermanentFailure`：验证 failure_type=2 + reason + failed_at 非空
- `TestMarkKeyTemporaryFailure`：验证 failure_type=1 + reason + failed_at 非空
- `TestClearKeyFailure`：验证 failure_type=0 + reason='' + failed_at=NULL
- `TestGetPlatformKeysIncludesFailureFields`：验证查询返回新字段

### 11.2 调度器测试（scheduler_test.go）

- `TestPickAvailableKeySkipsPermanentFailure`：failure_type=2 的 key 被跳过
- `TestPickAvailableKeyAllowsTemporaryFailure`：failure_type=1 的 key 不被跳过（由冷却管理）

### 11.3 网关测试

- 验证 401 路径不再调用 `DisablePlatformKey`，改为 `MarkKeyPermanentFailure`
- 验证 402/403 路径不再触碰 `rapi.available`，改为 `continue` 尝试下一个 key
- 验证成功路径调用 `ClearKeyFailure`

### 11.4 Bug 验证

- Google 平台 `available=0` 时，`GetEnabledRAPIsForLAPI` 仍返回 Gemini RAPI（不再被过滤）
- 端到端：添加 Gemini key → 发请求 → 不再出现 "No backends available"

### 11.5 探测端点测试

- `POST /api/platforms/{id}/keys/{keyId}/probe` 成功时清空 failure_type
- 失败时保留 failure_type 并返回错误详情

## 12. 不在本次范围

- `needs_user_action` 独立字段（当前从 `failure_type==2` 推导，未来需要再加）
- `failure_type=1` 的持久化冷却时间（当前重启后重试，不持久化 retry_at）
- 现有 `enabled=0` 历史数据的批量迁移
- 平台级 base_url 探测端点（与 key 探测分离，未来可加）
