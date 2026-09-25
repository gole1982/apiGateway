# 实体状态维护与 FSM 架构

本文档描述网关的状态维护架构：实体状态层（`internal/entity`）+ 通用 FSM 引擎
（`internal/fsm`），以及各组件如何协作。

## 背景：为什么重构

重构前，状态维护散落在三处，互相漂移：

| 位置 | 内容 |
|---|---|
| DB 列 | `rapi.available` / `credential.failure_type` / `enabled` / `expires_at` |
| scheduler 内存结构 | `unavailableUntil` / `invalidated` / `consecutiveFailures` |
| gateway 编排逻辑 | `classifyFailure` / `allKeysHardDead` / `handleAllKeysUnavailable` 里散落的 if/else |

典型后果（8-2 京东事故）：

- key 被 403 标记 `failure_type=2`（永久失败）后，RAPI 在 DB 里仍是
  `available=1`——"key 全死但 RAPI 看着健康"；
- 恢复逻辑只扫 `available=0` 的 RAPI，从不检查 key 池——恢复功能看不见坏 key；
- gateway 直接写 DB、直接改 scheduler 内存，两个事实源无法对账。

## 架构分层

```
┌─────────────────────────────────────────────────────┐
│ internal/gateway     编排层：发事件，不再直接改状态    │
│ internal/service     HTTP API：探测/恢复 → 发事件     │
├─────────────────────────────────────────────────────┤
│ internal/scheduler   协调器：持有实体，退化为薄层      │
├─────────────────────────────────────────────────────┤
│ internal/entity      实体状态层（单一事实源）          │
│   RAPI / Key / Platform 各自是 FSM                    │
│   Store 接口：状态转换副作用 → DB 持久化               │
├─────────────────────────────────────────────────────┤
│ internal/fsm         通用 FSM 引擎（表驱动）           │
└─────────────────────────────────────────────────────┘
```

## 通用 FSM 引擎（internal/fsm）

表驱动状态机，一次注册、全走 `Fire`：

```go
m := fsm.New(StateHealthy)
m.Add(fsm.Transition{From: StateHealthy, Event: EventSessionFailure, To: StateCooling, Guard: ..., Effect: ...})
m.Fire(EventSessionFailure, payload)
```

关键设计：

- **定时转换**：`SetTimer(d)` 注册保留事件 `TimerExpired`，`Advance(now)` 到期
  自动触发——冷却到期自动恢复，取代旧的手工清 `unavailableUntil`。
- **状态变更原子化，副作用在锁外执行**：`Fire` 内部只改状态；`Effect` 在
  machine 锁释放后执行，副作用里可以回调 `SetTimer` 而不死锁（有专门测试）。
- 未注册的 (From, Event) 组合被忽略并计数（`Ignored()`），可观测。
- Guard 返回 `(bool, error)`：false 表示"已是最新，忽略"；error 表示"合法
  但应拒绝"。参考实现见各实体的 `onXxx` 方法。

## 实体状态层（internal/entity）

### RAPI FSM（internal/entity/rapi.go）

状态：

- `healthy` — 正常服务
- `cooling` — 会话级冷却（429/5xx/网络错误），定时自动恢复
- `platform_failed` — 平台级冷却（quota/过载），指数退避自动恢复
- `invalidated` — 已持久化不可用 / 管理员禁用，只有 `Revalidate` 能离开

事件：`success` / `session_failure` / `platform_failure` / `invalidate` /
`revalidate` / `all_keys_soft` / `all_keys_hard`

关键行为：

- `PickAvailable()` 语义与旧 `PickAvailableKey` 一致：跳过冷却中/失效/禁用
  的 Key，全冷却返回 `nextAvail`。
- `AllKeysHard` 事件：所有 key 硬死（禁用/永久失效/过期）→ 持久化
  `available=false` + 原因，并失效 RAPI。**首次检测去重**：仅在 Healthy →
  Invalidated 的转换上持久化一次，返回 `true` 仅当发生了转换。
- `AllKeysSoft`：所有 key 都在冷却（瞬时）→ 短冷却，不落库、不失效。
- 指数退避：冷却时长按连续失败次数增长，`success` 清零。

### Key FSM（internal/entity/key.go）

状态：

- `healthy` — 可用
- `cooling` — 临时冷却（`failure_type=1`），自动恢复
- `platform_failed` — 平台级临时冷却
- `permanent_failed` — `failure_type=2`，只有 Success/Enable 离开
- `disabled` — `enabled=0`
- `expired` — 过 `ExpiresAt`

关键行为：

- **行级事实（disabled/expired/failure_type=2）通过 `Sync(row)` 从 DB 行同步**
  ——行治好（重新启用 / 探测成功后清 failure_type）→ 实体自动复活，无需事件。
  调用约定：`PickAvailableKey` 每次先 `Sync` 再 `Pickable`，与 DB 对账。
- 永久失败只在 Success / Enable 事件离开 `permanent_failed`。

### Platform FSM（internal/entity/platform.go）

状态：`healthy` / `unavailable`；事件：`detect_success` / `detect_failure`。

- 纯探测驱动 + 持久化，无冷却定时器。
- 探测成功（`/v1/models` 200）→ `OnDetectSuccess()` → `available=true` 落库；
  探测失败 → `OnDetectFailure()` → `available=false` 落库。
- 自环转换保证重复探测幂等且副作用仍执行。

### 失败分类（internal/entity/entity.go）

```go
ClassifyFailure(statusCode) → ScopeSystem (401) / ScopePlatform (402,403,409,423,451) / ScopeSession (其他)
```

- 401 → 系统级 → key 永久失败（换 key 可解）
- 402/403/409/423/451 → 平台级 → key 永久失败，**除非**命中可恢复计费错误
- 其他（429/5xx/网络）→ 会话级 → 临时冷却，自动恢复

### 可恢复计费错误（8-2 事故修复）

`IsRecoverableBillingError(body)` 按响应体关键字识别"充值后即可恢复"的计费错误：

- 中文：`积分不足`（京东 code 1058）、`余额不足`、`欠费`、`余额为0/零`
- 英文：`insufficient balance/quota`、`payment required`、`quota exhausted`、
  `exceeded your current quota`、`account balance`、`billing`

命中时，即使状态码是 403/402，也只标记 `failure_type=1`（临时）+ 短冷却，
额度恢复后自动回归链路——不再需要手动探测。

### 能力失配检测（key×model 能力黑名单）

`IsCapabilityMismatch(body)` 按响应体关键字识别"该 Key 无权访问此模型"类错误：
英文 `model not found` / `model does not exist` / `no such model` / `invalid
model` 等，中文 `模型不存在` / `无此模型` / `模型未开通` 等。

命中时（404/400/422/403）不再整 key 冷却，而是落库
`key_model_blocks(key_id, rapi_id, reason, expires_at)`——只禁用该 (key, model)
组合，key 继续服务其他模型。TTL 默认 24h（`capability_block_sec` 可配）到期自动
重试，请求或直测 2xx 自动解除；全池被 block 走硬死判定，原因明确
"所有 Key 无权访问此模型"。删除 key 时一并清理其全部 block。

### Store 接口

DB 写全部变成转换副作用：

```go
type Store interface {
    SetPlatformAvailable(id int64, available bool) error
    SetRAPIUnavailable(id int64, available bool, reason string) error
    MarkKeyPermanentFailure(keyID int64, reason string) error
    MarkKeyTemporaryFailure(keyID int64, reason string) error
    ClearKeyFailure(keyID int64) error
}
```

gateway / service 不再直接写 DB 状态列，只发事件；实体负责落库。锁序：
`Manager.mu → entity.mu → machine.mu → db.mu`（无环）。

## 协调器（internal/scheduler）

退化为薄层：持有 `map[int64]*entity.RAPI` / `*entity.Key` / `*entity.Platform`。

- 公开 API 原样保留（`PickAvailableKey` / `Mark*` / `RemoveKey` /
  `InvalidateRAPI` / `RevalidateRAPI` / `Snapshot` / `RecordRequest` / `Wait`），
  service / insights 零改动。
- `RemoveKey(keyID)`：key 删除时同步清理 `m.keys` 内存实体，避免残留。
- `Mark*` 系列只是实体事件的薄封装：`MarkFailure` → `Fire(session_failure)`
  ，`MarkKeyPermanentFailure` → `Fire(permanent_failure)`，以此类推。
- recovery loop 变薄：只调 `Advance(now)`，让定时转换自动生效。

## 编排层（internal/gateway）

- 持有 `entity.Store`（dbStore 实现），构造时注入 scheduler。
- `tryKeyForRAPI`：失败时调 `ClassifyFailure` + `IsRecoverableBillingError` +
  `IsCapabilityMismatch` 判定——能力失配记入 `key_model_blocks` 黑名单（组合级，
  不整 key 冷却）；其余走 scheduler 的 `MarkKey*`——状态流转与落库全部下沉到实体。
  循环内每轮重过滤被 block 的 key，全阻塞直接返回 `ErrAllKeysUnavailable`
  （避免 block 后空转重试打满上游）。
- `handleAllKeysUnavailable`：用 `entity.KeysHardDead(keys)` 区分硬死/软冷却，
  发 `all_keys_hard`（落库失效 + 通知）或 `all_keys_soft`（短冷却）。
- 探测（`RecoverUnhealthyRAPIs`、`probePlatform`）改用 `GetPlatformKeys` 中
  第一个可用 key（`FirstUsableKey`，语义同 `PickAvailableKey`），不再用可能
  为空的平台旧 `rapi.Token` 字段；探测结果驱动 Platform/RAPI 实体恢复。
- 探测循环提取为 `recoverRAPIs`，并暴露按平台（`RecoverPlatformRAPIs`）与按模型
  （`RecoverRAPIs`）收窄的入口——添加 Key / 白名单编辑后只探测受影响范围，
  探测不过的模型保持不可用（不再盲目乐观复活）。

## Key 生命周期管理

Key 的增删改走 `internal/service`（HTTP）→ `internal/db`（持久化）→
`internal/scheduler`（内存实体）三层，完备性约定：

- **添加**（POST）：`AddPlatformKey` 写入 `credential`，平台内轮换序号取
  `sort_order=max+1`，身份是 `token_hash`（`sha256(明文)[:16]`，空 token 落
  NULL 不参与唯一），`AUTOINCREMENT` 保证 id 永不复用（RAPI 白名单引用不会
  错指）；随后自动触发 `RecoverPlatformRAPIs`
  探测式恢复——此前因 key 全死而不可用的模型只有真正应答才复活。
- **更新**（PUT `?key_id=N`）：单 key 更新，token 留空 = 保留原值（不回传明文）。
- **整表替换**（平台弹窗保存）：`SetPlatformKeys` 先按 id（旧客户端按解密 token
  兜底）匹配保留身份，再按原 id 显式重插——避免 DELETE+INSERT 重建导致 RAPI
  `key_ids` 白名单失配；失败状态（`failure_type`/原因/时间）跨替换完整保留。
- **删除**（DELETE）：先 `DetachKeyFromRAPIs` 从所有 `rapi.key_ids` 剥离悬挂引用
  （否则池静默缩小且 `KeysHardDead(空池)` 回退平台 token，模型静默降级），再删行、
  调 `Manager.RemoveKey` 清内存实体；响应返回 `removed_from` 受影响模型列表。
- **直测一键恢复**（`POST /api/rapis/test` + `clear_on_success`）：模型页直测
  key×model，上游 2xx 时复用 `clearKeyFailureState()`——清 `failure_type`、
  `MarkKeySuccess` 立即重新入池、`RecoverRAPIs` 复活模型；与平台 key 探测共用同一
  恢复逻辑。

## 恢复路径一览

| 路径 | 触发 | 覆盖 key 级失败 |
|---|---|---|
| 启动扫描 `RecoverUnhealthyRAPIs` | 进程启动 | ✅（用真实 key 探测） |
| Dashboard「重试」`/api/rapis/restore` | 手动 | ✅ |
| Dashboard「重试全部」`/api/system/retry-unhealthy` | 手动 | ✅ |
| Dashboard 平台「恢复」`/api/platforms/restore` | 手动 | ✅（用真实 key） |
| key「探测」`/api/platforms/{id}/keys/{kid}/probe` | 手动 | ✅ |
| **直测一键恢复**（`/api/rapis/test` + `clear_on_success`） | 模型页直测 2xx | ✅（清失败标记 + 重新入池 + 复活模型） |
| **添加 Key 后自动重探测**（`RecoverPlatformRAPIs`） | POST/PUT key 后自动 | ✅（仅恢复真正应答的模型） |
| **白名单编辑后自动重探测**（`RecoverRAPIs`） | RAPI `key_ids` 保存后自动 | ✅ |
| 定时冷却 + 指数退避 | 运行时自动 | ✅（429/5xx/可恢复计费） |
| **key×model 能力黑名单**（`key_model_blocks` 表） | 请求时 404/403/400/422 命中
  "model not found" 类错误自动记录 | ✅（该 key 仅对此模型禁用，
  其余模型不受影响；24h TTL 到期自动重试；
  请求/直测 2xx 自动解除） |
| `Sync(row)` 行级对账 | 每次 Pick | ✅（重新启用/清 failure_type 即复活） |

## 测试覆盖

- `internal/fsm/fsm_test.go`：引擎（转换表、Guard、定时转换、副作用死锁）
- `internal/entity/entity_test.go`：RAPI/Key 全部转换、KeysHardDead、
  Platform FSM、计费错误判定、能力失配判定（`TestIsCapabilityMismatch`）
- `internal/entity/concurrency_test.go`：16 goroutine × 500 次混合事件无死锁
- `internal/db/db_test.go`：`DetachKeyFromRAPIs`（双引用剥离/单引用置空/幂等）、
  `key_model_blocks` 读写、analytics 时间戳回归（不可解析旧行不 500）
- `internal/scheduler/scheduler_test.go`：对外 API 行为回归
- `internal/gateway/gateway_test.go`：KeysHardDead 集成用例

## 未来方向

- 平台级不可用时的子 RAPI 级联失效/恢复（目前由 gateway 编排层负责通知）。
- `RecoverUnhealthyRAPIs` 增加周期性触发（如 15 分钟），不再依赖重启/手动。
- 把冷却/退避参数抽到 config，按平台粒度可调。
