# API Gateway 设计文档

## 1. 概述

API Gateway 是一个面向个人或小组使用的轻量级多 LLM API 网关代理（RAPI 数百级、LAPI 数十级）。它将多个不同协议格式的上游 API 通道聚合为统一的本地端点，支持 OpenAI / Anthropic / Gemini 三种 API 格式的客户端接入，通过 cost 排序选路、全错误吸收和被动冷却机制，尽可能让客户端请求获得响应。

### 架构分层

| 层次 | 职责 |
|------|------|
| **业务层** | 维持客户端 API 访问的可持续性——接受请求、路由匹配、返回响应或抽象错误 |
| **技术层** | 实现业务层目标——格式探测与转换、选路排序、包处理、故障转移、Token 管理 |

计费和日志功能作为下一阶段完善，当前架构预留扩展点。

### 技术栈

| 类别 | 选型 |
|------|------|
| 语言 | Go 1.21 |
| 数据库 | SQLite（modernc.org/sqlite，纯 Go） |
| 配置 | INI（gopkg.in/ini.v1） |
| 前端 | go:embed 嵌入单文件 HTML/JS SPA |
| 部署 | 单二进制，支持 Windows Service 和控制台模式 |

---

## 2. 实体模型

### 实体结构

```
Platform（上游服务商）
  ├── base_url, is_dynamic, token_command, notes
  ├── enabled / available（管理员控制 + 系统控制）
  ├── 1:N → PlatformKey（有序 API Key 列表）
  └── 1:N → RAPI

PlatformKey（平台下的 API Key）
  ├── platform_id, key_index（顺序，0 起步）
  └── token（API Key 字符串）

RAPI（具体模型端点）
  ├── alias, model（真实上游模型名）, platform_id
  ├── enabled / available
  ├── supported_formats（JSON数组，如 ["openai","anthropic"]）
  ├── cost 配置: base_cost, high_cost
  ├── 阈值配置: rpm_limit / rph_limit / rpd_limit / tpm_limit / tph_limit / tpd_limit
  ├── 时段配置: time_period_rules (JSON)
  └── N:M → LAPI（通过 lapi_rapi_order）

LAPI（对外暴露的统一接口）
  ├── alias（客户端请求中的 model 字段，强制小写存储）
  ├── enabled
  └── 绑定一组有序 RAPI

LAPI-RAPI 映射（lapi_rapi_order）
  ├── lapi_id, rapi_id
  └── order_index（默认排序权重，cost 相同时使用）
```

**Platform** 持有连接级信息——上游 API 地址、动态令牌刷新命令。Platform 本身不再直接存储单个 token，token 改由其下的有序 PlatformKey 列表管理。同 Platform 下的多个 RAPI 共享同一组 Key。

**PlatformKey** 是平台下的一条 API Key 记录。一个平台可以配置多个 Key，按 `key_index` 顺序排列。调用时按序选取第一个非冷却 Key，Key 级别的冷却独立计算，与 RAPI 冷却相互独立。兼容旧数据：平台迁移时将原有单 token 自动转为 index=0 的 PlatformKey。

**RAPI** 持有模型级信息——alias 用于管理标识，model 是上游 API 实际识别的模型字符串，supported_formats 记录该端点支持的 API 格式列表（通过格式探测自动填写或手动设置）。每个 RAPI 独立配置 cost 参数。

**LAPI** 是面向客户端的唯一入口。客户端在请求体中指定 `"model": "lapi-alias"`，系统据此查找 LAPI 并获取其绑定的 RAPI 链。alias 写入时强制转小写，匹配时对客户端输入也做 ToLower，实现大小写不敏感匹配。

**cost 是 RAPI 自身的属性，与所属 LAPI 无关。** 同一个 RAPI 被多个 LAPI 引用时，其 cost 计算结果一致，选路排序复杂度为 O(Nr)。

### 数据库表

```
platform          — 上游服务商配置（不再存 token 字段，保留字段供旧数据迁移）
platform_keys     — 平台 API Key 列表（platform_id + key_index + token）
rapi              — 模型端点配置 + cost 参数 + 支持格式
lapi              — 对外别名
lapi_rapi_order   — LAPI-RAPI 映射 + 排序
token_cache       — 动态 Token 缓存（platform_key_id 为主键）
```

以下表当前阶段不参与核心逻辑，保留表结构供后续计费和统计阶段使用：`rapi_metrics`、`request_trends`、`sessions`、`request_logs`、`log_events`。

---

## 3. 业务层

### 3.1 客户端入口

业务层暴露以下 URL 入口：

| 路径 | 说明 |
|------|------|
| `GET /v1/models` | 查询网关当前所有**已启用** LAPI，返回 OpenAI 兼容的模型列表格式（`{"object":"list","data":[{"id":"alias","object":"model",...}]}`）。客户端（Cursor、Claude Desktop、OpenAI SDK 等）可通过此接口自动发现可用模型。|
| `POST /v1/chat/completions` | OpenAI 格式请求入口 |
| `POST /v1/messages` | Anthropic 格式请求入口 |
| `POST /v1beta/…` | Gemini 格式请求入口 |

> **已知 Bug（见 §8.9）**：当前 `/v1/models` 返回全部 LAPI，未过滤 `enabled=false` 的条目，待修复。

### 3.2 请求处理

**请求验证**：JSON 解析失败返回 400。Gemini 格式的 model 可从 URL 路径提取（`/v1beta/models/{model}:generateContent`），提取后与请求体统一。

**LAPI 匹配**：将 model 字段转小写后匹配 LAPI alias。不存在则返回 401（`Unknown model`）。alias 在写入时已强制小写，查询 SQL 也使用 `LOWER()` 双重保证大小写不敏感。

**错误抽象**：客户端收到的错误一律为网关格式，不暴露后端 RAPI 的详细信息（URL、alias、状态码）。只有当所有 RAPI 都失败时才返回错误（503）。

**请求转发**：调用技术层完成选路、格式转换、包处理和故障转移，将上游响应转换为客户端格式后回写。

---

## 4. 技术层

### 4.1 选路

**可用性三重判定**，必须同时满足：

1. `r.enabled = 1 AND r.available = 1`（RAPI 未被管理员或系统禁用）
2. `p.enabled = 1 AND p.available = 1`（所属 Platform 未被禁用）
3. 调度器 `unavailableUntil` 已过期（不在冷却期）

条件 1 和 2 在 SQL 查询中过滤，条件 3 在调度器中检查。

**cost 排序**：查出候选 RAPI 后，按当前 cost 升序排列。cost 相同时，原生支持客户端 API 格式的 RAPI 优先（避免转换）；再相同则按 `order_index` 升序。

**选路遍历**：按排序结果遍历，选第一个满足可用性三重判定的 RAPI。

### 4.2 格式转换

**设计原则**：格式转换发生在**选定 RAPI 之后**，根据客户端格式和 RAPI 支持格式决定是否转换及如何转换，目标是信息损耗最小化。

**转换策略**（`pickTargetFormat`）：
- 若 RAPI 原生支持客户端格式 → 使用客户端格式，请求和响应零转换，直接透传
- 若不支持 → 按优先级选取 RAPI 支持列表中的第一个格式作为目标格式

**格式转换路径**（`clientFormat → targetFormat`，一步直接转换）：
- 请求：`ConvertRequest(body, clientFormat, targetFormat, rapiModel)`
- 响应：`ConvertResponse(body, targetFormat, clientFormat, lapiAlias)`
- 流式：`StreamConverter(from=targetFormat, to=clientFormat)`

支持的转换方向：OpenAI ↔ Anthropic ↔ Gemini（任意两格式之间，通过 OpenAI 作内部中继）。

> **当前代码偏差**：实现中在选路之前已将非 OpenAI 格式的请求统一转为 OpenAI 格式存为 `canonicalBody`，转发时再从 OpenAI 转为目标格式，实际执行了两步转换（`clientFormat → OpenAI → targetFormat`）。设计要求应修正为选路后的一步直接转换，以减少不必要的信息损耗。

**格式探测**（`apiformat.DetectFormats`）：
- 向上游并发发送各格式的探测请求
- 根据 HTTP 状态码判定支持性：2xx / 401 / 403 视为端点存在（支持），404 / 405 视为不支持
- 探测结果写入 `rapi.supported_formats` 字段
- 管理面板支持对单个 RAPI 或整个 Platform 触发探测

### 4.3 包处理

**URL 构建**：`BuildURL(baseURL, model, format)` 根据目标格式拼接正确的上游端点：
- OpenAI：`…/v1/chat/completions`
- Anthropic：`…/v1/messages`
- Gemini：`…/v1beta/models/{model}:generateContent`

**模型名替换**：转发前将请求体中的 model 字段替换为 RAPI 的真实模型名（在格式转换时通过 `replaceModelField` 完成）。

**请求体保留**：客户端 body 在请求入口完整读入内存，所有重试复用同一副本。body 在成功响应客户端或所有 RAPI 判定不可用（请求函数返回）之前始终保留，不会提前丢弃。

**Token 注入**：认证头按目标格式注入：
- OpenAI / Gemini：`Authorization: Bearer {token}`
- Anthropic：`x-api-key: {token}` + `anthropic-version: 2023-06-01`

**统一请求执行**：`doUpstreamRequest()` 封装上游 HTTP 请求的构建、日志记录和错误处理，流式和非流式处理器共用此方法。

### 4.4 多 Key 负载分担

**设计原则**：RAPI 实体不变（仍由 platform + model 唯一标识）。负载分担发生在**选定 RAPI 之后、发起上游请求之前**，体现为对该 RAPI 所属平台的 Key 列表执行顺序选取。

**Key 选取流程**（对每次上游调用）：

```
platformKeys = 按 key_index 顺序加载该平台的所有 Key
for key in platformKeys:
    if key 不在冷却期:
        使用该 key 发起请求
        if 响应为 429 / 503（受限）:
            对该 key 设置冷却（Retry-After 或指数退避）
            continue → 尝试下一个 key
        if 响应为 401:
            对该 key 刷新 token 并重试一次
            if 仍 401:
                对该 key 设置冷却
                continue → 尝试下一个 key
        if 2xx:
            return 成功
        其他错误（网络、5xx）:
            对该 key 设置冷却
            continue → 尝试下一个 key

所有 key 均已冷却 → MarkFailure(rapiID) → 进入 RAPI 级故障转移
```

**两级冷却**：

| 级别 | 冷却对象 | 触发条件 | 影响范围 |
|------|----------|----------|----------|
| Key 级 | `platform_key_id` | 该 Key 返回 429/503/401 | 仅跳过该 Key，同平台其他 Key 不受影响 |
| RAPI 级 | `rapi_id` | 平台所有 Key 均已冷却 | 该 RAPI 在调度器中不可用，触发 LAPI 级故障转移 |

**冷却时间计算**（Key 级和 RAPI 级共用同一策略）：
1. 优先使用上游响应头 `Retry-After` 或 `X-RateLimit-Reset` 指定的时间
2. 否则使用指数退避：`DefaultCooldown × 2^(consecutiveFailures-1)`，封顶 `MaxCooldown`（默认 5s 起步，60s 封顶）

**冷却状态存储**：Key 级冷却状态存储在调度器内存中（`platformKeyState` map，以 `platform_key_id` 为键），与 RAPI 级冷却的 `rapiState` 并列管理，均受 `recoveryLoop` 定时清理。

**用量计数**：计数器以 RAPI 为粒度聚合（不区分哪个 Key 发出的请求），保持与 cost 模型的一致性。

### 4.5 故障转移

**全错误吸收**：上游返回任何非 2xx 响应，网关均不直接返回客户端，先在 Key 级重试（见 §4.4），Key 级全部失败后才标记 RAPI 不可用并尝试下一个 RAPI。所有错误对客户端透明。

**流式故障转移边界**：`responseCommitted` 标记第一个数据块写入客户端的时刻。此前可透明切换 Key 或 RAPI；此后无法撤回，只能结束当前响应。

**冷却机制**：RAPI 级每次失败设置 `unavailableUntil`，防止短时间内重复使用失败的 RAPI。`recoveryLoop` 定时清除过期的冷却状态（Key 级和 RAPI 级）并唤醒等待队列。

### 4.6 等待队列

每个 LAPI 拥有独立等待队列。当所有 RAPI 均不可用（即每个 RAPI 的平台所有 Key 均处于冷却期）时，请求进入队列等待恢复，而非直接返回 503。

队列通过 `sync.Cond.Wait()` 阻塞，由 `recoveryLoop` 或其他请求的冷却结束触发 `Broadcast` 唤醒。队列满（默认 1024）返回 503。请求超时（默认 30s）时通过 generation 机制清空整个队列。

---

## 5. Cost 模型

### 5.1 运行时计数器

每个 RAPI 在内存中维护三个时间窗口的计数器（RAPI 级，跨所有 LAPI 聚合）：

| 窗口 | 计数 | 说明 |
|------|------|------|
| 1 分钟 | reqs, toks | 当前分钟内的请求数/Token 数 |
| 1 小时 | reqs, toks | 当前小时内的请求数/Token 数 |
| 24 小时 | reqs, toks | 当前 24 小时内的请求数/Token 数 |

时间窗口推进时自动重置计数。Token 计数优先使用上游响应头中的实际值（`Openai-Usage` / `X-Token-Usage`），无响应头时退化为 `len(content)/4 + 1` 近似估算。

> **注意**：运行时计数器存储在全局变量（`var counters = make(map[int64]*rapiCounters)`），当前与 Manager 实例未解耦，多 Manager 实例场景下存在数据竞争风险。

### 5.2 阈值

每个 RAPI 可配 6 个阈值：`rpm_limit`、`rph_limit`、`rpd_limit`、`tpm_limit`、`tph_limit`、`tpd_limit`。

阈值的作用不是阻止请求，而是标记"超过后 cost 应该变高"。任一计数器超过对应阈值即触发 high_cost。

### 5.3 时段配置

每个 RAPI 可配一组时段规则，存储为 JSON：

```json
[
  {"start": "09:00", "end": "22:00", "cost": 10},
  {"start": "22:00", "end": "09:00", "cost": 3}
]
```

支持跨午夜时段（start > end 时视为隔夜）。

### 5.4 cost 计算

```
computeCost(rapi):
  ① 如果当前时间在某个配置时段内 → 返回时段 cost
  ② 否则如果任一计数器超过对应阈值 → 返回 high_cost
  ③ 否则 → 返回 base_cost
```

优先级：时段 > 超限 > 基础。

选路排序：cost 升序 → 原生支持客户端格式（格式偏好） → order_index 升序。

---

## 6. Token 管理

Token 管理以 **PlatformKey** 为粒度，同 Platform 下多个 RAPI 共享同一组 Key。

### 6.1 静态 Key（is_dynamic = false）

平台下的每条 PlatformKey 直接存储 token 字符串，无需刷新。调用时从 `platform_keys` 表按 `key_index` 顺序加载，选取第一个非冷却 Key 直接使用。

### 6.2 动态 Key（is_dynamic = true）

动态 Key 模式下，`token_command` 配置在 Platform 级别。所有 Key 共享同一刷新命令，刷新产生的 token 写入 index=0 的 PlatformKey（动态模式下通常只配一个 Key）。

**双重缓存**：内存缓存（`map[platform_key_id]*tokenEntry`，RWMutex，5min TTL）→ DB 缓存（`token_cache` 表，5min TTL）→ 执行命令获取新 Token。

**定时刷新**：`StartBackgroundRefresh` 在启动时为每个 `isDynamic=true` 的 Platform 启动后台 goroutine，按 `refresh_interval_sec` 定时执行 `token_command`，刷新结果写入对应 PlatformKey 的缓存和数据库。

**401 被动刷新**：收到 401 时对当前 Key 执行 `token_command` 获取新 Token（不经缓存），用新 Token 重试该 Key 一次。仍失败则对该 Key 设置冷却，尝试下一个 Key。

**token_command 格式**：
- 普通命令：`cmd /c <command>`（通过 Windows cmd 执行）
- 带引号路径：`"C:\path\program.exe" arg1 arg2`（直接执行）

命令输出取第一行非 JSON、非空白行作为 Token 值。

---

## 7. API 接口

### Proxy API（端口默认 13579）

| 方法 | 路径 | 客户端格式 | 说明 |
|------|------|-----------|------|
| POST | /v1/chat/completions | OpenAI | Chat Completions 代理 |
| POST | /v1/messages | Anthropic | Messages API 代理 |
| POST | /v1beta/… | Gemini | generateContent 代理 |
| GET | /v1/models | OpenAI | 列出所有已启用 LAPI |
| GET | /status | — | 代理运行状态 |

### Dashboard REST API（端口默认 24680）

**Platform CRUD**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/platforms | 获取所有 Platform |
| POST | /api/platforms | 创建 Platform |
| PUT | /api/platforms | 更新 Platform |
| DELETE | /api/platforms?id={id} | 删除 Platform |
| POST | /api/platforms/toggle | 切换启用/禁用 |
| POST | /api/platforms/fetch-models | 探测远程模型列表（返回模型名数组，不创建 RAPI） |
| POST | /api/platforms/detect-formats | 探测平台支持的 API 格式，并应用到所有关联 RAPI |

> **平台模型探测与批量导入流程**（两步操作）：
> 1. 调用 `fetch-models` → 向平台 `/v1/models` 发 GET 请求 → 返回模型名列表（使用 index=0 的 Key 鉴权）
> 2. 前端展示列表供管理员勾选 → 调用 `POST /api/rapis/batch`（传入 `platform_id` + 勾选的模型名数组）→ 后端同时执行平台级格式探测，为每个模型创建 RAPI 并写入 `supported_formats`

**Platform Key 管理**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/platforms/{id}/keys | 获取平台 Key 列表（token 脱敏显示） |
| PUT | /api/platforms/{id}/keys | 全量替换 Key 列表（按数组顺序写入 key_index） |
| POST | /api/platforms/{id}/keys | 追加一条 Key |
| DELETE | /api/platforms/{id}/keys/{key_index} | 删除指定 Key |

**RAPI CRUD**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/rapis | 获取所有 RAPI（含 Platform 信息） |
| POST | /api/rapis | 创建 RAPI |
| PUT | /api/rapis | 更新 RAPI |
| DELETE | /api/rapis?id={id} | 删除 RAPI |
| POST | /api/rapis/toggle | 切换启用/禁用 |
| POST | /api/rapis/detect-formats | 探测单个 RAPI 支持格式 |
| POST | /api/rapis/batch | 批量创建 RAPI（含平台级格式探测） |

**LAPI CRUD**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/lapis | 获取所有 LAPI |
| POST | /api/lapis | 创建 LAPI（alias 自动转小写） |
| PUT | /api/lapis | 更新 LAPI（alias 自动转小写） |
| DELETE | /api/lapis?id={id} | 删除 LAPI |
| POST | /api/lapis/toggle | 切换启用/禁用 |
| GET | /api/lapis/{id}/rapis | 获取关联 RAPI 列表 |
| PUT | /api/lapis/{id}/rapis | 设置 RAPI 链路和顺序 |

**统计与状态**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/stats/rapis | RAPI 统计（含成功率、延迟、失败分类） |
| GET | /api/stats/lapis | LAPI 统计（含关联 RAPI 子统计） |
| GET | /api/status | 系统状态（含请求趋势） |
| GET | /api/notify/stream | SSE 实时事件推送 |
| GET | /api/logs/sessions | Session 列表 |
| GET | /api/logs/requests | 请求日志列表（支持过滤） |
| GET | /api/logs/request/{id} | 请求详情（含事件链） |
| DELETE | /api/logs/clear | 清空所有日志 |

---

## 8. 已知 Bug 与设计偏差

以下问题已通过代码审查确认，待修复。

### 8.1 格式转换路径偏差（设计偏差）

**问题**：当前实现在选路**之前**将所有非 OpenAI 格式请求统一转为 OpenAI（`canonicalBody`），选路之后再从 OpenAI 转为目标格式，实际执行了两步转换（`clientFormat → OpenAI → targetFormat`）。

**影响**：Anthropic 客户端对 Anthropic RAPI 的请求会经历双次无意义转换；任何两步转换都可能引入额外的信息损耗。

**预期**：应为一步直接转换——选定 RAPI 后，若客户端格式 == RAPI 格式则透传，否则 `clientFormat → targetFormat` 一步完成。

### 8.2 RequestID 双重生成导致日志链断裂

**问题**：`gateway.go` 在第103行生成 `requestID`，`RecordRequestReceived` 方法内部又自行调用 `NewRequestID()` 生成另一个 ID，导致 `REQUEST_RECEIVED` 事件的 RequestID 与同次请求的所有其他事件 RequestID 不同，日志无法关联。

**影响**：请求日志查询中 `REQUEST_RECEIVED` 事件孤立，无法拼出完整的请求生命周期链路。

### 8.3 流式处理中 sync.WaitGroup 未等待

**问题**：`handleStreamingRequest` 中，成功路径（第309-345行）和401重试成功路径（第388-425行）都创建了 `wg` 并 `wg.Add(1)`，但均未调用 `wg.Wait()`。

**影响**：goroutine 在函数返回后仍在读取已关闭的 `resp.Body`，产生数据竞争；`resp.Body` 提前被 GC 的场景下可能 panic。

### 8.4 流式转换错误时 response body 未 drain

**问题**：流式路径中 `converter.Run()` 出错时，直接 `continue` 进入下一次重试，未对 `resp.Body` 执行 `io.Copy(io.Discard, resp.Body)` 再关闭。

**影响**：HTTP keep-alive 连接无法复用，每次转换错误都会静默丢弃一条连接。

### 8.5 rapi_metrics 永远为空

**问题**：`gateway.go` 中只调用了内存计数器 `g.scheduler.RecordRequest(rapiID, tokensUsed)`，从未调用 `db.RecordRequest(rapiID, lapiID, statusCode, latencyMs, tokensUsed)`。

**影响**：Dashboard 的 RAPI/LAPI 统计数据（成功率、延迟、失败分类）始终显示零，统计功能实际上是无效的。

### 8.6 Gemini 请求 LAPI 查询时序错误

**问题**：`GetLAPIByAlias` 调用（第154行）发生在从 URL 提取 Gemini model 名称（第140-144行）之前。Gemini 客户端仅在 URL 中携带 model 名、body 中 model 为空时，第154行会以空字符串查库必然失败。

**影响**：纯 URL 携带模型名的 Gemini 请求（`/v1beta/models/{model}:generateContent`，body 无 model 字段）一律返回 401 Unknown model，即使 LAPI 配置正确也无法路由。

### 8.7 SSE 流式 Anthropic 转换输出格式错误

**问题**：`openaiChunkToAnthropic` 在 finish chunk 处将 `message_delta` 和 `message_stop` 两个 SSE 事件拼接为一个字符串后传入 `writeSSE()`，输出的 `data:` 字段包含原始换行和第二个 `data:` 前缀，格式不符合 SSE 规范。

**影响**：Anthropic SDK 客户端接收到格式错误的 SSE 帧，可能触发解析错误或丢失 `message_stop` 事件。

### 8.8 SupportsAPIFormat 字符串匹配不可靠

**问题**：`models.go` 中 `SupportsAPIFormat` 使用 `strings.Contains(r.SupportedFormats, `"`+format+`"`)` 做子串匹配，若 `supported_formats` 中存在包含其他格式名的自定义字符串，会产生误判。

**影响**：格式判断可能给出错误结果，导致不必要的格式转换或选路偏差。应改用 `apiformat.ParseFormats` + `apiformat.SupportsFormat` 做正确的 JSON 数组解析。

### 8.9 /v1/models 返回已禁用的 LAPI

**问题**：`/v1/models` 端点返回所有 LAPI，未过滤 `enabled = false` 的条目。

**影响**：客户端调用 `/v1/models` 发现某模型后实际请求该模型，会收到 503 "Model is disabled"，造成混淆。

### 8.10 log_events 孤儿数据清理缺失

**问题**：`CleanupOldRecords` 按 `maxRecords` 删除 `request_logs` 行时，对应的 `log_events` 行未同步删除，且 `log_events.request_id` 外键无 `ON DELETE CASCADE`，在外键启用的 SQLite 模式下会导致删除失败。

**影响**：日志清理失败，或 `log_events` 积累大量无法访问的孤儿行，无限膨胀。

---

## 9. 扩展预留

以下功能在当前阶段不实现核心逻辑，但架构中已预留扩展点：

**计费统计**：`rapi_metrics` 表结构已定义，`db.RecordRequest` 接口已存在（见 Bug 8.5）。下一阶段在网关成功/失败路径中调用该接口即可启用持久化统计。

**日志系统**：`logger` 包已实现异步写入架构（Session + RequestLog + LogEvent 三级），网关代码中的调用点均已就位，但因 Bug 8.2 导致日志链断裂。修复 8.2 后，日志系统可完整工作。

**健康检查**：`healthcheck` 包已完整实现——定时向每个启用的 RAPI 发送探测请求，根据返回码更新 `available` 字段并通过 Notify 推送变更。当前未在启动序列中调用，接入后可激活可用性判定中的"系统控制"条件。

---

## 10. 部署架构

### 初始化顺序

```
1. db.Init()                             — 初始化 SQLite，建表 + 迁移
2. config.Load()                         — 读取 proxy.cfg
3. notify.NewNotificationService()       — 创建进程内 pub/sub 通知服务
4. logStorage.InitTables()               — 初始化日志表
5. logger.Start()                        — 启动异步日志 Worker
6. sessionTracker 创建                   — 绑定 ConnState 钩子
7. proxyGateway.StartBackgroundRefresh() — 启动令牌定时刷新
8. httpServer.ListenAndServe()           — 启动 Proxy 端口
9. webServer.ListenAndServe()            — 启动 Dashboard 端口
```

### 配置（proxy.cfg）

```ini
proxy_port = 13579          # Proxy 端口
web_port = 24680            # Dashboard 端口
refresh_interval_sec = 600  # 令牌刷新间隔（秒）
```

### 部署文件

```
gateway.exe          # 主服务（单二进制）
proxy.cfg            # 配置文件
gateway.db           # SQLite 数据库（首次运行自动创建）
```

### 项目目录

```
apiGateway/
├── cmd/gateway/main.go             # 主入口（控制台/Service）
├── internal/
│   ├── config/config.go            # 配置读取
│   ├── db/db.go                    # SQLite 数据层
│   ├── gateway/gateway.go          # 代理核心（路由、格式转换、流式转发）
│   ├── models/models.go            # 数据模型
│   ├── notify/notify.go            # 进程内 pub/sub + SSE
│   ├── apiformat/                  # API 格式转换与探测
│   │   ├── format.go               # APIFormat 类型定义
│   │   ├── url.go                  # URL 构建与格式路径检测
│   │   ├── convert_request.go      # 请求格式转换
│   │   ├── convert_response.go     # 响应格式转换
│   │   ├── convert_stream.go       # 流式 SSE 格式转换
│   │   └── detect.go               # 格式探测
│   ├── scheduler/scheduler.go      # 调度器（选路、故障转移、等待队列、cost 计算）
│   ├── tokenrefresher/refresher.go # Token 管理（缓存、定时刷新、被动刷新）
│   ├── logger/                     # 异步日志系统
│   ├── service/service.go          # 服务编排 + Dashboard REST API
│   └── healthcheck/                # 健康检查（已实现，待接入）
├── proxy.cfg
└── go.mod / go.sum
```

### HTTP Client 配置

60s 总超时，`MaxIdleConns=100`，`IdleConnTimeout=90s`，`TLSHandshakeTimeout=10s`。

---

## 11. 已移除的旧设计

**滑动窗口限流（ratelimit.go）**：本地模拟上游限流状态不可靠，Token 估算不准确，且配置负担重。改为被动监控——根据上游实际响应设置冷却定时器。

**Reservation 预约-提交-回滚语义**：与滑动窗口绑定，窗口移除后不再需要。请求发出前不做容量预留，失败直接标记冷却并转移。

**12 维 RateLimits 配置（RPM/RPH/RPD/TPM/TPH/TPD × Platform + RAPI）**：替换为 cost 模型中的 6 个阈值（仅 RAPI 级），仅用于 cost 计算，不阻止请求。
