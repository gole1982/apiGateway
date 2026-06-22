# API Gateway 设计文档

## 1. 概述

API Gateway 是一个面向个人或小组使用的轻量级多 LLM API 网关代理（RAPI 数百级、LAPI 数十级）。它将多个免费或限流的上游 API 通道聚合为单一 OpenAI 兼容的本地端点，通过 cost 排序选路、全错误吸收和被动冷却机制，尽可能让客户端请求获得响应。

### 架构分层

| 层次 | 职责 |
|------|------|
| **业务层** | 维持客户端 API 访问的可持续性——接受请求、路由匹配、返回响应或抽象错误 |
| **技术层** | 实现业务层目标——选路排序、包处理、故障转移、Token 管理 |

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

### 四层结构

```
Platform（上游服务商）
  ├── base_url, token, is_dynamic, token_command
  ├── enabled / available（管理员控制 + 系统控制）
  └── 1:N → RAPI

RAPI（具体模型端点）
  ├── alias, model（真实上游模型名）, platform_id
  ├── enabled / available
  ├── cost 配置: base_cost, high_cost
  ├── 阈值配置: rpm_limit / rph_limit / rpd_limit / tpm_limit / tph_limit / tpd_limit
  ├── 时段配置: time_period_rules (JSON)
  └── N:M → LAPI（通过 lapi_rapi_order）

LAPI（对外暴露的统一接口）
  ├── alias（客户端请求中的 model 字段）
  └── 绑定一组 RAPI

LAPI-RAPI 映射（lapi_rapi_order）
  ├── lapi_id, rapi_id
  └── order_index（默认排序权重，cost 相同时使用）
```

**Platform** 持有连接级信息——上游 API 地址、认证 Token、动态令牌刷新命令。同 Platform 下的多个 RAPI 共享这些信息。

**RAPI** 持有模型级信息——别名（alias）用于管理标识，模型名（model）是上游 API 实际识别的模型字符串。每个 RAPI 独立配置 cost 参数。

**LAPI** 是面向客户端的唯一入口。客户端在请求体中指定 `"model": "lapi-alias"`，系统据此查找 LAPI 并获取其绑定的 RAPI 链。

**cost 是 RAPI 自身的属性，与所属 LAPI 无关。** 同一个 RAPI 被多个 LAPI 引用时，其 cost 计算结果一致，选路排序复杂度为 O(Nr)。

### 数据库表

```
platform        — 上游服务商配置
rapi            — 模型端点配置 + cost 参数
lapi            — 对外别名
lapi_rapi_order — LAPI-RAPI 映射 + 排序
token_cache     — Token 缓存（platform_id 为主键）
```

以下表当前阶段不参与核心逻辑，保留表结构供后续计费和统计阶段使用：`rapi_metrics`、`request_trends`、`sessions`、`request_logs`、`log_events`。

---

## 3. 业务层

业务层对客户端暴露 OpenAI 兼容的 `/v1/chat/completions` 接口。职责边界：

**请求验证**：校验请求格式和必填字段。JSON 解析失败返回 400，`model` 字段缺失或为空返回 400。

**LAPI 匹配**：用 `model` 字段匹配 LAPI 别名。不存在则返回 401（`Unknown model`）。

**错误抽象**：客户端收到的错误一律网关格式，不暴露后端 RAPI 的详细信息（URL、alias、状态码）。只有当所有 RAPI 都失败时才返回错误，错误类型为 503 `no backends available`。

**请求转发**：调用技术层完成选路、包处理和故障转移，将上游响应透传给客户端。

---

## 4. 技术层

### 4.1 选路

**可用性三重判定**，必须同时满足：

1. `r.enabled = 1 AND r.available = 1`（RAPI 未被管理员或系统禁用）
2. `p.enabled = 1 AND p.available = 1`（所属 Platform 未被禁用）
3. 调度器 `unavailableUntil` 已过期（不在冷却期）

条件 1 和 2 在 SQL 查询中过滤，条件 3 在调度器中检查。

**cost 排序**：查出候选 RAPI 后，按当前 cost 升序排列。cost 相同则按 `order_index` 升序。

**选路遍历**：按排序结果遍历，选第一个满足可用性三重判定的 RAPI。

### 4.2 包处理

**URL 规范化**：`NormalizeToCompletionsURL(baseURL)` 将各种格式的上游地址统一拼接为 `…/v1/chat/completions`。

**模型名替换**：`replaceModelInBody(body, lapiAlias, rapiModel)` 将请求体中的 LAPI 别名替换为 RAPI 的真实模型名。同时处理 `"model":"alias"` 和 `"model": "alias"` 两种 JSON 格式。

**Token 注入**：`Authorization: Bearer {token}`，token 从缓存获取或执行命令获取。

**统一请求执行**：`doUpstreamRequest()` 封装上游 HTTP 请求的构建、日志记录和错误处理，返回 `(*http.Response, statusCode, errBody, error)`。流式和非流式处理器共用此方法，确保请求日志和错误处理一致。

### 4.3 故障转移

**全错误吸收**：上游返回任何非 2xx 响应，网关均不直接返回客户端，而是标记该 RAPI 不可用并尝试下一个。所有错误对客户端透明。

**冷却时间来源**（优先级从高到低）：
1. 上游响应头 `Retry-After`（秒数或 HTTP 日期）或 `X-RateLimit-Reset`（Unix 时间戳）
2. 指数退避：`DefaultCooldown × 2^(consecutiveFailures-1)`，封顶 `MaxCooldown`（默认 5s 起步，60s 封顶）

**请求体安全**：请求 body 在入口完整读入内存，所有重试复用同一副本，无内容丢失风险。

**流式故障转移边界**：`responseCommitted` 标记第一个数据块写入客户端的时刻。此前可透明切换 RAPI；此后无法撤回，只能结束当前响应。

**401 Token 刷新重试**：收到 401 后，先刷新 Token（`RefreshPlatformOnDemand`），用新 Token 重试同一 RAPI。仍失败则转移到下一个 RAPI。

**冷却机制**：每次失败设置 `unavailableUntil`，防止短时间内重复使用失败的 RAPI。`recoveryLoop` 定时清除过期的冷却状态并唤醒等待队列。

### 4.4 等待队列

每个 LAPI 拥有独立等待队列。当所有 RAPI 均不可用时，请求进入队列等待恢复，而非直接返回 503。

队列通过 `sync.Cond.Wait()` 阻塞，由 `recoveryLoop` 或其他请求的冷却结束触发 `Broadcast` 唤醒。队列满（默认 1024）返回 503。请求超时（默认 30s）时通过 generation 机制清空整个队列。

---

## 5. Cost 模型

### 5.1 运行时计数器

每个 RAPI 在内存中维护三个时间窗口的计数器（RAPI 级，跨所有 LAPI 聚合）：

| 窗口 | 计数 | 说明 |
|------|------|------|
| 1 分钟 | reqMinute, tokMinute | 当前分钟内的请求数/Token 数 |
| 1 小时 | reqHour, tokHour | 当前小时内的请求数/Token 数 |
| 24 小时 | reqDay, tokDay | 当前 24 小时内的请求数/Token 数 |

时间窗口推进时自动重置计数。Token 计数优先使用上游响应头中的实际值（`Openai-Usage` / `X-Token-Usage`），无响应头时退化为 `len(content)/4 + 1` 近似估算。

### 5.2 阈值

每个 RAPI 可配 6 个阈值：`rpm_limit`、`rph_limit`、`rpd_limit`、`tpm_limit`、`tph_limit`、`tpd_limit`。

阈值的作用不是阻止请求（这是旧设计），而是标记"超过后 cost 应该变高"。任一计数器超过对应阈值即触发 high_cost。

### 5.3 时段配置

每个 RAPI 可配一组时段规则，存储为 JSON：

```json
[
  {"start": "09:00", "end": "22:00", "cost": 10},
  {"start": "22:00", "end": "09:00", "cost": 3}
]
```

当前时间匹配某时段时，该时段的 cost 覆盖 base_cost。

### 5.4 cost 计算

```
computeCost(rapi):
  ① 如果当前时间在某个配置时段内 → 返回时段 cost
  ② 否则如果任一计数器超过对应阈值 → 返回 high_cost
  ③ 否则 → 返回 base_cost
```

优先级：时段 > 超限 > 基础。

选路排序按 cost 升序，cost 相同则按 `order_index` 升序。

---

## 6. Token 管理

Token 管理由 `DynamicTokenProvider` 按 Platform 粒度管理，同 Platform 下多个 RAPI 共享 Token。

**双重缓存**：内存缓存（`map[int64]*tokenEntry`，RWMutex，5min TTL）→ DB 缓存（`token_cache` 表，5min TTL）→ 获取新 Token。

**定时刷新**：`StartBackgroundRefresh` 在启动时为每个 `isDynamic=true` 的 Platform 启动后台 goroutine，按 `refresh_interval_sec` 定时执行 `token_command`。

**401 被动刷新**：收到 401 时调用 `RefreshPlatformOnDemand` 清除缓存并重新获取 Token，用新 Token 重试。

---

## 7. API 接口

### Proxy API（端口默认 13579）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /v1/chat/completions | OpenAI 兼容的 Chat Completions 代理 |
| GET | /status | 代理运行状态 |

### Dashboard REST API（端口默认 24680）

**Platform CRUD**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/platforms | 获取所有 Platform |
| POST | /api/platforms | 创建 Platform |
| PUT | /api/platforms | 更新 Platform |
| DELETE | /api/platforms?id={id} | 删除 Platform |
| POST | /api/platforms/toggle | 切换启用/禁用 |

**RAPI CRUD**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/rapis | 获取所有 RAPI（含 Platform 信息） |
| POST | /api/rapis | 创建 RAPI |
| PUT | /api/rapis | 更新 RAPI |
| DELETE | /api/rapis?id={id} | 删除 RAPI |
| POST | /api/rapis/toggle | 切换启用/禁用 |

**LAPI CRUD**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/lapis | 获取所有 LAPI |
| POST | /api/lapis | 创建 LAPI |
| PUT | /api/lapis | 更新 LAPI |
| DELETE | /api/lapis?id={id} | 删除 LAPI |

**LAPI-RAPI 编排**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/lapis/{id}/rapis | 获取关联 RAPI 列表 |
| PUT | /api/lapis/{id}/rapis | 设置 RAPI 链路和顺序 |

**统计与状态**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/stats/rapis | RAPI 统计 |
| GET | /api/stats/lapis | LAPI 统计 |
| GET | /api/status | 系统状态 |
| GET | /api/notify/stream | SSE 事件推送 |

---

## 8. 扩展预留

以下功能在当前阶段不实现核心逻辑，但架构中已预留扩展点：

**计费统计**：`rapi_metrics` 表结构已定义，`RecordRequest` 接口已存在。下一阶段接入持久化写入和聚合查询即可。

**日志系统**：`logger` 包已实现异步写入架构，网关代码中保留 `RecordRequestReceived`、`RecordRoutingDecision`、`RecordUpstreamSent`、`RecordUpstreamResponse`、`RecordClientResponse`、`RecordError` 等调用点。下一阶段完善持久化和查询。

**健康检查**：`healthcheck` 包已实现完整逻辑。接入后自动更新 `rapi.available` 和 `platform.available` 字段，使可用性判定中的"系统控制"条件生效。

---

## 9. 部署架构

### 初始化顺序

```
1. db.Init()                        — 初始化 SQLite，建表 + 迁移
2. config.Load()                    — 读取 proxy.cfg
3. notify.NewNotificationService()  — 创建进程内 pub/sub 通知服务
4. logStorage.InitTables()          — 初始化日志表
5. logger.Start()                   — 启动异步日志 Worker
6. sessionTracker 创建              — 绑定 ConnState 钩子
7. proxyGateway.StartBackgroundRefresh() — 启动令牌定时刷新
8. httpServer.ListenAndServe()      — 启动 Proxy 端口
9. webServer.ListenAndServe()       — 启动 Dashboard 端口
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
│   ├── gateway/gateway.go          # 代理核心（路由、包处理、流式转发）
│   ├── models/models.go            # 数据模型 + URL 规范化
│   ├── notify/notify.go            # 进程内 pub/sub + SSE
│   ├── scheduler/
│   │   └── scheduler.go            # 调度器（选路、故障转移、等待队列、cost 计算）
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

## 10. 已移除的旧设计

以下组件在重构中移除，理由：

**滑动窗口限流（ratelimit.go）**：本地模拟上游限流状态不可靠，Token 估算不准确，且配置负担重。改为被动监控——根据上游实际响应设置冷却定时器。

**Reservation 预约-提交-回滚语义**：与滑动窗口绑定，窗口移除后不再需要。请求发出前不做容量预留，失败直接标记冷却并转移。

**12 维 RateLimits 配置（RPM/RPH/RPD/TPM/TPH/TPD × Platform + RAPI）**：替换为 cost 模型中的 6 个阈值，仅用于 cost 计算，不阻止请求。
