# API Gateway 请求日志追踪系统设计文档

## 1. 概述

### 1.1 背景
API Gateway 作为代理层，需要记录每一次请求的完整处理流程，以支持：
- **客户投诉溯源**：追踪特定客户端的所有请求记录
- **API厂商禁用溯源**：查找使用特定 rapi 的所有请求
- **中间故障排查**：分析请求失败原因、重试次数、fallback情况

### 1.2 目标
设计一个完整的请求日志系统，记录从客户端请求到最终响应的全流程，支持实时查看和历史查询导出。

## 2. 需求分析

### 2.1 功能需求
| 需求 | 描述 |
|------|------|
| 全流程记录 | 记录请求接收、路由决策、转发、响应、返回客户端的每个阶段 |
| 会话分组 | 按TCP连接（会话）分组显示日志，类似Wireshark的流量分组 |
| Key脱敏 | Authorization等敏感key脱敏为：前4字节+*+后4字节 |
| 实时查看 | Dashboard新增日志标签页，实时显示请求日志 |
| 历史查询 | 支持按时间范围、会话ID、请求ID、lapi、rapi、状态等过滤 |
| 数据导出 | 支持导出为CSV、JSON格式 |
| 异步写入 | 日志写入采用异步队列，不影响转发效率 |

### 2.2 非功能需求
| 需求 | 目标值 |
|------|--------|
| 日志写入延迟 | < 1ms（异步队列推入） |
| 队列容量 | 1000条事件 |
| 数据保留 | 30天或100000条记录 |
| 并发安全 | 支持100并发请求不丢失日志 |

## 3. 架构设计

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        HTTP Request                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Session Tracker                               │
│  - ConnState 钩子追踪 TCP 连接                                   │
│  - 生成 Session ID                                               │
│  - 注入 Session ID 到 Request Context                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Gateway Handler                               │
│  - 处理请求                                                       │
│  - 调用 Logger.RecordEvent() 记录各阶段事件                      │
│    (不等待写入，异步推入队列)                                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Logger Module (异步)                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │ Event Queue  │───▶│ Log Worker   │───▶│   Database   │       │
│  │  (channel)   │    │ (background) │    │   (SQLite)   │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                                                                   │
│  事件类型：                                                        │
│  - REQUEST_RECEIVED: 客户端请求到达                               │
│  - ROUTING_DECISION: 路由决策完成                                 │
│  - UPSTREAM_SENT: 转发到 rapi                                     │
│  - UPSTREAM_RESPONSE: rapi 返回响应                               │
│  - CLIENT_RESPONSE: 返回给客户端                                  │
│  - ERROR: 错误发生                                                │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 会话定义
- **会话 (Session)** = 同一个客户端 TCP 连接上的所有请求序列
- **请求 (Request)** = 单次 HTTP 请求及其完整处理过程

会话识别机制：
- 通过 `http.Server.ConnState` 钩子追踪 TCP 连接生命周期
- 连接建立时生成唯一 Session ID
- 每个请求从 Context 获取 Session ID

## 4. 数据流设计

### 4.1 事件记录流程

```
时间线    事件                    记录内容
────────────────────────────────────────────────────────────────────
T0      REQUEST_RECEIVED        │ - Request ID (生成)
        (客户端请求到达)          │ - Session ID (从Context获取)
                                │ - 客户端IP、Port
                                │ - 请求Method、Path、Headers、Body
                                │ - 时间戳
────────────────────────────────────────────────────────────────────
T1      ROUTING_DECISION        │ - Request ID
        (路由决策完成)            │ - lapi_alias
                                │ - matched_rapis (候选列表)
                                │ - 时间戳
────────────────────────────────────────────────────────────────────
T2      UPSTREAM_SENT           │ - Request ID
        (转发到rapi)             │ - selected_rapi
        (每次尝试都记录)          │ - upstream_url
                                │ - upstream_headers (key脱敏)
                                │ - upstream_body (改写后)
                                │ - retry_count (第几次尝试)
                                │ - 时间戳
────────────────────────────────────────────────────────────────────
T3      UPSTREAM_RESPONSE       │ - Request ID
        (rapi返回)               │ - response_status
        (每次尝试都记录)          │ - response_headers
                                │ - response_body (截断到50KB)
                                │ - latency_ms
                                │ - tokens_used
                                │ - 时间戳
────────────────────────────────────────────────────────────────────
T4      CLIENT_RESPONSE         │ - Request ID
        (返回给客户端)            │ - 最终状态
                                │ - 总耗时
                                │ - fallback_used
                                │ - 时间戳
────────────────────────────────────────────────────────────────────
T5      ERROR                   │ - Request ID
        (如有错误)               │ - error_message
                                │ - 发生阶段
                                │ - 时间戳
────────────────────────────────────────────────────────────────────
```

### 4.2 数据聚合
- 所有事件按 Request ID 聚合成一条完整的请求日志记录
- 写入 `request_logs` 表

## 5. 数据库设计

### 5.1 表结构

```sql
-- 会话表
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,           -- Session ID (UUID)
    client_ip TEXT,                -- 客户端IP
    client_port INTEGER,           -- 客户端端口
    started_at DATETIME,           -- 会话开始时间
    ended_at DATETIME,             -- 会话结束时间
    total_requests INTEGER DEFAULT 0,  -- 总请求数
    last_request_at DATETIME       -- 最后请求时间
);

-- 请求日志表
CREATE TABLE request_logs (
    id TEXT PRIMARY KEY,           -- Request ID (UUID)
    session_id TEXT,               -- 关联会话ID
    timestamp DATETIME,            -- 请求开始时间
    
    -- 客户端请求
    client_ip TEXT,
    request_method TEXT,
    request_path TEXT,
    request_headers TEXT,          -- JSON格式
    request_body TEXT,             -- 原始请求体
    
    -- 路由决策
    lapi_alias TEXT,               -- 匹配的lapi
    matched_rapis TEXT,            -- JSON数组：候选rapi列表
    selected_rapi TEXT,            -- 最终使用的rapi
    
    -- 转发详情
    upstream_url TEXT,
    upstream_headers TEXT,         -- JSON格式（key已脱敏）
    upstream_body TEXT,            -- 改写后的请求体
    
    -- 响应信息
    response_status INTEGER,
    response_headers TEXT,
    response_body TEXT,            -- 截断到50KB
    latency_ms INTEGER,
    tokens_used INTEGER,
    
    -- 错误记录
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    fallback_used BOOLEAN DEFAULT FALSE,
    
    -- 状态
    status TEXT DEFAULT 'pending', -- pending, success, failed
    completed_at DATETIME          -- 请求完成时间
);

-- 事件表（可选，用于详细追踪）
CREATE TABLE log_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT,
    event_type TEXT,               -- 事件类型
    timestamp DATETIME,
    data TEXT,                     -- JSON格式事件数据
    FOREIGN KEY (request_id) REFERENCES request_logs(id)
);

-- 索引
CREATE INDEX idx_sessions_client ON sessions(client_ip);
CREATE INDEX idx_sessions_started ON sessions(started_at);
CREATE INDEX idx_logs_session ON request_logs(session_id);
CREATE INDEX idx_logs_timestamp ON request_logs(timestamp);
CREATE INDEX idx_logs_lapi ON request_logs(lapi_alias);
CREATE INDEX idx_logs_rapi ON request_logs(selected_rapi);
CREATE INDEX idx_logs_status ON request_logs(status);
```

## 6. 模块设计

### 6.1 文件结构

```
internal/logger/
├── logger.go          # 主模块，事件队列和worker
├── session.go         # Session追踪器
├── events.go          # 事件类型定义
├── storage.go         # 数据库存储
├── sanitize.go        # Key脱敏处理
└── config.go          # 配置定义
```

### 6.2 核心接口

```go
// logger.go
type Logger struct {
    eventQueue chan LogEvent      // 事件队列（容量1000）
    worker     *LogWorker         // 后台worker
    storage    *LogStorage        // 数据库存储
    config     LogConfig          // 配置
}

func NewLogger(db *db.DB, config LogConfig) *Logger
func (l *Logger) Start(ctx context.Context)        // 启动worker
func (l *Logger) RecordEvent(event LogEvent)       // 异步记录事件（不阻塞）
func (l *Logger) Stop()                            // 停止worker
func (l *Logger) StartCleanup(ctx context.Context) // 启动自动清理

// session.go
type SessionTracker struct {
    sessions map[net.Conn]*Session  // 连接->会话映射
    mu       sync.RWMutex
    logger   *Logger
}

type Session struct {
    ID           string
    ClientIP     string
    ClientPort   int
    StartedAt    time.Time
    RequestCount int
}

func NewSessionTracker(logger *Logger) *SessionTracker
func (t *SessionTracker) OnConnState(conn net.Conn, state http.ConnState)  // ConnState钩子
func (t *SessionTracker) GetSessionID(r *http.Request) string              // 从请求获取SessionID
func (t *SessionTracker) InjectSessionID(r *http.Request) string           // 注入SessionID到Context

// events.go
type LogEvent struct {
    RequestID   string
    SessionID   string
    EventType   EventType
    Timestamp   time.Time
    Data        map[string]interface{}
}

type EventType int
const (
    REQUEST_RECEIVED EventType = iota
    ROUTING_DECISION
    UPSTREAM_SENT
    UPSTREAM_RESPONSE
    CLIENT_RESPONSE
    ERROR
)

// sanitize.go
func SanitizeKey(key string) string  // 前4字节+*+后4字节
func SanitizeHeaders(headers http.Header) http.Header

// storage.go
type LogStorage struct {
    db *db.DB
}

func (s *LogStorage) SaveSession(session *Session) error
func (s *LogStorage) SaveRequestLog(log *RequestLog) error
func (s *LogStorage) AppendEvent(requestID string, event *LogEvent) error
func (s *LogStorage) GetSessions(filter SessionFilter) ([]Session, error)
func (s *LogStorage) GetRequestLogs(filter RequestLogFilter) ([]RequestLog, error)
func (s *LogStorage) GetRequestDetail(requestID string) (*RequestLog, []LogEvent, error)
func (s *LogStorage) CleanupOldRecords(maxAgeDays int, maxRecords int) error

// config.go
type LogConfig struct {
    QueueCapacity   int  // 队列容量（默认1000）
    MaxAgeDays      int  // 保留天数（默认30）
    MaxRecords      int  // 最大记录数（默认100000）
    CleanupInterval int  // 清理间隔小时（默认24）
    MaxBodySize     int  // 最大Body大小KB（默认50）
    BatchSize       int  // 批量写入大小（默认100）
    BatchInterval   int  // 批量写入间隔秒（默认1）
}
```

## 7. UI设计

### 7.1 日志标签页布局

```
┌─────────────────────────────────────────────────────────────────┐
│  [Dashboard] [编排] [录入] [日志]                    Proxy: ✓   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ 搜索过滤栏                                                   ││
│  │ [时间范围: 最近1小时 ▼] [会话ID/请求ID搜索...] [状态: 全部 ▼]││
│  │ [lapi: 全部 ▼] [rapi: 全部 ▼] [刷新]                        ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ 会话列表                                                     ││
│  │ # │ 时间       │ 会话ID    │ 客户端IP    │ 请求数 │ 状态    ││
│  │ 1 │ 10:23:45   │ sess-abc  │ 192.168.1.1 │ 3      │ ✓      ││
│  │ 2 │ 10:24:12   │ sess-def  │ 192.168.1.2 │ 1      │ ✗      ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ 请求详情（树状展示）                                          ││
│  │ 请求 req-123 (sess-abc)                                      ││
│  │ ├─ REQUEST_RECEIVED                                          ││
│  │ │  • 时间: 10:23:45.123                                      ││
│  │ │  • Method: POST, Path: /v1/chat/completions               ││
│  │ │  • Headers: {...} [展开]                                   ││
│  │ │  • Body: {"model":"gpt-4",...} [展开]                     ││
│  │ ├─ ROUTING_DECISION                                          ││
│  │ │  • lapi: my-gpt                                            ││
│  │ │  • matched_rapis: [rapi-A, rapi-B]                        ││
│  │ ├─ UPSTREAM_SENT (尝试1: rapi-A)                             ││
│  │ │  • URL: https://api.openai.com/...                        ││
│  │ │  • Headers: Authorization: Bearer sk-****abcd             ││
│  │ ├─ UPSTREAM_RESPONSE (尝试1)                                 ││
│  │ │  • Status: 429 Too Many Requests                          ││
│  │ ├─ UPSTREAM_SENT (尝试2: rapi-B) [fallback]                  ││
│  │ ├─ UPSTREAM_RESPONSE (尝试2)                                 ││
│  │ │  • Status: 200 OK                                          ││
│  │ │  • Latency: 1523ms                                         ││
│  │ ├─ CLIENT_RESPONSE                                           ││
│  │ │  • 总耗时: 1757ms                                           ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ [导出CSV] [导出JSON] [清空日志]                              ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 7.2 API端点

```go
// 日志相关API
mux.HandleFunc("/api/logs/sessions", handleGetSessions)      // 获取会话列表
mux.HandleFunc("/api/logs/requests", handleGetRequestLogs)   // 获取请求日志列表
mux.HandleFunc("/api/logs/request/{id}", handleGetRequestDetail) // 获取请求详情
mux.HandleFunc("/api/logs/export", handleExportLogs)         // 导出日志
mux.HandleFunc("/api/logs/clear", handleClearLogs)           // 清空日志
```

## 8. 错误处理

### 8.1 边界情况处理

| 场景 | 处理方式 |
|------|----------|
| 日志队列满 | 丢弃最旧事件，记录警告日志，不阻塞请求处理 |
| 数据库写入失败 | 重试3次，失败后记录到备用文件 `logs/failed_events.log` |
| Session追踪失败 | 使用 fallback Session ID（基于 IP+Port+时间戳） |
| 请求Body过大 | 截断到 10KB，记录截断标记 |
| 响应Body过大 | 截断到 50KB，记录截断标记 |
| 敏感字段缺失 | 记录为 `<missing>`，不报错 |
| 并发写入冲突 | 使用数据库事务，失败重试 |

### 8.2 Key脱敏规则

```go
func SanitizeKey(key string) string {
    if len(key) <= 8 {
        return key[:2] + "****" + key[len(key)-2:]
    }
    return key[:4] + "****" + key[len(key)-4:]
}

// 示例
// "sk-proj-abcd1234efgh5678ijkl" -> "sk-p****ijkl"
// "Bearer abc123xyz" -> "Bear****xyz"
```

## 9. 性能考虑

### 9.1 异步写入机制

- 事件队列容量：1000（可配置）
- Worker数量：1（单worker避免并发写入冲突）
- 批量写入：每100条或每1秒批量写入数据库
- 内存缓冲：使用 sync.Pool 复用 LogEvent 对象

### 9.2 性能指标

| 指标 | 目标值 |
|------|--------|
| RecordEvent() 调用耗时 | < 1ms |
| 队列吞吐量 | > 1000 events/sec |
| 内存占用 | < 10MB（队列满时） |

## 10. 测试策略

### 10.1 测试矩阵

| 测试类型 | 测试内容 | 测试方法 |
|----------|----------|----------|
| 单元测试 | Key脱敏函数 | 输入各种key格式，验证输出格式正确 |
| 单元测试 | Session追踪 | 模拟连接状态变化，验证Session ID生成 |
| 单元测试 | 事件序列化 | 验证事件数据正确序列化为JSON |
| 集成测试 | 完整请求流程 | 发送测试请求，验证所有事件被记录 |
| 集成测试 | 并发请求 | 同时发送100个请求，验证日志不丢失不混淆 |
| 集成测试 | 错误场景 | 模拟rapi失败，验证fallback和错误记录 |
| 性能测试 | 日志写入延迟 | 测量 RecordEvent() 调用耗时（应<1ms） |
| 性能测试 | 队列吞吐量 | 测试高负载下队列处理能力 |
| 边界测试 | 大Body请求 | 发送超大请求体，验证截断逻辑 |
| 边界测试 | 队列满场景 | 填满队列，验证丢弃策略 |

### 10.2 测试用例示例

```go
func TestSanitizeKey(t *testing.T) {
    tests := []struct{
        input    string
        expected string
    }{
        {"sk-proj-abcd1234efgh5678ijkl", "sk-p****ijkl"},
        {"short", "sho****ort"},
        {"ab", "ab****ab"},
    }
    for _, tt := range tests {
        result := SanitizeKey(tt.input)
        assert.Equal(t, tt.expected, result)
    }
}

func TestConcurrentRequests(t *testing.T) {
    // 发送100个并发请求
    // 验证每个请求的日志完整且不混淆
}

func TestQueueFull(t *testing.T) {
    // 填满队列
    // 验证丢弃策略生效，不阻塞请求处理
}
```

## 11. 实现计划

### 11.1 实现步骤

1. **Phase 1: 基础模块** (预估2天)
   - 创建 logger 模块目录结构
   - 实现 sanitize.go（Key脱敏）
   - 实现 events.go（事件类型定义）
   - 实现 config.go（配置定义）

2. **Phase 2: 数据存储** (预估1天)
   - 实现 storage.go（数据库存储）
   - 创建数据库表结构
   - 实现查询和清理接口

3. **Phase 3: Session追踪** (预估1天)
   - 实现 session.go（Session追踪器）
   - 集成 ConnState 钩子
   - 实现 Session ID 注入

4. **Phase 4: 日志核心** (预估2天)
   - 实现 logger.go（事件队列和worker）
   - 实现异步写入机制
   - 实现批量写入和清理

5. **Phase 5: Gateway集成** (预估1天)
   - 在 gateway.go 中集成日志记录
   - 在各处理阶段调用 RecordEvent()

6. **Phase 6: UI实现** (预估2天)
   - 新增日志标签页
   - 实现会话列表和请求详情展示
   - 实现搜索过滤和导出功能

7. **Phase 7: 测试和优化** (预估1天)
   - 编写单元测试和集成测试
   - 性能测试和优化
   - Bug修复

### 11.2 依赖关系

```
Phase 1 ──▶ Phase 2 ──▶ Phase 3 ──▶ Phase 4 ──▶ Phase 5 ──▶ Phase 6 ──▶ Phase 7
```

## 12. 附录

### 12.1 事件数据结构示例

```json
// REQUEST_RECEIVED
{
  "request_id": "req-abc123",
  "session_id": "sess-xyz789",
  "event_type": "REQUEST_RECEIVED",
  "timestamp": "2026-06-15T10:23:45.123Z",
  "data": {
    "client_ip": "192.168.1.100",
    "client_port": 54321,
    "method": "POST",
    "path": "/v1/chat/completions",
    "headers": {
      "Content-Type": "application/json",
      "Authorization": "Bear****xyz"
    },
    "body": "{\"model\":\"gpt-4\",\"messages\":...}"
  }
}

// UPSTREAM_SENT
{
  "request_id": "req-abc123",
  "event_type": "UPSTREAM_SENT",
  "timestamp": "2026-06-15T10:23:45.234Z",
  "data": {
    "selected_rapi": "openai-primary",
    "upstream_url": "https://api.openai.com/v1/chat/completions",
    "upstream_headers": {
      "Authorization": "sk-p****ijkl"
    },
    "upstream_body": "{\"model\":\"gpt-4o\",\"messages\":...}",
    "retry_count": 1
  }
}

// ERROR
{
  "request_id": "req-abc123",
  "event_type": "ERROR",
  "timestamp": "2026-06-15T10:23:45.456Z",
  "data": {
    "error_message": "RAPI openai-primary returned 429 Too Many Requests",
    "stage": "UPSTREAM_RESPONSE"
  }
}
```

### 12.2 配置示例

```yaml
# proxy.cfg 中新增日志配置
[log]
queue_capacity = 1000
max_age_days = 30
max_records = 100000
cleanup_interval = 24
max_body_size_kb = 50
batch_size = 100
batch_interval_sec = 1
```