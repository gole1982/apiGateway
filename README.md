# API Gateway

多 LLM API 代理网关，提供成本路由、自动故障转移、多协议格式转换和动态令牌管理。

## 架构

三层资源模型：

- **Platform**（平台）— 上游 LLM 服务商，持有 Base URL 和认证凭据（API Key 或浏览器会话令牌）
- **RAPI**（远程 API）— 平台下的具体模型端点，携带成本、速率限制和协议格式配置
- **LAPI**（本地 API）— 面向客户端的别名，绑定一条 RAPI 路由链，请求按链顺序故障转移

请求流程：客户端 → LAPI 别名匹配 → 调度器按成本/可用性选取 RAPI → 上游代理 → 失败自动切换链中下一个 RAPI。

## 核心特性

- **成本路由**：base_cost → high_cost（超阈值）→ time_period_rules（分时），调度器优先选取最低成本可用 RAPI
- **故障转移**：所有非 2xx 响应触发切换；401 自动刷新令牌并重试同一 RAPI
- **被动监控**：冷却计时器 + 指数退避，无需主动健康检查
- **速率限制**：RPM / RPH / RPD / TPM / TPH / TPD 六维限制
- **多协议转换**：OpenAI（内部标准格式）↔ Anthropic ↔ Gemini 双向转换，含 SSE 流式转换
- **动态令牌**：浏览器扩展推送会话令牌，网关自动接管上游请求；AES-256-GCM 加密存储
- **Dashboard**：内嵌 Vue 3 SPA，三层决策视图（Health → Efficiency → Capacity），SSE 实时推送

## 端口

| 服务 | 默认端口 | 绑定地址 |
|------|---------|---------|
| Proxy | 13579 | 0.0.0.0 |
| Dashboard | 24680 | 127.0.0.1 |

通过 `proxy.cfg` 覆盖（与 gateway.exe 同目录）：

```ini
proxy_port = 54321
web_port = 54322
dial_timeout_sec = 30
response_timeout_sec = 300
cooldown_sec = 10
max_cooldown_sec = 120
request_max_wait_sec = 120
```

## 代理端点

```
POST /v1/chat/completions   — OpenAI 兼容聊天补全（自动检测客户端协议格式）
GET  /v1/models             — 模型列表
GET  /status                — 网关状态
```

客户端可使用 OpenAI、Anthropic 或 Gemini 格式请求，网关自动识别并转换：

```bash
curl -X POST http://localhost:54321/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "my-alias", "messages": [{"role": "user", "content": "Hello"}]}'
```

## 构建与运行

```bash
# 构建
go build -o gateway.exe ./cmd/gateway

# 运行
./gateway.exe

# 诊断日志工具
go build -o diaglog.exe ./cmd/diaglog
```

Go 1.21+，纯 Go SQLite 驱动（modernc.org/sqlite），无 CGO 依赖。

## 项目结构

```
cmd/
  gateway/          主入口
  diaglog/          诊断日志工具
internal/
  apiformat/        OpenAI ↔ Anthropic ↔ Gemini 协议转换层
  config/           proxy.cfg 配置加载
  crypto/           AES-256-GCM 令牌加密
  db/               SQLite 数据访问 + 迁移
  gateway/          代理核心：请求路由、故障转移、流式转发
  logger/           异步请求日志
  models/           数据模型
  notify/           SSE 通知推送
  scheduler/        成本路由调度器、速率限制、冷却管理
  service/          HTTP 服务 + Dashboard REST API + 内嵌前端
```

## 依赖

- `modernc.org/sqlite` — 纯 Go SQLite 驱动
- `gopkg.in/ini.v1` — INI 配置解析
- `github.com/google/uuid` — UUID 生成
