# API Gateway

多 LLM API 代理网关，提供成本路由、自动故障转移、多协议格式转换和动态令牌管理。

## 架构

四层资源模型：

- **Platform**（平台）— 上游 LLM 服务商，持有 Base URL 和认证凭据（API Key 或浏览器会话令牌）
- **Key**（密钥）— 平台下的动态密钥池，多 Key 轮换、独立健康状态与失败标记
- **RAPI**（远程 API）— 平台下的具体模型端点，携带成本、速率限制、协议格式配置和 Key 池白名单（`key_ids`）
- **LAPI**（本地 API）— 面向客户端的别名，绑定一条 RAPI 路由链，请求按链顺序故障转移

请求流程：客户端 → LAPI 别名匹配 → 调度器按成本/可用性选取 RAPI → 从该模型的 Key 池挑选可用 Key → 上游代理 → 失败自动切换池中下一个 Key / 链中下一个 RAPI。

实体状态层（`internal/entity` + `internal/fsm`）：RAPI / Key / Platform 各自是表驱动 FSM，状态转换原子化落库，调度器退化为薄协调层——详见 [docs/fsm-architecture.md](docs/fsm-architecture.md)。

## 核心特性

- **成本路由**：base_cost → high_cost（超阈值）→ time_period_rules（分时），调度器优先选取最低成本可用 RAPI
- **故障转移**：所有非 2xx 响应触发切换；401 将该 Key 标记永久失效并轮换到同 RAPI 的下一个 Key；同模型内多 Key 轮换 + 池级切换
- **Key 池管理**：每个模型通过 `key_ids` 白名单绑定平台 Key；Key 独立健康状态（临时冷却 / 永久失效 / 过期 / 禁用），失败自动换 Key
- **能力黑名单（key×model）**：平台侧撤销 Key 对某模型的权限时，自动识别 "model not found" 类错误并只禁用该 (Key, 模型) 组合（24h TTL 自动重试，请求成功 2xx 自动解除），不再整 Key 冷却
- **被动监控**：冷却计时器 + 指数退避，无需主动健康检查；可恢复计费错误（积分不足/欠费）走长冷却而非永久失败
- **速率限制**：RPM / RPH / RPD / TPM / TPH / TPD 六维限制
- **多协议转换**：OpenAI（内部标准格式）↔ Anthropic ↔ Gemini 双向转换，含 SSE 流式转换
- **令牌加密存储**：平台 Token 与 Key 经 AES-256-GCM 加密落库，密钥保存在 `~/.apiGateway.key`
- **一键排查与恢复**：模型页可直测 Key×Model（真实穿透上游），2xx 自动清除失败标记并重新入池；添加 Key / 编辑白名单后自动重探测并恢复此前因 Key 全死而不可用的模型
- **Dashboard**：内嵌 Vue 3 SPA，三层决策视图（Health → Efficiency → Capacity），接口页/模型页直接展示 Key 链路断点与 Key 池状态，SSE 实时推送

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
billing_cooldown_sec = 1800   ; 可恢复计费错误（积分不足/欠费）的长冷却秒数
capability_block_sec = 86400  ; key×model 能力黑名单 TTL 秒数，到期自动重试
request_max_wait_sec = 120
key_cursor_scope = session   ; session（默认）| platform —— 同平台多 key 的轮询作用域
```

### `key_cursor_scope`

同平台多 key 时，选 key 的轮询游标按什么作用域维护：

- **`session`（默认）** — 每个 `(平台, 会话)` 一个游标。同一会话的连续两轮**必然**落在不同 key 上，池内按顺序遍历；于是整个 key 池以 `len(pool) × Ts` 的速度被覆盖，所有 key 尽快进入冷却、池尽快整体耗尽。代价：不再保证跨会话的配额公平，且同一对话被拆到多个 key 上会损失上游 prompt 缓存命中。
- **`platform`** — 每平台一个游标，所有模型、所有会话共用（v2 之前的行为）。配额分摊更公平，但一个会话的相邻两轮在同一个 key 上的间隔是 `Ts × 会话数`，池耗尽更慢。

拿不到**连接级稳定**会话 id 的客户端（例如每个请求新建 TCP 连接的）一律按 `platform` 处理，与本配置无关 —— 原因见 `internal/logger/session.go` 的 `InjectSessionID`。

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

## Docker 部署

```bash
cp .env.example .env   # 填 APIGATEWAY_KEY（openssl rand -hex 32）
docker compose up -d --build
```

- 代理 `http://宿主机:13579`，面板 `http://宿主机:24680`。
- 配置文件是 `proxy.docker.cfg`（容器端口），**不是**仓库根的 `proxy.cfg`
  （本地 Windows 直运的 43210/43211）—— 挂错则端口映射失效。
- `APIGATEWAY_KEY` 生产必须设置：数据库 token 用它加密；容器重建后 key
  变了历史密文就解不开。`gateway-data` 卷持久化 `gateway.db`。
- 健康检查：`GET :13579/status`，`docker ps` 应显示 `healthy`。
- 更新：`docker compose pull && docker compose up -d`（镜像由 CI 在
  push main / tag 时自动构建推送到 GHCR）。

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
  entity/           实体状态层（RAPI / Key / Platform FSM，单一事实源）
  fsm/              通用表驱动状态机引擎
  gateway/          代理核心：请求路由、故障转移、流式转发
  logger/           异步请求日志
  models/           数据模型
  notify/           SSE 通知推送
  scheduler/        协调器：持有实体、成本路由、速率限制、冷却管理
  service/          HTTP 服务 + Dashboard REST API + 内嵌前端
```

## 依赖

- `modernc.org/sqlite` — 纯 Go SQLite 驱动
- `gopkg.in/ini.v1` — INI 配置解析
- `github.com/google/uuid` — UUID 生成
