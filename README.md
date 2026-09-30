# API Gateway

多 LLM API 代理网关，提供成本路由、自动故障转移、多协议格式转换、动态令牌管理，以及可选的**中心-边缘配置同步**（多台转发机共享一份定义）。

---

## 三种运行角色

同一份二进制，按「是否连中心 + 录入 key 的权限」自动判定角色，**不再依赖配置文件段名**（面板「中心配置」页录入 URL + key 即可，保存后热激活、免重启）：

| 角色 | 触发条件 | 行为 |
|------|---------|------|
| **管理端** | secret key（`sb_secret_…`，旧称 service_role）对定义表可写 | 面板定义类增删改**直写中心 Supabase**，写后即时拉回本地镜像；各代理节点约 60s 内自动拉取 |
| **代理端** | publishable key（`sb_publishable_…`，旧称 anon）只读 | 定时轮询中心版本号，变了才全量拉取并应用；定义类写被只读守卫拦截 |
| **独立模式** | 未连中心（无 key / 连接失败） | 纯本地 SQLite，所有操作读写本地，不与中心交互 |

角色探测用无副作用写：`PATCH /rest/v1/platform?id=eq.-1`（0 行受影响；200/204 = 管理端，401/403 = 代理端，连不上 = 未连接；404/5xx = 代理端 + 诊断错误）。程序还会把 **key 前缀（意图）** 与 **写探测结果（事实）** 交叉校验：若 `sb_publishable_` 前缀的 key 竟然能写，说明中心库没做写隔离，会以安全侧（代理端）为准并显式报错，而不是静默当成管理端。

> 信任边界、写隔离的职责、`center_key` 的边界、token 保密口径 → **[docs/安全模型.md](docs/安全模型.md)**（权威口径，勿在别处重新推导）。
> 搭建与隔离 SQL → [docs/初次配置指南.md](docs/初次配置指南.md) + [`scripts/supabase_schema_v2.sql`](scripts/supabase_schema_v2.sql) 第 7 节。

---

## 架构

### 四层资源模型（自然键身份 v2）

- **Platform**（平台）— 上游 LLM 服务商，身份 = 归一化 `base_url`（`name` 降级为显示名），持有认证凭据与格式/登录配置
- **Credential**（凭据 / 面板称 Key）— 全局唯一密钥，身份 = `token_hash`（`sha256(token)` 前 16 hex），归属平台、平台内按 `sort_order` 轮换
- **Endpoint / RAPI**（端点）— 具体模型端点，身份 = `(base_url, model)`（`alias` 降级为显示名），携带成本、速率/限额、协议格式
- **LAPI**（本地 API）— 面向客户端的公开别名，绑定一条 RAPI 路由链，请求按链顺序故障转移
- **Binding**（绑定）— 端点 × 凭据关联表（`endpoint_credential`），即调度三元组「凭据 × 端点 × 模型」的落库形态；空绑定 = 该端点使用平台全部凭据

请求流程：客户端 → LAPI 别名匹配 → 调度器按成本/可用性选取 RAPI → 从绑定的凭据池挑选可用凭据 → 上游代理 → 失败自动换池中下一个凭据 / 链中下一个端点。

> 自然键的意义：改名不改身份（不触发同步 stale 删除 / 行重建），运行时健康状态随行续存。设计见 [docs/superpowers/specs/2026-09-23-natural-key-identity-design.md](docs/superpowers/specs/2026-09-23-natural-key-identity-design.md)。

实体状态层（`internal/entity` + `internal/fsm`）：RAPI / Key / Platform 各自是表驱动 FSM，状态转换原子化落库，调度器退化为薄协调层——详见 [docs/fsm-architecture.md](docs/fsm-architecture.md)。

### 中心-边缘同步

- **权威划分**：定义类（平台/凭据/端点/接口/绑定）= 中心权威；健康态与遥测 = 代理本地，**永不进 Bundle**。
- **契约**：中心 `get_bundle(p_version)` 返回定义快照（业务键引用、不带自增 id），`get_version()` 供代理轮询短路。当前契约 `SchemaVersion = 2`。
- **边缘策略**：启动拉 1 次 + 定时轮询版本号；中心或网络不可达时沿用本地 `last_good` 继续转发（**fail-open**），绝不中断业务。设计见 [docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md](docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md)。
- **存储接缝**：`internal/store` 抽象定义类持久层——独立/代理模式为本地 `*db.DB`，管理模式为 `supabase.Store`（PostgREST 直写中心）。运行时读（请求热路径/指标/日志/健康态）始终读本地 SQLite。

---

## 核心特性

- **成本路由**：base_cost → high_cost（超阈值）→ time_period_rules（分时），调度器优先选取最低成本可用 RAPI
- **故障转移**：所有非 2xx 响应触发切换；401 将该凭据标记永久失效并轮换到同端点的下一个凭据；同模型内多凭据轮换 + 池级切换
- **凭据池管理**：每个端点通过绑定表关联平台凭据；凭据独立健康状态（临时冷却 / 永久失效 / 过期 / 禁用），失败自动换凭据
- **能力黑名单（key×model）**：平台侧撤销凭据对某模型的权限时，自动识别 "model not found" 类错误并只禁用该 (Key, 模型) 组合（24h TTL 自动重试，请求成功 2xx 自动解除），不再整凭据冷却
- **被动监控**：冷却计时器 + 指数退避，无需主动健康检查；可恢复计费错误（积分不足/欠费）走长冷却而非永久失败
- **速率限制**：RPM / RPH / RPD / TPM / TPH / TPD 六维限制，支持端点级 + 绑定级双层限额（0 = 不限）
- **多协议转换**：OpenAI（内部标准格式）↔ Anthropic ↔ Gemini 双向转换，含 SSE 流式转换
- **令牌加密存储**：平台 Token 与凭据经 AES-256-GCM 加密落库，密钥保存在 `~/.apiGateway.key`（Docker 下由 `APIGATEWAY_KEY` 注入）
- **一键排查与恢复**：模型页可直测 Key×Model（真实穿透上游），2xx 自动清除失败标记并重新入池；添加 Key / 编辑绑定后自动重探测；支持启动时对 `available=0` 的端点做健康回收
- **Dashboard**：内嵌单文件 Vue 3 SPA（`internal/service/dashboard.html`），三层决策视图（Health → Efficiency → Capacity）、单屏指标「仪表盘」、智能「洞察」、Key 链路断点直显、SSE 实时推送、日志级别热调
- **中心配置管理**：面板「中心配置」页录入 URL + key 自动判定角色并热激活，支持手动刷新、拉取模式切换、诊断、推送本地配置到中心

---

## 端口

| 服务 | 默认端口 | 绑定地址 |
|------|---------|---------|
| Proxy | 13579 | 0.0.0.0 |
| Dashboard | 24680 | 127.0.0.1（容器内自动改绑 0.0.0.0） |

面板监听地址可用环境变量 `APIGATEWAY_WEB_HOST` 显式覆盖；检测到容器时默认绑全网卡。

---

## 配置（`proxy.cfg`）

与 `gateway.exe` 同目录；文件缺失时全部走默认值（便于无头/容器启动）。也可在面板「中心配置」页录入 Supabase 参数（保存进 `settings` 表，优先级高于 `proxy.cfg`）。

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
key_cursor_scope = session    ; session（默认）| platform —— 同平台多 key 的轮询作用域

[health]                      ; 启动健康回收
retry_on_startup = true       ; 启动后探测所有 unavailable 端点，恢复能响应的
retry_concurrency = 8
retry_timeout_sec = 15

[log]                         ; 结构化控制台日志（JSON stderr）
level = info                  ; debug | info | warn | error
file = false                  ; 是否另写文件
file_path = logs/gateway.log

# —— 中心配置同步（代理端，可选）——
# 配了 source_url 才启用；未配置 = 纯本地模式。
[sync]
source_url = https://你的项目.supabase.co/rest/v1/rpc/get_bundle
version_url = https://你的项目.supabase.co/rest/v1/rpc/get_version
anon_key = <sb_publishable_…，只读，可放代理端>
center_key = <可选：32字节hex；留空 = 中心存明文 token>
poll_interval_sec = 60

# —— 管理端（可选，只配在你自己的管理机）——
[management]
supabase_url = https://你的项目.supabase.co
service_key = <sb_secret_…，可写全表；切勿提交进仓库>
center_key = <留空 = 中心存明文；填了则与所有代理端一致>
```

### `key_cursor_scope`

同平台多 key 时，选 key 的轮询游标按什么作用域维护：

- **`session`（默认）** — 每个 `(平台, 会话)` 一个游标。同一会话的连续两轮**必然**落在不同 key 上，池内按顺序遍历；于是整个 key 池以 `len(pool) × Ts` 的速度被覆盖，所有 key 尽快进入冷却、池尽快整体耗尽。代价：不再保证跨会话的配额公平，且同一对话被拆到多个 key 上会损失上游 prompt 缓存命中。
- **`platform`** — 每平台一个游标，所有模型、所有会话共用（v2 之前的行为）。配额分摊更公平，但一个会话的相邻两轮在同一个 key 上的间隔是 `Ts × 会话数`，池耗尽更慢。

拿不到**连接级稳定**会话 id 的客户端（例如每个请求新建 TCP 连接的）一律按 `platform` 处理，与本配置无关 —— 原因见 `internal/logger/session.go` 的 `InjectSessionID`。

### 环境变量

| 变量 | 作用 |
|------|------|
| `APIGATEWAY_KEY` | 主密钥（64 位 hex）。容器重建不丢 key 全靠它；留空则首启生成随机 key 且只存容器内 |
| `APIGATEWAY_DATA_DIR` | 数据目录（`gateway.db` 落此），Docker 下设为持久化卷 `/app/data` |
| `APIGATEWAY_WEB_HOST` | 面板绑定地址，覆盖默认的 `127.0.0.1`（容器内自动为 `0.0.0.0`） |

---

## 代理端点

```
POST /v1/chat/completions   — OpenAI 兼容聊天补全（自动检测客户端协议格式）
POST /v1/messages           — Anthropic Messages API
POST /v1beta/*              — Gemini 原生 API（generateContent 等）
GET  /v1/models             — 模型列表（仅暴露 enabled 的 LAPI）
GET  /health                — 存活探针，返回 {"status":"ok"}
GET  /status                — 网关状态（端口、RAPI/LAPI 数量、累计请求数）
```

客户端可使用 OpenAI、Anthropic 或 Gemini 格式请求，网关自动识别并转换：

```bash
curl -X POST http://localhost:54321/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "my-alias", "messages": [{"role": "user", "content": "Hello"}]}'
```

---

## 构建与运行

```bash
# 构建
go build -o gateway.exe ./cmd/gateway

# 运行
./gateway.exe

# 诊断日志工具（按 request_id 串联日志事件）
go build -o diaglog.exe ./cmd/diaglog

# 自然键身份迁移（默认 dry-run 预览合并清单，-apply 才落库；
# 正常升级由 db.Init 自动执行，此工具用于升级前审阅）
go run ./cmd/naturalmigrate -db gateway.db
go run ./cmd/naturalmigrate -db gateway.db -apply
```

Go 1.21+，纯 Go SQLite 驱动（modernc.org/sqlite），无 CGO 依赖。

版本号在构建时注入（供面板显示）：

```bash
go build -ldflags "-X gateway/internal/service.Version=$(git describe --tags)" -o gateway ./cmd/gateway
```

---

## Docker 部署

```bash
cp .env.example .env   # 填 APIGATEWAY_KEY（openssl rand -hex 32）
docker compose up -d --build
```

- 代理 `http://宿主机:13579`，面板 `http://宿主机:24680`。
- 配置文件是 `proxy.docker.cfg`（容器端口），**不是**仓库根的 `proxy.cfg`（本地 Windows 直运的 43210/43211）—— 挂错则端口映射失效。
- `APIGATEWAY_KEY` 生产必须设置：数据库 token 用它加密；容器重建后 key 变了历史密文就解不开。`gateway-data` 卷持久化 `gateway.db`。
- 健康检查：`GET :13579/status`，`docker ps` 应显示 `healthy`。
- 更新：一键脚本 `bash update.sh`，或手动 `docker compose pull && docker compose up -d`（镜像由 CI 在 push main / tag 时自动构建推送到 GHCR，多架构 `linux/amd64, linux/arm64`）。每次 push master 会自动打版本 tag 并建 Release，镜像同时带 `latest`、`sha-<7位>` 和版本号（如 `1.1.20261001`）三种标签。

---

## 项目结构

```
cmd/
  gateway/          主入口
  diaglog/          诊断日志工具（按 request_id 串联日志事件）
  naturalmigrate/   自然键身份迁移（dry-run / -apply）
internal/
  apiformat/        OpenAI ↔ Anthropic ↔ Gemini 协议转换层（含 SSE 流式）
  bundle/           中心 ↔ 代理定义快照契约（SchemaVersion=2）
  config/           proxy.cfg 配置加载
  crypto/           AES-256-GCM 令牌加密
  db/               SQLite 数据访问 + 迁移 + 分析聚合 + Bundle 导入导出
  entity/           实体状态层（RAPI / Key / Platform FSM，单一事实源）
  fsm/              通用表驱动状态机引擎
  gateway/          代理核心：请求路由、故障转移、流式转发
  logger/           异步请求日志 + 会话追踪
  models/           数据模型 + 自然键归一化（NormalizeBaseURL / TokenHash）
  notify/           SSE 通知推送
  scheduler/        协调器：持有实体、成本路由、速率限制、冷却管理
  service/          HTTP 服务 + Dashboard REST API + 内嵌单文件前端
  store/            定义类持久层接缝（本地 DB 或中心 Supabase）
  supabase/         中心 PostgREST 客户端（管理模式直写）
scripts/
  supabase_schema_v2.sql     中心 schema v2（自然键 + 版本触发器 + get_bundle/get_version）
  supabase_verify_v2.sql     只读复核
  supabase_seed_example.sql  示例配置
docs/
  初次配置指南.md            中心角色自动判定 + 指标仪表盘首次部署
  日常操作手册.md            大白话运维手册
  fsm-architecture.md        实体状态 / FSM 架构
  ci-cd.md                   CI/CD 说明
  superpowers/               设计定稿与实施计划
```

---

## 依赖

- `modernc.org/sqlite` — 纯 Go SQLite 驱动
- `gopkg.in/ini.v1` — INI 配置解析
- `github.com/google/uuid` — UUID 生成
- `github.com/stretchr/testify` — 测试断言

---

## 测试与 CI

```bash
go vet ./...
go test ./... -count=1
CGO_ENABLED=0 go build -o /tmp/gateway ./cmd/gateway
```

GitHub Actions（[.github/workflows/ci.yml](.github/workflows/ci.yml)）七个 job：

| Job | 作用 | 门槛 |
|-----|------|------|
| `test` | `go vet` + `go test` + Linux 编译 | 阻断 |
| `coverage` | 各包覆盖率地板线门禁 | 阻断 |
| `gofmt` | 仅检查本分支改动的 Go 文件格式 | 阻断 |
| `lint` | `golangci-lint`（固定 v1.60.3） | 当前允许失败 |
| `tag` | push master 时自动打版本 tag（读 [`VERSION`](VERSION)，形如 `v1.1.20261001`） | 仅 master |
| `release` | 交叉编译各平台二进制并建 Release（tag 事件 或 自动 tag 成功） | 仅发版 |
| `docker` | push 主分支/tag 时构建推送到 GHCR（含 `latest` + sha + 版本号） | 仅主分支/tag |

纯文档改动（`**.md`、`docs/**` 等）不触发任何 job。发版默认全自动：push master 即打 tag、建 Release、推镜像。详见 [docs/ci-cd.md](docs/ci-cd.md)。
