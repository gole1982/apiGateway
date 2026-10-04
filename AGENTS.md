# AGENTS.md

本仓库的持久记忆。新会话先读本文件，再读 [docs/安全模型.md](docs/安全模型.md)。

## 项目

多 LLM API 代理网关（Go）。同一份二进制按「是否连中心 + 录入 key 的权限」自动判定三种角色：
管理端（secret key，直写中心）/ 代理端（publishable key，只读同步）/ 独立模式（纯本地 SQLite）。

## 环境与命令

```bash
# Go 不一定预装；缺失时报 go: command not found，重装：
sudo apt-get update -qq && sudo apt-get install -y golang-go

go vet ./...
go test ./... -count=1
gofmt -l internal scripts          # CI 只检查改动文件的格式
CGO_ENABLED=0 go build -o /tmp/gateway ./cmd/gateway
```

- CI 见 `.github/workflows/ci.yml`：`test` / `coverage` / `gofmt` 阻断，`lint` 当前允许失败。
- `gofmt` 全量会报 `internal/gateway/gateway_test.go`、`internal/service/metrics_test.go` —— 这两个**本来就未格式化**，非改动引入，不要顺手改。
- 仓库是**浅克隆**（`git rev-parse --is-shallow-repository`）。`master` 落后 `origin/master`；**不要擅自推送或开 PR**。
- git user 已配置（gole1982）。提交信息末尾加 `Co-authored-by: openhands <openhands@all-hands.dev>`。

## 权威文档（勿重复推导）

| 主题 | 权威位置 |
|------|---------|
| 信任边界 / 写隔离 / token 保密 / 为什么不做 RBAC | **[docs/安全模型.md](docs/安全模型.md)** |
| 搭建、隔离 SQL、FAQ | [docs/初次配置指南.md](docs/初次配置指南.md) |
| 运维流程 | [docs/日常操作手册.md](docs/日常操作手册.md) |
| 编码规范 / 常见错误模式 | [dev-guide.md](dev-guide.md) |
| 中心-边缘同步设计 | [docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md](docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md) |

讨论安全相关话题时**直接引用 docs/安全模型.md 的结论**，不要从零重新论证。

## 关键约定（易踩坑）

- **角色判定靠写探测，不靠配置段名**。`probeSBRole` 做 `PATCH /rest/v1/platform?id=eq.-1`；
  并用 key 前缀（意图）交叉校验写探测结果（事实）。`sb_publishable_` 却能写 = 中心没做写隔离
  → 返回代理端 + 显式报错，**绝不静默当管理端**。
- **控制台密码/计费网址字段已删除**。`platform` 表不再有 `billing_address` /
  `login_password` 列（本地删列迁移 + 中心 v2 schema 已移除）；面板不再录入，
  `get_bundle` 不再下发。历史残留以删列为准，不要再加回来。
- **同步动作由 `syncAction` 四分支决定**：skip（版本一致，不打扰）/ pending（manual
  非强制，只记待拉取）/ merge（版本不一致 + 本地脏 → 并集合并）/ pull（版本不一致 +
  本地干净 → 直接拉取覆盖）。
- **并集合并（`sync_merge.go`，纯函数）**：自然键取并集，同键冲突**本地胜**；
  `AddsToCenter` 为真才回推中心（否则只空转 bump 版本号）；只读 key 只合本地、
  保留脏标记；**合并不传播删除**（删行走推送发布）。时间戳只比到秒（亚秒级差异不算冲突）。
- **脏标记经 Store 接缝自动标**（`edgeStore` 包装本地实现，`store.Use(wrapEdgeStore(...))`）：
  管理模式（直写中心）不标、离线不标、网关运行时写（走 db 直写）不标。
  **面板定义写必须走 `store.A()`**，绕过 store 直接调 db 会漏标。
- **`/api/sync/refresh` 是异步触发**（回 `{"accepted":true}`，后台执行），面板 toast
  不要写"拉取成功"；进度看状态接口 `merging` 字段。`syncOpMu` 串行化应用类操作。
- **`token` 可被 publishable key 读取**——设计内行为（代理端转发必需）。publishable key 等价于
  「全部上游 token 的钥匙」，按高价值密钥管理。
- **写隔离（中心 RLS）只落实权限约定，不是身份层**；准入靠人工分发 key。不要把它读成引入 RBAC。
- **`scripts/supabase_verify_v2.sql` 必须保持只读**：它是唯一会在生产中心上跑的脚本，
  `scripts/sql_contract_test.go` 有机器校验（关键字扫描会先剥离字符串字面量，权限名 `'UPDATE'` 不算关键字）。
- **v2 schema 契约**：`SchemaVersion = 2`；凭据身份 = `token_hash`（非 `platform_keys.key_index`）；
  端点↔凭据绑定走 `endpoint_credential` 表（非 `rapi.key_ids` CSV）。旧的 `scripts/supabase_schema.sql` 是 v1，**不要跑**。
- **发版全自动**：push master → `tag` job 读根目录 `VERSION`（只写大版本号，如 `1`）
  打 `v<大>.<小>.<YYYYMMDD>`：小版本在大版本未变时自动 +1，大版本变则归零。
  上一次发布从远端 tag 推导；重跑同一 commit 复用已有 tag。
- **`paths-ignore` 只能在 trigger 层**（job 层不支持，写错整个 workflow 解析失败）；
  且 GitHub **不对 tag push 评估 paths 过滤**。
- **Docker 更新两条路**：`update.sh`（拉镜像，服务器用）/ `local-update.sh`（只换
  容器内二进制，约 6 秒）。后者可行的前提是**镜像运行时层只有 `/app/gateway`
  一个文件**——前端 `dashboard.html` 是 `go:embed` 编译进二进制的，运行时依赖
  `ca-certificates`/`tzdata` 在基础镜像里。若将来新增运行时外部文件（模板、
  静态资源、新依赖），`local-update.sh` 会静默失效，必须同步改。
- **备份要放宿主机**（`bin/gateway.bak`），不要放容器内：容器崩溃进
  `Restarting` 循环后 `docker exec` 全部失败，容器内备份取不出来。停止态容器
  仍可用 `docker cp` 读写，这是回滚路径的基础。
- **`docker cp` 保留源文件权限**：拷贝前在宿主机 `chmod +x`，别在容器内
  `exec chmod`（停止态会失败）。
- **`Version` 由 ldflags 注入**（`gateway/internal/service.Version`），`/api/status`
  的 `version` 字段自报。Dockerfile 用 `ARG VERSION`、CI 用 `build-args`/`env` 传入；
  不传则为 `dev`。
- `.env` 存 `APIGATEWAY_KEY` 主密钥，已在 `.gitignore`；**切勿提交**。
- 改过 `ci.yml` 后用 `actionlint .github/workflows/ci.yml` 自检（本地已装于 /tmp/actionlint）。
