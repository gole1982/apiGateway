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
| 信任边界 / 写隔离 / center_key 边界 / token 保密 / 为什么不做 RBAC | **[docs/安全模型.md](docs/安全模型.md)** |
| 搭建、隔离 SQL、FAQ | [docs/初次配置指南.md](docs/初次配置指南.md) |
| 运维流程 | [docs/日常操作手册.md](docs/日常操作手册.md) |
| 编码规范 / 常见错误模式 | [dev-guide.md](dev-guide.md) |
| 中心-边缘同步设计 | [docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md](docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md) |

讨论安全相关话题时**直接引用 docs/安全模型.md 的结论**，不要从零重新论证。

## 关键约定（易踩坑）

- **角色判定靠写探测，不靠配置段名**。`probeSBRole` 做 `PATCH /rest/v1/platform?id=eq.-1`；
  并用 key 前缀（意图）交叉校验写探测结果（事实）。`sb_publishable_` 却能写 = 中心没做写隔离
  → 返回代理端 + 显式报错，**绝不静默当管理端**。
- **`login_password` 是 write-only**。中心 `get_bundle` 刻意不返回该列；同步落库必须保留本地既有密文
  （`bundle_apply.go` 的 `CASE WHEN ? = '' THEN ...`），否则每次同步会清空它。
- **`token` 可被 publishable key 读取**——设计内行为（代理端转发必需）。publishable key 等价于
  「全部上游 token 的钥匙」，按高价值密钥管理。
- **写隔离（中心 RLS）只落实权限约定，不是身份层**；准入靠人工分发 key。不要把它读成引入 RBAC。
- **`scripts/supabase_verify_v2.sql` 必须保持只读**：它是唯一会在生产中心上跑的脚本，
  `scripts/sql_contract_test.go` 有机器校验（关键字扫描会先剥离字符串字面量，权限名 `'UPDATE'` 不算关键字）。
- **v2 schema 契约**：`SchemaVersion = 2`；凭据身份 = `token_hash`（非 `platform_keys.key_index`）；
  端点↔凭据绑定走 `endpoint_credential` 表（非 `rapi.key_ids` CSV）。旧的 `scripts/supabase_schema.sql` 是 v1，**不要跑**。
- **发版全自动**：push master → `tag` job 读根目录 `VERSION`（形如 `1.1`）打 `v<版本>.<YYYYMMDD>`，
  同日重复加 `-2` 后缀；release/docker 靠 **job outputs**（不是 tag 事件）串联——`GITHUB_TOKEN`
  创建的 tag 不会触发新 workflow（GitHub 防递归）。
- **`paths-ignore` 只能在 trigger 层**（job 层不支持，写错整个 workflow 解析失败）；
  且 GitHub **不对 tag push 评估 paths 过滤**。
- 改过 `ci.yml` 后用 `actionlint .github/workflows/ci.yml` 自检（本地已装于 /tmp/actionlint）。
