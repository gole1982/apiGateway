# CI/CD

本仓库使用 GitHub Actions 做持续集成与持续交付。工作流定义在
`.github/workflows/ci.yml`。

## 触发时机

- `push` 到 `master` / `main`（含合并）
- 所有 `pull_request`
- 手动触发：仓库 Actions 页 → “CI” → Run workflow

## 三个 Job

| Job | 作用 | 是否合入门槛 |
|-----|------|--------------|
| `test`   | `go vet` + `go test ./...` + Linux 编译（gateway / diaglog） | ✅ 是（阻断） |
| `lint`   | `golangci-lint`（用仓库自带 `.golangci.yml`） | ⚠️ 当前**允许失败**（`continue-on-error: true`），先出报告 |
| `docker` | 推 master/main 时构建镜像并推送到 `ghcr.io/<repo>`，含 `latest` + 7 位 commit sha 标签 | 仅主分支 push 时执行 |

`concurrency` 设置了同一 ref 串行、可中断，避免 push/push 并发跑。

## 演进路线（建议分批合入）

1. **第一批（当前即启用）**：`test` 作为合入门槛；`lint` 以非阻断方式跑，
   让团队先看到存量问题报告。
2. **第二批**：本地/CI 把 `golangci-lint` 的存量告警清零后，去掉 `lint` job 的
   `continue-on-error`，使其成为合入门槛。
3. **第三批（可选）**：`go test -race` 目前被注释掉——本项目自带 FSM / 调度器 /
   单进程 SQLite 的并发状态代码，`-race` 会暴露既有竞态。抽时间用 `-race` 本地
   跑一轮、修掉竞态后再在 CI 开启，能显著提升可靠性。
4. **第四批（可选）**：提交信息规范（Commitlint）+ 自动 CHANGELOG（release-please）。

## 部署（CD）选项

当前 CI **只负责构建并推送镜像**，不做自动部署（避免误部署到生产）。

部署到单机服务器，二选一：

### A. 服务器手动拉取最新镜像

```bash
# 服务器上提前登录一次
echo "$GHCR_TOKEN" | docker login ghcr.io -u <user> --password-stdin

# docker-compose 改用远程镜像（把 build: . 换成 image: ghcr.io/gole1982/apiGateway:latest）
docker compose pull gateway && docker compose up -d
```

### B. 到达 CI 自动部署（需要准备 SSH 相关的 GitHub Secrets）

在 CI 的 `docker` job 之后追加一个 `deploy` 阶段，通过 SSH 登录服务器执行
`docker compose pull && up -d`：

- 需配置 Secrets：
  - `DEPLOY_HOST` / `DEPLOY_USER` / `DEPLOY_SSH_KEY`（或 `DEPLOY_TRIGGER`）
- 参考 action：`appleboy/ssh-action@v1`

> 部署目标若是多台 / 单机直连，推荐用 SSH + Docker Compose 方案；若是
> K8s/云原生环境，可改用 `kubeconfig` + Helm，那是另一套工作流。

## 本地先验证再推送

```bash
go vet ./...
go test ./...
CGO_ENABLED=0 go build -o /tmp/gateway ./cmd/gateway
docker build -t api-gateway .   # 可选，本地验证镜像
```

确保本地这些绿色后再 push，避免浪费 CI 名额。

## GHCR 权限

`docker` job 使用 `secrets.GITHUB_TOKEN` 推 `ghcr.io`，无需额外 Token。首次推送后，
到 GitHub → Settings → Packages 把该包的可见性设为 **Public**（或保持 Private 以便
私有仓库仅自己拉取）。