# CI/CD

本仓库使用 GitHub Actions 做持续集成与持续交付。工作流定义在
`.github/workflows/ci.yml`。

## 触发时机

- `push` 到 `master` / `main`（含合并）
- `push` `v*` tag（发版）
- 所有 `pull_request`
- 手动触发：仓库 Actions 页 → “CI” → Run workflow

**纯文档改动不触发任何 job**（含镜像重建）：`on.push.paths-ignore` 排除了
`**.md`、`docs/**`、`.gitignore`、`.dockerignore`、`LICENSE`。

> 注意：GitHub **不对 tag push 评估 paths 过滤**，所以手动推 `v*` tag 仍照常发版。
> 另：`paths-ignore` 只能写在 trigger 层，job 层不支持（写了会解析失败）。

## Job 一览

| Job | 作用 | 触发条件 | 门槛 |
|-----|------|---------|------|
| `test` | `go vet` + `go test ./...` + Linux 编译（gateway / diaglog） | 全部 | 阻断 |
| `coverage` | 各包覆盖率地板线门禁 | 全部 | 阻断 |
| `gofmt` | 仅检查本分支改动的 Go 文件格式 | 全部 | 阻断 |
| `lint` | `golangci-lint`（固定 v1.60.3） | 全部 | ⚠️ 当前允许失败 |
| `tag` | 自动打版本 tag（见下） | 仅 push master | 阻断 |
| `release` | 交叉编译各平台二进制 + 建 GitHub Release | tag 事件 或 `tag` job 成功 | 仅发版 |
| `docker` | 构建多架构镜像推 GHCR | 仅 push 分支/tag | 仅 push |

`concurrency` 设置了同一 ref 串行、可中断，避免 push/push 并发跑。

## 自动版本 tag

每次 push 到 `master`，`tag` job 会打一个 **`v<主版本>.<YYYYMMDD>`** 形式的 tag，
例如 `v1.1.20261001`：

- 主版本读仓库根的 [`VERSION`](../VERSION) 文件（内容形如 `1.1`；格式非法会让 job 变红）。
- 日期取 `Asia/Shanghai` 时区。
- **同日重复推送**自动加后缀：`v1.1.20261001-2`、`-3`……（用 `git ls-remote` 查远端判定）。
- tag 由 `github-actions[bot]` 创建并推送。

### 为什么 release/docker 不靠 tag 事件串起来

用 `GITHUB_TOKEN` 创建的 tag **不会**再触发一次 workflow —— 这是 GitHub 防递归的
**有意行为**（官方文档明确）。所以 `tag` job 打完 tag 后，不能指望"tag 事件把
`release` 拉起来"，而是把 tag 名经 **job outputs** 传给**同一次运行内**的
`release` 与 `docker`。

### release 的两种来源

| 来源 | 条件 | 取哪个 tag |
|------|------|-----------|
| 手动推 tag | `github.ref` 是 `refs/tags/v*` | 该 tag 本身 |
| 自动 tag | `tag` job 成功 | `needs.tag.outputs.version` |

对应地，`release` / `docker` 的 `if` 用 `always() && !failure()` 兜住 ——
被 `needs` 的 job 一旦 `skipped`，依赖它的 job 默认也会 `skipped`，
不加 `always()` 会导致 tag 事件下 docker 意外不跑。

## 发布流程

**日常（自动）**：改 `VERSION` 若需要新主版本 → push master → 自动打 tag +
建 Release + 推镜像。无需手动操作。

**手动补发**（想指定 tag 时）：

```bash
git tag -a v1.2.20261001 -m "Release v1.2.20261001"
git push origin v1.2.20261001
```

## 镜像标签

推送到 `ghcr.io/gole1982/apigateway`，标签：

| 标签 | 来源 |
|------|------|
| `latest` | 默认分支（master） |
| `sha-<7位>` | 每次构建 |
| `1.1.20261001` | 自动版本号（与 Release 同名） |
| `<tag>` | 手动推 `v*` tag 时 |

多架构：`linux/amd64, linux/arm64`。

> 仓库名必须**全小写**：Docker 拒绝含大写字母的 repository name（`apiGateway` →
> `apigateway`）。CI 里 owner 与仓库名都先小写化再喂给 metadata-action。

## 部署（CD）选项

当前 CI **只负责构建并推送镜像**，不做自动部署（避免误部署到生产）。

部署到单机服务器，二选一：

### A. 服务器手动拉取最新镜像

```bash
# 服务器上提前登录一次
echo "$GHCR_TOKEN" | docker login ghcr.io -u <user> --password-stdin

# docker-compose 用远程镜像（image: ghcr.io/gole1982/apigateway:latest）
docker compose pull gateway && docker compose up -d
```

或直接跑仓库根的 [`update.sh`](../update.sh)（`git pull` + `pull` + `up -d`）。

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

# workflow 语法自检（改过 ci.yml 后建议跑）
actionlint .github/workflows/ci.yml
```

确保本地这些绿色后再 push，避免浪费 CI 名额。

## GHCR 权限

`docker` job 使用 `secrets.GITHUB_TOKEN` 推 `ghcr.io`，无需额外 Token。首次推送后，
到 GitHub → Settings → Packages 把该包的可见性设为 **Public**（或保持 Private 以便
私有仓库仅自己拉取）。

## 演进路线（建议分批合入）

1. **第一批（已完成）**：`test` / `coverage` / `gofmt` 作为合入门槛；`lint` 以非阻断方式跑。
2. **第二批**：`golangci-lint` 存量告警清零后，去掉 `lint` job 的 `continue-on-error`。
3. **第三批（可选）**：`go test -race` 目前未启用——本项目自带 FSM / 调度器 /
   单进程 SQLite 的并发状态代码，`-race` 会暴露既有竞态。抽时间修掉竞态后再开启。
4. **第四批（可选）**：提交信息规范（Commitlint）+ 自动 CHANGELOG。
