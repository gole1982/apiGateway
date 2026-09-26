# =============================================================================
# API Gateway - Multi-stage Docker Build
# =============================================================================
# The tray app (gateway_tray.exe) is Windows-only and NOT included in the
# Docker image. Docker containers run in headless mode — the core proxy,
# dashboard, token management, and logging all work without a GUI.
#
# Usage (compose 推荐，见 docker-compose.yml）:
#   cp .env.example .env   # 填 APIGATEWAY_KEY（生产必须，否则重建丢 key）
#   docker compose up -d --build
#
# 裸 docker run（配置文件用仓库的 proxy.docker.cfg，不要用 proxy.cfg ——
# 后者是本地 Windows 直运的端口 43210/43211）：
#   docker build -t api-gateway .
#   docker run -d -p 13579:13579 -p 24680:24680 \
#     -v $(pwd)/proxy.docker.cfg:/app/proxy.cfg:ro \
#     -v gateway-data:/app/data \
#     -e APIGATEWAY_KEY=<64位hex> api-gateway
# =============================================================================

# ---- Stage 1: Build ----
FROM golang:1.21-alpine AS builder

RUN apk add --no-cache tzdata

WORKDIR /src

# 可选代理：goproxy.cn 在部分网络下比 proxy.golang.org 稳（后者拉 modernc.org
# 的大依赖时容易 HTTP/2 流中断）。
# 构建时传入：--build-arg GOPROXY=https://goproxy.cn,direct
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=$GOPROXY

# Cache dependencies.
#
# COPY go.mod go.sum 单独成层：go.mod/go.sum 变了才失效依赖层，源码改动
# 不会让 ~40MB 的 modernc.org/* 依赖树重下。
#
# --mount=type=cache 让 module cache 跨构建复用；不加"挂不上就退回"的兜底，
# 那会让真实失败被第二次同样的失败盖住，报错更难读。
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Build only the gateway (no tray — headless Docker)
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /gateway ./cmd/gateway

# ---- Stage 2: Runtime ----
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /gateway .

# Default config (overridable by volume mount or env)
RUN printf "proxy_port = 13579\nweb_port = 24680\nrefresh_interval_sec = 600\n" > /app/proxy.cfg

EXPOSE 13579 24680

VOLUME ["/app/data"]

# gateway.db 必须位于持久化 volume；否则每次重建容器都会重新创建空库。
ENV APIGATEWAY_DATA_DIR=/app/data

ENTRYPOINT ["/app/gateway"]
