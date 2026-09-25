# =============================================================================
# API Gateway - Multi-stage Docker Build
# =============================================================================
# The tray app (gateway_tray.exe) is Windows-only and NOT included in the
# Docker image. Docker containers run in headless mode — the core proxy,
# dashboard, token management, and logging all work without a GUI.
#
# Usage:
#   docker build -t api-gateway .
#   docker run -d -p 13579:13579 -p 24680:24680 -v $(pwd)/proxy.cfg:/app/proxy.cfg api-gateway
# =============================================================================

# ---- Stage 1: Build ----
FROM golang:1.21-alpine AS builder

RUN apk add --no-cache tzdata

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

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
