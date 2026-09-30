#!/bin/bash
# =============================================================================
# 最小改动更新：只把新编译的二进制塞进已运行的容器，不重建镜像、不拉取。
#
# 为什么可行：镜像运行时层只有 /app/gateway 一个文件（前端 dashboard.html 是
# go:embed 编译进二进制的，运行时依赖 ca-certificates/tzdata 已在基础镜像里）。
# 所以换掉这个二进制就等价于换了整个版本，约 6 秒完成。
#
# 用法：
#   bash local-update.sh                     # 更新本机容器
#   REMOTE=user@server bash local-update.sh  # 交叉编译后推到远端容器
#   bash local-update.sh --rollback          # 回滚到上一次备份的二进制
#
# 环境变量：
#   CONTAINER   容器名（默认 api-gateway）
#   REMOTE      user@host；设了就走 SSH 远端模式
#   WEB_PORT    容器内 web 端口（默认 24680，用于读取自报版本）
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")"

CONTAINER="${CONTAINER:-api-gateway}"
REMOTE="${REMOTE:-}"
WEB_PORT="${WEB_PORT:-24680}"
OUT=bin/gateway-local

# 备份放在 docker 宿主机上（不是容器内）—— 容器崩溃/起不来时依然能取用。
if [ -n "$REMOTE" ]; then
  BACKUP=/tmp/gateway.bak
  DOCKER="ssh $REMOTE docker"
else
  BACKUP="$PWD/bin/gateway.bak"
  DOCKER="docker"
fi

die() { echo "错误：$*" >&2; exit 1; }

# 健康检查用容器内 wget，与 compose healthcheck 同路径，本机/远端都适用。
# 全量静默：容器处于 stopped/restarting 时 docker exec 会把错误同时写到
# stdout，只重定向 stderr 挡不住，输出会刷屏。
health_ok() {
  $DOCKER exec "$CONTAINER" \
    wget -q -O /dev/null --spider http://localhost:13579/status >/dev/null 2>&1
}

wait_healthy() {
  for _ in $(seq 1 20); do
    health_ok && return 0
    sleep 1
  done
  return 1
}

# 运行中容器自报的版本（/api/status 里的 version 字段）。
running_version() {
  $DOCKER exec "$CONTAINER" \
    wget -q -O - "http://localhost:${WEB_PORT}/api/status" 2>/dev/null \
    | sed -n 's/.*"version":"\([^"]*\)".*/\1/p'
}

# 宿主机 → 容器 拷贝（兼容本机 / SSH 远端）。
# docker cp 保留源文件权限，所以先在宿主机 chmod +x；容器内不再 exec chmod——
# 回滚时容器可能已停止，exec 会失败（"container is not running"）。
host_to_container() { # $1=宿主机路径 $2=容器内路径
  chmod +x "$1" 2>/dev/null || true
  if [ -n "$REMOTE" ]; then
    ssh "$REMOTE" "docker cp '$1' '$CONTAINER:$2'"
  else
    docker cp "$1" "$CONTAINER:$2"
  fi
}
container_to_host() { # $1=容器内路径 $2=宿主机路径
  if [ -n "$REMOTE" ]; then
    ssh "$REMOTE" "docker cp '$CONTAINER:$1' '$2'"
  else
    docker cp "$CONTAINER:$1" "$2"
  fi
}

# 恢复备份并拉起服务。容器可能处于 Restarting 崩溃循环，此时 exec 会失败，
# 所以先 stop 再拷文件（停止态容器仍可用 docker cp 读写）。
restore_backup() {
  $DOCKER stop "$CONTAINER" >/dev/null 2>&1 || true
  host_to_container "$BACKUP" /app/gateway
  $DOCKER start "$CONTAINER" >/dev/null 2>&1 || true
}

# —— 回滚模式（容器在跑或已崩溃都能用）——
if [ "${1:-}" = "--rollback" ]; then
  [ -n "$REMOTE" ] || [ -f "$BACKUP" ] || die "本机找不到备份 $BACKUP，无法回滚"
  echo "==> 回滚到 $BACKUP"
  restore_backup
  wait_healthy && echo "回滚完成，服务正常（版本：$(running_version)）" \
    || die "回滚后健康检查仍失败，请查容器日志"
  exit 0
fi

# —— 1. 确认容器在跑，并探明目标架构 ——
$DOCKER inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true \
  || die "容器 $CONTAINER 未在运行${REMOTE:+（远端 $REMOTE）}，先启动它"

MACHINE=$($DOCKER exec "$CONTAINER" uname -m)
case "$MACHINE" in
  x86_64)  ARCH=amd64 ;;
  aarch64) ARCH=arm64 ;;
  *) die "未知架构：$MACHINE" ;;
esac
echo "==> 目标：$CONTAINER（linux/$ARCH）${REMOTE:+ @ $REMOTE}"

# —— 2. 编译（注入版本号，便于容器自报版本）——
echo "==> 编译"
mkdir -p bin
# CI 创建的版本 tag 本地默认没有，先拉一次；离线时忽略失败并回退到已有 tag。
git fetch --tags --quiet origin 2>/dev/null || true
VER=$(git describe --tags --always --dirty 2>/dev/null || echo dev)
CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
  go build -ldflags="-s -w -X gateway/internal/service.Version=$VER" -o "$OUT" ./cmd/gateway
echo "    版本：$VER"

# —— 3. 备份现有二进制到宿主机（供回滚）——
echo "==> 备份当前二进制到 $BACKUP"
container_to_host /app/gateway "$BACKUP"

# —— 4. 替换 ——
echo "==> 写入容器"
host_to_container "$OUT" /app/gateway

# —— 5. 重启 + 健康检查，失败自动回滚 ——
# restart 失败（如二进制不可执行）不能因 set -e 直接退出，否则永远走不到回滚。
echo "==> 重启"
$DOCKER restart "$CONTAINER" >/dev/null 2>&1 || true

if wait_healthy; then
  echo "==> 更新完成，服务正常（容器自报版本：$(running_version)）"
else
  echo "健康检查未通过，自动回滚..." >&2
  restore_backup
  wait_healthy && echo "已回滚到上一版本（更新失败）" >&2 \
    || echo "回滚后仍不健康，请手动排查" >&2
  exit 1
fi
