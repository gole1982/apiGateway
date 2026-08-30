#!/bin/bash
# =============================================================================
# 远程服务器一键更新脚本
#
# 用法（在服务器上、仓库目录里执行）：
#   bash update.sh
#
# 它做三件事：
#   1. 拉取最新代码（主要为了拿到最新的 docker-compose.yml 等配置文件）
#   2. 从 GitHub 拉取最新构建好的镜像（CI 已经帮你编译好了）
#   3. 用新镜像重启服务（数据不会丢，数据存在 Docker 卷里）
#
# 注意：第一次使用前需要先登录 GitHub 容器仓库（见 docs/日常操作手册.md）
# =============================================================================
set -e

cd "$(dirname "$0")"

echo "==> [1/3] 拉取最新代码"
git pull

echo "==> [2/3] 拉取最新镜像"
docker compose pull gateway

echo "==> [3/3] 重启服务"
docker compose up -d

echo ""
echo "==> 更新完成！当前运行的版本："
docker compose images gateway
echo ""
echo "==> 服务状态："
docker compose ps