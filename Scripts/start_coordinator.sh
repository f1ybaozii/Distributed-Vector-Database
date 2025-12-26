#!/bin/bash
# 启动协调节点
set -e

# 默认端口8080，支持传参指定
COORD_PORT=${1:-8081}
echo "📌 启动协调节点（端口：$COORD_PORT）..."

# 后台启动，日志输出到Static/logs
mkdir -p ./Static/logs
python -m src.coordinator.server $COORD_PORT > ./Static/logs/coordinator.log 2>&1 &
COORD_PID=$!
echo "协调节点PID：$COORD_PID"
echo "日志文件：./Static/logs/coordinator.log"

# 检查启动状态
sleep 2
if netstat -tulpn | grep -q ":$COORD_PORT "; then
    echo "✅ 协调节点启动成功（端口：$COORD_PORT）"
else
    echo "❌ 协调节点启动失败，查看日志：./Static/logs/coordinator.log"
    kill $COORD_PID 2>/dev/null || true
    exit 1
fi

# 保存PID用于停止
echo $COORD_PID > ./Static/coordinator.pid