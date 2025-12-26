#!/bin/bash
# 启动数据节点（支持指定节点ID和端口）
set -e

# 默认参数：node_1 9090
NODE_ID=${1:-node_1}
NODE_PORT=${2:-9090}
echo "📌 启动数据节点 $NODE_ID（端口：$NODE_PORT）..."

# 后台启动，日志输出到Static/logs
mkdir -p ./Static/logs
python -m src.datanode.server $NODE_ID $NODE_PORT > ./Static/logs/datanode_$NODE_ID.log 2>&1 &
NODE_PID=$!
echo "数据节点PID：$NODE_PID"
echo "日志文件：./Static/logs/datanode_$NODE_ID.log"

# 检查启动状态
sleep 2
if netstat -tulpn | grep -q ":$NODE_PORT "; then
    echo "✅ 数据节点 $NODE_ID 启动成功（端口：$NODE_PORT）"
else
    echo "❌ 数据节点启动失败，查看日志：./Static/logs/datanode_$NODE_ID.log"
    kill $NODE_PID 2>/dev/null || true
    exit 1
fi

# 保存PID用于停止
echo $NODE_PID >> ./Static/datanodes.pid