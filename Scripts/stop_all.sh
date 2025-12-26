#!/bin/bash
# 停止所有服务（协调节点+数据节点+ZK）
set -e

echo "📌 停止所有服务..."

# 停止协调节点
if [ -f ./Static/coordinator.pid ]; then
    COORD_PID=$(cat ./Static/coordinator.pid)
    echo "停止协调节点（PID：$COORD_PID）..."
    kill $COORD_PID 2>/dev/null || true
    rm -f ./Static/coordinator.pid
fi

# 停止数据节点
if [ -f ./Static/datanodes.pid ]; then
    for NODE_PID in $(cat ./Static/datanodes.pid); do
        echo "停止数据节点（PID：$NODE_PID）..."
        kill $NODE_PID 2>/dev/null || true
    done
    rm -f ./Static/datanodes.pid
fi

# 停止ZK
echo "停止ZooKeeper..."
zkServer.sh stop

# 清理临时文件
rm -f ./Static/*.pid

echo "✅ 所有服务已停止"