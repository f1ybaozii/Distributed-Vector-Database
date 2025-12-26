#!/bin/bash
# 注册数据节点到协调节点
set -e

# 默认参数
NODE_ID=${1:-node_1}
NODE_ADDR=${2:-127.0.0.1:9090}
COORD_ADDR=${3:-127.0.0.1:8081}
echo "📌 注册节点 $NODE_ID（地址：$NODE_ADDR）到协调节点 $COORD_ADDR..."

# 调用CLI注册
python -m src.cli.main_cli --coord-addr $COORD_ADDR register-node --node-id $NODE_ID --node-addr $NODE_ADDR

if [ $? -eq 0 ]; then
    echo "✅ 节点 $NODE_ID 注册成功"
else
    echo "❌ 节点 $NODE_ID 注册失败"
    exit 1
fi