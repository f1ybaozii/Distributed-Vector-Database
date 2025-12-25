"""网关CLI交互逻辑"""
import argparse
import json
import requests
from .core import init_gateway, start_gateway
from data_node import start_data_node
from config import GATEWAY_CONFIG, DATA_NODE_CONFIG

def main_cli():
    parser = argparse.ArgumentParser(description="分布式多模态向量数据库CLI")
    subparsers = parser.add_subparsers(dest="command", help="子命令：启动节点/操作数据")

    # 子命令1：启动数据节点
    parser_datanode = subparsers.add_parser("start-datanode", help="启动数据节点")
    parser_datanode.add_argument("--node-id", required=True, help="节点ID，如node1@127.0.0.1:9090")
    parser_datanode.add_argument("--zk-address", default=GATEWAY_CONFIG["zk_address"], help="ZK地址")
    parser_datanode.add_argument("--port", type=int, required=True, help="节点端口")
    parser_datanode.add_argument("--replica-num", type=int, default=DATA_NODE_CONFIG["replica_num"], help="副本数")
    parser_datanode.add_argument("--vector-dim", type=int, default=DATA_NODE_CONFIG["vector_dim"], help="向量维度")

    # 子命令2：启动网关
    parser_gateway = subparsers.add_parser("start-gateway", help="启动网关节点")
    parser_gateway.add_argument("--zk-address", default=GATEWAY_CONFIG["zk_address"], help="ZK地址")
    parser_gateway.add_argument("--model-cache-dir", help="CLIP模型本地路径")
    parser_gateway.add_argument("--host", default=GATEWAY_CONFIG["host"], help="网关监听地址")
    parser_gateway.add_argument("--port", type=int, default=GATEWAY_CONFIG["port"], help="网关端口")

    # 子命令3：上传文件
    parser_add = subparsers.add_parser("add-file", help="上传文件到数据库")
    parser_add.add_argument("--file-path", required=True, help="文件路径")
    parser_add.add_argument("--root-key", required=True, help="文档唯一标识")
    parser_add.add_argument("--attrs", default="{}", help="属性JSON字符串，如'{\"time\":\"2025-01-01\"}'")
    parser_add.add_argument("--replica-num", type=int, default=DATA_NODE_CONFIG["replica_num"], help="副本数")
    parser_add.add_argument("--gateway-url", default=f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}", help="网关地址")

    # 子命令4：检索数据
    parser_query = subparsers.add_parser("query", help="检索数据")
    parser_query.add_argument("--file-path", help="检索文件路径（文本/图片）")
    parser_query.add_argument("--vector", help="检索向量（JSON数组）", default=None)
    parser_query.add_argument("--filter", default="{}", help="过滤条件JSON字符串")
    parser_query.add_argument("--top-k", type=int, default=10, help="返回数量")
    parser_query.add_argument("--aggregate", type=bool, default=True, help="是否聚合到文档")
    parser_query.add_argument("--gateway-url", default=f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}", help="网关地址")

    # 子命令5：删除数据
    parser_delete = subparsers.add_parser("delete", help="删除数据")
    parser_delete.add_argument("--root-key", required=True, help="文档唯一标识")
    parser_delete.add_argument("--gateway-url", default=f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}", help="网关地址")

    # 解析参数并执行
    args = parser.parse_args()
    if args.command == "start-datanode":
        start_data_node(
            node_id=args.node_id,
            zk_address=args.zk_address,
            port=args.port,
            replica_num=args.replica_num,
            vector_dim=args.vector_dim
        )
    elif args.command == "start-gateway":
        init_gateway(
            zk_address=args.zk_address,
            model_cache_dir=args.model_cache_dir
        )
        start_gateway(
            host=args.host,
            port=args.port
        )
    elif args.command == "add-file":
        
        files = {"file": open(args.file_path, "rb")}
        data = {
            "key": args.root_key,
            "attrs": args.attrs,
            "replica_num": args.replica_num
        }
        resp = requests.post(f"{args.gateway_url}/add_file", files=files, data=data)
        
        # 仅加这1行核心校验：先判断状态码，再解析JSON
        if resp.status_code != 200:
            print(f"上传失败！状态码：{resp.status_code}，响应内容：{resp.text}")
        else:
            print(f"上传结果：{resp.json()}")
    elif args.command == "query":
        data = {
            "file_path": args.file_path,
            "vector": json.loads(args.vector) if args.vector else None,
            "filter": json.loads(args.filter),
            "top_k": args.top_k,
            "aggregate": args.aggregate
        }
        resp = requests.post(f"{args.gateway_url}/query", json=data)
        print(f"检索结果：{json.dumps(resp.json(), ensure_ascii=False, indent=2)}")
    elif args.command == "delete":
        data = {"key": args.root_key}
        resp = requests.post(f"{args.gateway_url}/delete", json=data)
        print(f"删除结果：{resp.json()}")
    else:
        parser.print_help()