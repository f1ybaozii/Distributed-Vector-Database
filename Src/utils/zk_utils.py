"""ZK辅助工具函数"""
from kazoo.client import KazooClient

def get_zk_client(zk_address: str) -> KazooClient:
    """创建ZK客户端（复用连接）"""
    zk = KazooClient(hosts=zk_address)
    zk.start()
    return zk

def get_node_by_key(zk: KazooClient, key: str) -> tuple[str, int]:
    """按key哈希路由到数据节点"""
    nodes = zk.get_children("/vector_db/nodes")
    if not nodes:
        raise ValueError("无可用数据节点")
    node_idx = hash(key) % len(nodes)
    node = nodes[node_idx]
    ip_port = node.split("@")[1]
    return ip_port.split(":")[0], int(ip_port.split(":")[1])