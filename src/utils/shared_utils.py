import hashlib
from Config import SHARD_COUNT

def get_shard_id(key: str, shard_count: int = SHARD_COUNT) -> int:
    """哈希分片：key→分片ID（复用计算逻辑）"""
    md5 = hashlib.md5(key.encode()).hexdigest()
    return int(md5, 16) % shard_count

def assign_shards_to_nodes(nodes: list, shard_count: int = SHARD_COUNT, replica_count: int = 2) -> dict:
    """分配分片到节点（轮询策略）"""
    shard_mapping = {}
    if not nodes:
        return shard_mapping
    for shard_id in range(shard_count):
        master_node = nodes[shard_id % len(nodes)]
        slave_nodes = [nodes[(shard_id + i) % len(nodes)] for i in range(1, replica_count + 1)]
        shard_mapping[shard_id] = {
            "master": master_node,
            "slaves": slave_nodes
        }
    return shard_mapping