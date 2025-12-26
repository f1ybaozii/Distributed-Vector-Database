import sys
import signal
import threading
from loguru import logger
from typing import Dict, Tuple
from Config import (
    SHARD_COUNT, REPLICA_COUNT, RPC_TIMEOUT,
    RPC_POOL_SIZE, RPC_POOL_IDLE_TIMEOUT
)
from src.utils import (
    get_zk_manager, get_shard_id, assign_shards_to_nodes
)

# Thrift导入
from src.vector_db import CoordinatorService, VectorNodeService
from src.vector_db.ttypes import (
    VectorData, SearchRequest, Response,SearchResult
)
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from src.vector_db.CoordinatorService import Iface
# RPC连接池（复用连接，避免重复创建）
class RPCClientPool:
    """RPC连接池：管理数据节点的RPC连接复用"""
    def __init__(self):
        self.pool: Dict[str, list] = {}  # node_id -> [(client, transport), ...]
        self.lock = threading.Lock()
        self.idle_timeout = RPC_POOL_IDLE_TIMEOUT
        self.max_size = RPC_POOL_SIZE

    def get_client(self, node_id: str) -> Tuple[VectorNodeService.Client, TTransport.TBufferedTransport]:
        """获取数据节点的RPC客户端（复用连接）"""
        with self.lock:
            # 获取节点地址
            zk_manager = get_zk_manager()
            nodes = zk_manager.get_all_nodes()
            if node_id not in nodes:
                logger.error(f"节点{node_id}不存在")
                return None, None

            # 检查连接池
            if node_id not in self.pool or not self.pool[node_id]:
                # 创建新连接
                host, port = nodes[node_id].split(":")
                transport = TSocket.TSocket(host, int(port))
                transport.setTimeout(RPC_TIMEOUT)
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = VectorNodeService.Client(protocol)
                transport.open()
                self.pool[node_id] = [(client, transport)]
            # 取出连接
            return self.pool[node_id].pop(0)

    def release_client(self, node_id: str, client: VectorNodeService.Client, transport: TTransport.TBufferedTransport):
        """释放客户端回连接池"""
        with self.lock:
            if node_id not in self.pool:
                self.pool[node_id] = []
            # 检查连接池大小
            if len(self.pool[node_id]) < self.max_size:
                self.pool[node_id].append((client, transport))
            else:
                # 超过最大数则关闭
                transport.close()

    def close_all(self):
        """关闭所有连接"""
        with self.lock:
            for node_id in self.pool:
                for _, transport in self.pool[node_id]:
                    if transport.isOpen():
                        transport.close()
            self.pool.clear()
        logger.info("RPC连接池已清空")

# 单例连接池
_rpc_pool = None
def get_rpc_client_pool() -> RPCClientPool:
    global _rpc_pool
    if _rpc_pool is None:
        _rpc_pool = RPCClientPool()
    return _rpc_pool

class CoordinatorHandler(Iface):
    """协调节点业务处理器"""
    def __init__(self):
        # 1. ZK管理器（复用单例）
        self.zk_manager = get_zk_manager()
        self.shard_count = SHARD_COUNT
        self.replica_count = REPLICA_COUNT

        # 2. RPC连接池
        self.rpc_pool = get_rpc_client_pool()

        # 3. 信号注册（优雅退出）
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """优雅退出：释放所有资源"""
        logger.info("\n接收到退出信号，清理协调节点资源...")
        # 关闭RPC连接池
        self.rpc_pool.close_all()
        # 关闭ZK连接
        self.zk_manager.close()
        logger.info("协调节点资源清理完成，退出")
        sys.exit(0)

    def register_node(self, node_id: str, address: str) -> Response:
        """注册数据节点"""
        try:
            self.zk_manager.register_node(node_id, address)
            # 重新分配分片
            nodes = list(self.zk_manager.get_all_nodes().keys())
            shard_mapping = assign_shards_to_nodes(nodes, self.shard_count, self.replica_count)
            for shard_id, mapping in shard_mapping.items():
                self.zk_manager.set_shard_mapping(shard_id, mapping["master"], mapping["slaves"])
            return Response(success=True, message=f"节点{node_id}注册成功，已重新分配分片")
        except Exception as e:
            logger.error(f"节点注册失败：{e}")
            return Response(success=False, message=str(e))

    def list_nodes(self) -> Response:
        """列出所有数据节点"""
        try:
            nodes = self.zk_manager.get_all_nodes()
            return Response(
                success=True,
                message=f"共{len(nodes)}个在线节点",
                vector_data=VectorData(metadata=nodes)
            )
        except Exception as e:
            return Response(success=False, message=str(e))

    # 其他代码不变，仅改造put/delete/get/search中的节点校验逻辑
    def put(self, data: VectorData) -> Response:
        """路由写入请求到分片主节点（新增节点可用性校验）"""
        try:
            # 计算分片ID
            shard_id = get_shard_id(data.key)
            # 获取分片主节点（已过滤离线节点）
            shard_nodes = self.zk_manager.get_shard_nodes(shard_id)
            if not shard_nodes:
                return Response(success=False, message=f"分片{shard_id}未分配节点")
            master_node = shard_nodes["master"]

            # 核心新增：校验节点是否在最新缓存中
            online_nodes = self.zk_manager.get_all_nodes()
            if master_node not in online_nodes:
                return Response(success=False, message=f"主节点{master_node}已离线，无法路由")

            # 从连接池获取客户端
            client, transport = self.rpc_pool.get_client(master_node)
            if not client:
                # 核心新增：获取客户端失败时，主动标记节点为离线
                self.zk_manager._remove_offline_node(master_node)
                return Response(success=False, message=f"无法连接主节点{master_node}，已标记为离线")

            # 调用主节点PUT
            resp = client.put(data)
            # 释放连接
            self.rpc_pool.release_client(master_node, client, transport)
            return resp
        except Exception as e:
            logger.error(f"PUT路由失败：{e}")
            return Response(success=False, message=str(e))

    def delete(self, key: str) -> Response:
        """路由删除请求"""
        try:
            shard_id = get_shard_id(key)
            shard_nodes = self.zk_manager.get_shard_nodes(shard_id)
            if not shard_nodes:
                return Response(success=False, message=f"分片{shard_id}未分配节点")
            master_node = shard_nodes["master"]

            client, transport = self.rpc_pool.get_client(master_node)
            if not client:
                return Response(success=False, message=f"无法连接主节点{master_node}")

            resp = client.delete(key)
            self.rpc_pool.release_client(master_node, client, transport)
            return resp
        except Exception as e:
            logger.error(f"DELETE路由失败：{e}")
            return Response(success=False, message=str(e))

    def get(self, key: str) -> Response:
        """路由获取请求"""
        try:
            shard_id = get_shard_id(key)
            shard_nodes = self.zk_manager.get_shard_nodes(shard_id)
            if not shard_nodes:
                return Response(success=False, message=f"分片{shard_id}未分配节点")
            master_node = shard_nodes["master"]

            client, transport = self.rpc_pool.get_client(master_node)
            if not client:
                return Response(success=False, message=f"无法连接主节点{master_node}")

            resp = client.get(key)
            self.rpc_pool.release_client(master_node, client, transport)
            return resp
        except Exception as e:
            logger.error(f"GET路由失败：{e}")
            return Response(success=False, message=str(e))

    def search(self, req: SearchRequest) -> Response:
        """路由检索请求（广播+结果合并）"""
        try:
            nodes = self.zk_manager.get_all_nodes()
            if not nodes:
                return Response(success=False, message="无在线数据节点")

            # 广播检索请求到所有节点
            all_results = []
            for node_id in nodes:
                client, transport = self.rpc_pool.get_client(node_id)
                if not client:
                    continue
                resp = client.search(req)
                self.rpc_pool.release_client(node_id, client, transport)
                if resp.success:
                    all_results.append(resp.search_result)

            # 合并结果（按相似度排序）
            merged_keys = []
            merged_scores = []
            merged_vectors = []
            for res in all_results:
                merged_keys.extend(res.keys)
                merged_scores.extend(res.scores)
                merged_vectors.extend(res.vectors)

            # 按分数升序（Faiss距离越小越相似）
            sorted_indices = sorted(range(len(merged_scores)), key=lambda i: merged_scores[i])
            top_k = req.top_k
            final_keys = [merged_keys[i] for i in sorted_indices[:top_k]]
            final_scores = [merged_scores[i] for i in sorted_indices[:top_k]]
            final_vectors = [merged_vectors[i] for i in sorted_indices[:top_k]]

            return Response(
                success=True,
                search_result=SearchResult(
                    keys=final_keys,
                    scores=final_scores,
                    vectors=final_vectors
                )
            )
        except Exception as e:
            logger.error(f"SEARCH路由失败：{e}")
            return Response(success=False, message=str(e))