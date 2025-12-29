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
    VectorData, SearchRequest, Response, SearchResult
)
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from src.vector_db.CoordinatorService import Iface

# ---------------- RPC连接池 ----------------
class RPCClientPool:
    """RPC连接池：管理数据节点的RPC连接复用"""
    def __init__(self):
        self.pool: Dict[str, list] = {}  # node_id -> [(client, transport), ...]
        self.lock = threading.Lock()
        self.idle_timeout = RPC_POOL_IDLE_TIMEOUT
        self.max_size = RPC_POOL_SIZE

    def get_client(self, node_id: str) -> Tuple[VectorNodeService.Client, TTransport.TBufferedTransport]:
        with self.lock:
            zk_manager = get_zk_manager()
            nodes = zk_manager.get_all_nodes()
            if node_id not in nodes:
                logger.error(f"节点{node_id}不存在")
                return None, None
            if node_id not in self.pool or not self.pool[node_id]:
                host, port = nodes[node_id].split(":")
                transport = TSocket.TSocket(host, int(port))
                transport.setTimeout(RPC_TIMEOUT)
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = VectorNodeService.Client(protocol)
                transport.open()
                self.pool[node_id] = [(client, transport)]
            return self.pool[node_id].pop(0)

    def release_client(self, node_id: str, client: VectorNodeService.Client, transport: TTransport.TBufferedTransport):
        with self.lock:
            if node_id not in self.pool:
                self.pool[node_id] = []
            if len(self.pool[node_id]) < self.max_size:
                self.pool[node_id].append((client, transport))
            else:
                transport.close()

    def close_all(self):
        with self.lock:
            for node_id in self.pool:
                for _, transport in self.pool[node_id]:
                    if transport.isOpen():
                        transport.close()
            self.pool.clear()
        logger.info("RPC连接池已清空")

_rpc_pool = None
def get_rpc_client_pool() -> RPCClientPool:
    global _rpc_pool
    if _rpc_pool is None:
        _rpc_pool = RPCClientPool()
    return _rpc_pool

# ---------------- 协调节点 Handler ----------------
class CoordinatorHandler(Iface):
    """协调节点业务处理器"""
    def __init__(self):
        self.zk_manager = get_zk_manager()
        self.shard_count = SHARD_COUNT
        self.replica_count = REPLICA_COUNT
        self.rpc_pool = get_rpc_client_pool()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"\n接收到退出信号 {signum}，清理协调节点资源...")
        self.rpc_pool.close_all()
        self.zk_manager.close()
        logger.info("协调节点资源清理完成，退出")
        sys.exit(0)

    # ---------------- 节点管理 ----------------
    def register_node(self, node_id: str, address: str) -> Response:
        try:
            self.zk_manager.register_node(node_id, address)
            nodes = list(self.zk_manager.get_all_nodes().keys())
            shard_mapping = assign_shards_to_nodes(nodes, self.shard_count, self.replica_count)
            for shard_id, mapping in shard_mapping.items():
                self.zk_manager.set_shard_mapping(shard_id, mapping["master"], mapping["slaves"])
            return Response(success=True, message=f"节点{node_id}注册成功，已重新分配分片")
        except Exception as e:
            logger.error(f"节点注册失败：{e}")
            return Response(success=False, message=str(e))

    def list_nodes(self) -> Response:
        try:
            nodes = self.zk_manager.get_all_nodes()
            return Response(
                success=True,
                message=f"共{len(nodes)}个在线节点",
                vector_data=VectorData(metadata=nodes)
            )
        except Exception as e:
            return Response(success=False, message=str(e))

    # ---------------- 路由写入 ----------------
    def put(self, data: VectorData) -> Response:
        try:
            shard_id = get_shard_id(data.key)
            shard_nodes = self.zk_manager.get_shard_nodes(shard_id)
            if not shard_nodes:
                return Response(success=False, message=f"分片{shard_id}未分配节点")
            master_node = shard_nodes["master"]
            online_nodes = self.zk_manager.get_all_nodes()
            if master_node not in online_nodes:
                return Response(success=False, message=f"主节点{master_node}已离线")
            client, transport = self.rpc_pool.get_client(master_node)
            if not client:
                self.zk_manager._remove_offline_node(master_node)
                return Response(success=False, message=f"无法连接主节点{master_node}，已标记离线")
            resp = client.put(data)
            self.rpc_pool.release_client(master_node, client, transport)
            return resp
        except Exception as e:
            logger.error(f"PUT路由失败：{e}")
            return Response(success=False, message=str(e))

    def delete(self, key: str) -> Response:
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

    # ---------------- 分布式搜索 ----------------
    def search(self, req: SearchRequest) -> Response:
        """广播搜索 + 全局 top-k 合并"""
        try:
            nodes = self.zk_manager.get_all_nodes()
            if not nodes:
                return Response(success=False, message="无在线数据节点")

            all_keys = []
            all_scores = []
            all_vectors = []
            seen_keys = set()

            # 扩大子节点 top_k，避免全局 top_k 不准确
            sub_req = SearchRequest(
                query_vector=req.query_vector,
                top_k=req.top_k  # 可根据节点数和删除比例适当调整
            )

            for node_id in nodes:
                client, transport = self.rpc_pool.get_client(node_id)
                if not client:
                    continue
                resp = client.search(sub_req)
                logger.info(f"SEARCH节点{node_id}返回：success={resp.success}, results={len(resp.search_result.keys) if resp.search_result else 0}")
                self.rpc_pool.release_client(node_id, client, transport)
                if not resp.success or not resp.search_result:
                    continue
                for k, s, v in zip(resp.search_result.keys, resp.search_result.scores, resp.search_result.vectors):
                    if k in seen_keys:
                        continue
                    seen_keys.add(k)
                    all_keys.append(k)
                    all_scores.append(s)
                    all_vectors.append(v)

            if not all_scores:
                return Response(success=True, search_result=SearchResult([], [], []))

            # 全局排序，取 top_k
            sorted_indices = sorted(range(len(all_scores)), key=lambda i: all_scores[i])
            top_k = req.top_k
            final_keys = [all_keys[i] for i in sorted_indices[:top_k]]
            final_scores = [all_scores[i] for i in sorted_indices[:top_k]]
            final_vectors = [all_vectors[i] for i in sorted_indices[:top_k]]

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
