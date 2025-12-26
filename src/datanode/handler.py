import sys
import numpy as np
import faiss
from loguru import logger
from src.vector_db.VectorNodeService import Iface
from src.vector_db.ttypes import VectorData, SearchRequest, SearchResult, Response
from src.utils.zk_manager import get_zk_manager
from src.utils.wal_manager import WALManager
from Config import ZK_NODES_PATH,VECTOR_DIM

class VectorNodeHandler(Iface):
    def __init__(self, node_id):
        # 基础初始化
        self.node_id = node_id
        self.zk_manager = get_zk_manager()
        
        # Faiss索引初始化
        self.dim = VECTOR_DIM
        base_index = faiss.IndexFlatL2(self.dim)
        self.index = faiss.IndexIDMap(base_index)
        
        # 核心映射表（metadata以字典形式存储在meta_map）
        self.vec_id_map = {}  # 业务key → Faiss内部ID
        self.meta_map = {}    # 业务key → 元数据字典（核心）
        self.next_id = 0
        
        # WAL初始化
        self.wal_manager = WALManager(node_id)
        self.wal_manager.replay(self)

    def put(self, data: VectorData, replay_mode=False) -> Response:
        """写入/更新向量（仅保留维度校验的异常处理）"""
        key = data.key
        # 向量维度校验（仅此处保留必要异常处理）
        try:
            vec_np = np.array(data.vector, dtype=np.float32)
            if len(vec_np) != self.dim:
                raise ValueError(f"向量维度错误，要求{self.dim}维，实际{len(vec_np)}维")
            vec = vec_np.reshape(1, -1)
        except ValueError as e:
            logger.error(f"向量{key}格式错误：{str(e)}")
            return Response(success=False, message=f"向量{key}格式错误：{str(e)}")

        # 新增/更新逻辑（无try）
        if key in self.vec_id_map:
            # 更新：删除旧值+重新添加
            old_id = self.vec_id_map[key]
            self.index.remove_ids(np.array([old_id], dtype=np.int64))
            self.index.add_with_ids(vec, np.array([old_id], dtype=np.int64))
            current_id = old_id
        else:
            # 新增：分配新ID
            current_id = self.next_id
            self.index.add_with_ids(vec, np.array([current_id], dtype=np.int64))
            self.vec_id_map[key] = current_id
            self.next_id += 1

        # 存储/更新metadata（字典形式）
        self.meta_map[key] = data.metadata  # data.metadata应为字典/None

        # 写入WAL
        if not replay_mode:
            self.wal_manager.write_log("PUT", key, data.vector, data.metadata, data.timestamp)

        logger.info(f"向量写入成功：{key} (ID: {current_id})")
        return Response(success=True, message=f"向量{key}写入成功")

    def delete(self, key: str, replay_mode=False) -> Response:
        """删除向量（无try，极简逻辑）"""
        if key not in self.vec_id_map:
            logger.warning(f"向量{key}不存在，删除失败")
            return Response(success=False, message=f"向量{key}不存在")
        
        # 删除Faiss索引+映射表
        vec_id = self.vec_id_map[key]
        self.index.remove_ids(np.array([vec_id], dtype=int))
        del self.vec_id_map[key]
        # 同步删除metadata
        if key in self.meta_map:
            del self.meta_map[key]
        
        # 写入WAL
        if not replay_mode:
            self.wal_manager.write_log("DELETE", key)
        
        logger.info(f"向量删除成功：{key}")
        return Response(success=True, message=f"向量{key}删除成功")

    def get(self, key: str) -> Response:
        """获取向量（仅保留Faiss操作的必要异常）"""
        if key not in self.vec_id_map:
            return Response(success=False, message=f"向量{key}不存在")
        
        vec_id = self.vec_id_map[key]
        # 仅对Faiss的reconstruct保留异常处理（防止ID越界）
        try:
            vec = self.index.index.reconstruct(vec_id - 1).tolist()
        except RuntimeError as e:
            logger.error(f"获取向量{key}失败：{str(e)}")
            return Response(success=False, message=f"获取向量{key}失败：{str(e)}")
        
        # 返回包含metadata（字典）的结果
        data = VectorData(
            key=key, 
            vector=vec,
            metadata=self.meta_map.get(key, None)  # 读取字典形式的metadata
        )
        return Response(success=True, vector_data=data)

    def search(self, req: SearchRequest) -> Response:
        """向量检索（仅保留必要异常）"""
        # 预处理查询向量（仅维度校验保留异常）
        try:
            query_vec = np.array(req.query_vector, dtype=np.float32).reshape(1, -1)
            if query_vec.shape[1] != self.dim:
                raise ValueError(f"查询向量维度错误，要求{self.dim}维，实际{query_vec.shape[1]}维")
        except ValueError as e:
            logger.error(f"检索失败：{str(e)}")
            return Response(success=False, message=f"检索失败：{str(e)}")

        top_k = req.top_k if req.top_k > 0 else 5
        distances, indices = self.index.search(query_vec, top_k)

        # 解析结果（无try）
        keys = []
        vectors = []
        valid_scores = []
        metadatas = []  # 存储字典形式的metadata
        
        # 解析过滤条件
        filter_key, filter_value = None, None
        if req.filter and "=" in req.filter:
            filter_key, filter_value = req.filter.split("=", 1)

        for i in range(len(indices[0])):
            vec_id = indices[0][i]
            if vec_id == -1:
                continue
            vec_id = int(vec_id)

            if vec_id not in self.vec_id_map.values():
                continue

            # 查找业务key
            target_key = None
            for key, id in self.vec_id_map.items():
                if id == vec_id:
                    target_key = key
                    break
            if not target_key:
                continue

            # 过滤逻辑（基于字典形式的metadata）
            if filter_key:
                meta = self.meta_map.get(target_key, {})  # 取出字典形式的metadata
                if meta.get(filter_key) != filter_value:
                    continue

            # 重构向量（无try，依赖前期ID校验）
            vec = self.index.index.reconstruct(vec_id).tolist()

            # 距离转相似度
            distance = float(distances[0][i])
            similarity = 1.0 / (1.0 + distance)

            # 填充结果（包含字典形式的metadata）
            keys.append(target_key)
            vectors.append(VectorData(key=target_key, vector=vec))
            valid_scores.append(similarity)
            metadatas.append(self.meta_map.get(target_key, None))

        # 构造结果
        result = SearchResult(
            keys=keys,
            scores=valid_scores,
            vectors=vectors,
            metadatas=metadatas  # 字典列表
        )
        return Response(success=True, search_result=result)

    def replicate(self, data: VectorData, op_type: str) -> Response:
        """副本同步（无try）"""
        if op_type == "PUT":
            return self.put(data, replay_mode=True)
        elif op_type == "DELETE":
            return self.delete(data.key, replay_mode=True)
        else:
            return Response(success=False, message=f"不支持的同步操作：{op_type}")

    def replay_wal(self) -> Response:
        """重放WAL（无try）"""
        self.wal_manager.replay(self)
        return Response(success=True, message="WAL日志重放完成")

    def offline(self) -> Response:
        """优雅下线（无try）"""
        node_path = f"{ZK_NODES_PATH}/{self.node_id}"
        if self.zk_manager.zk.exists(node_path):
            self.zk_manager.zk.delete(node_path)
        
        logger.info(f"节点{self.node_id}已优雅下线")
        return Response(success=True, message=f"节点{self.node_id}已优雅下线")

# 信号处理
import signal
def _signal_handler(signum, frame):
    logger.info("接收到退出信号，清理数据节点资源...")
    sys.exit(0)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)