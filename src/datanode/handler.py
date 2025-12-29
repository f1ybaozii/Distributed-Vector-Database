import sys
import numpy as np
import faiss
from loguru import logger
from src.vector_db.VectorNodeService import Iface
from src.vector_db.ttypes import VectorData, SearchRequest, SearchResult, Response
from src.utils.zk_manager import get_zk_manager
from src.utils.wal_manager import WALManager
from Config import ZK_NODES_PATH,VECTOR_DIM
import threading
import os
import json
import time 
class VectorNodeHandler:
    def __init__(self, node_id):
        self.node_id = node_id
        self.index_lock = threading.RLock()  # 并发锁
        
        # ========== 1. 新增：本地存储目录初始化（核心！）==========
        self.local_storage_dir = f"./Static/local_storage/{self.node_id}"
        self.index_dir = os.path.join(self.local_storage_dir, "faiss_index")
        self.map_dir = os.path.join(self.local_storage_dir, "maps")
        self.wal_dir = os.path.join(self.local_storage_dir, "wal")
        self.checkpoint_dir = os.path.join(self.local_storage_dir, "checkpoint")
        # 创建所有目录（不存在则新建）
        for dir_path in [self.index_dir, self.map_dir, self.wal_dir, self.checkpoint_dir]:
            os.makedirs(dir_path, exist_ok=True)
        
        # ========== 2. 初始化核心变量 ==========
        self.zk_manager = get_zk_manager()  # 复用你的ZK管理器
        self.wal_manager = WALManager(self.wal_dir)  # 复用你的WAL管理器
        self.vec_id_map = {}  # key → Faiss逻辑ID（np.int64）
        self.meta_map = {}    # key → 元数据（时间/标签等）
        self.next_id = 0      # ID自增器
        
        # ========== 3. 启动时加载本地存储（核心！）==========
        self.load_from_checkpoint()  # 优先加载快照，无则加载索引+映射表

    # ========== 4. Faiss索引持久化/加载（核心落盘逻辑）==========
    def save_faiss_index(self):
        """把Faiss索引持久化到本地磁盘"""
        with self.index_lock:
            index_path = os.path.join(self.index_dir, "vector_index.faiss")
            faiss.write_index(self.index, index_path)
            logger.info(f"[本地存储] Faiss索引落盘成功：{index_path}，索引总数：{self.index.index.ntotal}")

    def load_faiss_index(self):
        """从本地磁盘加载Faiss索引到内存"""
        index_path = os.path.join(self.index_dir, "vector_index.faiss")
        if os.path.exists(index_path):
            # 内存映射模式：大索引不占满内存
            self.index = faiss.read_index(index_path, faiss.IO_FLAG_MMAP)
            logger.info(f"[本地存储] 加载Faiss索引成功：{index_path}，索引总数：{self.index.index.ntotal}")
        else:
            # 无本地索引，初始化HNSW索引（比FlatL2快）
            dim = 512  # 替换为你的向量维度！
            base_index = faiss.IndexHNSWFlat(dim, 32)  # HNSW参数：32为邻居数
            self.index = faiss.IndexIDMap(base_index)
            logger.info(f"[本地存储] 无本地索引，初始化空HNSW索引（维度：{dim}）")

    # ========== 5. 业务映射表持久化/加载（落盘key-ID映射）==========
    def save_maps(self):
        """持久化vec_id_map/meta_map/next_id到JSON文件"""
        with self.index_lock:
            map_data = {
                "vec_id_map": {k: int(v) for k, v in self.vec_id_map.items()},  # np.int64转int（JSON兼容）
                "meta_map": self.meta_map,
                "next_id": self.next_id
            }
            map_path = os.path.join(self.map_dir, "id_maps.json")
            with open(map_path, "w", encoding="utf-8") as f:
                json.dump(map_data, f, indent=2)
            logger.info(f"[本地存储] 映射表落盘成功：{map_path}，key数：{len(self.vec_id_map)}")

    def load_maps(self):
        """从本地JSON加载映射表到内存"""
        map_path = os.path.join(self.map_dir, "id_maps.json")
        if os.path.exists(map_path):
            with open(map_path, "r", encoding="utf-8") as f:
                map_data = json.load(f)
            # 恢复时转回np.int64（Faiss要求）
            self.vec_id_map = {k: np.int64(v) for k, v in map_data["vec_id_map"].items()}
            self.meta_map = map_data.get("meta_map", {})
            self.next_id = map_data.get("next_id", 1)
            logger.info(f"[本地存储] 加载映射表成功：{map_path}，key数：{len(self.vec_id_map)}")
        else:
            self.vec_id_map = {}
            self.meta_map = {}
            self.next_id = 1
            logger.info("[本地存储] 无本地映射表，初始化空字典")

    # ========== 6. 快照功能（解决重启加载慢）==========
    def save_checkpoint(self):
        """保存全量快照：Faiss索引+映射表+WAL位置"""
        checkpoint_ts = int(time.time() * 1000)
        checkpoint_path = os.path.join(self.checkpoint_dir, f"checkpoint_{checkpoint_ts}")
        os.makedirs(checkpoint_path, exist_ok=True)
        
        # 保存Faiss索引到快照
        faiss.write_index(self.index, os.path.join(checkpoint_path, "vector_index.faiss"))
        # 保存映射表到快照
        map_data = {
            "vec_id_map": {k: int(v) for k, v in self.vec_id_map.items()},
            "meta_map": self.meta_map,
            "next_id": self.next_id
        }
        with open(os.path.join(checkpoint_path, "id_maps.json"), "w") as f:
            json.dump(map_data, f)
        # 记录快照对应的WAL时间戳
        with open(os.path.join(checkpoint_path, "wal_pos.txt"), "w") as f:
            f.write(str(checkpoint_ts))
        
        logger.info(f"[本地存储] 快照保存成功：{checkpoint_path}")

    def load_from_checkpoint(self):
        """优先加载最新快照，再重放增量WAL"""
        # 找最新快照
        checkpoint_dirs = [d for d in os.listdir(self.checkpoint_dir) if d.startswith("checkpoint_")]
        if checkpoint_dirs:
            latest_checkpoint = sorted(checkpoint_dirs)[-1]
            checkpoint_path = os.path.join(self.checkpoint_dir, latest_checkpoint)
            
            # 加载快照中的Faiss索引
            self.index = faiss.read_index(os.path.join(checkpoint_path, "vector_index.faiss"))
            # 加载快照中的映射表
            with open(os.path.join(checkpoint_path, "id_maps.json"), "r") as f:
                map_data = json.load(f)
            self.vec_id_map = {k: np.int64(v) for k, v in map_data["vec_id_map"].items()}
            self.meta_map = map_data["meta_map"]
            self.next_id = map_data["next_id"]
            
            # 重放快照后的增量WAL
            with open(os.path.join(checkpoint_path, "wal_pos.txt"), "r") as f:
                checkpoint_ts = int(f.read())
            self.wal_manager.replay_incremental(self, checkpoint_ts)  # 需实现WAL增量重放
            logger.info(f"[本地存储] 加载最新快照：{latest_checkpoint}，并重放增量WAL")
        else:
            # 无快照，直接加载索引+映射表
            self.load_faiss_index()
            self.load_maps()

    # ========== 7. 改造put方法（添加自动落盘）==========
    def put(self, data: VectorData, replay_mode=False) -> Response:
        key = data.key
        vec = np.array(data.vector, dtype=np.float32).reshape(1, -1)
        
        with self.index_lock:
            old_ntotal = self.index.index.ntotal
            old_id = self.vec_id_map.get(key)
            
            # 1. 删除旧key（重复key覆盖）
            if old_id is not None:
                del_id = np.int64(old_id)
                del_count = self.index.remove_ids(np.array([del_id], dtype=np.int64))
                if del_count == 0:
                    logger.error(f"[PUT] 删除旧key={key}失败，ID={del_id}")
                    return Response(success=False, message=f"删除旧key失败：{key}")
                del self.vec_id_map[key]
                if key in self.meta_map:
                    del self.meta_map[key]
                logger.info(f"[PUT] 成功删除旧key={key}，ID={del_id}")
            
            # 2. 添加新key
            new_id = np.int64(self.next_id)
            self.index.add_with_ids(vec, np.array([new_id], dtype=np.int64))
            self.vec_id_map[key] = new_id
            if data.metadata:
                self.meta_map[key] = data.metadata
            self.next_id += 1
            
            # 3. 自动落盘（非重放模式）
            if not replay_mode:
                self.save_faiss_index()
                self.save_maps()
                self.wal_manager.write_log("PUT", key, data.vector, data.metadata)
                # 定期保存快照（每100次操作，可自定义）
                if self.next_id % 100 == 0:
                    self.save_checkpoint()
        
        logger.info(f"[PUT] key={key}写入成功，新ID={new_id}，索引总数={self.index.index.ntotal}")
        return Response(success=True, message=f"key={key}写入成功")

    # ========== 8. 改造delete方法（添加自动落盘）==========
    def delete(self, key: str, replay_mode=False) -> Response:
        if key not in self.vec_id_map:
            logger.warning(f"[DELETE] key={key}不存在")
            return Response(success=False, message=f"key={key}不存在")
        
        with self.index_lock:
            vec_id = self.vec_id_map[key]
            del_count = self.index.remove_ids(np.array([vec_id], dtype=np.int64))
            if del_count == 0:
                logger.error(f"[DELETE] key={key}删除失败，ID={vec_id}")
                return Response(success=False, message=f"删除失败：{key}")
            
            # 删除映射表
            del self.vec_id_map[key]
            if key in self.meta_map:
                del self.meta_map[key]
            
            # 自动落盘（非重放模式）
            if not replay_mode:
                self.save_faiss_index()
                self.save_maps()
                self.wal_manager.write_log("DELETE", key)
        
        logger.info(f"[DELETE] key={key}删除成功")
        return Response(success=True, message=f"key={key}删除成功")

    # ========== 9. 检索方法（复用你修复后的逻辑）==========
    def search(self, req: SearchRequest) -> Response:
        query_vec = np.array(req.query_vector, dtype=np.float32).reshape(1, -1)
        top_k = req.top_k if req.top_k > 0 else 5
        
        distances, indices = self.index.search(query_vec, top_k)
        valid_logic_ids = set(self.index.id_map) if hasattr(self.index, "id_map") else set()
        
        keys = []
        vectors = []
        valid_scores = []
        for i in range(len(indices[0])):
            vec_id = indices[0][i]
            if vec_id == -1:
                continue
            vec_id_64 = np.int64(vec_id)
            
            # 校验ID有效性
            if vec_id_64 not in valid_logic_ids or vec_id_64 not in self.vec_id_map.values():
                logger.warning(f"[SEARCH] 无效ID：{vec_id}，跳过")
                continue
            
            # 查找业务key
            target_key = None
            for k, v in self.vec_id_map.items():
                if v == vec_id_64:
                    target_key = k
                    break
            if not target_key:
                continue
            
            # 重构向量
            try:
                vec = self.index.reconstruct(vec_id_64).tolist()
            except Exception as e:
                logger.error(f"[SEARCH] 重构ID={vec_id}失败：{e}")
                continue
            
            keys.append(target_key)
            vectors.append(VectorData(key=target_key, vector=vec))
            valid_scores.append(float(distances[0][i]))
        
        result = SearchResult(keys=keys, scores=valid_scores, vectors=vectors)
        return Response(success=True, search_result=result)

# 信号处理
import signal
def _signal_handler(signum, frame):
    logger.info("接收到退出信号，清理数据节点资源...")
    sys.exit(0)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)