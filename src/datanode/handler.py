import sys
import numpy as np
import hnswlib
import plyvel
import json
import time
import os
import threading
import signal
import shutil
from loguru import logger

# 原有业务导入（保持不变）
from src.vector_db.VectorNodeService import Iface
from src.vector_db.ttypes import VectorData, SearchRequest, SearchResult, Response
from src.utils.zk_manager import get_zk_manager
from src.utils.wal_manager import WALManager
from Config import ZK_NODES_PATH, VECTOR_DIM

class VectorNodeHandler:
    def __init__(self, node_id):
        self.node_id = node_id
        self.index_lock = threading.RLock()  # 全局并发锁
        self.leveldb_lock = threading.Lock()  # LevelDB专属锁
        
        # 1. 本地存储目录初始化
        self.local_storage_dir = f"./Static/local_storage/{self.node_id}"
        self.hnsw_index_dir = os.path.join(self.local_storage_dir, "hnsw_index")
        self.leveldb_dir = os.path.join(self.local_storage_dir, "leveldb_data")
        self.wal_dir = os.path.join(self.local_storage_dir, "wal")
        self.checkpoint_dir = os.path.join(self.local_storage_dir, "checkpoint")
        self.deleted_ids_path = os.path.join(self.local_storage_dir, "deleted_ids.json")
        
        # 创建目录
        for dir_path in [self.hnsw_index_dir, self.leveldb_dir, self.wal_dir, self.checkpoint_dir]:
            os.makedirs(dir_path, exist_ok=True)
        
        # 2. 核心组件初始化
        self.zk_manager = get_zk_manager()
        self.wal_manager = WALManager(self.wal_dir)
        self.vector_dim = VECTOR_DIM
        self.next_hnsw_id = 0  # HNSW自增ID
        self.deleted_ids = set()  # 软删除ID集合（HNSW不支持物理删除）
        
        # 3. HNSWlib 初始化（核心索引）
        self.hnsw_index = hnswlib.Index(space='l2', dim=self.vector_dim)  # L2距离，可改为cosine
        self._init_hnsw_index()
        
        # 4. LevelDB 初始化（存储key→(hnsw_id, vector, metadata)）
        self.leveldb = plyvel.DB(self.leveldb_dir, create_if_missing=True, write_buffer_size=64*1024*1024)
        
        # 5. 加载软删除ID + 快照恢复
        self._load_deleted_ids()
        self.load_from_checkpoint()
        
        # 6. 注册退出钩子
        import atexit
        atexit.register(self._on_exit)

    # ========== 退出时保存快照 + 清理资源 ==========
    def _on_exit(self):
        logger.info("执行退出逻辑：保存快照 + 关闭资源...")
        with self.index_lock:
            # 1. 保存HNSW索引
            self.hnsw_index.save_index(os.path.join(self.hnsw_index_dir, "index.bin"))
            # 2. 保存软删除ID
            self._save_deleted_ids()
            # 3. 保存快照
            self.save_checkpoint()
            # 4. 关闭LevelDB
            self.leveldb.close()
        logger.info("退出逻辑执行完成")

    # ========== HNSWlib 核心操作 ==========
    def _init_hnsw_index(self):
        """初始化HNSW索引（加载已有索引或新建）"""
        index_path = os.path.join(self.hnsw_index_dir, "index.bin")
        if os.path.exists(index_path):
            # 加载已有索引
            self.hnsw_index.load_index(index_path)
            # 恢复next_hnsw_id（索引中已有的元素数）
            self.next_hnsw_id = self.hnsw_index.get_current_count()
            logger.info(f"加载HNSW索引成功，当前元素数：{self.next_hnsw_id}")
        else:
            # 新建索引（参数：max_elements=初始容量，ef_construction=构建时的ef，M=邻居数）
            self.hnsw_index.init_index(max_elements=1000000, ef_construction=128, M=32)
            # 设置查询时的ef（越大越准，越慢）
            self.hnsw_index.set_ef(64)
            logger.info(f"初始化新HNSW索引，维度：{self.vector_dim}")

    def _rebuild_hnsw_index(self):
        """定期重建HNSW索引（清理已删除ID，释放空间）"""
        logger.info("开始重建HNSW索引（清理已删除ID）...")
        with self.index_lock:
            # 1. 导出有效数据
            valid_ids = []
            valid_vectors = []
            for hnsw_id in range(self.next_hnsw_id):
                if hnsw_id not in self.deleted_ids:
                    # 从LevelDB获取原始向量
                    key = self._get_key_by_hnsw_id(hnsw_id)
                    if key:
                        vec_data = self.leveldb.get(key.encode('utf-8'))
                        if vec_data:
                            vec_dict = json.loads(vec_data)
                            valid_vectors.append(np.array(vec_dict['vector'], dtype=np.float32))
                            valid_ids.append(hnsw_id)
            
            # 2. 重建索引
            self.hnsw_index = hnswlib.Index(space='l2', dim=self.vector_dim)
            self.hnsw_index.init_index(max_elements=len(valid_vectors) + 10000, ef_construction=128, M=32)
            self.hnsw_index.add_items(valid_vectors, valid_ids)
            self.hnsw_index.set_ef(64)
            # 3. 保存新索引
            self.hnsw_index.save_index(os.path.join(self.hnsw_index_dir, "index.bin"))
            # 4. 清空已删除ID
            self.deleted_ids.clear()
            self._save_deleted_ids()
        
        logger.info(f"HNSW索引重建完成，有效元素数：{len(valid_vectors)}")

    # ========== 软删除ID管理 ==========
    def _save_deleted_ids(self):
        """保存已删除ID到文件"""
        with open(self.deleted_ids_path, 'w', encoding='utf-8') as f:
            json.dump(list(self.deleted_ids), f)

    def _load_deleted_ids(self):
        """加载已删除ID"""
        if os.path.exists(self.deleted_ids_path):
            with open(self.deleted_ids_path, 'r', encoding='utf-8') as f:
                self.deleted_ids = set(json.load(f))
            logger.info(f"加载已删除ID数：{len(self.deleted_ids)}")

    # ========== LevelDB 辅助方法（key-HNSW ID映射）==========
    def _get_hnsw_id_by_key(self, key: str) -> int:
        """根据key查HNSW ID"""
        with self.leveldb_lock:
            vec_data = self.leveldb.get(key.encode('utf-8'))
            if not vec_data:
                return -1
            vec_dict = json.loads(vec_data)
            return vec_dict['hnsw_id']

    def _get_key_by_hnsw_id(self, hnsw_id: int) -> str:
        """根据HNSW ID查key（反向映射）"""
        with self.leveldb_lock:
            # 遍历LevelDB（小数据量可用，大数据量建议维护反向映射表）
            for key, value in self.leveldb.iterator():
                vec_dict = json.loads(value)
                if vec_dict['hnsw_id'] == hnsw_id:
                    return key.decode('utf-8')
            return ""

    # ========== 快照功能 ==========
    def save_checkpoint(self):
        """保存全量快照：HNSW索引 + LevelDB数据 + 软删除ID"""
        checkpoint_ts = int(time.time() * 1000)
        checkpoint_path = os.path.join(self.checkpoint_dir, f"checkpoint_{checkpoint_ts}")
        os.makedirs(checkpoint_path, exist_ok=True)
        
        # 1. 保存HNSW索引
        hnsw_checkpoint_path = os.path.join(checkpoint_path, "index.bin")
        self.hnsw_index.save_index(hnsw_checkpoint_path)
        
        # 2. 拷贝LevelDB数据
        leveldb_checkpoint_path = os.path.join(checkpoint_path, "leveldb_data")
        shutil.copytree(self.leveldb_dir, leveldb_checkpoint_path, dirs_exist_ok=True)
        
        # 3. 保存软删除ID
        deleted_ids_checkpoint_path = os.path.join(checkpoint_path, "deleted_ids.json")
        with open(deleted_ids_checkpoint_path, 'w', encoding='utf-8') as f:
            json.dump(list(self.deleted_ids), f)
        
        # 4. 记录WAL位置
        with open(os.path.join(checkpoint_path, "wal_pos.txt"), "w") as f:
            f.write(str(checkpoint_ts))
        
        logger.info(f"快照保存成功：{checkpoint_path}")

    def load_from_checkpoint(self):
        """从最新快照恢复"""
        checkpoint_dirs = [d for d in os.listdir(self.checkpoint_dir) if d.startswith("checkpoint_")]
        if not checkpoint_dirs:
            logger.info("无快照，使用当前数据")
            return
        
        # 加载最新快照
        latest_checkpoint = sorted(checkpoint_dirs)[-1]
        checkpoint_path = os.path.join(self.checkpoint_dir, latest_checkpoint)
        
        # 1. 恢复HNSW索引
        hnsw_checkpoint_path = os.path.join(checkpoint_path, "index.bin")
        if os.path.exists(hnsw_checkpoint_path):
            self.hnsw_index.load_index(hnsw_checkpoint_path,max_elements=1000000)
            self.next_hnsw_id = self.hnsw_index.get_current_count()
            logger.info(f"恢复HNSW索引：{hnsw_checkpoint_path}")
        
        # 2. 恢复LevelDB数据
        leveldb_checkpoint_path = os.path.join(checkpoint_path, "leveldb_data")
        if os.path.exists(leveldb_checkpoint_path):
            self.leveldb.close()
            shutil.rmtree(self.leveldb_dir, ignore_errors=True)
            shutil.copytree(leveldb_checkpoint_path, self.leveldb_dir)
            self.leveldb = plyvel.DB(self.leveldb_dir, create_if_missing=True)
            logger.info(f"恢复LevelDB数据：{leveldb_checkpoint_path}")
        
        # 3. 恢复软删除ID
        deleted_ids_checkpoint_path = os.path.join(checkpoint_path, "deleted_ids.json")
        if os.path.exists(deleted_ids_checkpoint_path):
            with open(deleted_ids_checkpoint_path, 'r', encoding='utf-8') as f:
                self.deleted_ids = set(json.load(f))
            logger.info(f"恢复已删除ID数：{len(self.deleted_ids)}")
        
        # 4. 重放增量WAL
        with open(os.path.join(checkpoint_path, "wal_pos.txt"), "r") as f:
            checkpoint_ts = int(f.read())
        self.wal_manager.replay_incremental(self, checkpoint_ts)
        logger.info(f"加载最新快照：{latest_checkpoint}，并重放增量WAL")

    # ========== 核心业务接口 ==========
    def put(self, data: VectorData, replay_mode=False) -> Response:
        key = data.key
        vec = np.array(data.vector, dtype=np.float32)
        metadata = data.metadata or {}

        # ===== 基础合法性检查（防止维度污染索引）=====
        if vec.ndim != 1 or vec.shape[0] != self.vector_dim:
            return Response(
                success=False,
                message=f"vector dim mismatch: expect {self.vector_dim}, got {vec.shape}"
            )

        with self.index_lock:
            # ===== 1. 索引健康检查（关键修复点）=====
            try:
                current_count = self.hnsw_index.get_current_count()
                max_elements = self.hnsw_index.get_max_elements()
            except Exception as e:
                logger.error(f"HNSW index state invalid, rebuilding: {e}")
                self._rebuild_hnsw_index()
                current_count = self.hnsw_index.get_current_count()
                max_elements = self.hnsw_index.get_max_elements()

            # ===== 2. 防止 index 已满或被异常污染 =====
            if current_count >= max_elements:
                logger.error(
                    f"HNSW index full or corrupted "
                    f"(current={current_count}, max={max_elements}), rebuilding"
                )
                self._rebuild_hnsw_index()

            # ===== 3. 处理 key 覆盖（软删除旧 ID）=====
            old_hnsw_id = self._get_hnsw_id_by_key(key)
            if old_hnsw_id != -1:
                self.deleted_ids.add(old_hnsw_id)
                with self.leveldb_lock:
                    self.leveldb.delete(key.encode("utf-8"))
                logger.info(
                    f"PUT overwrite: key={key}, old_hnsw_id={old_hnsw_id} marked deleted"
                )

            # ===== 4. 分配新 HNSW ID（连续、受控）=====
            new_hnsw_id = self.next_hnsw_id

            # ===== 5. 写入 HNSW（单点失败即中断）=====
            try:
                self.hnsw_index.add_items(
                    vec.reshape(1, -1),
                    np.array([new_hnsw_id], dtype=np.int64)
                )
            except RuntimeError as e:
                # 这是你现在遇到的核心异常兜底点
                logger.error(f"HNSW add_items failed, rebuilding index: {e}")
                self._rebuild_hnsw_index()

                # rebuild 后重试一次（只允许一次）
                new_hnsw_id = self.next_hnsw_id
                self.hnsw_index.add_items(
                    vec.reshape(1, -1),
                    np.array([new_hnsw_id], dtype=np.int64)
                )

            # ===== 6. ID 递增（只在 add 成功后）=====
            self.next_hnsw_id += 1

            # ===== 7. 写入 LevelDB（原子性在锁内）=====
            vec_dict = {
                "hnsw_id": new_hnsw_id,
                "vector": vec.tolist(),
                "metadata": metadata
            }
            with self.leveldb_lock:
                self.leveldb.put(
                    key.encode("utf-8"),
                    json.dumps(vec_dict).encode("utf-8")
                )

            # ===== 8. WAL + 持久化（非 replay）=====
            if not replay_mode:
                try:
                    self.hnsw_index.save_index(
                        os.path.join(self.hnsw_index_dir, "index.bin")
                    )
                    self._save_deleted_ids()
                    self.wal_manager.write_log(
                        "PUT", key, data.vector, metadata
                    )
                except Exception as e:
                    logger.error(f"Persistence failed after PUT key={key}: {e}")

                # ===== 9. 周期性维护 =====
                if self.next_hnsw_id % 200000 == 0:
                    self._rebuild_hnsw_index()

                if self.next_hnsw_id % 2000 == 0:
                    self.save_checkpoint()

        logger.info(f"PUT success: key={key}, hnsw_id={new_hnsw_id}")
        return Response(success=True, message=f"key={key} 写入成功")


    def delete(self, key: str, replay_mode=False) -> Response:
        with self.index_lock:
            # 1. 查HNSW ID
            hnsw_id = self._get_hnsw_id_by_key(key)
            if hnsw_id == -1:
                logger.warning(f"DELETE key={key}不存在")
                return Response(success=False, message=f"key={key}不存在")
            
            # 2. 标记删除 + 删除LevelDB数据
            self.deleted_ids.add(hnsw_id)
            with self.leveldb_lock:
                self.leveldb.delete(key.encode('utf-8'))
            
            # 3. 自动落盘 + WAL
            if not replay_mode:
                self._save_deleted_ids()
                self.wal_manager.write_log("DELETE", key)
        
        logger.info(f"DELETE key={key}成功，标记HNSW ID={hnsw_id}为删除")
        return Response(success=True, message=f"key={key}删除成功")

    def search(self, req: SearchRequest) -> Response:
        query_vec = np.array(req.query_vector, dtype=np.float32).reshape(1, -1)
        top_k = req.top_k if req.top_k > 0 else 5
        threshold = req.threshold

        with self.index_lock:
            current_count = self.hnsw_index.get_current_count()

            # ====== 核心修复 1：元素不足，直接返回 ======
            if current_count == 0:
                return Response(success=True, search_result=SearchResult(keys=[], scores=[], vectors=[]))

            # k 不能超过 current_count
            k = min(top_k, current_count)

            # ====== 核心修复 2：ef 必须 >= k ======
            ef = max(50, k * 2)
            self.hnsw_index.set_ef(ef)

            try:
                indices, distances = self.hnsw_index.knn_query(query_vec, k=k*2)
                logger.info("indices, distances:", indices, distances)
            except RuntimeError as e:
                # ====== 核心修复 3：一旦异常，索引视为不可用 ======
                logger.error(f"HNSW knn_query failed: {e}")
                return Response(success=False, message="HNSW index corrupted, search aborted")

            keys = []
            vectors = []
            scores = []

            for i in range(len(indices[0])):
                hnsw_id = int(indices[0][i])

                if hnsw_id in self.deleted_ids:
                    logger.info("跳过已删除ID:", hnsw_id)
                    continue

                key = self._get_key_by_hnsw_id(hnsw_id)
                if not key:
                    logger.warning("HNSW ID无对应Key，跳过:", hnsw_id)
                    continue

                vec_data = self.leveldb.get(key.encode("utf-8"))
                if not vec_data:
                    logger.warning("LevelDB无对应数据，跳过Key:", key)
                    continue

                vec_dict = json.loads(vec_data)
                score = float(distances[0][i])
                # if score > threshold:
                #     logger.info("跳过低于阈值的结果:", score)
                #     continue

                keys.append(key)
                vectors.append(VectorData(key=key, vector=vec_dict["vector"], metadata=vec_dict["metadata"]))
                scores.append(score)

                if len(keys) >= top_k:
                    break

            return Response(
                success=True,
                search_result=SearchResult(keys=keys, scores=scores, vectors=vectors)
            )


    def get(self, key: str) -> Response:
        with self.leveldb_lock:
            vec_data = self.leveldb.get(key.encode('utf-8'))
            if not vec_data:
                return Response(success=False, message=f"key={key}不存在")
            
            # 解析向量和元数据
            vec_dict = json.loads(vec_data)
            # 检查是否被删除
            if vec_dict['hnsw_id'] in self.deleted_ids:
                return Response(success=False, message=f"key={key}已被删除")
            
            data = VectorData(
                key=key,
                vector=vec_dict['vector'],
                metadata=vec_dict['metadata']
            )
            return Response(success=True, vector_data=data)

# ========== 信号处理 ==========
def _signal_handler(signum, frame):
    logger.info(f"接收到退出信号 {signum}，触发退出逻辑...")
    sys.exit(0)

# 注册信号
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)