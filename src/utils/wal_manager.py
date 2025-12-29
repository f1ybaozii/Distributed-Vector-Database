import os
import json
import time
import shutil
from loguru import logger
from typing import Dict, List
from src.vector_db.ttypes import VectorData

class WALManager:
    def __init__(self, node_id):
        # 核心：与VectorNodeHandler的WAL目录保持一致
        self.node_id = node_id
        self.wal_root_dir = f"./Static/local_storage/{self.node_id}/wal"
        self.wal_data_dir = os.path.join(self.wal_root_dir, "data")  # 存储WAL日志文件
        self.wal_checkpoint_dir = os.path.join(self.wal_root_dir, "checkpoint")  # 存储重放位点
        
        # 初始化目录
        os.makedirs(self.wal_data_dir, exist_ok=True)
        os.makedirs(self.wal_checkpoint_dir, exist_ok=True)
        
        # 日志滚动配置
        self.max_log_size = 10 * 1024 * 1024  # 单个日志文件最大10MB
        self.max_log_age = 7 * 24 * 3600      # 日志保留7天
        
        # 运行时状态
        self.current_log_file = self._get_current_log_file()
        self.replayed = False
        self.checkpoint_ts = self._load_checkpoint_ts()

    # ========== 辅助方法：日志文件管理 ==========
    def _get_current_log_file(self) -> str:
        """获取当前写入的日志文件（按大小滚动）"""
        log_files = sorted([
            f for f in os.listdir(self.wal_data_dir) 
            if f.startswith("wal_") and f.endswith(".log")
        ])
        
        # 无日志文件时新建
        if not log_files:
            new_file = os.path.join(self.wal_data_dir, f"wal_{int(time.time() * 1000)}.log")
            return new_file
        
        # 检查最后一个文件是否超过大小阈值
        last_file = os.path.join(self.wal_data_dir, log_files[-1])
        if os.path.getsize(last_file) < self.max_log_size:
            return last_file
        else:
            # 新建日志文件
            new_file = os.path.join(self.wal_data_dir, f"wal_{int(time.time() * 1000)}.log")
            return new_file

    def _load_checkpoint_ts(self) -> int:
        """加载上次重放的位点（时间戳）"""
        checkpoint_file = os.path.join(self.wal_checkpoint_dir, "checkpoint_ts.txt")
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                return int(f.read().strip())
        return 0

    def _save_checkpoint_ts(self, ts: int):
        """保存重放位点（避免重复重放）"""
        checkpoint_file = os.path.join(self.wal_checkpoint_dir, "checkpoint_ts.txt")
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            f.write(str(ts))
        self.checkpoint_ts = ts

    def _clean_expired_logs(self):
        """清理过期/过大的日志文件"""
        current_ts = int(time.time())
        log_files = [f for f in os.listdir(self.wal_data_dir) if f.startswith("wal_") and f.endswith(".log")]
        for log_file in log_files:
            file_path = os.path.join(self.wal_data_dir, log_file)
            # 按时间清理（日志文件名中的时间戳）
            file_ts = int(log_file.split("_")[1].split(".")[0]) / 1000
            if current_ts - file_ts > self.max_log_age:
                os.remove(file_path)
                logger.info(f"清理过期WAL日志：{log_file}")

    # ========== 核心方法：写入WAL日志 ==========
    def write_log(self, op_type: str, key: str, vector=None, metadata=None, timestamp=None):
        """
        写入WAL日志（原子写入+按大小滚动）
        :param op_type: PUT/DELETE
        :param key: 向量Key
        :param vector: 向量列表（PUT时传）
        :param metadata: 元数据字典（PUT时传）
        :param timestamp: 操作时间戳（默认当前时间）
        """
        # 构造日志条目
        log_ts = timestamp or int(time.time() * 1000)
        log_entry = {
            "op_type": op_type,
            "key": key,
            "vector": vector,
            "metadata": metadata,
            "timestamp": log_ts,
            "node_id": self.node_id
        }
        
        # 原子写入（临时文件+重命名，避免崩溃导致日志损坏）
        temp_file = f"{self.current_log_file}.tmp"
        with open(temp_file, "a", encoding="utf-8") as f:
            # 每行一个JSON，便于逐行读取
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        os.rename(temp_file, self.current_log_file)
        
        # 检查是否需要滚动日志文件
        if os.path.getsize(self.current_log_file) >= self.max_log_size:
            self.current_log_file = self._get_current_log_file()
        
        # 定期清理过期日志（每100次操作执行一次）
        if log_ts % 100 == 0:
            self._clean_expired_logs()

    # ========== 核心方法：全量重放WAL ==========
    def replay(self, handler):
        """全量重放WAL日志（节点首次启动/无快照时调用）"""
        if self.replayed:
            logger.info("WAL日志已重放，跳过")
            return
        
        logger.info(f"开始重放{self.node_id}的全量WAL日志...")
        # 1. 按时间排序所有日志文件
        log_files = sorted([
            os.path.join(self.wal_data_dir, f) 
            for f in os.listdir(self.wal_data_dir) 
            if f.startswith("wal_") and f.endswith(".log")
        ])
        
        # 2. 内存去重：保留每个key的最后一次操作
        unique_ops: Dict[str, dict] = {}
        max_ts = 0
        
        for log_file in log_files:
            try:
                with open(log_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        # 容错：跳过损坏的JSON行
                        try:
                            log_entry = json.loads(line)
                        except json.JSONDecodeError:
                            logger.warning(f"跳过损坏的WAL日志行：{log_file} -> {line[:50]}...")
                            continue
                        # 更新最新操作
                        key = log_entry["key"]
                        unique_ops[key] = log_entry
                        # 记录最大时间戳
                        if log_entry["timestamp"] > max_ts:
                            max_ts = log_entry["timestamp"]
            except Exception as e:
                logger.error(f"读取WAL日志文件失败：{log_file}，错误：{e}")
                continue
        
        # 3. 执行重放
        processed = 0
        for log_entry in unique_ops.values():
            op_type = log_entry["op_type"]
            key = log_entry["key"]
            try:
                if op_type == "PUT":
                    data = VectorData(
                        key=key,
                        vector=log_entry["vector"],
                        metadata=log_entry.get("metadata"),
                        timestamp=log_entry["timestamp"]
                    )
                    handler.put(data, replay_mode=True)  # 重放模式：不写入新WAL
                    logger.debug(f"重放WAL：PUT -> {key}")
                elif op_type == "DELETE":
                    handler.delete(key, replay_mode=True)
                    logger.debug(f"重放WAL：DELETE -> {key}")
                processed += 1
            except Exception as e:
                logger.error(f"重放WAL操作失败：{op_type} -> {key}，错误：{e}")
        
        # 4. 标记重放完成 + 保存位点
        self.replayed = True
        self._save_checkpoint_ts(max_ts)
        logger.info(f"WAL全量重放完成，共处理{processed}个唯一操作（原{len(log_files)}个日志文件）")

    # ========== 核心方法：增量重放WAL ==========
    def replay_incremental(self, handler, checkpoint_ts):
        """重放快照时间戳后的增量WAL日志（节点恢复快照后调用）"""
        logger.info(f"开始重放{self.node_id}的增量WAL日志（快照位点：{checkpoint_ts}）")
        # 1. 筛选增量日志文件（文件名时间戳 > 快照位点）
        log_files = [
            os.path.join(self.wal_data_dir, f) 
            for f in os.listdir(self.wal_data_dir) 
            if f.startswith("wal_") and f.endswith(".log") 
            and int(f.split("_")[1].split(".")[0]) > checkpoint_ts
        ]
        log_files = sorted(log_files)
        
        # 2. 内存去重：保留每个key的最后一次增量操作
        unique_ops: Dict[str, dict] = {}
        max_ts = checkpoint_ts
        
        for log_file in log_files:
            try:
                with open(log_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            log_entry = json.loads(line)
                        except json.JSONDecodeError:
                            logger.warning(f"跳过损坏的增量WAL日志行：{log_file} -> {line[:50]}...")
                            continue
                        # 仅处理快照后的操作
                        if log_entry["timestamp"] <= checkpoint_ts:
                            continue
                        key = log_entry["key"]
                        unique_ops[key] = log_entry
                        if log_entry["timestamp"] > max_ts:
                            max_ts = log_entry["timestamp"]
            except Exception as e:
                logger.error(f"读取增量WAL日志文件失败：{log_file}，错误：{e}")
                continue
        
        # 3. 执行增量重放
        processed = 0
        for log_entry in unique_ops.values():
            op_type = log_entry["op_type"]
            key = log_entry["key"]
            try:
                if op_type == "PUT":
                    data = VectorData(
                        key=key,
                        vector=log_entry["vector"],
                        metadata=log_entry.get("metadata"),
                        timestamp=log_entry["timestamp"]
                    )
                    handler.put(data, replay_mode=True)
                elif op_type == "DELETE":
                    handler.delete(key, replay_mode=True)
                processed += 1
            except Exception as e:
                logger.error(f"重放增量WAL操作失败：{op_type} -> {key}，错误：{e}")
        
        # 4. 更新位点
        self._save_checkpoint_ts(max_ts)
        logger.info(f"增量WAL日志重放完成，共处理{processed}个唯一操作（原{len(log_files)}个文件）")

    # ========== 辅助方法：备份WAL（可选） ==========
    def backup_wal(self, backup_dir: str):
        """备份WAL日志到指定目录"""
        os.makedirs(backup_dir, exist_ok=True)
        for f in os.listdir(self.wal_data_dir):
            if f.startswith("wal_") and f.endswith(".log"):
                shutil.copy2(os.path.join(self.wal_data_dir, f), backup_dir)
        logger.info(f"WAL日志备份完成，目标目录：{backup_dir}")