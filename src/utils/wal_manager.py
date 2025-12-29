import os
import json
import time
from loguru import logger
from src.vector_db.ttypes import VectorData

class WALManager:
    def __init__(self, node_id):
        # WAL日志存储目录
        self.wal_dir = f"./Static/wal/{node_id}"
        os.makedirs(self.wal_dir, exist_ok=True)
        self.node_id = node_id
        self.replayed = False
    def write_log(self, op_type: str, key: str, vector=None, metadata=None, timestamp=None):
        """写入WAL日志（极简实现）"""
        # 日志文件名：时间戳.log
        log_file = os.path.join(self.wal_dir, f"{int(time.time() * 1000)}.log")
        
        # 构造日志内容
        log_data = {
            "op_type": op_type,
            "key": key,
            "vector": vector,
            "metadata": metadata,
            "timestamp": timestamp or int(time.time() * 1000)
        }
        
        # 写入文件
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(log_data, f)
    def replay_incremental(self, handler, checkpoint_ts):
        """重放快照时间戳后的增量WAL日志"""
        log_files = [f for f in os.listdir(self.wal_dir) 
                     if f.endswith(".log") and int(f.split(".")[0]) > checkpoint_ts]
        log_files = sorted(log_files)
        
        for log_file in log_files:
            # 读取日志并执行对应操作（PUT/DELETE）
            with open(os.path.join(self.wal_dir, log_file), "r") as f:
                log_data = json.load(f)
                op_type = log_data["op_type"]
                key = log_data["key"]
                if op_type == "PUT":
                    vec = log_data["vector"]
                    metadata = log_data.get("metadata")
                    handler.put(VectorData(key=key, vector=vec, metadata=metadata), replay_mode=True)
                elif op_type == "DELETE":
                    handler.delete(key, replay_mode=True)
        logger.info(f"[WAL] 重放增量日志完成，共处理{len(log_files)}个文件")
    # src/utils/wal_manager.py
    def replay(self, handler):
        
        if self.replayed:
            return 
        logger.info(f"开始重放{self.node_id}的WAL日志...")
        log_files = sorted([f for f in os.listdir(self.wal_dir) if f.endswith(".log")])
        unique_logs = {}
        for log_file in log_files:
            file_path = os.path.join(self.wal_dir, log_file)
            with open(file_path, "r", encoding="utf-8") as f:
                log_data = json.load(f)
                unique_logs[log_data["key"]] = log_data
        
        # 执行重放：调用handler方法时，传递replay_mode=True（不写入新日志）
        processed = 0
        for log_data in unique_logs.values():
            op_type = log_data["op_type"]
            key = log_data["key"]
            
            if op_type == "PUT":
                data = VectorData(
                    key=key,
                    vector=log_data["vector"],
                    metadata=log_data["metadata"],
                    timestamp=log_data["timestamp"]
                )
                handler.put(data, replay_mode=True)  # 新增：重放模式
                logger.info(f"重放日志：PUT -> {key}")
            elif op_type == "DELETE":
                handler.delete(key, replay_mode=True)  # 新增：重放模式
                logger.info(f"重放日志：DELETE -> {key}")
            
            processed += 1
        
        self.replayed = True
        logger.info(f"WAL日志重放完成，共处理{processed}个唯一操作（原{len(log_files)}个文件）")