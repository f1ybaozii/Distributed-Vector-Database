"""WAL日志核心逻辑"""
import json
import os
import time
import threading

class WAL:
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        os.makedirs(wal_path, exist_ok=True)
        self.wal_file = open(os.path.join(wal_path, "wal.log"), "a+", encoding="utf-8")
        self.lock = threading.Lock()

    def write(self, op_type: str, record: dict):
        """写入日志（线程安全）"""
        with self.lock:
            log_entry = {"ts": time.time(), "op_type": op_type, "record": record}
            self.wal_file.write(json.dumps(log_entry) + "\n")
            self.wal_file.flush()

    def recover(self) -> list:
        """日志恢复"""
        self.wal_file.seek(0)
        records = []
        for line in self.wal_file:
            if line.strip():
                records.append(json.loads(line))
        return records