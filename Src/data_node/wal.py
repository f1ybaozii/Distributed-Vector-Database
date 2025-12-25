import json
import os
import time
import threading
from typing import List, Dict, Any

class WAL:
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        self.wal_file_path = os.path.join(wal_path, "wal.log")
        self.lock = threading.Lock()  # 全局锁，保证读写互斥
        self.wal_file = None
        
        # 初始化目录并打开文件
        os.makedirs(wal_path, exist_ok=True)
        self._open_file()

    def _open_file(self):
        """内部方法：安全打开文件（处理重复打开）"""
        if self.wal_file and not self.wal_file.closed:
            return
        # 使用buffering=1（行缓冲），减少flush开销，同时保证行写入完整性
        self.wal_file = open(self.wal_file_path, "a+", encoding="utf-8", buffering=1)

    def write(self, op_type: str, record: dict) -> bool:
        """
        写入日志（线程安全）
        :param op_type: 操作类型（如add/delete/update）
        :param record: 日志内容
        :return: 写入是否成功
        """
        if not op_type:
            raise ValueError("op_type不能为空")
    
        if not isinstance(record, dict):
            raise ValueError("record必须是字典或字典列表")     
        with self.lock:
            try:
                # 时间戳改用毫秒级整数，可读性更好且JSON序列化更稳定
                log_entry = {
                    "ts": int(time.time() * 1000),
                    "op_type": op_type,
                    "record": record
                }
                # 写入单行JSON，保证日志行完整性
                self.wal_file.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
                self.wal_file.flush()  # 强制刷盘，保证数据落盘
                return True
            except Exception as e:
                # 记录异常并尝试重新打开文件
                print(f"WAL写入失败: {e}")
                self._open_file()  # 尝试重新打开文件
                return False

    def recover(self) -> List[Dict[str, Any]]:
        """
        日志恢复（线程安全）
        :return: 解析后的日志列表（过滤无效行）
        """
        records = []
        with self.lock:
            try:
                # 确保文件已打开
                self._open_file()
                # 移动到文件开头
                self.wal_file.seek(0)
                # 按行读取，跳过空行和解析失败的行
                for line_num, line in enumerate(self.wal_file):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        print(f"WAL恢复失败：第{line_num+1}行JSON解析错误 - {e}")
                        continue
            except Exception as e:
                print(f"WAL恢复异常: {e}")
        return records

    def close(self):
        """主动关闭文件句柄（关键：防止资源泄露）"""
        with self.lock:
            if self.wal_file and not self.wal_file.closed:
                self.wal_file.close()
                self.wal_file = None

    def __del__(self):
        """析构函数：保证实例销毁时关闭文件"""
        self.close()

    def truncate(self):
        """清空WAL日志（谨慎使用，仅在确认数据已持久化后调用）"""
        with self.lock:
            self.wal_file.seek(0)
            self.wal_file.truncate()  # 清空文件
            self.wal_file.flush()