"""数据节点核心逻辑"""
import faiss
import threading
import traceback
from kazoo.client import KazooClient
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from .wal import WAL  # 基于优化后的WAL类
from .handler import DataNodeHandler
from config import DATA_NODE_CONFIG, WAL_BASE_DIR
import vector_db
import numpy as np

class DataNode:
    def __init__(self, node_id: str,
                 zk_address: str,
                 port: int,
                 replica_num: int = DATA_NODE_CONFIG["replica_num"],
                 vector_dim: int = DATA_NODE_CONFIG["vector_dim"]):
        # 核心参数
        self.node_id = node_id
        self.zk_address = zk_address
        self.port = port
        self.replica_num = replica_num
        self.vector_dim = vector_dim

        # 向量索引初始化（核心异常：启动阶段索引创建失败需终止）
        try:
            self.index = faiss.IndexHNSWFlat(vector_dim, 32)
            self.index.hnsw.efConstruction = DATA_NODE_CONFIG["hnsw_ef_construction"]
            self.index.hnsw.efSearch = DATA_NODE_CONFIG["hnsw_ef_search"]
        except Exception as e:
            raise RuntimeError(f"Index Initiallzation failed: {e}") from e

        # 数据存储
        self.key_to_record = {}
        self.lock = threading.RLock()  # 可重入锁保证核心操作原子性

        # ZK连接（核心异常：ZK连接失败需终止）
        try:
            self.zk = KazooClient(hosts=zk_address)
            self.zk.start()
            self.zk.ensure_path("/vector_db/nodes")
            self.zk.create(f"/vector_db/nodes/{node_id}", b"online", ephemeral=True)
        except Exception as e:
            raise ConnectionError(f"ZK连接失败: {e}") from e

        # # WAL初始化与恢复（核心：数据恢复失败需终止）
        self.wal = WAL(f"{WAL_BASE_DIR}/{node_id}")
        with self.lock:
            self._recover_from_wal()
   
        # 服务器实例
        self.server = None

    def _recover_from_wal(self):
        """WAL日志恢复（仅保留核心异常捕获）"""
        records = self.wal.recover()
        print(f"从WAL恢复数据，共{len(records)}条日志")
        
        for idx, record in enumerate(records):
            op_type = record.get("op_type")
            log_record = record.get("record", {})
            
            if op_type in ["add", "batch_add"]:
                self._apply_add_operation(op_type, log_record)
            elif op_type == "delete":
                self._apply_delete_operation(log_record)
        
        print(f"WAL恢复完成，当前数据量: {len(self.key_to_record)}")

    def _apply_add_operation(self, op_type, record):
        """应用添加操作（无多余try/except，依赖外层锁和顶层捕获）"""
        try:
            if op_type == "batch_add" and isinstance(record, list):
                for r in record:
                    self.key_to_record[r["key"]] = r
                    vec = np.array(r["vector"], dtype=np.float32).reshape(1, -1)
                    self.index.add(vec)
            elif op_type == "add" and isinstance(record, dict):
                self.key_to_record[record["key"]] = record
                vec = np.array(record["vector"], dtype=np.float32).reshape(1, -1)
                self.index.add(vec)
        except Exception as e:
            print(f"ADD FAILED: {e}")
           

    def _apply_delete_operation(self, record):
        """应用删除操作（无多余try/except）"""
        root_key = record.get("key")
        if not root_key:
            return
        
        to_delete = [k for k in self.key_to_record.keys() 
                    if self.key_to_record[k].get("root_key") == root_key]
        for k in to_delete:
            del self.key_to_record[k]
        
        self._rebuild_index()

    def _rebuild_index(self):
        """重建向量索引（核心操作，无多余try/except）"""
        with self.lock:
            # 重建索引
            self.index = faiss.IndexHNSWFlat(self.vector_dim, 32)
            self.index.hnsw.efConstruction = DATA_NODE_CONFIG["hnsw_ef_construction"]
            self.index.hnsw.efSearch = DATA_NODE_CONFIG["hnsw_ef_search"]
            
            # 批量添加向量
            vectors = [np.array(r["vector"], dtype=np.float32) 
                      for r in self.key_to_record.values()]
            if vectors:
                vec_matrix = np.vstack(vectors)
                self.index.add(vec_matrix)

    def write_wal(self, op_type: str, record: dict or list) -> bool:
        """写入WAL日志（核心：仅捕获WAL写入本身的异常）"""
        with self.lock:
            try:
                return self.wal.write(op_type, record)
            except Exception as e:
                print(f"WAL写入失败: {e}")
                return False

    def _sync_to_replicas(self, op_type: str, request):
        """副本同步（仅保留连接关闭的核心try/finally）"""
        if self.replica_num <= 1:
            return
        
        replica_nodes = self.zk.get_children("/vector_db/nodes")
        replica_nodes = [n for n in replica_nodes if n != self.node_id][:self.replica_num-1]
        
        for node in replica_nodes:
            transport = None
            try:
                ip_port = node.split("@")[1]
                ip, port = ip_port.split(":")
                
                transport = TSocket.TSocket(ip, int(port))
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = vector_db.VectorDBService.Client(protocol)
                
                transport.open()
                if op_type == "add":
                    client.add_record(request)
                elif op_type == "batch_add":
                    client.batch_add_records(request)
                elif op_type == "delete":
                    client.delete_record(request)
            finally:
                # 核心：无论同步成功/失败，必须关闭连接（防止资源泄露）
                if transport:
                    transport.close()

    def start(self):
        """启动数据节点（核心异常捕获）"""
        try:
            handler = DataNodeHandler(self)
            processor = vector_db.VectorDBService.Processor(handler)
            transport = TSocket.TServerSocket(port=self.port)
            tfactory = TTransport.TBufferedTransportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()
            
            self.server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
            print(f"数据节点 {self.node_id} 启动成功，端口：{self.port}")
            self.server.serve()
        except Exception as e:
            self.close()
            raise RuntimeError(f"节点启动失败: {e}") from e

    def close(self):
        """优雅关闭节点（核心资源释放，无多余try/except）"""
        print(f"关闭数据节点 {self.node_id}")
        with self.lock:
            # 1. 关闭WAL（核心：释放文件句柄）
            if hasattr(self, "wal") and self.wal:
                self.wal.close()
            
            # 2. 关闭ZK连接（核心：释放ZK连接）
            if hasattr(self, "zk") and self.zk and self.zk.connected:
                self.zk.stop()
                self.zk.close()
            
            # 3. 清空数据
            self.key_to_record.clear()
            self.index = None

    def __del__(self):
        """析构函数：兜底释放核心资源"""
        self.close()

# 快捷启动接口（仅保留顶层核心异常捕获）
def start_data_node(node_id: str,
                    zk_address: str,
                    port: int,
                    replica_num: int = None,
                    vector_dim: int = None):
    data_node = None
    try:
        data_node = DataNode(
            node_id=node_id,
            zk_address=zk_address,
            port=port,
            replica_num=replica_num or DATA_NODE_CONFIG["replica_num"],
            vector_dim=vector_dim or DATA_NODE_CONFIG["vector_dim"]
        )
        data_node.start()
    except KeyboardInterrupt:
        print(f"停止信号触发，关闭节点 {node_id}")
        if data_node:
            data_node.close()
    except Exception as e:
        print(f"节点启动/运行失败: {e}\n{traceback.format_exc()}")
        if data_node:
            data_node.close()
        raise