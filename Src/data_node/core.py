"""数据节点核心逻辑（修复accept属性缺失+精简try except）"""
import faiss
import threading
import traceback
from kazoo.client import KazooClient
from thrift.server import TServer
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.transport.TTransport import TTransportException

from .wal import WAL
from .handler import DataNodeHandler
from config import DATA_NODE_CONFIG, WAL_BASE_DIR
import vector_db
import numpy as np

# 【核心修复】不重写serve方法，仅包装原生TThreadPoolServer做异常捕获
class RobustThreadPoolServer(TServer.TThreadPoolServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_flag = threading.Event()  # 停止信号
    
    def serve(self):
        """仅包装原生serve方法，捕获客户端连接异常"""
        self._stop_flag.clear()
        original_serve = super().serve
        
        # 重定向原生serve的异常捕获
        def wrapped_serve():
            try:
                original_serve()
            except TTransportException as e:
                # 仅忽略客户端断开的核心异常
                if "read 0 bytes" in str(e) or e.type == TTransportException.END_OF_FILE:
                    return
                print(f"Transport警告: {e}")
            except Exception as e:
                if not self._stop_flag.is_set():
                    print(f"服务端异常: {e}\n{traceback.format_exc()}")
        
        wrapped_serve()
    
    def stop(self):
        """兼容停止逻辑"""
        self._stop_flag.set()
        # 关闭监听套接字触发退出
        if hasattr(self, 'serverTransport') and self.serverTransport:
            try:
                self.serverTransport.close()
            except Exception:
                pass

class DataNode:
    def __init__(self, node_id: str, zk_address: str, port: int,
                 replica_num: int = DATA_NODE_CONFIG["replica_num"],
                 vector_dim: int = DATA_NODE_CONFIG["vector_dim"]):
        # 核心参数
        self.node_id = node_id
        self.zk_address = zk_address
        self.port = port
        self.replica_num = replica_num
        self.vector_dim = vector_dim

        # 向量索引初始化（仅捕获初始化异常）
        self.index = faiss.IndexHNSWFlat(vector_dim, 32)
        self.index.hnsw.efConstruction = DATA_NODE_CONFIG["hnsw_ef_construction"]
        self.index.hnsw.efSearch = DATA_NODE_CONFIG["hnsw_ef_search"]

        # 数据存储
        self.key_to_record = {}
        self.lock = threading.RLock()

        # ZK连接（核心异常直接抛出，不冗余捕获）
        self.zk = KazooClient(hosts=zk_address)
        self.zk.start()
        self.zk.ensure_path("/vector_db/nodes")
        self.zk.create(f"/vector_db/nodes/{node_id}", b"online", ephemeral=True)

        # WAL初始化与恢复
        self.wal = WAL(f"{WAL_BASE_DIR}/{node_id}")
        self._recover_from_wal()

        # 服务器实例
        self.server = None

    def _recover_from_wal(self):
        """WAL日志恢复（精简冗余捕获）"""
        records = self.wal.recover()
        print(f"从WAL恢复数据，共{len(records)}条日志")
        
        for record in records:
            op_type = record.get("op_type")
            log_record = record.get("record", {})
            
            if op_type in ["add", "batch_add"]:
                self._apply_add_operation(op_type, log_record)
            elif op_type == "delete":
                self._apply_delete_operation(log_record)
        
        print(f"WAL恢复完成，当前数据量: {len(self.key_to_record)}")

    def _apply_add_operation(self, op_type, record):
        """应用添加操作（仅保留核心业务异常打印）"""
        if op_type == "batch_add" and isinstance(record, list):
            for r in record:
                self.key_to_record[r["key"]] = r
                vec = np.array(r["vector"], dtype=np.float32).reshape(1, -1)
                self.index.add(vec)
        elif op_type == "add" and isinstance(record, dict):
            self.key_to_record[record["key"]] = record
            vec = np.array(record["vector"], dtype=np.float32).reshape(1, -1)
            self.index.add(vec)

    def _apply_delete_operation(self, record):
        """应用删除操作（无冗余捕获）"""
        root_key = record.get("key")
        if not root_key:
            return
        
        to_delete = [k for k in self.key_to_record.keys() 
                    if self.key_to_record[k].get("root_key") == root_key]
        for k in to_delete:
            del self.key_to_record[k]
        
        self._rebuild_index()

    def _rebuild_index(self):
        """重建向量索引（无冗余捕获）"""
        with self.lock:
            self.index = faiss.IndexHNSWFlat(self.vector_dim, 32)
            self.index.hnsw.efConstruction = DATA_NODE_CONFIG["hnsw_ef_construction"]
            self.index.hnsw.efSearch = DATA_NODE_CONFIG["hnsw_ef_search"]
            
            vectors = [np.array(r["vector"], dtype=np.float32) 
                      for r in self.key_to_record.values()]
            if vectors:
                vec_matrix = np.vstack(vectors)
                self.index.add(vec_matrix)

    def write_wal(self, op_type: str, record: dict or list) -> bool:
        """写入WAL日志（精简冗余try except）"""
        with self.lock:
            try:
                return self.wal.write(op_type, record)
            except Exception as e:
                print(f"WAL写入失败: {e}")
                return False

    def _sync_to_replicas(self, op_type: str, request):
        """副本同步（精简冗余捕获）"""
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
                transport.setTimeout(10000)
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
            except TTransportException as e:
                print(f"副本同步连接异常 {node}: {e}")
            finally:
                if transport and transport.isOpen():
                    transport.close()

    def start(self):
        """启动数据节点（修复accept缺失+精简try except）"""
        # Handler异常包装器（仅捕获关键传输异常）
        class ExceptionSafeHandler:
            def __init__(self, handler):
                self.handler = handler
            
            def __getattr__(self, name):
                func = getattr(self.handler, name)
                def wrapper(*args, **kwargs):
                    try:
                        return func(*args, **kwargs)
                    except TTransportException as e:
                        if "read 0 bytes" in str(e):
                            return None
                        raise
                return wrapper
        
        # 核心服务器初始化（无冗余捕获）
        handler = DataNodeHandler(self)
        safe_handler = ExceptionSafeHandler(handler)
        processor = vector_db.VectorDBService.Processor(safe_handler)
        
        transport = TSocket.TServerSocket(port=self.port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        
        # 创建修复后的鲁棒服务器
        self.server = RobustThreadPoolServer(
            processor, transport, tfactory, pfactory,
            worker_count=20,
            daemon=True,
            exit_on_error=False
        )
        
        # TCP保活配置（精简冗余捕获）
        if hasattr(transport, 'handle') and transport.handle is not None:
            transport.handle.setsockopt(
                TSocket.socket.SOL_SOCKET,
                TSocket.socket.SO_KEEPALIVE,
                1
            )
        
        print(f"数据节点 {self.node_id} 启动成功，端口：{self.port}")
        self.server.serve()

    def close(self):
        """优雅关闭节点（精简冗余try except）"""
        print(f"关闭数据节点 {self.node_id}")
        with self.lock:
            # 停止服务器（核心逻辑，无冗余捕获）
            if self.server:
                self.server.stop()
            
            # 关闭WAL
            if hasattr(self, "wal"):
                self.wal.close()
            
            # 关闭ZK连接
            if self.zk and self.zk.connected:
                self.zk.stop()
                self.zk.close()
            
            # 清空数据
            self.key_to_record.clear()
            self.index = None

    def __del__(self):
        self.close()

def start_data_node(node_id: str, zk_address: str, port: int,
                    replica_num: int = None, vector_dim: int = None):
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