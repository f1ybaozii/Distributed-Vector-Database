"""数据节点核心逻辑"""
import faiss
import threading
from kazoo.client import KazooClient
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from .wal import WAL
from .handler import DataNodeHandler
from config import DATA_NODE_CONFIG, WAL_BASE_DIR
import vector_db
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

        # 向量索引初始化
        self.index = faiss.IndexHNSWFlat(vector_dim, 32)
        self.index.hnsw.efConstruction = DATA_NODE_CONFIG["hnsw_ef_construction"]
        self.index.hnsw.efSearch = DATA_NODE_CONFIG["hnsw_ef_search"]

        # 数据存储
        self.key_to_record = {}
        self.lock = threading.Lock()

        # ZK连接
        self.zk = KazooClient(hosts=zk_address)
        self.zk.start()
        self.zk.ensure_path("/vector_db/nodes")
        self.zk.create(f"/vector_db/nodes/{node_id}", b"online", ephemeral=True)

        # WAL初始化
        self.wal = WAL(f"{WAL_BASE_DIR}/{node_id}")
        self._recover_from_wal()

    def _recover_from_wal(self):
        """WAL日志恢复"""
        records = self.wal.recover()
        with self.lock:
            for record in records:
                if record["op_type"] in ["add", "batch_add"]:
                    if isinstance(record["record"], list):
                        for r in record["record"]:
                            self.key_to_record[r["key"]] = r
                            self.index.add(faiss.array_to_vec(r["vector"]).reshape(1, -1))
                    else:
                        r = record["record"]
                        self.key_to_record[r["key"]] = r
                        self.index.add(faiss.array_to_vec(r["vector"]).reshape(1, -1))
                elif record["op_type"] == "delete":
                    key = record["record"]["key"]
                    to_delete = [k for k in self.key_to_record.keys() if self.key_to_record[k].get("root_key") == key]
                    for k in to_delete:
                        del self.key_to_record[k]
                    self._rebuild_index()

    def _rebuild_index(self):
        """重建向量索引"""
        vectors = [r["vector"] for r in self.key_to_record.values()]
        self.index = faiss.IndexHNSWFlat(self.vector_dim, 32)
        if vectors:
            self.index.add(faiss.array_to_vec(vectors))

    def _sync_to_replicas(self, op_type: str, request):
        """副本同步"""
        replica_nodes = self.zk.get_children("/vector_db/nodes")
        replica_nodes = [n for n in replica_nodes if n != self.node_id][:self.replica_num-1]
        for node in replica_nodes:
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
            transport.close()

    def start(self):
        """启动数据节点"""
        handler = DataNodeHandler(self)
        processor = vector_db.VectorDBService.Processor(handler)
        transport = TSocket.TServerSocket(port=self.port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
        print(f"数据节点 {self.node_id} 启动成功，端口：{self.port}")
        server.serve()

# 快捷启动接口
def start_data_node(node_id: str,
                    zk_address: str,
                    port: int,
                    replica_num: int = None,
                    vector_dim: int = None):
    data_node = DataNode(
        node_id=node_id,
        zk_address=zk_address,
        port=port,
        replica_num=replica_num or DATA_NODE_CONFIG["replica_num"],
        vector_dim=vector_dim or DATA_NODE_CONFIG["vector_dim"]
    )
    data_node.start()