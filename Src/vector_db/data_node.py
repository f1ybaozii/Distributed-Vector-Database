import faiss
import json
import os
import time
import threading
from kazoo.client import KazooClient
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import vector_db.VectorDBService as VectorDBService

# 预写日志（WAL）
class WAL:
    def __init__(self, wal_path):
        self.wal_path = wal_path
        os.makedirs(wal_path, exist_ok=True)
        self.wal_file = open(os.path.join(wal_path, "wal.log"), "a+", encoding="utf-8")
        self.lock = threading.Lock()

    def write(self, op_type, record):
        """写入日志：op_type=add/update/delete"""
        with self.lock:
            log_entry = {
                "ts": time.time(),
                "op_type": op_type,
                "record": record
            }
            self.wal_file.write(json.dumps(log_entry) + "\n")
            self.wal_file.flush()

    def recover(self):
        """故障恢复：从WAL恢复数据"""
        self.wal_file.seek(0)
        records = []
        for line in self.wal_file:
            if line.strip():
                records.append(json.loads(line))
        return records

# 数据节点实现
class DataNodeHandler(VectorDBService.Iface):
    def __init__(self, node_id, zk_address, replica_num=3):
        # 初始化FAISS索引（HNSW）
        self.dim = 512  # CLIP统一输出512维向量
        self.index = faiss.IndexHNSWFlat(self.dim, 32)
        self.index.hnsw.efConstruction = 40
        self.index.hnsw.efSearch = 16

        # 键值映射：key -> (vector, attrs, file_type, file_path)
        self.key_to_data = {}
        self.lock = threading.Lock()

        # 初始化ZK，注册节点
        self.zk = KazooClient(hosts=zk_address)
        self.zk.start()
        self.node_id = node_id
        self.replica_num = replica_num
        self.zk.ensure_path("/vector_db/nodes")
        self.zk.create(f"/vector_db/nodes/{node_id}", b"online", ephemeral=True)

        # 初始化WAL
        self.wal = WAL(f"./wal/{node_id}")
        # 从WAL恢复数据
        self._recover_from_wal()

    def _recover_from_wal(self):
        """从WAL恢复数据"""
        records = self.wal.recover()
        for record in records:
            with self.lock:
                if record["op_type"] in ["add", "update"]:
                    vec_record = record["record"]
                    key = vec_record["key"]
                    vector = vec_record["vector"]
                    attrs = vec_record.get("attrs", {})
                    file_type = vec_record.get("file_type", "")
                    file_path = vec_record.get("file_path", "")
                    self.key_to_data[key] = (vector, attrs, file_type, file_path)
                    self.index.add(faiss.array_to_vec(vector).reshape(1, -1))
                elif record["op_type"] == "delete":
                    key = record["record"]["key"]
                    if key in self.key_to_data:
                        del self.key_to_data[key]
                        # FAISS删除需重建索引（简化实现，生产环境用IVF索引）
                        self._rebuild_index()

    def _rebuild_index(self):
        """重建FAISS索引（处理删除）"""
        vectors = [v[0] for v in self.key_to_data.values()]
        self.index = faiss.IndexHNSWFlat(self.dim, 32)
        self.index.add(faiss.array_to_vec(vectors))

    def add_record(self, request):
        """添加向量记录（含多副本）"""
        try:
            with self.lock:
                record = request.record
                key = record.key
                vector = record.vector
                # 更新维度（适配图片/文本）
                if len(vector) != self.dim:
                    self.dim = len(vector)
                    self.index = faiss.IndexHNSWFlat(self.dim, 32)
                # 写入WAL
                self.wal.write("add", {
                    "key": key,
                    "vector": vector,
                    "attrs": record.attrs,
                    "file_type": record.file_type,
                    "file_path": record.file_path
                })
                # 写入本地存储
                self.key_to_data[key] = (vector, record.attrs, record.file_type, record.file_path)
                self.index.add(faiss.array_to_vec(vector).reshape(1, -1))
                # 同步到副本节点（简化：ZK获取副本节点列表，RPC同步）
                self._sync_to_replicas("add", request)
            return vector_db.BaseResponse(code=0, msg="success")
        except Exception as e:
            return vector_db.BaseResponse(code=-1, msg=f"add failed: {str(e)}")

    def update_record(self, request):
        """更新向量记录"""
        try:
            with self.lock:
                record = request.record
                key = record.key
                if key not in self.key_to_data:
                    return vector_db.BaseResponse(code=-1, msg="key not found")
                # 写入WAL
                self.wal.write("update", {
                    "key": key,
                    "vector": record.vector,
                    "attrs": record.attrs,
                    "file_type": record.file_type,
                    "file_path": record.file_path
                })
                # 更新本地存储
                self.key_to_data[key] = (record.vector, record.attrs, record.file_type, record.file_path)
                self._rebuild_index()
                # 同步到副本
                self._sync_to_replicas("update", request)
            return vector_db.BaseResponse(code=0, msg="success")
        except Exception as e:
            return vector_db.BaseResponse(code=-1, msg=f"update failed: {str(e)}")

    def delete_record(self, request):
        """删除向量记录"""
        try:
            with self.lock:
                key = request.key
                if key not in self.key_to_data:
                    return vector_db.BaseResponse(code=-1, msg="key not found")
                # 写入WAL
                self.wal.write("delete", {"key": key})
                # 删除本地存储
                del self.key_to_data[key]
                self._rebuild_index()
                # 同步到副本
                self._sync_to_replicas("delete", request)
            return vector_db.BaseResponse(code=0, msg="success")
        except Exception as e:
            return vector_db.BaseResponse(code=-1, msg=f"delete failed: {str(e)}")

    def query(self, request):
        """混合查询：向量检索+条件过滤"""
        try:
            with self.lock:
                # 1. 向量检索（如果指定）
                candidate_keys = set(self.key_to_data.keys())
                if request.vector:
                    query_vec = faiss.array_to_vec(request.vector).reshape(1, -1)
                    distances, indices = self.index.search(query_vec, request.top_k)
                    # 映射索引到key
                    key_list = list(self.key_to_data.keys())
                    candidate_keys = set([key_list[i] for i in indices[0] if i < len(key_list)])

                # 2. 条件过滤（如time>2025-01-01）
                filtered_records = []
                for key in candidate_keys:
                    vector, attrs, file_type, file_path = self.key_to_data[key]
                    # 简单条件过滤（示例：支持等于/大于/小于）
                    match = True
                    for k, v in request.filter.items():
                        if k not in attrs:
                            match = False
                            break
                        # 处理数值/时间比较
                        if v.startswith(">"):
                            match = attrs[k] > v[1:]
                        elif v.startswith("<"):
                            match = attrs[k] < v[1:]
                        else:
                            match = attrs[k] == v
                        if not match:
                            break
                    if match:
                        filtered_records.append(vector_db.VectorRecord(
                            key=key,
                            vector=vector,
                            attrs=attrs,
                            file_type=file_type,
                            file_path=file_path
                        ))
                # 限制返回数量
                filtered_records = filtered_records[:request.top_k]
            return vector_db.QueryResponse(records=filtered_records, code=0, msg="success")
        except Exception as e:
            return vector_db.QueryResponse(records=[], code=-1, msg=f"query failed: {str(e)}")

    def _sync_to_replicas(self, op_type, request):
        """同步到副本节点（简化实现：ZK获取副本列表）"""
        replica_nodes = self.zk.get_children("/vector_db/nodes")
        replica_nodes = [n for n in replica_nodes if n != self.node_id][:self.replica_num-1]
        for node in replica_nodes:
            # 假设节点地址格式为 node_id@ip:port
            ip_port = node.split("@")[1]
            ip, port = ip_port.split(":")
            try:
                transport = TSocket.TSocket(ip, int(port))
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = VectorDBService.Client(protocol)
                transport.open()
                if op_type == "add":
                    client.add_record(request)
                elif op_type == "update":
                    client.update_record(request)
                elif op_type == "delete":
                    client.delete_record(request)
                transport.close()
            except Exception as e:
                print(f"sync to replica {node} failed: {str(e)}")

# 启动数据节点
def start_data_node(node_id, zk_address, port):
    handler = DataNodeHandler(node_id, zk_address)
    processor = VectorDBService.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print(f"DataNode {node_id} started on port {port}")
    server.serve()

if __name__ == "__main__":
    # 示例：启动数据节点 node1@127.0.0.1:9090
    start_data_node("node1@127.0.0.1:9090", "127.0.0.1:2181", 9090)