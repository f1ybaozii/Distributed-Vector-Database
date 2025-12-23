from flask import Flask, request, jsonify
import requests
from kazoo.client import KazooClient
from encoder import VectorEncoder
import vector_db.VectorDBService as VectorDBService
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

app = Flask(__name__)
encoder = VectorEncoder()
zk = KazooClient(hosts="127.0.0.1:2181")
zk.start()

# 简单哈希分片：key -> 数据节点
def get_node_by_key(key):
    nodes = zk.get_children("/vector_db/nodes")
    if not nodes:
        raise Exception("no data nodes available")
    node_idx = hash(key) % len(nodes)
    node = nodes[node_idx]
    ip_port = node.split("@")[1]
    return ip_port.split(":")[0], int(ip_port.split(":")[1])

# HTTP接口：上传文件并添加向量
@app.route("/add_file", methods=["POST"])
def add_file():
    try:
        # 获取文件
        file = request.files.get("file")
        if not file:
            return jsonify({"code": -1, "msg": "no file uploaded"})
        # 保存临时文件
        file_path = f"./tmp/{file.filename}"
        file.save(file_path)
        # 编码为向量
        vector, file_type = encoder.encode_file(file_path)
        # 构建请求
        key = request.form.get("key", file.filename)
        attrs = json.loads(request.form.get("attrs", "{}"))
        # 路由到数据节点
        ip, port = get_node_by_key(key)
        transport = TSocket.TSocket(ip, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = VectorDBService.Client(protocol)
        transport.open()
        # 构造WriteRequest
        record = vector_db.VectorRecord(
            key=key,
            vector=vector,
            attrs=attrs,
            file_type=file_type,
            file_path=file_path
        )
        request_obj = vector_db.WriteRequest(record=record, replica_num=3)
        resp = client.add_record(request_obj)
        transport.close()
        return jsonify({"code": resp.code, "msg": resp.msg})
    except Exception as e:
        return jsonify({"code": -1, "msg": str(e)})

# HTTP接口：查询（向量+条件）
@app.route("/query", methods=["POST"])
def query():
    try:
        data = request.json
        # 可选：上传文件作为查询向量
        vector = None
        if "file_path" in data:
            vector, _ = encoder.encode_file(data["file_path"])
        else:
            vector = data.get("vector")
        filter_cond = data.get("filter", {})
        top_k = data.get("top_k", 10)
        # 路由到数据节点（简化：随机选一个节点，生产环境需广播/分片查询）
        ip, port = get_node_by_key("query")
        transport = TSocket.TSocket(ip, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = VectorDBService.Client(protocol)
        transport.open()
        # 构造QueryRequest
        request_obj = vector_db.QueryRequest(
            vector=vector,
            filter=filter_cond,
            top_k=top_k
        )
        resp = client.query(request_obj)
        transport.close()
        # 转换为JSON返回
        records = []
        for r in resp.records:
            records.append({
                "key": r.key,
                "vector": r.vector,
                "attrs": r.attrs,
                "file_type": r.file_type,
                "file_path": r.file_path
            })
        return jsonify({"code": resp.code, "msg": resp.msg, "records": records})
    except Exception as e:
        return jsonify({"code": -1, "msg": str(e)})

# HTTP接口：删除记录
@app.route("/delete", methods=["POST"])
def delete():
    try:
        data = request.json
        key = data.get("key")
        if not key:
            return jsonify({"code": -1, "msg": "key is required"})
        ip, port = get_node_by_key(key)
        transport = TSocket.TSocket(ip, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = VectorDBService.Client(protocol)
        transport.open()
        request_obj = vector_db.DeleteRequest(key=key)
        resp = client.delete_record(request_obj)
        transport.close()
        return jsonify({"code": resp.code, "msg": resp.msg})
    except Exception as e:
        return jsonify({"code": -1, "msg": str(e)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)