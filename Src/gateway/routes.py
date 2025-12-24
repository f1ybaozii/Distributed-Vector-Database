"""网关HTTP路由逻辑"""
import json
import os
from flask import request, jsonify
from utils import get_node_by_key, get_thrift_client
from config import TMP_DIR

# 全局变量（由core.py初始化）
encoder = None
zk = None

def register_routes(app):
    """注册HTTP路由"""
    @app.route("/add_file", methods=["POST"])
    def add_file():
        file = request.files.get("file")
        if not file:
            return jsonify({"code": -1, "msg": "未上传文件"})
        
        root_key = request.form.get("key", file.filename)
        attrs = json.loads(request.form.get("attrs", "{}"))
        replica_num = int(request.form.get("replica_num", 3))

        # 保存临时文件
        os.makedirs(TMP_DIR, exist_ok=True)
        file_path = f"{TMP_DIR}/{file.filename}"
        file.save(file_path)

        # 编码文件
        encode_result, file_type = encoder.encode_file(file_path)

        # 构造批量请求
        batch_records = []
        for chunk_idx, (chunk_text, vec) in enumerate(encode_result):
            chunk_key = f"{root_key}:{chunk_idx}"
            record = vector_db.VectorRecord(
                key=chunk_key,
                vector=vec,
                attrs=attrs,
                file_type=file_type,
                file_path=file_path,
                root_key=root_key,
                chunk_id=str(chunk_idx),
                chunk_text=chunk_text
            )
            batch_records.append(record)

        # 路由到数据节点
        ip, port = get_node_by_key(zk, root_key)
        client, transport = get_thrift_client(ip, port)
        batch_request = vector_db.BatchWriteRequest(records=batch_records, replica_num=replica_num)
        resp = client.batch_add_records(batch_request)
        transport.close()

        return jsonify({
            "code": resp.code,
            "msg": resp.msg,
            "root_key": root_key,
            "chunk_count": len(batch_records),
            "file_type": file_type
        })

    @app.route("/query", methods=["POST"])
    def query():
        data = request.json
        vector = None
        if "file_path" in data:
            encode_result, _ = encoder.encode_file(data["file_path"])
            vector = encode_result[0][1]
        elif "vector" in data:
            vector = data["vector"]

        filter_cond = data.get("filter", {})
        top_k = data.get("top_k", 10)
        aggregate = data.get("aggregate", True)

        ip, port = get_node_by_key(zk, "query")
        client, transport = get_thrift_client(ip, port)
        query_request = vector_db.QueryRequest(
            vector=vector,
            filter=filter_cond,
            top_k=top_k,
            aggregate=aggregate
        )
        resp = client.query(query_request)
        transport.close()

        result = {
            "code": resp.code,
            "msg": resp.msg,
            "records": [
                {
                    "key": r.key,
                    "root_key": r.root_key,
                    "chunk_id": r.chunk_id,
                    "chunk_text": r.chunk_text,
                    "attrs": r.attrs,
                    "file_type": r.file_type
                } for r in resp.records
            ],
            "aggregated_records": {
                root_key: [
                    {
                        "chunk_id": r.chunk_id,
                        "chunk_text": r.chunk_text,
                        "similarity": float(sum([a*b for a,b in zip(vector, r.vector)]) if vector else 0)
                    } for r in records
                ] for root_key, records in resp.aggregated_records.items()
            }
        }
        return jsonify(result)

    @app.route("/delete", methods=["POST"])
    def delete():
        data = request.json
        root_key = data.get("key")
        if not root_key:
            return jsonify({"code": -1, "msg": "key不能为空"})
        
        ip, port = get_node_by_key(zk, root_key)
        client, transport = get_thrift_client(ip, port)
        delete_request = vector_db.DeleteRequest(key=root_key)
        resp = client.delete_record(delete_request)
        transport.close()
        
        return jsonify({"code": resp.code, "msg": resp.msg})