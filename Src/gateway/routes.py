"""网关HTTP路由逻辑"""
import json
import os
from flask import request, jsonify
from utils import get_node_by_key, get_thrift_client
from config import TMP_DIR
import vector_db  # 确保vector_db正确导入
from vector_db.ttypes import (
    VectorRecord,
    BatchWriteRequest,
    QueryRequest,
    DeleteRequest
)  # 导入所有需要的结构体
# 全局变量（由core.py初始化）


def register_routes(app):
    
    """注册HTTP路由"""
    @app.route("/add_file", methods=["POST"])
    def add_file():
        from .core import encoder, zk
        # 前置校验1：核心依赖是否初始化
        if encoder is None:
            return jsonify({"code": -100, "msg": "Encoder is not initialized: encoder=None"}), 500
        if zk is None or not zk.connected:
            return jsonify({"code": -101, "msg": "Zookeeper is not connected"}), 500
        
        # 前置校验2：文件是否上传
        file = request.files.get("file")
        if not file:
            return jsonify({"code": -1, "msg": "Unsuccessfully add files"}), 400
        
        # 解析参数（基础校验）
        root_key = request.form.get("key", file.filename)
        try:
            attrs = json.loads(request.form.get("attrs", "{}"))
        except json.JSONDecodeError:
            return jsonify({"code": -2, "msg": "attrs are illegal."}), 400
        
        try:
            replica_num = int(request.form.get("replica_num", 3))
        except ValueError:
            return jsonify({"code": -3, "msg": "replica_num should be nums."}), 400

        # 保存临时文件（确保目录可写）
        try:
            os.makedirs(TMP_DIR, exist_ok=True)
            file_path = f"{TMP_DIR}/{file.filename}"
            file.save(file_path)
        except Exception as e:
            return jsonify({"code": -4, "msg": f"Unsuccessfully save temp files{str(e)}"}), 500

        # 编码文件（核心步骤，异常时返回JSON）
        try:
            encode_result, file_type = encoder.encode_file(file_path)
        except Exception as e:
            return jsonify({"code": -5, "msg": f"Unsuccessfully encode files.{str(e)}"}), 500

        # 构造批量请求
        batch_records = []
        for chunk_idx, (chunk_text, vec) in enumerate(encode_result):
            chunk_key = f"{root_key}:{chunk_idx}"
            record = VectorRecord(
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

        # 路由到数据节点（核心依赖校验）
        try:
            ip, port = get_node_by_key(zk, root_key)
        except Exception as e:
            return jsonify({"code": -6, "msg": f"DataNode Routed failed:{str(e)}"}), 500
        
        # 连接数据节点并写入（关键异常捕获）
        try:
            client, transport = get_thrift_client(ip, port)
            batch_request = BatchWriteRequest(records=batch_records, replica_num=replica_num)
            resp = client.batch_add_records(batch_request)
            transport.close()
        except Exception as e:
            return jsonify({"code": -7, "msg": f"DataNode written failed:{str(e)}"}), 500

        # 正常返回（标准JSON）
        return jsonify({
            "code": resp.code,
            "msg": resp.msg,
            "root_key": root_key,
            "chunk_count": len(batch_records),
            "file_type": file_type
        })

    # query/delete路由按相同逻辑补充「前置校验+核心步骤异常捕获」
    # （仅保留关键逻辑，示例query路由极简修复）
    @app.route("/query", methods=["POST"])
    def query():
        from .core import encoder, zk
        if encoder is None or zk is None or not zk.connected:
            return jsonify({"code": -100, "msg": "Initialization Unsuccessfully"}), 500
        
        data = request.json or {}
        vector = None
        
        # ====== 新增：处理前端直接传的text参数 ======
        if "text" in data:
            try:
                # 直接编码文本，无需临时文件
                chunk_vectors = encoder.encode_text_chunks(data["text"])
                vector = chunk_vectors[0][1]  # 取第一个chunk的向量
            except Exception as e:
                return jsonify({"code": -5, "msg": f"Text Encoded Unsuccessfully:{str(e)}"}), 500
        # ====== 原有逻辑保留 ======
        elif "file_path" in data:
            try:
                encode_result, _ = encoder.encode_file(data["file_path"])
                vector = encode_result[0][1]
            except Exception as e:
                return jsonify({"code": -5, "msg": f"Files Encoded Unsuccessfully:{str(e)}"}), 500
        elif "vector" in data:
            vector = data["vector"]
        else:
            return jsonify({"code": -4, "msg": "Lack text/file_path/vector Parameters"}), 400

        filter_cond = data.get("filter", {})
        top_k = data.get("top_k", 10)
        aggregate = data.get("aggregate", True)

        try:
            ip, port = get_node_by_key(zk, "query")
            client, transport = get_thrift_client(ip, port)
            query_request = QueryRequest(
                vector=vector,
                filter=filter_cond,
                top_k=top_k,
                aggregate=aggregate
            )
            resp = client.query(query_request)
            transport.close()
        except Exception as e:
            return jsonify({"code": -7, "msg": f"DataNode Query Unsuccessfully：{str(e)}"}), 500

        # 构造返回结果（原有逻辑不变）
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
        from .core import encoder, zk
        if zk is None or not zk.connected:
            return jsonify({"code": -101, "msg": "ZK连接未初始化/已断开"}), 500
        
        data = request.json or {}
        root_key = data.get("key")
        if not root_key:
            return jsonify({"code": -1, "msg": "key不能为空"}), 400
        
        try:
            ip, port = get_node_by_key(zk, root_key)
            client, transport = get_thrift_client(ip, port)
            delete_request = DeleteRequest(key=root_key)
            resp = client.delete_record(delete_request)
            transport.close()
        except Exception as e:
            return jsonify({"code": -7, "msg": f"数据节点删除失败：{str(e)}"}), 500
        
        return jsonify({"code": resp.code, "msg": resp.msg})