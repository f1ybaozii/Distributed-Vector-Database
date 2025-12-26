from flask import request, jsonify
import json
import os
from utils import get_node_by_key
from config import TMP_DIR
from vector_db.ttypes import (
    VectorRecord,
    BatchWriteRequest,
    QueryRequest,
    DeleteRequest
)

def register_routes(app):

    @app.route("/add_file", methods=["POST"])
    def add_file():
        from .core import encoder, zk, client_pool

        if encoder is None or zk is None or not zk.connected:
            return jsonify({"code": -1, "msg": "Gateway not ready"}), 500

        file = request.files.get("file")
        if not file:
            return jsonify({"code": -2, "msg": "No file"}), 400

        root_key = request.form.get("key", file.filename)
        attrs = json.loads(request.form.get("attrs", "{}"))
        replica_num = int(request.form.get("replica_num", 3))

        os.makedirs(TMP_DIR, exist_ok=True)
        file_path = os.path.join(TMP_DIR, file.filename)
        file.save(file_path)

        encode_result, file_type = encoder.encode_file(file_path)

        records = []
        for i, (text, vec) in enumerate(encode_result):
            records.append(
                VectorRecord(
                    key=f"{root_key}:{i}",
                    vector=vec,
                    attrs=attrs,
                    file_type=file_type,
                    file_path=file_path,
                    root_key=root_key,
                    chunk_id=str(i),
                    chunk_text=text
                )
            )

        ip, port = get_node_by_key(zk, root_key)
        client = client_pool.get_client(ip, port)

        req = BatchWriteRequest(records=records, replica_num=replica_num)
        resp = client.batch_add_records(req)

        return jsonify({"code": resp.code, "msg": resp.msg})


    @app.route("/query", methods=["POST"])
    def query():
        from .core import encoder, zk, client_pool

        data = request.json or {}

        if "text" in data:
            vector = encoder.encode_text_chunks(data["text"])[0][1]
        elif "vector" in data:
            vector = data["vector"]
        else:
            return jsonify({"code": -1, "msg": "Missing input"}), 400

        ip, port = get_node_by_key(zk, "query")
        client = client_pool.get_client(ip, port)

        req = QueryRequest(
            vector=vector,
            filter=data.get("filter", {}),
            top_k=data.get("top_k", 10),
            aggregate=data.get("aggregate", True)
        )
        resp = client.query(req)

        return jsonify({
            "code": resp.code,
            "records": [
                {
                    "key": r.key,
                    "chunk_id": r.chunk_id,
                    "chunk_text": r.chunk_text,
                    "attrs": r.attrs
                } for r in resp.records
            ]
        })


    @app.route("/delete", methods=["POST"])
    def delete():
        from .core import zk, client_pool

        root_key = (request.json or {}).get("key")
        if not root_key:
            return jsonify({"code": -1, "msg": "Missing key"}), 400

        ip, port = get_node_by_key(zk, root_key)
        client = client_pool.get_client(ip, port)

        resp = client.delete_record(DeleteRequest(key=root_key))
        return jsonify({"code": resp.code, "msg": resp.msg})
