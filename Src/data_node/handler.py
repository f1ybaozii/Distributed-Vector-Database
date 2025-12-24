"""数据节点Thrift处理器"""
import faiss
import vector_db.VectorDBService as VectorDBService

class DataNodeHandler(VectorDBService.Iface):
    def __init__(self, data_node):
        self.data_node = data_node

    def add_record(self, request):
        with self.data_node.lock:
            record = request.record
            self.data_node.key_to_record[record.key] = {
                "key": record.key,
                "vector": record.vector,
                "attrs": record.attrs,
                "file_type": record.file_type,
                "file_path": record.file_path,
                "root_key": record.root_key,
                "chunk_id": record.chunk_id,
                "chunk_text": record.chunk_text
            }
            self.data_node.wal.write("add", self.data_node.key_to_record[record.key])
            self.data_node.index.add(faiss.array_to_vec(record.vector).reshape(1, -1))
            self.data_node._sync_to_replicas("add", request)
        return vector_db.BaseResponse(code=0, msg="success")

    def batch_add_records(self, request):
        with self.data_node.lock:
            records = request.records
            for record in records:
                self.data_node.key_to_record[record.key] = {
                    "key": record.key,
                    "vector": record.vector,
                    "attrs": record.attrs,
                    "file_type": record.file_type,
                    "file_path": record.file_path,
                    "root_key": record.root_key,
                    "chunk_id": record.chunk_id,
                    "chunk_text": record.chunk_text
                }
            self.data_node.wal.write("batch_add", [self.data_node.key_to_record[r.key] for r in records])
            vectors = [faiss.array_to_vec(r.vector).reshape(1, -1) for r in records]
            self.data_node.index.add(faiss.concat_vecs(vectors))
            self.data_node._sync_to_replicas("batch_add", request)
        return vector_db.BaseResponse(code=0, msg="batch add success")

    def delete_record(self, request):
        with self.data_node.lock:
            key = request.key
            to_delete_keys = [k for k in self.data_node.key_to_record.keys() if self.data_node.key_to_record[k].get("root_key") == key] if ":" not in key else [key]
            for k in to_delete_keys:
                del self.data_node.key_to_record[k]
            self.data_node.wal.write("delete", {"key": key})
            self.data_node._rebuild_index()
            self.data_node._sync_to_replicas("delete", request)
        return vector_db.BaseResponse(code=0, msg="delete success")

    def query(self, request):
        with self.data_node.lock:
            candidate_keys = set()
            if request.vector:
                query_vec = faiss.array_to_vec(request.vector).reshape(1, -1)
                distances, indices = self.data_node.index.search(query_vec, request.top_k * 5)
                key_list = list(self.data_node.key_to_record.keys())
                candidate_keys = set([key_list[i] for i in indices[0] if i < len(key_list)])

            filtered_records = []
            for key in candidate_keys:
                record = self.data_node.key_to_record[key]
                match = True
                for k, v in (request.filter or {}).items():
                    if k not in record.get("attrs", {}):
                        match = False
                        break
                    if v.startswith(">"):
                        match = record["attrs"][k] > v[1:]
                    elif v.startswith("<"):
                        match = record["attrs"][k] < v[1:]
                    else:
                        match = record["attrs"][k] == v
                if match:
                    filtered_records.append(vector_db.VectorRecord(
                        key=record["key"],
                        vector=record["vector"],
                        attrs=record["attrs"],
                        file_type=record["file_type"],
                        file_path=record["file_path"],
                        root_key=record["root_key"],
                        chunk_id=record["chunk_id"],
                        chunk_text=record["chunk_text"]
                    ))

            aggregated = {}
            if request.aggregate:
                for r in filtered_records:
                    root_key = r.root_key or r.key
                    aggregated[root_key] = aggregated.get(root_key, []) + [r]
                aggregated = dict(list(aggregated.items())[:request.top_k])

            return vector_db.QueryResponse(
                records=filtered_records[:request.top_k],
                code=0,
                msg="success",
                aggregated_records=aggregated
            )