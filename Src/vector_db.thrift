namespace py vector_db

struct VectorRecord {
    1: required string key,
    2: required list<double> vector,
    3: optional map<string, string> attrs,
    4: optional string file_type,
    5: optional string file_path,
    6: optional string root_key,
    7: optional string chunk_id,
    8: optional string chunk_text
}

struct BatchWriteRequest {
    1: required list<VectorRecord> records,
    2: required i32 replica_num = 3
}

struct WriteRequest {
    1: required VectorRecord record,
    2: required i32 replica_num = 3
}

struct DeleteRequest {
    1: required string key
}

struct QueryRequest {
    1: optional list<double> vector,
    2: optional map<string, string> filter,
    3: optional i32 top_k = 10,
    4: optional bool aggregate = true
}

struct QueryResponse {
    1: required list<VectorRecord> records,
    2: required i32 code = 0,
    3: optional string msg = "success",
    4: optional map<string, list<VectorRecord>> aggregated_records
}

struct BaseResponse {
    1: required i32 code = 0,
    2: optional string msg = "success"
}

service VectorDBService {
    BaseResponse add_record(1: WriteRequest request),
    BaseResponse batch_add_records(1: BatchWriteRequest request),
    BaseResponse update_record(1: WriteRequest request),
    BaseResponse delete_record(1: DeleteRequest request),
    QueryResponse query(1: QueryRequest request)
}