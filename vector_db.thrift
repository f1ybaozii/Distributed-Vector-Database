namespace py vector_db
namespace java com.vector_db

// 向量数据结构
struct VectorRecord {
    1: required string key,          // 唯一键
    2: required list<double> vector, // 向量数据
    3: optional map<string, string> attrs, // 结构化字段（时间、类型等）
    4: optional string file_type,    // 文件类型（text/image）
    5: optional string file_path     // 原始文件路径
}

// 请求/响应结构
struct WriteRequest {
    1: required VectorRecord record,
    2: required i32 replica_num = 3  // 副本数
}

struct DeleteRequest {
    1: required string key
}

struct QueryRequest {
    1: optional list<double> vector, // 向量检索
    2: optional map<string, string> filter, // 条件过滤（如time>2025-01-01）
    3: optional i32 top_k = 10       // 召回数
}

struct QueryResponse {
    1: required list<VectorRecord> records,
    2: required i32 code = 0,
    3: optional string msg = "success"
}

struct BaseResponse {
    1: required i32 code = 0,
    2: optional string msg = "success"
}

// 服务接口
service VectorDBService {
    BaseResponse add_record(1: WriteRequest request),
    BaseResponse update_record(1: WriteRequest request),
    BaseResponse delete_record(1: DeleteRequest request),
    QueryResponse query(1: QueryRequest request)
}