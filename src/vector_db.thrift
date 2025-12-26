/**
 * 分布式向量数据库 RPC 定义
 * 生成路径：src/vector_db（匹配要求的 src.vector_db 命名空间）
 */
namespace py src.vector_db  // 核心修改：命名空间改为 src.vector_db
namespace cpp vector_db.rpc
namespace java com.vector_db.rpc

// -------------------------- 基础数据结构 --------------------------
/**
 * 向量数据结构
 */
struct VectorData {
    1: required string key,                // 向量唯一标识
    2: optional list<double> vector,       // 向量值（512维，CLIP生成）
    3: optional map<string, string> metadata,  // 元数据（标签/时间等）
    4: optional i64 timestamp = 0,        // 时间戳（毫秒）
}

/**
 * 检索请求结构
 */
struct SearchRequest {
    1: required list<double> query_vector,  // 查询向量
    2: optional i32 top_k = 5,              // 返回Top-K数量
    3: optional map<string, string> filter, // 过滤条件（如tag=test）
    4: optional double threshold = 0.0,     // 相似度阈值（Faiss距离）
}

/**
 * 检索结果结构
 */
struct SearchResult {
    1: optional list<string> keys,          // 匹配的向量Key
    2: optional list<double> scores,        // 相似度分数（Faiss距离）
    3: optional list<VectorData> vectors,   // 匹配的完整向量数据
    // 新增：每个匹配结果对应的元数据列表（与keys/scores一一对应）
    4: optional list<map<string, string>> metadatas,  // 匹配结果的元数据列表
}

/**
 * 通用响应结构
 */
struct Response {
    1: required bool success,               // 操作是否成功
    2: optional string message = "",        // 响应信息
    3: optional VectorData vector_data,     // 单个向量结果（get/put）
    4: optional SearchResult search_result, // 检索结果（search）
}

// -------------------------- 数据节点服务接口 --------------------------
service VectorNodeService {
    /**
     * 写入/更新向量
     */
    Response put(1: VectorData data),

    /**
     * 删除向量
     */
    Response delete(1: string key),

    /**
     * 获取向量
     */
    Response get(1: string key),

    /**
     * 向量检索
     */
    Response search(1: SearchRequest req),

    /**
     * 副本同步接口（主节点→副本节点）
     */
    Response replicate(1: VectorData data, 2: string op_type),

    /**
     * 重放WAL日志（故障恢复）
     */
    Response replay_wal(),
    /**
     * 新增：主动下线（优雅断开连接）
     */
    Response offline(),  // 无参数，返回操作结果
    Response get_all_vectors(),
}

// -------------------------- 协调节点服务接口 --------------------------
service CoordinatorService {
    /**
     * 注册数据节点到ZK
     */
    Response register_node(1: string node_id, 2: string address),

    /**
     * 列出所有在线数据节点
     */
    Response list_nodes(),

    /**
     * 路由写入请求到分片主节点
     */
    Response put(1: VectorData data),

    /**
     * 路由删除请求到分片主节点
     */
    Response delete(1: string key),

    /**
     * 路由获取请求到分片主节点
     */
    Response get(1: string key),

    /**
     * 路由检索请求（广播到所有节点+结果合并）
     */
    Response search(1: SearchRequest req),
}