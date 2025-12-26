# ZooKeeper配置
ZK_SERVERS = "127.0.0.1:2181"
ZK_SESSION_TIMEOUT = 30000  # 会话超时时间（ms）
ZK_BASE_PATH = "/vector_db"
ZK_NODES_PATH = f"{ZK_BASE_PATH}/nodes"
ZK_SHARDS_PATH = f"{ZK_BASE_PATH}/shards"

# 单例ZK连接标识
ZK_SINGLETON_KEY = "vector_db_zk_singleton"