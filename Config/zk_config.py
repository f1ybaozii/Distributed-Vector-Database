# ZooKeeper配置
ZK_SERVERS = "192.168.14.149:2181,192.168.14.150:2181,192.168.14.151:2181"
ZK_SESSION_TIMEOUT = 30000  # 会话超时时间（ms）
ZK_BASE_PATH = "/vector_db"
ZK_NODES_PATH = f"{ZK_BASE_PATH}/nodes"
ZK_SHARDS_PATH = f"{ZK_BASE_PATH}/shards"

# 单例ZK连接标识
ZK_SINGLETON_KEY = "vector_db_zk_singleton"