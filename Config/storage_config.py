# 存储基础配置
VECTOR_DIM = 512  # CLIP-ViT-B/32默认512维
SHARD_COUNT = 4   # 分片数量
REPLICA_COUNT = 2 # 副本数量

# WAL配置
WAL_BASE_DIR = "./Static/wal"
WAL_ROTATE_SIZE = 1024 * 1024 * 100  # 100MB日志轮转

# 原始数据存储配置
RAW_STORAGE_TYPE = "sqlite"  # 默认存储类型：file/sqlite/mysql
RAW_STORAGE_CONFIG = {
    "sqlite": {"db_path": "./Data/raw_data.db"},
    "file": {"file_path": "./Data/raw_data.json"},
    "mysql": {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "123456",
        "db": "vector_db"
    }
}

# Faiss索引配置
FAISS_HNSW_M = 16          # HNSW邻居数
FAISS_EF_CONSTRUCTION = 40 # 构建时EF值
FAISS_EF_SEARCH = 64       # 检索时EF值