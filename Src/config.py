"""全局可配置参数（统一管理，避免硬编码）"""
import os

# 基础路径配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TMP_DIR = os.path.join(BASE_DIR, "data/tmp")
WAL_BASE_DIR = os.path.join(BASE_DIR, "data/wal")
MODEL_BASE_DIR = os.path.join(BASE_DIR, "models/clip")

# 编码器默认配置
ENCODER_CONFIG = {
    "model_name": "openai/clip-vit-base-patch32",
    "max_tokens": 77,
    "chunk_overlap": 10,
    "device": "cpu"
}

# 数据节点默认配置
DATA_NODE_CONFIG = {
    "vector_dim": 512,
    "replica_num": 3,
    "hnsw_ef_construction": 40,
    "hnsw_ef_search": 16
}

# 网关默认配置
GATEWAY_CONFIG = {
    "host": "0.0.0.0",
    "port": 8080,
    "zk_address": "127.0.0.1:2181"
}