"""全局可配置参数（统一管理，避免硬编码）"""
import os
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # Src目录
PROJECT_ROOT = os.path.dirname(ROOT_DIR)  # Src的同级目录（根目录）
# 基础路径配置
# 2. 模型路径：指向 Src/../Model/clip/clip-vit-base-patch32/
MODEL_BASE_DIR = os.path.join(PROJECT_ROOT, "Model", "clip-vit-base-patch32")

# 3. 数据路径（WAL/向量数据）：指向 Src/../Data/
DATA_DIR = os.path.join(PROJECT_ROOT, "Data")
WAL_BASE_DIR = os.path.join(DATA_DIR, "wal")  # WAL日志统一放到Data/
TMP_DIR = os.path.join(DATA_DIR, "tmp")  # 临时文件目录
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
    "port": 8081,
    "zk_address": "127.0.0.1:2181"
}