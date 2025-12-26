import numpy as np
from Config import VECTOR_DIM

def vector_to_list(vec: np.ndarray) -> list:
    """numpy向量转列表"""
    return vec.tolist() if isinstance(vec, np.ndarray) else vec

def list_to_vector(lst: list) -> np.ndarray:
    """列表转numpy向量（校验维度）"""
    vec = np.array(lst, dtype=np.float32)
    if len(vec) != VECTOR_DIM:
        raise ValueError(f"向量维度错误：期望{VECTOR_DIM}，实际{len(vec)}")
    return vec

def normalize_vector(vec: np.ndarray) -> np.ndarray:
    """向量归一化"""
    return vec / np.linalg.norm(vec) if np.linalg.norm(vec) > 0 else vec