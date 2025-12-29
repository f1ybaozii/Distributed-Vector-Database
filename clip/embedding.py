import os
import sys
import numpy as np
import cv2
from PIL import Image
from pathlib import Path
from loguru import logger

# ========== 1. 依赖导入 & 全局配置 ==========
# CLIP模型相关
import torch
from transformers import CLIPProcessor, CLIPModel

# Thrift接口相关（适配你的分布式向量库）
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from src.vector_db import CoordinatorService  # 替换为你的CoordinatorService路径
from src.vector_db.ttypes import VectorData, SearchRequest  # 替换为你的ttypes路径

# 全局配置
CLIP_MODEL_PATH = "./Model/clip-vit-base-patch32"  # CLIP模型路径
VECTOR_DB_COORD_ADDR = "192.168.14.149:8081"  # 协调节点地址（替换为你的实际地址）
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
SUPPORTED_IMAGE_EXT = [".jpg", ".jpeg", ".png", ".bmp", ".gif"]  # 支持的图片格式

# ========== 2. CLIP模型初始化（单例） ==========
class CLIPEmbedding:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # 加载CLIP模型和处理器
            logger.info(f"加载CLIP模型：{CLIP_MODEL_PATH}（设备：{DEVICE}）")
            cls._instance.model = CLIPModel.from_pretrained(CLIP_MODEL_PATH).to(DEVICE)
            cls._instance.processor = CLIPProcessor.from_pretrained(CLIP_MODEL_PATH)
        return cls._instance

    def image2vec(self, image_path: str) -> list:
        """图片转512维向量（归一化）"""
        try:
            # 读取并预处理图片
            image = Image.open(image_path).convert("RGB")
            inputs = self.processor(images=image, return_tensors="pt").to(DEVICE)
            # 生成嵌入向量
            with torch.no_grad():
                vec = self.model.get_image_features(**inputs)
            # 归一化 + 转列表（适配向量库存储）
            vec = vec / vec.norm(dim=-1, keepdim=True)
            return vec.squeeze().cpu().numpy().tolist()
        except Exception as e:
            logger.error(f"图片[{image_path}]嵌入失败：{e}")
            return None

    def text2vec(self, text: str) -> list:
        """文本转512维向量（归一化）"""
        try:
            # 预处理文本
            inputs = self.processor(text=text, return_tensors="pt").to(DEVICE)
            # 生成嵌入向量
            with torch.no_grad():
                vec = self.model.get_text_features(**inputs)
            # 归一化 + 转列表
            vec = vec / vec.norm(dim=-1, keepdim=True)
            return vec.squeeze().cpu().numpy().tolist()
        except Exception as e:
            logger.error(f"文本[{text}]嵌入失败：{e}")
            return None

# ========== 3. 向量库接口封装 ==========
class VectorDBClient:
    def __init__(self, coord_addr: str = VECTOR_DB_COORD_ADDR):
        self.host, self.port = coord_addr.split(":")
        self.port = int(self.port)
        self.client = self._init_client()

    def _init_client(self):
        """初始化向量库客户端"""
        try:
            transport = TSocket.TSocket(self.host, self.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = CoordinatorService.Client(protocol)
            transport.open()
            logger.info(f"向量库客户端连接成功：{self.host}:{self.port}")
            return client
        except Exception as e:
            logger.error(f"向量库连接失败：{e}")
            sys.exit(1)

    def put_image_vector(self, image_path: str):
        """单张图片入库：KEY=文件名，metadata=文件路径"""
        # 1. 生成向量
        clip = CLIPEmbedding()
        vec = clip.image2vec(image_path)
        if vec is None:
            return False
        
        # 2. 构造入库数据
        file_name = os.path.basename(image_path)  # KEY=文件名
        metadata = {"file_path": os.path.abspath(image_path)}  # metadata存完整路径
        
        vector_data = VectorData(
            key=file_name,
            vector=vec,
            metadata=metadata
        )
        
        # 3. 调用put接口
        try:
            resp = self.client.put(vector_data)
            if resp.success:
                logger.info(f"图片[{file_name}]入库成功")
                return True
            else:
                logger.error(f"图片[{file_name}]入库失败：{resp.message}")
                return False
        except Exception as e:
            logger.error(f"图片[{file_name}]入库异常：{e}")
            return False

    def batch_put_images(self, image_dir: str):
        """批量处理文件夹下的所有图片入库"""
        image_dir = os.path.abspath(image_dir)
        if not os.path.isdir(image_dir):
            logger.error(f"图片文件夹不存在：{image_dir}")
            return
        
        # 遍历所有图片文件
        image_paths = []
        for file in os.listdir(image_dir):
            ext = os.path.splitext(file)[-1].lower()
            if ext in SUPPORTED_IMAGE_EXT:
                image_paths.append(os.path.join(image_dir, file))
        
        if not image_paths:
            logger.warning(f"文件夹[{image_dir}]下无有效图片")
            return
        
        # 批量入库
        success_count = 0
        for img_path in image_paths:
            if self.put_image_vector(img_path):
                success_count += 1
        
        logger.info(f"批量入库完成：总数={len(image_paths)}，成功={success_count}，失败={len(image_paths)-success_count}")

    def text_search(self, text: str, top_k: int = 5) -> list:
        """文本检索：返回topk图片路径列表"""
        # 1. 文本转向量
        clip = CLIPEmbedding()
        vec = clip.text2vec(text)
        if vec is None:
            return []
        
        # 2. 构造检索请求
        search_req = SearchRequest(
            query_vector=vec,
            top_k=top_k
        )
        
        # 3. 调用search接口
        try:
            resp = self.client.search(search_req)
            if not resp.success:
                logger.error(f"检索失败：{resp.message}")
                return []
            
            # 4. 解析结果（提取metadata中的文件路径）
            image_paths = []
            for vec_data in resp.search_result.vectors:
                if "file_path" in vec_data.metadata:
                    image_paths.append(vec_data.metadata["file_path"])
            
            logger.info(f"文本[{text}]检索完成，返回{len(image_paths)}张图片")
            return image_paths
        except Exception as e:
            logger.error(f"检索异常：{e}")
            return []

# ========== 4. 图片拼接展示 ==========
def concat_images(image_paths: list, top_k: int, save_path: str = "search_result.jpg"):
    """拼接topk图片并展示/保存"""
    # 过滤有效图片
    valid_imgs = []
    for img_path in image_paths[:top_k]:
        if os.path.exists(img_path):
            img = cv2.imread(img_path)
            if img is not None:
                # 统一尺寸（避免拼接变形）
                img = cv2.resize(img, (224, 224))
                valid_imgs.append(img)
        else:
            logger.warning(f"图片不存在：{img_path}")
    
    if not valid_imgs:
        logger.error("无有效图片可拼接")
        return
    
    # 计算拼接布局（2列排列）
    rows = (len(valid_imgs) + 1) // 2
    cols = min(2, len(valid_imgs))
    canvas_h = rows * 224
    canvas_w = cols * 224
    
    # 创建空白画布
    canvas = np.zeros((canvas_h, canvas_w, 3), dtype=np.uint8)
    
    # 拼接图片
    for idx, img in enumerate(valid_imgs):
        row = idx // 2
        col = idx % 2
        canvas[row*224:(row+1)*224, col*224:(col+1)*224, :] = img
    
    # 保存+展示
    cv2.imwrite(save_path, canvas)
    logger.info(f"拼接结果已保存：{os.path.abspath(save_path)}")
    cv2.imshow(f"Top-{top_k} Search Result", canvas)
    cv2.waitKey(0)
    cv2.destroyAllWindows()

# ========== 5. 主函数（直接运行） ==========
def main():
    # 配置参数（按需修改）
    IMAGE_DIR = "./Data"  # 待处理图片文件夹
    SEARCH_TEXT = "a cat sitting on the sofa"  # 检索文本
    TOP_K = 20  # 返回topk数量

    # 步骤1：初始化向量库客户端
    db_client = VectorDBClient()

    # 步骤2：批量入库图片（注释掉则跳过入库，直接检索）
    db_client.batch_put_images(IMAGE_DIR)

    # 步骤3：文本检索
    image_paths = db_client.text_search(SEARCH_TEXT, TOP_K)

    # 步骤4：拼接展示结果
    if image_paths:
        concat_images(image_paths, TOP_K)
    else:
        logger.warning("未检索到匹配图片")

if __name__ == "__main__":
    # 初始化日志
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>",
        level="INFO"
    )
    main()