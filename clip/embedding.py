import os
import sys
import torch
from loguru import logger
from PIL import Image
from transformers import CLIPProcessor, CLIPModel

# 添加项目根目录到sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
print(PROJECT_ROOT)

# 全局配置
CLIP_MODEL_PATH = os.path.join(PROJECT_ROOT, "Model/clip-vit-base-patch32")
DEVICE = "cpu"
SUPPORTED_IMAGE_EXT = [".jpg", ".jpeg", ".png", ".bmp", ".gif"]

class CLIPEmbedding:
    """CLIP模型嵌入工具类（单例模式）"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_model()
        return cls._instance

    def _init_model(self):
        """初始化CLIP模型和处理器"""
        try:
            logger.info(f"加载CLIP模型：{CLIP_MODEL_PATH}（设备：{DEVICE}）")
            self.model = CLIPModel.from_pretrained(CLIP_MODEL_PATH).to(DEVICE)
            self.processor = CLIPProcessor.from_pretrained(CLIP_MODEL_PATH)
            logger.info("CLIP模型加载成功")
        except Exception as e:
            logger.error(f"CLIP模型加载失败：{e}")
            raise e

    def image2vec(self, image_path: str) -> list:
        """
        图片转512维向量（归一化）
        :param image_path: 图片路径
        :return: 512维向量列表，失败返回None
        """
        if not os.path.exists(image_path):
            logger.error(f"图片不存在：{image_path}")
            return None
        
        ext = os.path.splitext(image_path)[-1].lower()
        if ext not in SUPPORTED_IMAGE_EXT:
            logger.error(f"不支持的图片格式：{ext}（支持：{SUPPORTED_IMAGE_EXT}）")
            return None

        try:
            # 读取并预处理图片
            image = Image.open(image_path).convert("RGB")
            inputs = self.processor(images=image, return_tensors="pt").to(DEVICE)
            
            # 生成嵌入向量
            with torch.no_grad():
                vec = self.model.get_image_features(**inputs)
            
            # 归一化（必须，保证检索精度）
            vec = vec / vec.norm(dim=-1, keepdim=True)
            return vec.squeeze().cpu().numpy().tolist()
        except Exception as e:
            logger.error(f"图片[{image_path}]嵌入失败：{e}")
            return None

    def text2vec(self, text: str) -> list:
        """
        文本转512维向量（归一化）
        :param text: 输入文本
        :return: 512维向量列表，失败返回None
        """
        if not text or text.strip() == "":
            logger.error("空文本无法嵌入")
            return None

        try:
            # 预处理文本
            inputs = self.processor(text=text, return_tensors="pt").to(DEVICE)
            
            # 生成嵌入向量
            with torch.no_grad():
                vec = self.model.get_text_features(**inputs)
            
            # 归一化
            vec = vec / vec.norm(dim=-1, keepdim=True)
            return vec.squeeze().cpu().numpy().tolist()
        except Exception as e:
            logger.error(f"文本[{text}]嵌入失败：{e}")
            return None

# ========== 嵌入测试主函数 ==========
def main():
    """测试CLIP嵌入功能"""
    # 初始化日志
    logger.remove()
    logger.add(sys.stdout, format="<green>{time}</green> | <level>{level}</level> | {message}", level="INFO")

    # 1. 初始化CLIP嵌入工具
    clip = CLIPEmbedding()

    # 2. 测试图片嵌入
    test_image_path = os.path.join(PROJECT_ROOT, "Data/__1Mu7EZXOM.jpg")  # 替换为你的测试图片
    img_vec = clip.image2vec(test_image_path)
    if img_vec:
        logger.info(f"图片嵌入成功，向量维度：{len(img_vec)}，前5维：{img_vec[:5]}")
    else:
        logger.error("图片嵌入测试失败")

    # 3. 测试文本嵌入
    test_text = "a cat sitting on the sofa"
    text_vec = clip.text2vec(test_text)
    if text_vec:
        logger.info(f"文本嵌入成功，向量维度：{len(text_vec)}，前5维：{text_vec[:5]}")
    else:
        logger.error("文本嵌入测试失败")

if __name__ == "__main__":
    main()