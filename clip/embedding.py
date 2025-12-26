import torch
import open_clip
from PIL import Image
from loguru import logger
from Config import VECTOR_DIM
from src.utils import normalize_vector

# 单例锁
_clip_lock = torch.locks.Lock()
_clip_instance = None

class CLIPEmbedding:
    """CLIP-ViT-B/32嵌入生成器（仅生成向量，移除原始存储）"""
    def __init__(self):
        self.model_name = "ViT-B-32"
        self.pretrained = "openai"
        self.device = "cuda" if torch.cuda.is_available() else "cpu"

        # 模型加载到Model目录
        logger.info(f"加载CLIP模型到{self.device}...")
        self.model, self.preprocess, self.tokenizer = open_clip.create_model_and_transforms(
            self.model_name,
            pretrained=self.pretrained,
            device=self.device,
            cache_dir="./Model"
        )
        self.model.eval()

    @torch.no_grad()
    def embed_text(self, text: str) -> list:
        """文本→512维向量"""
        try:
            text_tokens = self.tokenizer([text]).to(self.device)
            text_emb = self.model.encode_text(text_tokens)
            text_emb = normalize_vector(text_emb.cpu().numpy().squeeze())
            if len(text_emb) != VECTOR_DIM:
                raise ValueError(f"维度错误：{len(text_emb)}≠{VECTOR_DIM}")
            return text_emb.tolist()
        except Exception as e:
            logger.error(f"文本嵌入失败：{e}")
            raise

    @torch.no_grad()
    def embed_image(self, image_path: str) -> list:
        """图像→512维向量"""
        try:
            image = Image.open(image_path).convert("RGB")
            image_tensor = self.preprocess(image).unsqueeze(0).to(self.device)
            img_emb = self.model.encode_image(image_tensor)
            img_emb = normalize_vector(img_emb.cpu().numpy().squeeze())
            if len(img_emb) != VECTOR_DIM:
                raise ValueError(f"维度错误：{len(img_emb)}≠{VECTOR_DIM}")
            return img_emb.tolist()
        except Exception as e:
            logger.error(f"图像嵌入失败：{e}")
            raise

def get_clip_embedding() -> CLIPEmbedding:
    """单例获取"""
    global _clip_instance
    with _clip_lock:
        if _clip_instance is None:
            _clip_instance = CLIPEmbedding()
        return _clip_instance