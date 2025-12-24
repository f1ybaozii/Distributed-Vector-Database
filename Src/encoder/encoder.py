from transformers import CLIPProcessor, CLIPModel
import torch
from PIL import Image
import os
import warnings
import tiktoken
from config import ENCODER_CONFIG, MODEL_BASE_DIR

warnings.filterwarnings("ignore")

class VectorEncoder:
    def __init__(self, model_cache_dir: str = None,
                 max_tokens: int = ENCODER_CONFIG["max_tokens"],
                 chunk_overlap: int = ENCODER_CONFIG["chunk_overlap"],
                 device: str = ENCODER_CONFIG["device"]):
        self.max_tokens = max_tokens
        self.chunk_overlap = chunk_overlap
        self.device = torch.device(device)

        # 模型路径（优先自定义，否则用默认）
        self.model_cache_dir = model_cache_dir or MODEL_BASE_DIR
        os.makedirs(self.model_cache_dir, exist_ok=True)

        # 禁用网络，加载本地模型
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
        self.model = CLIPModel.from_pretrained(
            self.model_cache_dir,
            local_files_only=True,
            torch_dtype=torch.float32,
            low_cpu_mem_usage=True
        )
        self.processor = CLIPProcessor.from_pretrained(
            self.model_cache_dir,
            local_files_only=True
        )
        self.model.to(self.device)
        self.model.eval()
        self.vector_dim = 512

        # Tokenizer初始化
        self.tokenizer = tiktoken.encoding_for_model("gpt-3.5-turbo")

    def _split_text_to_chunks(self, text: str) -> list:
        if not text or text.strip() == "":
            raise ValueError("输入文本不能为空")
        
        tokens = self.tokenizer.encode(text.strip())
        chunks = []
        start = 0
        while start < len(tokens):
            end = start + self.max_tokens
            chunk_tokens = tokens[start:end]
            chunk_text = self.tokenizer.decode(chunk_tokens)
            chunks.append(chunk_text.strip())
            start += self.max_tokens - self.chunk_overlap
        return chunks

    def encode_text_chunks(self, text: str) -> list:
        chunks = self._split_text_to_chunks(text)
        chunk_vectors = []
        for chunk in chunks:
            inputs = self.processor(
                text=chunk,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=self.max_tokens
            ).to(self.device)
            
            with torch.no_grad():
                vec = self.model.get_text_features(** inputs)
                vec = torch.nn.functional.normalize(vec, p=2, dim=1)
                vec = vec.squeeze().cpu().tolist()
            chunk_vectors.append((chunk, vec))
        return chunk_vectors

    def encode_text_file_chunks(self, file_path: str) -> list:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在：{file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
        except UnicodeDecodeError:
            with open(file_path, 'r', encoding='gbk') as f:
                text = f.read()
        return self.encode_text_chunks(text)

    def encode_image_file(self, file_path: str) -> list:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"图片文件不存在：{file_path}")
        
        image = Image.open(file_path).convert("RGB")
        inputs = self.processor(images=image, return_tensors="pt").to(self.device)
        with torch.no_grad():
            vec = self.model.get_image_features(** inputs)
            vec = torch.nn.functional.normalize(vec, p=2, dim=1)
        return vec.squeeze().cpu().tolist()

    def encode_file(self, file_path: str) -> tuple[list, str]:
        ext = os.path.splitext(file_path)[-1].lower()
        if ext in ['.txt', '.md', '.json', '.csv', '.xml']:
            return self.encode_text_file_chunks(file_path), "text"
        elif ext in ['.jpg', '.png', '.jpeg', '.bmp', '.gif', '.webp']:
            return [(None, self.encode_image_file(file_path))], "image"
        else:
            raise ValueError(f"不支持的文件类型：{ext}，仅支持文本/图片")

    def get_vector_dim(self) -> int:
        return self.vector_dim

# 快捷创建接口
def create_encoder(model_cache_dir: str = None,
                   max_tokens: int = None,
                   chunk_overlap: int = None,
                   device: str = None) -> VectorEncoder:
    return VectorEncoder(
        model_cache_dir=model_cache_dir,
        max_tokens=max_tokens or ENCODER_CONFIG["max_tokens"],
        chunk_overlap=chunk_overlap or ENCODER_CONFIG["chunk_overlap"],
        device=device or ENCODER_CONFIG["device"]
    )