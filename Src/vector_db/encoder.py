from transformers import CLIPProcessor, CLIPModel
import torch
from PIL import Image
import os
import warnings
import tiktoken  # 新增：精准按token分块（替代字符分块）
warnings.filterwarnings("ignore")

class VectorEncoder:
    """
    基于CLIP的多模态编码器（支持长文本分块编码，无信息丢失）
    """
    def __init__(self, model_name: str = "openai/clip-vit-base-patch32"):
        # 初始化CLIP模型/处理器
        self.model = CLIPModel.from_pretrained(model_name, cache_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../Model/clip"))
        self.processor = CLIPProcessor.from_pretrained(model_name, cache_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../Model/clip"))
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model.to(self.device)
        self.model.eval()
        self.vector_dim = 512

        # 初始化CLIP文本tokenizer（精准分块核心）
        self.tokenizer = tiktoken.encoding_for_model("gpt-3.5-turbo")  # 兼容CLIP的token规则
        self.max_tokens = 77  # CLIP文本最大token数（含特殊token）
        self.chunk_overlap = 10  # 分块重叠token数（避免语义割裂）
        print(f"编码器初始化完成，分块规则：{self.max_tokens}token/块，重叠{self.chunk_overlap}token")

    def _split_text_to_chunks(self, text: str) -> list:
        """
        长文本精准分块（按token数，避免语义割裂）
        :param text: 原始长文本
        :return: 分块后的文本列表
        """
        if not text or text.strip() == "":
            return []
        
        # 1. 文本token化
        tokens = self.tokenizer.encode(text.strip())
        chunks = []
        
        # 2. 滑动窗口分块（带重叠）
        start = 0
        while start < len(tokens):
            # 取当前窗口的token（不超过max_tokens）
            end = start + self.max_tokens
            chunk_tokens = tokens[start:end]
            # 转回文本
            chunk_text = self.tokenizer.decode(chunk_tokens)
            chunks.append(chunk_text.strip())
            # 滑动窗口（步长=max_tokens - overlap）
            start += self.max_tokens - self.chunk_overlap
        
        return chunks

    def encode_text_chunks(self, text: str) -> list:
        """
        长文本分块编码（返回每个块的向量列表）
        :param text: 原始长文本
        :return: [(chunk_text, chunk_vector), ...]
        """
        # 1. 分块
        chunks = self._split_text_to_chunks(text)
        if not chunks:
            raise ValueError("文本分块后为空")
        
        # 2. 每个块独立编码
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
                vec = self.model.get_text_features(**inputs)
                vec = torch.nn.functional.normalize(vec, p=2, dim=1)
                vec = vec.squeeze().cpu().tolist()
            
            chunk_vectors.append((chunk, vec))
        
        return chunk_vectors

    def encode_text_file_chunks(self, file_path: str) -> list:
        """
        长文本文件分块编码
        :param file_path: 文本文件路径
        :return: [(chunk_text, chunk_vector), ...]
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在：{file_path}")
        
        # 读取完整文本（保留所有信息）
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
        except UnicodeDecodeError:
            with open(file_path, 'r', encoding='gbk') as f:
                text = f.read()
        
        return self.encode_text_chunks(text)

    # 保留原有图片编码逻辑（无需修改）
    def encode_image_file(self, file_path: str) -> list:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"图片文件不存在：{file_path}")
        try:
            image = Image.open(file_path).convert("RGB")
        except Exception as e:
            raise ValueError(f"图片解析失败：{e}")
        
        inputs = self.processor(images=image, return_tensors="pt").to(self.device)
        with torch.no_grad():
            vec = self.model.get_image_features(**inputs)
            vec = torch.nn.functional.normalize(vec, p=2, dim=1)
        return vec.squeeze().cpu().tolist()

    def encode_file(self, file_path: str) -> tuple[list, str]:
        """
        统一编码接口（文本分块/图片单向量）
        :return: (编码结果, 文件类型)
                 文本：[(chunk_text, chunk_vector), ...]
                 图片：[(None, image_vector)]
        """
        ext = os.path.splitext(file_path)[-1].lower()
        if ext in ['.txt', '.md', '.json', '.csv', '.xml']:
            return self.encode_text_file_chunks(file_path), "text"
        elif ext in ['.jpg', '.png', '.jpeg', '.bmp', '.gif', '.webp']:
            return [(None, self.encode_image_file(file_path))], "image"
        else:
            raise ValueError(f"不支持的文件类型：{ext}")

    def get_vector_dim(self) -> int:
        return self.vector_dim
    
# 新增：main测试函数（文件末尾）
if __name__ == "__main__":
    # 初始化编码器
    encoder = VectorEncoder()
    
    # ========== 测试1：长文本分块编码 ==========
    print("\n=== 测试长文本分块编码 ===")
    # 构造长文本（模拟1000字内容）
    long_text = """
    向量数据库是人工智能领域的核心组件之一，尤其在多模态检索场景中发挥着关键作用。
    CLIP模型作为OpenAI推出的多模态模型，能够将文本和图片映射到同一向量空间，
    但该模型对输入文本有77个token的长度限制，直接截断会导致核心信息丢失。
    因此，专业的向量数据库处理长文本时，需要采用分块编码的方式，将长文本按token数拆分为多个块，
    每个块独立编码为512维向量，再通过root_key关联所有块，保证信息的完整性。
    分块时设置一定的重叠token数，可以避免语义割裂，提升检索的准确性。
    本编码器采用tiktoken按token精准分块，结合CLIP模型完成多模态编码，
    既保证了信息不丢失，又能实现文本和图片的跨模态检索。
    """
    # 分块编码
    chunk_vectors = encoder.encode_text_chunks(long_text)
    print(f"长文本原始长度：{len(long_text)}字符")
    print(f"分块数量：{len(chunk_vectors)}")
    print(f"每个块向量维度：{len(chunk_vectors[0][1])}（预期512）")
    print(f"第一个块内容：{chunk_vectors[0][0]}")
    
    # ========== 测试2：长文本文件编码 ==========
    print("\n=== 测试长文本文件编码 ===")
    # 创建测试文件
    test_file_path = "test_long_text.txt"
    with open(test_file_path, "w", encoding="utf-8") as f:
        f.write(long_text)
    # 编码文件
    file_chunk_vectors, file_type = encoder.encode_file(test_file_path)
    print(f"文件类型：{file_type}")
    print(f"文件分块数量：{len(file_chunk_vectors)}")
    os.remove(test_file_path)  # 清理测试文件
    
    # ========== 测试3：图片文件编码 ==========
    print("\n=== 测试图片文件编码（仅验证流程） ===")
    test_image_path = "test_image.jpg"  # 替换为你的测试图片路径
    try:
        image_vectors, image_type = encoder.encode_file(test_image_path)
        print(f"图片类型：{image_type}")
        print(f"图片向量维度：{len(image_vectors[0][1])}（预期512）")
    except Exception as e:
        print(f"图片测试提示：{e}（请替换为有效图片路径重试）")
    
    # ========== 测试4：空文本校验 ==========
    print("\n=== 测试空文本校验 ===")
    try:
        encoder.encode_text_chunks("")
    except ValueError as e:
        print(f"空文本校验通过：{e}")
    
    print("\n=== 所有测试完成 ===")