import os
import sys
from loguru import logger

# 添加项目根目录到sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# 项目内部导入
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from src.vector_db.CoordinatorService import Client as CoordinatorClient
from src.vector_db.ttypes import VectorData, SearchRequest, Response
# 导入CLIP嵌入工具
from clip.embedding import CLIPEmbedding, SUPPORTED_IMAGE_EXT

COORDINATOR_HOST="192.168.14.149"
COORDINATOR_PORT="8081"

class VectorDBOperation:
    """向量库操作类（存储+检索）"""
    def __init__(self):
        self.coord_addr = f"{COORDINATOR_HOST}:{COORDINATOR_PORT}"
        self.client = None
        self.transport = None
        self.clip = CLIPEmbedding()  # 初始化CLIP嵌入工具
        self._init_db_client()

    def _init_db_client(self):
        """初始化向量库客户端"""
        try:
            host, port = self.coord_addr.split(":")
            self.transport = TSocket.TSocket(host, int(port))
            self.transport = TTransport.TBufferedTransport(self.transport)
            protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            self.client = CoordinatorClient(protocol)
            self.transport.open()
            logger.info(f"向量库客户端连接成功：{self.coord_addr}")
        except Exception as e:
            logger.error(f"向量库连接失败：{e}")
            raise e

    def __del__(self):
        """析构时关闭连接"""
        if self.transport and self.transport.isOpen():
            self.transport.close()
            logger.info("向量库客户端连接已关闭")

    def put_image(self, image_path: str) -> bool:
        """
        单张图片入库：KEY=文件名，metadata=文件路径
        :param image_path: 图片路径
        :return: 成功返回True，失败返回False
        """
        # 1. 生成图片向量
        vec = self.clip.image2vec(image_path)
        if vec is None:
            return False

        # 2. 构造入库数据
        file_name = image_path.split("/")[-1].split(".")[0]
        metadata = {
            "type":"image",
            "dataset":"unsplash-25K",
            "file_path": image_path,
            "dimension": str(len(vec))
        }

        vector_data = VectorData(
            key=file_name,
            vector=vec,
            metadata=metadata
        )
        # 3. 调用PUT接口
   
        resp: Response = self.client.put(vector_data)
        if resp.success:
            logger.info(f"图片[{file_name}]入库成功")
            return True
        else:
            logger.error(f"图片[{file_name}]入库失败：{resp.message}")
            # 捕获容量超限错误，提示扩容
            if "exceeds the specified limit" in resp.message:
                logger.warning("HNSW索引容量不足，请修改datanode/handler.py增大max_elements并重启数据节点")
            return False
        
    def batch_put_images(self, image_dir: str) -> tuple:
        """
        批量图片入库
        :param image_dir: 图片文件夹路径
        :return: (总数, 成功数, 失败数)
        """
        if not os.path.isdir(image_dir):
            logger.error(f"文件夹不存在：{image_dir}")
            return (0, 0, 0)

        # 遍历所有图片
        image_paths = []
        flag=True
        for file in os.listdir(image_dir):
            if flag:
                ext = os.path.splitext(file)[-1].lower()
                if ext in SUPPORTED_IMAGE_EXT:
                    image_paths.append(os.path.join(image_dir, file))
            if not flag and file.endswith('FFo23lzk2YA.jpg'):
                flag=True

        if not image_paths:
            logger.warning(f"文件夹[{image_dir}]下无有效图片")
            return (0, 0, 0)

        # 批量入库
        success_count = 0
        total = len(image_paths)
        for img_path in image_paths:
            if self.put_image(img_path):
                success_count += 1

        fail_count = total - success_count
        logger.info(f"入库完成：总数={total}，成功={success_count}，失败={fail_count}")
        return (total, success_count, fail_count)

    def text_search(self, text: str, top_k: int = 5) -> list:
        """
        文本检索图片
        :param text: 检索文本
        :param top_k: 返回TOP-K数量
        :return: 匹配结果列表，每个元素：{"file_path": 图片路径, "score": 相似度分数}
        """
        # 1. 文本转向量
        vec = self.clip.text2vec(text)
        if vec is None:
            return []

        # 2. 构造检索请求
        search_req = SearchRequest(
            query_vector=vec,
            top_k=top_k
        )

        # 3. 调用SEARCH接口
        resp: Response = self.client.search(search_req)
        if not resp.success:
            logger.error(f"检索失败：{resp.message}")
            return []

        # 4. 解析结果（路径 + 相似度分数）
        result_list = []
        if resp.search_result and resp.search_result.vectors and resp.search_result.scores:
            # 确保路径和分数一一对应（按检索排序）
            vectors = resp.search_result.vectors
            scores = resp.search_result.scores
            for idx, vec_data in enumerate(vectors):
                if "file_path" in vec_data.metadata and idx < len(scores):
                    result_list.append({
                        "file_path": vec_data.metadata["file_path"],
                        "score": float(scores[idx])  # 转float避免Thrift类型问题
                    })

        logger.info(f"文本[{text}]检索完成，返回{len(result_list)}张图片（带分数）")
        return result_list
       

# ========== 测试存储和检索 ==========
if __name__ == "__main__":
    # 初始化日志
    logger.remove()
    logger.add(sys.stdout, format="<green>{time}</green> | <level>{level}</level> | {message}", level="INFO")

    # 1. 初始化操作类
    db_op = VectorDBOperation()

    #2. 测试批量入库（替换为你的图片文件夹）
    # test_image_dir = os.path.join(PROJECT_ROOT, "Data")
    # total, success, fail = db_op.batch_put_images(test_image_dir)
    # logger.info(f"批量入库测试结果：总数={total}，成功={success}，失败={fail}")

    test_text = "a cat sitting on the sofa"
    result = db_op.text_search(test_text, top_k=3)
    logger.info(f"文本[{test_text}]检索结果：{result}")