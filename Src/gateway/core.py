"""网关核心逻辑"""
from flask import Flask
from encoder import create_encoder
from utils import get_zk_client
from .routes import register_routes, encoder, zk
from config import GATEWAY_CONFIG

def init_gateway(zk_address: str = GATEWAY_CONFIG["zk_address"],
                 model_cache_dir: str = None):
    """初始化网关（全局资源）"""
    global encoder, zk
    # 初始化编码器
    encoder = create_encoder(model_cache_dir=model_cache_dir)
    # 初始化ZK客户端
    zk = get_zk_client(zk_address)
    print("网关初始化成功")

def create_gateway_app() -> Flask:
    """创建Flask应用"""
    app = Flask(__name__)
    register_routes(app)
    return app

def start_gateway(host: str = GATEWAY_CONFIG["host"],
                  port: int = GATEWAY_CONFIG["port"]):
    """启动网关"""
    app = create_gateway_app()
    app.run(host=host, port=port)