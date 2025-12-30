import sys
from loguru import logger
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from Config import COORDINATOR_DEFAULT_PORT
# 关键修正：导入Thrift生成的Service和Iface
from src.vector_db import CoordinatorService
from src.vector_db.CoordinatorService import Iface

# 导入自定义Handler（需继承Iface）
from src.coordinator.handler import CoordinatorHandler

def start_coordinator(port: int = COORDINATOR_DEFAULT_PORT):
    """启动协调节点服务"""
    # 初始化处理器
    handler = CoordinatorHandler()
    processor = CoordinatorService.Processor(handler)

    # 初始化Thrift服务器
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadPoolServer(
        processor, transport, tfactory, pfactory,
        worker_count=10
    )

    logger.info(f"协调节点启动成功，监听端口：{port}")
    logger.info("按Ctrl+C退出")
    try:
        server.serve()
    except KeyboardInterrupt:
        logger.info("协调节点停止中...")
        server.stop()

if __name__ == "__main__":
    # 命令行启动：python server.py [port]
    port = int(sys.argv[1]) if len(sys.argv)>=2 else COORDINATOR_DEFAULT_PORT
    start_coordinator(port)