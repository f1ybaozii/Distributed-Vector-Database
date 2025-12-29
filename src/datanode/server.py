import sys
from loguru import logger
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from Config import DATANODE_DEFAULT_PORT_START
from src.vector_db import VectorNodeService  # 必须导入生成的Service类
from src.vector_db.VectorNodeService import Iface  # 导入接口
from src.datanode.handler import VectorNodeHandler  # 导入更新后的handler

def start_datanode(node_id: str, port: int = None):
    """启动数据节点服务（适配新的本地存储逻辑）"""
    if not port:
        port = DATANODE_DEFAULT_PORT_START + int(node_id.split("_")[-1]) - 1

    handler = VectorNodeHandler(node_id=node_id)


    # 初始化Thrift服务器（原有逻辑不变）
    processor = VectorNodeService.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadPoolServer(
        processor, transport, tfactory, pfactory,
        worker_count=5  # 线程池大小
    )

    logger.info(f"数据节点{node_id}启动成功，监听端口：{port}")
    logger.info("按Ctrl+C退出")
    try:
        server.serve()
    except KeyboardInterrupt:
        logger.info("数据节点停止中...")
        server.stop()
    except Exception as e:
        logger.error(f"数据节点启动失败：{e}")
        sys.exit(1)

if __name__ == "__main__":
    # 命令行启动：python server.py node_1 9090
    if len(sys.argv) < 2:
        logger.error("用法：python server.py <node_id> [port]")
        sys.exit(1)
    node_id = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) >=3 else None
    start_datanode(node_id, port)