"""Thrift辅助工具函数"""
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
import vector_db.VectorDBService as VectorDBService

def get_thrift_client(ip: str, port: int) -> VectorDBService.Client:
    """创建Thrift客户端"""
    transport = TSocket.TSocket(ip, port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = VectorDBService.Client(protocol)
    transport.open()
    return client, transport