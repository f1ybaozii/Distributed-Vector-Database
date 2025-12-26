from flask import Flask
from encoder import create_encoder
from utils import get_zk_client
from .routes import register_routes
from config import GATEWAY_CONFIG
import threading
import atexit
import signal
import sys
import vector_db
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

# 全局资源
encoder = None
zk = None
online_datanodes = []
app = None
client_pool = None


class DataNodeClientPool:
    """
    进程级 Thrift Client 池：
    - (ip, port) -> (client, transport)
    - 长连接 + 自动复用
    """
    def __init__(self):
        self._clients = {}
        self._lock = threading.Lock()

    def get_client(self, ip, port, timeout=30000):
        key = (ip, int(port))
        with self._lock:
            if key in self._clients:
                client, transport = self._clients[key]
                if transport.isOpen():
                    return client
                self._clients.pop(key, None)

            sock = TSocket.TSocket(ip, int(port))
            sock.setTimeout(timeout)
            transport = TTransport.TBufferedTransport(sock)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = vector_db.VectorDBService.Client(protocol)
            transport.open()

            self._clients[key] = (client, transport)
            return client

    def close(self, ip, port):
        key = (ip, int(port))
        with self._lock:
            pair = self._clients.pop(key, None)
            if pair:
                _, transport = pair
                try:
                    if transport.isOpen():
                        transport.close()
                except Exception:
                    pass

    def close_all(self):
        with self._lock:
            for _, transport in self._clients.values():
                try:
                    if transport.isOpen():
                        transport.close()
                except Exception:
                    pass
            self._clients.clear()


def init_gateway(zk_address=GATEWAY_CONFIG["zk_address"], model_cache_dir=None):
    global encoder, zk, online_datanodes, client_pool

    encoder = create_encoder(model_cache_dir=model_cache_dir)
    zk = get_zk_client(zk_address)
    zk.start()

    zk.ensure_path("/vector_db/nodes")
    online_datanodes = zk.get_children("/vector_db/nodes")

    client_pool = DataNodeClientPool()

    def watch_datanodes(children):
        global online_datanodes
        removed = set(online_datanodes) - set(children)
        for node in removed:
            ip, port = node.split("@")[1].split(":")
            client_pool.close(ip, port)
        online_datanodes = children

    zk.ChildrenWatch("/vector_db/nodes", watch_datanodes)


def create_gateway_app():
    app = Flask(__name__)
    app.config["encoder"] = encoder
    app.config["zk"] = zk
    register_routes(app)
    return app


def cleanup_resources():
    global zk, client_pool
    if client_pool:
        client_pool.close_all()
    if zk and zk.connected:
        zk.stop()
        zk.close()


def start_gateway(host=GATEWAY_CONFIG["host"], port=GATEWAY_CONFIG["port"]):
    global app
    init_gateway()
    app = create_gateway_app()

    atexit.register(cleanup_resources)
    signal.signal(signal.SIGINT, lambda *_: cleanup_resources() or sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: cleanup_resources() or sys.exit(0))

    app.run(
        host=host,
        port=port,
        debug=False,
        use_reloader=False,
        threaded=True
    )


def run_gateway():
    start_gateway(
        host=GATEWAY_CONFIG["host"],
        port=GATEWAY_CONFIG["port"]
    )