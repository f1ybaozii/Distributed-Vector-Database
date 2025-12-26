import threading
import time
from loguru import logger
from kazoo.client import KazooClient, KazooState
from Config import (
    ZK_SERVERS, ZK_SESSION_TIMEOUT, ZK_BASE_PATH,
    ZK_NODES_PATH, ZK_SHARDS_PATH, ZK_SINGLETON_KEY
)

# 单例锁
_zk_lock = threading.Lock()
_zk_instance = None

class ZKManager:
    """ZK连接复用管理器（新增节点监听+缓存+健康检查）"""
    def __init__(self):
        self.zk = KazooClient(
            hosts=ZK_SERVERS,
            timeout=ZK_SESSION_TIMEOUT / 1000,  # 转秒
        )
        self.zk.start()
        self._init_zk_paths()

        # 核心新增：节点列表缓存（实时更新）
        self.node_cache = {}
        self.node_cache_lock = threading.Lock()
        
        # 核心新增：监听ZK节点目录变化
        self._watch_nodes()
        
        # 核心新增：定时健康检查（主动检测离线节点）
        self.health_check_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True  # 守护线程，退出时自动结束
        )
        self.health_check_thread.start()
        
        logger.info(f"ZK连接初始化成功：{ZK_SERVERS}，已启动节点监听和健康检查")

    def _init_zk_paths(self):
        """初始化ZK基础节点"""
        for path in [ZK_BASE_PATH, ZK_NODES_PATH, ZK_SHARDS_PATH]:
            if not self.zk.exists(path):
                self.zk.create(path, b"", makepath=True)
                logger.info(f"ZK节点初始化：{path}")

    def _watch_nodes(self):
        """监听ZK节点目录变化（实时更新缓存）"""
        def _node_change_watcher(event):
            """节点目录变化回调（新增/删除节点时触发）"""
            logger.info(f"ZK节点目录变化：{event.type}，重新加载节点列表")
            self._refresh_node_cache()
            # 重新注册监听（ZK watch是一次性的，需重新注册）
            self._watch_nodes()

        # 首次加载缓存+注册监听
        self._refresh_node_cache()
        self.zk.get_children(ZK_NODES_PATH, watch=_node_change_watcher)

    def _refresh_node_cache(self):
        """刷新节点缓存（从ZK读取最新列表）"""
        with self.node_cache_lock:
            self.node_cache.clear()
            for node_id in self.zk.get_children(ZK_NODES_PATH):
                node_path = f"{ZK_NODES_PATH}/{node_id}"
                try:
                    address, _ = self.zk.get(node_path)
                    self.node_cache[node_id] = address.decode()
                except Exception as e:
                    logger.error(f"读取节点{node_id}信息失败：{e}")
        logger.info(f"节点缓存已刷新，当前在线节点：{list(self.node_cache.keys())}")

    def _health_check_loop(self):
        """定时健康检查（主动检测节点是否真的在线）"""
        import socket
        while True:
            time.sleep(5)  # 每5秒检查一次
            offline_nodes = []
            with self.node_cache_lock:
                # 遍历缓存中的节点，检测端口是否可达
                for node_id, addr in self.node_cache.items():
                    host, port = addr.split(":")
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(2)  # 超时2秒
                    try:
                        sock.connect((host, int(port)))
                        sock.close()
                    except:
                        offline_nodes.append(node_id)
                        logger.warning(f"健康检查失败：节点{node_id}({addr})离线")
            
            # 清理离线节点（强制删除ZK临时节点）
            for node_id in offline_nodes:
                self._remove_offline_node(node_id)

    def _remove_offline_node(self, node_id: str):
        """强制删除ZK离线节点（加速清理）"""
        node_path = f"{ZK_NODES_PATH}/{node_id}"
        try:
            if self.zk.exists(node_path):
                self.zk.delete(node_path)
                logger.info(f"已强制删除离线节点{node_id}的ZK临时节点")
            # 同步清理缓存
            with self.node_cache_lock:
                if node_id in self.node_cache:
                    del self.node_cache[node_id]
        except Exception as e:
            logger.error(f"删除离线节点{node_id}失败：{e}")

    # ========== 原有方法改造（使用缓存） ==========
    def register_node(self, node_id: str, address: str):
        """注册数据节点（临时节点）"""
        node_path = f"{ZK_NODES_PATH}/{node_id}"
        if not self.zk.exists(node_path):
            # 临时节点：会话关闭自动删除
            self.zk.create(node_path, address.encode(), ephemeral=True)
            logger.info(f"节点注册成功：{node_id} -> {address}")
            # 主动刷新缓存
            self._refresh_node_cache()
        return True

    def get_all_nodes(self):
        """获取所有在线数据节点（返回缓存，实时更新）"""
        with self.node_cache_lock:
            # 返回缓存副本，避免并发修改
            return self.node_cache.copy()

    def set_shard_mapping(self, shard_id: int, master_node: str, slave_nodes: list):
        """设置分片-节点映射"""
        import json
        shard_path = f"{ZK_SHARDS_PATH}/{shard_id}"
        mapping = json.dumps({"master": master_node, "slaves": slave_nodes})
        if self.zk.exists(shard_path):
            self.zk.set(shard_path, mapping.encode())
        else:
            self.zk.create(shard_path, mapping.encode())
        logger.info(f"分片{shard_id}映射更新：主节点={master_node}，副本={slave_nodes}")

    def get_shard_nodes(self, shard_id: int):
        """获取分片对应的节点"""
        import json
        shard_path = f"{ZK_SHARDS_PATH}/{shard_id}"
        if not self.zk.exists(shard_path):
            return None
        mapping = json.loads(self.zk.get(shard_path)[0].decode())
        # 过滤分片映射中的离线节点
        with self.node_cache_lock:
            if mapping["master"] not in self.node_cache:
                logger.warning(f"分片{shard_id}主节点{mapping['master']}离线，自动切换副本")
                # 切换到第一个在线副本
                for slave in mapping["slaves"]:
                    if slave in self.node_cache:
                        mapping["master"] = slave
                        break
            # 清理离线副本
            mapping["slaves"] = [s for s in mapping["slaves"] if s in self.node_cache]
        return mapping

    def close(self):
        """优雅关闭ZK连接"""
        if self.zk.connected:
            self.zk.stop()
            self.zk.close()
            logger.info("ZK连接已关闭")

def get_zk_manager() -> ZKManager:
    """获取单例ZK管理器（连接复用）"""
    global _zk_instance
    with _zk_lock:
        if _zk_instance is None:
            _zk_instance = ZKManager()
        return _zk_instance

# 程序退出时自动关闭ZK连接
import atexit
def _cleanup_zk():
    global _zk_instance
    if _zk_instance:
        _zk_instance.close()
atexit.register(_cleanup_zk)