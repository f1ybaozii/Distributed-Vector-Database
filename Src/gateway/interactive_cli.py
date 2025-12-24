"""可持续交互的CLI（替代原有cli.py）"""
import cmd
import argparse
import json
import requests
import subprocess
import os
import psutil
from config import GATEWAY_CONFIG, DATA_NODE_CONFIG

class VectorDBInteractiveCLI(cmd.Cmd):
    intro = "分布式向量数据库交互式CLI（输入 help 查看命令）\n"
    prompt = "vectordb> "

    # ====== 服务管理命令 ======
    def do_start_zk(self, arg):
        """启动ZK（格式：start_zk <zk_path>）"""
        zk_path = arg or "~/zookeeper"
        cmd = f"sudo bash ~/zookeeper/zookeeper/bin/zkServer.sh start"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(f"ZK启动结果：\n{result.stdout}\n{result.stderr}")

    def do_start_datanode(self, arg):
        """后台启动数据节点（格式：start_datanode <node_id> <zk_address> <port>）"""
        args = arg.split()
        node_id = args[0] if len(args)>0 else "node1@0.0.0.0:9090"
        zk_address = args[1] if len(args)>1 else "127.0.0.1:2181"
        port = args[2] if len(args)>2 else "9090"
        
        # 后台启动，输出日志到datanode_<port>.log
        log_file = f"datanode_{port}.log"
        cmd = f"nohup python main.py start-datanode --node-id {node_id} --zk-address {zk_address} --port {port} > {log_file} 2>&1 &"
        subprocess.run(cmd, shell=True)
        print(f"数据节点已后台启动，日志文件：{log_file}")

    def do_start_gateway(self, arg):
        """后台启动网关（格式：start_gateway <zk_address> <model_dir> <port>）"""
        args = arg.split()
        zk_address = args[0] if len(args)>0 else "127.0.0.1:2181"
        model_dir = args[1] if len(args)>1 else "./models/clip/openai/clip-vit-base-patch32/"
        port = args[2] if len(args)>2 else "8080"
        
        log_file = f"gateway_{port}.log"
        cmd = f"nohup python main.py start-gateway --zk-address {zk_address} --model-cache-dir {model_dir} --host 0.0.0.0 --port {port} > {log_file} 2>&1 &"
        subprocess.run(cmd, shell=True)
        print(f"网关已后台启动，日志文件：{log_file}")

    def do_status(self, arg):
        """查看所有服务状态（ZK/数据节点/网关）"""
        # 1. ZK状态
        try:
            zk_status = subprocess.run("~/zookeeper/zookeeper/bin/zkServer.sh status", shell=True, capture_output=True, text=True)
            print("=== ZK状态 ===")
            print(zk_status.stdout or zk_status.stderr)
        except:
            print("=== ZK状态 ===")
            print("ZK未安装/未启动")

        # 2. 数据节点状态
        print("\n=== 数据节点状态 ===")
        datanode_procs = [p for p in psutil.process_iter() if "data_node" in p.cmdline() and "python" in p.cmdline()]
        if datanode_procs:
            for p in datanode_procs:
                cmd = " ".join(p.cmdline())
                print(f"PID: {p.pid} | 命令: {cmd} | 状态: {p.status()}")
        else:
            print("无运行中的数据节点")

        # 3. 网关状态
        print("\n=== 网关状态 ===")
        gateway_procs = [p for p in psutil.process_iter() if "gateway" in p.cmdline() and "python" in p.cmdline()]
        if gateway_procs:
            for p in gateway_procs:
                cmd = " ".join(p.cmdline())
                print(f"PID: {p.pid} | 命令: {cmd} | 状态: {p.status()}")
        else:
            print("无运行中的网关")

        # 4. 端口监听状态
        print("\n=== 端口监听状态 ===")
        ports = [2181, 9090, 8080]
        for port in ports:
            try:
                output = subprocess.check_output(f"netstat -tulpn | grep {port}", shell=True, text=True)
                print(f"端口 {port}: 已监听\n{output.strip()}")
            except:
                print(f"端口 {port}: 未监听")

    def do_stop_all(self, arg):
        """停止所有服务（ZK/数据节点/网关）"""
        # 停止数据节点
        datanode_procs = [p for p in psutil.process_iter() if "data_node" in p.cmdline() and "python" in p.cmdline()]
        for p in datanode_procs:
            p.kill()
            print(f"已停止数据节点 PID: {p.pid}")

        # 停止网关
        gateway_procs = [p for p in psutil.process_iter() if "gateway" in p.cmdline() and "python" in p.cmdline()]
        for p in gateway_procs:
            p.kill()
            print(f"已停止网关 PID: {p.pid}")

        # 停止ZK
        try:
            subprocess.run("~/zookeeper/bin/zkServer.sh stop", shell=True, capture_output=True, text=True)
            print("已停止ZK")
        except:
            print("ZK停止失败/未启动")

    # ====== 数据操作命令 ======
    def do_add_file(self, arg):
        """上传文件（格式：add_file <file_path> <root_key> [attrs] [gateway_url]）"""
        args = arg.split(maxsplit=3)
        file_path = args[0] if len(args)>0 else input("请输入文件路径: ")
        root_key = args[1] if len(args)>1 else input("请输入根Key: ")
        attrs = args[2] if len(args)>2 else "{}"
        gateway_url = args[3] if len(args)>3 else f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}"

        if not os.path.exists(file_path):
            print(f"错误：文件 {file_path} 不存在")
            return

        files = {"file": open(file_path, "rb")}
        data = {"key": root_key, "attrs": attrs, "replica_num": DATA_NODE_CONFIG["replica_num"]}
        try:
            resp = requests.post(f"{gateway_url}/add_file", files=files, data=data)
            print(f"上传结果：\n{json.dumps(resp.json(), ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"上传失败：{e}")

    def do_query(self, arg):
        """检索数据（格式：query <query_text> [top_k] [gateway_url]）"""
        args = arg.split(maxsplit=2)
        query_text = args[0] if len(args)>0 else input("请输入查询文本: ")
        top_k = args[1] if len(args)>1 else "3"
        gateway_url = args[2] if len(args)>2 else f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}"

        # 创建临时查询文件
        query_file = "tmp_query.txt"
        with open(query_file, "w", encoding="utf-8") as f:
            f.write(query_text)

        data = {
            "file_path": query_file,
            "filter": {},
            "top_k": int(top_k),
            "aggregate": True
        }
        try:
            resp = requests.post(f"{gateway_url}/query", json=data)
            print(f"检索结果：\n{json.dumps(resp.json(), ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"检索失败：{e}")
        finally:
            if os.path.exists(query_file):
                os.remove(query_file)

    def do_rag_answer(self, arg):
        """RAG问答（格式：rag_answer <query> <llm_api_key> [top_k] [gateway_url]）"""
        args = arg.split(maxsplit=3)
        query = args[0] if len(args)>0 else input("请输入问题: ")
        llm_api_key = args[1] if len(args)>1 else input("请输入LLM API Key: ")
        top_k = args[2] if len(args)>2 else "3"
        gateway_url = args[3] if len(args)>3 else f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}"

        from rag import rag_answer
        try:
            result = rag_answer(
                query=query,
                gateway_url=gateway_url,
                llm_api_key=llm_api_key,
                top_k=int(top_k)
            )
            print(f"RAG问答结果：\n{json.dumps(result, ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"RAG失败：{e}")

    def do_delete(self, arg):
        """删除数据（格式：delete <root_key> [gateway_url]）"""
        args = arg.split(maxsplit=1)
        root_key = args[0] if len(args)>0 else input("请输入根Key: ")
        gateway_url = args[1] if len(args)>1 else f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}"

        data = {"key": root_key}
        try:
            resp = requests.post(f"{gateway_url}/delete", json=data)
            print(f"删除结果：\n{json.dumps(resp.json(), ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"删除失败：{e}")

    # ====== 基础命令 ======
    def do_quit(self, arg):
        """退出CLI"""
        print("退出交互式CLI...")
        return True

    def do_exit(self, arg):
        """退出CLI（同quit）"""
        return self.do_quit(arg)

def start_interactive_cli():
    """启动交互式CLI"""
    cli = VectorDBInteractiveCLI()
    cli.cmdloop()

if __name__ == "__main__":
    start_interactive_cli()