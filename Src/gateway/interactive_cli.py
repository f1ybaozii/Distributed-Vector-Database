"""分布式向量数据库交互式CLI（规范版）"""
import cmd
import json
import requests
import subprocess
import os
import psutil
from config import GATEWAY_CONFIG, DATA_NODE_CONFIG

class VectorDBInteractiveCLI(cmd.Cmd):
    intro = "分布式向量数据库交互式管理工具\n输入 'help' 查看所有可用命令，输入 'quit' 退出工具\n"
    prompt = "vectordb > "

    # ====== 服务管理命令（规范表述+完整错误捕获） ======
    def do_start_zk(self, arg):
        """启动Zookeeper服务
        格式：start_zk [zk安装路径]
        示例：start_zk ~/zookeeper
        """
        zk_path = arg.strip() or "~/zookeeper"
        zk_exec_path = os.path.expanduser(f"{zk_path}/zookeeper/bin/zkServer.sh")
        
        # 检查执行文件是否存在
        if not os.path.exists(zk_exec_path):
            print("[错误] Zookeeper执行文件不存在，请检查安装路径：{}".format(zk_exec_path))
            return
        
        # 执行启动命令并捕获完整输出
        cmd_str = f"sudo bash {zk_exec_path} start"
        result = subprocess.run(
            cmd_str, 
            shell=True, 
            capture_output=True, 
            text=True,
            encoding="utf-8"
        )
        
        # 规范输出结果
        print("[信息] Zookeeper启动命令执行结果：")
        if result.stdout:
            print(f"标准输出：\n{result.stdout.strip()}")
        if result.stderr:
            print(f"错误输出：\n{result.stderr.strip()}")
        print(f"执行返回码：{result.returncode}")
        if result.returncode == 0:
            print("[成功] Zookeeper服务启动请求已提交")
        else:
            print("[错误] Zookeeper服务启动失败，返回码：{}".format(result.returncode))

    def do_start_datanode(self, arg):
        """启动数据节点服务（后台运行）
        格式：start_datanode <节点ID> <ZK地址> <监听端口>
        示例：start_datanode node1@0.0.0.0:9090 127.0.0.1:2181 9090
        """
        args = arg.strip().split()
        node_id = args[0] if len(args) > 0 else "node1@0.0.0.0:9090"
        zk_address = args[1] if len(args) > 1 else "127.0.0.1:2181"
        port = args[2] if len(args) > 2 else "9090"
        
        # 定义日志文件路径
        log_file = os.path.abspath(f"datanode_{port}.log")
        # 构造后台启动命令
        cmd_str = (
            f"nohup python main.py start-datanode "
            f"--node-id {node_id} "
            f"--zk-address {zk_address} "
            f"--port {port} "
            f"> {log_file} 2>&1 &"
        )
        
        # 执行命令并捕获错误（nohup后台启动需先验证命令合法性）
        # 步骤1：先检查命令语法（dry run）
        check_cmd = cmd_str.replace("nohup ", "").replace("> {0} 2>&1 &".format(log_file), "")
        check_result = subprocess.run(
            f"which {check_cmd.split()[0]} && {check_cmd} --help",
            shell=True,
            capture_output=True,
            text=True
        )
        
        if check_result.returncode != 0:
            print("[错误] 数据节点启动命令合法性检查失败：")
            print(f"错误输出：{check_result.stderr.strip()}")
            return
        
        # 步骤2：执行后台启动命令
        run_result = subprocess.run(
            cmd_str,
            shell=True,
            capture_output=True,
            text=True
        )
        
        # 规范输出
        print("[信息] 数据节点后台启动命令执行结果：")
        if run_result.stdout:
            print(f"标准输出：{run_result.stdout.strip()}")
        if run_result.stderr:
            print(f"错误输出：{run_result.stderr.strip()}")
        print(f"执行返回码：{run_result.returncode}")
        
        if run_result.returncode == 0:
            print("[成功] 数据节点已提交后台运行")
            print(f"[信息] 运行日志文件：{log_file}")
            print(f"[提示] 可通过 'status' 命令查看节点运行状态")
        else:
            print("[错误] 数据节点后台启动请求提交失败，返回码：{}".format(run_result.returncode))

    def do_start_gateway(self, arg):
        """启动网关服务（后台运行）
        格式：start_gateway <ZK地址> <模型目录> <监听端口>
        示例：start_gateway 127.0.0.1:2181 ../Model/clip/openai/clip-vit-base-patch32/ 8080
        """
        args = arg.strip().split()
        zk_address = args[0] if len(args) > 0 else "127.0.0.1:2181"
        model_dir = args[1] if len(args) > 1 else "../Model/clip/openai/clip-vit-base-patch32/"
        port = args[2] if len(args) > 2 else "8080"
        
        # 路径规范化
        model_dir = os.path.abspath(model_dir)
        log_file = os.path.abspath(f"gateway_{port}.log")
        
        # 构造启动命令
        cmd_str = (
            f"nohup python main.py start-gateway "
            f"--zk-address {zk_address} "
            f"--model-cache-dir {model_dir} "
            f"--host 0.0.0.0 "
            f"--port {port} "
            f"> {log_file} 2>&1 &"
        )
        
        # 步骤1：命令合法性检查
        check_cmd = cmd_str.replace("nohup ", "").replace("> {0} 2>&1 &".format(log_file), "")
        check_result = subprocess.run(
            f"which {check_cmd.split()[0]} && {check_cmd} --help",
            shell=True,
            capture_output=True,
            text=True
        )
        
        if check_result.returncode != 0:
            print("[错误] 网关启动命令合法性检查失败：")
            print(f"错误输出：{check_result.stderr.strip()}")
            return
        
        # 步骤2：执行后台启动
        run_result = subprocess.run(
            cmd_str,
            shell=True,
            capture_output=True,
            text=True
        )
        
        # 规范输出
        print("[信息] 网关后台启动命令执行结果：")
        if run_result.stdout:
            print(f"标准输出：{run_result.stdout.strip()}")
        if run_result.stderr:
            print(f"错误输出：{run_result.stderr.strip()}")
        print(f"执行返回码：{run_result.returncode}")
        
        if run_result.returncode == 0:
            print("[成功] 网关服务已提交后台运行")
            print(f"[信息] 运行日志文件：{log_file}")
            print(f"[信息] 模型加载路径：{model_dir}")
            print(f"[提示] 可通过 'status' 命令查看网关运行状态")
        else:
            print("[错误] 网关服务启动请求提交失败，返回码：{}".format(run_result.returncode))

    def do_status(self, arg):
        """查看系统整体运行状态
        格式：status
        """
        print("=" * 60)
        print("系统运行状态检查结果")
        print("=" * 60)
        
        # 1. Zookeeper状态检查
        print("\n[1] Zookeeper服务状态")
        print("-" * 40)
        zk_path = os.path.expanduser("~/zookeeper/zookeeper/bin/zkServer.sh")
        if os.path.exists(zk_path):
            result = subprocess.run(
                f"bash {zk_path} status",
                shell=True,
                capture_output=True,
                text=True
            )
            if result.stdout:
                print(f"状态信息：{result.stdout.strip()}")
            if result.stderr:
                print(f"错误信息：{result.stderr.strip()}")
            print(f"检查返回码：{result.returncode}")
        else:
            print("[警告] Zookeeper执行文件未找到，跳过状态检查")
        
        # 2. 数据节点状态检查
        print("\n[2] 数据节点服务状态")
        print("-" * 40)
        datanode_procs = []
        for proc in psutil.process_iter(["pid", "cmdline", "status", "create_time"]):
            try:
                if proc.info["cmdline"] and "start-datanode" in " ".join(proc.info["cmdline"]):
                    datanode_procs.append(proc.info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        if datanode_procs:
            print(f"运行中的数据节点数量：{len(datanode_procs)}")
            for idx, proc in enumerate(datanode_procs, 1):
                print(f"\n节点{idx}：")
                print(f"  PID：{proc['pid']}")
                print(f"  启动命令：{' '.join(proc['cmdline'])}")
                print(f"  运行状态：{proc['status']}")
                print(f"  创建时间：{psutil.Process(proc['pid']).create_time()}")
        else:
            print("[信息] 未检测到运行中的数据节点进程")
        
        # 3. 网关服务状态检查
        print("\n[3] 网关服务状态")
        print("-" * 40)
        gateway_procs = []
        for proc in psutil.process_iter(["pid", "cmdline", "status", "create_time"]):
            try:
                if proc.info["cmdline"] and "start-gateway" in " ".join(proc.info["cmdline"]):
                    gateway_procs.append(proc.info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        if gateway_procs:
            print(f"运行中的网关数量：{len(gateway_procs)}")
            for idx, proc in enumerate(gateway_procs, 1):
                print(f"\n网关{idx}：")
                print(f"  PID：{proc['pid']}")
                print(f"  启动命令：{' '.join(proc['cmdline'])}")
                print(f"  运行状态：{proc['status']}")
                print(f"  创建时间：{psutil.Process(proc['pid']).create_time()}")
        else:
            print("[信息] 未检测到运行中的网关进程")
        
        # 4. 端口监听状态检查
        print("\n[4] 端口监听状态")
        print("-" * 40)
        ports = [2181, 9090, 8080]
        for port in ports:
            try:
                output = subprocess.check_output(
                    f"netstat -tulpn | grep :{port}",
                    shell=True,
                    text=True,
                    stderr=subprocess.STDOUT
                )
                print(f"端口 {port}：[已监听]")
                print(f"  监听详情：{output.strip()}")
            except subprocess.CalledProcessError as e:
                print(f"端口 {port}：[未监听]")
                print(f"  检查结果：{e.output.strip()}")
        
        print("\n" + "=" * 60)

    def do_stop_all(self, arg):
        """停止所有运行中的服务（ZK/数据节点/网关）
        格式：stop_all
        """
        print("[信息] 开始终止所有服务进程...")
        
        # 1. 终止数据节点进程
        print("\n[1] 终止数据节点进程")
        print("-" * 40)
        datanode_count = 0
        for proc in psutil.process_iter(["pid", "cmdline"]):
            try:
                if proc.info["cmdline"] and "start-datanode" in " ".join(proc.info["cmdline"]):
                    proc.kill()
                    proc.wait()
                    print(f"[成功] 已终止数据节点进程（PID：{proc.info['pid']}）")
                    datanode_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                print(f"[错误] 终止数据节点进程失败：{str(e)}")
        if datanode_count == 0:
            print("[信息] 未检测到运行中的数据节点进程")
        
        # 2. 终止网关进程
        print("\n[2] 终止网关进程")
        print("-" * 40)
        gateway_count = 0
        for proc in psutil.process_iter(["pid", "cmdline"]):
            try:
                if proc.info["cmdline"] and "start-gateway" in " ".join(proc.info["cmdline"]):
                    proc.kill()
                    proc.wait()
                    print(f"[成功] 已终止网关进程（PID：{proc.info['pid']}）")
                    gateway_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                print(f"[错误] 终止网关进程失败：{str(e)}")
        if gateway_count == 0:
            print("[信息] 未检测到运行中的网关进程")
        
        # 3. 停止Zookeeper服务
        print("\n[3] 停止Zookeeper服务")
        print("-" * 40)
        zk_path = os.path.expanduser("~/zookeeper/zookeeper/bin/zkServer.sh")
        if os.path.exists(zk_path):
            result = subprocess.run(
                f"sudo bash {zk_path} stop",
                shell=True,
                capture_output=True,
                text=True
            )
            print(f"执行返回码：{result.returncode}")
            if result.stdout:
                print(f"标准输出：{result.stdout.strip()}")
            if result.stderr:
                print(f"错误输出：{result.stderr.strip()}")
            if result.returncode == 0:
                print("[成功] Zookeeper服务已停止")
            else:
                print("[错误] Zookeeper服务停止失败")
        else:
            print("[警告] Zookeeper执行文件未找到，跳过停止操作")
        
        print("\n[信息] 所有服务终止操作执行完成")

    # ====== 数据操作命令（规范表述+完整错误捕获） ======
    def do_add_file(self, arg):
        """上传文件并转换为向量存储
        格式：add_file <文件路径> <根标识> [属性JSON] [网关地址]
        示例：add_file ./test.txt test_001 '{"type":"text"}' http://127.0.0.1:8080
        """
        args = arg.strip().split(maxsplit=3)
        file_path = args[0] if len(args) > 0 else input("请输入文件路径：")
        root_key = args[1] if len(args) > 1 else input("请输入数据根标识：")
        attrs = args[2] if len(args) > 2 else "{}"
        gateway_url = args[3] if len(args) > 3 else f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}"

        # 路径校验
        file_path = os.path.abspath(file_path)
        if not os.path.exists(file_path):
            print(f"[错误] 文件不存在：{file_path}")
            return
        if not os.path.isfile(file_path):
            print(f"[错误] 路径不是有效文件：{file_path}")
            return

        # 执行上传
        try:
            with open(file_path, "rb") as f:
                files = {"file": f}
                data = {
                    "key": root_key,
                    "attrs": attrs,
                    "replica_num": DATA_NODE_CONFIG["replica_num"]
                }
                response = requests.post(
                    f"{gateway_url}/add_file",
                    files=files,
                    data=data,
                    timeout=30
                )
            # 规范输出
            print("[信息] 文件上传请求响应结果：")
            print(f"响应状态码：{response.status_code}")
            print(f"响应内容：\n{json.dumps(response.json(), ensure_ascii=False, indent=2)}")
            if response.status_code == 200 and response.json().get("code") == 0:
                print("[成功] 文件上传并完成向量存储")
            else:
                print("[错误] 文件上传失败，响应码：{}".format(response.status_code))
        except requests.exceptions.RequestException as e:
            print(f"[错误] 文件上传请求异常：{str(e)}")
        except Exception as e:
            print(f"[错误] 文件上传处理异常：{str(e)}")

    def do_query(self, arg):
        """基于文本检索相关向量数据
        格式：query <检索文本> [返回数量] [网关地址]
        示例：query "向量数据库使用方法" 3 http://127.0.0.1:8080
        """
        args = arg.strip().split(maxsplit=2)
        query_text = args[0] if len(args) > 0 else input("请输入检索文本：")
        top_k = args[1] if len(args) > 1 else "3"
        gateway_url = args[2] if len(args) > 2 else f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}"

        # 验证参数
        try:
            top_k = int(top_k)
            if top_k <= 0:
                raise ValueError("返回数量必须为正整数")
        except ValueError as e:
            print(f"[错误] 参数校验失败：{str(e)}")
            return

        # 创建临时查询文件
        tmp_file = "tmp_query.txt"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                f.write(query_text)
            
            # 执行检索请求
            data = {
                "file_path": tmp_file,
                "filter": {},
                "top_k": top_k,
                "aggregate": True
            }
            response = requests.post(
                f"{gateway_url}/query",
                json=data,
                timeout=30
            )
            
            # 规范输出
            print("[信息] 向量检索请求响应结果：")
            print(f"响应状态码：{response.status_code}")
            print(f"响应内容：\n{json.dumps(response.json(), ensure_ascii=False, indent=2)}")
            if response.status_code == 200:
                print("[成功] 检索请求执行完成")
            else:
                print("[错误] 检索请求失败，响应码：{}".format(response.status_code))
        except requests.exceptions.RequestException as e:
            print(f"[错误] 检索请求异常：{str(e)}")
        except Exception as e:
            print(f"[错误] 检索处理异常：{str(e)}")
        finally:
            # 清理临时文件
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
                print(f"[信息] 临时文件已清理：{tmp_file}")

    def do_rag_answer(self, arg):
        """基于检索增强生成（RAG）回答问题
        格式：rag_answer <问题文本> <LLM API密钥> [返回数量] [网关地址]
        示例：rag_answer "向量数据库原理" "sk-xxx" 3 http://127.0.0.1:8080
        """
        args = arg.strip().split(maxsplit=3)
        query = args[0] if len(args) > 0 else input("请输入问题文本：")
        llm_api_key = args[1] if len(args) > 1 else input("请输入LLM API密钥：")
        top_k = args[2] if len(args) > 2 else "3"
        gateway_url = args[3] if len(args) > 3 else f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}"

        # 参数验证
        try:
            top_k = int(top_k)
            if top_k <= 0:
                raise ValueError("返回数量必须为正整数")
            if not llm_api_key:
                raise ValueError("LLM API密钥不能为空")
        except ValueError as e:
            print(f"[错误] 参数校验失败：{str(e)}")
            return

        # 执行RAG问答
        try:
            from rag import rag_answer
            result = rag_answer(
                query=query,
                gateway_url=gateway_url,
                llm_api_key=llm_api_key,
                top_k=top_k
            )
            # 规范输出
            print("[信息] RAG问答处理结果：")
            print(f"{json.dumps(result, ensure_ascii=False, indent=2)}")
            if result.get("code") == 0:
                print("[成功] RAG问答生成完成")
            else:
                print("[错误] RAG问答生成失败，错误码：{}".format(result.get("code")))
        except ImportError:
            print("[错误] RAG模块导入失败，请检查rag目录是否存在")
        except Exception as e:
            print(f"[错误] RAG问答处理异常：{str(e)}")

    def do_delete(self, arg):
        """删除指定标识的向量数据
        格式：delete <根标识> [网关地址]
        示例：delete test_001 http://127.0.0.1:8080
        """
        args = arg.strip().split(maxsplit=1)
        root_key = args[0] if len(args) > 0 else input("请输入数据根标识：")
        gateway_url = args[1] if len(args) > 1 else f"http://{GATEWAY_CONFIG['host']}:{GATEWAY_CONFIG['port']}"

        # 执行删除请求
        try:
            data = {"key": root_key}
            response = requests.post(
                f"{gateway_url}/delete",
                json=data,
                timeout=30
            )
            # 规范输出
            print("[信息] 数据删除请求响应结果：")
            print(f"响应状态码：{response.status_code}")
            print(f"响应内容：\n{json.dumps(response.json(), ensure_ascii=False, indent=2)}")
            if response.status_code == 200 and response.json().get("code") == 0:
                print("[成功] 向量数据删除完成")
            else:
                print("[错误] 数据删除失败，响应码：{}".format(response.status_code))
        except requests.exceptions.RequestException as e:
            print(f"[错误] 数据删除请求异常：{str(e)}")
        except Exception as e:
            print(f"[错误] 数据删除处理异常：{str(e)}")

    # ====== 基础命令 ======
    def do_quit(self, arg):
        """退出交互式管理工具
        格式：quit
        """
        print("[信息] 感谢使用分布式向量数据库交互式管理工具")
        print("[信息] 工具已退出")
        return True

    def do_exit(self, arg):
        """退出交互式管理工具（同quit）
        格式：exit
        """
        return self.do_quit(arg)

    def do_help(self, arg):
        """查看命令帮助信息
        格式：help [命令名]
        示例：help start_gateway
        """
        if arg.strip():
            # 查看指定命令帮助
            if hasattr(self, f"do_{arg.strip()}"):
                func = getattr(self, f"do_{arg.strip()}")
                print(f"\n{func.__doc__}")
            else:
                print(f"[错误] 未知命令：{arg.strip()}")
        else:
            # 查看所有命令帮助
            print("\n可用命令列表（输入 'help 命令名' 查看详细用法）：")
            print("-" * 50)
            for attr in dir(self):
                if attr.startswith("do_") and attr not in ["do_help", "do_quit", "do_exit"]:
                    cmd_name = attr[3:]
                    func = getattr(self, attr)
                    # 提取简短描述
                    short_desc = func.__doc__.split("\n")[0].strip()
                    print(f"{cmd_name:<15} - {short_desc}")
            print("-" * 50)
            print("quit/exit    - 退出工具")
            print("help         - 查看命令帮助")

def start_interactive_cli():
    """启动交互式管理工具"""
    cli = VectorDBInteractiveCLI()
    cli.cmdloop()

if __name__ == "__main__":
    start_interactive_cli()