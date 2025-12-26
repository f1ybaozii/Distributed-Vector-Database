import click
from loguru import logger
from colorama import init, Fore, Style
from Config import COORDINATOR_DEFAULT_PORT
# Thrift导入
from src.vector_db import CoordinatorService
from src.vector_db.ttypes import SearchRequest
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

# 初始化彩色输出
init(autoreset=True)

@click.group()
@click.option("--coord-addr", default=f"127.0.0.1:{COORDINATOR_DEFAULT_PORT}", help="协调节点地址")
@click.pass_context
def cli(ctx, coord_addr):
    """分布式向量数据库CLI工具"""
    ctx.ensure_object(dict)
    # 初始化协调节点客户端
    host, port = coord_addr.split(":")
    transport = TSocket.TSocket(host, int(port))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    ctx.obj["client"] = CoordinatorService.Client(protocol)
    ctx.obj["transport"] = transport
    ctx.obj["coord_addr"] = coord_addr

# 节点管理命令
@cli.command()
@click.option("--node-id", required=True, help="节点ID")
@click.option("--node-addr", required=True, help="节点地址（host:port）")
@click.pass_context
def register_node(ctx, node_id, node_addr):
    """注册数据节点"""
    client = ctx.obj["client"]
    transport = ctx.obj["transport"]
    try:
        transport.open()
        resp = client.register_node(node_id, node_addr)
        transport.close()
        if resp.success:
            click.echo(Fore.GREEN + f"✅ {resp.message}")
        else:
            click.echo(Fore.RED + f"❌ {resp.message}")
    except Exception as e:
        transport.close()
        click.echo(Fore.RED + f"❌ 注册失败：{str(e)}")

@cli.command()
@click.pass_context
def list_nodes(ctx):
    """列出所有数据节点"""
    client = ctx.obj["client"]
    transport = ctx.obj["transport"]
    try:
        transport.open()
        resp = client.list_nodes()
        transport.close()
        if resp.success:
            click.echo(Fore.BLUE + "\n📌 数据节点列表：")
            from prettytable import PrettyTable
            table = PrettyTable()
            table.field_names = ["节点ID", "地址"]
            for node_id, addr in resp.vector_data.metadata.items():
                table.add_row([node_id, addr])
            click.echo(table)
        else:
            click.echo(Fore.RED + f"❌ {resp.message}")
    except Exception as e:
        transport.close()
        click.echo(Fore.RED + f"❌ 获取节点失败：{str(e)}")

# 向量操作命令
@cli.command()
@click.option("--key", required=True, help="向量Key")
@click.option("--vector", required=True, help="向量（逗号分隔）")
@click.option("--metadata", help="元数据（key=value,key2=value2）")
@click.pass_context
def put(ctx, key, vector, metadata):
    """写入/更新向量"""
    # 解析向量
    try:
        vector_list = [float(x.strip()) for x in vector.split(",")]
    except:
        click.echo(Fore.RED + "❌ 向量格式错误：逗号分隔的数字")
        return
    # 解析元数据
    meta_dict = {}
    if metadata:
        try:
            for item in metadata.split(","):
                k, v = item.split("=")
                meta_dict[k.strip()] = v.strip()
        except:
            click.echo(Fore.RED + "❌ 元数据格式错误：key=value,key2=value2")
            return

    # 构造请求
    from src.vector_db.ttypes import VectorData
    data = VectorData(
        key=key,
        vector=vector_list,
        metadata=meta_dict
    )

    # 发送请求
    client = ctx.obj["client"]
    transport = ctx.obj["transport"]
    try:
        transport.open()
        resp = client.put(data)
        transport.close()
        if resp.success:
            click.echo(Fore.GREEN + f"✅ 写入成功！Key={key}")
        else:
            click.echo(Fore.RED + f"❌ {resp.message}")
    except Exception as e:
        transport.close()
        click.echo(Fore.RED + f"❌ 网络错误：{str(e)}")

@cli.command()
@click.option("--key", required=True, help="向量Key")
@click.pass_context
def delete(ctx, key):
    """删除向量"""
    client = ctx.obj["client"]
    transport = ctx.obj["transport"]
    try:
        transport.open()
        resp = client.delete(key)
        transport.close()
        if resp.success:
            click.echo(Fore.GREEN + f"✅ 删除成功！Key={key}")
        else:
            click.echo(Fore.RED + f"❌ {resp.message}")
    except Exception as e:
        transport.close()
        click.echo(Fore.RED + f"❌ 网络错误：{str(e)}")

@cli.command()
@click.option("--key", required=True, help="向量Key")
@click.pass_context
def get(ctx, key):
    """获取向量"""
    client = ctx.obj["client"]
    transport = ctx.obj["transport"]
    try:
        transport.open()
        resp = client.get(key)
        transport.close()
        if resp.success:
            data = resp.vector_data
            click.echo(Fore.GREEN + f"✅ 获取成功！")
            click.echo(Fore.BLUE + f"📌 Key：{data.key}")
            click.echo(Fore.BLUE + f"📌 向量维度：{len(data.vector)}")
            click.echo(Fore.BLUE + f"📌 元数据：{data.metadata}")
        else:
            click.echo(Fore.RED + f"❌ {resp.message}")
    except Exception as e:
        transport.close()
        click.echo(Fore.RED + f"❌ 网络错误：{str(e)}")

@cli.command()
@click.option("--query-vec", required=True, help="查询向量（逗号分隔）")
@click.option("--top-k", default=5, help="返回Top-K")
@click.option("--filter", help="过滤条件（key=value）")
@click.option("--threshold", default=0.0, help="相似度阈值")
@click.pass_context
def search(ctx, query_vec, top_k, filter, threshold):
    """向量检索"""
    # 解析向量
    try:
        query_list = [float(x.strip()) for x in query_vec.split(",")]
    except:
        click.echo(Fore.RED + "❌ 向量格式错误：逗号分隔的数字")
        return
    # 解析过滤条件
    filter_dict = {}
    if filter:
        try:
            for item in filter.split(","):
                k, v = item.split("=")
                filter_dict[k.strip()] = v.strip()
        except:
            click.echo(Fore.RED + "❌ 过滤条件格式错误：key=value,key2=value2")
            return

    # 构造请求
    req = SearchRequest(
        query_vector=query_list,
        top_k=top_k,
        filter=filter_dict,
        threshold=threshold
    )

    # 发送请求
    client = ctx.obj["client"]
    transport = ctx.obj["transport"]
    try:
        transport.open()
        resp = client.search(req)
        transport.close()
        if resp.success:
            res = resp.search_result
            click.echo(Fore.GREEN + f"✅ 检索成功！共{len(res.keys)}条结果")
            from prettytable import PrettyTable
            table = PrettyTable()
            table.field_names = ["排名", "Key", "相似度分数", "元数据"]
            for i, (k, s, vec) in enumerate(zip(res.keys, res.scores, res.vectors)):
                table.add_row([i+1, k, f"{s:.4f}", vec.metadata])
            click.echo(table)
        else:
            click.echo(Fore.RED + f"❌ {resp.message}")
    except Exception as e:
        transport.close()
        click.echo(Fore.RED + f"❌ 网络错误：{str(e)}")

if __name__ == "__main__":
    cli()