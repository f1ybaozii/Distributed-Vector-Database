import uuid
import click
from loguru import logger
from colorama import init, Fore, Style
from Config import COORDINATOR_DEFAULT_PORT
from clip.embedding import get_clip_embedding
# 仅保留Thrift向量入库，移除原始存储
from Externel_tools.vector_db_rpc import CoordinatorService
from Externel_tools.vector_db_rpc.ttypes import VectorData
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

init(autoreset=True)

@click.group()
def cli():
    """CLIP嵌入+向量库入库（无原始存储）"""
    pass

@cli.command()
@click.option("--text", required=True, help="嵌入文本")
@click.option("--metadata", help="元数据（key=value,key2=value2）")
@click.option("--coord-addr", default=f"127.0.0.1:{COORDINATOR_DEFAULT_PORT}", help="协调节点地址")
def ingest_text(text, metadata, coord_addr):
    """文本嵌入→向量库"""
    # 解析元数据
    meta_dict = {}
    if metadata:
        try:
            for item in metadata.split(","):
                k, v = item.split("=")
                meta_dict[k.strip()] = v.strip()
        except:
            click.echo(Fore.RED + "❌ 元数据格式错误")
            return

    # 生成向量
    clip = get_clip_embedding()
    try:
        vec = clip.embed_text(text)
    except Exception as e:
        click.echo(Fore.RED + f"❌ 嵌入失败：{e}")
        return

    # 入库到向量库
    data_id = f"clip_text_{uuid.uuid4().hex[:8]}"
    vector_data = VectorData(
        key=data_id,
        vector=vec,
        metadata=meta_dict,
        timestamp=int(uuid.uuid1().time)
    )

    # 调用协调节点
    host, port = coord_addr.split(":")
    transport = TSocket.TSocket(host, int(port))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = CoordinatorService.Client(protocol)

    try:
        transport.open()
        resp = client.put(vector_data)
        transport.close()
        if resp.success:
            click.echo(Fore.GREEN + f"✅ 文本入库成功！ID={data_id}")
        else:
            click.echo(Fore.RED + f"❌ 入库失败：{resp.message}")
    except Exception as e:
        transport.close()
        click.echo(Fore.RED + f"❌ 网络错误：{e}")

@cli.command()
@click.option("--image-path", required=True, help="图像路径")
@click.option("--metadata", help="元数据（key=value,key2=value2）")
@click.option("--coord-addr", default=f"127.0.0.1:{COORDINATOR_DEFAULT_PORT}", help="协调节点地址")
def ingest_image(image_path, metadata, coord_addr):
    """图像嵌入→向量库"""
    # 解析元数据
    meta_dict = {}
    if metadata:
        try:
            for item in metadata.split(","):
                k, v = item.split("=")
                meta_dict[k.strip()] = v.strip()
        except:
            click.echo(Fore.RED + "❌ 元数据格式错误")
            return

    # 生成向量
    clip = get_clip_embedding()
    try:
        vec = clip.embed_image(image_path)
    except Exception as e:
        click.echo(Fore.RED + f"❌ 嵌入失败：{e}")
        return

    # 入库到向量库
    data_id = f"clip_img_{uuid.uuid4().hex[:8]}"
    vector_data = VectorData(
        key=data_id,
        vector=vec,
        metadata=meta_dict,
        timestamp=int(uuid.uuid1().time)
    )

    # 调用协调节点
    host, port = coord_addr.split(":")
    transport = TSocket.TSocket(host, int(port))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = CoordinatorService.Client(protocol)

    try:
        transport.open()
        resp = client.put(vector_data)
        transport.close()
        if resp.success:
            click.echo(Fore.GREEN + f"✅ 图像入库成功！ID={data_id}")
        else:
            click.echo(Fore.RED + f"❌ 入库失败：{resp.message}")
    except Exception as e:
        transport.close()
        click.echo(Fore.RED + f"❌ 网络错误：{e}")

@cli.command()
@click.option("--text", required=True, help="文本")
def get_text_vec(text):
    """仅生成文本向量（不入库）"""
    clip = get_clip_embedding()
    try:
        vec = clip.embed_text(text)
        click.echo(",".join(map(str, vec)))
    except Exception as e:
        click.echo(Fore.RED + f"❌ 生成失败：{e}")

@cli.command()
@click.option("--image-path", required=True, help="图像路径")
def get_image_vec(image_path):
    """仅生成图像向量（不入库）"""
    clip = get_clip_embedding()
    try:
        vec = clip.embed_image(image_path)
        click.echo(",".join(map(str, vec)))
    except Exception as e:
        click.echo(Fore.RED + f"❌ 生成失败：{e}")

if __name__ == "__main__":
    cli()