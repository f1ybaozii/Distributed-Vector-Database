"""分布式向量数据库统一入口"""
import sys
import os

# 设置项目根目录（Src/）为基础路径
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
# 把gen-py目录加入模块搜索路径
sys.path.append(os.path.join(PROJECT_ROOT, "gen-py"))

# 原有导入语句
"""分布式向量数据库统一入口"""
from gateway.interactive_cli import start_interactive_cli

if __name__ == "__main__":
    start_interactive_cli()