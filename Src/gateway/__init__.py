"""网关模块入口"""
from .core import init_gateway, create_gateway_app, start_gateway
from .interactive_cli import start_interactive_cli
from .cli import main_cli