"""
命令组注册表

集中管理所有命令组，便于扩展和维护。
"""

from alphahome.cli.commands.base import CommandGroup
from alphahome.cli.commands.ddb import DDBCommandGroup
from alphahome.cli.commands.mv import MVCommandGroup
from alphahome.cli.commands.prod import ProdCommandGroup
from alphahome.cli.commands.gui import GUICommandGroup

# 所有命令组注册表
COMMAND_GROUPS: list[type[CommandGroup]] = [
    ProdCommandGroup,   # 生产脚本（通常最常用）
    DDBCommandGroup,    # DolphinDB 工具
    MVCommandGroup,     # 物化视图
    GUICommandGroup,    # GUI 应用
]


def get_all_command_groups() -> list[CommandGroup]:
    """获取所有已注册的命令组实例"""
    return [group_class() for group_class in COMMAND_GROUPS]
