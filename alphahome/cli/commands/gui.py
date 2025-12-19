"""
GUI 命令组

提供启动 GUI 的命令入口。
"""

import argparse
import sys

from alphahome.cli.core.exitcodes import SUCCESS, FAILURE
from alphahome.cli.core.logging_config import get_cli_logger
from alphahome.cli.commands.base import CommandGroup

logger = get_cli_logger(__name__)


class GUICommandGroup(CommandGroup):
    """GUI 命令组"""
    
    group_name = "gui"
    group_help = "启动图形界面"
    
    def add_subparsers(self, subparsers: argparse._SubParsersAction) -> None:
        """添加 GUI 命令到子解析器容器"""
        gui_parser = subparsers.add_parser(
            self.group_name,
            help=self.group_help,
            description="AlphaHome 图形用户界面"
        )
        
        gui_parser.set_defaults(func=_run_gui)


def _run_gui(args) -> int:
    """运行 GUI 应用"""
    try:
        logger.info("启动 AlphaHome GUI...")
        
        from alphahome.gui.main_window import run_gui
        
        run_gui()
        return SUCCESS
        
    except KeyboardInterrupt:
        logger.info("GUI 被用户关闭")
        return SUCCESS
    except Exception as e:
        logger.error(f"GUI 启动失败: {e}", exc_info=True)
        return FAILURE
