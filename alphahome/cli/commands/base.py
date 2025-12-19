"""
命令组基类

定义命令组的通用接口和模式。
"""

import argparse
from abc import ABC, abstractmethod
from typing import Any, Optional


class CommandGroup(ABC):
    """命令组基类，所有子命令组继承此类"""
    
    # 子类应实现此属性
    group_name: str  # 命令组名称
    group_help: str  # 命令组帮助信息
    
    @abstractmethod
    def add_subparsers(self, subparsers: argparse._SubParsersAction) -> None:
        """
        向指定的 subparsers 容器添加此命令组。
        
        Args:
            subparsers: 由 parser.add_subparsers() 返回的子解析器容器
        """
        pass
