#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AlphaHome 统一命令行界面主入口

提供统一的 CLI 框架，整合所有生产能力、工具和工具集。
"""

import argparse
import sys
import logging
from typing import Optional, List

from alphahome.cli.core.exitcodes import SUCCESS, FAILURE, INVALID_ARGS, INTERRUPTED
from alphahome.cli.core.logging_config import setup_cli_logging, get_cli_logger
from alphahome.cli.core.exceptions import CLIError
from alphahome.cli.commands.registry import get_all_command_groups

logger = get_cli_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """
    构建主解析器及所有子命令。
    
    Returns:
        构建完成的 ArgumentParser 实例
    """
    parser = argparse.ArgumentParser(
        prog='ah',
        description='AlphaHome 统一命令行界面 - 量化数据和生产工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 列出可用的生产脚本
  ah prod list
  
  # 运行数据采集任务
  ah prod run data-collection -- --workers 5 --log_level DEBUG
  
  # 初始化 DolphinDB 5分钟K线表
  ah ddb init-kline5m --db-path dfs://kline_5min
  
  # 刷新所有物化视图
  ah mv refresh-all
  
  # 启动 GUI
  ah gui
  
更多信息，使用 `ah <command> --help` 查看。
        """
    )
    
    # 全局参数
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='日志级别 (默认: INFO)'
    )
    parser.add_argument(
        '--format',
        choices=['text', 'json'],
        default='text',
        help='输出格式 (默认: text)'
    )
    parser.add_argument(
        '--config',
        help='显式指定配置文件路径'
    )
    
    # 创建子命令容器
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 添加所有命令组
    command_groups = get_all_command_groups()
    for group in command_groups:
        group.add_subparsers(subparsers)
    
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """
    CLI 主函数。
    
    Args:
        argv: 命令行参数列表（用于测试）。如果为 None，使用 sys.argv[1:]
        
    Returns:
        退出码（0=成功，非0=失败）
    """
    try:
        # 解析命令行参数
        parser = build_parser()
        args = parser.parse_args(argv)
        
        # 设置日志级别
        setup_cli_logging(args.log_level)
        
        logger.debug(f"命令行参数: {args}")
        
        # 检查是否指定了子命令
        if not hasattr(args, 'func'):
            parser.print_help()
            return INVALID_ARGS
        
        # 执行子命令
        result = args.func(args)
        
        # 确保返回值是整数
        if isinstance(result, bool):
            return SUCCESS if result else FAILURE
        return int(result) if result is not None else SUCCESS
        
    except CLIError as e:
        logger.error(f"CLI 错误: {e.message}")
        return e.exit_code
        
    except KeyboardInterrupt:
        logger.warning("用户中断执行")
        return INTERRUPTED
        
    except SystemExit as e:
        # 处理子命令可能的 sys.exit 调用
        return int(e.code) if e.code is not None else SUCCESS
        
    except Exception as e:
        logger.error(f"未处理的异常: {e}", exc_info=True)
        return FAILURE


def main_sync() -> int:
    """
    同步版本的 main 函数，用于 setuptools entry_points。
    """
    return main()


if __name__ == '__main__':
    sys.exit(main())
