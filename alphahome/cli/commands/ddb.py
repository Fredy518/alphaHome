"""
DolphinDB 命令组

集成现有的 alphahome-ddb CLI 功能。

为了简化，直接代理到原有的 alphahome-ddb CLI 入口。
"""

import argparse
import sys
import subprocess
from pathlib import Path
from typing import Optional

from alphahome.cli.core.exitcodes import SUCCESS, FAILURE
from alphahome.cli.core.logging_config import get_cli_logger
from alphahome.cli.commands.base import CommandGroup

logger = get_cli_logger(__name__)


class DDBCommandGroup(CommandGroup):
    """DolphinDB 命令组"""
    
    group_name = "ddb"
    group_help = "DolphinDB 数据库工具"
    
    def add_subparsers(self, subparsers: argparse._SubParsersAction) -> None:
        """添加 ddb 子命令到子解析器容器"""
        ddb_parser = subparsers.add_parser(
            self.group_name,
            help=self.group_help,
            description="AlphaHome DolphinDB 工具集"
        )
        
        # 添加通用的 DolphinDB 参数
        ddb_parser.add_argument('--host', default=None, help='DolphinDB host')
        ddb_parser.add_argument('--port', type=int, default=None, help='DolphinDB port')
        ddb_parser.add_argument('--username', default=None, help='DolphinDB username')
        ddb_parser.add_argument('--password', default=None, help='DolphinDB password')
        ddb_parser.add_argument('--format', choices=['text', 'json'], default='text', help='输出格式')
        
        # 子命令
        ddb_sub = ddb_parser.add_subparsers(dest='ddb_cmd', help='DolphinDB 命令')
        
        # init-kline5m
        init_parser = ddb_sub.add_parser('init-kline5m', help='初始化 5分钟K线表')
        init_parser.add_argument('--db-path', default='dfs://kline_5min', help='数据库路径')
        init_parser.add_argument('--table', default='kline_5min', help='表名')
        init_parser.add_argument('--start-month', type=int, default=200501, help='起始月份')
        init_parser.add_argument('--end-month', type=int, default=203012, help='结束月份')
        init_parser.add_argument('--hash-buckets', type=int, default=10, help='Hash桶数')
        init_parser.set_defaults(func=_init_kline5m)
        
        # import-hikyuu-5min
        import_parser = ddb_sub.add_parser('import-hikyuu-5min', help='导入Hikyuu5分钟数据')
        import_parser.add_argument('--hikyuu-data-dir', help='Hikyuu数据目录')
        import_parser.add_argument('--db-path', default='dfs://kline_5min', help='数据库路径')
        import_parser.add_argument('--table', default='kline_5min', help='表名')
        import_parser.add_argument('--codes', help='股票代码列表（逗号分隔）')
        import_parser.add_argument('--codes-file', help='股票代码文件')
        import_parser.add_argument('--start', help='开始日期')
        import_parser.add_argument('--end', help='结束日期')
        import_parser.add_argument('--incremental', action='store_true', help='增量导入')
        import_parser.add_argument('--chunk-rows', type=int, default=200000, help='批量行数')
        import_parser.add_argument('--price-scale', type=float, default=1000.0, help='价格缩放')
        import_parser.add_argument('--amount-scale', type=float, default=10.0, help='金额缩放')
        import_parser.add_argument('--dry-run', action='store_true', help='试运行')
        import_parser.add_argument('--init', action='store_true', help='初始化表')
        import_parser.set_defaults(func=_import_hikyuu)
        
        # drop-db
        drop_parser = ddb_sub.add_parser('drop-db', help='删除数据库')
        drop_parser.add_argument('--db-path', default='dfs://kline_5min', help='数据库路径')
        drop_parser.add_argument('--yes', action='store_true', help='确认删除')
        drop_parser.set_defaults(func=_drop_db)
        
        ddb_parser.set_defaults(group='ddb')


def _init_kline5m(args) -> int:
    """初始化 5分钟K线表"""
    try:
        from alphahome.integrations.dolphindb.cli import cmd_init_kline5m
        return cmd_init_kline5m(args) or SUCCESS
    except Exception as e:
        logger.error(f"初始化失败: {e}", exc_info=True)
        return FAILURE


def _import_hikyuu(args) -> int:
    """导入Hikyuu5分钟数据"""
    try:
        from alphahome.integrations.dolphindb.cli import cmd_import_hikyuu_5min
        return cmd_import_hikyuu_5min(args) or SUCCESS
    except Exception as e:
        logger.error(f"导入失败: {e}", exc_info=True)
        return FAILURE


def _drop_db(args) -> int:
    """删除数据库"""
    try:
        from alphahome.integrations.dolphindb.cli import cmd_drop_db
        return cmd_drop_db(args) or SUCCESS
    except Exception as e:
        logger.error(f"删除失败: {e}", exc_info=True)
        return FAILURE
