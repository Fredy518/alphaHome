"""
物化视图命令组

集成现有的 refresh-materialized-view CLI 功能。
"""

import argparse
import asyncio
from typing import Optional

from alphahome.cli.core.exitcodes import SUCCESS, FAILURE
from alphahome.cli.core.logging_config import get_cli_logger
from alphahome.cli.commands.base import CommandGroup

logger = get_cli_logger(__name__)


class MVCommandGroup(CommandGroup):
    """物化视图命令组"""
    
    group_name = "mv"
    group_help = "物化视图管理"
    
    def add_subparsers(self, subparsers: argparse._SubParsersAction) -> None:
        """添加物化视图子命令到子解析器容器"""
        mv_parser = subparsers.add_parser(
            self.group_name,
            help=self.group_help,
            description="AlphaHome 物化视图管理工具"
        )
        
        # 子命令
        sub = mv_parser.add_subparsers(dest='mv_command', help='物化视图命令')
        
        # refresh: 刷新单个视图
        refresh_parser = sub.add_parser('refresh', help='刷新单个物化视图')
        refresh_parser.add_argument('view_name', help='物化视图名称')
        refresh_parser.add_argument(
            '--strategy',
            choices=['full', 'concurrent'],
            default='full',
            help='刷新策略'
        )
        refresh_parser.add_argument(
            '--db-url',
            help='数据库 URL'
        )
        refresh_parser.set_defaults(func=_refresh_view)
        
        # refresh-all: 刷新所有视图
        refresh_all_parser = sub.add_parser('refresh-all', help='刷新所有物化视图')
        refresh_all_parser.add_argument(
            '--strategy',
            choices=['full', 'concurrent'],
            default='full',
            help='刷新策略'
        )
        refresh_all_parser.add_argument(
            '--db-url',
            help='数据库 URL'
        )
        refresh_all_parser.set_defaults(func=_refresh_all_views)
        
        # status: 查看单个视图状态
        status_parser = sub.add_parser('status', help='查看物化视图刷新状态')
        status_parser.add_argument('view_name', help='物化视图名称')
        status_parser.add_argument(
            '--db-url',
            help='数据库 URL'
        )
        status_parser.set_defaults(func=_get_view_status)
        
        # status-all: 查看所有视图状态
        status_all_parser = sub.add_parser('status-all', help='查看所有物化视图刷新状态')
        status_all_parser.add_argument(
            '--db-url',
            help='数据库 URL'
        )
        status_all_parser.set_defaults(func=_get_all_views_status)
        
        mv_parser.set_defaults(group='mv')


def _refresh_view(args) -> int:
    """刷新单个物化视图"""
    try:
        # 动态导入以避免启动时的依赖问题
        from alphahome.processors.materialized_views.cli import (
            refresh_materialized_view,
            get_db_connection_string
        )
        from alphahome.common.db_manager import DBManager
        
        db_url = args.db_url or get_db_connection_string()
        db = DBManager(db_url)
        
        # 异步调用
        result = asyncio.run(refresh_materialized_view(
            view_name=args.view_name,
            strategy=getattr(args, 'strategy', 'full'),
            db_connection=db
        ))
        
        logger.info(f"视图 {args.view_name} 刷新完成: {result}")
        return SUCCESS if result.get('status') == 'success' else FAILURE
        
    except Exception as e:
        logger.error(f"刷新视图失败: {e}", exc_info=True)
        return FAILURE


def _refresh_all_views(args) -> int:
    """刷新所有物化视图"""
    try:
        from alphahome.processors.materialized_views.cli import (
            refresh_all_materialized_views,
            get_db_connection_string
        )
        from alphahome.common.db_manager import DBManager
        
        db_url = args.db_url or get_db_connection_string()
        db = DBManager(db_url)
        
        result = asyncio.run(refresh_all_materialized_views(
            strategy=getattr(args, 'strategy', 'full'),
            db_connection=db
        ))
        
        logger.info(f"所有视图刷新完成: {result}")
        return SUCCESS if result.get('status') in ('success', 'partial_success') else FAILURE
        
    except Exception as e:
        logger.error(f"刷新所有视图失败: {e}", exc_info=True)
        return FAILURE


def _get_view_status(args) -> int:
    """查看单个物化视图的刷新状态"""
    try:
        from alphahome.processors.materialized_views.cli import (
            get_materialized_view_status,
            get_db_connection_string
        )
        from alphahome.common.db_manager import DBManager
        
        db_url = args.db_url or get_db_connection_string()
        db = DBManager(db_url)
        
        status = asyncio.run(get_materialized_view_status(
            view_name=args.view_name,
            db_connection=db
        ))
        
        logger.info(f"视图 {args.view_name} 状态: {status}")
        return SUCCESS
        
    except Exception as e:
        logger.error(f"获取视图状态失败: {e}", exc_info=True)
        return FAILURE


def _get_all_views_status(args) -> int:
    """查看所有物化视图的刷新状态"""
    try:
        from alphahome.processors.materialized_views.cli import (
            MATERIALIZED_VIEWS,
            get_materialized_view_status,
            get_db_connection_string
        )
        from alphahome.common.db_manager import DBManager
        
        db_url = args.db_url or get_db_connection_string()
        db = DBManager(db_url)
        
        for view_name in MATERIALIZED_VIEWS.keys():
            status = asyncio.run(get_materialized_view_status(
                view_name=view_name,
                db_connection=db
            ))
            logger.info(f"{view_name}: {status}")
        
        return SUCCESS
        
    except Exception as e:
        logger.error(f"获取所有视图状态失败: {e}", exc_info=True)
        return FAILURE
