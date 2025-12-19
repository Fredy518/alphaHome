#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CLI commands for materialized view management.

Provides command-line interface for:
- Refreshing single materialized views
- Refreshing all materialized views
- Specifying refresh strategies (full/concurrent)
- Viewing refresh status and history

Requirements: 6.1, 6.2, 6.3
"""

import asyncio
import os
import sys
from typing import Optional, List
from datetime import datetime
import json
import logging

from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import ConfigManager
from .refresh import MaterializedViewRefresh
from .monitor import MaterializedViewMonitor
from alphahome.processors.tasks.pit.pit_financial_indicators_mv import PITFinancialIndicatorsMV
from alphahome.processors.tasks.pit.pit_industry_classification_mv import PITIndustryClassificationMV
from alphahome.processors.tasks.market.market_technical_indicators_mv import MarketTechnicalIndicatorsMV
from alphahome.processors.tasks.market.sector_aggregation_mv import SectorAggregationMV

logger = logging.getLogger(__name__)


# =============================================================================
# Materialized View Registry
# =============================================================================

# Registry of all available materialized views
MATERIALIZED_VIEWS = {
    'pit_financial_indicators_mv': PITFinancialIndicatorsMV,
    'pit_industry_classification_mv': PITIndustryClassificationMV,
    'market_technical_indicators_mv': MarketTechnicalIndicatorsMV,
    'sector_aggregation_mv': SectorAggregationMV,
}


# =============================================================================
# CLI Command Functions
# =============================================================================

def get_db_connection_string(explicit_db_url: Optional[str] = None) -> str:
    """Get database connection string from args/env/config."""
    if explicit_db_url:
        return explicit_db_url

    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        return db_url

    try:
        config_manager = ConfigManager()
        db_url = config_manager.get_database_url()
        if db_url:
            return db_url
    except Exception:
        pass

    raise RuntimeError(
        "No database URL configured. Provide --db-url, set DATABASE_URL, or configure ~/.alphahome/config.json (database.url)."
    )

async def refresh_materialized_view(
    view_name: str,
    schema: str = 'materialized_views',
    strategy: str = 'full',
    db_connection=None,
) -> dict:
    """
    Refresh a single materialized view.
    
    Parameters:
    - view_name: Name of the materialized view (without schema)
    - schema: Schema name (default: materialized_views)
    - strategy: Refresh strategy ('full' or 'concurrent')
    - db_connection: Database connection object
    
    Returns:
    {
        'status': 'success' | 'failed',
        'view_name': str,
        'full_name': str,
        'refresh_time': datetime,
        'duration_seconds': float,
        'row_count': int,
        'error_message': str (if failed)
    }
    
    Requirements: 6.1, 6.2, 6.3
    """
    if not db_connection:
        raise ValueError("Database connection is required")
    
    if strategy not in ('full', 'concurrent'):
        raise ValueError(
            f"Invalid refresh strategy: {strategy}. "
            f"Must be 'full' or 'concurrent'."
        )
    
    logger.info(
        f"Refreshing materialized view: {schema}.{view_name} "
        f"(strategy: {strategy})"
    )
    
    refresh = MaterializedViewRefresh(db_connection=db_connection, logger=logger)
    result = await refresh.refresh(
        view_name=view_name,
        schema=schema,
        strategy=strategy,
    )
    
    return result


async def refresh_all_materialized_views(
    strategy: str = 'full',
    db_connection=None,
) -> dict:
    """
    Refresh all registered materialized views.
    
    Parameters:
    - strategy: Refresh strategy ('full' or 'concurrent')
    - db_connection: Database connection object
    
    Returns:
    {
        'status': 'success' | 'partial_success' | 'failed',
        'total': int,
        'succeeded': int,
        'failed': int,
        'results': [
            {
                'view_name': str,
                'status': 'success' | 'failed',
                'duration_seconds': float,
                'row_count': int,
                'error_message': str (if failed)
            },
            ...
        ]
    }
    
    Requirements: 6.1, 6.2, 6.3
    """
    if not db_connection:
        raise ValueError("Database connection is required")
    
    logger.info(f"Refreshing all materialized views (strategy: {strategy})")
    
    results = []
    succeeded = 0
    failed = 0
    
    for view_name in MATERIALIZED_VIEWS.keys():
        try:
            result = await refresh_materialized_view(
                view_name=view_name,
                schema='materialized_views',
                strategy=strategy,
                db_connection=db_connection,
            )
            results.append(result)
            
            if result['status'] == 'success':
                succeeded += 1
                logger.info(
                    f"✓ {view_name}: {result['row_count']} rows "
                    f"({result['duration_seconds']:.2f}s)"
                )
            else:
                failed += 1
                logger.error(
                    f"✗ {view_name}: {result.get('error_message', 'Unknown error')}"
                )
        except Exception as e:
            failed += 1
            logger.error(f"✗ {view_name}: {type(e).__name__}: {e}")
            results.append({
                'view_name': view_name,
                'status': 'failed',
                'error_message': f"{type(e).__name__}: {e}",
            })
    
    # Determine overall status
    if failed == 0:
        overall_status = 'success'
    elif succeeded > 0:
        overall_status = 'partial_success'
    else:
        overall_status = 'failed'
    
    return {
        'status': overall_status,
        'total': len(MATERIALIZED_VIEWS),
        'succeeded': succeeded,
        'failed': failed,
        'results': results,
    }


async def get_materialized_view_status(
    view_name: str,
    db_connection=None,
) -> dict:
    """
    Get the refresh status of a materialized view.
    
    Parameters:
    - view_name: Name of the materialized view
    - db_connection: Database connection object
    
    Returns:
    {
        'view_name': str,
        'last_refresh_time': datetime | None,
        'refresh_status': str,
        'row_count': int,
        'refresh_duration_seconds': float,
        'error_message': str | None
    }
    
    Requirements: 6.2
    """
    if not db_connection:
        raise ValueError("Database connection is required")
    
    monitor = MaterializedViewMonitor(db_connection=db_connection)
    status = await monitor.get_latest_refresh_status(view_name)
    
    if status is None:
        return {
            'view_name': view_name,
            'last_refresh_time': None,
            'refresh_status': 'never_refreshed',
            'row_count': 0,
            'refresh_duration_seconds': 0,
            'error_message': None,
        }
    
    return {
        'view_name': view_name,
        'last_refresh_time': status.get('last_refresh_time'),
        'refresh_status': status.get('refresh_status'),
        'row_count': status.get('row_count'),
        'refresh_duration_seconds': status.get('refresh_duration_seconds'),
        'error_message': status.get('error_message'),
    }


async def get_all_materialized_views_status(
    db_connection=None,
) -> dict:
    """
    Get the refresh status of all materialized views.
    
    Parameters:
    - db_connection: Database connection object
    
    Returns:
    {
        'total': int,
        'views': [
            {
                'view_name': str,
                'last_refresh_time': datetime | None,
                'refresh_status': str,
                'row_count': int,
                'refresh_duration_seconds': float,
                'error_message': str | None
            },
            ...
        ]
    }
    
    Requirements: 6.2
    """
    if not db_connection:
        raise ValueError("Database connection is required")
    
    views = []
    for view_name in MATERIALIZED_VIEWS.keys():
        status = await get_materialized_view_status(
            view_name=view_name,
            db_connection=db_connection,
        )
        views.append(status)
    
    return {
        'total': len(views),
        'views': views,
    }


async def get_materialized_view_history(
    view_name: str,
    limit: int = 10,
    db_connection=None,
) -> dict:
    """
    Get the refresh history of a materialized view.
    
    Parameters:
    - view_name: Name of the materialized view
    - limit: Maximum number of history records to return
    - db_connection: Database connection object
    
    Returns:
    {
        'view_name': str,
        'total_records': int,
        'history': [
            {
                'refresh_time': datetime,
                'refresh_status': str,
                'row_count': int,
                'refresh_duration_seconds': float,
                'error_message': str | None
            },
            ...
        ]
    }
    
    Requirements: 6.2
    """
    if not db_connection:
        raise ValueError("Database connection is required")
    
    monitor = MaterializedViewMonitor(db_connection=db_connection)
    history = await monitor.get_refresh_history(view_name, limit=limit)
    
    return {
        'view_name': view_name,
        'total_records': len(history),
        'history': history,
    }


# =============================================================================
# CLI Entry Point (for command-line usage)
# =============================================================================

def format_result(result: dict, format: str = 'text') -> str:
    """
    Format result for display.
    
    Parameters:
    - result: Result dictionary
    - format: Output format ('text' or 'json')
    
    Returns:
    Formatted string
    """
    if format == 'json':
        # Convert datetime objects to ISO format strings
        def serialize(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        return json.dumps(result, default=serialize, indent=2)
    
    # Text format
    if 'results' in result:
        # Batch refresh result
        lines = [
            f"Refresh Results:",
            f"  Status: {result['status']}",
            f"  Total: {result['total']}",
            f"  Succeeded: {result['succeeded']}",
            f"  Failed: {result['failed']}",
            "",
            "Details:",
        ]
        for r in result['results']:
            if r['status'] == 'success':
                lines.append(
                    f"  ✓ {r['view_name']}: {r['row_count']} rows "
                    f"({r['duration_seconds']:.2f}s)"
                )
            else:
                lines.append(
                    f"  ✗ {r['view_name']}: {r.get('error_message', 'Unknown error')}"
                )
        return "\n".join(lines)
    
    elif 'refresh_time' in result:
        # Single refresh result
        if result['status'] == 'success':
            return (
                f"Refresh successful:\n"
                f"  View: {result['full_name']}\n"
                f"  Rows: {result['row_count']}\n"
                f"  Duration: {result['duration_seconds']:.2f}s\n"
                f"  Time: {result['refresh_time']}"
            )
        else:
            return (
                f"Refresh failed:\n"
                f"  View: {result['full_name']}\n"
                f"  Error: {result.get('error_message', 'Unknown error')}"
            )
    
    elif 'history' in result:
        # History result
        lines = [
            f"Refresh History for {result['view_name']}:",
            f"  Total records: {result['total_records']}",
            "",
        ]
        for h in result['history']:
            lines.append(
                f"  {h['refresh_time']}: {h['refresh_status']} "
                f"({h['row_count']} rows, {h['refresh_duration_seconds']:.2f}s)"
            )
        return "\n".join(lines)
    
    elif 'views' in result:
        # All views status result
        lines = [
            f"Materialized Views Status:",
            f"  Total: {result['total']}",
            "",
        ]
        for v in result['views']:
            if v['refresh_status'] == 'never_refreshed':
                lines.append(f"  {v['view_name']}: Never refreshed")
            else:
                lines.append(
                    f"  {v['view_name']}: {v['refresh_status']} "
                    f"({v['row_count']} rows, {v['last_refresh_time']})"
                )
        return "\n".join(lines)
    
    else:
        # Generic result
        return json.dumps(result, indent=2, default=str)


async def main(argv: Optional[List[str]] = None) -> int:
    """
    Main CLI entry point.
    
    Usage:
    - python -m alphahome.processors.materialized_views.cli refresh <view_name> [--strategy full|concurrent]
    - python -m alphahome.processors.materialized_views.cli refresh-all [--strategy full|concurrent]
    - python -m alphahome.processors.materialized_views.cli status <view_name>
    - python -m alphahome.processors.materialized_views.cli status-all
    - python -m alphahome.processors.materialized_views.cli history <view_name> [--limit 10]
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Materialized View Management CLI',
        prog='refresh-materialized-view',
    )
    parser.add_argument(
        '--db-url',
        default=None,
        help='Database connection URL (defaults to DATABASE_URL or ~/.alphahome/config.json)',
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Log level (default: INFO)',
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Refresh single view
    refresh_parser = subparsers.add_parser(
        'refresh',
        help='Refresh a single materialized view'
    )
    refresh_parser.add_argument(
        'view_name',
        help='Name of the materialized view'
    )
    refresh_parser.add_argument(
        '--strategy',
        choices=['full', 'concurrent'],
        default='full',
        help='Refresh strategy (default: full)'
    )
    refresh_parser.add_argument(
        '--format',
        choices=['text', 'json'],
        default='text',
        help='Output format (default: text)'
    )
    
    # Refresh all views
    refresh_all_parser = subparsers.add_parser(
        'refresh-all',
        help='Refresh all materialized views'
    )
    refresh_all_parser.add_argument(
        '--strategy',
        choices=['full', 'concurrent'],
        default='full',
        help='Refresh strategy (default: full)'
    )
    refresh_all_parser.add_argument(
        '--format',
        choices=['text', 'json'],
        default='text',
        help='Output format (default: text)'
    )
    
    # Get status of single view
    status_parser = subparsers.add_parser(
        'status',
        help='Get refresh status of a materialized view'
    )
    status_parser.add_argument(
        'view_name',
        help='Name of the materialized view'
    )
    status_parser.add_argument(
        '--format',
        choices=['text', 'json'],
        default='text',
        help='Output format (default: text)'
    )
    
    # Get status of all views
    status_all_parser = subparsers.add_parser(
        'status-all',
        help='Get refresh status of all materialized views'
    )
    status_all_parser.add_argument(
        '--format',
        choices=['text', 'json'],
        default='text',
        help='Output format (default: text)'
    )
    
    # Get history of single view
    history_parser = subparsers.add_parser(
        'history',
        help='Get refresh history of a materialized view'
    )
    history_parser.add_argument(
        'view_name',
        help='Name of the materialized view'
    )
    history_parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Maximum number of history records (default: 10)'
    )
    history_parser.add_argument(
        '--format',
        choices=['text', 'json'],
        default='text',
        help='Output format (default: text)'
    )
    
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    if not args.command:
        parser.print_help()
        print("\n" + "="*60)
        print("💡 迁移提示：建议使用统一CLI")
        print("   refresh-materialized-view 命令将继续可用，但推荐使用:")
        print("   ah mv refresh <view_name>     # 刷新单个视图")
        print("   ah mv refresh-all            # 刷新所有视图")
        print("   ah mv status <view_name>      # 查看视图状态")
        print("   ah mv status-all             # 查看所有视图状态")
        print("="*60)
        return 1

    # 显示迁移提示（仅在非help场景）
    print("\n💡 提示：推荐使用统一CLI 'ah mv ...' 替代 'refresh-materialized-view ...'")
    print("   例如: ah mv refresh-all --strategy concurrent")
    print()
    
    db_manager: Optional[DBManager] = None
    try:
        db_url = get_db_connection_string(args.db_url)
        db_manager = DBManager(db_url, mode="async")
        await db_manager.connect()

        if args.command == 'refresh':
            result = await refresh_materialized_view(
                view_name=args.view_name,
                strategy=args.strategy,
                db_connection=db_manager,
            )
            print(format_result(result, format=args.format))
            return 0 if result['status'] == 'success' else 1
        
        elif args.command == 'refresh-all':
            result = await refresh_all_materialized_views(
                strategy=args.strategy,
                db_connection=db_manager,
            )
            print(format_result(result, format=args.format))
            return 0 if result['status'] in ('success', 'partial_success') else 1
        
        elif args.command == 'status':
            result = await get_materialized_view_status(
                view_name=args.view_name,
                db_connection=db_manager,
            )
            print(format_result(result, format=args.format))
            return 0
        
        elif args.command == 'status-all':
            result = await get_all_materialized_views_status(
                db_connection=db_manager,
            )
            print(format_result(result, format=args.format))
            return 0
        
        elif args.command == 'history':
            result = await get_materialized_view_history(
                view_name=args.view_name,
                limit=args.limit,
                db_connection=db_manager,
            )
            print(format_result(result, format=args.format))
            return 0
        
        else:
            parser.print_help()
            return 1
    
    except Exception as e:
        logger.error(f"Command failed: {e}", exc_info=True)
        print(f"Error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    
    finally:
        try:
            if db_manager is not None:
                await db_manager.close()
        except Exception:
            pass


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))


def main_sync() -> int:
    """Console-script entrypoint."""
    # 显示全局迁移提示
    print("提示：推荐使用统一CLI 'ah mv ...' 替代 'refresh-materialized-view ...'")
    print("      例如: ah mv refresh-all --strategy concurrent")
    print()
    return asyncio.run(main())
