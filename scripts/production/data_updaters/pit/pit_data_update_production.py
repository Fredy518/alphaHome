#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT数据统一更新生产脚本
统一管理所有PIT（Point-in-Time）数据更新任务

配置说明：
- 使用项目的统一配置文件系统 (config.json)
- 数据库连接配置位于 config.json -> database.url
- 无需额外的配置文件

使用方法：
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental

功能特性：
- 统一管理多个PIT数据更新任务
- 支持并行执行，提升更新效率
- 智能任务调度和资源管理
- 详细的执行日志和状态监控
- 支持重试机制和错误恢复
- 生产级别的可靠性保证
- 使用项目的统一配置和数据库管理系统
"""

import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

# 添加项目根目录到 Python 路径
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)

from alphahome.common.db_manager import create_async_manager
from alphahome.common.config_manager import get_database_url
from alphahome.common.logging_utils import get_logger

# 导入PIT管理器
try:
    # 作为模块导入时使用相对导入
    from .pit_balance_quarterly_manager import PITBalanceQuarterlyManager
    from .pit_income_quarterly_manager import PITIncomeQuarterlyManager
    from .pit_financial_indicators_manager import PITFinancialIndicatorsManager
    from .pit_industry_classification_manager import PITIndustryClassificationManager
except ImportError:
    # 作为主脚本运行时使用绝对导入
    from pit_balance_quarterly_manager import PITBalanceQuarterlyManager
    from pit_income_quarterly_manager import PITIncomeQuarterlyManager
    from pit_financial_indicators_manager import PITFinancialIndicatorsManager
    from pit_industry_classification_manager import PITIndustryClassificationManager

logger = get_logger(__name__)


class PITDataUpdateCoordinator:
    """PIT数据更新协调器"""

    def __init__(self, max_workers: int = 2, max_retries: int = 3, retry_delay: int = 5):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.db_manager = None
        self.db_url = None

        # 初始化各个管理器并进入上下文
        self.managers = {}
        self.manager_contexts = {}

        manager_classes = {
            'balance': PITBalanceQuarterlyManager,
            'income': PITIncomeQuarterlyManager,
            'financial_indicators': PITFinancialIndicatorsManager,
            'industry_classification': PITIndustryClassificationManager
        }

        for name, manager_class in manager_classes.items():
            manager = manager_class()
            context = manager.__enter__()
            self.managers[name] = manager
            self.manager_contexts[name] = context

    async def initialize(self):
        """初始化数据库连接"""
        # 使用项目的统一配置系统获取数据库连接
        self.db_url = get_database_url()
        if not self.db_url:
            raise ValueError("数据库连接配置未找到，请检查config.json文件")

        self.db_manager = create_async_manager(self.db_url)
        logger.info("PIT数据更新协调器初始化完成")

    async def cleanup(self):
        """清理资源"""
        logger.info("开始清理PIT管理器资源...")
        for name, manager in self.managers.items():
            try:
                manager.__exit__(None, None, None)
                logger.info(f"成功清理 {name} 管理器")
            except Exception as e:
                logger.error(f"清理 {name} 管理器时出错: {e}")
        logger.info("PIT管理器资源清理完成")

    async def update_balance_data(self, mode: str = 'incremental', **kwargs):
        """更新资产负债表数据"""
        logger.info(f"开始更新资产负债表数据，模式: {mode}")
        try:
            if mode == 'incremental':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self.managers['balance'].incremental_update
                )
            elif mode == 'full':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self.managers['balance'].full_backfill
                )
            logger.info(f"资产负债表数据更新完成: {result}")
        except Exception as e:
            logger.error(f"资产负债表数据更新失败: {e}")
            raise

    async def update_income_data(self, mode: str = 'incremental', **kwargs):
        """更新利润表数据"""
        logger.info(f"开始更新利润表数据，模式: {mode}")
        try:
            if mode == 'incremental':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self.managers['income'].incremental_update
                )
            elif mode == 'full':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self.managers['income'].full_backfill
                )
            logger.info(f"利润表数据更新完成: {result}")
        except Exception as e:
            logger.error(f"利润表数据更新失败: {e}")
            raise

    async def update_financial_indicators(self, mode: str = 'incremental', **kwargs):
        """更新财务指标数据"""
        logger.info(f"开始更新财务指标数据，模式: {mode}")
        try:
            if mode == 'incremental':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self.managers['financial_indicators'].incremental_update
                )
            elif mode == 'full':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self.managers['financial_indicators'].full_backfill
                )
            logger.info(f"财务指标数据更新完成: {result}")
        except Exception as e:
            logger.error(f"财务指标数据更新失败: {e}")
            raise

    async def update_industry_classification(self, mode: str = 'incremental', **kwargs):
        """更新行业分类数据"""
        logger.info(f"开始更新行业分类数据，模式: {mode}")
        try:
            if mode == 'incremental':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self.managers['industry_classification'].incremental_update
                )
            elif mode == 'full':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self.managers['industry_classification'].full_backfill
                )
            logger.info(f"行业分类数据更新完成: {result}")
        except Exception as e:
            logger.error(f"行业分类数据更新失败: {e}")
            raise

    async def run_updates(self, targets: List[str], mode: str = 'incremental', parallel: bool = False):
        """运行指定的更新任务"""
        logger.info(f"开始执行PIT数据更新，目标: {targets}, 模式: {mode}, 并行: {parallel}")

        update_tasks = []
        for target in targets:
            if target == 'balance':
                update_tasks.append(('balance', self.update_balance_data(mode)))
            elif target == 'income':
                update_tasks.append(('income', self.update_income_data(mode)))
            elif target == 'financial_indicators':
                update_tasks.append(('financial_indicators', self.update_financial_indicators(mode)))
            elif target == 'industry_classification':
                update_tasks.append(('industry_classification', self.update_industry_classification(mode)))

        if parallel:
            # 并行执行
            tasks = [task for _, task in update_tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            # 顺序执行
            for task_name, task in update_tasks:
                logger.info(f"开始执行任务: {task_name}")
                try:
                    await task
                    logger.info(f"任务 {task_name} 执行成功")
                except Exception as e:
                    logger.error(f"任务 {task_name} 执行失败: {e}")
                    if not parallel:  # 顺序执行时遇到错误继续下一个任务
                        continue

        logger.info("所有PIT数据更新任务执行完成")


async def main():
    parser = argparse.ArgumentParser(description='PIT数据统一更新生产脚本')
    parser.add_argument('--target', nargs='+', choices=['balance', 'income', 'financial_indicators', 'industry_classification', 'all'],
                       default=['all'], help='要更新的目标数据类型')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental',
                       help='更新模式：incremental(增量) 或 full(全量)')
    parser.add_argument('--parallel', action='store_true', help='是否并行执行')
    parser.add_argument('--workers', type=int, default=2, help='最大并发进程数')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                       help='日志级别')

    args = parser.parse_args()

    # 设置日志级别
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # 解析目标
    if 'all' in args.target:
        targets = ['balance', 'income', 'financial_indicators', 'industry_classification']
    else:
        targets = args.target

    logger.info(f"PIT数据更新启动 - 目标: {targets}, 模式: {args.mode}, 并行: {args.parallel}")

    coordinator = PITDataUpdateCoordinator(max_workers=args.workers)

    try:
        await coordinator.initialize()
        await coordinator.run_updates(targets, args.mode, args.parallel)
        logger.info("PIT数据更新执行完成")
    except Exception as e:
        logger.error(f"PIT数据更新执行失败: {e}")
        sys.exit(1)
    finally:
        # 确保清理资源
        try:
            await coordinator.cleanup()
        except Exception as e:
            logger.error(f"清理资源时出错: {e}")


if __name__ == '__main__':
    asyncio.run(main())
