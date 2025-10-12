#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
大盘指数每日指标数据获取示例
演示如何使用TushareIndexDailyBasicTask获取指定12个主要指数的每日指标数据

支持的功能：
1. 历史全量更新：获取所有指定指数的全量历史数据
2. 增量更新：获取最新的数据，追加到现有数据中
3. 指定的12个主要指数：上证综指、沪深300、中证500等

使用方法：
python docs/examples/tushare_index_dailybasic_usage.py
"""

import asyncio
import logging
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.task_system.task_factory import UnifiedTaskFactory


async def run_index_dailybasic_task():
    """运行大盘指数每日指标数据获取任务"""

    # 初始化日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)

    try:
        # 初始化任务工厂
        logger.info("初始化任务工厂...")
        await UnifiedTaskFactory.initialize()

        # 创建任务实例
        logger.info("创建大盘指数每日指标任务...")
        task = UnifiedTaskFactory.create_task(
            task_name="tushare_index_dailybasic",
            update_type="incremental",  # 或 "backfill"
            start_date="20240901",  # 可选，增量模式会自动从最新日期开始
            end_date="20240930"     # 可选，默认到今天
        )

        if task is None:
            logger.error("创建任务失败")
            return

        # 执行任务
        logger.info("开始执行任务...")
        result = await task.execute()

        # 输出结果
        logger.info("任务执行完成")
        logger.info(f"处理结果: {result}")

        if result and 'error' not in result:
            logger.info("✅ 任务执行成功")
        else:
            logger.error("❌ 任务执行失败")

    except Exception as e:
        logger.error(f"任务执行过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


async def demonstrate_backfill():
    """演示历史全量更新模式"""

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("演示历史全量更新模式...")
    logger.info("注意：全量更新会清空现有数据并重新获取所有历史数据")

    try:
        await UnifiedTaskFactory.initialize()

        # 全量更新模式
        task = UnifiedTaskFactory.create_task(
            task_name="tushare_index_dailybasic",
            update_type="backfill"  # 全量更新模式
        )

        if task:
            logger.info("执行全量更新...")
            result = await task.execute()
            logger.info(f"全量更新结果: {result}")

    except Exception as e:
        logger.error(f"全量更新演示失败: {e}")


async def demonstrate_incremental():
    """演示增量更新模式"""

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("演示增量更新模式...")
    logger.info("增量更新只获取新的数据，不会清空现有数据")

    try:
        await UnifiedTaskFactory.initialize()

        # 增量更新模式（默认）
        task = UnifiedTaskFactory.create_task(
            task_name="tushare_index_dailybasic",
            update_type="incremental",  # 增量更新模式
            start_date="20240925",     # 可选，指定开始日期
            end_date="20240928"        # 可选，指定结束日期
        )

        if task:
            logger.info("执行增量更新...")
            result = await task.execute()
            logger.info(f"增量更新结果: {result}")

    except Exception as e:
        logger.error(f"增量更新演示失败: {e}")


async def check_data_status():
    """检查当前数据状态"""

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        await UnifiedTaskFactory.initialize()

        # 创建任务实例来检查状态
        task = UnifiedTaskFactory.create_task("tushare_index_dailybasic")

        if task:
            # 获取表状态
            status = await task.get_table_status()
            logger.info("数据表状态:")
            for key, value in status.items():
                logger.info(f"  {key}: {value}")

            # 检查最近几天的数据
            logger.info("\n检查最近几天的数据...")
            # 这里可以添加数据检查逻辑

    except Exception as e:
        logger.error(f"检查数据状态失败: {e}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='大盘指数每日指标数据获取示例')
    parser.add_argument('--mode', choices=['incremental', 'backfill', 'check'],
                       default='incremental', help='执行模式')
    parser.add_argument('--start-date', help='开始日期 (YYYYMMDD)')
    parser.add_argument('--end-date', help='结束日期 (YYYYMMDD)')

    args = parser.parse_args()

    if args.mode == 'backfill':
        asyncio.run(demonstrate_backfill())
    elif args.mode == 'check':
        asyncio.run(check_data_status())
    else:  # incremental
        asyncio.run(run_index_dailybasic_task())


if __name__ == "__main__":
    main()
