#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
可转债基本信息数据获取示例
演示如何使用TushareCBondBasicTask获取可转债基本信息

支持的功能：
1. 全量更新：获取所有可转债的基本信息
2. 包含转债与正股对应关系、转股价、利率等详细信息
3. 权限要求：需要至少2000积分

使用方法：
python docs/examples/tushare_cbond_basic_usage.py
"""

import asyncio
import logging
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.task_system.task_factory import UnifiedTaskFactory


async def run_cbond_basic_task():
    """运行可转债基本信息获取任务"""

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
        logger.info("创建可转债基本信息任务...")
        task = UnifiedTaskFactory.create_task(
            task_name="tushare_cbond_basic",
            update_type="full",  # 可转债基本信息为全量更新
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


async def check_cbond_data():
    """检查可转债数据状态"""

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        await UnifiedTaskFactory.initialize()

        # 创建任务实例来检查状态
        task = UnifiedTaskFactory.create_task("tushare_cbond_basic")

        if task:
            # 获取表状态
            status = await task.get_table_status()
            logger.info("可转债基本信息表状态:")
            for key, value in status.items():
                logger.info(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"检查数据状态失败: {e}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='可转债基本信息数据获取示例')
    parser.add_argument('--mode', choices=['run', 'check'],
                       default='run', help='执行模式')

    args = parser.parse_args()

    if args.mode == 'check':
        asyncio.run(check_cbond_data())
    else:  # run
        asyncio.run(run_cbond_basic_task())


if __name__ == "__main__":
    main()
