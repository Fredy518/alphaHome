#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tushare 数据源智能增量更新生产脚本
自动执行所有 tushare fetchers 任务的智能增量更新

使用方法：
python scripts/production/data_updaters/tushare/tushare_smart_update_production.py --workers 5 --max_retries 3

功能特性：
- 自动发现所有 tushare 相关的 fetch 任务
- 支持并行执行，提升更新效率
- 智能跳过不支持智能增量的任务
- 详细的执行日志和状态监控
- 支持重试机制和错误恢复
- 生产级别的数据一致性保证
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
sys.path.insert(0, '.')

from alphahome.common.db_manager import create_async_manager
from alphahome.common.logging_utils import get_logger
from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.common.constants import UpdateTypes
from alphahome.common.config_manager import get_database_url

logger = get_logger(__name__)


class TushareProductionUpdater:
    """Tushare 数据源生产级更新器"""

    def __init__(self, max_workers: int = 3, max_retries: int = 3, retry_delay: int = 5, dry_run: bool = False):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.dry_run = dry_run
        self.db_manager = None
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # Tushare API 并发限制说明
        self.tushare_concurrency_note = """
        📋 Tushare API 并发说明:
        - Tushare API 本身有并发限制（默认20个并发）
        - 不同API有不同限制（如daily:80, stock_basic:20）
        - 建议脚本并发数不超过 Tushare API 限制的 1/2
        - 当前设置: 脚本并发={}, Tushare默认并发=20
        """.format(max_workers)

        # 统计信息
        self.stats = {
            'total_tasks': 0,
            'successful_tasks': 0,
            'failed_tasks': 0,
            'skipped_tasks': 0,
            'start_time': None,
            'end_time': None
        }

    async def initialize(self):
        """初始化数据库连接和任务工厂"""
        try:
            logger.info("正在初始化数据库连接...")

            # 获取数据库连接字符串
            db_url = get_database_url()
            if not db_url:
                raise ValueError("无法获取数据库连接字符串，请检查配置文件")

            self.db_manager = create_async_manager(db_url)
            await UnifiedTaskFactory.initialize()

            logger.info("✅ 数据库连接和任务工厂初始化成功")
            return True
        except Exception as e:
            logger.error(f"❌ 初始化失败: {e}")
            return False

    async def get_tushare_tasks(self) -> List[str]:
        """获取所有 tushare 相关的 fetch 任务"""
        try:
            # 获取所有已注册的任务
            all_tasks = UnifiedTaskFactory.get_all_task_names()

            tushare_tasks = []
            for task_name in all_tasks:
                try:
                    task_info = UnifiedTaskFactory.get_task_info(task_name)

                    # 筛选条件：data_source 为 tushare 且 task_type 为 fetch
                    if (task_info.get('type') == 'fetch' and
                        hasattr(UnifiedTaskFactory._task_registry[task_name], 'data_source') and
                        UnifiedTaskFactory._task_registry[task_name].data_source == 'tushare'):

                        tushare_tasks.append(task_name)
                        logger.debug(f"发现 Tushare 任务: {task_name}")

                except Exception as e:
                    logger.warning(f"获取任务信息失败 {task_name}: {e}")
                    continue

            logger.info(f"✅ 发现 {len(tushare_tasks)} 个 Tushare fetch 任务")
            return sorted(tushare_tasks)

        except Exception as e:
            logger.error(f"❌ 获取 Tushare 任务列表失败: {e}")
            return []

    async def execute_task_with_retry(self, task_name: str, attempt: int = 1) -> Dict[str, Any]:
        """执行单个任务，支持重试机制"""
        try:
            logger.info(f"[{task_name}] 开始执行 (尝试 {attempt}/{self.max_retries + 1})")

            # 干运行模式：不实际执行任务
            if self.dry_run:
                logger.info(f"[{task_name}] 干运行模式，跳过实际执行")
                return {
                    'task_name': task_name,
                    'status': 'skipped_dry_run',
                    'message': '干运行模式',
                    'execution_time': 0.0,
                    'attempts': attempt
                }

            # 创建任务实例
            task_instance = await UnifiedTaskFactory.create_task_instance(
                task_name,
                update_type=UpdateTypes.SMART  # 使用智能增量模式
            )

            # 检查是否支持智能增量更新
            if not task_instance.supports_incremental_update():
                skip_reason = getattr(task_instance, 'get_incremental_skip_reason', lambda: '不支持智能增量')()
                logger.warning(f"[{task_name}] 跳过: {skip_reason}")
                return {
                    'task_name': task_name,
                    'status': 'skipped',
                    'message': skip_reason,
                    'attempts': attempt
                }

            # 执行任务
            start_time = time.time()
            result = await task_instance.execute()
            execution_time = time.time() - start_time

            if isinstance(result, dict):
                task_status = result.get('status', 'unknown')
                if task_status == 'success':
                    logger.info(f"[{task_name}] 执行成功，耗时: {execution_time:.2f}秒")
                    return {
                        'task_name': task_name,
                        'status': 'success',
                        'result': result,
                        'execution_time': execution_time,
                        'attempts': attempt
                    }
                elif task_status == 'partial_success':
                    logger.info(f"[{task_name}] 部分成功（有验证警告），耗时: {execution_time:.2f}秒")
                    return {
                        'task_name': task_name,
                        'status': 'partial_success',
                        'result': result,
                        'execution_time': execution_time,
                        'attempts': attempt
                    }
                else:
                    logger.warning(f"[{task_name}] 执行完成但状态异常: {result}")
                    return {
                        'task_name': task_name,
                        'status': task_status,  # 使用原始状态
                        'result': result,
                        'execution_time': execution_time,
                        'attempts': attempt
                    }
            else:
                logger.warning(f"[{task_name}] 执行结果格式异常: {result}")
                return {
                    'task_name': task_name,
                    'status': 'error',
                    'result': result,
                    'execution_time': execution_time,
                    'attempts': attempt
                }

        except Exception as e:
            logger.error(f"[{task_name}] 执行失败 (尝试 {attempt}): {e}")

            # 检查是否需要重试
            if attempt <= self.max_retries:
                logger.info(f"[{task_name}] {self.retry_delay}秒后重试...")
                await asyncio.sleep(self.retry_delay)
                return await self.execute_task_with_retry(task_name, attempt + 1)
            else:
                return {
                    'task_name': task_name,
                    'status': 'failed',
                    'error': str(e),
                    'attempts': attempt
                }

    async def execute_tasks_parallel(self, task_names: List[str]) -> List[Dict[str, Any]]:
        """并行执行多个任务"""
        logger.info(f"🚀 开始并行执行 {len(task_names)} 个任务 (最大并发: {self.max_workers})")

        # 创建任务列表
        tasks = []
        semaphore = asyncio.Semaphore(self.max_workers)

        async def execute_with_semaphore(task_name: str):
            async with semaphore:
                return await self.execute_task_with_retry(task_name)

        # 启动所有任务
        for task_name in task_names:
            task = asyncio.create_task(execute_with_semaphore(task_name))
            tasks.append(task)

        # 等待所有任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理结果
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                task_name = task_names[i]
                logger.error(f"[{task_name}] 任务执行异常: {result}")
                processed_results.append({
                    'task_name': task_name,
                    'status': 'error',
                    'error': str(result),
                    'attempts': 1
                })
            else:
                processed_results.append(result)

        return processed_results

    def print_execution_summary(self, results: List[Dict[str, Any]]):
        """打印执行摘要"""
        total_time = self.stats['end_time'] - self.stats['start_time']
        total_time_minutes = total_time.total_seconds() / 60

        print("\n" + "="*80)
        print("📊 Tushare 智能增量更新执行摘要")
        print("="*80)
        print(f"执行时间: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')} - {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总耗时: {total_time_minutes:.2f} 分钟")
        print(f"总任务数: {self.stats['total_tasks']}")
        print(f"✅ 成功任务: {self.stats['successful_tasks']}")
        print(f"❌ 失败任务: {self.stats['failed_tasks']}")
        print(f"⏭️ 跳过任务: {self.stats['skipped_tasks']}")
        print(f"⚠️  异常任务: {sum(1 for r in results if r.get('status') == 'error' and isinstance(r, dict))}")
        print(f"🔶 部分成功: {sum(1 for r in results if r.get('status') == 'partial_success' and isinstance(r, dict))}")
        print(f"成功率: {(self.stats['successful_tasks'] / max(self.stats['total_tasks'], 1) * 100):.2f}%")
        if self.stats['successful_tasks'] > 0:
            avg_time_per_task = sum(r.get('execution_time', 0) for r in results if r.get('execution_time')) / self.stats['successful_tasks']
            print(f"平均任务耗时: {avg_time_per_task:.2f}秒")
        print()

        # 显示失败的任务详情
        failed_tasks = [r for r in results if isinstance(r, dict) and r.get('status') in ['failed', 'error']]
        if failed_tasks:
            print("❌ 失败任务详情:")
            for task in failed_tasks:
                error_msg = task.get('error', '未知错误')
                status = task.get('status', 'unknown')
                print(f"   - {task['task_name']} ({status}): {error_msg}")
            print()

        # 显示部分成功的任务详情
        partial_success_tasks = [r for r in results if isinstance(r, dict) and r.get('status') == 'partial_success']
        if partial_success_tasks:
            print("🔶 部分成功任务详情:")
            for task in partial_success_tasks:
                result = task.get('result', {})
                validation_details = result.get('validation_details', {})
                failed_count = len(validation_details.get('failed_validations', {}))
                print(f"   - {task['task_name']}: {failed_count}个验证规则失败")
            print()

        # 显示跳过的任务详情
        skipped_tasks = [r for r in results if isinstance(r, dict) and r.get('status') == 'skipped']
        if skipped_tasks:
            print("⏭️ 跳过任务详情:")
            for task in skipped_tasks:
                print(f"   - {task['task_name']}: {task.get('message', '不支持智能增量')}")
            print()

        print("🎯 建议:")
        if self.stats['failed_tasks'] > 0:
            print("   - 检查失败任务的网络连接或 API 权限")
            print("   - 查看详细日志了解具体错误原因")
        if self.stats['successful_tasks'] / max(self.stats['total_tasks'], 1) < 0.8:
            print("   - 成功率较低，建议降低并发数或增加重试次数")
        else:
            print("   - 更新执行成功，数据已保持最新状态")

    async def run_production_update(self) -> bool:
        """运行生产级更新"""
        self.stats['start_time'] = datetime.now()

        try:
            # 初始化
            if not await self.initialize():
                return False

            # 获取 Tushare 任务列表
            tushare_tasks = await self.get_tushare_tasks()
            if not tushare_tasks:
                logger.error("❌ 未发现任何 Tushare 任务")
                return False

            self.stats['total_tasks'] = len(tushare_tasks)

            # 执行任务
            logger.info("🚀 开始生产级 Tushare 数据更新...")
            results = await self.execute_tasks_parallel(tushare_tasks)

            # 统计结果
            for result in results:
                status = result.get('status', 'unknown')
                if status in ['success', 'partial_success']:
                    self.stats['successful_tasks'] += 1
                elif status in ['failed', 'error']:
                    self.stats['failed_tasks'] += 1
                elif status in ['skipped', 'skipped_dry_run']:
                    self.stats['skipped_tasks'] += 1
                elif status == 'completed_with_warnings':
                    # 兼容旧的状态，归类为部分成功
                    self.stats['successful_tasks'] += 1
                else:
                    # 处理其他未知状态
                    logger.warning(f"未知任务状态: {status} for task {result.get('task_name')}")
                    self.stats['failed_tasks'] += 1  # 归类为失败

            # 打印摘要
            self.stats['end_time'] = datetime.now()
            self.print_execution_summary(results)

            # 返回成功状态
            success_rate = self.stats['successful_tasks'] / max(self.stats['total_tasks'], 1)
            return success_rate >= 0.8  # 80% 成功率视为整体成功

        except Exception as e:
            logger.error(f"❌ 生产级更新执行失败: {e}")
            return False
        finally:
            # 清理资源
            if self.executor:
                self.executor.shutdown(wait=True)
            if self.db_manager:
                await self.db_manager.close()


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Tushare 数据源智能增量更新生产脚本')
    parser.add_argument('--workers', type=int, default=3,
                       help='最大并发工作进程数 (默认: 3)')
    parser.add_argument('--max_retries', type=int, default=3,
                       help='单个任务最大重试次数 (默认: 3)')
    parser.add_argument('--retry_delay', type=int, default=5,
                       help='重试间隔秒数 (默认: 5)')
    parser.add_argument('--log_level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='日志级别 (默认: INFO)')
    parser.add_argument('--dry-run', action='store_true',
                       help='启用干运行模式，只显示将要执行的任务，不实际执行')

    args = parser.parse_args()

    # 设置日志级别
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    print("🚀 Tushare 数据源智能增量更新生产脚本")
    print("=" * 60)
    print(f"并发进程数: {args.workers}")
    print(f"最大重试次数: {args.max_retries}")
    print(f"重试间隔: {args.retry_delay}秒")
    print(f"日志级别: {args.log_level}")
    print(f"干运行模式: {'是' if args.dry_run else '否'}")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # 显示 Tushare API 并发限制说明
    updater = TushareProductionUpdater(
        max_workers=args.workers,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay,
        dry_run=args.dry_run
    )
    print(updater.tushare_concurrency_note)
    print()

    # 创建更新器并执行
    # 重用之前创建的updater实例，确保参数一致
    # updater 已经在前面创建过了，这里直接使用

    success = await updater.run_production_update()

    # 返回退出码
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
