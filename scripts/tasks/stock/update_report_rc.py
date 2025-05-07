#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
券商盈利预测数据更新脚本

使用 TaskUpdaterBase 基类实现的券商盈利预测数据更新工具。
支持以下功能：
1. 按日期范围更新 (基于报告日期 report_date)
2. 支持全量更新
3. 可选按股票代码过滤
4. 详细的日志记录和结果汇总
"""

import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime, timedelta
import dotenv
from pathlib import Path
import pandas as pd
from alphahome.fetchers.task_factory import TaskFactory
from scripts.base.task_updater_base import TaskUpdaterBase

# 添加项目根目录到系统路径
# Correct path for scripts/tasks/stock/
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 定义目标任务名称常量
TARGET_TASK_NAME = "tushare_stock_report_rc"

class ReportRcUpdater(TaskUpdaterBase):
    """
    券商盈利预测数据更新工具

    继承自 TaskUpdaterBase，用于更新券商盈利预测数据。
    支持按日期范围更新、全量更新，可选按股票代码过滤。
    """

    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="券商盈利预测",
            description="券商盈利预测数据更新工具",
            support_report_period=False # Uses report_date, not financial period
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    async def update_task(self, start_date=None, end_date=None, ts_code=None, full_update=False):
        """
        更新券商盈利预测数据

        Args:
            start_date: 开始日期 (报告日期)，格式：YYYYMMDD
            end_date: 结束日期 (报告日期)，格式：YYYYMMDD
            ts_code: 股票代码 (可选)
            full_update: 是否全量更新

        Returns:
            tuple: (成功更新数量, 失败数量, 错误信息列表)
        """
        log_prefix = f"股票代码 {ts_code}: " if ts_code else ""
        self.logger.info(f"{log_prefix}开始更新券商盈利预测数据...")

        try:
            task = await TaskFactory.get_task(TARGET_TASK_NAME)
            # Pass ts_code to the task run method
            result = await task.execute(
                start_date=start_date,
                end_date=end_date,
                ts_code=ts_code,
                full_update=full_update
            )

            if not isinstance(result, dict):
                self.logger.error(f"{log_prefix}任务返回结果格式错误: {result}")
                return 0, 1, [f"任务返回结果格式错误: {result}"]

            # Fix: Extract counts based on actual returned keys ('rows', 'status')
            status = result.get('status', 'unknown')
            rows = result.get('rows', 0)
            # failed_batches = result.get('failed_batches', 0) # We might log this if needed

            if status in ['success', 'partial_success']:
                success_count = int(rows) if pd.notna(rows) else 0 # Handle potential NaN
                failed_count = 0 # Assume success means 0 failed rows for this summary
            elif status == 'no_data':
                success_count = 0
                failed_count = 0
            else: # Handle 'error' or other statuses
                success_count = 0
                failed_count = 1 # Indicate the task/batch failed overall

            error_msgs = result.get("error_msgs", [])
            # Add the main error from the result if status indicates failure
            if status not in ['success', 'partial_success', 'no_data'] and result.get('error'):
                if not error_msgs: # Avoid duplicate errors if already in list
                    error_msgs = [result.get('error')]
                elif result.get('error') not in error_msgs:
                    error_msgs.append(result.get('error'))

            self.logger.info(f"{log_prefix}更新完成。成功: {success_count}, 失败: {failed_count}")
            if error_msgs:
                self.logger.warning(f"{log_prefix}错误信息: {error_msgs}")

            return success_count, failed_count, error_msgs

        except Exception as e:
            error_msg = f"{log_prefix}更新过程发生错误: {str(e)}"
            self.logger.error(error_msg, exc_info=True) # Log stack trace
            return 0, 1, [error_msg]

    async def summarize_result(self, success_count, failed_count, error_msgs,
                            start_date=None, end_date=None, ts_code=None, full_update=False):
        """
        汇总更新结果

        Args:
            success_count: 成功更新数量
            failed_count: 失败数量
            error_msgs: 错误信息列表
            start_date: 开始日期
            end_date: 结束日期
            ts_code: 股票代码 (可选)
            full_update: 是否全量更新
        """
        total = success_count + failed_count
        log_prefix = f"股票代码 {ts_code}: " if ts_code else ""

        if total == 0:
            self.logger.info(f"{log_prefix}本次更新无数据")
            return

        success_rate = (success_count / total) * 100 if total > 0 else 0

        # 输出更新模式
        if full_update:
            mode_desc = "全量更新"
        elif start_date and end_date:
            mode_desc = f"指定日期范围更新 ({start_date} 至 {end_date})"
        else:
            mode_desc = "增量更新 (依赖任务内部逻辑)" # Task handles incremental logic based on dates
        self.logger.info(f"{log_prefix}更新模式: {mode_desc}")

        # 输出更新结果统计
        self.logger.info(f"{log_prefix}更新结果汇总:")
        self.logger.info(f"{log_prefix}总数据量: {total}")
        self.logger.info(f"{log_prefix}成功数量: {success_count}")
        self.logger.info(f"{log_prefix}失败数量: {failed_count}")
        self.logger.info(f"{log_prefix}成功率: {success_rate:.2f}%")

        # 如果有错误信息，输出详细错误
        if error_msgs:
            self.logger.warning(f"{log_prefix}错误详情:")
            for msg in error_msgs:
                self.logger.warning(f"{log_prefix}- {msg}")

async def main():
    updater = ReportRcUpdater()

    # 使用基类的参数解析器，获得更多通用参数如 --auto
    parser = updater.setup_parser()
    
    # 添加此脚本特有的参数
    parser.add_argument("--ts-code", help="股票代码 (可选, 如 600000.SH)")

    args = parser.parse_args()

    # 参数检查: 全量更新和日期范围更新互斥
    if args.full_update and (args.start_date or args.end_date):
        parser.error("全量更新模式 (--full-update) 不能与指定日期范围 (--start-date/--end-date) 同时使用。")
    # 如果不是全量更新，则必须提供日期范围(如果不是自动模式)
    if not args.full_update and not (args.start_date and args.end_date) and not getattr(args, 'auto', False):
        parser.error("必须提供 --start-date 和 --end-date 用于范围更新，或使用 --full-update 进行全量更新，或使用 --auto 进行自动模式。")
    # 日期配对检查 (仅在非全量更新时)
    if not args.full_update and not getattr(args, 'auto', False):
        if args.start_date and not args.end_date:
            parser.error("如果指定开始日期，必须同时指定结束日期")
        if args.end_date and not args.start_date:
            parser.error("如果指定结束日期，必须同时指定开始日期")

    try:
        # 如果是全量更新或自动模式，将日期设为None (由任务内部决定)
        is_auto = getattr(args, 'auto', False)
        start_date = None if args.full_update or is_auto else args.start_date
        end_date = None if args.full_update or is_auto else args.end_date
        
        # 自动模式下，启用全量更新或尝试使用smart_incremental_update
        full_update = args.full_update or (is_auto and not (args.start_date or args.end_date))

        # 执行更新
        success_count, failed_count, error_msgs = await updater.update_task(
            start_date=start_date,
            end_date=end_date,
            ts_code=args.ts_code, # Pass ts_code
            full_update=full_update
        )

        # 汇总结果
        await updater.summarize_result(
            success_count,
            failed_count,
            error_msgs,
            start_date=start_date,
            end_date=end_date,
            ts_code=args.ts_code, # Pass ts_code
            full_update=full_update
        )

    except Exception as e:
        logging.error(f"更新过程发生未知错误: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    # 使用示例：
    # 1. 指定日期范围更新所有股票
    #    python scripts/tasks/stock/update_report_rc.py --start-date 20230101 --end-date 20231231
    #
    # 2. 指定日期范围更新特定股票
    #    python scripts/tasks/stock/update_report_rc.py --start-date 20230101 --end-date 20231231 --ts-code 600000.SH
    #
    # 3. 全量更新所有股票 (从任务默认起始日期开始)
    #    python scripts/tasks/stock/update_report_rc.py --full-update
    #
    # 4. 全量更新特定股票
    #    python scripts/tasks/stock/update_report_rc.py --full-update --ts-code 000001.SZ
    asyncio.run(main()) 