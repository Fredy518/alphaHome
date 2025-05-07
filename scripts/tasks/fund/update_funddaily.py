#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
执行 Tushare 公募基金日线行情 (fund_daily) 更新任务。
支持增量更新。
"""

import asyncio
import sys
import os
import logging
import argparse
from datetime import datetime
from typing import Dict, Any
import dotenv
from pathlib import Path
import pandas as pd

# Setup paths and environment
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# Imports
from alphahome.fetchers.db_manager import DBManager
from alphahome.fetchers.task_factory import TaskFactory
from scripts.base.task_updater_base import TaskUpdaterBase

TARGET_TASK_NAME = "tushare_fund_daily"

class FundDailyUpdater(TaskUpdaterBase):
    """
    公募基金日线行情数据更新工具
    """
    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="公募基金日线行情",
            description="公募基金日线行情数据更新工具"
            # support_report_period=False # This is the only valid optional arg
        )
        self.logger.name = self.__class__.__name__

    async def update_task(self, args: argparse.Namespace) -> Dict[str, Any]:
        self.logger.info(f"开始执行任务: {self.task_name}")
        try:
            task = await TaskFactory.get_task(self.task_name)
            execute_kwargs = {
                'show_progress': getattr(args, 'show_progress', False)
            }
            
            # 处理 --full-update 参数
            if args.full_update:
                start_date = getattr(task, 'default_start_date', '19700101')
                execute_kwargs['start_date'] = start_date
                self.logger.info(f"全量更新模式，强制起始日期为: {start_date}")
                if not args.end_date:
                    execute_kwargs['end_date'] = datetime.now().strftime('%Y%m%d')
                else:
                    execute_kwargs['end_date'] = args.end_date
            else:
                # 处理 --start-date 和 --end-date
                if args.start_date: execute_kwargs['start_date'] = args.start_date
                if args.end_date: execute_kwargs['end_date'] = args.end_date
                # 自动增量逻辑由 task.get_batch_list 处理
                
            # 处理任务特定参数
            if hasattr(args, 'ts_code') and args.ts_code: execute_kwargs['ts_code'] = args.ts_code
            
            result = await task.execute(**execute_kwargs)
            if not isinstance(result, dict):
                self.logger.error(f"任务 {self.task_name} 返回结果格式错误: {result}")
                return {"status": "error", "error": "任务返回结果格式错误", "rows": 0}
            self.logger.info(f"任务 {self.task_name} 执行完成，状态: {result.get('status', 'unknown')}, 行数: {result.get('rows', 0)}")
            return result
        except Exception as e:
            error_msg = f"任务 {self.task_name} 更新过程发生错误: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {"status": "error", "error": error_msg, "rows": 0}

    def summarize_result(self, result: Dict[str, Any], args: argparse.Namespace):
        # (Use the same summary logic as FundNavUpdater)
        status = result.get('status', 'unknown')
        rows = result.get('rows', 0)
        error_msg = result.get('error')
        self.logger.info(f"------ {self.task_type} ({self.task_name}) 更新结果汇总 ------")
        start_log = f"起始日期: {args.start_date if args.start_date else '自动(最新)'}"
        end_log = f"结束日期: {args.end_date if args.end_date else '自动(今天)'}"
        self.logger.info(f"更新范围: {start_log} -> {end_log}")
        if status == 'success':
            self.logger.info(f"状态: 成功")
            self.logger.info(f"更新行数: {rows}")
        elif status == 'no_data':
            self.logger.info(f"状态: 无新数据")
        elif status == 'partial_success':
            self.logger.warning(f"状态: 部分成功, 行数: {rows}")
            if 'failed_batches' in result: self.logger.warning(f"失败批次数: {result['failed_batches']}")
            if error_msg: self.logger.warning(f"相关信息: {error_msg}")
        elif status == 'failure':
            self.logger.error(f"状态: 失败, 行数: {rows}")
            if 'failed_batches' in result: self.logger.error(f"失败批次数: {result['failed_batches']}")
            if error_msg: self.logger.error(f"错误信息: {error_msg}")
            else: self.logger.error("错误信息: 未知错误")
        elif status == 'error':
            self.logger.error(f"状态: 更新流程错误")
            if error_msg: self.logger.error(f"错误信息: {error_msg}")
            else: self.logger.error("错误信息: 未知流程错误")
        else:
            self.logger.warning(f"状态: {status}, 行数: {rows}")
            if error_msg: self.logger.warning(f"相关信息: {error_msg}")
        self.logger.info(f"--------------------------------------------------")

async def main():
    updater = FundDailyUpdater()
    # 使用基类设置的解析器
    parser = updater.setup_parser()
    # 添加此任务特定的参数
    parser.add_argument('--ts-code', type=str, help="指定基金代码 (TS代码)")
    args = parser.parse_args()

    # 不再直接检查环境变量
    # db_dsn = os.environ.get("DATABASE_URL")
    # tushare_token = os.environ.get("TUSHARE_TOKEN")
    # if not tushare_token: logging.error("错误：未设置 TUSHARE_TOKEN"); sys.exit(1)
    # if not db_dsn: logging.error("错误：未设置 DATABASE_URL"); sys.exit(1)

    # db_manager = None
    result = None
    try:
        # db_manager = DBManager(db_dsn)
        # await db_manager.connect()
        await TaskFactory.initialize()  # 自动使用配置/环境变量
        result = await updater.update_task(args)
        updater.summarize_result(result, args)
    except Exception as e:
        updater.logger.error(f"更新主流程发生未知错误: {str(e)}", exc_info=True)
        if result is None: result = {"status": "error", "error": f"主流程错误: {str(e)}", "rows": 0}
        updater.summarize_result(result, args)
        sys.exit(1)
    finally:
        await TaskFactory.shutdown()
        # if db_manager: await db_manager.close(); logging.info("数据库连接已关闭")

if __name__ == "__main__":
    asyncio.run(main()) 