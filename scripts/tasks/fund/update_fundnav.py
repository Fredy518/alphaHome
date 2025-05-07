#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
执行 Tushare 公募基金净值 (fund_nav) 更新任务。
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
from alphahome.fetchers.task_factory import TaskFactory
from scripts.base.task_updater_base import TaskUpdaterBase

# 将项目根目录添加到 Python 路径
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# 配置基础日志
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)
# 使用任务名作为日志文件名基础
log_filename = f"update_fundnav_{datetime.now().strftime('%Y%m%d')}.log"
log_filepath = os.path.join(log_dir, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# 定义目标任务名称常量
TARGET_TASK_NAME = "tushare_fund_nav"

class FundNavUpdater(TaskUpdaterBase):
    """
    公募基金净值数据更新工具
    """

    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="公募基金净值",
            description="公募基金净值数据更新工具"
            # support_report_period=False # This is the only valid optional arg
        )
        self.logger.name = self.__class__.__name__

    async def update_task(self, args: argparse.Namespace) -> Dict[str, Any]:
        """
        执行基金净值更新任务。
        """
        self.logger.info(f"开始执行任务: {self.task_name}")
        try:
            task = await TaskFactory.get_task(self.task_name)
            execute_kwargs = {
                'show_progress': getattr(args, 'show_progress', False)
            }

            # 处理 --full-update 参数
            if args.full_update:
                # 获取任务的默认开始日期，如果没有定义，则使用一个非常早的日期
                start_date = getattr(task, 'default_start_date', '19700101')
                execute_kwargs['start_date'] = start_date
                self.logger.info(f"全量更新模式，强制起始日期为: {start_date}")
                # 全量更新通常也需要结束日期，默认为今天
                if not args.end_date:
                    execute_kwargs['end_date'] = datetime.now().strftime('%Y%m%d')
                else:
                    execute_kwargs['end_date'] = args.end_date
            else:
                # 传递日期参数 (如果用户在命令行提供)
                if args.start_date:
                    execute_kwargs['start_date'] = args.start_date
                if args.end_date:
                    execute_kwargs['end_date'] = args.end_date
                # 如果用户未提供 start_date，任务的 get_batch_list 会自动处理增量逻辑
            
            # 传递其他可能的任务特定参数
            if hasattr(args, 'ts_code') and args.ts_code:
                execute_kwargs['ts_code'] = args.ts_code
            # if hasattr(args, 'market') and args.market: # 底层任务不支持 market 参数
            #     execute_kwargs['market'] = args.market

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
        """
        汇总增量更新结果。
        """
        # (与 FundBasicUpdater 的 summarize_result 类似)
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
            self.logger.info(f"说明: 在指定日期范围内没有需要更新的数据。")
        elif status == 'partial_success':
            self.logger.warning(f"状态: 部分成功")
            self.logger.info(f"更新行数: {rows}")
            if 'failed_batches' in result:
                self.logger.warning(f"失败批次数: {result['failed_batches']}")
            if error_msg:
                self.logger.warning(f"相关信息: {error_msg}")
        elif status == 'failure':
            self.logger.error(f"状态: 失败")
            self.logger.info(f"更新行数: {rows}")
            if 'failed_batches' in result:
                self.logger.error(f"失败批次数: {result['failed_batches']}")
            if error_msg:
                self.logger.error(f"错误信息: {error_msg}")
            else:
                self.logger.error("错误信息: 未知错误")
        elif status == 'error':
            self.logger.error(f"状态: 更新流程错误")
            if error_msg:
                self.logger.error(f"错误信息: {error_msg}")
            else:
                self.logger.error("错误信息: 未知流程错误")
        else:
            self.logger.warning(f"状态: {status}")
            self.logger.info(f"更新行数: {rows}")
            if error_msg:
                self.logger.warning(f"相关信息: {error_msg}")
        self.logger.info(f"--------------------------------------------------")


async def main():
    updater = FundNavUpdater()
    # 使用基类设置的解析器
    parser = updater.setup_parser() 
    # 添加此任务特定的参数
    parser.add_argument('--ts-code', type=str, help="指定基金代码 (TS代码)")
    parser.add_argument('--market', type=str, choices=['E', 'O'], help="指定市场 (E场内/O场外)")
    
    args = parser.parse_args()

    # 获取 DSN，优先环境变量，否则使用默认值
    # db_dsn = os.environ.get("DATABASE_URL", "postgresql://postgres:wuhao123@localhost:5432/tusharedb")
    # tushare_token = os.environ.get("TUSHARE_TOKEN")

    # if not tushare_token:
    #     logging.error("错误：未设置 TUSHARE_TOKEN 环境变量")
    #     sys.exit(1)
    # 不再需要检查 db_dsn 是否为空，因为有了默认值
    # if not db_dsn:
    #     logging.error("错误：未设置 DATABASE_URL 环境变量或提供默认值")
    #     sys.exit(1)

    # db_manager = None # No longer needed
    result = None
    try:
        # db_manager = DBManager(db_dsn)
        # await db_manager.connect()
        await TaskFactory.initialize() # 自动使用配置/环境变量
        result = await updater.update_task(args)
        updater.summarize_result(result, args)
    except Exception as e:
        logging.error(f"更新主流程发生未知错误: {str(e)}", exc_info=True)
        if result is None:
            result = {"status": "error", "error": f"主流程错误: {str(e)}", "rows": 0}
        # Ensure summary is called even on error
        updater.summarize_result(result if result else {"status": "error", "error": f"主流程错误: {str(e)}", "rows": 0}, args)
        sys.exit(1)
    finally:
        await TaskFactory.shutdown()
        # if db_manager: # No longer needed
        #     await db_manager.close()
        #     logging.info("数据库连接已关闭")

if __name__ == "__main__":
    asyncio.run(main()) 