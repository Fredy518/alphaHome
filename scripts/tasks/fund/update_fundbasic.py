#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
执行 Tushare 公募基金列表全量更新任务。
借鉴 update_indexbasic.py 的模式。
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
from alphahome.fetchers import DBManager, TaskFactory

# 将项目根目录添加到 Python 路径
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# 配置基础日志
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_filename = f"update_fundbasic_{datetime.now().strftime('%Y%m%d')}.log"
log_filepath = os.path.join(log_dir, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath, encoding='utf-8'),
        logging.StreamHandler(sys.stdout) # 同时输出到控制台
    ]
)

# 导入必要的模块 (确保在添加 sys.path 之后)
from scripts.base.task_updater_base import TaskUpdaterBase
# from alphahome.data_module.tasks.fund.tushare_fund_basic import TushareFundBasicTask # TaskFactory 会处理加载

# 定义目标任务名称常量
TARGET_TASK_NAME = "tushare_fund_basic"

class FundBasicUpdater(TaskUpdaterBase):
    """
    公募基金列表数据更新工具 (全量替换)
    """

    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="公募基金列表",
            description="公募基金列表全量更新工具"
            # support_report_period=False (default is False)
        )
        self.logger.name = self.__class__.__name__

    async def update_task(self, args: argparse.Namespace) -> Dict[str, Any]:
        """
        执行公募基金列表的全量更新任务。
        """
        self.logger.info(f"开始执行任务: {self.task_name}")
        try:
            task = await TaskFactory.get_task(self.task_name)
            execute_kwargs = {
                'show_progress': getattr(args, 'show_progress', False)
            }
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
        汇总全量更新结果。
        """
        status = result.get('status', 'unknown')
        rows = result.get('rows', 0)
        error_msg = result.get('error')

        self.logger.info(f"------ {self.task_type} ({self.task_name}) 更新结果汇总 ------")
        self.logger.info(f"更新模式: 全量替换 (任务内置逻辑)")

        if status == 'success':
            self.logger.info(f"状态: 成功")
            self.logger.info(f"更新行数: {rows}")
        elif status == 'no_data':
            self.logger.info(f"状态: 无数据")
            self.logger.info(f"说明: Tushare API 未返回任何数据。")
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
    updater = FundBasicUpdater()
    parser = argparse.ArgumentParser(description=f"更新 {updater.task_type} ({updater.task_name}) 数据")
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help="显示进度条")
    args = parser.parse_args()

    db_dsn = os.environ.get("DATABASE_URL", "postgresql://postgres:wuhao123@localhost:5432/tusharedb")
    tushare_token = os.environ.get("TUSHARE_TOKEN")

    if not tushare_token:
        logging.error("错误：未设置 TUSHARE_TOKEN 环境变量")
        sys.exit(1)
    if not db_dsn:
        logging.error("错误：未设置 DATABASE_URL 环境变量或提供默认值")
        sys.exit(1)

    result = None
    try:
        # 使用TaskFactory初始化数据库连接，传入数据库连接字符串
        await TaskFactory.initialize(db_dsn)
        # 执行任务
        result = await updater.update_task(args)
        updater.summarize_result(result, args)
    except Exception as e:
        logging.error(f"更新主流程发生未知错误: {str(e)}", exc_info=True)
        if result is None:
            result = {"status": "error", "error": f"主流程错误: {str(e)}", "rows": 0}
        updater.summarize_result(result, args)
        sys.exit(1)
    finally:
        # 关闭TaskFactory管理的数据库连接
        await TaskFactory.shutdown()
        logging.info("数据库连接已关闭")

if __name__ == "__main__":
    asyncio.run(main()) 