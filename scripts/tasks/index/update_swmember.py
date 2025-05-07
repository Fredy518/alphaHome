#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
运行申万行业成分 (TushareIndexSwmemberTask) 更新任务的脚本。

默认获取所有最新的行业成分 (is_new='Y') 并进行 UPSERT 更新。
命名遵循 tushare_index_swmember 模式。

使用方法:
python scripts/tasks/index/run_tushare_index_swmember.py [--full-update]

说明:
- 此脚本用于启动 'tushare_index_swmember' 任务。
- 该任务使用 UPSERT 方式更新数据，不是全量替换。
- --full-update 参数主要影响日志记录，指示这是一次获取全量最新数据的尝试。
"""

import os
import sys
import asyncio
import logging
import argparse
from typing import Dict, Any
import dotenv
from pathlib import Path
import pandas as pd
from alphahome.fetchers.task_factory import TaskFactory
from scripts.base.task_updater_base import TaskUpdaterBase

# 添加项目根目录到系统路径
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# 配置基本日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 定义目标任务名称常量
TARGET_TASK_NAME = "tushare_index_swmember"

class IndexSwmemberUpdater(TaskUpdaterBase):
    """
    申万(SW)行业成分数据更新工具

    继承自 TaskUpdaterBase，用于更新申万行业成分数据。
    """

    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="申万行业成分",
            description="申万(SW)行业成分更新工具 (获取最新)",
            support_report_period=False
        )
        self.logger.name = self.__class__.__name__

    async def update_task(self, args: argparse.Namespace) -> Dict[str, Any]:
        self.logger.info(f"开始执行任务: {self.task_name}")
        try:
            task = await TaskFactory.get_task(self.task_name)
            execute_kwargs = {}
            if hasattr(args, 'show_progress') and args.show_progress is not None:
                 execute_kwargs['show_progress'] = args.show_progress
            else:
                 execute_kwargs['show_progress'] = True
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
        status = result.get('status', 'unknown')
        rows = result.get('rows', 0)
        error_msg = result.get('error')
        failed_batches = result.get('failed_batches', 0)
        self.logger.info(f"------ {self.task_type} ({self.task_name}) 更新结果汇总 ------")
        if hasattr(args, 'full_update') and args.full_update:
            self.logger.info(f"更新模式: 获取全量最新成分 (is_new='Y') 进行 Upsert (指定了 --full-update 标志)")
        else:
            self.logger.info(f"更新模式: 获取全量最新成分 (is_new='Y') 进行 Upsert")
        if status == 'success':
            self.logger.info(f"状态: 成功")
            self.logger.info(f"更新/插入行数: {rows}")
        elif status == 'partial_success':
            self.logger.warning(f"状态: 部分成功")
            self.logger.info(f"更新/插入行数: {rows}")
            self.logger.warning(f"失败批次数: {failed_batches}")
            if error_msg:
                 self.logger.warning(f"相关信息: {error_msg}")
        elif status == 'no_data':
             self.logger.info(f"状态: 无数据")
             self.logger.info(f"说明: Tushare API 未返回任何数据。")
        elif status == 'failure':
            self.logger.error(f"状态: 完全失败")
            self.logger.info(f"更新/插入行数: {rows}")
            if error_msg:
                self.logger.error(f"错误信息: {error_msg}")
            else:
                self.logger.error("错误信息: 未知错误")
        else:
            self.logger.warning(f"状态: {status} (未知或非标准)")
            self.logger.info(f"更新/插入行数: {rows}")
            if error_msg:
                self.logger.warning(f"相关信息: {error_msg}")
        self.logger.info(f"--------------------------------------------------")

async def main():
    updater = IndexSwmemberUpdater()

    parser = argparse.ArgumentParser(description=f"更新 {updater.task_type} ({updater.task_name}) 数据")
    parser.add_argument("--full-update", action="store_true", help="执行一次最新的全量成分获取和更新")
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help="显示进度条")

    args = parser.parse_args()

    await TaskFactory.initialize()
    
    result = None
    try:
        result = await updater.update_task(args)
        updater.summarize_result(result, args)

    except Exception as e:
        logging.error(f"更新主流程发生未知错误: {str(e)}", exc_info=True)
        if result is None:
            result = {"status": "error", "error": f"主流程错误: {str(e)}", "rows": 0}
        updater.summarize_result(result, args)
        sys.exit(1)
    finally:
        await TaskFactory.shutdown()

if __name__ == "__main__":
    # 使用示例：
    # python scripts/tasks/index/run_tushare_index_swmember.py
    # python scripts/tasks/index/run_tushare_index_swmember.py --full-update
    asyncio.run(main()) 