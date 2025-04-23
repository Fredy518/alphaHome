#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
运行中信(CITIC)行业成分 (TushareIndexCiMemberTask) 更新任务的脚本。

该任务获取最新和历史成分数据，并使用 UPSERT 方式更新数据库。

使用方法:
python scripts/tasks/index/run_tushare_index_cimember.py [--show-progress]
"""

import os
import sys
import asyncio
import logging
import argparse
from typing import Dict, Any
import dotenv
from pathlib import Path

# 添加项目根目录到系统路径
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

from scripts.base.task_updater_base import TaskUpdaterBase
from data_module.task_factory import TaskFactory

# 配置基本日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 定义目标任务名称常量
TARGET_TASK_NAME = "tushare_index_cimember"

class IndexCiMemberUpdater(TaskUpdaterBase):
    """
    中信(CITIC)行业成分数据更新工具 (UPSERT)
    """

    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="中信行业成分",
            description="中信(CITIC)行业成分更新工具 (含历史, UPSERT)",
            support_report_period=False
        )
        self.logger.name = self.__class__.__name__

    async def update_task(self, args: argparse.Namespace) -> Dict[str, Any]:
        """执行任务更新"""
        self.logger.info(f"开始执行任务: {self.task_name} (UPSERT 模式)")
        try:
            task = await TaskFactory.get_task(self.task_name)
            execute_kwargs = {'show_progress': args.show_progress}
            result = await task.execute(**execute_kwargs)
            if not isinstance(result, dict):
                self.logger.error(f"任务 {self.task_name} 返回结果格式错误: {result}")
                return {"status": "error", "error": "任务返回结果格式错误", "rows": 0}
            self.logger.info(f"任务 {self.task_name} 执行完成，状态: {result.get('status', 'unknown')}, 行数: {result.get('rows', 0)}, 失败批次: {result.get('failed_batches', 0)}")
            return result
        except Exception as e:
            error_msg = f"任务 {self.task_name} 更新过程发生错误: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {"status": "error", "error": error_msg, "rows": 0}

    def summarize_result(self, result: Dict[str, Any], args: argparse.Namespace):
        """汇总并打印更新结果"""
        status = result.get('status', 'unknown')
        rows = result.get('rows', 0)
        failed_batches = result.get('failed_batches', 0)
        error_msg = result.get('error')
        
        self.logger.info(f"------ {self.task_type} ({self.task_name}) 更新结果汇总 ------")
        self.logger.info(f"更新模式: UPSERT (获取最新 Y 和历史 N 数据)")
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
            if error_msg:
                self.logger.error(f"错误信息: {error_msg}")
            else:
                self.logger.error("错误信息: 未知错误")
        else:
            self.logger.warning(f"状态: {status} (未知或非标准)")
            if error_msg:
                self.logger.warning(f"相关信息: {error_msg}")
        self.logger.info(f"--------------------------------------------------")

async def main():
    updater = IndexCiMemberUpdater()

    parser = argparse.ArgumentParser(description=f"更新 {updater.task_type} ({updater.task_name}) 数据 (UPSERT)")
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
    # python scripts/tasks/index/run_tushare_index_cimember.py
    # python scripts/tasks/index/run_tushare_index_cimember.py --no-show-progress
    asyncio.run(main()) 