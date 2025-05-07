#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中信行业指数日线行情 (ci_daily) 更新脚本

使用 TaskUpdaterBase 基类实现的中信行业指数日线行情数据更新工具。
支持按日期范围更新、按天数更新和全量更新模式。
支持指定指数代码进行更新。
"""

import argparse
import asyncio
import logging
import sys
import os
from typing import Dict, Any
from datetime import datetime
import dotenv
from pathlib import Path
from alphahome.fetchers.task_factory import TaskFactory
from scripts.base.task_updater_base import TaskUpdaterBase

# 动态调整 sys.path 和加载 .env
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# 现在可以正常导入项目模块
from scripts.base.task_updater_base import TaskUpdaterBase
from alphahome.fetchers.task_factory import TaskFactory

# 配置模块级 logger
logger = logging.getLogger(__name__)

# 定义目标任务名称
TARGET_TASK_NAME = "tushare_index_cidaily"

class CiDailyUpdater(TaskUpdaterBase):
    """继承自 TaskUpdaterBase，用于更新中信行业指数日线行情数据。"""

    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="中信行业日线行情",
            description=f"更新 {TARGET_TASK_NAME} 数据"
        )
        self.logger.name = self.__class__.__name__

    def add_args(self, parser: argparse.ArgumentParser):
        """添加任务特定的参数"""
        # 调用父类方法添加通用日期参数，父类 setup_parser 已处理
        parser.add_argument(
            '--ts-code',
            type=str,
            help='指定要更新的中信行业指数代码 (例如: CI005001.CI)'
        )

    async def update_task(self, args: argparse.Namespace) -> Dict[str, Any]:
        """执行单个更新任务 (逻辑参照 update_swdaily.py)"""
        self.logger.info(f"开始执行任务: {self.task_name}")
        try:
            task = await TaskFactory.get_task(self.task_name)
            if not task:
                 raise ValueError(f"无法获取任务实例: {self.task_name}")
                 
            execute_kwargs = {
                'show_progress': getattr(args, 'show_progress', False)
            }
            
            if args.full_update:
                start_date = getattr(task, 'default_start_date', '19900101')
                execute_kwargs['start_date'] = start_date
                self.logger.info(f"全量更新模式，强制起始日期为: {start_date}")
                execute_kwargs['end_date'] = args.end_date if args.end_date else datetime.now().strftime('%Y%m%d')
            else:
                if args.start_date: execute_kwargs['start_date'] = args.start_date
                if args.end_date: execute_kwargs['end_date'] = args.end_date

            if hasattr(args, 'ts_code') and args.ts_code:
                 execute_kwargs['ts_code'] = args.ts_code
            
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
        """汇总任务结果 (与 update_swdaily.py 逻辑一致)"""
        status = result.get('status', 'unknown')
        rows = result.get('rows', 0)
        error_msg = result.get('error')
        failed_batches = result.get('failed_batches', 0)
        
        self.logger.info(f"------ {self.task_type} ({self.task_name}) 更新结果汇总 ------")
        
        start_log = f"起始日期: {args.start_date if args.start_date else '自动(最新)'}"
        end_log = f"结束日期: {args.end_date if args.end_date else '自动(今天)'}"
        if args.full_update: start_log = "起始日期: 全量"
        self.logger.info(f"更新范围: {start_log} -> {end_log}")
        if hasattr(args, 'ts_code') and args.ts_code:
            self.logger.info(f"指数代码: {args.ts_code}")
            
        if status == 'success':
            self.logger.info(f"状态: 成功")
            self.logger.info(f"更新行数: {rows}")
        elif status == 'no_data':
            self.logger.info(f"状态: 无新数据")
        elif status == 'up_to_date':
            self.logger.info(f"状态: 数据已是最新")
        elif status == 'partial_success':
            self.logger.warning(f"状态: 部分成功, 行数: {rows}")
            if failed_batches > 0: self.logger.warning(f"失败批次数: {failed_batches}")
            if error_msg: self.logger.warning(f"相关信息: {error_msg}")
        elif status == 'failure':
            self.logger.error(f"状态: 失败, 行数: {rows}")
            if failed_batches > 0: self.logger.error(f"失败批次数: {failed_batches}")
            if error_msg: self.logger.error(f"错误信息: {error_msg}")
            else: self.logger.error("错误信息: 未知错误 (任务内部失败)")
        elif status == 'error':
            self.logger.error(f"状态: 更新流程错误")
            if error_msg: self.logger.error(f"错误信息: {error_msg}")
            else: self.logger.error("错误信息: 未知流程错误")
        else:
            self.logger.warning(f"状态: {status}, 行数: {rows}")
            if error_msg: self.logger.warning(f"相关信息: {error_msg}")
            
        self.logger.info(f"--------------------------------------------------")

async def main():
    """主执行函数 (与 update_swdaily.py 逻辑一致)"""
    updater = CiDailyUpdater()
    parser = updater.setup_parser() # 使用基类设置解析器
    updater.add_args(parser) # 添加特定参数
    args = parser.parse_args()

    result = None
    try:
        await TaskFactory.initialize()
        updater.logger.info("TaskFactory 初始化成功")
        result = await updater.update_task(args)
        updater.summarize_result(result, args)
    except Exception as e:
        updater.logger.error(f"执行更新脚本时发生错误: {e}", exc_info=True)
        if result is None: result = {"status": "error", "error": f"主流程错误: {str(e)}", "rows": 0}
        updater.summarize_result(result, args)
        sys.exit(1)
    finally:
        await TaskFactory.shutdown()
        updater.logger.info("TaskFactory 关闭完成")

if __name__ == "__main__":
    asyncio.run(main())
    # 使用示例:
    # python scripts/tasks/index/update_cidaily.py --days 5
    # python scripts/tasks/index/update_cidaily.py --start-date 20230101 --end-date 20231231
    # python scripts/tasks/index/update_cidaily.py --start-date 20230101 --end-date 20231231 --ts-code CI005001.CI
    # python scripts/tasks/index/update_cidaily.py --full-update
    # python scripts/tasks/index/update_cidaily.py --full-update --ts-code CI005001.CI 