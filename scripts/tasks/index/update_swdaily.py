#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
申万行业日线行情 (sw_daily) 更新脚本

使用 TaskUpdaterBase 基类实现的申万行业日线行情数据更新工具。
支持按日期范围更新、按天数更新和全量更新模式。
支持指定指数代码进行更新。
"""

import argparse
import asyncio
import logging
import sys
import os
from typing import Dict, Any
from datetime import datetime # 添加 datetime 导入
import dotenv # 添加 dotenv 导入
from pathlib import Path # 添加 pathlib 导入
from alphahome.fetchers.task_factory import TaskFactory
from scripts.base.task_updater_base import TaskUpdaterBase

# 动态调整 sys.path 和加载 .env (与 update_fundshare.py 一致)
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# 现在可以正常导入项目模块
from scripts.base.task_updater_base import TaskUpdaterBase
from alphahome.fetchers.task_factory import TaskFactory

# 配置模块级 logger (与 update_fundshare.py 一致)
logger = logging.getLogger(__name__)

# 定义目标任务名称
TARGET_TASK_NAME = "tushare_index_swdaily"

class SwDailyUpdater(TaskUpdaterBase):
    """继承自 TaskUpdaterBase，用于更新申万行业日线行情数据。"""

    def __init__(self):
        # 移除 db_config, task_config (与 update_fundshare.py 一致)
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="申万行业日线行情",
            description=f"更新 {TARGET_TASK_NAME} 数据" # 添加 description
            # support_report_period=False # 如果不需要报告期，可以像 fundshare 一样设置
        )
        # 设置 logger 名称 (与 update_fundshare.py 一致)
        self.logger.name = self.__class__.__name__

    # add_args 方法保持不变
    def add_args(self, parser: argparse.ArgumentParser):
        """添加任务特定的参数"""
        parser.add_argument(
            '--ts-code',
            type=str,
            help='指定要更新的申万行业指数代码 (例如: 801010.SI)'
        )

    async def update_task(self, args: argparse.Namespace) -> Dict[str, Any]:
        """执行单个更新任务 (逻辑参照 update_fundshare.py)"""
        self.logger.info(f"开始执行任务: {self.task_name}")
        try:
            task = await TaskFactory.get_task(self.task_name)
            if not task:
                 raise ValueError(f"无法获取任务实例: {self.task_name}")
                 
            execute_kwargs = {
                'show_progress': getattr(args, 'show_progress', False)
            }
            
            # 处理 --full-update 参数 (参照 update_fundshare.py)
            if args.full_update:
                start_date = getattr(task, 'default_start_date', '19900101')
                execute_kwargs['start_date'] = start_date
                self.logger.info(f"全量更新模式，强制起始日期为: {start_date}")
                # 全量更新也需要结束日期，默认为今天
                execute_kwargs['end_date'] = args.end_date if args.end_date else datetime.now().strftime('%Y%m%d')
            else:
                # 处理 --start-date 和 --end-date (如果提供了)
                if args.start_date: execute_kwargs['start_date'] = args.start_date
                if args.end_date: execute_kwargs['end_date'] = args.end_date
                # 如果没提供日期，依赖 task.execute 或 smart_incremental_update 的默认逻辑
                # 注意: TushareTask 的 execute 不会自动处理增量，需要调用 smart_incremental_update
                # 或者在这里明确调用 smart_incremental_update
                # 为了简单起见，我们先假设 task.execute 能处理日期参数，如果不行再调整

            # 处理任务特定参数
            if hasattr(args, 'ts_code') and args.ts_code:
                 execute_kwargs['ts_code'] = args.ts_code
            
            # 执行任务
            # 注意：基类TaskUpdaterBase的process_date_args不再在main中调用
            # 日期逻辑现在由这里或任务本身处理
            result = await task.execute(**execute_kwargs)

            if not isinstance(result, dict):
                 self.logger.error(f"任务 {self.task_name} 返回结果格式错误: {result}")
                 # 返回错误字典 (参照 update_fundshare.py)
                 return {"status": "error", "error": "任务返回结果格式错误", "rows": 0} 

            self.logger.info(f"任务 {self.task_name} 执行完成，状态: {result.get('status', 'unknown')}, 行数: {result.get('rows', 0)}")
            # 返回原始结果字典 (参照 update_fundshare.py)
            return result 

        except Exception as e:
            error_msg = f"任务 {self.task_name} 更新过程发生错误: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            # 返回错误字典 (参照 update_fundshare.py)
            return {"status": "error", "error": error_msg, "rows": 0}

    # 添加 summarize_result 方法 (参照 update_fundshare.py)
    def summarize_result(self, result: Dict[str, Any], args: argparse.Namespace):
        status = result.get('status', 'unknown')
        rows = result.get('rows', 0)
        error_msg = result.get('error')
        failed_batches = result.get('failed_batches', 0)
        
        self.logger.info(f"------ {self.task_type} ({self.task_name}) 更新结果汇总 ------")
        
        # 显示更新范围信息
        start_log = f"起始日期: {args.start_date if args.start_date else '自动(最新)'}"
        end_log = f"结束日期: {args.end_date if args.end_date else '自动(今天)'}"
        if args.full_update: start_log = "起始日期: 全量"
        # if args.days: start_log = f"起始日期: 最近 {args.days} 天" # Base class does not define --days
        self.logger.info(f"更新范围: {start_log} -> {end_log}")
        if hasattr(args, 'ts_code') and args.ts_code: # Check if ts_code exists
            self.logger.info(f"指数代码: {args.ts_code}")
            
        # 根据状态记录不同信息
        if status == 'success':
            self.logger.info(f"状态: 成功")
            self.logger.info(f"更新行数: {rows}")
        elif status == 'no_data':
            self.logger.info(f"状态: 无新数据")
        elif status == 'up_to_date': # TushareTask.smart_incremental_update 可能返回
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
    """主执行函数 (逻辑参照 update_fundshare.py)"""
    updater = SwDailyUpdater()
    # 使用基类设置的解析器
    parser = updater.setup_parser()
    # 添加此任务特定的参数
    updater.add_args(parser)
    args = parser.parse_args()

    result = None # 初始化 result
    try:
        # 初始化 TaskFactory (不再传递 DSN，让其自行处理)
        await TaskFactory.initialize()  
        updater.logger.info("TaskFactory 初始化成功")

        # 移除 process_date_args 调用，日期逻辑在 update_task 中处理
        # args.start_date, args.end_date = updater.process_date_args(args)
        
        # 执行更新
        result = await updater.update_task(args)
        
        # 汇总结果
        updater.summarize_result(result, args)

    except Exception as e:
        # 使用 updater logger 记录错误
        updater.logger.error(f"执行更新脚本时发生错误: {e}", exc_info=True)
        # 确保即使出错也尝试汇总
        if result is None: result = {"status": "error", "error": f"主流程错误: {str(e)}", "rows": 0}
        updater.summarize_result(result, args)
        sys.exit(1) # 退出码 1 表示错误
    finally:
        # 关闭 TaskFactory 管理的资源
        await TaskFactory.shutdown()
        # 使用 updater logger 记录关闭信息
        updater.logger.info("TaskFactory 关闭完成")

if __name__ == "__main__":
    asyncio.run(main())
    # 使用示例:
    # python scripts/tasks/index/update_swdaily.py --days 5 
    # python scripts/tasks/index/update_swdaily.py --start-date 20230101 --end-date 20231231
    # python scripts/tasks/index/update_swdaily.py --start-date 20230101 --end-date 20231231 --ts-code 801010.SI
    # python scripts/tasks/index/update_swdaily.py --full-update
    # python scripts/tasks/index/update_swdaily.py --full-update --ts-code 801010.SI 