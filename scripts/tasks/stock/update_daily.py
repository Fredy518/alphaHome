#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票日线数据更新脚本

使用 TaskUpdaterBase 基类实现的股票日线数据更新工具。
支持以下功能：
1. 按天数更新数据
2. 支持指定日期范围更新
3. 支持多种更新模式（增量/全量）
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
from alphahome.fetchers.task_factory import TaskFactory
from scripts.base.task_updater_base import TaskUpdaterBase

# 添加项目根目录到系统路径
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
TARGET_TASK_NAME = "tushare_stock_daily"

class DailyUpdater(TaskUpdaterBase):
    """股票日线数据更新器"""
    
    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="股票日线",
            description="股票日线数据更新工具",
            support_report_period=False
        )
        self.logger = logging.getLogger('update_daily')

async def main():
    updater = DailyUpdater()
    
    # ---- 恢复命令行参数解析 ----
    parser = argparse.ArgumentParser(description='Tushare 股票日线数据更新工具 (支持全量和增量)')
    parser.add_argument('--days', type=int, help='增量更新: 更新最近 N 个交易日的数据')
    parser.add_argument('--start-date', help='增量或范围更新: 更新起始日期 (格式: YYYYMMDD)')
    parser.add_argument('--end-date', help='增量或范围更新: 更新结束日期 (格式: YYYYMMDD, 默认为当天)')
    parser.add_argument('--full-update', action='store_true', help='全量更新模式: 从 19901219 (Tushare最早日期) 开始更新')
    parser.add_argument('--auto', action='store_true', help='自动增量模式: 从数据库最新日期的下一个交易日开始更新 (默认模式)')
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')
    args = parser.parse_args()
    # ---- 结束参数解析 ----

    start_time = datetime.now()
    updater.logger.info(f"开始执行 tushare_stock_daily 数据更新...")

    # 初始化 TaskFactory (连接数据库等)
    await TaskFactory.initialize()
    updater.logger.info("TaskFactory 初始化成功")

    task = None # 初始化 task 变量以备 finally 使用
    try:
        task_name = TARGET_TASK_NAME
        task = await TaskFactory.get_task(task_name)
        if not task:
             updater.logger.error(f"任务 '{task_name}' 获取失败。")
             return
        updater.logger.info(f"任务 '{task_name}' 获取成功")

        # 定义要传递给任务的通用参数 (不含 concurrent_limit)
        common_kwargs = {
            'show_progress': args.show_progress
        }

        update_mode_desc = "" # Initialize description
        run_args = {} # Initialize run_args

        # 确定更新模式和日期范围
        if args.full_update:
            update_mode_desc = "全量"
            # Tushare 日线数据最早日期为 19901219
            run_args = {'start_date': '19901219', 'end_date': args.end_date or datetime.now().strftime('%Y%m%d')}
        elif args.start_date:
            update_mode_desc = "指定日期范围"
            run_args = {'start_date': args.start_date, 'end_date': args.end_date or datetime.now().strftime('%Y%m%d')}
        elif args.days:
            update_mode_desc = f"最近 {args.days} 交易日增量"
            run_args = {'update_mode': 'trade_day', 'trade_days_lookback': args.days, 'end_date': args.end_date}
        else: # 默认或明确指定 --auto
            update_mode_desc = "自动增量"
            updater.logger.info("执行自动增量更新，查询数据库最新日期...")
            latest_date = await task.get_latest_date()
            
            if latest_date:
                # 从最新日期的下一天开始
                start_date_obj = latest_date + timedelta(days=1)
                start_date_str = start_date_obj.strftime('%Y%m%d')
                updater.logger.info(f"数据库最新日期为: {latest_date.strftime('%Y%m%d')}, 更新将从 {start_date_str} 开始")
            else:
                # 如果没有数据或表不存在，执行首次全量更新（从最早日期开始）
                start_date_str = '19901219' # Tushare 股票日线最早日期
                updater.logger.info(f"未找到现有数据，将从最早日期 {start_date_str} 开始更新")
            
            end_date_str = args.end_date or datetime.now().strftime('%Y%m%d')
            run_args = {'start_date': start_date_str, 'end_date': end_date_str}
            
        updater.logger.info(f"模式: {update_mode_desc} 更新，日期范围: {run_args.get('start_date', 'N/A')} 到 {run_args.get('end_date', 'N/A')}")
        
        # 确保 run_args 包含必要参数
        if 'start_date' not in run_args and 'trade_days_lookback' not in run_args:
            updater.logger.error("更新参数错误：必须提供 start_date 或 trade_days_lookback")
            return
            
        # 将 update_mode 添加回 run_args，因为它可能在 --days 模式下被覆盖
        if 'update_mode' not in run_args:
            run_args['update_mode'] = 'trade_day' # 默认按交易日
    
        result = await task.execute(**run_args, **common_kwargs) # 调用 task 实例的 execute
        updater.logger.info(f"{update_mode_desc} 更新结果: {result}")

    except Exception as e:
        task_name_log = task.name if task else task_name # 记录任务名
        updater.logger.error(f"任务 '{task_name_log}' 执行过程中发生错误: {str(e)}", exc_info=True)
    finally:
        # 关闭 TaskFactory (断开数据库连接等)
        await TaskFactory.shutdown()
        updater.logger.info("TaskFactory 已关闭")

    end_time = datetime.now()
    duration = end_time - start_time
    updater.logger.info(f"tushare_stock_daily 数据更新执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    asyncio.run(main()) 