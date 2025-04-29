#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
业绩预告数据更新脚本

使用 TaskUpdaterBase 基类实现的业绩预告数据更新工具。
支持以下功能：
1. 按季度或年度更新数据
2. 支持指定报告期更新
3. 支持多种更新模式（增量/全量）
4. 详细的日志记录和结果汇总
"""

import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime, timedelta
import calendar # Keep for calculate_report_dates
import dotenv
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import pandas as pd
import numpy as np

# 添加项目根目录到系统路径
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

from alphahome.data_module.task_factory import TaskFactory
from alphahome.data_module.tools.calendar import get_last_trade_day, get_trade_cal
from scripts.base.task_updater_base import TaskUpdaterBase

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 定义目标任务名称常量
TARGET_TASK_NAME = "tushare_fina_forecast"

class ForecastUpdater(TaskUpdaterBase):
    """业绩预告数据更新器"""
    
    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="业绩预告",
            description="业绩预告数据更新工具",
            support_report_period=True
        )
        self.logger = logging.getLogger('update_forecast')
    
    def calculate_report_dates(self, args) -> Tuple[str, str]:
        """计算基于季度或年度选项的报告期范围"""
        today = datetime.now()
        end_date = args.end_date or today.strftime('%Y%m%d')

        if args.report_period:
            period = args.report_period
            if len(period) != 6:
                raise ValueError("报告期格式应为YYYYMM")
            year, month = int(period[:4]), int(period[4:])
            if month not in [3, 6, 9, 12]:
                raise ValueError("报告期月份只能是3、6、9、12")
            _, last_day = calendar.monthrange(year, month)
            report_date = f"{year}{month:02d}{last_day:02d}"
            return report_date, report_date
        
        elif args.quarters:
            quarters = args.quarters
            current_month = today.month
            current_quarter_month = ((current_month - 1) // 3 + 1) * 3
            current_year = today.year
            
            _, last_day = calendar.monthrange(current_year, current_quarter_month)
            current_quarter_end = datetime(current_year, current_quarter_month, last_day)
            
            if today > current_quarter_end:
                latest_quarter_year, latest_quarter_month = current_year, current_quarter_month
            else:
                if current_quarter_month == 3:
                    latest_quarter_year, latest_quarter_month = current_year - 1, 12
                else:
                    latest_quarter_year, latest_quarter_month = current_year, current_quarter_month - 3
            
            for _ in range(quarters - 1):
                if latest_quarter_month == 3:
                    latest_quarter_year, latest_quarter_month = latest_quarter_year - 1, 12
                else:
                    latest_quarter_month -= 3
            
            start_date = f"{latest_quarter_year}{latest_quarter_month:02d}01"
            return start_date, end_date
        
        elif args.years:
            years = args.years
            start_year = today.year - years + 1
            start_date = f"{start_year}0101"
            return start_date, end_date
        
        else:
            # 默认增量更新通常基于数据库最新日期，但这里回溯1年作为默认范围
            one_year_ago = (today - timedelta(days=365)).strftime('%Y%m%d')
            return one_year_ago, end_date
    
    async def update_task(self, args) -> Dict[str, Any]:
        """实现具体的更新逻辑"""
        task = await TaskFactory.get_task(self.task_name)
        if task is None:
            self.logger.error(f"无法获取任务实例: {self.task_name}")
            return {
                'task_name': self.task_name,
                'status': 'error',
                'error': f"无法获取任务实例: {self.task_name}",
                'rows': 0,
                'failed_batches': 0
            }
        
        update_kwargs = {'show_progress': args.show_progress}
        
        if args.full_update:
            start_date = getattr(task, 'default_start_date', '19900101')
            end_date = datetime.now().strftime('%Y%m%d')
            self.logger.info(f"执行全量更新，从 {start_date} 到 {end_date}")
            return await task.execute(start_date=start_date, end_date=end_date, **update_kwargs)
        
        elif args.report_period or args.quarters or args.years:
            start_date, end_date = self.calculate_report_dates(args)
            self.logger.info(f"执行财务报告期更新，从 {start_date} 到 {end_date}")
            return await task.execute(start_date=start_date, end_date=end_date, **update_kwargs)
        
        elif args.start_date:
            end_date = args.end_date or datetime.now().strftime('%Y%m%d')
            self.logger.info(f"执行日期范围更新，从 {args.start_date} 到 {end_date}")
            return await task.execute(start_date=args.start_date, end_date=end_date, **update_kwargs)
        
        else:
            self.logger.info("执行智能增量更新，回溯1年")
            return await task.smart_incremental_update(lookback_days=365, **update_kwargs)

    def format_report_period(self, period: Optional[str]) -> str:
        """格式化报告期为易读格式
        Args:
            period: 报告期, 如 '202103'
        Returns:
            str: 格式化后的报告期，如 '2021年Q1'
        """
        if not period:
            return "未指定"
        if len(period) != 6:
            return period
        year, month = period[:4], int(period[4:])
        quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
        quarter = quarter_map.get(month, f"{month}月")
        return f"{year}年{quarter}"
    
    def summarize_result(self, result: Dict[str, Any], args: argparse.Namespace) -> None:
        """生成业绩预告数据更新的汇总报告
        Args:
            result: 更新结果字典
            args: 命令行参数
        """
        self.logger.info("业绩预告数据更新结果汇总:")
        if not isinstance(result, dict): 
            self.logger.error(f"结果格式错误: {result}")
            return

        # 显示更新参数
        if args.report_period: 
            self.logger.info(f"查询公告日期对应的报告期: {self.format_report_period(args.report_period)}")
        elif args.quarters: 
            self.logger.info(f"查询最近 {args.quarters} 个季度的公告")
        elif args.years: 
            self.logger.info(f"查询最近 {args.years} 年的公告")
        elif args.start_date: 
            self.logger.info(f"查询公告日期范围: {args.start_date} 至 {args.end_date or datetime.now().strftime('%Y%m%d')}")
        elif args.full_update: 
            self.logger.info("更新模式: 全量更新 (基于公告日期)")
        else: 
            self.logger.info("更新模式: 智能增量更新 (默认回溯1年，基于公告日期)")

        task_name = result.get('task_name', self.task_name)
        task_type = self.task_type
        status = result.get('status', 'unknown')
        rows = result.get('rows', 0)
        failed_batches = result.get('failed_batches', 0)

        if status == 'success': 
            self.logger.info(f"- {task_type} ({task_name}): 成功, 更新 {rows} 行数据")
        elif status == 'partial_success': 
            self.logger.info(f"- {task_type} ({task_name}): 部分成功, 更新 {rows} 行数据, {failed_batches} 个批次失败")
        elif status == 'no_data': 
            self.logger.info(f"- {task_type} ({task_name}): 没有需要更新的数据")
        else: 
            error = result.get('error', 'unknown error')
            self.logger.error(f"- {task_type} ({task_name}): 失败, 错误: {error}")

        self.logger.info(f"总计更新: {rows} 行 {task_type} 数据")

async def main():
    parser = argparse.ArgumentParser(description='业绩预告数据更新工具')
    parser.add_argument('--quarters', type=int, help='更新最近 N 个季度的数据')
    parser.add_argument('--years', type=int, help='更新最近 N 年的数据')
    parser.add_argument('--report-period', help='指定报告期 (格式: YYYYMM)')
    parser.add_argument('--start-date', help='更新起始日期 (格式: YYYYMMDD)')
    parser.add_argument('--end-date', help='更新结束日期 (格式: YYYYMMDD)')
    parser.add_argument('--full-update', action='store_true', help='全量更新模式')
    parser.add_argument('--auto', action='store_true', help='自动增量模式')
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')
    args = parser.parse_args()

    updater = ForecastUpdater()
    start_time = datetime.now()
    updater.logger.info(f"开始执行业绩预告 ({updater.task_name}) 数据更新...")

    await TaskFactory.initialize()
    updater.logger.info("TaskFactory初始化成功")

    result = None
    try:
        result = await updater.update_task(args)
        updater.summarize_result(result, args)
    except Exception as e:
        updater.logger.error(f"业绩预告数据更新过程中发生错误: {str(e)}", exc_info=True)
        if result is None:
            updater.summarize_result(
                {
                    'task_name': updater.task_name,
                    'status': 'error',
                    'error': f"更新主流程异常: {str(e)}",
                    'rows': 0,
                    'failed_batches': 0
                },
                args
            )
    finally:
        await TaskFactory.shutdown()
        updater.logger.info("TaskFactory已关闭")

    end_time = datetime.now()
    duration = end_time - start_time
    updater.logger.info(f"业绩预告数据更新执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    """
    使用示例:
    1. 默认增量更新（智能模式，回溯1年）:
       python scripts/update_forecast_task.py
    2. 更新最近N个季度:
       python scripts/update_forecast_task.py --quarters 4
    3. 更新最近N年:
       python scripts/update_forecast_task.py --years 2
    4. 更新特定报告期:
       python scripts/update_forecast_task.py --report-period 202103
    5. 指定日期范围更新:
       python scripts/update_forecast_task.py --start-date 20230101 --end-date 20230630
    6. 全量更新:
       python scripts/update_forecast_task.py --full-update
    """
    asyncio.run(main()) 