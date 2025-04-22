#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
业绩快报数据更新脚本

专门用于更新股票业绩快报数据。
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
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

from data_module.task_factory import TaskFactory
from data_module.tools.calendar import get_last_trade_day, get_trade_cal

# 配置日志
log_filename = f'update_express_task_{datetime.now().strftime("%Y%m%d")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename)
    ]
)

logger = logging.getLogger('update_express_task')

# 定义目标任务
TARGET_TASK_NAME = "tushare_stock_express" # 业绩快报

def calculate_report_dates(args: argparse.Namespace) -> Tuple[str, str]:
    """计算基于季度或年度选项的报告期范围 (与利润表/现金流量表脚本相同)
    Args:
        args: 命令行参数
    Returns:
        Tuple[str, str]: 开始日期和结束日期，格式为YYYYMMDD
    """
    # Logic is identical to the previous scripts for calculate_report_dates
    today = datetime.now()
    end_date = args.end_date or today.strftime('%Y%m%d')

    if args.report_period:
        period = args.report_period
        if len(period) != 6: raise ValueError("报告期格式应为YYYYMM")
        year, month = int(period[:4]), int(period[4:])
        if month not in [3, 6, 9, 12]: raise ValueError("报告期月份只能是3、6、9、12")
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
            if current_quarter_month == 3: latest_quarter_year, latest_quarter_month = current_year - 1, 12
            else: latest_quarter_year, latest_quarter_month = current_year, current_quarter_month - 3
        for _ in range(quarters - 1):
            if latest_quarter_month == 3: latest_quarter_year, latest_quarter_month = latest_quarter_year - 1, 12
            else: latest_quarter_month -= 3
        start_date = f"{latest_quarter_year}{latest_quarter_month:02d}01"
        return start_date, end_date
    elif args.years:
        years = args.years
        start_year = today.year - years + 1
        start_date = f"{start_year}0101"
        return start_date, end_date
    else:
        one_year_ago = (today - timedelta(days=365)).strftime('%Y%m%d')
        return one_year_ago, end_date

async def update_task(args: argparse.Namespace) -> Dict[str, Any]:
    """更新业绩快报任务的数据 (逻辑与之前脚本类似)
    Args:
        args: 命令行参数
    Returns:
        Dict: 更新结果
    """
    task_name = TARGET_TASK_NAME
    try:
        task = await TaskFactory.get_task(task_name)
        if task is None:
            logger.error(f"无法获取任务实例: {task_name}")
            return {'task_name': task_name, 'status': 'error', 'error': f"无法获取任务实例: {task_name}", 'rows': 0, 'failed_batches': 0}

        logger.info(f"开始更新业绩快报任务: {task_name}")
        update_kwargs = {'show_progress': args.show_progress}
        result = None

        # Determine start/end dates based on args (same logic as previous scripts)
        if args.full_update:
            start_date = task.default_start_date if hasattr(task, 'default_start_date') else '19900101'
            end_date = datetime.now().strftime('%Y%m%d')
            logger.info(f"任务 {task_name}: 执行全量更新，从 {start_date} 到 {end_date}")
            # Note: Express/Forecast might use different date parameters in execute
            # Assuming 'start_date' and 'end_date' are used for report period filtering here
            result = await task.execute(start_date=start_date, end_date=end_date, **update_kwargs)
        elif args.report_period or args.quarters or args.years:
            start_date, end_date = calculate_report_dates(args)
            logger.info(f"任务 {task_name}: 执行财务报告期更新，从 {start_date} 到 {end_date}")
            # Use calculated start/end dates
            result = await task.execute(start_date=start_date, end_date=end_date, **update_kwargs)
        elif args.start_date:
            start_date = args.start_date
            end_date = args.end_date or datetime.now().strftime('%Y%m%d')
            logger.info(f"任务 {task_name}: 执行日期范围更新，从 {start_date} 到 {end_date}")
            result = await task.execute(start_date=start_date, end_date=end_date, **update_kwargs)
        else:
            # Default: Smart incremental. Express/Forecast might need different lookback?
            # Keeping 1 year for now, but this might need adjustment based on task specifics.
            logger.info(f"任务 {task_name}: 执行智能增量更新，回溯1年")
            result = await task.smart_incremental_update(lookback_days=365, **update_kwargs)

        # Result handling (same as previous scripts)
        if result is None: return {'task_name': task_name, 'status': 'error', 'error': '任务执行返回None结果', 'rows': 0, 'failed_batches': 0}
        if not isinstance(result, dict): return {'task_name': task_name, 'status': 'error', 'error': f'任务执行返回非字典结果: {type(result)}', 'rows': 0, 'failed_batches': 0}

        standardized_result = {
            'task_name': task_name,
            'status': result.get('status', 'unknown'),
            'rows': pd.to_numeric(result.get('rows', np.nan), errors='coerce'),
            'failed_batches': pd.to_numeric(result.get('failed_batches', np.nan), errors='coerce')
        }
        rows = standardized_result['rows']
        standardized_result['rows'] = 0 if rows is None or (isinstance(rows, float) and np.isnan(rows)) else int(rows)
        failed_batches = standardized_result['failed_batches']
        standardized_result['failed_batches'] = 0 if failed_batches is None or (isinstance(failed_batches, float) and np.isnan(failed_batches)) else int(failed_batches)

        if standardized_result['status'] not in ['success', 'partial_success', 'no_data']:
            standardized_result['error'] = result.get('error', 'unknown error')
        return standardized_result

    except Exception as e:
        logger.error(f"任务 {task_name} 更新失败: {str(e)}", exc_info=True)
        return {'task_name': task_name, 'status': 'error', 'error': str(e), 'rows': 0, 'failed_batches': 0}

def format_report_period(period: Optional[str]) -> str:
    """格式化报告期为易读格式 (与之前脚本相同)
    Args:
        period: 报告期, 如 '202103'
    Returns:
        str: 格式化后的报告期，如 '2021年Q1'
    """
    # Identical to previous scripts
    if not period: return "未指定"
    if len(period) != 6: return period
    year, month = period[:4], int(period[4:])
    quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
    quarter = quarter_map.get(month, f"{month}月")
    return f"{year}年{quarter}"

def summarize_result(result: Dict[str, Any], args: argparse.Namespace) -> None:
    """生成业绩快报数据更新的汇总报告
    Args:
        result: 更新结果字典
        args: 命令行参数
    """
    logger.info("业绩快报数据更新结果汇总:")
    if not isinstance(result, dict): logger.error(f"结果格式错误: {result}"); return

    # Display update parameters (same as previous scripts)
    if args.report_period: logger.info(f"报告期: {format_report_period(args.report_period)}")
    elif args.quarters: logger.info(f"更新季度数: 最近 {args.quarters} 个季度")
    elif args.years: logger.info(f"更新年份数: 最近 {args.years} 年")
    elif args.start_date: logger.info(f"更新日期范围: {args.start_date} 至 {args.end_date or datetime.now().strftime('%Y%m%d')}")
    elif args.full_update: logger.info("更新模式: 全量更新")
    else: logger.info("更新模式: 智能增量更新 (默认回溯1年)")

    task_name = result.get('task_name', TARGET_TASK_NAME)
    task_type = "业绩快报"
    status = result.get('status', 'unknown')
    rows = result.get('rows', 0)
    failed_batches = result.get('failed_batches', 0)

    # Log result (same structure as previous scripts)
    if status == 'success': logger.info(f"- {task_type} ({task_name}): 成功, 更新 {rows} 行数据")
    elif status == 'partial_success': logger.info(f"- {task_type} ({task_name}): 部分成功, 更新 {rows} 行数据, {failed_batches} 个批次失败")
    elif status == 'no_data': logger.info(f"- {task_type} ({task_name}): 没有需要更新的数据")
    else: logger.error(f"- {task_type} ({task_name}): 失败, 错误: {result.get('error', 'unknown error')}")

    logger.info(f"总计更新: {rows} 行 {task_type} 数据")

async def main():
    parser = argparse.ArgumentParser(description='业绩快报数据更新工具')
    # Arguments are the same as previous scripts, excluding --max-concurrent
    parser.add_argument('--quarters', type=int, help='更新最近 N 个季度的数据')
    parser.add_argument('--years', type=int, help='更新最近 N 年的数据')
    parser.add_argument('--report-period', help='指定报告期 (格式: YYYYMM)')
    parser.add_argument('--start-date', help='更新起始日期 (格式: YYYYMMDD)')
    parser.add_argument('--end-date', help='更新结束日期 (格式: YYYYMMDD)')
    parser.add_argument('--full-update', action='store_true', help='全量更新模式')
    parser.add_argument('--auto', action='store_true', help='自动增量模式')
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"开始执行业绩快报 ({TARGET_TASK_NAME}) 数据更新...")

    await TaskFactory.initialize()
    logger.info("TaskFactory初始化成功")

    result = None
    try:
        result = await update_task(args)
        summarize_result(result, args)
    except Exception as e:
        logger.error(f"业绩快报数据更新过程中发生错误: {str(e)}", exc_info=True)
        if result is None:
             summarize_result({'task_name': TARGET_TASK_NAME, 'status': 'error', 'error': f"更新主流程异常: {str(e)}", 'rows': 0, 'failed_batches': 0}, args)
    finally:
        await TaskFactory.shutdown()
        logger.info("TaskFactory已关闭")

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"业绩快报数据更新执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    """
    使用示例:
    1. 默认增量更新（智能模式，回溯1年）:
       python examples/update_express_task.py
    # ... (其他示例与之前类似)
    6. 全量更新:
       python examples/update_express_task.py --full-update
    """
    asyncio.run(main()) 