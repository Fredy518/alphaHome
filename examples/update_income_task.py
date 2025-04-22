#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
利润表数据更新脚本

专门用于更新股票利润表数据。
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
import calendar
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
from data_module.tools.calendar import get_last_trade_day, get_trade_cal # Keep calendar tools if needed by calculate_report_dates

# 配置日志
log_filename = f'update_income_task_{datetime.now().strftime("%Y%m%d")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename)
    ]
)

logger = logging.getLogger('update_income_task')

# 定义目标任务
TARGET_TASK_NAME = "tushare_stock_income" # 利润表

def calculate_report_dates(args: argparse.Namespace) -> Tuple[str, str]:
    """计算基于季度或年度选项的报告期范围

    Args:
        args: 命令行参数

    Returns:
        Tuple[str, str]: 开始日期和结束日期，格式为YYYYMMDD
    """
    today = datetime.now()
    end_date = args.end_date or today.strftime('%Y%m%d')

    if args.report_period:
        # 处理单个报告期
        period = args.report_period
        if len(period) != 6:
            raise ValueError("报告期格式应为YYYYMM，如202103表示2021年第一季度")

        year = int(period[:4])
        month = int(period[4:])

        # 检查月份有效性：只能是3、6、9、12（季末月）
        if month not in [3, 6, 9, 12]:
            raise ValueError("报告期月份只能是3、6、9、12，分别表示一、二、三、四季度")

        # 将报告期转换为月末日期
        _, last_day = calendar.monthrange(year, month)
        report_date = f"{year}{month:02d}{last_day:02d}"

        # 对于单个报告期，开始日期和结束日期相同
        return report_date, report_date

    elif args.quarters:
        # 处理最近N个季度
        quarters = args.quarters
        # 确定当前是哪个季度
        current_month = today.month
        current_quarter_month = ((current_month - 1) // 3 + 1) * 3
        current_year = today.year

        # 获取当前季度的结束日期
        _, last_day = calendar.monthrange(current_year, current_quarter_month)
        current_quarter_end = datetime(current_year, current_quarter_month, last_day)

        # 如果当前日期已经超过了当前季度末，使用当前季度作为最新季度
        # 否则使用上一个季度作为最新季度
        if today > current_quarter_end:
            latest_quarter_year = current_year
            latest_quarter_month = current_quarter_month
        else:
            # 上一个季度
            if current_quarter_month == 3:
                latest_quarter_year = current_year - 1
                latest_quarter_month = 12
            else:
                latest_quarter_year = current_year
                latest_quarter_month = current_quarter_month - 3

        # 回溯N个季度
        for _ in range(quarters - 1):
            if latest_quarter_month == 3:
                latest_quarter_year -= 1
                latest_quarter_month = 12
            else:
                latest_quarter_month -= 3

        # 获取开始季度的第一天
        start_date = f"{latest_quarter_year}{latest_quarter_month:02d}01"

        return start_date, end_date

    elif args.years:
        # 处理最近N年
        years = args.years
        start_year = today.year - years + 1
        start_date = f"{start_year}0101"  # 从指定年份的1月1日开始

        return start_date, end_date

    # 默认返回1年的数据
    one_year_ago = (today - timedelta(days=365)).strftime('%Y%m%d')
    return one_year_ago, end_date

async def update_task(args: argparse.Namespace) -> Dict[str, Any]:
    """更新单个财务任务的数据

    Args:
        args: 命令行参数

    Returns:
        Dict: 更新结果
    """
    task_name = TARGET_TASK_NAME
    try:
        # 获取任务实例
        task = await TaskFactory.get_task(task_name)
        if task is None:
            logger.error(f"无法获取任务实例: {task_name}")
            return {
                'task_name': task_name,
                'status': 'error',
                'error': f"无法获取任务实例: {task_name}",
                'rows': np.nan,
                'failed_batches': np.nan
            }

        logger.info(f"开始更新利润表任务: {task_name}")

        # 准备更新参数
        update_kwargs = {
            'show_progress': args.show_progress
        }

        # 根据不同的更新模式设置参数
        if args.full_update:
            # 全量更新模式
            start_date = task.default_start_date if hasattr(task, 'default_start_date') else '19900101'
            end_date = datetime.now().strftime('%Y%m%d')
            logger.info(f"任务 {task_name}: 执行全量更新，从 {start_date} 到 {end_date}")

            result = await task.execute(
                start_date=start_date,
                end_date=end_date,
                **update_kwargs
            )
        elif args.report_period or args.quarters or args.years:
            # 基于报告期的更新
            start_date, end_date = calculate_report_dates(args)
            logger.info(f"任务 {task_name}: 执行财务报告期更新，从 {start_date} 到 {end_date}")

            result = await task.execute(
                start_date=start_date,
                end_date=end_date,
                **update_kwargs
            )
        elif args.start_date:
            # 指定日期范围的更新
            end_date = args.end_date or datetime.now().strftime('%Y%m%d')
            logger.info(f"任务 {task_name}: 执行日期范围更新，从 {args.start_date} 到 {end_date}")

            result = await task.execute(
                start_date=args.start_date,
                end_date=end_date,
                **update_kwargs
            )
        else:
            # 默认智能增量更新
            logger.info(f"任务 {task_name}: 执行智能增量更新，回溯1年")

            result = await task.smart_incremental_update(
                lookback_days=365,
                **update_kwargs
            )

        # 处理结果为None的情况
        if result is None:
            logger.warning(f"任务 {task_name} 返回的结果为None，将转换为包含np.nan的结果")
            return {
                'task_name': task_name,
                'status': 'error',
                'error': '任务执行返回None结果',
                'rows': np.nan,
                'failed_batches': np.nan
            }

        # 确保结果是字典类型
        if not isinstance(result, dict):
            logger.warning(f"任务 {task_name} 返回的结果不是字典类型: {type(result)}，将转换为包含np.nan的结果")
            return {
                'task_name': task_name,
                'status': 'error',
                'error': f'任务执行返回非字典结果: {type(result)}',
                'rows': np.nan,
                'failed_batches': np.nan
            }

        # 标准化结果
        standardized_result = {
            'task_name': task_name,
            'status': result.get('status', 'unknown'),
            'rows': pd.to_numeric(result.get('rows', np.nan), errors='coerce'),
            'failed_batches': pd.to_numeric(result.get('failed_batches', np.nan), errors='coerce')
        }

        # 处理NaN值为0，并确保整数类型
        rows = standardized_result['rows']
        standardized_result['rows'] = 0 if rows is None or (isinstance(rows, float) and np.isnan(rows)) else int(rows)
        failed_batches = standardized_result['failed_batches']
        standardized_result['failed_batches'] = 0 if failed_batches is None or (isinstance(failed_batches, float) and np.isnan(failed_batches)) else int(failed_batches)


        # 如果失败，添加错误信息
        if standardized_result['status'] not in ['success', 'partial_success', 'no_data']:
            standardized_result['error'] = result.get('error', 'unknown error')

        return standardized_result

    except Exception as e:
        logger.error(f"任务 {task_name} 更新失败: {str(e)}", exc_info=True)
        return {
            'task_name': task_name,
            'status': 'error',
            'error': str(e),
            'rows': 0, # Use 0 instead of np.nan for consistency in summary
            'failed_batches': 0 # Use 0 instead of np.nan
        }

def format_report_period(period: Optional[str]) -> str:
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

    year = period[:4]
    month = int(period[4:])

    # 将月份转换为季度
    quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
    quarter = quarter_map.get(month, f"{month}月")

    return f"{year}年{quarter}"

def summarize_result(result: Dict[str, Any], args: argparse.Namespace) -> None:
    """生成利润表数据更新的汇总报告

    Args:
        result: 更新结果字典
        args: 命令行参数
    """
    logger.info("利润表数据更新结果汇总:")

    if not isinstance(result, dict):
        logger.error(f"结果格式错误，无法生成汇总报告: {result}")
        return

    # 显示更新参数
    if args.report_period:
        logger.info(f"报告期: {format_report_period(args.report_period)}")
    elif args.quarters:
        logger.info(f"更新季度数: 最近 {args.quarters} 个季度")
    elif args.years:
        logger.info(f"更新年份数: 最近 {args.years} 年")
    elif args.start_date:
        end_date = args.end_date or datetime.now().strftime('%Y%m%d')
        logger.info(f"更新日期范围: {args.start_date} 至 {end_date}")
    elif args.full_update:
        logger.info("更新模式: 全量更新")
    else:
        logger.info("更新模式: 智能增量更新 (默认回溯1年)")

    # 显示任务结果
    task_name = result.get('task_name', TARGET_TASK_NAME)
    task_type = "利润表" # Fixed for this script
    status = result.get('status', 'unknown')
    rows = result.get('rows', 0) # Default to 0
    failed_batches = result.get('failed_batches', 0) # Default to 0

    if status == 'success':
        logger.info(f"- {task_type} ({task_name}): 成功, 更新 {rows} 行数据")
    elif status == 'partial_success':
        logger.info(f"- {task_type} ({task_name}): 部分成功, 更新 {rows} 行数据, {failed_batches} 个批次失败")
    elif status == 'no_data':
        logger.info(f"- {task_type} ({task_name}): 没有需要更新的数据")
    else:
        error = result.get('error', 'unknown error')
        logger.error(f"- {task_type} ({task_name}): 失败, 错误: {error}")

    # 显示总结
    logger.info(f"总计更新: {rows} 行 {task_type} 数据")


async def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='利润表数据更新工具')
    parser.add_argument('--quarters', type=int, help='更新最近 N 个季度的数据（默认: 4）')
    parser.add_argument('--years', type=int, help='更新最近 N 年的数据')
    parser.add_argument('--report-period', help='指定报告期 (格式: YYYYMM, 如: 202103 表示2021年第一季度)')
    parser.add_argument('--start-date', help='更新起始日期 (格式: YYYYMMDD)')
    parser.add_argument('--end-date', help='更新结束日期 (格式: YYYYMMDD, 默认为当天)')
    parser.add_argument('--full-update', action='store_true', help='全量更新模式: 从最早日期开始更新')
    parser.add_argument('--auto', action='store_true', help='自动增量模式: 从数据库最新日期开始更新（默认行为）')
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')
    # Removed --max-concurrent

    args = parser.parse_args()

    # 记录开始时间
    start_time = datetime.now()
    logger.info(f"开始执行利润表 ({TARGET_TASK_NAME}) 数据更新...")

    # 初始化TaskFactory
    await TaskFactory.initialize()
    logger.info("TaskFactory初始化成功")

    result = None
    try:
        # 执行更新
        result = await update_task(args)

        # 输出详细结果
        summarize_result(result, args)

    except Exception as e:
        logger.error(f"利润表数据更新过程中发生错误: {str(e)}", exc_info=True)
        # Ensure summary is still called with error info if update_task failed before returning
        if result is None:
             summarize_result({
                'task_name': TARGET_TASK_NAME,
                'status': 'error',
                'error': f"更新主流程异常: {str(e)}",
                'rows': 0,
                'failed_batches': 0
             }, args)


    finally:
        # 关闭TaskFactory
        await TaskFactory.shutdown()
        logger.info("TaskFactory已关闭")

    # 计算总耗时
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"利润表数据更新执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    """
    使用示例:
    1. 默认增量更新（智能模式，回溯1年）:
       python examples/update_income_task.py

    2. 更新最近N个季度:
       python examples/update_income_task.py --quarters 4

    3. 更新最近N年:
       python examples/update_income_task.py --years 2

    4. 更新特定报告期:
       python examples/update_income_task.py --report-period 202103

    5. 指定日期范围更新:
       python examples/update_income_task.py --start-date 20230101 --end-date 20230630

    6. 全量更新:
       python examples/update_income_task.py --full-update
    """
    asyncio.run(main()) 