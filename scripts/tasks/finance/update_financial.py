#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
财务数据更新脚本

专门用于更新股票财务相关的数据任务（资产负债表、利润表、现金流量表、业绩快报、业绩预告）。
支持以下功能：
1. 按季度或年度更新财务数据
2. 支持指定报告期更新
3. 支持多种更新模式（增量/全量）
4. 有限并行执行任务，控制内存使用
5. 详细的日志记录和财务数据特定的结果汇总
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
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import numpy as np

# 添加项目根目录到系统路径
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

from data_module.task_factory import TaskFactory
from data_module.task_decorator import get_registered_tasks
from data_module.tools.calendar import get_last_trade_day, get_trade_cal

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'update_financial_tasks_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)

logger = logging.getLogger('update_financial_tasks')

# 定义财务相关任务列表
FINANCIAL_TASKS = [
    "tushare_fina_balancesheet",  # 资产负债表
    "tushare_fina_income",        # 利润表
    "tushare_fina_cashflow",      # 现金流量表
    "tushare_fina_express",       # 业绩快报
    "tushare_fina_forecast"       # 业绩预告
]

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

async def update_task(task_name: str, args: argparse.Namespace, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """更新单个财务任务的数据
    
    Args:
        task_name: 任务名称
        args: 命令行参数
        semaphore: 信号量，用于控制并发数
        
    Returns:
        Dict: 更新结果
    """
    async with semaphore:  # 使用信号量控制并发
        try:
            # 获取任务实例
            task = await TaskFactory.get_task(task_name)
            if task is None:
                logger.error(f"无法获取任务实例: {task_name}")
                return {
                    'task_name': task_name,
                    'status': 'error',
                    'error': f"无法获取任务实例: {task_name}"
                }
                
            logger.info(f"开始更新财务任务: {task_name}")
            
            # 准备更新参数
            update_kwargs = {
                'show_progress': args.show_progress
            }
            
            # 根据不同的更新模式设置参数
            if args.full_update:
                # 全量更新模式
                # 使用任务定义的默认起始日期
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
                # 对于财务数据，使用较长的回溯时间（1年）
                logger.info(f"任务 {task_name}: 执行智能增量更新，回溯1年")
                
                # 为财务数据设置更长的回溯天数
                result = await task.smart_incremental_update(
                    lookback_days=365,  # 财务数据默认回溯1年
                    **update_kwargs
                )
            
            # 处理结果为None的情况，转换为包含np.nan的字典
            if result is None:
                logger.warning(f"任务 {task_name} 返回的结果为None，将转换为包含np.nan的结果")
                return {
                    'task_name': task_name,
                    'status': 'error',
                    'error': '任务执行返回None结果',
                    'rows': np.nan,
                    'failed_batches': np.nan
                }
                
            # 确保结果是字典类型，如果不是，将其转换为包含np.nan的字典
            if not isinstance(result, dict):
                logger.warning(f"任务 {task_name} 返回的结果不是字典类型: {type(result)}，将转换为包含np.nan的结果")
                return {
                    'task_name': task_name,
                    'status': 'error',
                    'error': f'任务执行返回非字典结果: {type(result)}',
                    'rows': np.nan,
                    'failed_batches': np.nan
                }
            
            # 创建包含所有必要键的结果字典，对None值使用np.nan代替
            # 这样在后续的pandas操作中可以更好地处理
            standardized_result = {
                'task_name': task_name,
                'status': result.get('status', 'unknown'),
                'rows': pd.to_numeric(result.get('rows', np.nan), errors='coerce'),  # 将非数值类型强制转换为np.nan
                'failed_batches': pd.to_numeric(result.get('failed_batches', np.nan), errors='coerce')  # 同上
            }
            
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
                'rows': np.nan,
                'failed_batches': np.nan
            }

async def update_financial_tasks(args: argparse.Namespace) -> List[Dict[str, Any]]:
    """更新所有财务任务的数据
    
    Args:
        args: 命令行参数
        
    Returns:
        List[Dict]: 所有财务任务的更新结果
    """
    # 初始化TaskFactory
    await TaskFactory.initialize()
    logger.info("TaskFactory初始化成功")
    
    try:
        # 获取所有已注册的任务
        registered_tasks = get_registered_tasks()
        logger.info(f"发现 {len(registered_tasks)} 个已注册的任务")
        
        # 过滤出财务相关的任务
        financial_task_names = [name for name in registered_tasks if name in FINANCIAL_TASKS]
        
        if not financial_task_names:
            logger.warning("未找到任何财务相关的任务")
            return []
            
        logger.info(f"将更新以下 {len(financial_task_names)} 个财务任务: {', '.join(financial_task_names)}")
        
        # 创建信号量控制并发数
        semaphore = asyncio.Semaphore(args.max_concurrent)
        
        # 创建任务更新协程
        update_tasks = [
            update_task(task_name, args, semaphore)
            for task_name in financial_task_names
        ]
        
        # 并行执行所有任务（受信号量控制）
        results = await asyncio.gather(*update_tasks, return_exceptions=True)
        
        # 标准化结果列表，处理可能的异常
        standardized_results = []
        for i, result in enumerate(results):
            # 获取当前任务名称
            task_name = financial_task_names[i] if i < len(financial_task_names) else 'unknown'
            
            if isinstance(result, Exception):
                # 将异常转换为标准错误结果字典
                error_msg = f"任务执行异常: {str(result)}"
                logger.error(f"任务 {task_name} 执行异常: {error_msg}")
                standardized_results.append({
                    'task_name': task_name,
                    'status': 'error',
                    'error': error_msg,
                    'rows': np.nan,
                    'failed_batches': np.nan
                })
            elif result is None:
                # 处理None结果
                logger.warning(f"任务 {task_name} 返回None结果")
                standardized_results.append({
                    'task_name': task_name,
                    'status': 'error',
                    'error': '任务返回None，可能是执行失败',
                    'rows': np.nan,
                    'failed_batches': np.nan
                })
            else:
                # 正常结果直接添加
                standardized_results.append(result)
                
        # 将None和NaN转换为0
        for result in standardized_results:
            if isinstance(result, dict):
                # 处理rows
                if 'rows' in result:
                    try:
                        if result['rows'] is None or (isinstance(result['rows'], float) and np.isnan(result['rows'])):
                            result['rows'] = 0
                        else:
                            result['rows'] = int(result['rows'])
                    except (ValueError, TypeError):
                        logger.warning(f"无法将rows值 {result.get('rows')} 转换为整数，设为0")
                        result['rows'] = 0
                        
                # 处理failed_batches
                if 'failed_batches' in result:
                    try:
                        if result['failed_batches'] is None or (isinstance(result['failed_batches'], float) and np.isnan(result['failed_batches'])):
                            result['failed_batches'] = 0
                        else:
                            result['failed_batches'] = int(result['failed_batches'])
                    except (ValueError, TypeError):
                        logger.warning(f"无法将failed_batches值 {result.get('failed_batches')} 转换为整数，设为0")
                        result['failed_batches'] = 0
        
        # 处理结果
        success_count = sum(1 for r in standardized_results if isinstance(r, dict) and r.get('status') in ['success', 'partial_success'])
        error_count = len(standardized_results) - success_count
        
        logger.info(f"财务任务更新完成: {success_count} 成功, {error_count} 失败")
        
        return standardized_results
    
    finally:
        # 关闭TaskFactory
        await TaskFactory.shutdown()
        logger.info("TaskFactory已关闭")

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

def summarize_financial_results(results: List[Dict[str, Any]], args: argparse.Namespace) -> None:
    """生成财务数据更新的详细汇总报告
    
    Args:
        results: 更新结果列表
        args: 命令行参数
    """
    logger.info("\n财务数据更新结果汇总:")
    
    # 确保结果列表不为None
    if results is None:
        logger.error("结果列表为None，无法生成汇总报告")
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
    
    # 按任务类型分组显示结果
    task_type_map = {
        "tushare_fina_balancesheet": "资产负债表",
        "tushare_fina_income": "利润表",
        "tushare_fina_cashflow": "现金流量表",
        "tushare_fina_express": "业绩快报",
        "tushare_fina_forecast": "业绩预告"
    }
    
    try:
        # 使用pandas处理结果
        results_df = pd.DataFrame(results)
        
        # 如果DataFrame为空，直接返回
        if results_df.empty:
            logger.warning("结果列表为空，无法生成汇总报告")
            return
            
        # 确保关键列存在，如果不存在则添加
        for col in ['task_name', 'status', 'rows', 'failed_batches']:
            if col not in results_df.columns:
                results_df[col] = np.nan
        
        # 确保数值列是数值类型
        results_df['rows'] = pd.to_numeric(results_df['rows'], errors='coerce').fillna(0).astype(int)
        results_df['failed_batches'] = pd.to_numeric(results_df['failed_batches'], errors='coerce').fillna(0).astype(int)
        
        # 计算总更新行数
        total_rows = results_df['rows'].sum()
        
        # 添加任务类型列
        results_df['task_type'] = results_df['task_name'].apply(lambda x: task_type_map.get(x, x))
        
        # 显示每个任务的结果
        for _, row in results_df.iterrows():
            status = row.get('status')
            task_type = row.get('task_type')
            
            if status == 'success':
                logger.info(f"- {task_type}: 成功, 更新 {int(row['rows'])} 行数据")
            elif status == 'partial_success':
                logger.info(f"- {task_type}: 部分成功, 更新 {int(row['rows'])} 行数据, {int(row['failed_batches'])} 个批次失败")
            elif status == 'no_data':
                logger.info(f"- {task_type}: 没有需要更新的数据")
            else:
                error = row.get('error', 'unknown error')
                logger.error(f"- {task_type}: 失败, 错误: {error}")
        
        # 显示总结
        logger.info(f"\n总计更新: {total_rows} 行财务数据")
        
        # 数据一致性检查提示
        if total_rows > 0:
            # 过滤出主要报表
            main_reports = ["tushare_fina_balancesheet", "tushare_fina_income", "tushare_fina_cashflow"]
            main_report_results = results_df[results_df['task_name'].isin(main_reports)]
            main_report_success = main_report_results[main_report_results['status'].isin(['success', 'partial_success'])]
            
            if len(main_report_success) == len(main_reports):
                logger.info("✓ 所有主要财务报表（资产负债表、利润表、现金流量表）更新成功")
            else:
                logger.warning("⚠ 部分主要财务报表更新失败，可能导致数据不一致")
                
            # 检查不同报表数据量是否一致
            if len(main_report_success) >= 2:
                rows_list = main_report_success['rows'].tolist()
                if all(r == rows_list[0] for r in rows_list):
                    logger.info(f"✓ 主要财务报表数据行数一致: 每个报表 {rows_list[0]} 行")
                else:
                    # 格式化不一致报告
                    report_rows = []
                    for _, row in main_report_success.iterrows():
                        task = row['task_name']
                        rows = int(row['rows'])
                        report_name = task_type_map.get(task, task)
                        report_rows.append(f"{report_name}: {rows}行")
                    
                    logger.warning(f"⚠ 主要财务报表数据行数不一致: {', '.join(report_rows)}")
    
    except Exception as e:
        # 如果pandas处理失败，使用传统方式处理
        logger.warning(f"使用pandas处理结果时出错: {e}，将使用传统方式")
        
        # 计算总更新行数
        total_rows = 0
        
        # 显示每个任务的结果
        for result in results:
            if not isinstance(result, dict):
                logger.warning(f"跳过非字典类型的结果: {type(result)}")
                continue
                
            status = result.get('status', 'unknown')
            task_name = result.get('task_name', 'unknown')
            task_type = task_type_map.get(task_name, task_name)
            
            if status == 'success':
                rows = result.get('rows', 0)
                if not isinstance(rows, int):
                    logger.warning(f"任务 {task_name} 返回的行数不是整数类型: {type(rows)}")
                    rows = 0
                total_rows += rows
                logger.info(f"- {task_type}: 成功, 更新 {rows} 行数据")
            elif status == 'partial_success':
                rows = result.get('rows', 0)
                if not isinstance(rows, int):
                    logger.warning(f"任务 {task_name} 返回的行数不是整数类型: {type(rows)}")
                    rows = 0
                total_rows += rows
                failed_batches = result.get('failed_batches', 0)
                if not isinstance(failed_batches, int):
                    logger.warning(f"任务 {task_name} 返回的失败批次数不是整数类型: {type(failed_batches)}")
                    failed_batches = 0
                logger.info(f"- {task_type}: 部分成功, 更新 {rows} 行数据, {failed_batches} 个批次失败")
            elif status == 'no_data':
                logger.info(f"- {task_type}: 没有需要更新的数据")
            else:
                error = result.get('error', 'unknown error')
                logger.error(f"- {task_type}: 失败, 错误: {error}")
        
        # 显示总结
        logger.info(f"\n总计更新: {total_rows} 行财务数据")
        
        # 简化的数据一致性检查
        if total_rows > 0:
            # 过滤出主要报表的结果
            main_reports = ["tushare_fina_balancesheet", "tushare_fina_income", "tushare_fina_cashflow"]
            main_report_results = [r for r in results if isinstance(r, dict) and r.get('task_name') in main_reports]
            main_report_success = [r for r in main_report_results if r.get('status') in ['success', 'partial_success']]
            
            if len(main_report_success) == len(main_reports):
                logger.info("✓ 所有主要财务报表（资产负债表、利润表、现金流量表）更新成功")
            else:
                logger.warning("⚠ 部分主要财务报表更新失败，可能导致数据不一致")

async def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='财务数据更新工具')
    parser.add_argument('--quarters', type=int, help='更新最近 N 个季度的数据（默认: 4）')
    parser.add_argument('--years', type=int, help='更新最近 N 年的数据')
    parser.add_argument('--report-period', help='指定报告期 (格式: YYYYMM, 如: 202103 表示2021年第一季度)')
    parser.add_argument('--start-date', help='更新起始日期 (格式: YYYYMMDD)')
    parser.add_argument('--end-date', help='更新结束日期 (格式: YYYYMMDD, 默认为当天)')
    parser.add_argument('--full-update', action='store_true', help='全量更新模式: 从最早日期开始更新')
    parser.add_argument('--auto', action='store_true', help='自动增量模式: 从数据库最新日期开始更新（默认行为）')
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')
    parser.add_argument('--max-concurrent', type=int, default=3, help='最大并发任务数量 (默认: 3)')
    
    args = parser.parse_args()
    
    # 记录开始时间
    start_time = datetime.now()
    logger.info("开始执行财务数据更新...")
    
    try:
        # 执行更新
        results = await update_financial_tasks(args)
        
        # 输出详细结果
        summarize_financial_results(results, args)
        
    except Exception as e:
        logger.error(f"财务数据更新过程中发生错误: {str(e)}", exc_info=True)
    
    # 计算总耗时
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"财务数据更新执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    """
    使用示例:
    1. 默认增量更新（智能模式，回溯1年）:
       python examples/update_financial_tasks.py
       
    2. 更新最近N个季度:
       python examples/update_financial_tasks.py --quarters 4
       
    3. 更新最近N年:
       python examples/update_financial_tasks.py --years 2
       
    4. 更新特定报告期:
       python examples/update_financial_tasks.py --report-period 202103
       
    5. 指定日期范围更新:
       python examples/update_financial_tasks.py --start-date 20230101 --end-date 20230630
       
    6. 全量更新:
       python examples/update_financial_tasks.py --full-update
       
    7. 控制并发数:
       python examples/update_financial_tasks.py --max-concurrent 2
    """
    asyncio.run(main()) 