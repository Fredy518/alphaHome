#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
全局数据更新脚本

自动更新所有已注册的数据任务。支持以下功能：
1. 自动识别所有已注册的任务
2. 支持多种更新模式（增量/全量）
3. 支持并行执行任务
4. 详细的日志记录
5. 错误处理和重试机制
"""

import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime, timedelta
import dotenv
from pathlib import Path
from typing import List, Dict, Any

# 添加项目根目录到系统路径
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

from alphahome.data_module.task_factory import TaskFactory
from alphahome.data_module.tools.calendar import get_last_trade_day, get_trade_cal

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'update_all_tasks_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)

logger = logging.getLogger('update_all_tasks')

async def update_task(task_name: str, args: argparse.Namespace) -> Dict[str, Any]:
    """更新单个任务的数据
    
    Args:
        task_name: 任务名称
        args: 命令行参数
        
    Returns:
        Dict: 更新结果
    """
    try:
        # 获取任务实例
        task = await TaskFactory.get_task(task_name)
        logger.info(f"开始更新任务: {task_name}")
        
        # 准备更新参数
        update_kwargs = {
            'show_progress': args.show_progress,
            'use_trade_days': True  # 默认使用交易日模式
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
        elif args.days:
            # 指定天数的增量更新
            result = await task.smart_incremental_update(
                lookback_days=args.days,
                **update_kwargs
            )
        elif args.start_date:
            # 指定日期范围的更新
            end_date = args.end_date or datetime.now().strftime('%Y%m%d')
            result = await task.execute(
                start_date=args.start_date,
                end_date=end_date,
                **update_kwargs
            )
        else:
            # 默认智能增量更新
            result = await task.smart_incremental_update(**update_kwargs)
        
        return {
            'task_name': task_name,
            'status': result.get('status'),
            'rows': result.get('rows', 0),
            'failed_batches': result.get('failed_batches', 0)
        }
        
    except Exception as e:
        logger.error(f"任务 {task_name} 更新失败: {str(e)}", exc_info=True)
        return {
            'task_name': task_name,
            'status': 'error',
            'error': str(e)
        }

async def update_all_tasks(args: argparse.Namespace) -> List[Dict[str, Any]]:
    """更新所有任务的数据
    
    Args:
        args: 命令行参数
        
    Returns:
        List[Dict]: 所有任务的更新结果
    """
    # 初始化TaskFactory
    await TaskFactory.initialize()
    logger.info("TaskFactory初始化成功")
    
    # 预加载交易日历数据以减少重复API调用
    try:
        today = datetime.now().strftime('%Y%m%d')
        
        if args.full_update:
            # 如果是全量更新，则预加载更大范围的交易日历
            start_date = '19901219'  # Tushare最早的数据日期
            logger.info("预加载交易日历数据（全量更新模式）...")
            await get_trade_cal(start_date=start_date, end_date=today)
            logger.info(f"已预加载从 {start_date} 到 {today} 的交易日历数据")
        elif args.days:
            # 如果使用增量更新模式，预加载近期交易日历
            # 预加载最近约2个月的日历，平衡效率和常见的回看需求
            lookback_start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            logger.info("预加载交易日历数据（指定天数增量模式）...")
            await get_trade_cal(start_date=lookback_start_date, end_date=today)
            logger.info(f"已预加载从 {lookback_start_date} 到 {today} 的交易日历数据")
        elif args.start_date:
            # 如果指定了日期范围，则预加载该范围的交易日历
            end_date = args.end_date or today
            logger.info("预加载交易日历数据（指定日期范围模式）...")
            await get_trade_cal(start_date=args.start_date, end_date=end_date)
            logger.info(f"已预加载从 {args.start_date} 到 {end_date} 的交易日历数据")
        else:
            # 默认方式：预加载近期数据
            lookback_start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            logger.info("预加载交易日历数据（默认增量模式）...")
            await get_trade_cal(start_date=lookback_start_date, end_date=today) 
            logger.info(f"已预加载从 {lookback_start_date} 到 {today} 的交易日历数据")
    except Exception as e:
        logger.warning(f"预加载交易日历数据失败: {str(e)}")
    
    try:
        # 获取所有任务名称
        all_task_names = await TaskFactory.get_all_task_names()
        logger.info(f"发现 {len(all_task_names)} 个已注册任务: {all_task_names}")
        
        if not all_task_names:
            logger.warning("未找到任何任务可执行")
            return []
            
        # 创建任务更新协程
        update_tasks = [
            update_task(task_name, args)
            for task_name in all_task_names
        ]
        
        # 并行执行所有任务
        results = await asyncio.gather(*update_tasks, return_exceptions=True)
        
        # 处理结果
        success_count = sum(1 for r in results if isinstance(r, dict) and r.get('status') in ['success', 'partial_success'])
        error_count = len(results) - success_count
        
        logger.info(f"任务更新完成: {success_count} 成功, {error_count} 失败")
        
        return results
    
    finally:
        # 关闭TaskFactory
        await TaskFactory.shutdown()
        logger.info("TaskFactory已关闭")

async def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='全局数据更新工具')
    parser.add_argument('--days', type=int, help='增量更新: 更新最近 N 个交易日的数据')
    parser.add_argument('--start-date', help='更新起始日期 (格式: YYYYMMDD)')
    parser.add_argument('--end-date', help='更新结束日期 (格式: YYYYMMDD, 默认为当天)')
    parser.add_argument('--full-update', action='store_true', help='全量更新模式: 从最早日期开始更新')
    parser.add_argument('--auto', action='store_true', help='自动增量模式: 从数据库最新日期开始更新（默认行为）')
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')
    
    args = parser.parse_args()
    
    # 记录开始时间
    start_time = datetime.now()
    logger.info("开始执行全局数据更新...")
    
    try:
        # 执行更新
        results = await update_all_tasks(args)
        
        # 输出详细结果
        logger.info("\n更新结果汇总:")
        for result in results:
            # 首先检查 gather 是否返回了异常
            if isinstance(result, Exception):
                # 尝试从异常中获取更多信息，但 task_name 可能未知
                # logging 框架会自动记录异常类型和堆栈
                logger.error(f"- unknown_task: 失败 (gather 捕获到异常), 错误: {result}", exc_info=False) # exc_info=False 因为日志框架可能已记录
                continue # 处理下一个结果
                
            # 确认 result 是字典
            if not isinstance(result, dict):
                logger.error(f"- unknown_task: 失败, 无效的结果类型: {type(result)}")
                continue
                
            status = result.get('status', 'unknown')
            task_name = result.get('task_name', 'unknown')
            
            if status == 'success':
                rows = result.get('rows', 0)
                logger.info(f"- {task_name}: 成功, 更新 {rows} 行数据")
            elif status == 'partial_success':
                rows = result.get('rows', 0)
                failed_batches = result.get('failed_batches', 0)
                logger.info(f"- {task_name}: 部分成功, 更新 {rows} 行数据, {failed_batches} 个批次失败")
            else: # status 不是 success 或 partial_success (可能是 'error', 'no_data', 'up_to_date', 'failure', 'unknown' 等)
                error = result.get('error') # 尝试获取 error 键
                # 如果 status 是 error 但 error 键不存在或为 None，使用 'unknown error'
                error_msg = error if error else 'unknown error'
                logger.error(f"- {task_name}: 失败, 错误: {error_msg}")
                # 如果有更详细的原始异常，也可能需要记录，但这取决于 update_task 如何返回错误
        
    except Exception as e:
        logger.error(f"全局更新过程中发生错误: {str(e)}", exc_info=True)
    
    # 计算总耗时
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"全局数据更新执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    """
    使用示例:
    1. 默认增量更新（智能模式）:
       python examples/update_all_tasks.py
       
    2. 更新最近N个交易日:
       python examples/update_all_tasks.py --days 5
       
    3. 指定日期范围更新:
       python examples/update_all_tasks.py --start-date 20230101 --end-date 20230131
       
    4. 全量更新:
       python examples/update_all_tasks.py --full-update
    """
    asyncio.run(main()) 