#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
P因子季度并行计算脚本
支持按季度分割计算，实现"土法"并行

使用方法：
python scripts/production/factor_calculators/p_factor/p_factor_parallel_by_quarter.py --worker_id 0 --total_workers 16 --quarter 2020Q1 --quarter 2020Q2
"""

import sys
import os
import argparse
import time
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Tuple
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

# 动态导入p因子计算器
import importlib.util
spec = importlib.util.spec_from_file_location(
    "production_p_factor_calculator", 
    Path(__file__).parent / "production_p_factor_calculator.py"
)
calc_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calc_module)
ProductionPFactorCalculator = calc_module.ProductionPFactorCalculator


def parse_quarter(quarter_str: str) -> Tuple[int, int]:
    """
    解析季度字符串
    
    Args:
        quarter_str: 季度字符串，格式如 "2020Q1"
    
    Returns:
        tuple: (年份, 季度)
    """
    try:
        year_str, quarter_str = quarter_str.split('Q')
        year = int(year_str)
        quarter = int(quarter_str)
        
        if quarter < 1 or quarter > 4:
            raise ValueError(f"季度必须在1-4之间: {quarter}")
        
        return year, quarter
    except Exception as e:
        raise ValueError(f"无效的季度格式: {quarter_str}, 期望格式: YYYYQN")


def get_quarter_date_range(year: int, quarter: int) -> Tuple[str, str]:
    """
    获取指定季度的日期范围
    
    Args:
        year: 年份
        quarter: 季度 (1-4)
    
    Returns:
        tuple: (开始日期, 结束日期)
    """
    if quarter == 1:
        start_date = f"{year}-01-01"
        end_date = f"{year}-03-31"
    elif quarter == 2:
        start_date = f"{year}-04-01"
        end_date = f"{year}-06-30"
    elif quarter == 3:
        start_date = f"{year}-07-01"
        end_date = f"{year}-09-30"
    elif quarter == 4:
        start_date = f"{year}-10-01"
        end_date = f"{year}-12-31"
    else:
        raise ValueError(f"无效的季度: {quarter}")
    
    return start_date, end_date


def process_quarters(quarters: List[Tuple[int, int]], worker_id: int, total_workers: int):
    """处理分配的季度"""
    print(f"Worker {worker_id}: 开始处理 {len(quarters)} 个季度")
    
    # 初始化P因子计算器
    calculator = ProductionPFactorCalculator()
    
    total_start_time = time.time()
    total_success = 0
    total_failed = 0
    
    for i, (year, quarter) in enumerate(quarters, 1):
        quarter_start_time = time.time()
        quarter_str = f"{year}Q{quarter}"
        
        print(f"Worker {worker_id}: [{i}/{len(quarters)}] 开始处理 {quarter_str}")
        
        # 计算该季度的日期范围
        start_date, end_date = get_quarter_date_range(year, quarter)
        
        try:
            # 执行P因子计算
            result = calculator.calculate_p_factors_batch_pit(
                start_date=start_date,
                end_date=end_date,
                mode='backfill'  # 季度计算通常是回填模式
            )
            
            total_success += result['success_count']
            total_failed += result['failed_count']
            
            quarter_time = time.time() - quarter_start_time
            print(f"Worker {worker_id}: {quarter_str} 计算完成")
            print(f"  成功: {result['success_count']:,}")
            print(f"  失败: {result['failed_count']:,}")
            print(f"  耗时: {quarter_time:.2f} 秒")
            
            if quarter_time > 0:
                throughput = result['success_count'] / quarter_time
                print(f"  吞吐量: {throughput:.1f} 只/秒")
            
        except Exception as e:
            print(f"Worker {worker_id}: {quarter_str} 计算失败: {e}")
            total_failed += 500  # 估算失败数量
    
    total_time = time.time() - total_start_time
    
    print("=" * 50)
    print(f"Worker {worker_id}: 所有季度计算完成")
    print("=" * 50)
    print(f"处理季度数: {len(quarters)}")
    print(f"总成功: {total_success:,}")
    print(f"总失败: {total_failed:,}")
    print(f"总耗时: {total_time:.2f} 秒")
    
    if total_time > 0:
        overall_throughput = total_success / total_time
        print(f"总吞吐量: {overall_throughput:.1f} 只/秒")
    
    return {
        'worker_id': worker_id,
        'quarters': quarters,
        'total_success': total_success,
        'total_failed': total_failed,
        'total_time': total_time
    }


def generate_quarters_range(start_quarter: str, end_quarter: str) -> List[Tuple[int, int]]:
    """
    生成季度范围内的所有季度

    Args:
        start_quarter: 开始季度，格式: YYYYQN
        end_quarter: 结束季度，格式: YYYYQN

    Returns:
        所有季度列表 [(year, quarter), ...]
    """
    start_year, start_q = parse_quarter(start_quarter)
    end_year, end_q = parse_quarter(end_quarter)

    quarters = []

    current_year = start_year
    current_quarter = start_q

    while (current_year < end_year) or (current_year == end_year and current_quarter <= end_q):
        quarters.append((current_year, current_quarter))

        # 移动到下一个季度
        current_quarter += 1
        if current_quarter > 4:
            current_quarter = 1
            current_year += 1

    return quarters


def main():
    parser = argparse.ArgumentParser(description='P因子季度并行计算脚本')
    parser.add_argument('--worker_id', type=int, required=True, help='当前worker ID (从0开始)')
    parser.add_argument('--total_workers', type=int, required=True, help='总worker数量')

    # 新增：支持季度范围参数
    parser.add_argument('--start_quarter', type=str, help='开始季度，格式: YYYYQN (例如: 2025Q3)')
    parser.add_argument('--end_quarter', type=str, help='结束季度，格式: YYYYQN (例如: 2025Q4)')

    # 保持兼容：仍支持单个季度参数
    parser.add_argument('--quarter', action='append', help='要处理的季度，格式: YYYYQN，可以多次指定 (与范围参数互斥)')

    args = parser.parse_args()

    # 处理季度参数（新旧格式兼容）
    quarters = []

    if args.start_quarter and args.end_quarter:
        # 使用新的范围格式
        if args.quarter:
            print(f"Worker {args.worker_id}: 不能同时指定 --quarter 和 --start_quarter/--end_quarter")
            sys.exit(1)

        try:
            quarters = generate_quarters_range(args.start_quarter, args.end_quarter)
            print(f"Worker {args.worker_id}: 生成季度范围: {args.start_quarter} ~ {args.end_quarter} (共 {len(quarters)} 个季度)")
        except ValueError as e:
            print(f"Worker {args.worker_id}: {e}")
            sys.exit(1)

    elif args.quarter:
        # 使用旧的单个季度格式
        for quarter_str in args.quarter:
            try:
                year, quarter = parse_quarter(quarter_str)
                quarters.append((year, quarter))
            except ValueError as e:
                print(f"Worker {args.worker_id}: 错误: {e}")
                sys.exit(1)
    else:
        print(f"Worker {args.worker_id}: 必须指定季度参数：使用 --start_quarter 和 --end_quarter 指定范围，或使用 --quarter 指定单个季度")
        sys.exit(1)

    if not quarters:
        print(f"Worker {args.worker_id}: 没有分配到季度，退出")
        return
    
    # 按时间顺序排序季度
    quarters.sort(key=lambda x: (x[0], x[1]))
    
    quarter_strs = [f"{year}Q{q}" for year, q in quarters]
    
    print(f"P因子季度并行计算 - Worker {args.worker_id}")
    print(f"分配季度: {quarter_strs}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    # 执行计算
    result = process_quarters(quarters, args.worker_id, args.total_workers)
    
    print("-" * 50)
    print(f"Worker {args.worker_id} 完成，结果:")
    processed_quarters = [f"{year}Q{q}" for year, q in result['quarters']]
    print(f"  处理季度: {processed_quarters}")
    print(f"  成功计算: {result['total_success']:,} 只")
    print(f"  计算失败: {result['total_failed']:,} 只")
    print(f"  总耗时: {result['total_time']:.2f} 秒")
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
