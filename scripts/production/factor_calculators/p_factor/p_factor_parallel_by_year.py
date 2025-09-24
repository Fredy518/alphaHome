#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
P因子年度并行计算脚本
支持按年度分割计算，实现"土法"并行

使用方法：
python scripts/production/factor_calculators/p_factor/p_factor_parallel_by_year.py --start_year 2020 --end_year 2024 --worker_id 0 --total_workers 10
"""

import sys
import os
import argparse
import time
from datetime import datetime, timedelta
import pandas as pd
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


def smart_year_allocation(years, workers):
    """
    智能年份分配算法
    按时间顺序轮询分配，早期年份优先合并
    
    Args:
        years: 年份列表
        workers: 工作进程数
    
    Returns:
        list: 每个进程分配的年份列表
    """
    total_years = len(years)
    
    if total_years <= workers:
        # 年份数 <= 进程数，每个进程分配一个年份
        allocation = [[year] for year in years]
        # 补充空列表
        while len(allocation) < workers:
            allocation.append([])
        return allocation
    
    # 年份数 > 进程数，需要轮询分配
    allocation = [[] for _ in range(workers)]
    
    # 按年份排序（保持时间顺序）
    years_sorted = sorted(years)
    
    # 计算每个进程应该分配的年数
    base_years_per_worker = total_years // workers  # 每个进程的基础年数
    extra_years = total_years % workers  # 多出来的年数
    
    # 前extra_years个进程多分配1年
    years_per_worker = [base_years_per_worker + 1 if i < extra_years else base_years_per_worker 
                       for i in range(workers)]
    
    # 按时间顺序分配年份
    year_index = 0
    for worker_id in range(workers):
        for _ in range(years_per_worker[worker_id]):
            if year_index < len(years_sorted):
                allocation[worker_id].append(years_sorted[year_index])
                year_index += 1
    
    return allocation


def get_my_years(start_year, end_year, worker_id, total_workers):
    """获取当前worker应该处理的年份"""
    years = list(range(start_year, end_year + 1))
    allocation = smart_year_allocation(years, total_workers)
    return allocation[worker_id]


def process_years(years, worker_id, total_workers):
    """处理分配的年份"""
    print(f"Worker {worker_id}: 开始处理年份 {years}")
    
    # 初始化P因子计算器
    calculator = ProductionPFactorCalculator()
    
    total_start_time = time.time()
    total_success = 0
    total_failed = 0
    
    for year in years:
        year_start_time = time.time()
        print(f"Worker {worker_id}: 开始处理 {year} 年")
        
        # 计算该年的日期范围
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
        try:
            # 执行P因子计算
            result = calculator.calculate_p_factors_batch_pit(
                start_date=start_date,
                end_date=end_date,
                mode='backfill'  # 年度计算通常是回填模式
            )
            
            total_success += result['success_count']
            total_failed += result['failed_count']
            
            year_time = time.time() - year_start_time
            print(f"Worker {worker_id}: {year} 年计算完成")
            print(f"  成功: {result['success_count']:,}")
            print(f"  失败: {result['failed_count']:,}")
            print(f"  耗时: {year_time:.2f} 秒")
            
            if year_time > 0:
                throughput = result['success_count'] / year_time
                print(f"  吞吐量: {throughput:.1f} 只/秒")
            
        except Exception as e:
            print(f"Worker {worker_id}: {year} 年计算失败: {e}")
            total_failed += 1000  # 估算失败数量
    
    total_time = time.time() - total_start_time
    
    print("=" * 50)
    print(f"Worker {worker_id}: 所有年份计算完成")
    print("=" * 50)
    print(f"处理年份: {years}")
    print(f"总成功: {total_success:,}")
    print(f"总失败: {total_failed:,}")
    print(f"总耗时: {total_time:.2f} 秒")
    
    if total_time > 0:
        overall_throughput = total_success / total_time
        print(f"总吞吐量: {overall_throughput:.1f} 只/秒")
    
    return {
        'worker_id': worker_id,
        'years': years,
        'total_success': total_success,
        'total_failed': total_failed,
        'total_time': total_time
    }


def main():
    parser = argparse.ArgumentParser(description='P因子年度并行计算脚本')
    parser.add_argument('--start_year', type=int, required=True, help='开始年份')
    parser.add_argument('--end_year', type=int, required=True, help='结束年份')
    parser.add_argument('--worker_id', type=int, required=True, help='当前worker ID (从0开始)')
    parser.add_argument('--total_workers', type=int, required=True, help='总worker数量')
    
    args = parser.parse_args()
    
    # 获取当前worker应该处理的年份
    my_years = get_my_years(args.start_year, args.end_year, args.worker_id, args.total_workers)
    
    if not my_years:
        print(f"Worker {args.worker_id}: 没有分配到年份，退出")
        return
    
    print(f"P因子年度并行计算 - Worker {args.worker_id}")
    print(f"分配年份: {my_years}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    # 执行计算
    result = process_years(my_years, args.worker_id, args.total_workers)
    
    print("-" * 50)
    print(f"Worker {args.worker_id} 完成，结果:")
    print(f"  处理年份: {result['years']}")
    print(f"  成功计算: {result['total_success']:,} 只")
    print(f"  计算失败: {result['total_failed']:,} 只")
    print(f"  总耗时: {result['total_time']:.2f} 秒")
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
