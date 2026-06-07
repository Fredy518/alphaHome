#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
G因子年度并行计算脚本
支持按年度分割计算，实现"土法"并行

使用方法：
python scripts/production/factor_calculators/g_factor/g_factor_parallel_by_year.py --start_year 2020 --end_year 2024 --worker_id 0 --total_workers 10
"""

import sys
import os
import argparse
import time
from datetime import datetime, timedelta
import pandas as pd

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.tools.context import ResearchContext
from research.pgs_factor.processors.production_g_factor_calculator import ProductionGFactorCalculator


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


def get_friday_dates_in_year(year: int) -> list:
    """获取指定年份的所有周五日期（G因子计算日）"""
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    # 生成该年份的所有日期
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # 只保留周五（weekday() == 4）
    friday_dates = []
    for date in date_range:
        if date.weekday() == 4:  # 4 = 周五
            friday_dates.append(date.strftime('%Y-%m-%d'))
    
    return friday_dates


def get_stock_codes_for_year(context, year: int) -> list:
    """获取指定年份的股票代码列表"""
    try:
        # 查询该年份有P因子数据的股票
        query = """
        SELECT DISTINCT ts_code 
        FROM pgs_factors.p_factor 
        WHERE EXTRACT(YEAR FROM calc_date) = %s
        AND p_score IS NOT NULL
        ORDER BY ts_code
        """
        
        results = context.db_manager.fetch_sync(query, (year,))
        # 处理查询结果，跳过列名行
        stock_codes = []
        for row in results:
            if isinstance(row, dict):
                stock_codes.append(row['ts_code'])
            else:
                stock_codes.append(row[0])
        return stock_codes
                
    except Exception as e:
        print(f"获取{year}年股票代码失败: {e}")
        return []


def calculate_g_factors_for_year(context, year: int, worker_id: int, total_workers: int):
    """计算指定年份的G因子"""
    print(f"🚀 工作进程 {worker_id}/{total_workers} 开始计算 {year} 年G因子")
    print(f"⏰ 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 初始化计算器
        calculator = ProductionGFactorCalculator(context)
        
        # 获取该年份的股票代码
        stock_codes = get_stock_codes_for_year(context, year)
        print(f"📊 {year}年股票数量: {len(stock_codes)}")
        
        if not stock_codes:
            print(f"⚠️ {year}年没有可计算的股票数据")
            return
        
        # 使用批量计算接口，自动处理周五逻辑
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
        print(f"📅 {year}年计算范围: {start_date} ~ {end_date}")
        print(f"🎯 使用批量计算接口，自动生成周五计算日")
        
        # 开始批量计算
        start_time = time.time()
        
        # 使用批量计算接口，自动处理周五日期生成和模式检测
        result = calculator.calculate_g_factors_batch_pit(
            start_date=start_date,
            end_date=end_date,
            mode='backfill'  # 强制使用回填模式
        )
        
        success_count = result['success_count']
        failed_count = result['failed_count']
        
        # 最终统计
        total_time = result['total_time']
        total_dates = result['total_dates']
        successful_dates = result['successful_dates']
        failed_dates = result['failed_dates']
        
        print(f"\n🎉 {year}年G因子计算完成!")
        print(f"⏰ 总耗时: {total_time:.1f}秒 ({total_time/60:.1f}分钟)")
        print(f"📅 计算日期: {total_dates} 个周五")
        print(f"✅ 成功日期: {successful_dates} 个")
        print(f"❌ 失败日期: {failed_dates} 个")
        print(f"✅ 成功计算: {success_count:,} 次")
        print(f"❌ 失败计算: {failed_count:,} 次")
        print(f"📊 成功率: {success_count/(success_count+failed_count)*100:.1f}%")
        print(f"🚀 吞吐量: {success_count/total_time:.1f} 次/秒")
        
    except Exception as e:
        print(f"❌ {year}年G因子计算失败: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description='G因子年度并行计算')
    parser.add_argument('--start_year', type=int, required=True, help='开始年份')
    parser.add_argument('--end_year', type=int, required=True, help='结束年份')
    parser.add_argument('--worker_id', type=int, required=True, help='工作进程ID (0-based)')
    parser.add_argument('--total_workers', type=int, required=True, help='总工作进程数')
    
    args = parser.parse_args()
    
    # 验证参数
    if args.worker_id >= args.total_workers:
        print(f"❌ worker_id ({args.worker_id}) 必须小于 total_workers ({args.total_workers})")
        sys.exit(1)
    
    if args.start_year > args.end_year:
        print(f"❌ start_year ({args.start_year}) 必须小于等于 end_year ({args.end_year})")
        sys.exit(1)
    
    # 计算该工作进程负责的年份 - 使用智能分配算法
    years = list(range(args.start_year, args.end_year + 1))
    worker_years_list = smart_year_allocation(years, args.total_workers)
    worker_years = worker_years_list[args.worker_id] if args.worker_id < len(worker_years_list) else []
    
    print(f"🔧 工作进程配置:")
    print(f"   进程ID: {args.worker_id}/{args.total_workers}")
    print(f"   负责年份: {worker_years}")
    print(f"   年份范围: {args.start_year}-{args.end_year}")
    
    if not worker_years:
        print(f"⚠️ 工作进程 {args.worker_id} 没有分配到年份，退出")
        return
    
    # 初始化研究上下文
    try:
        context = ResearchContext()
        print(f"✅ 研究上下文初始化成功")
    except Exception as e:
        print(f"❌ 研究上下文初始化失败: {e}")
        sys.exit(1)
    
    # 计算分配的年份
    for year in worker_years:
        try:
            calculate_g_factors_for_year(context, year, args.worker_id, args.total_workers)
        except Exception as e:
            print(f"❌ 工作进程 {args.worker_id} 计算 {year} 年失败: {e}")
            continue
    
    print(f"🎉 工作进程 {args.worker_id} 完成所有分配任务!")


if __name__ == "__main__":
    main()
