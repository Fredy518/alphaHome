#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
G因子季度并行计算脚本
支持按季度分割计算，实现"土法"并行

使用方法：
python scripts/production/factor_calculators/g_factor/g_factor_parallel_by_quarter.py --worker_id 0 --total_workers 16 --quarter 2020Q1 --quarter 2020Q2
"""

import sys
import os
import argparse
import time
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Tuple

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.tools.context import ResearchContext
from research.pgs_factor.processors.production_g_factor_calculator import ProductionGFactorCalculator


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


def get_friday_dates_in_quarter(year: int, quarter: int) -> List[str]:
    """获取指定季度的所有周五日期（G因子计算日）"""
    start_date, end_date = get_quarter_date_range(year, quarter)
    
    # 生成该季度的所有日期
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # 只保留周五（weekday() == 4）
    friday_dates = []
    for date in date_range:
        if date.weekday() == 4:  # 4 = 周五
            friday_dates.append(date.strftime('%Y-%m-%d'))
    
    return friday_dates


def get_stock_codes_for_quarter(context, year: int, quarter: int) -> List[str]:
    """获取指定季度的股票代码列表"""
    try:
        start_date, end_date = get_quarter_date_range(year, quarter)
        
        # 查询该季度有P因子数据的股票
        query = """
        SELECT DISTINCT ts_code 
        FROM pgs_factors.p_factor 
        WHERE calc_date >= %s AND calc_date <= %s
        AND p_score IS NOT NULL
        ORDER BY ts_code
        """
        
        results = context.db_manager.fetch_sync(query, (start_date, end_date))
        # 处理查询结果，跳过列名行
        stock_codes = []
        for row in results:
            if isinstance(row, dict):
                stock_codes.append(row['ts_code'])
            else:
                stock_codes.append(row[0])
        return stock_codes
                
    except Exception as e:
        print(f"获取{year}年Q{quarter}股票代码失败: {e}")
        return []


def calculate_g_factors_for_quarter(context, year: int, quarter: int, worker_id: int, total_workers: int):
    """计算指定季度的G因子"""
    print(f"🚀 工作进程 {worker_id}/{total_workers} 开始计算 {year}年Q{quarter} G因子")
    print(f"⏰ 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 初始化计算器
        calculator = ProductionGFactorCalculator(context)
        
        # 获取该季度的股票代码
        stock_codes = get_stock_codes_for_quarter(context, year, quarter)
        print(f"📊 {year}年Q{quarter}股票数量: {len(stock_codes)}")
        
        if not stock_codes:
            print(f"⚠️ {year}年Q{quarter}没有可计算的股票数据")
            return
        
        # 获取季度日期范围
        start_date, end_date = get_quarter_date_range(year, quarter)
        
        print(f"📅 {year}年Q{quarter}计算范围: {start_date} ~ {end_date}")
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
        
        print(f"\n🎉 {year}年Q{quarter} G因子计算完成!")
        print(f"⏰ 总耗时: {total_time:.1f}秒 ({total_time/60:.1f}分钟)")
        print(f"📅 计算日期: {total_dates} 个周五")
        print(f"✅ 成功日期: {successful_dates} 个")
        print(f"❌ 失败日期: {failed_dates} 个")
        print(f"✅ 成功计算: {success_count:,} 次")
        print(f"❌ 失败计算: {failed_count:,} 次")
        print(f"📊 成功率: {success_count/(success_count+failed_count)*100:.1f}%")
        print(f"🚀 吞吐量: {success_count/total_time:.1f} 次/秒")
        
    except Exception as e:
        print(f"❌ {year}年Q{quarter} G因子计算失败: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description='G因子季度并行计算')
    parser.add_argument('--worker_id', type=int, required=True, help='工作进程ID (0-based)')
    parser.add_argument('--total_workers', type=int, required=True, help='总工作进程数')
    parser.add_argument('--quarter', action='append', required=True, help='季度，格式: YYYYQN (可多次指定)')
    
    args = parser.parse_args()
    
    # 验证参数
    if args.worker_id >= args.total_workers:
        print(f"❌ worker_id ({args.worker_id}) 必须小于 total_workers ({args.total_workers})")
        sys.exit(1)
    
    if not args.quarter:
        print(f"❌ 必须指定至少一个季度")
        sys.exit(1)
    
    # 解析季度参数
    quarters = []
    for quarter_str in args.quarter:
        try:
            year, quarter = parse_quarter(quarter_str)
            quarters.append((year, quarter))
        except ValueError as e:
            print(f"❌ {e}")
            sys.exit(1)
    
    print(f"🔧 工作进程配置:")
    print(f"   进程ID: {args.worker_id}/{args.total_workers}")
    print(f"   负责季度: {[f'{year}Q{q}' for year, q in quarters]}")
    
    # 初始化研究上下文
    try:
        context = ResearchContext()
        print(f"✅ 研究上下文初始化成功")
    except Exception as e:
        print(f"❌ 研究上下文初始化失败: {e}")
        sys.exit(1)
    
    # 计算分配的季度
    for year, quarter in quarters:
        try:
            calculate_g_factors_for_quarter(context, year, quarter, args.worker_id, args.total_workers)
        except Exception as e:
            print(f"❌ 工作进程 {args.worker_id} 计算 {year}年Q{quarter} 失败: {e}")
            continue
    
    print(f"🎉 工作进程 {args.worker_id} 完成所有分配任务!")


if __name__ == "__main__":
    main()
