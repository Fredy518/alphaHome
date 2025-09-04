#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
G因子计算进度监控脚本
实时监控各个年份的计算进度

使用方法：
python scripts/analysis/monitor_g_factor_progress.py --start_year 2020 --end_year 2024
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


def get_g_factor_progress(context, start_year: int, end_year: int):
    """获取G因子计算进度"""
    try:
        query = """
        SELECT 
            EXTRACT(YEAR FROM calc_date) as year,
            COUNT(DISTINCT calc_date) as completed_dates,
            COUNT(*) as total_records,
            MIN(calc_date) as first_date,
            MAX(calc_date) as last_date
        FROM pgs_factors.g_factor 
        WHERE EXTRACT(YEAR FROM calc_date) BETWEEN %s AND %s
        GROUP BY EXTRACT(YEAR FROM calc_date)
        ORDER BY year
        """
        
        results = context.db_manager.fetch_sync(query, (start_year, end_year))
        
        progress_data = []
        for row in results:
            year, completed_dates, total_records, first_date, last_date = row
            progress_data.append({
                'year': int(year),
                'completed_dates': completed_dates,
                'total_records': total_records,
                'first_date': first_date,
                'last_date': last_date
            })
        
        return progress_data
                
    except Exception as e:
        print(f"获取进度数据失败: {e}")
        return []


def estimate_total_trading_days(year: int) -> int:
    """估算指定年份的交易日数量"""
    # 简单估算：每年约250个交易日
    return 250


def format_progress_bar(completed: int, total: int, width: int = 30) -> str:
    """格式化进度条"""
    if total == 0:
        return "[" + " " * width + "] 0.0%"
    
    percentage = completed / total
    filled = int(width * percentage)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {percentage:.1%}"


def main():
    parser = argparse.ArgumentParser(description='G因子计算进度监控')
    parser.add_argument('--start_year', type=int, default=2020, help='开始年份')
    parser.add_argument('--end_year', type=int, default=2024, help='结束年份')
    parser.add_argument('--refresh', type=int, default=30, help='刷新间隔秒数 (默认: 30)')
    
    args = parser.parse_args()
    
    try:
        context = ResearchContext()
        print("✅ 研究上下文初始化成功")
    except Exception as e:
        print(f"❌ 研究上下文初始化失败: {e}")
        sys.exit(1)
    
    print("📊 G因子计算进度监控")
    print("=" * 80)
    print(f"📅 监控年份范围: {args.start_year}-{args.end_year}")
    print(f"🔄 刷新间隔: {args.refresh}秒")
    print(f"🕐 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        while True:
            # 清屏 (Windows)
            if os.name == 'nt':
                os.system('cls')
            else:
                os.system('clear')
            
            print("📊 G因子计算进度监控")
            print("=" * 80)
            print(f"🕐 更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
            # 获取进度数据
            progress_data = get_g_factor_progress(context, args.start_year, args.end_year)
            
            if not progress_data:
                print("⚠️ 暂无进度数据")
                time.sleep(args.refresh)
                continue
            
            # 显示进度
            total_completed_dates = 0
            total_estimated_dates = 0
            total_records = 0
            
            print(f"{'年份':<6} {'进度条':<35} {'完成日期':<8} {'总记录':<10} {'时间范围'}")
            print("-" * 80)
            
            for data in progress_data:
                year = data['year']
                completed_dates = data['completed_dates']
                total_records = data['total_records']
                first_date = data['first_date']
                last_date = data['last_date']
                
                # 估算该年份的总交易日数
                estimated_dates = estimate_total_trading_days(year)
                
                # 格式化进度条
                progress_bar = format_progress_bar(completed_dates, estimated_dates)
                
                # 时间范围
                time_range = f"{first_date} ~ {last_date}" if first_date and last_date else "N/A"
                
                print(f"{year:<6} {progress_bar:<35} {completed_dates:<8} {total_records:<10} {time_range}")
                
                total_completed_dates += completed_dates
                total_estimated_dates += estimated_dates
                total_records += total_records
            
            print("-" * 80)
            
            # 总体进度
            overall_progress = format_progress_bar(total_completed_dates, total_estimated_dates)
            print(f"{'总计':<6} {overall_progress:<35} {total_completed_dates:<8} {total_records:<10}")
            
            # 估算剩余时间
            if total_completed_dates > 0:
                remaining_dates = total_estimated_dates - total_completed_dates
                if remaining_dates > 0:
                    # 简单估算：假设每天需要1分钟计算
                    estimated_remaining_minutes = remaining_dates
                    estimated_remaining_hours = estimated_remaining_minutes / 60
                    print(f"\n⏱️ 预计剩余时间: {estimated_remaining_hours:.1f}小时")
            
            print(f"\n🔄 下次刷新: {args.refresh}秒后 (按Ctrl+C退出)")
            
            time.sleep(args.refresh)
            
    except KeyboardInterrupt:
        print("\n\n👋 监控已停止")
    except Exception as e:
        print(f"\n❌ 监控出错: {e}")


if __name__ == "__main__":
    main()
