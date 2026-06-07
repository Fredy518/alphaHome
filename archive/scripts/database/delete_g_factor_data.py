#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
删除G因子数据脚本
用于清理指定日期范围的G因子数据

使用方法：
python scripts/database/delete_g_factor_data.py --start_date 2008-01-01 --end_date 2025-12-31
python scripts/database/delete_g_factor_data.py --start_date 2008-01-01  # 删除2008年及以后的所有数据
"""

import sys
import os
import argparse
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.tools.context import ResearchContext


def delete_g_factor_data(context, start_date: str, end_date: str = None):
    """删除指定日期范围的G因子数据"""
    
    # 构建删除条件
    if end_date:
        where_condition = "calc_date >= %s AND calc_date <= %s"
        params = (start_date, end_date)
        print(f"🗑️ 准备删除 {start_date} 到 {end_date} 的G因子数据")
    else:
        where_condition = "calc_date >= %s"
        params = (start_date,)
        print(f"🗑️ 准备删除 {start_date} 及以后的所有G因子数据")
    
    try:
        # 先查询要删除的数据量
        count_query = f"""
        SELECT COUNT(*) as total_count,
               COUNT(DISTINCT calc_date) as date_count,
               COUNT(DISTINCT ts_code) as stock_count,
               MIN(calc_date) as min_date,
               MAX(calc_date) as max_date
        FROM pgs_factors.g_factor 
        WHERE {where_condition}
        """
        
        print("📊 查询待删除数据统计...")
        results = context.db_manager.fetch_sync(count_query, params)
        
        if results and len(results) > 0:
            row = results[0]
            
            # 处理不同的结果格式（字典或元组）
            if isinstance(row, dict):
                total_count = int(row.get('total_count', 0))
                date_count = int(row.get('date_count', 0))
                stock_count = int(row.get('stock_count', 0))
                min_date = row.get('min_date')
                max_date = row.get('max_date')
            else:
                # 元组格式
                total_count = int(row[0]) if row[0] is not None else 0
                date_count = int(row[1]) if row[1] is not None else 0
                stock_count = int(row[2]) if row[2] is not None else 0
                min_date = row[3]
                max_date = row[4]
            
            print(f"📈 数据统计:")
            print(f"   总记录数: {total_count:,}")
            print(f"   计算日期数: {date_count:,}")
            print(f"   股票数: {stock_count:,}")
            print(f"   日期范围: {min_date} ~ {max_date}")
            
            if total_count == 0:
                print("✅ 没有找到需要删除的数据")
                return
            
            # 确认删除
            print(f"\n⚠️ 警告: 即将删除 {total_count:,} 条G因子记录!")
            confirm = input("确认删除? (输入 'YES' 确认): ")
            
            if confirm != 'YES':
                print("❌ 操作已取消")
                return
            
            # 执行删除
            delete_query = f"DELETE FROM pgs_factors.g_factor WHERE {where_condition}"
            
            print("🗑️ 正在删除数据...")
            start_time = datetime.now()
            
            # 执行删除操作
            context.db_manager.execute_sync(delete_query, params)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"✅ 删除完成!")
            print(f"⏰ 耗时: {duration:.2f} 秒")
            print(f"📊 已删除 {total_count:,} 条记录")
            
        else:
            print("❌ 查询失败")
            
    except Exception as e:
        print(f"❌ 删除操作失败: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description='删除G因子数据')
    parser.add_argument('--start_date', type=str, required=True, 
                       help='开始日期 (YYYY-MM-DD格式)')
    parser.add_argument('--end_date', type=str, 
                       help='结束日期 (YYYY-MM-DD格式，可选)')
    parser.add_argument('--dry_run', action='store_true',
                       help='试运行模式，只查询不删除')
    
    args = parser.parse_args()
    
    # 验证日期格式
    try:
        datetime.strptime(args.start_date, '%Y-%m-%d')
        if args.end_date:
            datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError:
        print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
        sys.exit(1)
    
    # 验证日期范围
    if args.end_date and args.start_date > args.end_date:
        print("❌ 开始日期不能晚于结束日期")
        sys.exit(1)
    
    print("🚀 G因子数据删除工具")
    print("=" * 50)
    print(f"📅 删除范围: {args.start_date}" + (f" ~ {args.end_date}" if args.end_date else " 及以后"))
    print(f"🔍 模式: {'试运行' if args.dry_run else '实际删除'}")
    print(f"🕐 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 初始化研究上下文
    try:
        context = ResearchContext()
        print("✅ 研究上下文初始化成功")
    except Exception as e:
        print(f"❌ 研究上下文初始化失败: {e}")
        sys.exit(1)
    
    if args.dry_run:
        print("🔍 试运行模式 - 只查询不删除")
        try:
            # 试运行：只查询统计信息，不执行删除
            if args.end_date:
                where_condition = "calc_date >= %s AND calc_date <= %s"
                params = (args.start_date, args.end_date)
            else:
                where_condition = "calc_date >= %s"
                params = (args.start_date,)
            
            count_query = f"""
            SELECT COUNT(*) as total_count,
                   COUNT(DISTINCT calc_date) as date_count,
                   COUNT(DISTINCT ts_code) as stock_count,
                   MIN(calc_date) as min_date,
                   MAX(calc_date) as max_date
            FROM pgs_factors.g_factor 
            WHERE {where_condition}
            """
            
            results = context.db_manager.fetch_sync(count_query, params)
            
            if results and len(results) > 0:
                row = results[0]
                
                # 处理不同的结果格式
                if isinstance(row, dict):
                    total_count = int(row.get('total_count', 0))
                    date_count = int(row.get('date_count', 0))
                    stock_count = int(row.get('stock_count', 0))
                    min_date = row.get('min_date')
                    max_date = row.get('max_date')
                else:
                    total_count = int(row[0]) if row[0] is not None else 0
                    date_count = int(row[1]) if row[1] is not None else 0
                    stock_count = int(row[2]) if row[2] is not None else 0
                    min_date = row[3]
                    max_date = row[4]
                
                print(f"📈 试运行结果:")
                print(f"   总记录数: {total_count:,}")
                print(f"   计算日期数: {date_count:,}")
                print(f"   股票数: {stock_count:,}")
                print(f"   日期范围: {min_date} ~ {max_date}")
                
                if total_count == 0:
                    print("✅ 没有找到需要删除的数据")
                else:
                    print(f"⚠️ 试运行完成，发现 {total_count:,} 条记录待删除")
            else:
                print("❌ 试运行查询失败")
                
        except Exception as e:
            print(f"❌ 试运行失败: {e}")
        return
    
    # 执行删除
    try:
        delete_g_factor_data(context, args.start_date, args.end_date)
    except Exception as e:
        print(f"❌ 删除失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
