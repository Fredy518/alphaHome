#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试修正后的PIT逻辑
"""

import sys
import os
from datetime import datetime

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.tools.context import ResearchContext
from alphahome.pit import (
    PITBalanceQuarterlyManager,
    PITFinancialIndicatorsManager,
    PITIncomeQuarterlyManager,
)

def main():
    print("🧪 测试修正后的PIT逻辑...")
    print("=" * 60)
    
    try:
        with ResearchContext() as ctx:
            print("✅ 数据库连接成功")
            
            # 清空财务指标表
            print("\n🗑️  清空财务指标表...")
            ctx.db_manager.execute_sync("TRUNCATE TABLE pgs_factors.pit_financial_indicators_mvp")
            
            # 创建PIT管理器
            manager = PITManager(ctx, batch_size=20)
            print("✅ PIT管理器初始化成功")
            
            # 测试增量模式（最近3天）
            print("\n🔄 测试PIT逻辑 (最近3天)...")
            print("📝 关键验证点:")
            print("   1. ann_date应该是真实的公告日期，不是计算日期")
            print("   2. 不同财报期的数据应该有不同的ann_date")
            print("   3. 24Q2财报不可能在2025年8月公告")
            
            start_time = datetime.now()
            
            result = manager.unified_sync_and_calculate(mode='incremental', days=3)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "=" * 60)
            print("🎉 测试完成!")
            print("=" * 60)
            
            print(f"📊 处理结果:")
            print(f"   PIT数据同步: {result['pit_sync']['processed_records']} 条记录")
            print(f"   财务指标计算: 成功 {result['indicator_calc']['success_count']}, 失败 {result['indicator_calc']['failed_count']}")
            print(f"   总耗时: {duration:.1f} 秒")
            
            if result['success']:
                print(f"\n✅ 计算成功!")
                
                # 验证ann_date的正确性
                print(f"\n🔍 验证ann_date逻辑...")
                
                # 检查ann_date分布
                ann_date_stats = ctx.query_dataframe("""
                    SELECT 
                        ann_date,
                        COUNT(*) as record_count,
                        COUNT(DISTINCT ts_code) as stock_count,
                        COUNT(DISTINCT end_date) as period_count,
                        MIN(end_date) as earliest_period,
                        MAX(end_date) as latest_period
                    FROM pgs_factors.pit_financial_indicators_mvp
                    GROUP BY ann_date
                    ORDER BY ann_date DESC
                    LIMIT 10
                """)
                
                if ann_date_stats is not None and not ann_date_stats.empty:
                    print(f"📅 公告日期分布 (前10个):")
                    for _, row in ann_date_stats.iterrows():
                        print(f"   {row['ann_date']}: {row['record_count']} 条记录, {row['stock_count']} 只股票")
                        print(f"      财报期范围: {row['earliest_period']} ~ {row['latest_period']}")
                
                # 检查是否有异常的ann_date
                recent_ann_dates = ctx.query_dataframe("""
                    SELECT DISTINCT ann_date
                    FROM pgs_factors.pit_financial_indicators_mvp
                    WHERE ann_date >= '2025-01-01'
                    ORDER BY ann_date
                """)
                
                if recent_ann_dates is not None and not recent_ann_dates.empty:
                    print(f"\n⚠️  2025年的公告日期 (应该很少或没有):")
                    for _, row in recent_ann_dates.iterrows():
                        print(f"   {row['ann_date']}")
                else:
                    print(f"\n✅ 没有发现2025年的异常公告日期")
                
                # 检查24Q2财报的ann_date
                q2_2024_data = ctx.query_dataframe("""
                    SELECT 
                        ts_code, end_date, ann_date, data_source
                    FROM pgs_factors.pit_financial_indicators_mvp
                    WHERE end_date = '2024-06-30'
                    ORDER BY ann_date
                    LIMIT 5
                """)
                
                if q2_2024_data is not None and not q2_2024_data.empty:
                    print(f"\n📊 24Q2财报样本 (应该在2024年7-10月公告):")
                    for _, row in q2_2024_data.iterrows():
                        print(f"   {row['ts_code']}: 财报期 {row['end_date']}, 公告日 {row['ann_date']}")
                
            else:
                print(f"\n❌ 测试失败: {result.get('error', '未知错误')}")
                
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
