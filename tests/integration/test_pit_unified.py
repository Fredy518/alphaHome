#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试PIT统一处理功能
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
    print("🧪 测试PIT统一处理功能...")
    print("=" * 50)
    
    try:
        with ResearchContext() as ctx:
            print("✅ 数据库连接成功")
            
            # 创建PIT管理器
            manager = PITManager(ctx, batch_size=50)
            print("✅ PIT管理器初始化成功")
            
            # 测试增量模式（较快）
            print("\n🔄 测试统一增量处理 (最近3天)...")
            start_time = datetime.now()
            
            result = manager.unified_sync_and_calculate(mode='incremental', days=3)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "=" * 50)
            print("🎉 测试完成!")
            print("=" * 50)
            
            print(f"📊 处理结果:")
            print(f"   PIT数据同步: {result['pit_sync']['processed_records']} 条记录")
            print(f"   财务指标计算: 成功 {result['indicator_calc']['success_count']}, 失败 {result['indicator_calc']['failed_count']}")
            print(f"   总耗时: {duration:.1f} 秒")
            
            if result['success']:
                print(f"\n✅ 测试成功!")
                
                # 检查是否有数据写入
                count_result = ctx.query_dataframe("""
                    SELECT COUNT(*) as count 
                    FROM pgs_factors.pit_financial_indicators_mvp
                """)
                
                if count_result is not None and not count_result.empty:
                    count = count_result.iloc[0]['count']
                    print(f"📊 财务指标表记录数: {count:,}")
                
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
