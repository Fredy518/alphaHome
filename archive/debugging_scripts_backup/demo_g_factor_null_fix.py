#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
演示G因子空值处理修复效果
展示修复前后的差异
"""

import sys
import os
import pandas as pd
import numpy as np

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.pgs_factor.processors.production_g_factor_calculator import ProductionGFactorCalculator


def demo_null_handling_fix():
    """演示空值处理修复效果"""
    print("🔧 G因子空值处理修复演示")
    print("=" * 60)
    
    # 创建测试数据 - 模拟真实场景
    test_data = pd.DataFrame({
        'ts_code': ['股票A', '股票B', '股票C', '股票D', '股票E'],
        'g_efficiency_surprise': [1.2, 0.8, np.nan, 1.5, 0.9],  # 股票C的ES为空
        'g_efficiency_momentum': [0.5, np.nan, 0.7, 0.3, np.nan],  # 股票B和E的EM为空
        'g_revenue_momentum': [2.1, 1.8, 2.3, 1.9, 2.0],  # 所有股票都有RM
        'g_profit_momentum': [1.5, 1.2, 1.8, 1.3, 1.6]   # 所有股票都有PM
    })
    
    print("📊 测试数据:")
    print(test_data)
    print()
    
    # 创建计算器实例
    class MockContext:
        def __init__(self):
            self.db_manager = None
    
    context = MockContext()
    calculator = ProductionGFactorCalculator(context)
    
    # 计算排名和G评分
    print("🔢 计算排名和G评分...")
    ranked_data = calculator._calculate_cross_sectional_rankings(test_data.copy())
    g_scores = calculator._calculate_final_g_score(ranked_data)
    
    # 显示结果
    result_df = pd.DataFrame({
        '股票代码': test_data['ts_code'],
        'ES因子': test_data['g_efficiency_surprise'],
        'EM因子': test_data['g_efficiency_momentum'],
        'RM因子': test_data['g_revenue_momentum'],
        'PM因子': test_data['g_profit_momentum'],
        'ES排名': ranked_data['rank_es'],
        'EM排名': ranked_data['rank_em'],
        'RM排名': ranked_data['rank_rm'],
        'PM排名': ranked_data['rank_pm'],
        'G评分': g_scores
    })
    
    print("📈 计算结果:")
    print(result_df.round(2))
    print()
    
    # 分析结果
    print("📊 结果分析:")
    for idx, row in result_df.iterrows():
        stock_code = row['股票代码']
        g_score = row['G评分']
        
        # 统计有效因子
        valid_factors = []
        if pd.notna(row['ES因子']):
            valid_factors.append('ES')
        if pd.notna(row['EM因子']):
            valid_factors.append('EM')
        if pd.notna(row['RM因子']):
            valid_factors.append('RM')
        if pd.notna(row['PM因子']):
            valid_factors.append('PM')
        
        print(f"  {stock_code}:")
        print(f"    有效因子: {len(valid_factors)}/4 ({', '.join(valid_factors)})")
        print(f"    G评分: {g_score:.2f}")
        
        if len(valid_factors) < 4:
            print(f"    ✅ 部分因子为空，但G评分基于{len(valid_factors)}个有效因子计算")
        else:
            print(f"    ✅ 所有因子有效，G评分正常计算")
        print()
    
    print("🎯 修复效果总结:")
    print("  - 空值因子不参与排名计算")
    print("  - 部分因子为空时，G评分基于有效因子计算")
    print("  - 确保G评分的连续性和合理性")
    print("  - 避免空值因子导致G评分为0的问题")


if __name__ == "__main__":
    demo_null_handling_fix()
