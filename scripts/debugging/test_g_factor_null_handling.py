#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试G因子空值处理逻辑
验证动态权重计算是否正确处理空值因子
"""

import sys
import os
import pandas as pd
import numpy as np

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.pgs_factor.processors.production_g_factor_calculator import ProductionGFactorCalculator


def test_null_handling():
    """测试空值处理逻辑"""
    print("🧪 测试G因子空值处理逻辑")
    print("=" * 50)
    
    # 创建测试数据
    test_data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ', '000003.SZ', '000004.SZ', '000005.SZ'],
        'g_efficiency_surprise': [1.2, 0.8, np.nan, 1.5, 0.9],  # 包含空值
        'g_efficiency_momentum': [0.5, np.nan, 0.7, 0.3, np.nan],  # 包含空值
        'g_revenue_momentum': [2.1, 1.8, 2.3, 1.9, 2.0],  # 无空值
        'g_profit_momentum': [1.5, 1.2, 1.8, 1.3, 1.6]   # 无空值
    })
    
    print("📊 测试数据:")
    print(test_data)
    print()
    
    # 创建计算器实例（使用模拟上下文）
    class MockContext:
        def __init__(self):
            self.db_manager = None
    
    context = MockContext()
    calculator = ProductionGFactorCalculator(context)
    
    # 测试排名计算
    print("🔢 计算排名...")
    ranked_data = calculator._calculate_cross_sectional_rankings(test_data.copy())
    
    print("📈 排名结果:")
    print(ranked_data[['ts_code', 'rank_es', 'rank_em', 'rank_rm', 'rank_pm']])
    print()
    
    # 测试最终G评分计算
    print("🎯 计算最终G评分...")
    g_scores = calculator._calculate_final_g_score(ranked_data)
    
    print("🏆 最终G评分结果:")
    result_df = pd.DataFrame({
        'ts_code': test_data['ts_code'],
        'g_score': g_scores,
        'has_es': ranked_data['g_efficiency_surprise'].notna(),
        'has_em': ranked_data['g_efficiency_momentum'].notna(),
        'has_rm': ranked_data['g_revenue_momentum'].notna(),
        'has_pm': ranked_data['g_profit_momentum'].notna()
    })
    print(result_df)
    print()
    
    # 验证逻辑
    print("✅ 验证结果:")
    for idx, row in result_df.iterrows():
        ts_code = row['ts_code']
        g_score = row['g_score']
        has_es = row['has_es']
        has_em = row['has_em']
        has_rm = row['has_rm']
        has_pm = row['has_pm']
        
        # 计算有效因子数量
        valid_factors = sum([has_es, has_em, has_rm, has_pm])
        
        print(f"  {ts_code}:")
        print(f"    有效因子: {valid_factors}/4 (ES:{has_es}, EM:{has_em}, RM:{has_rm}, PM:{has_pm})")
        print(f"    G评分: {g_score:.2f}")
        
        # 验证空值处理
        if not has_es and not has_em and not has_rm and not has_pm:
            assert g_score == 0, f"所有因子都为空时，G评分应该为0，但得到{g_score}"
            print(f"    ✅ 所有因子为空，G评分正确为0")
        elif valid_factors < 4:
            # 部分因子有效时，G评分应该基于有效因子计算，不应该为0
            assert g_score > 0, f"部分因子有效时，G评分不应该为0，但得到{g_score}"
            print(f"    ✅ 部分因子有效，G评分基于{valid_factors}个有效因子计算")
        else:
            print(f"    ✅ 所有因子有效，G评分正常计算")
        print()
    
    print("🎉 空值处理逻辑测试完成!")


if __name__ == "__main__":
    test_null_handling()
