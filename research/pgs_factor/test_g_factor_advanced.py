"""
测试高级G因子计算系统
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_g_factor_calculation():
    """测试G因子计算"""
    from g_factor_advanced import AdvancedGFactorCalculator
    
    print("\n" + "="*60)
    print("测试高级G因子计算模块")
    print("="*60)
    
    # 创建测试数据
    np.random.seed(42)
    stocks = [f"{i:06d}.SZ" for i in range(1, 51)]
    
    # 当前P_score
    current_p_scores = pd.DataFrame({
        'ts_code': stocks,
        'p_score': np.random.randn(50) * 10 + 50
    })
    
    # 历史P_score（模拟8个季度的数据）
    historical_p_scores = {}
    dates = ['20250801', '20250501', '20250201', '20241101',
             '20240801', '20240501', '20240201', '20231101',
             '20230801', '20230501', '20230201', '20221101']
    
    for i, date in enumerate(dates):
        historical_p_scores[date] = pd.DataFrame({
            'ts_code': stocks,
            'p_score': np.random.randn(50) * 10 + 45 - i * 0.5
        })
    
    # 模拟财务数据
    financial_records = []
    for stock in stocks:
        for i, date in enumerate(dates[:8]):  # 使用最近8个季度
            financial_records.append({
                'ts_code': stock,
                'ann_date': date,
                'end_date': date,
                'revenue': np.random.randn() * 1000000 + 10000000 * (1 + i * 0.02),
                'n_income_attr_p': np.random.randn() * 100000 + 1000000 * (1 + i * 0.03)
            })
    financial_data = pd.DataFrame(financial_records)
    
    # 创建计算器
    calculator = AdvancedGFactorCalculator()
    
    # 计算G因子
    print("\n开始计算G因子...")
    result = calculator.calculate_g_factors(
        stocks, 
        '20250801',
        financial_data,
        current_p_scores,
        historical_p_scores
    )
    
    # 显示结果摘要
    print("\n计算结果摘要:")
    print("-" * 50)
    print(f"总股票数: {len(stocks)}")
    print(f"成功计算: {result['g_score'].notna().sum()}")
    print(f"缺失数据: {result['g_score'].isna().sum()}")
    
    # 显示前10只股票的详细结果
    print("\n前10只股票的G因子详情:")
    print("-" * 50)
    display_columns = ['ts_code', 'g_score', 'data_quality', 'valid_factors',
                      'g_efficiency_surprise', 'g_efficiency_momentum',
                      'g_revenue_momentum', 'g_profit_momentum']
    
    display_df = result.head(10)[display_columns]
    for col in display_df.columns:
        if col not in ['ts_code', 'data_quality']:
            display_df[col] = display_df[col].round(2)
    
    print(display_df.to_string(index=False))
    
    # G因子分布统计
    print("\nG因子分布统计:")
    print("-" * 50)
    print(f"平均值: {result['g_score'].mean():.2f}")
    print(f"标准差: {result['g_score'].std():.2f}")
    print(f"最小值: {result['g_score'].min():.2f}")
    print(f"25分位: {result['g_score'].quantile(0.25):.2f}")
    print(f"中位数: {result['g_score'].median():.2f}")
    print(f"75分位: {result['g_score'].quantile(0.75):.2f}")
    print(f"最大值: {result['g_score'].max():.2f}")
    
    # 数据质量分布
    print("\n数据质量分布:")
    print("-" * 50)
    quality_counts = result['data_quality'].value_counts()
    for quality, count in quality_counts.items():
        percentage = count / len(result) * 100
        print(f"{quality}: {count} ({percentage:.1f}%)")
    
    # 测试排名功能
    print("\n排名功能测试:")
    print("-" * 50)
    rank_columns = [col for col in result.columns if col.startswith('rank_')]
    if rank_columns:
        print("子因子排名范围检查:")
        for col in rank_columns:
            valid_ranks = result[col].dropna()
            if len(valid_ranks) > 0:
                print(f"  {col}: [{valid_ranks.min():.1f}, {valid_ranks.max():.1f}]")
    
    return result


def test_batch_processor():
    """测试批量处理器（简化版）"""
    print("\n" + "="*60)
    print("测试G因子批量处理器")
    print("="*60)
    
    # 创建测试数据
    stocks = [f"{i:06d}.SZ" for i in range(1, 21)]
    
    # 模拟一个简单的批量处理场景
    print("\n模拟公告触发的批量处理:")
    print("-" * 50)
    
    affected_stocks = stocks[:5]  # 假设前5只股票受影响
    print(f"受影响股票: {affected_stocks}")
    
    # 创建模拟数据
    np.random.seed(123)
    
    # 创建测试DataFrame
    test_results = pd.DataFrame({
        'ts_code': stocks,
        'g_score': np.random.randn(20) * 15 + 50,
        'data_quality': np.random.choice(['high', 'medium', 'low'], 20, p=[0.3, 0.5, 0.2]),
        'valid_factors': np.random.choice([2, 3, 4], 20, p=[0.2, 0.3, 0.5])
    })
    
    # 筛选受影响的股票
    affected_results = test_results[test_results['ts_code'].isin(affected_stocks)]
    
    print(f"\n受影响股票的G因子结果:")
    print(affected_results[['ts_code', 'g_score', 'data_quality']].to_string(index=False))
    
    # 模拟处理统计
    stats = {
        'total_stocks': len(affected_stocks),
        'processed': len(affected_results[affected_results['g_score'].notna()]),
        'failed': 0,
        'skipped': len(affected_results[affected_results['g_score'].isna()]),
        'start_time': datetime.now(),
        'end_time': datetime.now()
    }
    
    print(f"\n处理统计:")
    print(f"  总数: {stats['total_stocks']}")
    print(f"  成功: {stats['processed']}")
    print(f"  失败: {stats['failed']}")
    print(f"  跳过: {stats['skipped']}")
    print(f"  成功率: {stats['processed']/stats['total_stocks']*100:.1f}%")


def test_subfactor_analysis():
    """测试子因子分析"""
    print("\n" + "="*60)
    print("子因子相关性分析")
    print("="*60)
    
    # 生成模拟的子因子数据
    np.random.seed(456)
    n = 100
    
    # 创建有一定相关性的子因子
    base = np.random.randn(n)
    subfactors = pd.DataFrame({
        'efficiency_surprise': base + np.random.randn(n) * 0.5,
        'efficiency_momentum': base * 0.8 + np.random.randn(n) * 0.6,
        'revenue_momentum': np.random.randn(n),
        'profit_momentum': np.random.randn(n) * 1.2
    })
    
    # 计算相关性矩阵
    corr_matrix = subfactors.corr()
    
    print("\n子因子相关性矩阵:")
    print("-" * 50)
    print(corr_matrix.round(3).to_string())
    
    # 分析子因子贡献度
    print("\n子因子贡献度分析:")
    print("-" * 50)
    
    # 转换为排名
    ranks = pd.DataFrame()
    for col in subfactors.columns:
        ranks[f'rank_{col}'] = subfactors[col].rank(pct=True) * 100
    
    # 计算等权重的G_score
    g_score = ranks.mean(axis=1)
    
    # 计算每个子因子与最终G_score的相关性
    contributions = {}
    for col in ranks.columns:
        contributions[col] = ranks[col].corr(g_score)
    
    print("各子因子与最终G_score的相关性:")
    for factor, corr in sorted(contributions.items(), key=lambda x: x[1], reverse=True):
        print(f"  {factor}: {corr:.3f}")


def main():
    """主测试函数"""
    print("\n" + "="*60)
    print("高级G因子系统综合测试")
    print("="*60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. 测试G因子计算
        g_factor_results = test_g_factor_calculation()
        
        # 2. 测试批量处理器
        test_batch_processor()
        
        # 3. 测试子因子分析
        test_subfactor_analysis()
        
        print("\n" + "="*60)
        print("所有测试完成！")
        print("="*60)
        
        return g_factor_results
        
    except Exception as e:
        print(f"\n测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    results = main()
