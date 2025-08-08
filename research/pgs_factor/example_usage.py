"""
P/G/S因子计算系统使用示例
=========================

展示如何使用P/G/S因子计算系统进行因子计算和分析
"""

import sys
import os
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import logging
from datetime import datetime, timedelta
from research.tools.context import ResearchContext
from research.pgs_factor import PGSFactorCalculator

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def calculate_weekly_factors(start_date: str, end_date: str, output_dir: str):
    """
    计算每周五的P/G/S因子
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        output_dir: 输出目录
    """
    # 初始化研究上下文
    context = ResearchContext()
    
    # 创建P/G/S因子计算器
    calculator = PGSFactorCalculator(context)
    
    # 获取交易日历
    # 使用交易日历（tushare.others_calendar）获取交易日
    from research.pgs_factor.data_loader import PGSDataLoader
    loader = PGSDataLoader(context)
    trade_cal = loader.get_trading_dates(start_date, end_date)
    trade_dates = trade_cal[trade_cal['is_open'] == 1][['cal_date']].rename(columns={'cal_date': 'trade_date'})
    
    if trade_dates.empty:
        print("No trading dates found")
        return
    
    # 转换为datetime并筛选周五
    trade_dates['trade_date'] = pd.to_datetime(trade_dates['trade_date'])
    friday_dates = trade_dates[trade_dates['trade_date'].dt.dayofweek == 4]['trade_date']
    
    print(f"Found {len(friday_dates)} Fridays to calculate")
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 计算每个周五的因子
    all_factors = []
    for trade_date in friday_dates:
        date_str = trade_date.strftime('%Y-%m-%d')
        print(f"\nCalculating factors for {date_str}...")
        
        try:
            # 计算因子（这里只计算部分股票作为示例）
            sample_stocks = get_sample_stocks(context, date_str, n=50)
            
            factors_df = calculator.calculate_factors(date_str, stocks=sample_stocks)
            
            if not factors_df.empty:
                # 添加日期标记
                factors_df['calc_date'] = date_str
                all_factors.append(factors_df)
                
                # 保存当日因子
                output_file = os.path.join(output_dir, f'pgs_factors_{date_str}.csv')
                calculator.save_factors(factors_df, output_file)
                
                # 打印统计信息
                print_factor_statistics(factors_df, date_str)
                
        except Exception as e:
            print(f"Error calculating factors for {date_str}: {e}")
            continue
    
    # 合并所有因子数据
    if all_factors:
        combined_factors = pd.concat(all_factors, ignore_index=True)
        combined_file = os.path.join(output_dir, 'pgs_factors_combined.csv')
        combined_factors.to_csv(combined_file, index=False, encoding='utf-8-sig')
        print(f"\nCombined factors saved to {combined_file}")
    
    # 关闭连接
    context.close()

def get_sample_stocks(context, trade_date: str, n: int = 50) -> list:
    """
    获取样本股票列表
    
    Args:
        context: ResearchContext实例
        trade_date: 交易日期
        n: 样本数量
        
    Returns:
        股票代码列表
    """
    # 获取当日交易的股票，选择市值最大的N只
    query = """
    SELECT ts_code, total_mv
    FROM tushare.stock_dailybasic
    WHERE trade_date = %(trade_date)s
    AND total_mv IS NOT NULL
    ORDER BY total_mv DESC
    LIMIT %(n)s
    """
    
    df = context.query_dataframe(query, {'trade_date': trade_date, 'n': n})
    return df['ts_code'].tolist() if not df.empty else []

def print_factor_statistics(factors_df: pd.DataFrame, date_str: str):
    """
    打印因子统计信息
    
    Args:
        factors_df: 因子数据
        date_str: 日期字符串
    """
    print(f"\n=== Factor Statistics for {date_str} ===")
    print(f"Total stocks: {len(factors_df)}")
    
    # P因子统计
    if 'p_score' in factors_df.columns:
        p_valid = factors_df['p_score'].notna().sum()
        p_mean = factors_df['p_score'].mean()
        p_std = factors_df['p_score'].std()
        print(f"P Factor - Valid: {p_valid}, Mean: {p_mean:.2f}, Std: {p_std:.2f}")
    
    # G因子统计
    if 'g_score' in factors_df.columns:
        g_valid = factors_df['g_score'].notna().sum()
        g_mean = factors_df['g_score'].mean()
        g_std = factors_df['g_score'].std()
        print(f"G Factor - Valid: {g_valid}, Mean: {g_mean:.2f}, Std: {g_std:.2f}")
    
    # S因子统计
    if 's_score' in factors_df.columns:
        s_valid = factors_df['s_score'].notna().sum()
        s_mean = factors_df['s_score'].mean()
        s_std = factors_df['s_score'].std()
        print(f"S Factor - Valid: {s_valid}, Mean: {s_mean:.2f}, Std: {s_std:.2f}")
    
    # Top 10股票
    if 'total_score' in factors_df.columns:
        top10 = factors_df.nlargest(10, 'total_score')[['ts_code', 'total_score', 'p_score', 'g_score', 's_score']]
        print("\nTop 10 Stocks by Total Score:")
        print(top10.to_string(index=False))

def analyze_factor_performance(output_dir: str):
    """
    分析因子表现
    
    Args:
        output_dir: 因子数据目录
    """
    # 读取合并的因子数据
    combined_file = os.path.join(output_dir, 'pgs_factors_combined.csv')
    
    if not os.path.exists(combined_file):
        print(f"Combined factor file not found: {combined_file}")
        return
    
    factors_df = pd.read_csv(combined_file)
    factors_df['calc_date'] = pd.to_datetime(factors_df['calc_date'])
    
    print("\n=== Factor Performance Analysis ===")
    
    # 因子覆盖率分析
    coverage = factors_df.groupby('calc_date').agg({
        'p_score': lambda x: x.notna().sum() / len(x) * 100,
        'g_score': lambda x: x.notna().sum() / len(x) * 100,
        's_score': lambda x: x.notna().sum() / len(x) * 100
    })
    
    print("\nFactor Coverage (%):")
    print(coverage)
    
    # 因子相关性分析
    correlation = factors_df[['p_score', 'g_score', 's_score']].corr()
    print("\nFactor Correlation Matrix:")
    print(correlation)
    
    # 因子稳定性分析
    stability = factors_df.groupby('calc_date')[['p_score', 'g_score', 's_score']].std()
    print("\nFactor Stability (Std by Date):")
    print(stability)

def main():
    """主函数"""
    # 设置参数
    start_date = '2024-01-01'
    end_date = '2024-03-31'
    output_dir = 'output/pgs_factors'
    
    print("=== P/G/S Factor Calculation System ===")
    print(f"Period: {start_date} to {end_date}")
    print(f"Output: {output_dir}")
    
    # 计算因子
    calculate_weekly_factors(start_date, end_date, output_dir)
    
    # 分析表现
    analyze_factor_performance(output_dir)
    
    print("\n=== Calculation Complete ===")

if __name__ == "__main__":
    main()
