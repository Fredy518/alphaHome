#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单回测示例

演示如何使用 MemoryDataProvider 进行基金组合回测。
"""

import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 导入回测框架
from alphahome.fund_backtest import (
    BacktestEngine,
    MemoryDataProvider,
    PortfolioConfig,
)
from alphahome.fund_analysis import PerformanceAnalyzer


def _stable_hash(s: str) -> int:
    """
    生成稳定的哈希值，不受 PYTHONHASHSEED 影响
    
    Args:
        s: 输入字符串
    
    Returns:
        稳定的整数哈希值
    """
    return int(hashlib.md5(s.encode()).hexdigest(), 16) % (2**32)


def generate_mock_nav_data(
    fund_ids: list,
    start_date: str,
    end_date: str,
    initial_nav: float = 1.0,
    annual_return: float = 0.08,
    volatility: float = 0.15
) -> pd.DataFrame:
    """
    生成模拟净值数据
    
    Args:
        fund_ids: 基金代码列表
        start_date: 开始日期
        end_date: 结束日期
        initial_nav: 初始净值
        annual_return: 年化收益率
        volatility: 年化波动率
    
    Returns:
        净值面板 DataFrame
    """
    dates = pd.date_range(start_date, end_date, freq='B')  # 工作日
    n_days = len(dates)
    
    # 日收益率参数
    daily_return = annual_return / 252
    daily_vol = volatility / np.sqrt(252)
    
    nav_data = {}
    for fund_id in fund_ids:
        # 生成随机收益率（使用稳定哈希作为种子，确保跨进程可复现）
        np.random.seed(_stable_hash(fund_id))
        returns = np.random.normal(daily_return, daily_vol, n_days)
        nav = initial_nav * np.cumprod(1 + returns)
        nav_data[fund_id] = nav
    
    return pd.DataFrame(nav_data, index=dates)


def generate_mock_rebalance_records(
    portfolio_id: str,
    fund_ids: list,
    rebalance_dates: list,
    weights_list: list = None
) -> pd.DataFrame:
    """
    生成模拟调仓记录
    
    Args:
        portfolio_id: 组合ID
        fund_ids: 基金代码列表
        rebalance_dates: 调仓日期列表
        weights_list: 权重列表，每个元素是一个权重字典
    
    Returns:
        调仓记录 DataFrame
    """
    records = []
    
    for i, dt in enumerate(rebalance_dates):
        if weights_list and i < len(weights_list):
            weights = weights_list[i]
        else:
            # 默认等权
            n = len(fund_ids)
            weights = {fid: 1.0 / n for fid in fund_ids}
        
        for fund_id, weight in weights.items():
            records.append({
                'rebalance_date': pd.Timestamp(dt),
                'fund_id': fund_id,
                'fund_name': f'基金{fund_id}',
                'target_weight': weight,
            })
    
    return pd.DataFrame(records)


def run_simple_backtest():
    """运行简单回测示例"""
    
    print("=" * 60)
    print("基金组合回测示例")
    print("=" * 60)
    
    # 1. 定义基金和日期范围
    fund_ids = ['000001.OF', '000002.OF', '000003.OF']
    start_date = '2023-01-01'
    end_date = '2023-12-31'
    
    # 2. 生成模拟数据
    print("\n[1] 生成模拟数据...")
    nav_panel = generate_mock_nav_data(
        fund_ids=fund_ids,
        start_date=start_date,
        end_date=end_date,
        initial_nav=1.0,
        annual_return=0.10,
        volatility=0.18
    )
    print(f"    净值面板: {nav_panel.shape[0]} 天 x {nav_panel.shape[1]} 只基金")
    
    # 3. 生成调仓记录
    rebalance_dates = ['2023-01-03', '2023-04-03', '2023-07-03', '2023-10-09']
    weights_list = [
        {'000001.OF': 0.4, '000002.OF': 0.3, '000003.OF': 0.3},  # Q1
        {'000001.OF': 0.3, '000002.OF': 0.4, '000003.OF': 0.3},  # Q2
        {'000001.OF': 0.3, '000002.OF': 0.3, '000003.OF': 0.4},  # Q3
        {'000001.OF': 0.33, '000002.OF': 0.33, '000003.OF': 0.34},  # Q4
    ]
    
    rebalance_records = generate_mock_rebalance_records(
        portfolio_id='test_portfolio',
        fund_ids=fund_ids,
        rebalance_dates=rebalance_dates,
        weights_list=weights_list
    )
    print(f"    调仓记录: {len(rebalance_dates)} 次调仓")
    
    # 4. 生成费率数据
    fee_df = pd.DataFrame({
        'fund_id': fund_ids,
        'purchase_fee': [0.015, 0.012, 0.01],  # 申购费率
        'redeem_fee': [0.005, 0.005, 0.005],   # 赎回费率
    })
    
    # 5. 创建数据提供者
    print("\n[2] 初始化数据提供者...")
    data_provider = MemoryDataProvider(
        nav_panel=nav_panel,
        rebalance_records={'test_portfolio': rebalance_records},
        fee_df=fee_df.set_index('fund_id')
    )
    
    # 6. 创建回测引擎
    print("\n[3] 创建回测引擎...")
    engine = BacktestEngine(data_provider)
    
    # 7. 添加组合配置
    config = PortfolioConfig(
        portfolio_id='test_portfolio',
        portfolio_name='测试组合',
        initial_cash=1000000.0,  # 100万初始资金
        setup_date='2023-01-03',
        rebalance_delay=2,          # T+2 申购确认
        purchase_fee_rate=0.0015,   # 0.15% 申购费（1折后）
        redeem_fee_rate=0.0,        # 0% 赎回费（免费）
        management_fee=0.005,       # 0.5% 年化管理费
        rebalance_effective_delay=1 # T+1 调仓生效
    )
    engine.add_portfolio(config)
    
    # 8. 运行回测（使用复权净值处理分红）
    print("\n[4] 运行回测...")
    results = engine.run(start_date, end_date, use_adj_nav=True)
    
    # 9. 输出结果
    print("\n[5] 回测结果:")
    print("-" * 40)
    
    result = results['test_portfolio']
    metrics = result.metrics
    
    print(f"    累计收益率: {metrics.get('cumulative_return', 0):.2%}")
    print(f"    年化收益率: {metrics.get('annualized_return', 0):.2%}")
    print(f"    年化波动率: {metrics.get('annualized_volatility', 0):.2%}")
    print(f"    最大回撤:   {metrics.get('max_drawdown', 0):.2%}")
    print(f"    夏普比率:   {metrics.get('sharpe_ratio', 0):.2f}")
    print(f"    回测天数:   {metrics.get('total_days', 0)}")
    
    # 10. 净值序列
    print("\n[6] 净值序列 (最后5天):")
    print(result.nav_series.tail())
    
    # 11. 交易记录
    if not result.trades.empty:
        print(f"\n[7] 交易记录: {len(result.trades)} 笔")
        print(result.trades[['fund_id', 'side', 'amount', 'units', 'fee', 'settle_date']].head(10))
    
    print("\n" + "=" * 60)
    print("回测完成!")
    print("=" * 60)
    
    return result


if __name__ == '__main__':
    run_simple_backtest()
