#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
多股票回测示例 - 轻量级设计

展示如何使用backtrader进行多股票回测，无多余wrapper
"""

import sys
import os
from datetime import date
import backtrader as bt

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from alphahome.backtesting import PostgreSQLDataFeed
from alphahome.backtesting.strategies.examples.dual_moving_average import DualMovingAverageStrategy
from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import get_database_url


def main():
    """
    多股票轻量级回测
    """
    print("多股票轻量级回测示例")
    print("=" * 40)
    
    # 创建数据库管理器 - 使用统一的配置管理器
    connection_string = get_database_url()
    if not connection_string:
        print("未找到数据库配置，使用默认连接字符串")
        connection_string = "postgresql://postgres:password@localhost:5432/tusharedb"
    
    db_manager = DBManager(connection_string)
    
    # 股票列表
    stocks = ['000001.SZ', '000002.SZ', '600000.SH']
    
    # 创建backtrader Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加多只股票数据源
    for i, ts_code in enumerate(stocks):
        data_feed = PostgreSQLDataFeed(
            ts_code=ts_code,
            db_manager=db_manager,
            start_date=date(2023, 1, 1),
            end_date=date(2023, 12, 31),
            table_name='tushare_stock_daily',
            name=f'stock_{i}_{ts_code[:6]}'  # 为每个数据源命名
        )
        cerebro.adddata(data_feed)
        print(f"添加股票数据: {ts_code}")
    
    # 添加策略
    cerebro.addstrategy(DualMovingAverageStrategy,
                       fast_period=5,
                       slow_period=20,
                       printlog=False)  # 关闭详细日志避免输出过多
    
    # 设置broker
    cerebro.broker.setcash(500000.0)  # 更多资金用于多股票
    cerebro.broker.setcommission(commission=0.001)
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    
    print(f"起始资金: {cerebro.broker.getvalue():,.2f}")
    
    # 运行回测
    results = cerebro.run()
    
    # 获取分析结果
    strategy = results[0]
    
    final_value = cerebro.broker.getvalue()
    total_return = (final_value - 500000.0) / 500000.0
    
    print("\n=== 回测结果 ===")
    print(f"最终价值: {final_value:,.2f}")
    print(f"总收益: {total_return:.2%}")
    
    # 输出分析器结果
    sharpe_ratio = strategy.analyzers.sharpe.get_analysis().get('sharperatio', 0)
    max_drawdown = strategy.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0)
    
    print(f"夏普比率: {sharpe_ratio:.3f}")
    print(f"最大回撤: {max_drawdown:.2%}")
    
    print("多股票回测完成！")


if __name__ == "__main__":
    main() 