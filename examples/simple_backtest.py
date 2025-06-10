#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
最简单的回测示例 - 轻量级设计

直接使用backtrader + PostgreSQL数据源，无多余wrapper
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
    轻量级回测 - 直接使用backtrader
    """
    print("轻量级回测示例 (直接使用backtrader)")
    print("=" * 50)
    
    # 创建数据库管理器 - 使用统一的配置管理器
    connection_string = get_database_url()
    if not connection_string:
        print("未找到数据库配置")
        print("请创建config.json文件，或使用示例数据库连接字符串")
        connection_string = "postgresql://postgres:password@localhost:5432/tusharedb"
        print(f"使用默认连接字符串: {connection_string}")
    
    db_manager = DBManager(connection_string)
    
    # 创建backtrader Cerebro引擎 (无wrapper)
    cerebro = bt.Cerebro()
    
    # 添加数据源 (我们唯一提供的价值)
    data_feed = PostgreSQLDataFeed(
        ts_code='000001.SZ',  # 平安银行
        db_manager=db_manager,
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        table_name='tushare_stock_daily'
    )
    cerebro.adddata(data_feed)
    
    # 添加策略 (直接使用backtrader)
    cerebro.addstrategy(DualMovingAverageStrategy, 
                       fast_period=5, 
                       slow_period=20,
                       printlog=True)
    
    # 设置broker (直接使用backtrader)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)
    
    print(f"起始资金: {cerebro.broker.getvalue():,.2f}")
    
    # 运行回测 (直接使用backtrader)
    results = cerebro.run()
    
    final_value = cerebro.broker.getvalue()
    total_return = (final_value - 100000.0) / 100000.0
    
    print(f"最终价值: {final_value:,.2f}")
    print(f"总收益: {total_return:.2%}")
    print("回测完成！")


if __name__ == "__main__":
    main() 