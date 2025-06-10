#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BT Extensions - Backtrader 增强插件

为 Backtrader 提供数据库连接、性能优化和增强功能的轻量级插件集合。

核心功能：
1. 数据库数据源 - 连接PostgreSQL等数据库到Backtrader
2. 批量数据加载 - 优化大规模数据加载性能
3. 智能缓存系统 - 减少重复数据查询
4. 并行执行工具 - 支持多股票、多策略并行回测
5. 结果增强分析 - 扩展Backtrader的分析功能
"""

# 数据源扩展
from .data.feeds import PostgreSQLDataFeed

# 执行增强工具
from .execution.parallel_runner import ParallelBacktestRunner
from .execution.batch_loader import BatchDataLoader

# 缓存系统
from .utils.cache_manager import CacheManager
from .utils.performance_monitor import PerformanceMonitor

# 结果分析增强
from .analyzers.enhanced_analyzer import EnhancedAnalyzer

__version__ = "1.0.0"
__all__ = [
    # 数据源
    'PostgreSQLDataFeed',
    
    # 执行工具
    'ParallelBacktestRunner', 
    'BatchDataLoader',
    
    # 缓存和性能
    'CacheManager',
    'PerformanceMonitor',
    
    # 分析工具
    'EnhancedAnalyzer'
]

# 使用说明
"""
基本使用方式（直接使用backtrader）：

import backtrader as bt
from alphahome.backtesting import PostgreSQLDataFeed
from alphahome.backtesting.strategies.examples.dual_moving_average import DualMovingAverageStrategy

# 创建Cerebro引擎
cerebro = bt.Cerebro()

# 添加数据源 (我们提供的核心价值)
data_feed = PostgreSQLDataFeed(
    ts_code='000001.SZ', 
    db_manager=db_manager,
    start_date=date(2023, 1, 1),
    end_date=date(2023, 12, 31)
)
cerebro.adddata(data_feed)

# 添加策略
cerebro.addstrategy(DualMovingAverageStrategy, fast_period=5, slow_period=20)

# 设置broker
cerebro.broker.setcash(100000.0)
cerebro.broker.setcommission(commission=0.001)

# 运行回测
results = cerebro.run()
""" 