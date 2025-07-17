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

# 结果分析增强
from .analyzers.enhanced_analyzer import EnhancedAnalyzer

# 数据源扩展
from .data.feeds import PostgreSQLDataFeed
from .execution.batch_loader import BatchDataLoader

# 执行增强工具
from .execution.parallel_runner import ParallelBacktestRunner

# 缓存系统
from .utils.cache_manager import CacheManager
from .utils.performance_monitor import PerformanceMonitor

__version__ = "1.0.0"
__all__ = [
    # 数据源
    "PostgreSQLDataFeed",
    # 执行工具
    "ParallelBacktestRunner",
    "BatchDataLoader",
    # 缓存和性能
    "CacheManager",
    "PerformanceMonitor",
    # 分析工具
    "EnhancedAnalyzer",
]

# 使用说明
"""
核心用例：大规模并行回测

该插件最核心的价值是提供大规模并行回测能力。

完整示例请参考 `alphahome/bt_extensions/README.md`。

基本启动代码片段:

if __name__ == '__main__':
    import multiprocessing as mp
    from datetime import date
    from alphahome.bt_extensions.execution.parallel_runner import ParallelBacktestRunner
    # from your_project.strategies import MyStrategy # 导入你的策略

    # 确保在Windows上多进程正常工作
    mp.freeze_support()

    # 定义回测参数
    stock_codes = ['000001.SZ', '000002.SZ', '600036.SH', '600519.SH']
    start_date = date(2023, 1, 1)
    end_date = date(2023, 12, 31)

    # 初始化并运行
    runner = ParallelBacktestRunner()
    results = runner.run_parallel_backtests(
        stock_codes=stock_codes,
        strategy_class=MyStrategy, # 替换为你的策略
        start_date=start_date,
        end_date=end_date
    )

    # 查看摘要
    print(results.get('summary'))
"""
