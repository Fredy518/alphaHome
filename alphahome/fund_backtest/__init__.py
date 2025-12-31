"""
基金组合回测框架 (fund_backtest)

本模块专门用于场外基金组合的历史回测，提供完整的回测引擎和数据接口。

支持功能：
- 场外基金组合回测
- 按调仓记录生成组合净值
- 可扩展手续费/申购赎回确认规则/分红处理
- 支持多组合并行
- 输出净值与绩效指标

主要组件：
- BacktestEngine: 回测引擎，协调数据加载、交易执行、估值计算
- Portfolio: 组合管理，包含现金和基金持仓
- DataProvider: 数据提供者抽象接口
- MemoryDataProvider: 内存数据提供者实现

绩效分析功能已独立为 alphahome.fund_analysis 模块，请使用：
    from alphahome.fund_analysis import PerformanceAnalyzer

使用示例：
    from alphahome.fund_backtest import BacktestEngine, PortfolioConfig
    from alphahome.fund_analysis import PerformanceAnalyzer
    
    engine = BacktestEngine(data_provider)
    engine.add_portfolio(config)
    results = engine.run(start_date, end_date)
"""

from .core.engine import BacktestEngine, PortfolioConfig, BacktestResult
from .core.portfolio import Portfolio, Position
from .core.order import Order, OrderSide, OrderStatus
from .data.provider import DataProvider
from .data.memory_provider import MemoryDataProvider

__all__ = [
    # 核心组件
    'BacktestEngine',
    'PortfolioConfig',
    'BacktestResult',
    'Portfolio',
    'Position', 
    'Order',
    'OrderSide',
    'OrderStatus',
    # 数据层
    'DataProvider',
    'MemoryDataProvider',
]
