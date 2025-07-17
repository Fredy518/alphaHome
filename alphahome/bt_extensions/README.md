# `bt_extensions` - Backtrader 增强插件

`bt_extensions` 是为业界标准的回测框架 Backtrader 提供的一套生产级增强插件。它旨在解决 Backtrader 在处理大规模数据、并行回测和深度结果分析方面的不足，使其能够胜任复杂、专业的量化研究任务。

## 核心功能

- **高性能数据加载**: 通过 `BatchDataLoader` 和 `AlphaDataTool` 集成，实现高效的批量数据预加载和缓存，极大减少回测过程中的 I/O 等待。
- **大规模并行回测**: `ParallelBacktestRunner` 利用多核 CPU，可同时对数百上千只股票进行回测，将数小时的计算缩短到几分钟。
- **深度结果分析**: `EnhancedAnalyzer` 提供远超 Backtrader 内置的分析指标，包括更详细的收益、风险和交易统计。
- **数据库无缝集成**: `PostgreSQLDataFeed` 可直接与数据库连接，或接收预加载的 `pandas.DataFrame`，实现灵活的数据供给。

## 架构概览

`bt_extensions` 的核心设计思想是“调度与执行分离”。

1.  **`ParallelBacktestRunner` (调度器)**: 负责顶层的任务调度。它将大量的股票列表分割成小批次，并通过多进程（`ProcessPoolExecutor`）将这些批次分发给不同的 CPU核心。它不关心数据如何加载，只负责管理并行任务。
2.  **`_run_batch_backtest` (执行单元)**: 这是在每个独立的子进程中运行的函数。它的职责是：
    -   初始化当前进程所需的环境（如数据库连接）。
    -   使用 `BatchDataLoader` **批量预加载**当前批次所有股票的数据。
    -   对批次中的每只股票，将**已加载到内存的数据**（`DataFrame`）传递给 `PostgreSQLDataFeed`。
    -   运行标准 Backtrader `Cerebro` 引擎。
    -   收集并返回结果。
3.  **`BatchDataLoader` (数据加载器)**: 负责与 `AlphaDataTool` 通信，高效地一次性获取多只股票的数据。
4.  **`PostgreSQLDataFeed` (数据适配器)**: 负责将一个 `DataFrame` 适配成 Backtrader 可识别的数据源格式。

这种架构的优势在于，每个组件的职责都非常单一，并且避免了在循环中逐个查询数据库的低效模式。

## 核心用例：大规模并行回测

以下是一个完整、可运行的代码示例，展示了如何使用 `ParallelBacktestRunner` 对多只股票运行同一个策略。

```python
import multiprocessing as mp
from datetime import date
import backtrader as bt

# 假设这是你的策略定义
class MyStrategy(bt.Strategy):
    params = (('period', 20),)

    def __init__(self):
        self.sma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.p.period)

    def next(self):
        if self.data.close[0] > self.sma[0]:
            self.buy()
        elif self.data.close[0] < self.sma[0]:
            self.sell()

# 主程序入口
if __name__ == '__main__':
    # 确保在Windows上多进程正常工作
    mp.freeze_support()

    from alphahome.bt_extensions.execution.parallel_runner import ParallelBacktestRunner
    from alphahome.common.config_manager import ConfigManager

    # 1. 初始化配置管理器 (确保你的 config.json 文件配置正确)
    # ConfigManager.initialize('path/to/your/config.json')
    
    # 2. 定义回测参数
    stock_codes = ['000001.SZ', '000002.SZ', '600036.SH', '600519.SH'] # 示例股票列表
    start_date = date(2023, 1, 1)
    end_date = date(2023, 12, 31)
    
    # 3. 初始化并行回测执行器
    # 使用 CPU核心数 - 1 个进程
    runner = ParallelBacktestRunner(max_workers=None)
    
    # 4. 运行并行回测
    results = runner.run_parallel_backtests(
        stock_codes=stock_codes,
        strategy_class=MyStrategy,
        strategy_params={'period': 15},
        start_date=start_date,
        end_date=end_date,
        initial_cash=100000.0,
        commission=0.001
    )
    
    # 5. 查看结果
    print("--- 回测摘要 ---")
    print(results.get('summary'))
    
    print("\n--- 详细结果 (前2条) ---")
    for stock, result in list(results.get('results', {}).items())[:2]:
        print(f"\n股票: {stock}")
        print(f"  最终市值: {result['final_value']:.2f}")
        print(f"  总回报率: {result['total_return'] * 100:.2f}%")
        print(f"  夏普比率: {result['sharpe_ratio']:.2f}")
        print(f"  最大回撤: {result['max_drawdown']:.2f}%")

```

## `EnhancedAnalyzer` 使用详解

`EnhancedAnalyzer` 提供了远超 Backtrader 内置分析器的深度分析能力，包含收益、风险、交易统计等多个维度的详细指标。

### 基本用法

在 Cerebro 中添加 EnhancedAnalyzer：

```python
from alphahome.bt_extensions.analyzers.enhanced_analyzer import EnhancedAnalyzer

# 创建 Cerebro 实例
cerebro = bt.Cerebro()

# 添加 EnhancedAnalyzer
cerebro.addanalyzer(EnhancedAnalyzer, _name='enhanced')

# 运行回测
results = cerebro.run()
strat = results[0]

# 获取分析结果
analysis = strat.analyzers.enhanced.get_analysis()
```

### 完整示例：单股票回测分析

```python
import backtrader as bt
from datetime import date
from alphahome.bt_extensions.analyzers.enhanced_analyzer import EnhancedAnalyzer
from alphahome.bt_extensions.data.feeds import PostgreSQLDataFeed

class MyStrategy(bt.Strategy):
    def __init__(self):
        self.sma = bt.indicators.SimpleMovingAverage(self.data.close, period=20)
    
    def next(self):
        if self.data.close[0] > self.sma[0] and not self.position:
            self.buy(size=100)
        elif self.data.close[0] < self.sma[0] and self.position:
            self.close()

# 设置回测
cerebro = bt.Cerebro()
cerebro.broker.setcash(100000.0)
cerebro.broker.setcommission(commission=0.001)

# 添加数据
data = PostgreSQLDataFeed(
    dataname='000001.SZ',
    fromdate=date(2023, 1, 1),
    todate=date(2023, 12, 31)
)
cerebro.adddata(data)

# 添加策略和分析器
cerebro.addstrategy(MyStrategy)
cerebro.addanalyzer(EnhancedAnalyzer, _name='enhanced')

# 运行回测
results = cerebro.run()
strat = results[0]
analysis = strat.analyzers.enhanced.get_analysis()

# 打印详细分析结果
print("=== 回测分析结果 ===")
print(f"股票代码: {data._dataname}")
print(f"回测期间: {data.fromdate} 至 {data.todate}")

print("\n--- 收益指标 ---")
print(f"总收益率: {analysis['returns']['total_return']*100:.2f}%")
print(f"年化收益率: {analysis['returns']['annual_return']*100:.2f}%")
print(f"最终资产: {analysis['returns']['final_value']:.2f}")

print("\n--- 风险指标 ---")
print(f"夏普比率: {analysis['risk']['sharpe_ratio']:.3f}")
print(f"最大回撤: {analysis['risk']['max_drawdown']*100:.2f}%")
print(f"最大回撤期间: {analysis['risk']['max_drawdown_period']}")
print(f"波动率: {analysis['risk']['volatility']*100:.2f}%")

print("\n--- 交易统计 ---")
print(f"总交易次数: {analysis['trades']['total_trades']}")
print(f"盈利交易: {analysis['trades']['winning_trades']}")
print(f"亏损交易: {analysis['trades']['losing_trades']}")
print(f"胜率: {analysis['trades']['win_rate']*100:.2f}%")
print(f"平均盈利: {analysis['trades']['avg_win']:.2f}")
print(f"平均亏损: {analysis['trades']['avg_loss']:.2f}")
print(f"盈亏比: {analysis['trades']['profit_factor']:.2f}")

print("\n--- 持仓分析 ---")
print(f"平均持仓天数: {analysis['positions']['avg_holding_days']:.1f}")
print(f"最长持仓天数: {analysis['positions']['max_holding_days']}")
print(f"最短持仓天数: {analysis['positions']['min_holding_days']}")
```

### 分析结果结构

EnhancedAnalyzer 返回的分析结果是一个嵌套字典，主要包含以下结构：

```python
analysis = {
    'returns': {
        'total_return': float,      # 总收益率
        'annual_return': float,     # 年化收益率
        'final_value': float,       # 最终资产价值
    },
    'risk': {
        'sharpe_ratio': float,      # 夏普比率
        'max_drawdown': float,      # 最大回撤比例
        'max_drawdown_period': str, # 最大回撤期间
        'volatility': float,        # 波动率
    },
    'trades': {
        'total_trades': int,        # 总交易次数
        'winning_trades': int,      # 盈利交易次数
        'losing_trades': int,       # 亏损交易次数
        'win_rate': float,          # 胜率
        'avg_win': float,           # 平均盈利
        'avg_loss': float,          # 平均亏损
        'profit_factor': float,     # 盈亏比
    },
    'positions': {
        'avg_holding_days': float,  # 平均持仓天数
        'max_holding_days': int,    # 最长持仓天数
        'min_holding_days': int,    # 最短持仓天数
    }
}
```

### 与 ParallelBacktestRunner 集成使用

EnhancedAnalyzer 也可以与 ParallelBacktestRunner 结合使用，对多只股票进行并行回测并获取每只股票的详细分析：

```python
from alphahome.bt_extensions.execution.parallel_runner import ParallelBacktestRunner

# 运行并行回测
runner = ParallelBacktestRunner(max_workers=None)
results = runner.run_parallel_backtests(
    stock_codes=['000001.SZ', '000002.SZ', '600036.SH'],
    strategy_class=MyStrategy,
    start_date=date(2023, 1, 1),
    end_date=date(2023, 12, 31),
    initial_cash=100000.0,
    commission=0.001
)

# 查看每只股票的详细分析
for stock_code, result in results['results'].items():
    print(f"\n=== {stock_code} 分析结果 ===")
    analysis = result['analysis']['enhanced']
    
    print(f"总收益率: {analysis['returns']['total_return']*100:.2f}%")
    print(f"夏普比率: {analysis['risk']['sharpe_ratio']:.3f}")
    print(f"胜率: {analysis['trades']['win_rate']*100:.2f}%")
```