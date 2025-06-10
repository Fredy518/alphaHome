# BT Extensions - Backtrader 增强插件

**轻量级 Backtrader 插件集合，专注于数据库连接和性能优化**

## 🎯 设计定位

`btextensions` 是 Backtrader 的增强插件，而非独立的回测引擎。它专注于：

- 🔗 **数据库桥梁**：将本地数据库无缝连接到 Backtrader
- ⚡ **性能优化**：批量加载、智能缓存、并行处理
- 📊 **分析增强**：更丰富的回测结果分析
- 🛠️ **工具集成**：与现有工具链完美配合

避免重复发明轮子，专注于增强现有 Backtrader 生态。

## 🚀 核心功能

### 1. 数据库数据源
```python
from alphahome.btextensions import PostgreSQLDataFeed

data_feed = PostgreSQLDataFeed(
    db_manager=db_manager,
    ts_code='000001.SZ',
    start_date=date(2023, 1, 1),
    end_date=date(2023, 12, 31)
)
cerebro.adddata(data_feed)
```

### 2. 批量数据加载
```python
from alphahome.btextensions import BatchDataLoader, CacheManager

# 智能缓存 + 批量加载
cache_manager = CacheManager(max_memory_mb=512)
batch_loader = BatchDataLoader(db_manager, cache_manager)

stock_data = batch_loader.load_stocks_data(
    stock_codes=['000001.SZ', '000002.SZ', '600000.SH'],
    start_date=date(2023, 1, 1),
    end_date=date(2023, 12, 31)
)
```

### 3. 并行回测执行
```python
from alphahome.btextensions import ParallelBacktestRunner

runner = ParallelBacktestRunner(max_workers=4, batch_size=50)

results = runner.run_parallel_backtests(
    stock_codes=stock_list,
    strategy_class=MyStrategy,
    strategy_params={'param1': value1},
    start_date=start_date,
    end_date=end_date
)
```

### 4. 性能监控
```python
from alphahome.btextensions import PerformanceMonitor

monitor = PerformanceMonitor()
monitor.start_monitoring()

# 执行回测任务...

stats = monitor.stop_monitoring()
monitor.print_stats(stats)
```

### 5. 增强分析
```python
from alphahome.btextensions import EnhancedAnalyzer

cerebro.addanalyzer(EnhancedAnalyzer, _name='enhanced')

# 运行回测后
enhanced_analysis = strat.analyzers.enhanced.get_analysis()
print(f"夏普比率: {enhanced_analysis['risk']['sharpe_ratio']:.3f}")
print(f"策略评级: {enhanced_analysis['performance']['grade']}")
```

## 📦 模块结构

```
btextensions/
├── __init__.py              # 模块入口，导出主要类
├── data/
│   └── feeds.py            # PostgreSQL数据源
├── execution/
│   ├── batch_loader.py     # 批量数据加载器
│   └── parallel_runner.py  # 并行回测执行器
├── utils/
│   ├── cache_manager.py    # 智能缓存管理
│   └── performance_monitor.py  # 性能监控器
└── analyzers/
    └── enhanced_analyzer.py    # 增强分析器
```

## 🔧 安装和使用

1. **导入模块**：
```python
from alphahome.btextensions import (
    PostgreSQLDataFeed,
    BatchDataLoader,
    ParallelBacktestRunner,
    CacheManager,
    PerformanceMonitor,
    EnhancedAnalyzer
)
```

2. **典型使用流程**：
```python
# 1. 创建数据源
data_feed = PostgreSQLDataFeed(db_manager, ts_code='000001.SZ', ...)

# 2. 设置Cerebro
cerebro = bt.Cerebro()
cerebro.addstrategy(MyStrategy)
cerebro.adddata(data_feed)
cerebro.addanalyzer(EnhancedAnalyzer, _name='enhanced')

# 3. 运行回测
results = cerebro.run()

# 4. 分析结果
enhanced_analysis = results[0].analyzers.enhanced.get_analysis()
```

## 📈 性能特点

- **批量优化**：批量SQL查询减少数据库连接次数
- **智能缓存**：LRU内存缓存 + 磁盘持久化，大幅提升重复查询性能
- **并行处理**：多进程并行回测，充分利用多核CPU资源
- **内存管理**：智能内存监控和清理，避免内存溢出
- **性能监控**：实时监控CPU、内存、I/O使用情况

## 🎯 与其他模块的集成

`btextensions` 与 alphaHome 生态系统完美集成：

- 使用 `common.sync_db_manager` 进行数据库操作
- 使用 `common.config_manager` 进行配置管理
- 使用 `common.logging_utils` 进行日志记录

## 📚 示例

完整的使用示例请参考：
- `examples/enhanced_backtrader_demo.py` - 完整功能演示
- `examples/final_sync_backtest_demo.py` - 基础回测示例

## 🚨 注意事项

1. **专注插件定位**：本模块是 Backtrader 的插件，而不是独立的回测引擎
2. **数据库依赖**：需要配置有效的数据库连接
3. **内存管理**：大量数据时注意内存使用，可调整缓存配置
4. **并行限制**：并行进程数建议不超过CPU核心数

## 🔄 版本信息

- **版本**: 1.0.0
- **兼容性**: Backtrader >= 1.9.76, Python >= 3.8
- **依赖**: pandas, numpy, psutil, backtrader 