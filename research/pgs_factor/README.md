# P/G 历史研究目录与兼容入口

> 本目录保存历史研究说明。正式计算与入库权威为 `alphahome.factors`，数学版本 P v2.0、G v1.1，自然周五、同日 P→G。下文旧版本、性能宣传与命令仅作为历史记录；生产使用 `python -m alphahome.factors --help` 及根目录入口矩阵。未经消费者验收不删除兼容文件。

# PGS因子计算系统 v2.0 🚀

> 轻量级、高性能的A股P/G/S因子计算系统 - 无历史包袱，全新架构

## ✨ 核心特性

- 🏗️ **三层数据流架构**：清晰的数据流向和组件分离
- ⚡ **高性能设计**：基于PIT数据库的纯SQL实现
- 🎯 **简洁API**：统一的DataPipeline接口
- 🔄 **智能数据管理**：自动化的数据同步和因子计算
- 📊 **多数据源融合**：整合正式财报、业绩快报、业绩预告
- 🛡️ **时间点准确性**：严格的PIT数据管理，避免未来函数

## 🏗️ 系统架构

```text
AlphaHome原始数据 → PIT数据库 → 因子存储
        ↑              ↑           ↑
   SourceLoader   PITManager  Production*Calculator
        ↑              ↑           ↑
            DataPipeline (统一协调)
```

### 📁 目录结构

```text
research/pgs_factor/
├── core/              # 核心组件
│   ├── pit_manager.py    # PIT数据管理
│   └── data_pipeline.py  # 数据管道协调
├── data/              # 数据访问层
│   └── source_loader.py  # 原始数据加载
├── processors/        # 生产级计算器
│   ├── production_p_factor_calculator.py
│   ├── production_g_factor_calculator.py
│   └── production_financial_indicators_calculator.py
├── database/          # 数据库管理
└── utils/            # 工具函数
```

## 🚀 快速开始

### 基础使用

```python
from research.pgs_factor import DataPipeline
from research.tools.context import ResearchContext

# 创建数据管道
with ResearchContext() as ctx:
    pipeline = DataPipeline(ctx)
    
    # 同步PIT数据 (Layer 1 → Layer 2)
    pipeline.sync_pit_data(
        sources=['report', 'express', 'forecast'],
        mode='incremental'
    )
    
    # 计算因子 (Layer 2 → Layer 3)
    pipeline.calculate_factors(
        factors=['P', 'G', 'S'],
        calc_date='2024-12-31'
    )
    
    # 查询结果
    results = pipeline.query_factors(
        calc_date='2024-12-31',
        factors=['P', 'G', 'S']
    )
```

### 单独使用组件

```python
from research.pgs_factor import PITManager, DataPipeline, SourceLoader

# PIT数据管理
pit_manager = PITManager(ctx)
pit_manager.ensure_tables_exist()
pit_manager.full_rebuild()

# 因子计算
pipeline = DataPipeline(ctx)
results = pipeline.calculate_factors(['P', 'G'], '2024-12-31')
```

## 📊 因子说明

| 因子类型 | 核心指标 | 说明 |
|---------|---------|------|
| **P因子** | ROE, ROA, 毛利率, 净利率 | 盈利能力评估 |
| **G因子** | 惊喜因子, 动量因子 | 成长能力评估 |
| **S因子** | 负债率, Beta, ROE波动率 | 安全能力评估 |

## 🎯 核心组件

### DataPipeline - 统一协调器

```python
pipeline = DataPipeline(context)
pipeline.sync_pit_data()      # 数据同步
pipeline.calculate_factors()  # 因子计算
pipeline.query_factors()     # 结果查询
```

### PITManager - 数据转换枢纽

```python
manager = PITManager(context)
manager.ensure_tables_exist()    # 自动建表
manager.process_report_data()    # 处理财报数据
manager.process_express_data()   # 处理快报数据
```

### DataPipeline - 统一数据管道

```python
pipeline = DataPipeline(context)
pipeline.calculate_factors(['P', 'G'], calc_date)
```

### SourceLoader - 原始数据加载

```python
loader = SourceLoader(context)
loader.load_income_data(stocks, start_date, end_date)
loader.load_balance_data(stocks, start_date, end_date)
```

## ⚡ 性能特性

- **纯SQL计算**：最大化数据库性能
- **智能缓存**：减少重复计算
- **批量处理**：高效的数据处理
- **延迟加载**：按需加载组件

## 🔧 配置

系统通过ResearchContext自动加载配置，支持：
- 数据库连接配置
- 因子计算参数
- 性能优化设置

## 📈 版本特性

### v2.0.0-clean
- ✅ 全新三层架构设计
- ✅ 移除历史包袱，轻装上阵
- ✅ 简化组件命名
- ✅ 统一的DataPipeline接口
- ✅ 高性能PIT数据管理

## 🎉 开始使用

1. **安装依赖**：确保ResearchContext可用
2. **初始化数据**：运行`pipeline.sync_pit_data()`
3. **计算因子**：使用`pipeline.calculate_factors()`
4. **查询结果**：通过`pipeline.query_factors()`获取数据

轻量级设计，强大功能，开箱即用！🚀

## 🔄 从v1.0迁移

### 旧代码
```python
from research.pgs_factor.examples.pit_data_manager import PITDataManager
from research.pgs_factor.data_loader import PGSDataLoader
```

### 新代码
```python
from research.pgs_factor import DataPipeline, PITManager, SourceLoader
```

### 统一接口
```python
# 旧方式：分散调用
manager = PITDataManager(ctx)
manager.ensure_tables_exist()
manager.process_report_data()

# 新方式：统一管道
pipeline = DataPipeline(ctx)
pipeline.sync_pit_data()
pipeline.calculate_factors()
```

## 🆘 故障排除

### 常见问题

1. **导入错误**：确保使用新的导入路径
2. **数据库连接**：检查ResearchContext配置
3. **性能问题**：查看日志和性能监控

### 获取帮助

- 查看详细日志信息
- 运行内置验证工具
- 检查性能监控报告

---

**无历史包袱，轻装上阵！** 🎯
