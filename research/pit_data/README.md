# PIT数据管理器使用指南

本目录包含了PIT（Point-in-Time）数据管理器的相关代码，用于管理和维护金融数据的时点性数据。

## 主要管理器

### 1. PIT利润表管理器 (`pit_income_quarterly_manager.py`)

负责pit_income_quarterly表的历史全量回填和增量更新。

#### 使用方法：

```bash
# 全量历史回填
python pit_income_quarterly_manager.py --mode full-backfill --start-date 2000-01-01 --end-date 2024-12-31

# 增量更新（最近7天）
python pit_income_quarterly_manager.py --mode incremental --days 7

# 单股历史回填
python pit_income_quarterly_manager.py --mode single-backfill --ts-code 600000.SH --validate

# 查看表状态
python pit_income_quarterly_manager.py --status

# 数据验证
python pit_income_quarterly_manager.py --validate
```

#### 参数说明：
- `--mode`: 执行模式
  - `full-backfill`: 历史全量回填
  - `incremental`: 增量更新
  - `single-backfill`: 单股历史回填
- `--start-date`: 开始日期 (YYYY-MM-DD)
- `--end-date`: 结束日期 (YYYY-MM-DD)
- `--days`: 增量更新天数
- `--batch-size`: 每批股票数
- `--ts-code`: 指定股票代码（如 600000.SH）
- `--status`: 显示表状态
- `--validate`: 验证数据完整性

### 2. PIT财务指标管理器 (`pit_financial_indicators_manager.py`)

负责pit_financial_indicators表的历史全量回填和增量更新。**支持真正重新计算所有历史财务指标**。

#### 核心特性：
- **全量回填**: 重新计算指定日期范围内所有股票的历史财务指标
- **单股回填**: 重新计算指定股票的所有历史财务指标
- **增量更新**: 更新最近N天的数据
- **智能数据源处理**: 自动按优先级处理多数据源（report > express > forecast）
- **高性能**: 支持批量处理，性能优秀（200+只/秒）
- **精确指标**: 提供归属母公司股东的净利润同比增长率等关键财务指标

#### 使用方法：

```bash
# 全量历史回填 - 重新计算所有历史财务指标
python pit_financial_indicators_manager.py --mode full-backfill --start-date 2025-01-01 --end-date 2025-08-27

# 全量历史回填 - 使用默认日期范围
python pit_financial_indicators_manager.py --mode full-backfill

# 增量更新（最近7天）- 正确处理每个公告日期
python pit_financial_indicators_manager.py --mode incremental --days 7

# 单股财务指标计算 - 重新计算该股票所有历史财务指标
python pit_financial_indicators_manager.py --mode single-backfill --ts-code 600000.SH --validate

# 查看表状态
python pit_financial_indicators_manager.py --status

# 数据验证
python pit_financial_indicators_manager.py --validate
```

#### 参数说明：
同上，与利润表管理器参数一致。

## 核心功能

### 数据回填策略

1. **历史全量回填**: 从指定开始日期到结束日期，**重新计算所有历史时间点的财务指标**
   - 获取该时间段内所有股票的所有历史利润表记录
   - 为每个公告日期计算财务指标
   - 确保真正覆盖所有历史数据

2. **单股历史回填**: **重新计算指定股票的所有历史财务指标**
   - 获取该股票的所有历史利润表记录
   - 为每个历史公告日期计算财务指标
   - 生成完整的财务指标历史记录

3. **增量更新**: 获取最近N天内有新披露的利润表记录，按公告日期分组，为每个公告日期计算财务指标

### 核心财务指标说明

#### 增长率指标
- **revenue_yoy_growth**: 营收同比增长率(%)
- **n_income_yoy_growth**: **归属母公司股东的净利润同比增长率(%)** ⭐
  - 基于 `n_income_attr_p` 字段计算
  - 反映母公司股东实际获得的利润增长
  - 是衡量公司盈利能力的核心指标
- **operate_profit_yoy_growth**: 经营利润同比增长率(%)

#### 盈利能力指标
- **roe_excl_ttm**: 净资产收益率(扣除少数股东权益)TTM
- **roa_excl_ttm**: 总资产报酬率(扣除少数股东权益)TTM
- **gpa_ttm**: 总资产利润率TTM
- **net_margin_ttm**: 销售净利率TTM
- **operating_margin_ttm**: 营业利润率TTM

#### 运营效率指标
- **asset_turnover_ttm**: 总资产周转率TTM
- **roi_ttm**: 投资回报率TTM

#### 资本结构指标
- **equity_multiplier**: 权益乘数
- **debt_to_asset_ratio**: 资产负债率
- **equity_ratio**: 股东权益比率

### 数据源智能处理

- **优先级排序**: report > express > forecast
- **自动去重**: 同一股票、同一日期只保留优先级最高的数据源
- **兼容性保证**: 支持多数据源的混合使用

### 数据验证

- 自动检查数据完整性
- 验证关键字段非空性
- 生成验证报告

### 性能优化

- 批量处理机制（支持数千只股票同时处理）
- 并行计算支持
- 缓存机制
- 动态批次大小调整
- 进度实时报告

## 数据流程

```
利润表数据(pit_income_quarterly) -> 财务指标计算器 -> 财务指标表(pit_financial_indicators)
```

## 注意事项

### 数据依赖关系
1. **财务指标计算依赖于利润表数据的完整性**
2. **建议在计算财务指标前先确保利润表数据是最新的**
3. **单股回填模式**适用于调试和特定股票分析
4. **全量回填模式**适用于批量重新计算历史数据
5. **增量更新模式**适用于日常维护

### 性能考虑
- **全量回填**可能需要较长时间（取决于数据量）
- **建议在系统负载较低时运行**
- **可以通过指定较短的时间范围来测试**

### 数据源说明
- **report**: 正式财报数据，优先级最高
- **express**: 业绩快报数据，优先级中等
- **forecast**: 业绩预告数据，优先级最低
- 系统会自动按优先级保留最佳数据源

### 故障排除
- 如果遇到性能问题，可以适当调整 `--batch-size` 参数
- 如果遇到内存问题，可以缩小日期范围分批处理
- 日志文件会记录详细的错误信息用于排查

## 日志

所有操作都会生成详细的日志文件，保存在`logs/`目录下，文件名格式为：
- `pit_income_quarterly_YYYYMMDD.log`
- `pit_financial_indicators_YYYYMMDD.log`

## 更新历史

- **v2.1** (2025-08-27): 修复增量更新逻辑，正确处理每个公告日期而不是使用固定as_of_date
- **v2.0** (2025-08-27): 重构全量回填逻辑，支持真正重新计算所有历史财务指标
- **v1.5** (2025-08-27): 修复单股回填逻辑，支持历史数据重新计算
- **v1.0**: 初始版本，支持基本的增量更新功能
