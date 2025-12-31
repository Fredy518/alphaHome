# 基金绩效分析模块 (fund_analysis)

## 概述

`fund_analysis` 模块提供专业的基金绩效分析功能，支持收益分析、风险评估、回撤分析、归因分析等多维度评价。

## 核心组件

### 分析器 (Analyzers)

| 分析器 | 功能 | 主要指标 |
|--------|------|----------|
| `PerformanceAnalyzer` | 收益绩效分析 | 年化收益、夏普比率、索提诺比率 |
| `RiskAnalyzer` | 风险指标分析 | 波动率、VaR、跟踪误差 |
| `DrawdownAnalyzer` | 回撤分析 | 最大回撤、回撤持续期 |
| `PeriodicAnalyzer` | 周期分析 | 月度/季度/年度收益 |
| `MetricsAnalyzer` | 综合指标 | Calmar、信息比率 |
| `AttributionAnalyzer` | 归因分析 | Brinson归因、因子归因 |

## 快速开始

### 基础分析

```python
import pandas as pd
from alphahome.fund_analysis import (
    PerformanceAnalyzer,
    RiskAnalyzer,
    DrawdownAnalyzer,
)

# 准备净值数据（DatetimeIndex + nav 列）
nav_data = pd.DataFrame({
    'nav': [1.0, 1.02, 1.01, 1.05, 1.03, 1.08],
}, index=pd.date_range('2024-01-01', periods=6, freq='D'))

# 绩效分析
perf = PerformanceAnalyzer(nav_data)
print(f"年化收益: {perf.annualized_return:.2%}")
print(f"夏普比率: {perf.sharpe_ratio:.2f}")

# 风险分析
risk = RiskAnalyzer(nav_data)
print(f"年化波动率: {risk.annualized_volatility:.2%}")
print(f"VaR(95%): {risk.var_95:.2%}")

# 回撤分析
dd = DrawdownAnalyzer(nav_data)
print(f"最大回撤: {dd.max_drawdown:.2%}")
print(f"回撤持续天数: {dd.max_drawdown_duration}")
```

### 基准比较

```python
from alphahome.fund_analysis import PerformanceAnalyzer

# 带基准的分析
benchmark = pd.DataFrame({
    'nav': [1.0, 1.01, 1.02, 1.03, 1.02, 1.04],
}, index=nav_data.index)

perf = PerformanceAnalyzer(nav_data, benchmark=benchmark)
print(f"超额收益: {perf.excess_return:.2%}")
print(f"信息比率: {perf.information_ratio:.2f}")
print(f"跟踪误差: {perf.tracking_error:.2%}")
```

### 周期分析

```python
from alphahome.fund_analysis import PeriodicAnalyzer

periodic = PeriodicAnalyzer(nav_data)

# 月度收益
monthly = periodic.monthly_returns()
print("月度收益:")
print(monthly)

# 年度收益
yearly = periodic.yearly_returns()
print("年度收益:")
print(yearly)
```

## 配置选项

### PerformanceConfig

```python
from alphahome.fund_analysis import PerformanceConfig

config = PerformanceConfig(
    risk_free_rate=0.02,      # 无风险利率（年化）
    trading_days_per_year=252, # 年交易日数
    min_periods=20,            # 最小计算周期
)

perf = PerformanceAnalyzer(nav_data, config=config)
```

## 输出格式

所有分析器支持导出为 DataFrame：

```python
# 获取完整报告
report = perf.to_dataframe()
print(report)

# 导出为 JSON
report_dict = perf.to_dict()
```

## 与 fund_backtest 集成

```python
from alphahome.fund_backtest import BacktestEngine
from alphahome.fund_analysis import PerformanceAnalyzer

# 运行回测
engine = BacktestEngine(...)
result = engine.run()

# 分析回测结果
perf = PerformanceAnalyzer(result.nav_series)
print(perf.summary())
```

## 注意事项

1. **数据要求**：净值数据应为 DatetimeIndex，列名建议为 `nav`
2. **频率推断**：模块会自动推断数据频率（日/周/月），也可手动指定
3. **缺失值处理**：使用 `ffill()` 填充，首行空值会被删除
4. **精度**：所有百分比返回小数形式（如 0.05 表示 5%）

## 相关文档

- [组合回测框架](../backtest_framework_design.md)
- [Barra 风险模型](./barra_risk_model.md)
