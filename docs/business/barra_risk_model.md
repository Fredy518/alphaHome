# Barra 多因子风险模型

## 概述

`barra` 模块实现了 Barra 风格的多因子风险模型，用于组合风险分解、风险预测和收益归因。

## 核心组件

| 组件 | 功能 |
|------|------|
| `RiskModel` | 风险模型核心类 |
| `RiskModelConfig` | 模型配置 |
| `MultiPeriodLinker` | 多期归因连接器 |
| `link_carino` | Carino 方法多期连接 |
| `link_menchero` | Menchero 方法多期连接 |

## 快速开始

### 基础风险计算

```python
import numpy as np
import pandas as pd
from alphahome.barra import RiskModel, RiskModelConfig

# 配置风险模型
config = RiskModelConfig(
    half_life_factor=60,      # 因子协方差半衰期
    half_life_specific=120,   # 特质方差半衰期
    min_periods=60,           # 最小样本期数
)

# 创建模型实例
model = RiskModel(config)

# 准备因子收益数据
factor_returns = pd.DataFrame({
    'market': np.random.randn(252) * 0.01,
    'size': np.random.randn(252) * 0.005,
    'value': np.random.randn(252) * 0.005,
}, index=pd.date_range('2024-01-01', periods=252, freq='D'))

# 估计因子协方差矩阵
factor_cov = model.estimate_factor_covariance(factor_returns)
print("因子协方差矩阵:")
print(factor_cov)
```

### 组合风险分解

```python
# 组合因子暴露
exposures = pd.Series({
    'market': 1.0,
    'size': 0.3,
    'value': -0.2,
})

# 计算组合风险
portfolio_variance = model.compute_portfolio_risk(
    exposures=exposures,
    factor_covariance=factor_cov,
)
print(f"组合方差: {portfolio_variance:.6f}")
print(f"组合波动率: {np.sqrt(portfolio_variance):.4%}")
```

### 特质风险估计

```python
# 残差收益（股票层面）
residuals = pd.DataFrame(
    np.random.randn(252, 100) * 0.02,
    index=pd.date_range('2024-01-01', periods=252, freq='D'),
    columns=[f'stock_{i}' for i in range(100)]
)

# 估计特质方差
specific_var = model.estimate_specific_variance(residuals)
print(f"平均特质波动率: {np.sqrt(specific_var.mean()):.4%}")
```

## 多期收益归因

### Carino 方法

```python
from alphahome.barra import link_carino

# 单期归因结果
period_attributions = pd.DataFrame({
    'period': ['2024Q1', '2024Q2', '2024Q3', '2024Q4'],
    'selection': [0.02, -0.01, 0.03, 0.01],
    'allocation': [0.01, 0.02, -0.01, 0.02],
    'interaction': [0.005, -0.005, 0.01, -0.005],
    'total': [0.035, 0.005, 0.03, 0.025],
})

# 连接多期归因
linked = link_carino(period_attributions)
print("Carino 多期连接结果:")
print(linked)
```

### Menchero 方法

```python
from alphahome.barra import link_menchero

# Menchero 方法（适合几何收益）
linked = link_menchero(period_attributions)
print("Menchero 多期连接结果:")
print(linked)
```

## 配置选项

### RiskModelConfig 详解

```python
config = RiskModelConfig(
    # 协方差估计
    half_life_factor=60,       # 因子协方差半衰期（天）
    half_life_specific=120,    # 特质方差半衰期（天）
    
    # 数据要求
    min_periods=60,            # 最小样本期数
    
    # 特质风险
    newey_west_lags=5,         # Newey-West 调整滞后阶数
    
    # 协方差调整
    eigenfactor_adjustment=True,  # 特征因子调整
    volatility_regime_adj=True,   # 波动率状态调整
)
```

## 风险分解示例

```python
# 完整的组合风险分解
def decompose_risk(model, weights, exposures, factor_cov, specific_var):
    """
    将组合风险分解为因子风险和特质风险
    """
    # 因子风险
    factor_risk = model.compute_portfolio_risk(
        exposures=exposures,
        factor_covariance=factor_cov,
    )
    
    # 特质风险
    specific_risk = (weights ** 2 * specific_var).sum()
    
    # 总风险
    total_risk = factor_risk + specific_risk
    
    return {
        'factor_risk': factor_risk,
        'specific_risk': specific_risk,
        'total_risk': total_risk,
        'factor_pct': factor_risk / total_risk,
        'specific_pct': specific_risk / total_risk,
    }
```

## 与其他模块集成

### 与 fund_analysis 集成

```python
from alphahome.fund_analysis import RiskAnalyzer
from alphahome.barra import RiskModel

# fund_analysis 提供历史风险指标
risk_analyzer = RiskAnalyzer(nav_data)
historical_vol = risk_analyzer.annualized_volatility

# barra 提供前瞻风险预测
risk_model = RiskModel(config)
predicted_vol = risk_model.predict_volatility(exposures, factor_cov)
```

## 注意事项

1. **数据频率**：模型默认假设日频数据，其他频率需调整半衰期参数
2. **因子定义**：需要外部提供因子暴露和因子收益，模块不包含因子定义
3. **数值稳定性**：协方差矩阵自动进行正定性检查和修正
4. **内存使用**：大规模股票池的特质风险估计可能消耗较多内存

## 相关文档

- [基金绩效分析](./fund_analysis.md)
- [组合回测框架](../backtest_framework_design.md)
