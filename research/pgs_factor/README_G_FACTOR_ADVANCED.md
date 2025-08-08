# 高级G因子计算系统

## 概述

高级G因子计算系统是一个全新设计的成长因子评估框架，通过综合多个成长子因子来全面评估股票的成长潜力。

## 核心特性

### 1. 四个成长子因子

#### G_Efficiency_Surprise (效率惊喜因子)
- **计算公式**: ΔP_score_YoY / Std(ΔP_score_YoY)
- **含义**: 衡量盈利能力改善的相对强度
- **特点**: 考虑了历史波动性，突出超预期的改善

#### G_Efficiency_Momentum (效率动量因子)  
- **计算公式**: ΔP_score_YoY
- **含义**: 衡量盈利能力改善的绝对力度
- **特点**: 直接反映同比变化幅度

#### G_Revenue_Momentum (营收动量因子)
- **计算公式**: 营业收入(TTM)同比增长率
- **含义**: 衡量市场扩张的速度
- **特点**: 反映公司的市场拓展能力

#### G_Profit_Momentum (利润动量因子)
- **计算公式**: 扣非净利润(TTM)同比增长率
- **含义**: 衡量最终盈利成果的增长
- **特点**: 反映公司的盈利增长能力

### 2. 百分位排名系统

- 每个子因子在全市场股票中计算百分位排名（0-100）
- 生成Rank_ES、Rank_EM、Rank_RM、Rank_PM四个排名变量
- 确保不同量纲的因子具有可比性

### 3. 综合评分

- **计算公式**: Final_G_Score = 0.25×Rank_ES + 0.25×Rank_EM + 0.25×Rank_RM + 0.25×Rank_PM
- 等权重合成，可根据需要调整权重配置
- 分数范围: 0-100，越高表示成长性越好

## 系统架构

### 核心模块

#### `g_factor_advanced.py`
- **AdvancedGFactorCalculator**: 高级G因子计算器
  - 计算4个成长子因子
  - 执行百分位排名转换
  - 合成最终G_score
  - 数据质量评估

#### `g_factor_batch_advanced.py`
- **GFactorBatchProcessorAdvanced**: 批量处理器
  - 基于财务公告日期的触发计算
  - 全市场批量计算
  - 数据库读写管理
  - 处理进度跟踪

## 使用示例

### 1. 基础计算

```python
from g_factor_advanced import AdvancedGFactorCalculator
import pandas as pd

# 创建计算器
calculator = AdvancedGFactorCalculator()

# 准备数据
stocks = ['000001.SZ', '000002.SZ', ...]
calc_date = '20250801'
financial_data = pd.DataFrame(...)  # 财务数据
p_scores_current = pd.DataFrame(...)  # 当前P_score
p_scores_historical = {...}  # 历史P_score字典

# 计算G因子
results = calculator.calculate_g_factors(
    stocks, 
    calc_date,
    financial_data,
    p_scores_current,
    p_scores_historical
)

# 查看结果
print(results[['ts_code', 'g_score', 'data_quality']])
```

### 2. 批量处理

```python
from g_factor_batch_advanced import GFactorBatchProcessorAdvanced
from database.db_manager import PGSFactorDBManager

# 初始化
db_manager = PGSFactorDBManager(context)
processor = GFactorBatchProcessorAdvanced(db_manager)

# 公告触发处理
affected_stocks = ['000001.SZ', '000002.SZ']
result = processor.process_announcement_triggered('20250801', affected_stocks)

# 全市场处理
result = processor.process_full_market('20250801')
```

### 3. 自定义权重

```python
# 调整子因子权重
new_weights = {
    'efficiency_surprise': 0.3,
    'efficiency_momentum': 0.3,
    'revenue_momentum': 0.2,
    'profit_momentum': 0.2
}
calculator.update_weights(new_weights)
```

## 数据要求

### P_score数据
- 当前期P_score（用于计算效率因子）
- 历史P_score（建议至少12个季度，目标20个季度）

### 财务数据
- 营业收入（revenue或total_revenue字段）
- 扣非净利润（n_income_attr_p_non_recurring字段）
- 公告日期（ann_date）
- 报告期末日期（end_date）

## 数据质量标准

- **High**: 4个子因子全部有效
- **Medium**: 3个子因子有效
- **Low**: 2个或更少子因子有效

## 性能优化

- 批量计算支持，默认batch_size=100
- 并发处理支持，max_workers=4
- 增量计算机制，仅更新受影响股票

## 测试

运行完整测试套件：

```bash
python test_g_factor_advanced.py
```

测试包括：
- G因子计算测试
- 批量处理测试
- 子因子相关性分析
- 数据质量验证

## 输出示例

```
股票代码   G_Score  数据质量  有效因子数
000001.SZ   86.73    high        4
000002.SZ   28.06    high        4
000003.SZ   30.10    high        4
```

## 注意事项

1. **数据完整性**: 确保P_score历史数据充足，否则效率惊喜因子可能无法计算
2. **时间对齐**: 注意财务数据的公告日期和报告期对齐
3. **异常值处理**: 系统会自动处理负利润等特殊情况
4. **排名稳定性**: 全市场排名需要足够的股票样本（建议>100只）

## 版本历史

- v2.0 (2025-01): 高级G因子系统上线
  - 新增4个成长子因子
  - 实现百分位排名系统
  - 支持权重自定义
  - 增强数据质量评估

## 联系与支持

如有问题或建议，请联系量化研究团队。
