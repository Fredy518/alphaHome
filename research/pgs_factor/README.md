# P/G/S因子计算系统

## 概述

优化后的A股P/G/S因子计算系统，基于Point-in-Time数据库实现，确保在任何历史时点都只使用该时点之前已知的数据，避免未来函数问题。

### 核心特性

- **PIT数据库表**：基于持久化的`pgs_factors.pit_income_quarterly`和`pit_balance_quarterly`表，严格控制数据的时间戳，确保回测的准确性。所有数据已预处理为单季度值，支持高效的SQL查询和聚合。
- **纯SQL计算**：无类包装，直接SQL计算P/G/S因子，一个查询处理全市场，性能极致。
- **三维因子体系**：
  - **P（Profitability）盈利能力**：ROE、ROA等核心盈利指标
  - **G（Growth）成长能力**：盈利能力的"加速度"，同比/环比变化
  - **S（Safety）安全能力**：财务杠杆、市场风险、盈利稳定性

## 系统架构

```
pgs_factor/
├── __init__.py              # 模块初始化
├── data_loader.py           # 数据加载器  
├── cumulative_handler.py    # 累积值处理器
├── database/                # 数据库管理模块
│   ├── db_manager.py        # PIT数据库管理器
│   └── db_schema.sql        # 数据库表结构
├── examples/                # 示例和工具脚本
│   └── pit_data_manager.py  # PIT数据管理器
└── README.md               # 本文档
```

## 数据来源与口径

- 财务数据：直接读取 PIT 统一视图 `pgs_factors.v_pit_financial_quarterly`，该视图已将利润表转换为单季口径，并与同报告期的资产负债表在点时（PIT）原则下对齐，避免应用层重复转换与对齐。
  - 视图字段：`ts_code, end_date, ann_date, data_source, year, quarter, n_income_attr_p, revenue, operate_profit, total_profit, income_tax, tot_assets, tot_liab, tot_equity`。
  - 应用层在载入后会标记 `is_single_quarter=True`，以便下游跳过重复单季化处理。
- 行情/基础指标：`tushare.stock_daily`, `tushare.stock_dailybasic`；指数：`tushare.index_factor_pro`；交易日历：`tushare.others_calendar`。

### ROE(TTM) 与 ROE_EXCL

- 分子：`n_income_attr_p` 智能TTM（点时、单季口径，融合快报/预告）。当仅有年报（上市前仅披露年报，无季报）或总体可用季度数不足4个时，退化为使用最近一份年报的全年 `n_income_attr_p` 作为 TTM，`ttm_type='annual_only'`，`confidence≈0.6`；若无任何年报则为 NaN。
- 分母：平均净资产（期初/期末均以报告期正式报表 `tot_assets - tot_liab` 计算；数据不足时使用当前值作为期初回退）。
- ROE_EXCL：当前暂以归母净利润近似扣非净利润（待补充 `n_income_attr_p_non_recurring` 后替换）。

## 安装与配置

### 前置条件

1. AlphaHome项目已正确安装
2. PostgreSQL数据库已配置，包含以下数据表（或等价视图/同名表）：
   - `tushare_fina_balancesheet` - 资产负债表
   - `tushare_fina_forecast` - 业绩预告
   - `tushare_fina_express` - 业绩快报
   - `tushare_stock_daily` - 股票日行情
   - 指数行情表（默认）：`tushare.index_factor_pro`
   - 交易日历表：`tushare.others_calendar`
   - G子因子明细表（高级G因子可选）：`pgs_factors.g_subfactors`

### 数据库连接

系统会自动使用AlphaHome的数据库配置，无需额外设置。

## 快速开始

### 1. 基本使用

```python
from research.tools.context import ResearchContext
from research.pgs_factor import PGSFactorCalculator

# 初始化
context = ResearchContext()
calculator = PGSFactorCalculator(context)

# 计算指定日期的因子
trade_date = '2024-03-29'  # 周五
factors_df = calculator.calculate_factors(trade_date)

# 保存结果
calculator.save_factors(factors_df, 'output/factors.csv')
```

### 2. 批量计算

```python
# 计算多个周五的因子
import pandas as pd

dates = pd.date_range('2024-01-01', '2024-03-31', freq='W-FRI')

for date in dates:
    date_str = date.strftime('%Y-%m-%d')
    factors = calculator.calculate_factors(date_str)
    calculator.save_factors(factors, f'output/factors_{date_str}.csv')
```

### 3. 指定股票池
### 4. 初始化数据库Schema（可选）

```bash
python -c "from research.tools.context import ResearchContext; from research.pgs_factor.database.db_manager import PGSFactorDBManager; ctx=ResearchContext(); PGSFactorDBManager(ctx).init_schema()"
```

说明：这会创建/更新 `pgs_factors` 下的核心表，含 `p_factor`、`g_factor`、`s_factor`、`quality_metrics`、`processing_log`、`factor_summary`（视图）与 `g_subfactors`（明细表）。

### 5. P因子入库测试（示例脚本）

```bash
python research/pgs_factor/examples/test_p_factor_storage.py
```

脚本会写入示例P因子数据至 `pgs_factors.p_factor`，并回查打印，验证写入路径。


```python
# 只计算特定股票的因子
stocks = ['000001.SZ', '000002.SZ', '600000.SH']
factors = calculator.calculate_factors('2024-03-29', stocks=stocks)
```

## 因子详解

### P因子（盈利能力）

#### 核心指标
- **ROE(TTM)**：净资产收益率（分子统一使用归母净利润 `n_income_attr_p`），智能TTM计算
- **ROA(TTM)**：总资产收益率，智能TTM计算
- **毛利率**：可由利润表字段计算（示例实现中为近似口径）

#### 智能TTM计算流程
1. 获取最新4个季度的正式财报
2. 检查是否有更新的业绩快报
3. 如无快报，检查是否有业绩预告
4. 根据数据可用性，选择最优计算方式

### G因子（成长能力）

#### 新版本设计（基于P_score变化）
- **双因子结构**：
  - **Factor_A（惊喜因子）**：ΔP_score_YoY / Std(ΔP_score_YoY)
  - **Factor_B（绝对动量因子）**：ΔP_score_YoY
- **排名合成**：G_score = 0.5 × Rank_A + 0.5 × Rank_B

#### 数据要求
- **最少12个季度**：至少需要12个季度的P_score历史数据
- **目标20个季度**：理想情况下需要20个季度（5年）数据
- **数据不足处理**：当历史数据少于12个季度时，G_score设为NaN

#### 计算特点
- 基于P_score（综合盈利能力）而非单一ROE指标
- 考虑相对惊喜（标准化）和绝对动量
- 通过百分位排名避免极端值影响

### S因子（安全能力）

#### 核心指标
- **资产负债率**：总负债/总资产
- **Beta系数**：相对市场的系统性风险
- **ROE波动率**：盈利稳定性度量

#### 风险评估
- 财务杠杆风险
- 市场系统性风险
- 盈利稳定性风险

## 输出格式

计算结果包含以下字段：

```csv
ts_code,trade_date,
p_score,g_score,s_score,
roe_ttm,roa_ttm,gross_margin,
factor_a,factor_b,rank_a,rank_b,
p_score_yoy,p_score_yoy_pct,
debt_ratio,beta,roe_volatility,
p_score_zscore,g_score_zscore,s_score_zscore,
total_score,total_rank,p_rank,g_rank,s_rank
```

## 运行示例

```bash
# 运行示例脚本
cd E:\CodePrograms\alphaHome
python research/pgs_factor/example_usage.py
```

示例脚本将：
1. 计算2024年Q1每个周五的P/G/S因子
2. 生成因子统计报告
3. 分析因子相关性和稳定性
4. 输出Top10股票列表

## 注意事项

### 数据质量
- 确保财务数据的完整性，特别是公告日期字段
- 业绩预告数据可能存在修正，系统会自动使用最新公告
- 部分股票可能缺少业绩快报或预告数据

### 计算效率
- 首次运行需要加载大量历史数据，可能耗时较长
- 建议按需加载数据，避免一次性处理过多股票
- 可以考虑将计算结果缓存，避免重复计算
- 历史P分数优先直接从数据库读取（`get_historical_p_scores`），避免重复重算

### 参数调优
- P/G/S权重可根据市场环境调整（默认4:3:3）
- 历史序列长度可调整（默认20个季度）
- Beta计算的回看天数可调整（默认252天）

## 数据库接口补充说明（研究侧）

- `PGSFactorDBManager.save_p_factor(df, ann_date, data_source)`：写入P因子，自动按(ts_code, calc_date, data_source)去重更新。
- `PGSFactorDBManager.get_historical_p_scores(stocks, start_date, end_date)`：返回按日期分组的历史P分数字典，供G因子计算使用。
- 高级G批处理可选接口：`save_g_subfactors(...)`、`get_active_stocks(...)`、`get_financial_data(...)`、`update_processing_progress(...)`。

## 扩展开发

### 添加新指标
在`factor_calculator.py`中相应的计算函数中添加：

```python
def _calculate_p_factor(self, stock, trade_date):
    # 添加新的盈利指标
    factors['new_metric'] = self.calculate_new_metric(stock, trade_date)
```

### 行业中性化
在`_post_process_factors`方法中添加行业中性化逻辑：

```python
def _industry_neutralize(self, factors_df):
    # 加载行业分类
    # 在行业内标准化
    # 返回中性化后的因子
```

### 集成到策略
可以将P/G/S因子集成到多因子模型中：

```python
# 在策略中使用
top_stocks = factors_df.nlargest(30, 'total_score')['ts_code'].tolist()
```

## 常见问题

### Q: 如何处理缺失值？
A: 系统会自动处理缺失值，在计算综合得分时会忽略缺失的因子。

### Q: 支持哪些市场？
A: 目前支持A股市场，包括沪深主板、创业板、科创板。

### Q: 如何调整因子权重？
A: 在`_post_process_factors`方法中修改weights字典。

## 更新日志

### v2.0.0 (2025-08-07)
- **重大更新：G因子计算逻辑重构**
  - 基于P_score变化的双因子方案（惊喜因子+绝对动量）
  - 增加数据完整性要求（最少12个季度）
  - 百分位排名合成，提升稳定性
- **累积值处理优化**
  - 新增cumulative_handler模块
  - 自动识别并处理财务数据中的累积值
  - 修正ROE计算中的累积值问题
- **数据质量改进**
  - 优化资产负债表字段映射（使用tot_equity）
  - 改进TTM计算的数据覆盖
  - 增强错误处理和日志记录

### v1.0.0 (2024-08-07)
- 初始版本发布
- 实现P/G/S三维因子计算
- 支持智能TTM计算
- 集成Point-in-Time数据库

## 联系方式

如有问题或建议，请通过AlphaHome项目issue反馈。
