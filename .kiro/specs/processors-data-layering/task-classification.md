# 任务分类表

本文档对 alphahome/processors 模块中的现有任务进行分类，明确各任务的数据流向、分层归属和重构方向。

## 分类标准

| 分类 | 说明 |
|------|------|
| 处理层保留 | 任务仅做数据清洗、对齐、标准化，无特征计算逻辑 |
| 特征下沉 | 任务主要是特征计算，应从任务层下沉到特征层函数 |
| 混合需拆分 | 任务混合了数据处理和特征计算，需拆分为 clean 数据 + 特征计算两部分 |

## 任务分类总览

| 任务 | 分类 | 目标 clean 表 |
|------|------|---------------|
| index_valuation | 混合需拆分 | clean.index_valuation_base |
| index_volatility | 混合需拆分 | clean.index_volatility_base |
| industry_return | 处理层保留 | clean.industry_base |
| industry_breadth | 特征下沉 | clean.industry_base |
| market_money_flow | 混合需拆分 | clean.money_flow_base |
| market_technical | 混合需拆分 | clean.market_technical_base |
| style_index_return | 特征下沉 | clean.style_base |
| futures_basis | 混合需拆分 | clean.futures_base |
| member_position | 混合需拆分 | clean.futures_holding_base |
| option_iv | 混合需拆分 | clean.option_iv_base |

## 详细分类表

### 1. index_valuation（指数估值）

| 属性 | 值 |
|------|-----|
| task_name | index_valuation |
| input_tables | tushare.index_dailybasic, akshare.macro_bond_rate |
| output_table | processor_index_valuation |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | *_PE_Pctl_10Y, *_PB_Pctl_10Y, *_PE_Pctl_12M, *_PB_Pctl_12M, *_ERP |
| classification | 混合需拆分 |
| target_clean_table | clean.index_valuation_base |
| features_to_extract | rolling_percentile (10Y/12M 窗口), ERP 计算 |

**Clean 层输出列：**
- trade_date, ts_code, pe_ttm, pb, China_10Y_Yield
- 血缘列：_source_table, _processed_at, _data_version, _ingest_job_id

**特征层计算：**
- `rolling_percentile(pe_ttm, window=2520, min_periods=252)` → *_PE_Pctl_10Y
- `rolling_percentile(pb, window=2520, min_periods=252)` → *_PB_Pctl_10Y
- `rolling_percentile(pe_ttm, window=252, min_periods=60)` → *_PE_Pctl_12M
- `rolling_percentile(pb, window=252, min_periods=60)` → *_PB_Pctl_12M
- `(1/pe_ttm) - (yield/100)` → *_ERP

---

### 2. index_volatility（指数波动率）

| 属性 | 值 |
|------|-----|
| task_name | index_volatility |
| input_tables | tushare.index_factor_pro |
| output_table | processor_index_volatility |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | *_RV_20D, *_RV_60D, *_RV_252D, *_RV_20D_Pctl, *_RV_Ratio_20_60 |
| classification | 混合需拆分 |
| target_clean_table | clean.index_volatility_base |
| features_to_extract | rolling_std (RV 计算), rolling_percentile |

**Clean 层输出列：**
- trade_date, ts_code, close, close_unadj, _adj_method
- 血缘列：_source_table, _processed_at, _data_version, _ingest_job_id

**特征层计算：**
- `returns.rolling(20).std() * sqrt(252)` → *_RV_20D
- `returns.rolling(60).std() * sqrt(252)` → *_RV_60D
- `returns.rolling(252).std() * sqrt(252)` → *_RV_252D
- `rolling_percentile(rv20, window=252, min_periods=60)` → *_RV_20D_Pctl
- `rv20 / rv60` → *_RV_Ratio_20_60

---

### 3. industry_return（行业收益）

| 属性 | 值 |
|------|-----|
| task_name | industry_return |
| input_tables | tushare.index_swdaily, tushare.index_swmember |
| output_table | processor_industry_return |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | SW_* (pct_change) |
| classification | 处理层保留 |
| target_clean_table | clean.industry_base |
| features_to_extract | - |

**说明：** 该任务仅做数据透视和简单收益率计算（pct_change），属于数据标准化范畴，保留在处理层。

**Clean 层输出列：**
- trade_date, ts_code, close, pct_change
- 血缘列：_source_table, _processed_at, _data_version, _ingest_job_id

---

### 4. industry_breadth（行业宽度）

| 属性 | 值 |
|------|-----|
| task_name | industry_breadth |
| input_tables | tushare.index_swdaily, tushare.index_swmember |
| output_table | processor_industry_breadth |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | Industry_Up_Ratio, Industry_Strong_Ratio, Industry_Weak_Ratio, Industry_Return_Std, Industry_Return_Skew, Industry_Up_Ratio_5D |
| classification | 特征下沉 |
| target_clean_table | clean.industry_base |
| features_to_extract | 横截面统计 (up_ratio, std, skew), rolling_mean |

**说明：** 该任务基于 clean.industry_base 的行业收益数据计算横截面统计特征，应下沉到特征层。

**特征层计算：**
- `(returns > 0).sum() / count` → Industry_Up_Ratio
- `(returns > 0.01).sum() / count` → Industry_Strong_Ratio
- `(returns < -0.01).sum() / count` → Industry_Weak_Ratio
- `returns.std(axis=1)` → Industry_Return_Std
- `returns.skew(axis=1)` → Industry_Return_Skew
- `Industry_Up_Ratio.rolling(5).mean()` → Industry_Up_Ratio_5D

---

### 5. market_money_flow（市场资金流）

| 属性 | 值 |
|------|-----|
| task_name | market_money_flow |
| input_tables | tushare.stock_moneyflow, tushare.stock_dailybasic |
| output_table | processor_market_money_flow |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | Total_Net_MF, Net_MF_Rate, Net_MF_Rate_ZScore, Net_MF_Rate_Pctl, Net_MF_ZScore |
| classification | 混合需拆分 |
| target_clean_table | clean.money_flow_base |
| features_to_extract | rolling_zscore, rolling_percentile |

**Clean 层输出列：**
- trade_date, total_net_mf_amount, total_circ_mv
- 血缘列：_source_table, _processed_at, _data_version, _ingest_job_id

**特征层计算：**
- `total_net_mf_amount / (total_circ_mv * 10000)` → Net_MF_Rate
- `rolling_zscore(Net_MF_Rate, window=252)` → Net_MF_Rate_ZScore
- `rolling_percentile(Net_MF_Rate, window=252)` → Net_MF_Rate_Pctl
- `rolling_zscore(Total_Net_MF, window=252)` → Net_MF_ZScore

---

### 6. market_technical（市场技术特征）

| 属性 | 值 |
|------|-----|
| task_name | market_technical |
| input_tables | tushare.stock_factor_pro |
| output_table | processor_market_technical |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | Mom_*_Median, Mom_*_Pos_Ratio, Vol_*_Median, Vol_Ratio_*, *_ZScore, *_Pctl |
| classification | 混合需拆分 |
| target_clean_table | clean.market_technical_base |
| features_to_extract | rolling_zscore, rolling_percentile, 横截面统计 |

**Clean 层输出列：**
- trade_date, ts_code, close, vol, turnover_rate, pct_chg
- 血缘列：_source_table, _processed_at, _data_version, _ingest_job_id

**特征层计算：**
- 横截面动量统计：mom_*_median, mom_*_pos_ratio, strong_mom_ratio, weak_mom_ratio
- 横截面波动统计：vol_*_median, high_vol_ratio, low_vol_ratio
- 横截面量比统计：vol_ratio_*_median, vol_expand_ratio, vol_shrink_ratio
- 价量背离统计：price_up_vol_down_ratio, price_down_vol_up_ratio
- `rolling_zscore(*, window=252)` → *_ZScore
- `rolling_percentile(*, window=500)` → *_Pctl

---

### 7. style_index_return（风格指数收益）

| 属性 | 值 |
|------|-----|
| task_name | style_index_return |
| input_tables | tushare.index_factor_pro |
| output_table | processor_style_index_return |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | *_Return, *_Return_5D, *_Return_20D, *_Return_60D |
| classification | 特征下沉 |
| target_clean_table | clean.style_base |
| features_to_extract | pct_change (多窗口) |

**说明：** 该任务基于风格指数收盘价计算多窗口收益率，属于纯特征计算，应下沉到特征层。

**Clean 层输出列：**
- trade_date, ts_code, close
- 血缘列：_source_table, _processed_at, _data_version, _ingest_job_id

**特征层计算：**
- `prices.pct_change()` → *_Return
- `prices.pct_change(5)` → *_Return_5D
- `prices.pct_change(20)` → *_Return_20D
- `prices.pct_change(60)` → *_Return_60D

---

### 8. futures_basis（期货基差）

| 属性 | 值 |
|------|-----|
| task_name | futures_basis |
| input_tables | tushare.future_daily, tushare.index_factor_pro |
| output_table | processor_futures_basis |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | *_Basis, *_Basis_Ratio, *_Basis_ZScore, *_Basis_Pctl, *_Basis_Ratio_ZScore, *_Basis_Ratio_Pctl |
| classification | 混合需拆分 |
| target_clean_table | clean.futures_base |
| features_to_extract | rolling_zscore, rolling_percentile |

**Clean 层输出列：**
- trade_date, ts_code, close (期货), settle, oi
- trade_date, ts_code, close (现货指数)
- 血缘列：_source_table, _processed_at, _data_version, _ingest_job_id

**特征层计算：**
- `index_close - futures_close` → *_Basis (加权平均)
- `basis / index_close` → *_Basis_Ratio
- `rolling_zscore(basis, window=252)` → *_Basis_ZScore
- `rolling_percentile(basis, window=252)` → *_Basis_Pctl

---

### 9. member_position（期货会员持仓）

| 属性 | 值 |
|------|-----|
| task_name | member_position |
| input_tables | tushare.future_holding |
| output_table | processor_member_position |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | *_NET_LONG, *_NET_CHG, *_RATIO, *_ZScore, *_Pctl |
| classification | 混合需拆分 |
| target_clean_table | clean.futures_holding_base |
| features_to_extract | rolling_zscore, rolling_percentile |

**Clean 层输出列：**
- trade_date, symbol, total_long_hld, total_short_hld, total_long_chg, total_short_chg
- 血缘列：_source_table, _processed_at, _data_version, _ingest_job_id

**特征层计算：**
- `total_long_hld - total_short_hld` → *_NET_LONG
- `total_long_chg - total_short_chg` → *_NET_CHG
- `total_long_hld / total_short_hld` → *_RATIO
- `rolling_zscore(*, window=120)` → *_ZScore
- `rolling_percentile(*, window=120)` → *_Pctl

---

### 10. option_iv（期权隐含波动率）

| 属性 | 值 |
|------|-----|
| task_name | option_iv |
| input_tables | tushare.option_daily, tushare.option_basic, tushare.fund_daily |
| output_table | processor_option_iv |
| primary_key | trade_date |
| time_column | trade_date |
| feature_columns | *_IV_Near, *_IV_Next, *_IV_30D, *_IV_ShortTerm |
| classification | 混合需拆分 |
| target_clean_table | clean.option_iv_base |
| features_to_extract | BS 反推 IV, 方差插值 |

**Clean 层输出列：**
- trade_date, ts_code (期权), opt_price, oi, call_put, exercise_price, maturity_date, days_to_expiry
- trade_date, ts_code (ETF), close
- 血缘列：_source_table, _processed_at, _data_version, _ingest_job_id

**特征层计算：**
- BS 模型反推 IV → *_IV_Near, *_IV_Next
- 方差线性插值 → *_IV_30D
- 近月/次近月平均 → *_IV_ShortTerm

---

## 汇总统计

| 分类 | 任务数量 | 任务列表 |
|------|----------|----------|
| 处理层保留 | 1 | industry_return |
| 特征下沉 | 2 | industry_breadth, style_index_return |
| 混合需拆分 | 7 | index_valuation, index_volatility, market_money_flow, market_technical, futures_basis, member_position, option_iv |

## 迁移优先级建议

| 优先级 | 任务 | 理由 |
|--------|------|------|
| P0 | index_valuation | 高复用、长窗口特征，已在 design.md 中定义 clean 表 |
| P0 | index_volatility | 高复用、风控必需，已在 design.md 中定义 clean 表 |
| P1 | market_money_flow | 高复用、资金流分析核心 |
| P1 | futures_basis | 高复用、基差分析核心 |
| P2 | market_technical | 特征较多，需分批迁移 |
| P2 | option_iv | IV 计算复杂，需单独处理 |
| P3 | industry_breadth | 依赖 industry_return 的 clean 数据 |
| P3 | style_index_return | 特征简单，可快速迁移 |
| P3 | member_position | 依赖 futures 的 clean 数据 |

## 待提取特征函数汇总

| 特征函数 | 使用任务 | 说明 |
|----------|----------|------|
| rolling_percentile | index_valuation, index_volatility, market_money_flow, market_technical, futures_basis, member_position | 滚动分位数 |
| rolling_zscore | market_money_flow, market_technical, futures_basis, member_position | 滚动标准化 |
| rolling_std | index_volatility | 滚动标准差（RV 计算） |
| pct_change | industry_return, style_index_return | 收益率计算 |
| cross_section_stats | industry_breadth, market_technical | 横截面统计（中位数、分位数、比例） |
| bs_implied_vol | option_iv | BS 模型反推 IV |
| variance_interpolation | option_iv | 方差线性插值 |

---

*文档生成时间：2025-12-10*
*Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6*
