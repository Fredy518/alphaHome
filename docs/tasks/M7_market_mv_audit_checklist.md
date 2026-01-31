# M7 Market 域 MV 算法正确性对齐审查 Checklist

更新时间：2026-01-30（第二轮改进完成）

> 目标：对 M7 market 域全部 MV 做"源表 → fetcher 参照实现 → MV SQL"三方口径对齐，重点覆盖：字段/单位、时间语义（滚动窗口/是否排除当日）、PIT/前视风险、NULL/异常值防护、与 data_infra.fetchers 的一致性。

## 0. 使用说明

- 本文是**可追踪的审查清单**：每个 MV 下按维度列出可勾选项。
- 约定：
  - `[x]` 已审查并确认
  - `[ ]` 待审查
  - `⚠️` 有不确定点/需要确认源表字段类型或业务意图
  - `🧪` 建议增加/补强测试或数据质量监控
  - `✅` 本轮已改进

## 1. 综览（按 MV）

| MV (recipe.name) | 源表 | 参照 fetcher | 字段/单位 | 时间语义/前视 | PIT | 备注 |
|---|---|---|---|---|---|---|
| market_stats | rawdata.stock_dailybasic | 无（独立 MV） | ✅ | ✅ | N/A | ✅ 已改进：输出全市场股票数 + 有效样本数 + 覆盖率 + 数据质量监控 |
| market_sentiment_daily | tushare.stock_factor_pro / stock_limitlist / stock_st / **stock_margin** | market_return_distribution（部分指标） | ✅ | ✅ | N/A | ✅ 已合并 margin 指标（日频） |
| ~~market_margin_monthly~~ | — | — | — | — | — | ⚠️ **已废弃**：margin 指标已合并至 market_sentiment_daily |
| margin_turnover_daily | tushare.stock_margin / stock_daily | margin.MarginTurnoverFetcher | ✅ | ✅ | N/A | amount 千元→元已对齐 |
| market_technical_daily | tushare.stock_factor_pro | market_technical.MarketTechnicalFetcher | ✅ | ✅ | N/A | pct_chg 为百分数刻度；amount 与源表一致 |
| industry_features_daily | tushare.index_swdaily / index_swmember | 无（独立 MV） | ✅ | ✅ | N/A | 返回为小数收益（非百分数） |
| style_features_daily | tushare.index_factor_pro | style.StyleIndexReturnFetcher / StyleMomentumFetcher | ✅ | ✅ | N/A | ✅ 已修正：500价值=H30351.CSI，500成长=H30352.CSI |
| index_technical_daily | tushare.index_factor_pro | index.IndexBollSignalsFetcher / IndexMA120DistanceFetcher（部分） | ✅ | ✅ | N/A | ✅ 已统一指数清单（含 SZZZ） |
| index_features_daily | index_dailybasic / index_factor_pro / macro_bond_rate | 无（独立 MV） | ✅ | ✅ | N/A | 估值分位数采用"仅历史观测"统计（排除当日） |
| index_fundamental_daily | index_weight / stock_dailybasic | index_fundamental.IndexFundamentalFetcher | ✅ | ✅ | ✅ | ✅ 已统一指数清单（含 SZZZ） |
| etf_flow_daily | fund_share / fund_nav / fund_etf_basic | etf.ETFFlowFetcher | ✅ | ✅ | N/A | ✅ 已改为动态枚举（从 fund_etf_basic.index_code 筛选） |
| macro_rate_daily | akshare.macro_bond_rate | 无（独立 MV） | ✅ | ✅ | N/A | 分位数改为"历史窗口 <= 当前值的比例"（排除当日） |

## 2. 单个 MV 审查清单

### 2.1 market_stats ✅ 已改进

文件：alphahome/features/recipes/mv/market/market_stats_daily.py

- [x] 源表字段存在性：rawdata.stock_dailybasic 具备 trade_date/pe_ttm/pb/ps_ttm/dv_ttm/turnover_rate/total_mv/circ_mv
- [x] 单位：total_mv/circ_mv 视为"元"或"万元"？（本 MV 仅做聚合/统计，不做比值换算）
- [x] 异常值过滤：pe_ttm (0,1000)、pb (0,100)、total_mv > 0
- [x] ✅ stock_count 口径：已改进，同时输出 total_stock_count（全市场）+ valid_stock_count（有效样本）+ valid_coverage_ratio
- [x] ✅ 数据质量监控：已添加 prev_total_stock_count、prev_pe_ttm_median 用于检测突变

### 2.2 market_sentiment_daily ✅ 已改进

文件：alphahome/features/recipes/mv/market/market_sentiment_daily.py

- [x] 源表字段：
  - stock_factor_pro: close_hfq, ma_hfq_60, ma_hfq_90, pct_chg
  - stock_limitlist: limit/open_times/limit_times/first_time/amount
  - stock_st: ts_code, trade_date
  - ✅ stock_margin: rzye, rzmre, rzche（新增，合并自 market_margin_monthly）
  - ✅ stock_dailybasic: circ_mv（用于计算融资余额占比）
- [x] 市场宽度：above_ma60/above_ma90 按日计数 + 比例
- [x] 新高新低：52周 rolling window 使用 `ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING`，排除当日避免自比较
- [x] 涨跌停：使用 stock_limitlist + stock_st 精确识别，且分 board_type（主板/20cm/北交所）
- [x] first_time 解析兼容 'HH:MM:SS' 与 'HHMMSS'
- [x] ✅ 融资融券指标：total_margin_balance、margin_circ_ratio、margin_net_buy_billion（日频）
- [x] early_limit_count 阈值：固定 10:00（不需要配置化）
- [x] ✅ 单位说明：total_margin_balance/total_short_balance 为“元”，并新增 *_billion 字段为“亿元”以便下游直接使用

### 2.3 market_margin_monthly ⚠️ 已废弃

**状态：已废弃/已移除（原月频 MV 不再对外提供）**

- ⚠️ margin 指标已合并至 market_sentiment_daily（日频）
- 迁移指引：使用 market_sentiment_daily 中的 margin_* 字段替代

### 2.4 margin_turnover_daily

文件：alphahome/features/recipes/mv/market/margin_turnover_daily.py

参照：data_infra/fetchers/margin.py::MarginTurnoverFetcher

- [x] 市场成交额：stock_daily.amount 单位"千元"，MV 内转换为元（*1000）
- [x] 两融成交占比：对齐 fetcher 的口径 = (rzmre + rzche) / market_amount_yuan * 100
- [x] 避免字段缺失：不依赖 stock_margin.rqchl（fetcher 明确提示该字段常不存在）
- [x] spike：对齐 fetcher = ratio / MA20 - 1

### 2.5 market_technical_daily

文件：alphahome/features/recipes/mv/market/market_technical_daily.py

参照：data_infra/fetchers/market_technical.py::MarketTechnicalFetcher

- [x] 动量：用 close_hfq 的 LAG(5/10/20/60) 并乘以 100（百分数刻度）
- [x] 量比：分母窗口为 `ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING`、`20 PRECEDING AND 1 PRECEDING`（排除当日）
- [x] 波动：STDDEV(pct_chg) * sqrt(252)（pct_chg 为百分数刻度，输出为年化百分数刻度）
- [x] 价量背离阈值：0.8/1.2 与 fetcher 一致
- [x] amount 字段单位：与源表保持一致（无单位转换）

### 2.6 industry_features_daily

文件：alphahome/features/recipes/mv/market/industry_features_daily.py

- [x] 二级行业清单：index_swmember.l2_code 去重
- [x] 日收益率：close / lag(close) - 1（小数）
- [x] 宽度指标：上涨/强势/弱势行业比例
- [x] 滚动：5D/20D rolling mean 使用 CURRENT ROW（不前视）
- [x] ✅ 行业收益分布形态：输出偏度（industry_return_skew）与峰度（industry_return_kurtosis_excess）

### 2.7 style_features_daily ✅ 已改进

文件：alphahome/features/recipes/mv/market/style_features_daily.py

参照：data_infra/fetchers/style.py::StyleIndexReturnFetcher + StyleMomentumFetcher

- [x] 指数价格：index_factor_pro.close
- [x] 收益：ret_5d/20d/60d 为 pct_change（小数）
- [x] 相对强弱：large_small/value_growth/dividend_excess 与 fetcher 一致（差值形式）
- [x] ✅ 指数代码修正：
  - 500价值：H30351.CSI（原误用 000925.CSI）
  - 500成长：H30352.CSI（原误用 000926.CSI）

### 2.8 index_technical_daily ✅ 已改进

文件：alphahome/features/recipes/mv/market/index_technical_daily.py

参照：data_infra/fetchers/index.py::IndexBollSignalsFetcher + IndexMA120DistanceFetcher

- [x] 布林带字段：boll_upper_bfq / boll_lower_bfq（与 fetcher 一致）
- [x] MA60/MA120：SQL rolling AVG(close)
- [x] 信号：close 与 boll 轨比较输出 0/1
- [x] ✅ 指数清单：已统一为 HS300/ZZ500/ZZ1000/SZ50/CYB/SZZZ（6个）

### 2.9 index_features_daily

文件：alphahome/features/recipes/mv/market/index_features_daily.py

- [x] 估值分位数：使用 LATERAL 子查询统计"历史窗口内 <= 当前值的比例"，且 trade_date < 当前日（排除当日）
- [x] ERP：ERP = 1/PE - CN10Y（以小数利率参与，yield/100）
- [x] 波动率：index_factor_pro.close 的日收益做 STDDEV * sqrt(252)
- [x] as-of join：宏观利率使用最近一个 <= trade_date 的 10y

### 2.10 index_fundamental_daily ✅ 已改进

文件：alphahome/features/recipes/mv/market/index_fundamental_daily.py

参照：data_infra/fetchers/index_fundamental.py::IndexFundamentalFetcher

- [x] PIT 权重：每个 trade_date 仅使用 <= trade_date 的最近一次权重披露日
- [x] 权重归一：weight_norm = weight / sum(weight)
- [x] PE/PB：倒数加权（E/P 或 B/P 加权后取倒数）
- [x] 覆盖率：pe_coverage/pb_coverage 输出用于质量监控
- [x] ✅ 指数清单：已统一为 HS300/ZZ500/ZZ1000/SZ50/CYB/SZZZ（6个）

### 2.11 etf_flow_daily ✅ 已改进

文件：alphahome/features/recipes/mv/market/etf_flow_daily.py

参照：data_infra/fetchers/etf.py::ETFFlowFetcher

- [x] 日期字段：fund_share.trade_date；fund_nav.nav_date → trade_date
- [x] NAV 对齐：使用 as-of join（取最近一个 <= trade_date 的 NAV）并以 1.0 兜底
- [x] 资金流定义：Δ份额 × NAV（每只 ETF 先算再汇总），单位为亿元（/10000）
- [x] ✅ ETF 列表策略：已改为动态枚举（从 fund_etf_basic.index_code 筛选跟踪目标指数的 ETF）
  - 目标指数：HS300/ZZ500/ZZ1000/SZ50/CYB/KC50/CSI1000/CSI500/CSI300/SSE50/科创50
  - 仅使用上市后数据（list_date 过滤）

### 2.12 macro_rate_daily

文件：alphahome/features/recipes/mv/market/macro_rate_daily.py

- [x] 原始表透视：按 (date,country,term) pivot 成宽表
- [x] 利差：10y-2y、30y-10y、中美利差
- [x] 变化：LAG 差分并乘 100 转 bp
- [x] 分位数：使用历史窗口（1y/3y/5y）中 <= 当前值的比例，且 trade_date < 当前日（排除当日）

## 3. 本轮改进汇总

### 3.1 已完成改进

1. **market_stats**：
   - 新增 total_stock_count（全市场股票数）
   - 新增 valid_stock_count（有效估值样本数）
   - 新增 valid_coverage_ratio（有效覆盖率）
   - 新增 prev_total_stock_count、prev_pe_ttm_median（数据质量监控）

2. **market_sentiment_daily**：
   - 合并 margin 指标（日频），新增字段：
     - total_margin_balance（两融余额，亿元）
     - margin_circ_ratio（融资余额/流通市值，%）
     - margin_net_buy_billion（融资净买入，亿元）
   - 更新血缘：新增 stock_margin、stock_dailybasic

3. **market_margin_monthly**：
   - 标记为 DEPRECATED
   - 移除 @feature_register
   - 保留文件供历史参考

4. **style_features_daily**：
   - 修正 500价值指数代码：000925.CSI → H30351.CSI
   - 修正 500成长指数代码：000926.CSI → H30352.CSI

5. **index_technical_daily / index_fundamental_daily**：
   - 统一指数清单为 6 个：HS300/ZZ500/ZZ1000/SZ50/CYB/SZZZ
   - 新增上证指数（000001.SH）

6. **etf_flow_daily**：
   - 从静态 ETF 列表改为动态枚举
   - 从 fund_etf_basic.index_code 筛选跟踪目标指数的 ETF
   - 添加 list_date 过滤（仅使用上市后数据）

### 3.2 新增测试

- 新增 `tests/unit/test_features/test_mv_sql_patterns.py`
- 覆盖 PIT/排除当日等关键 SQL 模式的单测
- 12 个测试用例，全部通过

### 3.3 本轮（Phase 2）新增 MV

1. **futures_features_daily**（新增）
   - 涵盖：IF/IC/IM/IH 基差、会员持仓净多空、前20席位
   - 源表：future_daily、future_holding、index_daily
   - 文件：alphahome/features/recipes/mv/market/futures_features_daily.py

2. **risk_appetite_daily**（新增）
   - 涵盖：ST 股票情绪、北交所情绪
   - 源表：stock_factor_pro、stock_st、stock_basic
   - 文件：alphahome/features/recipes/mv/market/risk_appetite_daily.py

3. **market_size_daily**（新增）
   - 涵盖：大小盘收益差、成交额集中度
   - 源表：stock_factor_pro
   - 文件：alphahome/features/recipes/mv/market/market_size_daily.py

### 3.4 待后续确认

- industry_features_daily：是否需要输出行业收益的偏度/峰度
- early_limit_count 的阈值（10:00）是否需要配置化
- pcr_weekly / cb_risk_appetite / microcap_risk_appetite 数据覆盖率验证
- market_industry_flow 与 industry_features_daily 的功能重叠评估
