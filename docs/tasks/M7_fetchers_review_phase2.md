# M7 Phase 2：19 项"需评审"Fetcher 甄别报告

> 文档日期：2026-01-30  
> 审查范围：`data_infra/fetchers/` 中 19 个标记为 🔵（需评审）的 Fetcher  
> 评审目标：逐项给出"入库 / 不入库 / 保留评审"决策及理由

---

## 1. 评审汇总

| 序号 | Fetcher | 决策 | 主要理由 | 落库形态 |
|------|---------|------|----------|----------|
| 1 | `money_flow` | ❌ 不入库 | 口径不稳（主力定义多变）；缺大单/特大单字段 | - |
| 2 | `futures_basis` | ✅ 入库 | 口径清晰；依赖表已有（future_daily、index_factor_pro） | MV |
| 3 | `member_position` | ✅ 入库 | 会员持仓净多空/多空比；数据源 future_holding 可用 | MV |
| 4 | `pcr_weekly` | 🔵 保留 | 周频；ETF 期权覆盖率待验证（OP510050/300/500） | - |
| 5 | `option_iv` | ❌ 不入库 | 强依赖 scipy；BS 反推极端情况 NaN 多；计算应留消费端 | - |
| 6 | `iv_term_structure` | ❌ 不入库 | 依赖 option_iv；叠加复杂度 | - |
| 7 | `rsrs` | ❌ 不入库 | 择时信号（策略层），非特征；参数敏感（窗口 18/600） | - |
| 8 | `market_industry_flow` | 🔵 保留 | 申万二级行业 join 复杂；与 industry_features_daily 功能重叠待评估 | - |
| 9 | `cb_risk_appetite` | 🔵 保留 | cbond_daily 覆盖率待验证（2020 年后数据较全） | - |
| 10 | `st_risk_appetite` | ✅ 入库 | stock_st + stock_factor_pro 均可用；ST 情绪指标稳定 | MV |
| 11 | `bse_risk_appetite` | ✅ 入库 | 北交所流动性指标；stock_basic+factor_pro 可用 | MV |
| 12 | `microcap_risk_appetite` | 🔵 保留 | "微盘股"口径需确认（后 10% 市值 or 固定阈值？） | - |
| 13 | `risk_appetite_composite` | ❌ 不入库 | 组合指标，依赖前述子 Fetcher；不宜在生产端固化权重 | - |
| 14 | `index_factor_pro` | ❌ 不入库 | 原表已落库（tushare.index_factor_pro）；Fetcher 仅做列筛选+重命名，无增量价值 | - |
| 15 | `market_valuation_distribution` | ✅ 入库 | PE/PB 分位聚合；与 market_stats 互补 | MV |
| 16 | `market_turnover_distribution` | ✅ 入库 | 换手率分布、成交集中度；与 market_technical 互补 | MV |
| 17 | `market_momentum_distribution` | ✅ 入库 | 动量/RSI 分布；与 market_technical 互补 | MV |
| 18 | `market_size_dispersion` | ✅ 入库 | 大小盘收益差、集中度；独立指标 | MV |
| 19 | `market_volatility_distribution` | ✅ 入库 | ATR/振幅分布；与 index_features_daily 互补 | MV |

---

## 2. 逐项详细评审

### 2.1 money_flow（❌ 不入库）

**源码**：`data_infra/fetchers/flow.py::MoneyFlowFetcher`

**依赖表**：
- `tushare.stock_moneyflow`（主力净流入）
- `tushare.stock_dailybasic`（流通市值）

**输出**：
- `total_net_mf_amount`、`Net_MF_ZScore`、`Net_MF_Rate_Daily`
- 原设计含大单/特大单字段，但 **stock_moneyflow 表无 net_mf_amount_lg/net_mf_amount_elg**

**问题**：
1. 口径不稳：不同数据源对"主力"的定义不同（大单阈值 50w/100w/200w 均有）
2. 缺失关键字段：大单/特大单字段在 tushare 表中不存在
3. 跨策略复用低：资金流策略依赖自定义口径

**决策**：❌ 不入库（保留 QuantLab 端按需消费）

---

### 2.2 futures_basis（✅ 入库）

**源码**：`data_infra/fetchers/futures.py::FuturesBasisFetcher`

**依赖表**：
- `tushare.future_daily`（期货日行情）
- `tushare.index_factor_pro`（现货指数）

**输出**：
- `{IF/IC/IM}_Basis`：加权平均基差
- `{IF/IC/IM}_Basis_Ratio`：非年化贴水率（基差/现货点位）

**优点**：
- 口径清晰（加权平均基差）
- 依赖表已入库、覆盖率高
- 跨策略复用（CTA、对冲、择时）

**决策**：✅ 入库（MV）

---

### 2.3 member_position（✅ 入库）

**源码**：`data_infra/fetchers/futures.py::MemberPositionFetcher`

**依赖表**：
- `tushare.future_holding`（会员持仓明细）

**输出**：
- `{IF/IC/IH/IM}_MEMBER_POSITION_NET_LONG`：净多头
- `{IF/IC/IH/IM}_MEMBER_POSITION_NET_CHG`：净变化
- `{IF/IC/IH/IM}_MEMBER_POSITION_RATIO`：多空比

**优点**：
- 口径稳定（多头持仓 - 空头持仓）
- 机构持仓信号有跨策略价值

**注意**：future_holding 数据从 2015-04 开始，早期覆盖率需验证

**决策**：✅ 入库（MV）

---

### 2.4 pcr_weekly（🔵 保留评审）

**源码**：`data_infra/fetchers/options.py::PCRFetcher`

**依赖表**：
- `tushare.option_basic`（期权基本信息）
- `tushare.option_daily`（期权日行情）

**输出**：
- 分标的 PCR（ETF50/HS300/ZZ500/KC50 等）
- 全市场汇总 PCR

**问题**：
1. **周频**：与日频 MV 体系不完全对齐
2. **ETF 期权覆盖率**：OP510050（50ETF）数据最全，500ETF/科创50 较新
3. 不含 CFFEX 指数期权（设计有意排除）

**决策**：🔵 保留评审（待验证 option_basic/daily 覆盖率后再定）

---

### 2.5 option_iv（❌ 不入库）

**源码**：`data_infra/fetchers/option_iv.py::OptionIVFetcher`

**依赖表**：
- `tushare.option_daily`
- `tushare.option_basic`

**输出**：
- 分标的隐含波动率（BS 反推）
- VIX 类似指标

**问题**：
1. **强依赖 scipy**：生产环境需额外安装
2. **BS 反推极端情况 NaN 多**：深度虚值期权、低流动性合约
3. **计算逻辑复杂**：应留在消费端按需计算

**决策**：❌ 不入库

---

### 2.6 iv_term_structure（❌ 不入库）

**源码**：`data_infra/fetchers/option_iv.py::IVTermStructureFetcher`

**依赖**：依赖 option_iv 的输出

**问题**：叠加 option_iv 的所有问题

**决策**：❌ 不入库

---

### 2.7 rsrs（❌ 不入库）

**源码**：`data_infra/fetchers/rsrs.py::RSRSFetcher`

**依赖表**：
- `tushare.index_factor_pro`（指数 high/low）

**输出**：
- `RSRS_Beta`、`RSRS_R2`、`RSRS_ZScore`、`RSRS_ZScore_R2`

**问题**：
1. **择时信号而非特征**：RSRS 是策略层产物，参数（回归窗口 18、Z-Score 窗口 600）高度敏感
2. **不宜固化**：不同策略需要不同参数组合
3. 信号本身不应落库（应落"输入"而非"输出"）

**决策**：❌ 不入库（保留 QuantLab 端按策略参数计算）

---

### 2.8 market_industry_flow（🔵 保留评审）

**源码**：`data_infra/fetchers/industry.py::MarketIndustryFlowFetcher`

**依赖表**：
- `tushare.stock_factor_pro`
- `tushare.index_swmember`（申万二级）

**输出**：
- 行业内上涨/下跌个股数量与比例
- 行业成交额、上涨/下跌成交额占比
- 行业成交额占全市场比例

**问题**：
1. 与 `industry_features_daily` 存在功能重叠（同样基于申万二级）
2. 输出粒度更细（每行一个行业），join 复杂度高
3. 需评估是否合并到 industry_features_daily 或独立落库

**决策**：🔵 保留评审（待与 industry_features_daily 对比后决定）

---

### 2.9 cb_risk_appetite（🔵 保留评审）

**源码**：`data_infra/fetchers/risk_appetite.py::ConvertibleBondRiskAppetiteFetcher`

**依赖表**：
- `tushare.cbond_daily`
- `tushare.stock_factor_pro`（全市场成交额）

**输出**：
- 可转债成交额占比、涨跌幅分布、转股溢价率

**问题**：
1. **cbond_daily 覆盖率**：2020 年后数据较全，早期需验证
2. 可转债市场规模近年增长快，历史回测代表性存疑

**决策**：🔵 保留评审（待验证 cbond_daily 覆盖率后再定）

---

### 2.10 st_risk_appetite（✅ 入库）

**源码**：`data_infra/fetchers/risk_appetite.py::STStockRiskAppetiteFetcher`

**依赖表**：
- `tushare.stock_st`（每日 ST 股票列表）
- `tushare.stock_factor_pro`

**输出**：
- ST 股票成交额占比、涨跌幅分布、涨跌停比例、换手率

**优点**：
- 口径清晰（动态 ST 列表避免幸存者偏差）
- stock_st 数据从 2016-08 开始，覆盖率可接受
- ST 情绪是经典风险偏好指标

**决策**：✅ 入库（MV）

---

### 2.11 bse_risk_appetite（✅ 入库）

**源码**：`data_infra/fetchers/risk_appetite.py::BSERiskAppetiteFetcher`

**依赖表**：
- `tushare.stock_basic`（exchange = 'BSE' 或 ts_code LIKE '%.BJ'）
- `tushare.stock_factor_pro`

**输出**：
- 北交所股票成交额占比、涨跌幅分布、大涨大跌比例

**优点**：
- 口径清晰（按 exchange 筛选）
- 北交所 2021-11 开市，数据完整
- 边缘市场情绪指标有独立价值

**决策**：✅ 入库（MV）

---

### 2.12 microcap_risk_appetite（🔵 保留评审）

**源码**：`data_infra/fetchers/risk_appetite.py::MicroCapRiskAppetiteFetcher`

**依赖表**：
- `tushare.stock_factor_pro`

**输出**：
- 微盘股成交额占比、涨跌幅分布、与大盘相对强弱

**问题**：
1. **"微盘股"口径不统一**：后 10% 市值 vs 固定阈值（<20 亿 / <50 亿）
2. 不同口径下信号差异大

**决策**：🔵 保留评审（需明确口径后再落库）

---

### 2.13 risk_appetite_composite（❌ 不入库）

**源码**：`data_infra/fetchers/risk_appetite.py::RiskAppetiteCompositeFetcher`

**依赖**：cb/st/bse/microcap 四个子 Fetcher 的输出

**输出**：
- 综合风险偏好得分

**问题**：
1. **组合权重不宜固化**：不同策略对子指标的权重偏好不同
2. 应落"原子指标"，组合在消费端完成

**决策**：❌ 不入库

---

### 2.14 index_factor_pro（❌ 不入库）

**源码**：`data_infra/fetchers/index_factor_pro.py::IndexFactorProFetcher`

**依赖表**：
- `tushare.index_factor_pro`

**输出**：
- 指数技术指标（按指数别名重命名）

**问题**：
1. **源表已落库**：tushare.index_factor_pro 已在 rawdata 层
2. Fetcher 仅做列筛选 + 重命名，无增量价值
3. 下游可直接查源表

**决策**：❌ 不入库

---

### 2.15 market_valuation_distribution（✅ 入库）

**源码**：`data_infra/fetchers/market_cross_section.py::MarketValuationDistributionFetcher`

**依赖表**：
- `tushare.stock_factor_pro`

**输出**：
- PE/PB 中位数、均值、分位数
- 低估/高估股票比例
- 历史分位数（1000 日滚动）

**优点**：
- 与 `market_stats` 互补（market_stats 输出全市场统计，此处输出分布）
- 口径清晰（PE_TTM > 0 且 < 500 过滤极端值）

**决策**：✅ 入库（可合并到 market_stats 或独立 MV）

---

### 2.16 market_turnover_distribution（✅ 入库）

**源码**：`data_infra/fetchers/market_cross_section.py::MarketTurnoverDistributionFetcher`

**依赖表**：
- `tushare.stock_factor_pro`

**输出**：
- 换手率中位数、分位数
- 高/低换手股票比例
- 成交集中度（top10/top50 成交额占比）

**优点**：
- 与 `market_technical_daily` 互补（换手率分布维度）
- 成交集中度是独立指标

**决策**：✅ 入库（可合并到 market_technical_daily 或独立 MV）

---

### 2.17 market_momentum_distribution（✅ 入库）

**源码**：`data_infra/fetchers/market_cross_section.py::MarketMomentumDistributionFetcher`

**依赖表**：
- `tushare.stock_factor_pro`

**输出**：
- RSI 分布（中位数、超买/超卖比例）
- MA 强度得分（高于 MA20/MA60 比例）

**优点**：
- 与 `market_technical_daily` 互补（个股动量分布维度）

**决策**：✅ 入库（可合并到 market_technical_daily 或独立 MV）

---

### 2.18 market_size_dispersion（✅ 入库）

**源码**：`data_infra/fetchers/market_cross_section.py::MarketSizeDispersionFetcher`

**依赖表**：
- `tushare.stock_factor_pro`

**输出**：
- 大/小盘股平均收益及收益差
- 大盘股成交额占比
- 市值中位数

**优点**：
- 独立指标（大小盘轮动信号）
- 口径清晰（按市值十分位分组）

**决策**：✅ 入库（MV）

---

### 2.19 market_volatility_distribution（✅ 入库）

**源码**：`data_infra/fetchers/market_cross_section.py::MarketVolatilityDistributionFetcher`

**依赖表**：
- `tushare.stock_factor_pro`

**输出**：
- ATR 分布（中位数、P75）
- 日内振幅分布
- 高波动股票比例

**优点**：
- 与 `index_features_daily` 互补（个股波动率分布维度）

**决策**：✅ 入库（可合并到 market_technical_daily 或独立 MV）

---

## 3. 决策统计

| 决策 | 数量 | 占比 |
|------|------|------|
| ✅ 入库 | 10 | 52.6% |
| ❌ 不入库 | 6 | 31.6% |
| 🔵 保留评审 | 5 | 26.3% |

### 3.1 可入库项（10 个）

建议落库形态均为 **MV**，可考虑合并到已有 MV 或新建独立 MV：

| Fetcher | 建议落库位置 |
|---------|-------------|
| futures_basis | 新建 `futures_features_daily` |
| member_position | 合并到 `futures_features_daily` |
| st_risk_appetite | 新建 `risk_appetite_daily` |
| bse_risk_appetite | 合并到 `risk_appetite_daily` |
| market_valuation_distribution | 合并到 `market_stats` 或独立 |
| market_turnover_distribution | 合并到 `market_technical_daily` 或独立 |
| market_momentum_distribution | 合并到 `market_technical_daily` 或独立 |
| market_size_dispersion | 新建 `market_size_daily` |
| market_volatility_distribution | 合并到 `market_technical_daily` 或独立 |

### 3.2 不入库项（6 个）

| Fetcher | 原因 |
|---------|------|
| money_flow | 口径不稳；缺关键字段 |
| option_iv | 强依赖 scipy；计算应留消费端 |
| iv_term_structure | 依赖 option_iv |
| rsrs | 择时信号非特征 |
| risk_appetite_composite | 组合权重不宜固化 |
| index_factor_pro | 源表已落库；无增量价值 |

### 3.3 保留评审项（5 个）

| Fetcher | 待确认事项 |
|---------|-----------|
| pcr_weekly | 验证 option_basic/daily 覆盖率 |
| market_industry_flow | 与 industry_features_daily 功能对比 |
| cb_risk_appetite | 验证 cbond_daily 覆盖率 |
| microcap_risk_appetite | 明确"微盘股"口径 |

---

## 4. 下一步行动

1. **立即落库（高确定性）**：
   - `futures_basis` + `member_position` → 新建 `futures_features_daily`
   - `st_risk_appetite` + `bse_risk_appetite` → 新建 `risk_appetite_daily`
   - `market_size_dispersion` → 新建 `market_size_daily`

2. **评估后决定（合并 or 独立）**：
   - `market_valuation_distribution` / `market_turnover_distribution` / `market_momentum_distribution` / `market_volatility_distribution`
   - 建议：先合并到 `market_stats` 或 `market_technical_daily`，避免 MV 碎片化

3. **数据覆盖率验证后决定**：
   - `pcr_weekly`：查 option_daily 最早日期与覆盖率
   - `cb_risk_appetite`：查 cbond_daily 最早日期与覆盖率
   - `microcap_risk_appetite`：与用户确认口径后落库

4. **更新设计文档**：
   - 把本次决策同步到 `docs/architecture/features_module_design.md` 的 Section 5.4
