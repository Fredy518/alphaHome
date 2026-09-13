# AlphaDB 宏观数据字典
文件名：macro_data_dictionary.md
创建于：2026-06-21
创建者：ZCode
用途：供 CrossLens SPEC-015 及下游消费方参考的宏观数据表字段、口径、代理标注、新鲜度与推荐用法说明

## 概述
本文档覆盖 28 张宏观数据任务表（物理表位于 `akshare.*` / `fred.*` / `pbc.*` / `nbs.*` / `excel.*` schema，`rawdata.*` 为自动管理的同名视图）。所有表经 `@task_register()` 注册，可通过 GUI 或生产 SMART runner 增量更新；新表会在任务首次成功保存时自动创建。

**通用约定：**
- 所有数值列单位见各表"口径"列（% / pp / 亿元 / 百万美元）
- `update_time` 为行写入时间戳（TIMESTAMP，自动维护）
- 主键（PK）驱动 UPSERT，重跑幂等无重复
- `rawdata.<table>` 视图 = `SELECT * FROM <schema>.<table>`，下游查询可用任一
- `period_end_date` 只表示宏观指标统计期，不表示发布日期或当时可用日。下游做 PIT 研究时，优先连接可审计的发布日历；没有发布日证据时必须保留策略约定的保守固定滞后，不得把月末标签当作月末已知。

---

## 零、旧策略 40 指标历史合同（1 表）

### `excel.macro_style_rotation_legacy` — 原始工作簿的可追溯历史面板

| 字段 | 口径/说明 |
|---|---|
| `period_end_date` + `indicator_code` *(PK)* | 2011-02 至 2020-12，119 个月 × 40 个指标 |
| `value` / `is_observed` / `missing_reason` | 原值、是否有观测以及结构性 1 月/序列尚未开始/公式历史不足/源空值标记 |
| `release_date` | 固定为 NULL；旧工作簿不含可信发布日，不补造 PIT 日期 |
| `strategy_available_date_proxy` | 只保留旧策略“统计月后第 2 个月月初可用”的保守代理 |
| `source_workbook/source_cell/source_formula` | 工作簿、单元格和原公式追溯信息 |
| `source_workbook_sha256` | `7526814b13c4e4ada4d09639cf0ca3071e94e6549bebff84e1e8e57e1d3912b1` |

- **生产覆盖**：4,760 行，4,507 个有效观测，253 个显式缺失；40 个指标、119 个月的键均存在，无重复主键。
- **40 指标合同**：GROWTH 10 个、CONSUMPTION 7 个、CURRENCY 7 个、REAL_ESTATE 7 个、RATE 9 个；它是研究输入全集，不等于最终 13 票规则。
- **消费指标更正**：合同中是 `tot_retail_sales_yoy`，不是原 13 票代码中误写的 `retail_sales_yoy`。
- **任务行为**：`excel_macro_style_rotation_legacy` 可随“全选 + 智能增量”一起运行；固定重读只读历史区间并按主键幂等回写。任务会严格校验上述 SHA-256，工作簿内容漂移时失败关闭，不覆盖冻结历史。
- **路径配置**：优先读取任务配置 `excel_file_path`，其次读取环境变量 `ALPHAHOME_MACRO_STYLE_ROTATION_WORKBOOK`，最后回退到 AlphaHome 相邻的 `macroStrategy/宏观指标与逻辑.xlsx`；不依赖固定盘符。

---

## 一、中国宏观/流动性（11 表）

### 1. `akshare.macro_usa_cpi` — 美国 CPI 同比
> ⚠️ 表名含 usa 但归类此处因与美实际利率计算强相关；实为美国数据

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 数据月份（月初日，如 2026-05-01） |
| `release_date` | DATE | BLS 发布日期 |
| `cpi_yoy` | NUMERIC | 美国 CPI 同比（%） |
| `cpi_prev_yoy` | NUMERIC | 前值同比（%） |

- **数据源**：akshare `macro_usa_cpi_yoy`（美国 BLS，月频）
- **新鲜度**：最新 2026-05-01，月中发布上月，⚠️ 略滞后（akshare 接口偶有延迟）
- **用途**：算美实际利率 `us_real_yield_10y = 美债10Y名义 - cpi_yoy` → P0 `us_real_yield_10y_change_6m`
- **注意**：akshare 该接口仅提供 YoY，无 MoM；未发布月份（cpi_yoy IS NULL）已丢弃

### 2. `akshare.macro_policy_rate` — 中国政策利率（LPR/基准贷款利率）

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 利率公告日 |
| `lpr_1y` | NUMERIC | 1 年期 LPR（%） |
| `lpr_5y` | NUMERIC | 5 年期 LPR（%） |
| `benchmark_loan_1y` | NUMERIC | 历史 1 年期基准贷款利率（%） |
| `benchmark_loan_5y` | NUMERIC | 历史 5 年期以上基准贷款利率（%） |
| `mlf_1y` | NUMERIC | 1 年期 MLF 利率（**预留列，当前恒 NULL**） |

- **数据源**：akshare `macro_china_lpr`
- **新鲜度**：最新 2026-05-20（LPR 每月 20 日公布），✅ 新鲜
- **用途**：`policy_rate` → P0 `policy_rate_change_6m`
- **⚠️ 口径注意**：
  - 2019-08 前 `lpr_1y/lpr_5y` 为 NULL（LPR 改革前），用 `benchmark_loan_1y/5y` 填充
  - `mlf_1y` 恒 NULL（akshare 无 MLF 数据源），**需用 LPR 近似 policy_rate 或后续接 CFETS/Wind**
  - 每行为公告日（不定期/月频），非交易日序列

### 3. `akshare.macro_repo_rate` — 银行间回购定盘利率

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `fr001` | NUMERIC | 回购定盘利率 1 天（%） |
| `fr007` | NUMERIC | 回购定盘利率 7 天（%），**DR007 代理** |
| `fr014` | NUMERIC | 回购定盘利率 14 天（%） |

- **数据源**：akshare `repo_rate_query(symbol=回购定盘利率)`
- **新鲜度**：最新 2026-06-18，✅ 新鲜
- **用途**：`liquidity_metrics.dr007`（代理）
- **⚠️ 口径注意**：`fr007` 是**定盘利率（报价撮合）**，非 DR007（存款类机构成交加权利率）。走势相关但口径不同，**下游 evidence confidence 需降权**。回溯仅至 2023-06。

### 3A. `excel.macro_dr007_history` — DR007 历史缓存证据

`rawdata.macro_dr007_history` 指向本表。该序列用于没有 Wind/iFinD 在线
API 权限时的历史研究补库，不替代实时官方数据源。

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `trade_date` *(PK)* | DATE | 工作簿中的日度日期 |
| `dr007_pct` | NUMERIC | 存款类机构 7 天质押式回购成交加权利率（%） |
| `availability_date_proxy` | DATE | 日度市场收盘可用日代理，等于 `trade_date` |
| `is_weekend` | BOOLEAN | 工作簿缓存是否落在周末；下游仍须按正式交易日历筛选 |
| `source_series_id` | VARCHAR | iFinD 指标编号 `L001619493` |
| `source_workbook_sha256` | VARCHAR | 本次来源工作簿内容哈希 |
| `source_workbook_mtime` | TIMESTAMP | 本项目首次可核验的文件修改时间 |
| `source_cell` | VARCHAR | 原工作簿单元格位置 |
| `evidence_status` | VARCHAR | 固定为历史供应商缓存、无在线 API 复核 |

- **数据源**：`宏观指标与逻辑.xlsx` 的 `DateRate`/`DR007` 列，iFinD
  指标编号 `L001619493`。
- **现有覆盖**：2014-12-15 至 2025-05-27，共 2,799 行；2021-09-01
  至 2024-12-31 覆盖全部 807 个沪深交易日。
- **维护约束**：`tasks.excel_macro_dr007_history.expected_workbook_sha256`
  锁定来源版本；工作簿变化后必须复核哈希再更新配置。
- **使用边界**：工作簿含周末缓存值，模型输入只连接正式交易日；
  `FR007` 与 `DR007` 不是同一指标，禁止拼接或相互冒充。

### 4. `akshare.macro_money_supply` — 中国货币供应量（M0/M1/M2，长表）

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `month` *(PK)* | VARCHAR | 月份（YYYYMM，如 202605） |
| `aggregate` *(PK)* | VARCHAR | 货币层次：M0/M1/M2 |
| `measure` *(PK)* | VARCHAR | 度量：amount(亿元)/yoy(同比%)/mom(环比%) |
| `value` | NUMERIC | 数值（amount 单位亿元，yoy/mom 单位 %） |

- **数据源**：akshare `macro_china_money_supply`
- **新鲜度**：最新 202605，✅ 新鲜
- **用途**：M1-M2 剪刀差（流动性收紧最早信号）。`剪刀差 = M2.yoy - M1.yoy`
- **⚠️ 注意**：
  - 长表存储，查询需 `WHERE aggregate='M1' AND measure='yoy'`
  - `date_column=None`，SMART 增量会全量回写（数据量小，可接受）
  - AlphaDB 原有 `tushare.macro_cn_m2` 仅含 M2 同比，本表为全口径增量，建议下游统一改用本表

### 5. `akshare.macro_cn_cb_balance` — 中国央行资产负债表（长表）

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 月份（月初日） |
| `item` *(PK)* | VARCHAR | 资产负债项目（中文，如"外汇""储备货币""政府存款"） |
| `value` | NUMERIC | 余额（亿元） |

- **数据源**：akshare `macro_china_central_bank_balance`
- **新鲜度**：最新 2026-05-01，✅ 新鲜
- **用途**：流动性根源（外汇占款=基础货币投放主渠道；储备货币；政府存款）
- **⚠️ 注意**：
  - 26 个项目列 melt 成长表，`item` 为中文项目名
  - 部分旧科目（如"对金融机构负债""准备金存款"）仅 1993 年有值，央行科目多次调整所致，非 bug
  - NULL 率 29% 主要来自早期缺值

### 6. `akshare.macro_cci` — 大宗商品指数

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `cci` | NUMERIC | 大宗商品指数（输入型通胀综合指标） |
| `change` | NUMERIC | 较上一观测日涨跌幅（%），非点位差 |

- **数据源**：项目内 `index_cci_cx` 扩展适配财新新版公开接口 `POST /dataindices/cci`（`month=""` 获取全历史）
- **新鲜度**：2026-09-05 smart 复跑核验，最新 2026-09-04，共 4,289 行
- **用途**：输入型通胀综合指标（单看铜/油不够，CCI 综合）。与 `future_daily`（单品种）互补

### 7. `akshare.macro_fixed_asset_investment` — 固定资产投资

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `period_end_date` *(PK)* | DATE | 统计期月末，**不是发布日期** |
| `period_label` | VARCHAR | 上游月份标签 |
| `monthly_value` | NUMERIC | 固定资产投资当月值（亿元） |
| `monthly_yoy` | NUMERIC | 固定资产投资当月同比（%） |
| `monthly_mom` | NUMERIC | 固定资产投资当月环比（%） |
| `cumulative_value` | NUMERIC | 自年初累计值（亿元） |
| `source_url` | TEXT | 东方财富公开宏观数据页 |

- **数据源**：AkShare `macro_china_gdzctz`（东方财富公开页）
- **覆盖与新鲜度（2026-09-04 核验）**：160 行，2012-02 至 2026-07
- **用途**：旧风格轮动策略的 `fixedasset_investment_yoy` 对应 `monthly_yoy`
- **⚠️ PIT**：源接口不提供可信发布日期；必须使用发布日历或保守固定滞后

### 8. `akshare.macro_industrial_value_added` — 规模以上工业增加值

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `period_end_date` *(PK)* | DATE | 统计期月末，**不是发布日期** |
| `period_label` | VARCHAR | 上游月份标签 |
| `monthly_yoy` | NUMERIC | 工业增加值当月同比（%） |
| `cumulative_yoy` | NUMERIC | 工业增加值累计同比（%） |
| `period_source_date` | DATE | 上游所谓“发布时间”，实为统计期月初，**明确不可作发布日期** |
| `source_url` | TEXT | 东方财富公开宏观数据页 |

- **数据源**：AkShare `macro_china_gyzjz`（东方财富公开页）
- **覆盖与新鲜度（2026-09-04 核验）**：204 行，2008-02 至 2026-07
- **用途**：旧风格轮动策略的 `industrial_value_added_yoy` 对应 `monthly_yoy`
- **⚠️ PIT**：`period_source_date` 不能用于可用日判断；必须使用发布日历或保守固定滞后

### 9. `akshare.macro_retail_sales` — 社会消费品零售总额

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `period_end_date` *(PK)* | DATE | 统计期月末，**不是发布日期** |
| `period_label` | VARCHAR | 上游月份标签 |
| `monthly_value` | NUMERIC | 社会消费品零售总额当月值（亿元） |
| `monthly_yoy` | NUMERIC | 社会消费品零售总额当月同比（%） |
| `monthly_mom` | NUMERIC | 社会消费品零售总额当月环比（%） |
| `cumulative_value` | NUMERIC | 累计值（亿元） |
| `cumulative_yoy` | NUMERIC | 累计同比（%） |
| `source_url` | TEXT | 东方财富公开宏观数据页 |

- **数据源**：AkShare `macro_china_consumer_goods_retail`（东方财富公开页）
- **覆盖与新鲜度（2026-09-04 核验）**：208 行，2008-01 至 2026-07
- **用途**：13 票风格轮动方案中的消费票使用 `monthly_yoy`；不要误接累计同比
- **⚠️ PIT**：源接口不提供可信发布日期；必须使用发布日历或保守固定滞后

### 10. `pbc.macro_mlt_loan` — 人民币中长期贷款月增量（住户 + 企业）

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `period_end_date` *(PK)* | DATE | 统计期月末，**不是可用日** |
| `release_date` | DATE | 央行金融统计数据报告的官方发布日期 |
| `release_time` | TIMESTAMP | 页面提供精确时间时保存；只有日期时为空 |
| `report_basis` | VARCHAR | 原报告口径：`monthly` / `ytd` |
| `household_mlt_reported_100m_cny` | NUMERIC | 原报告住户中长期贷款金额（亿元） |
| `enterprise_mlt_reported_100m_cny` | NUMERIC | 原报告非金融企业中长期贷款金额（亿元） |
| `household_mlt_monthly_100m_cny` | NUMERIC | 统一后的住户当月增量（亿元） |
| `enterprise_mlt_monthly_100m_cny` | NUMERIC | 统一后的企业当月增量（亿元） |
| `total_mlt_monthly_100m_cny` | NUMERIC | 两部分合计的当月增量（亿元） |
| `normalization_status` | VARCHAR | 直接月值、累计差分或缺少前值 |
| `source_title/source_url/source_hash` | TEXT | 官方页面与内容哈希 |

- **数据源**：中国人民银行《金融统计数据报告》正文及官方发布时间
- **更新方式**：任务 `pbc_macro_mlt_loan`，支持 GUI“全选 + 智能增量”，SMART 回看 120 天；为处理年内累计披露，会从回看起点所在年份 1 月重新计算。除已登记的历史结构性缺口和尚在发布宽限期内的月份外，任一统计期抓取失败都会整批失败关闭，避免空派生值覆盖旧值
- **用途**：旧策略 `long_loan_newadded` 应接 `total_mlt_monthly_100m_cny`，不是只接企业列。2012-11 官网样本 `1515 + (-31) = 1484`，2023-02 样本 `863 + 11100 = 11963`，均与旧 Wind 序列一致
- **生产覆盖（2026-09-04）**：185 行，2011-01 至 2026-07；2011-05 原页只有 PDF 附件，2022-04 官方改报住房/消费/经营贷款而无住户中长期分项，因此这两月不在官方结构化表中猜填；2026-08 当日尚未发布。旧策略所需的 2011-02 至 2020-12 信号值由上述 Excel 历史面板保留。
- **⚠️ PIT**：以 `release_time` 优先、`release_date` 次之决定可用时点；统计期月末不能代替发布日期。当前表是官方历史发布页快照，不是逐次修订 vintage 库

### 11. `nbs.macro_housing_newstarts` — 房屋新开工面积月同比

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `period_end_date` *(PK)* | DATE | 统计期月末；2 月表示 1—2 月合并桶 |
| `release_date` | DATE | 国家统计局官方发布日期 |
| `release_time` | TIMESTAMP | 页面提供精确时间时保存 |
| `cumulative_area_10k_sqm` | NUMERIC | 官方年内累计新开工面积（万平方米） |
| `cumulative_yoy_reported` | NUMERIC | 官方累计同比（%，可能使用修订后的可比基数） |
| `monthly_area_10k_sqm` | NUMERIC | 累计值差分；2 月为 1—2 月合并值 |
| `monthly_yoy_derived` | NUMERIC | 同月桶同比（%），旧策略所需口径 |
| `completed_cumulative_*` / `completed_monthly_*` | NUMERIC | 房屋竣工面积的官方累计值/同比与派生月值/月同比 |
| `sold_cumulative_*` / `sold_monthly_*` | NUMERIC | 商品房销售面积的官方累计值/同比与派生月值/月同比 |
| `monthly_bucket` | VARCHAR | `jan_feb_combined` / `calendar_month` |
| `derivation_status` | VARCHAR | 差分是否具备连续前值 |
| `calculation_version` | VARCHAR | 当前为 `nbs_housing_cumulative_difference_v2` |
| `source_title/source_url/source_hash` | TEXT | 官方页面与内容哈希 |

- **数据源**：国家统计局“全国房地产市场基本情况/房地产开发投资和销售情况”官方发布页
- **更新方式**：任务 `nbs_macro_housing_newstarts`，支持 GUI“全选 + 智能增量”，SMART 回看 120 天；自动读取当前发布列表并保留 2021 年官方历史页种子。已过发布宽限期的月份缺页、文章抓取失败或累计/月同比派生链断裂时整批失败关闭
- **用途**：旧策略 `newstarts_area_yoy` / `completed_yoy` / `sold_area_yoy` 分别接三组 `monthly_yoy_derived`；不要误接官方累计同比
- **⚠️ 1 月口径**：统计制度不单独发布 1 月，2 月按 1—2 月合并桶与上年同桶比较，不能伪造单月 1 月/2 月值
- **生产覆盖（2026-09-04）**：61 行，2021-02 至 2026-07；三类官方累计指标均无缺失，各有 50 个可计算的月同比。2011-02 至 2020-12 的旧策略信号已由 Excel 历史面板补齐，但仍不冒充官方发布日 vintage。
- **⚠️ PIT**：以官方 `release_time/release_date` 为可用时点；本表同时保留官方累计同比和策略派生同比，二者不可混用

---

## 二、美国宏观（5 表）

### 12. `akshare.macro_fed_decision` — 美联储利率决议

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | FOMC 决议日 |
| `rate` | NUMERIC | 美联储利率决议今值（%） |
| `rate_forecast` | NUMERIC | 市场预测值（%） |
| `rate_prev` | NUMERIC | 前值（%） |

- **数据源**：akshare `macro_bank_usa_interest_rate`
- **新鲜度**：⚠️ **严重滞后**，最新 2025-07-31（akshare 接口更新滞后近 1 年，2025-09 后 FOMC 会议为未发布 NaN 被丢弃）
- **用途**：与 `fred.macro_fed_rate` 互补——决议=政策动作点，有效利率=市场实际
- **⚠️ 不宜作实时信号**：数据源滞后，需用 `fred.macro_fed_rate.target_upper` 变化点替代实时观测

### 13. `akshare.macro_core_pce` — 美国核心 PCE

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | PCE 发布日 |
| `rate` | NUMERIC | 核心 PCE 年率今值（%，美联储通胀锚） |
| `rate_forecast` | NUMERIC | 市场预测值（%） |
| `rate_prev` | NUMERIC | 前值（%） |

- **数据源**：akshare `macro_usa_core_pce_price`
- **新鲜度**：⚠️ **滞后**，最新 2025-08-29（akshare 接口滞后）
- **用途**：美联储通胀目标锚定核心 PCE 2%（非 CPI），FOMC 决策真正依据
- **⚠️ 不宜作实时信号**：数据源滞后

### 14. `akshare.macro_usa_nonfarm` — 美国非农就业

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 非农发布日 |
| `rate` | NUMERIC | 非农就业新增今值（**万人**） |
| `rate_forecast` | NUMERIC | 市场预测值（万人） |
| `rate_prev` | NUMERIC | 前值（万人） |

- **数据源**：akshare `macro_usa_non_farm`
- **用途**：美联储双目标就业侧（新增流量，与失业率存量互补）
- **⚠️ 单位**：rate 单位为**万人**（非 %），易误用

### 15. `akshare.macro_usa_unemployment` — 美国失业率

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 失业率发布日 |
| `rate` | NUMERIC | 失业率今值（%） |
| `rate_forecast` | NUMERIC | 市场预测值（%） |
| `rate_prev` | NUMERIC | 前值（%） |

- **数据源**：akshare `macro_usa_unemployment_rate`
- **用途**：美联储双目标就业侧（存量比率，与非农互补）

### 16. `fred.macro_fed_balance` — 美联储资产负债表

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 发布日（周三） |
| `total_assets` | NUMERIC | 美联储总资产（**百万美元**，QT/QE 进程指标） |

- **数据源**：FRED `WALCL`（周频）
- **新鲜度**：最新 2026-06-17，✅ 新鲜
- **用途**：全球流动性总闸门。QT（缩表）= total_assets 下降
- **⚠️ 单位**：百万美元（非亿、非万亿美元）

---

## 三、全球流动性/风险（3 表）

### 17. `fred.macro_dxy` — 美元指数

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `dxy_close` | NUMERIC | 美元指数收盘 |

- **数据源**：FRED `DTWEXBGS` + Yahoo `DX-Y.NYB` fallback
- **新鲜度**：最新 2026-06-12，✅ 新鲜
- **用途**：`global_liquidity_metrics.usd_index`；global_score 外部因子
- **⚠️ 口径注意**：主源 `DTWEXBGS` 是**贸易加权广义美元指数（2006 基期=100）**，非 ICE DXY（6 发达货币篮子）。FRED 不可达时 fallback 至 Yahoo `DX-Y.NYB`（ICE DXY 本尊），**口径会切换**，下游需按 source 标注或降权

### 18. `fred.macro_vix` — VIX 波动率指数

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 美股交易日 |
| `vix_close` | NUMERIC | VIX 收盘（VIXCLS，仅收盘价） |

- **数据源**：FRED `VIXCLS` + Yahoo `^VIX` fallback
- **新鲜度**：最新 2026-06-17，✅ 新鲜
- **用途**：`market_regime_label` 分类辅助（risk_on/risk_off）
- **⚠️ 口径注意**：仅收盘价（无 OHLC）。fallback Yahoo ^VIX 口径一致无需降权

### 19. `fred.macro_credit_spread` — 美国信用利差

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `credit_spread` | NUMERIC | Baa-Aaa 企业债收益率利差（**pp**） |

- **数据源**：FRED `BAAFF`
- **新鲜度**：最新 2026-06-17，✅ 新鲜
- **用途**：信用风险先行指标。走阔=风险偏好下降/流动性紧张
- **⚠️ 单位**：pp（百分点），非 %

---

## 四、利率体系（8 表）

### 20. `fred.macro_fed_rate` — 美联储联邦基金利率

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `target_upper` | NUMERIC | 联邦基金目标利率上限（%） |
| `target_lower` | NUMERIC | 联邦基金目标利率下限（%） |
| `effective_rate` | NUMERIC | 有效联邦基金利率（%） |

- **数据源**：FRED `DFEDTARU/DFEDTARL/DFF`
- **新鲜度**：最新 2026-06-20，✅ 新鲜
- **用途**：`global_liquidity_metrics.fed_target_rate`；美联储政策姿态主指标
- **对账已验证**：effective_rate 全部落在 [target_lower, target_upper] 区间内 ✅

### 21. `fred.macro_sofr` — SOFR（担保隔夜融资利率）

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `sofr` | NUMERIC | 担保隔夜融资利率（%） |

- **数据源**：FRED `SOFR`
- **新鲜度**：最新 2026-06-17，✅ 新鲜
- **用途**：LIBOR 退出后的美元短期利率基准

### 22. `fred.macro_sofr_term` — SOFR 期限结构

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `sofr_30d` | NUMERIC | SOFR 30 天复合平均（%） |
| `sofr_90d` | NUMERIC | SOFR 90 天复合平均（%） |
| `sofr_180d` | NUMERIC | SOFR 180 天复合平均（%） |

- **数据源**：FRED `SOFR30/90/180DAYAVG`
- **用途**：SOFR 利率曲线期限结构

### 23. `fred.macro_us_short_rate` — 美元隔夜短期利率集合

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `iorb` | NUMERIC | 准备金利率（%） |
| `obfr` | NUMERIC | 隔夜银行融资利率（%，无担保口径） |
| `on_rrp` | NUMERIC | 隔夜逆回购利率（%，联储政策下限） |

- **数据源**：FRED `IORB/OBFR/RRPONTSYAWARD`
- **用途**：美元短端利率全貌；`iorb` 算 SOFR-IORB 利差替代 TED
- **⚠️ 高 NULL 率（53%，合理）**：三序列起点不一（on_rrp 2013、obfr 2016、iorb 2021），外连接后早期日部分列 NULL

### 24. `fred.macro_treasury_yield` — 美国国债关键期限收益率

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `yield_1m` | NUMERIC | 1 个月美国国债收益率（%） |
| `yield_3m` | NUMERIC | 3 个月美国国债收益率（%） |
| `yield_5y` | NUMERIC | 5 年美国国债收益率（%，FRED DGS5） |
| `yield_10y` | NUMERIC | 10 年美国国债收益率（%，FRED DGS10） |

- **数据源**：FRED `DGS1MO/DGS3MO/DGS5/DGS10`
- **覆盖与新鲜度（2026-09-04 核验）**：16,872 行，1962-01-02 至 2026-09-02
- **用途**：1M/3M 用于短端融资压力与 SOFR-3M 国债利差；5Y/10Y 为风格轮动提供官方 FRED 口径
- **⚠️ 起点差异**：5Y/10Y 自 1962 年起、3M 自 1981 年起、1M 自 2001 年起；早期短端为空是源序列边界，不是缺数故障
- **补充**：5Y/10Y 与 `macro_bond_rate` 的第三方镜像并存，下游应明确所选来源口径

### 25. `fred.macro_ted` — TED 利差（已停用）

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `ted_spread` | NUMERIC | TED 利差（pp，**序列已于 2022-01-21 停用**） |

- **数据源**：FRED `TEDRATE`（已停用）
- **新鲜度**：⚠️ **冻结于 2022-01-21**（LIBOR 退出导致序列停用）
- **用途**：仅历史参考，**不作实时信号**
- **实时替代**：(1) SOFR-IORB 利差 = `macro_sofr.sofr - macro_us_short_rate.iorb`；(2) SOFR-3M国债 利差 = `macro_sofr.sofr - macro_treasury_yield.yield_3m`

### 26-27. `akshare.macro_ecb_rate` / `akshare.macro_boj_rate` — 欧/日央行利率决议

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 决议日 |
| `rate` | NUMERIC | 政策利率今值（%） |
| `rate_forecast` | NUMERIC | 市场预测值（%） |
| `rate_prev` | NUMERIC | 前值（%） |

- **数据源**：akshare `macro_bank_euro_interest_rate` / `macro_bank_japan_interest_rate`
- **新鲜度**：⚠️ **严重滞后**（ECB 最新 2025-07-24、BOJ 最新 2025-07-31，akshare 接口滞后近 1 年）
- **用途**：全球流动性第二/第三引擎
- **⚠️ 不宜作实时信号**：数据源滞后

---

## 五、口径/代理偏差汇总（下游必读）

| 字段 | 偏差类型 | 说明 | 处理建议 |
|---|---|---|---|
| `macro_dxy.dxy_close` | 代理+口径切换 | 主源 DTWEXBGS（广义，2006基期）；fallback DX-Y.NYB（ICE DXY） | 标注口径；fallback 触发时降权 |
| `macro_repo_rate.fr007` | 代理 | FR007 定盘（报价）≠ DR007（成交） | evidence confidence 降权 |
| `macro_vix.vix_close` | 口径裁剪 | 仅收盘价（无 OHLC） | market_regime 仅需收盘，可接受 |
| `macro_policy_rate.mlf_1y` | 缺失 | 恒 NULL（无数据源） | 用 LPR 近似 policy_rate |
| `macro_fed_decision/core_pce/ecb_rate/boj_rate` | 数据源滞后 | akshare 事件类接口滞后近 1 年 | 不作实时信号，用 FRED 日频表替代 |
| `macro_ted.ted_spread` | 停用 | 冻结 2022-01-21 | 用 SOFR-IORB 或 SOFR-国债利差替代 |
| `macro_mlt_loan.total_mlt_monthly_100m_cny` | 名称易误解 | 旧 Wind 序列实际为住户+企业中长期贷款，不是企业单项 | 使用合计列复现旧策略，单项研究再用 enterprise 列 |
| `macro_housing_newstarts.monthly_yoy_derived` | 派生口径 | 官方只报累计值/累计同比；月同比由累计值差分后计算 | 与官方累计同比分列，并保留 calculation_version |

## 六、单位易错点汇总

| 字段 | 单位 | 易错为 |
|---|---|---|
| `macro_usa_nonfarm.rate` | **万人** | % |
| `macro_fed_balance.total_assets` | **百万美元** | 亿/万亿 |
| `macro_credit_spread.credit_spread` | **pp** | % |
| `macro_ted.ted_spread` | **pp** | % |
| `macro_money_supply.value` (measure=amount) | **亿元** | — |
| `macro_cn_cb_balance.value` | **亿元** | — |
| `macro_mlt_loan.*_100m_cny` | **亿元人民币** | 万亿元 |
| `macro_housing_newstarts.*_10k_sqm` | **万平方米** | 亿平方米 |

## 七、推荐用法速查

| 需求 | 推荐表/计算 |
|---|---|
| 美实际利率 | `macro_bond_rate(US,10y) - macro_usa_cpi.cpi_yoy` |
| 美联储政策姿态(实时) | `macro_fed_rate.target_upper`（**非** fed_decision，后者滞后） |
| 政策利率变动 | `macro_policy_rate.lpr_1y`（中国）；`macro_fed_rate.target_upper`（美国） |
| M1-M2 剪刀差 | `macro_money_supply` 中 `M2.yoy - M1.yoy` |
| 国内流动性 | `macro_repo_rate.fr007`（DR007 代理，降权）+ `macro_cn_cb_balance`（外汇占款） |
| 全球流动性 | `macro_fed_balance.total_assets`（QT/QE）+ `macro_fed_rate` |
| 信用风险 | `macro_credit_spread.credit_spread`（走阔=紧张） |
| TED 实时替代 | `macro_sofr.sofr - macro_us_short_rate.iorb` |
| 风险偏好 | `macro_vix.vix_close` + `macro_credit_spread` |
| 输入型通胀 | `macro_cci.cci` |
| 旧策略中长期贷款票 | `macro_mlt_loan.total_mlt_monthly_100m_cny`，按官方发布时间做 PIT |
| 旧策略房地产三票 | `macro_housing_newstarts` 的新开工/竣工/销售 `monthly_yoy_derived`，2 月使用 1—2 月合并桶 |
| 旧策略 40 指标历史复现 | `macro_style_rotation_legacy`；只用 `strategy_available_date_proxy` 复现旧的两月滞后，不将其写成真实发布日 |

## 八、新鲜度审计记录

- **2026-09-04 定向复核**：fixed_asset_investment / industrial_value_added / retail_sales 均更新至 2026-07；treasury_yield 更新至 2026-09-02。四张表与当时源端返回逐字段一致。
- **2026-09-04 新任务生产入库验收**：`macro_style_rotation_legacy` 4,760 行（40 指标 × 119 月）、`macro_mlt_loan` 185 行、`macro_housing_newstarts` 61 行；三张 `rawdata` 视图行数一致，无重复主键、无“发布日早于统计期”记录。2025-03 中长期贷款月增量还原为 20,847 亿元；2026-06/07 房屋新开工面积月同比还原为 -26.044568% / -28.521272%。
- **以下为 2026-06-21 基线记录，未在 2026-09-04 全量重审：**

- ✅ **新鲜**：repo_rate、policy_rate、money_supply、cn_cb_balance、cci、dxy、fed_rate、vix、sofr、sofr_term、us_short_rate、treasury_yield、fed_balance、credit_spread、usa_cpi
- ⚠️ **数据源滞后（akshare 事件类，非采集问题）**：fed_decision（325天）、ecb_rate（332天）、boj_rate（325天）、core_pce
- ⚠️ **已停用**：ted（冻结 2022-01-21）
- ℹ️ FRED 日频表轻微滞后 3-9 天多为周末/节假日，非问题

## 九、已验证的数据质量（对账结论）

- ✅ `fed_decision.rate_prev` == 上一期 `rate`（全部一致）
- ✅ `fed_rate` 无 target_lower > target_upper，effective_rate 全落区间内
- ✅ `money_supply` M2 > M1 > M0 层级嵌套全部成立
- ✅ SOFR vs IORB 利差 -0.05~+0.04pp（符合银行准备金利率附近拆借）
- ✅ 无采集/转换 bug（前序问题已修：商品列过滤、日期解析、melt value_vars）
- ✅ 中长期贷款 2012-11 与 2023-02 样本分别还原为 1,484 / 11,963 亿元，与旧 Wind 序列逐值一致
- ✅ 房屋新开工、竣工和商品房销售面积均保留“官方累计同比”和“策略月增量同比”；2 月合并桶单独标识
- ✅ 旧工作簿的 40 指标合同严格对账；消费指标仅接受 `tot_retail_sales_yoy`，旧误别名 `retail_sales_yoy` 不入库

## 十、暂缓项/待补齐

| 缺口 | 原因 | 影响 |
|---|---|---|
| MLF | akshare 无接口，CFETS/Wind 需授权 | `macro_policy_rate.mlf_1y` 恒 NULL，用 LPR 近似 |
| 工业产能利用率 | akshare 无 turnkey 函数 | P0 `industry_capacity_utilization` = null |
| 分行业 capex | akshare 仅全国 FAI 总量 | P0 `industry_capex_growth_yoy` = null |
| 房屋新开工 2011-02 至 2020-12 官方页索引 | 国家统计局当前静态发布列表不再覆盖该区间；数值可见但仍需逐月绑定原始发布日期与页面 | 旧策略数值已由 Excel 补齐；官方任务从 2021-02 持续维护，历史段仍不可冒充真实发布日 vintage |
