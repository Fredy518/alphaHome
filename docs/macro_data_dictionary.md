# AlphaDB 宏观数据字典
文件名：macro_data_dictionary.md  
创建于：2026-06-21  
创建者：ZCode  
用途：供 CrossLens SPEC-015 及下游消费方参考的宏观数据表字段、口径、代理标注、新鲜度与推荐用法说明

## 概述
本文档覆盖 22 张宏观数据表（物理表位于 `akshare.*` / `fred.*` schema，`rawdata.*` 为自动管理的同名视图）。所有表经 `@task_register()` 注册，可通过 GUI 或生产 SMART runner 增量更新。

**通用约定：**
- 所有数值列单位见各表"口径"列（% / pp / 亿元 / 百万美元）
- `update_time` 为行写入时间戳（TIMESTAMP，自动维护）
- 主键（PK）驱动 UPSERT，重跑幂等无重复
- `rawdata.<table>` 视图 = `SELECT * FROM <schema>.<table>`，下游查询可用任一

---

## 一、中国宏观/流动性（6 表）

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
| `change` | NUMERIC | 当日变化值 |

- **数据源**：akshare `index_cci_cx`
- **新鲜度**：最新 2026-06-18，✅ 新鲜
- **用途**：输入型通胀综合指标（单看铜/油不够，CCI 综合）。与 `future_daily`（单品种）互补

---

## 二、美国宏观（5 表）

### 7. `akshare.macro_fed_decision` — 美联储利率决议

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

### 8. `akshare.macro_core_pce` — 美国核心 PCE

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

### 9. `akshare.macro_usa_nonfarm` — 美国非农就业

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 非农发布日 |
| `rate` | NUMERIC | 非农就业新增今值（**万人**） |
| `rate_forecast` | NUMERIC | 市场预测值（万人） |
| `rate_prev` | NUMERIC | 前值（万人） |

- **数据源**：akshare `macro_usa_non_farm`
- **用途**：美联储双目标就业侧（新增流量，与失业率存量互补）
- **⚠️ 单位**：rate 单位为**万人**（非 %），易误用

### 10. `akshare.macro_usa_unemployment` — 美国失业率

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 失业率发布日 |
| `rate` | NUMERIC | 失业率今值（%） |
| `rate_forecast` | NUMERIC | 市场预测值（%） |
| `rate_prev` | NUMERIC | 前值（%） |

- **数据源**：akshare `macro_usa_unemployment_rate`
- **用途**：美联储双目标就业侧（存量比率，与非农互补）

### 11. `fred.macro_fed_balance` — 美联储资产负债表

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

### 12. `fred.macro_dxy` — 美元指数

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `dxy_close` | NUMERIC | 美元指数收盘 |

- **数据源**：FRED `DTWEXBGS` + Yahoo `DX-Y.NYB` fallback
- **新鲜度**：最新 2026-06-12，✅ 新鲜
- **用途**：`global_liquidity_metrics.usd_index`；global_score 外部因子
- **⚠️ 口径注意**：主源 `DTWEXBGS` 是**贸易加权广义美元指数（2006 基期=100）**，非 ICE DXY（6 发达货币篮子）。FRED 不可达时 fallback 至 Yahoo `DX-Y.NYB`（ICE DXY 本尊），**口径会切换**，下游需按 source 标注或降权

### 13. `fred.macro_vix` — VIX 波动率指数

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 美股交易日 |
| `vix_close` | NUMERIC | VIX 收盘（VIXCLS，仅收盘价） |

- **数据源**：FRED `VIXCLS` + Yahoo `^VIX` fallback
- **新鲜度**：最新 2026-06-17，✅ 新鲜
- **用途**：`market_regime_label` 分类辅助（risk_on/risk_off）
- **⚠️ 口径注意**：仅收盘价（无 OHLC）。fallback Yahoo ^VIX 口径一致无需降权

### 14. `fred.macro_credit_spread` — 美国信用利差

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

### 15. `fred.macro_fed_rate` — 美联储联邦基金利率

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

### 16. `fred.macro_sofr` — SOFR（担保隔夜融资利率）

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `sofr` | NUMERIC | 担保隔夜融资利率（%） |

- **数据源**：FRED `SOFR`
- **新鲜度**：最新 2026-06-17，✅ 新鲜
- **用途**：LIBOR 退出后的美元短期利率基准

### 17. `fred.macro_sofr_term` — SOFR 期限结构

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `sofr_30d` | NUMERIC | SOFR 30 天复合平均（%） |
| `sofr_90d` | NUMERIC | SOFR 90 天复合平均（%） |
| `sofr_180d` | NUMERIC | SOFR 180 天复合平均（%） |

- **数据源**：FRED `SOFR30/90/180DAYAVG`
- **用途**：SOFR 利率曲线期限结构

### 18. `fred.macro_us_short_rate` — 美元隔夜短期利率集合

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `iorb` | NUMERIC | 准备金利率（%） |
| `obfr` | NUMERIC | 隔夜银行融资利率（%，无担保口径） |
| `on_rrp` | NUMERIC | 隔夜逆回购利率（%，联储政策下限） |

- **数据源**：FRED `IORB/OBFR/RRPONTSYAWARD`
- **用途**：美元短端利率全貌；`iorb` 算 SOFR-IORB 利差替代 TED
- **⚠️ 高 NULL 率（53%，合理）**：三序列起点不一（on_rrp 2013、obfr 2016、iorb 2021），外连接后早期日部分列 NULL

### 19. `fred.macro_treasury_yield` — 美国短端国债收益率

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `yield_1m` | NUMERIC | 1 个月美国国债收益率（%） |
| `yield_3m` | NUMERIC | 3 个月美国国债收益率（%） |

- **数据源**：FRED `DGS1MO/DGS3MO`
- **用途**：算 SOFR-3M国债 利差替代 TED
- **⚠️ 高 NULL 率（47%，合理）**：yield_1m 2001 起、yield_3m 1981 起，起点不一
- **补充**：AlphaDB 原有 `macro_bond_rate` 含 2y/5y/10y/30y，本表补 1M/3M 短端

### 20. `fred.macro_ted` — TED 利差（已停用）

| 字段 | 类型 | 口径/说明 |
|---|---|---|
| `date` *(PK)* | DATE | 交易日 |
| `ted_spread` | NUMERIC | TED 利差（pp，**序列已于 2022-01-21 停用**） |

- **数据源**：FRED `TEDRATE`（已停用）
- **新鲜度**：⚠️ **冻结于 2022-01-21**（LIBOR 退出导致序列停用）
- **用途**：仅历史参考，**不作实时信号**
- **实时替代**：(1) SOFR-IORB 利差 = `macro_sofr.sofr - macro_us_short_rate.iorb`；(2) SOFR-3M国债 利差 = `macro_sofr.sofr - macro_treasury_yield.yield_3m`

### 21-22. `akshare.macro_ecb_rate` / `akshare.macro_boj_rate` — 欧/日央行利率决议

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

## 六、单位易错点汇总

| 字段 | 单位 | 易错为 |
|---|---|---|
| `macro_usa_nonfarm.rate` | **万人** | % |
| `macro_fed_balance.total_assets` | **百万美元** | 亿/万亿 |
| `macro_credit_spread.credit_spread` | **pp** | % |
| `macro_ted.ted_spread` | **pp** | % |
| `macro_money_supply.value` (measure=amount) | **亿元** | — |
| `macro_cn_cb_balance.value` | **亿元** | — |

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

## 八、新鲜度审计结果（2026-06-21）

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

## 十、暂缓项（无免费数据源）

| 缺口 | 原因 | 影响 |
|---|---|---|
| MLF | akshare 无接口，CFETS/Wind 需授权 | `macro_policy_rate.mlf_1y` 恒 NULL，用 LPR 近似 |
| 工业产能利用率 | akshare 无 turnkey 函数 | P0 `industry_capacity_utilization` = null |
| 分行业 capex | akshare 仅全国 FAI 总量 | P0 `industry_capex_growth_yoy` = null |
