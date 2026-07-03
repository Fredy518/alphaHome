# AlphaDB 宏观数据补齐状态汇总
文件名：macro_gap_fill_status.md
创建于：2026-06-21
创建者：ZCode
关联：CrossLens SPEC-015（Macro/Meso 域实现规格）

## 概述
基于 2026-06-20 对 AlphaDB live schema 实测 + akshare 1.18.64 运行时验证 + FRED keyless 端点验证 + Yahoo v8 验证，本汇总记录宏观数据缺口补齐的最终落地状态：22 项已实现（第一批宏观缺口 9 + SOFR 体系 3 + 第二批宏观/流动性 10），3 项暂缓，并标注所有代理/口径偏差供 SPEC-015 下游引用。

## 一、已实现（22 项）

### 第一批：原始宏观缺口（9 项）

| # | 缺口 | 任务名 | 数据源 | 目标表 | 物理表(schema.table) | SPEC-015 用途 |
|---|---|---|---|---|---|---|
| 1 | 美国CPI同比 | `akshare_macro_usa_cpi` | akshare `macro_usa_cpi_yoy` | `macro_usa_cpi` | `akshare.macro_usa_cpi` | us_real_yield_10y → P0 `us_real_yield_10y_change_6m` |
| 2 | LPR/基准贷款利率 | `akshare_macro_lpr` | akshare `macro_china_lpr` | `macro_policy_rate` | `akshare.macro_policy_rate` | policy_rate → P0 `policy_rate_change_6m` |
| 5 | 美元指数 | `fred_macro_dxy` | FRED `DTWEXBGS` + Yahoo `DX-Y.NYB` fallback | `macro_dxy` | `fred.macro_dxy` | global_score usd_index |
| 6 | 美联储联邦基金利率(有效) | `fred_macro_fed_rate` | FRED `DFEDTARU/DFEDTARL/DFF` | `macro_fed_rate` | `fred.macro_fed_rate` | global_score fed_target_rate |
| 6b | 美联储利率决议 | `akshare_macro_usa_fed_decision` | akshare `macro_bank_usa_interest_rate` | `macro_fed_decision` | `akshare.macro_fed_decision` | 与 fed_rate 互补：决议=政策动作点 |
| 7 | DR007代理 | `akshare_macro_repo_rate` | akshare `repo_rate_query` | `macro_repo_rate` | `akshare.macro_repo_rate` | liquidity_metrics dr007（代理） |
| 8 | VIX | `fred_macro_vix` | FRED `VIXCLS` + Yahoo `^VIX` fallback | `macro_vix` | `fred.macro_vix` | market_regime 辅助 |
| 9 | TED利差(历史) | `fred_macro_ted` | FRED `TEDRATE`(停用) | `macro_ted` | `fred.macro_ted` | 可选 global_score（历史，2022-01 冻结） |
| 10 | SOFR(隔夜) | `fred_macro_sofr` | FRED `SOFR` | `macro_sofr` | `fred.macro_sofr` | 全球短期利率基准 |
| 10b | SOFR期限结构 | `fred_macro_sofr_term` | FRED `SOFR30/90/180DAYAVG` | `macro_sofr_term` | `fred.macro_sofr_term` | SOFR 利率曲线期限结构 |
| 10c | 美元隔夜短利率 | `fred_macro_us_short_rate` | FRED `IORB/OBFR/RRPONTSYAWARD` | `macro_us_short_rate` | `fred.macro_us_short_rate` | 美元短端利率全貌；IORB 算 SOFR-IORB 利差替代 TED |
| 10d | 美国短端国债 | `fred_macro_treasury_yield` | FRED `DGS1MO/DGS3MO` | `macro_treasury_yield` | `fred.macro_treasury_yield` | 算 SOFR-3M国债 利差替代 TED |

### 第三批：宏观/流动性补强（10 项）

| # | 缺口 | 任务名 | 数据源 | 目标表 | 物理表 | SPEC-015 用途 |
|---|---|---|---|---|---|---|
| P0 | 中国货币供应(M0/M1/M2) | `akshare_macro_money_supply` | akshare `macro_china_money_supply` | `macro_money_supply` | `akshare.macro_money_supply` | M1-M2 剪刀差（流动性最早信号） |
| P0 | 美联储资产负债表 | `fred_macro_fed_balance` | FRED `WALCL` | `macro_fed_balance` | `fred.macro_fed_balance` | 全球流动性总闸门（QT/QE） |
| P0 | 美国核心PCE | `akshare_macro_core_pce` | akshare `macro_usa_core_pce_price` | `macro_core_pce` | `akshare.macro_core_pce` | 美联储通胀锚（比 CPI 关键） |
| P1 | 中国央行资产负债表 | `akshare_macro_cn_cb_balance` | akshare `macro_china_central_bank_balance` | `macro_cn_cb_balance` | `akshare.macro_cn_cb_balance` | 流动性根源（外汇占款+储备货币） |
| P1 | 美国非农就业 | `akshare_macro_usa_nonfarm` | akshare `macro_usa_non_farm` | `macro_usa_nonfarm` | `akshare.macro_usa_nonfarm` | 美联储双目标就业侧 |
| P1 | 美国失业率 | `akshare_macro_usa_unemployment` | akshare `macro_usa_unemployment_rate` | `macro_usa_unemployment` | `akshare.macro_usa_unemployment` | 美联储双目标就业侧（存量比率） |
| P1 | 美国信用利差 | `fred_macro_credit_spread` | FRED `BAAFF` | `macro_credit_spread` | `fred.macro_credit_spread` | 信用风险先行指标 |
| P1 | 欧央行利率决议 | `akshare_macro_ecb_rate` | akshare `macro_bank_euro_interest_rate` | `macro_ecb_rate` | `akshare.macro_ecb_rate` | 全球流动性第二引擎 |
| P1 | 日央行利率决议 | `akshare_macro_boj_rate` | akshare `macro_bank_japan_interest_rate` | `macro_boj_rate` | `akshare.macro_boj_rate` | 全球流动性第三引擎（日元套息） |
| P2 | 大宗商品指数(CCI) | `akshare_macro_cci` | akshare `index_cci_cx` | `macro_cci` | `akshare.macro_cci` | 输入型通胀综合指标 |

所有任务经 `@task_register()` 自动注册，由 `discover_tasks()` 自动发现，可在 GUI 与生产 CLI runner 中运行。物理表落 `akshare.*` / `fred.*` schema，`rawdata.*` 视图自动创建。

## 二、暂缓（3 项，无免费程序化数据源）

| # | 缺口 | 暂缓原因 | SPEC-015 影响 | 后续建议 |
|---|---|---|---|---|
| 3 | 分行业产能利用率 | akshare 1.18.64 无对应函数（运行时+源码grep确认）；仅 `macro_china_nbs_nation` 通用接口需逆向统计局指标代码，非 turnkey | P0 `industry_capacity_utilization` = null；industry_cycle_stage 降级 | 接入 Wind/Choice（若有授权）或逆向 NBS 指标代码 |
| 4 | 分行业capex | akshare 仅 `macro_china_gdzctz`（全国城镇FAI总量，非分行业）；无分行业 turnkey 函数 | P0 `industry_capex_growth_yoy` = null；capex_cycle_stage 降级 | 用 `index_swdaily` 申万行业 pe/close 作景气代理（需标注），或接入统计局分行业FAI |
| 2b | MLF（中期借贷便利） | akshare 1.18.64 无对应接口（源码grep `mlf`/`中期借贷` 零命中确认）；CFETS/Wind 需授权 | `macro_policy_rate.mlf_1y` 列已预留（恒 NULL），不阻塞 policy_rate_change_6m（可用 LPR） | 接入 CFETS 或 Wind |

## 三、代理/口径偏差（下游使用须知）

SPEC-015 下游计算 evidence/metric 时，以下偏差需在 confidence 中降权或标注：

| 字段 | 实际数据 | SPEC-015 目标 | 偏差说明 | 建议处理 |
|---|---|---|---|---|
| `macro_dxy.dxy_close` | 主源 FRED DTWEXBGS（贸易加权广义，2006基期=100）；FRED不可达时 fallback Yahoo DX-Y.NYB（ICE DXY，6发达货币篮子） | ICE DXY | 主源与 fallback 口径、数值、基期均不同 | 标注口径；fallback 触发时数据口径切换，需按 source 标注或降权 |
| `macro_repo_rate.fr007` | akshare FR007 定盘利率（报价撮合） | DR007（存款类机构成交加权利率） | 报价 vs 成交，走势相关但口径不同 | evidence confidence 降权 |
| `macro_vix.vix_close` | 主源 FRED VIXCLS（仅收盘价）；FRED不可达时 fallback Yahoo ^VIX（同为 CBOE VIX 收盘，口径一致） | CBOE VIX（含 OHLC） | 无 OHLC | market_regime 仅需收盘，可接受；fallback 口径一致无需降权 |
| `macro_policy_rate.mlf_1y` | NULL（预留列） | MLF 利率 | 数据源暂不可得 | 用 LPR 近似 policy_rate，或后续接源 |
| `macro_fed_decision` | akshare 美联储利率决议（事件频，今值/预测值/前值） | — | 与 `macro_fed_rate`（日频有效利率）口径互补 | 决议=政策动作点，有效利率=市场实际，下游按需选用 |
| `macro_ted.ted_spread` | FRED TEDRATE（冻结于 2022-01-21） | 实时 TED 利差 | 序列已停用，不更新 | 不作实时信号，仅历史参考。实时替代已有两个：(1) SOFR-IORB 利差（macro_sofr - macro_us_short_rate.iorb）；(2) SOFR-3M国债 利差（macro_sofr - macro_treasury_yield.yield_3m） |

## 四、P0 metric 落地影响

| P0 metric | 是否可算 | 依赖表 |
|---|---|---|
| `real_yield_change_6m` | ✅ | macro_bond_rate(CN) + macro_cpi(CN)（均已有） |
| `us_real_yield_10y_change_6m` | ✅（本次补齐） | macro_bond_rate(US) + **macro_usa_cpi(本次)** |
| `policy_rate_change_6m` | ✅（本次补齐） | **macro_policy_rate(本次 LPR)** |
| `cn_us_yield_spread_10y` | ✅ | macro_bond_rate(CN/US)（均已有） |
| `commodity_input_cost_change` | ✅ | future_daily（已有） |
| `industry_capacity_utilization` | ❌（暂缓#3） | 需产能利用率数据源 |
| `industry_capex_growth_yoy` | ❌（暂缓#4） | 需分行业 capex 数据源 |

**结论**：本次补齐后，7 个 P0 metric 中 5 个可落地（较补齐前 +2）。剩余 2 个（产能/capex）受限于无免费数据源，需后续接入授权源或代理。

## 五、新增源基础设施

为支持 FRED 系列任务，新建 `alphahome/fetchers/sources/fred/` 源模块（无新依赖，仅用 requests + pandas）：
- `fred_api.py`：`FredAPI` 类，封装 keyless fredgraph.csv 端点，异步化（`asyncio.to_thread`）、重试、限流。`. `→NaN。
- `fred_task.py`：`FredTask(FetcherTask)` 声明式基类，`data_source="fred"`，子类声明 `series_ids`+`column_mapping`+`schema_def`。`fetch_batch` 多序列按 `observation_date` 外连接合并（缺数据序列自动补 NaN 列保证 schema 一致）；`process_data` 类型转换+日期规整+生效窗口过滤+去重。支持 `yahoo_fallback` 声明：某 FRED 序列失败/无数据时自动转 Yahoo v8 取收盘。
- `__init__.py`：导出 `FredAPI/FredAPIError/FredTask`。

为支持 DXY/VIX 在 FRED 不可达环境（如沙箱网络策略屏蔽 FRED）下的数据可用性，新建 `alphahome/fetchers/sources/yahoo/` 源模块：
- `yahoo_api.py`：`YahooAPI` 类，直连 Yahoo v8 chart API（绕过 yfinance 库的 429 限流），返回 `observation_date + close` 两列（对齐 FredAPI 形态）。需 User-Agent 头，无需 key。
- 由 `FredTask.yahoo_fallback` 映射驱动：DXY(`DTWEXBGS→DX-Y.NYB`)、VIX(`VIXCLS→^VIX`)。

为消除事件类宏观任务的重复代码（美联储/欧央行/日央行决议、非农、失业率、核心PCE 输出同构 `商品/日期/今值/预测值/前值`），新建事件类基类：
- `alphahome/fetchers/sources/akshare/akshare_macro_event_task.py`：`AkShareMacroEventTask(AkShareNoDateSingleBatchTask)`，封装共享 `process_data`（仅保留 schema_def 列、规整日期、丢弃未发布 NaN 行、去重）。6 个事件类任务继承它（含重构的 fed_decision）。

未改动 `config_manager.py`（FRED keyless、Yahoo keyless，均无需 secret）；未改动 `pyproject.toml`（requests 已传递安装）；未触碰 yieldcurve 归档相关文件。

## 六、验证
- 单元测试：`tests/unit/test_akshare_macro_tasks.py`（22 任务）+ `tests/unit/test_fred_macro_tasks.py`（含 Yahoo fallback + SOFR 体系 + 第二批 FRED）共 47 项全绿；`tests/unit/` 全量 396 项全绿。
- 22 任务均经 `discover_tasks()` 确认注册成功。
- 真实数据路径抽查：FRED fed_rate/fed_balance/credit_spread、akshare usa_cpi/fed_decision/money_supply/cn_cb_balance 实测通过；Yahoo v8 直连 ^VIX/DX-Y.NYB 实测 HTTP 200。
