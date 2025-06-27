# TushareDB 使用文档

## 引言

`tusharedb` 是 `alphahome` 项目用于存储从 [Tushare Pro API](https://tushare.pro/) 获取的金融数据的 PostgreSQL 数据库。本文档旨在说明数据库的结构和内容。

**注意:** 本文档基于对 `alphahome` 项目代码的分析生成。实际数据库结构可能因迁移或手动更改而略有不同。

## 数据库概览

*   **数据来源:** Tushare Pro API
*   **更新方式:** 主要通过 `alphahome/fetchers/tasks/` 下定义的 `TushareTask` 子类进行更新。根据任务配置，可以是增量更新（基于日期）或全量替换。
*   **核心表:** 存储股票、基金、指数、财务等各类金融时间序列和基础信息数据。

## 表结构详情

### 1. `tushare_stock_daily`

*   **描述:** 存储A股股票的每日交易数据，包括开盘价、收盘价、最高价、最低价、成交量、成交额等。通过增量方式更新。
*   **Tushare API:** `daily`
*   **列信息:**

| 列名        | 数据类型        | 约束       | 注释                                   |
|-------------|-----------------|------------|----------------------------------------|
| `ts_code`   | VARCHAR(10)     | NOT NULL   | Tushare股票代码 (例如: 600000.SH)      |
| `trade_date`| DATE            | NOT NULL   | 交易日期 (YYYY-MM-DD)                  |
| `open`      | NUMERIC(10,4)   |            | 开盘价                                 |
| `high`      | NUMERIC(10,4)   |            | 最高价                                 |
| `low`       | NUMERIC(10,4)   |            | 最低价                                 |
| `close`     | NUMERIC(10,4)   |            | 收盘价                                 |
| `pre_close` | NUMERIC(10,4)   |            | 昨收盘价                               |
| `change`    | NUMERIC(10,4)   |            | 涨跌额                                 |
| `pct_chg`   | NUMERIC(10,4)   |            | 涨跌幅 (%)                             |
| `volume`    | NUMERIC(20,4)   |            | 成交量 (手) (源自 Tushare `vol` 字段)  |
| `amount`    | NUMERIC(20,4)   |            | 成交额 (千元)                          |

### 2. `tushare_stock_basic`

*   **描述:** 存储A股、港股、美股等上市公司的基础信息，如名称、地域、行业、上市状态、上市日期等。通过全量替换方式更新。
*   **Tushare API:** `stock_basic`
*   **列信息:**

| 列名           | 数据类型        | 约束       | 注释                                                       |
|----------------|-----------------|------------|------------------------------------------------------------|
| `ts_code`      | VARCHAR(15)     | NOT NULL   | Tushare股票/证券代码 (例如: 600000.SH 或 000001.SZ)      |
| `symbol`       | VARCHAR(10)     |            | 股票代码 (例如: 600000)                                  |
| `name`         | VARCHAR(50)     |            | 股票名称 (例如: 浦发银行)                                |
| `area`         | VARCHAR(50)     |            | 地域 (例如: 上海)                                          |
| `industry`     | VARCHAR(100)    |            | 所属行业 (例如: 银行)                                      |
| `fullname`     | VARCHAR(255)    |            | 股票全称                                                   |
| `enname`       | VARCHAR(255)    |            | 英文全称                                                   |
| `cnspell`      | VARCHAR(50)     |            | 拼音缩写                                                   |
| `market`       | VARCHAR(50)     |            | 市场类型 (例如: 主板, 科创板, 创业板, 北交所)                |
| `exchange`     | VARCHAR(10)     |            | 交易所代码 (例如: SSE, SZSE, BSE, HKEX, NYSE, NASDAQ)       |
| `curr_type`    | VARCHAR(5)      |            | 交易货币 (例如: CNY, HKD, USD)                             |
| `list_status`  | VARCHAR(1)      |            | 上市状态 (L上市 D退市 P暂停上市)                           |
| `list_date`    | DATE            |            | 上市日期 (YYYY-MM-DD)                                      |
| `delist_date`  | DATE            |            | 退市日期 (YYYY-MM-DD)，可能为 NULL                         |
| `is_hs`        | VARCHAR(1)      |            | 是否沪深港通标的 (N否 H沪股通 S深股通)                     |
| `act_name`     | VARCHAR(100)    |            | 实际控制人名称                                             |
| `act_ent_type` | VARCHAR(100)    |            | 实际控制人企业性质                                         |

### 3. `tushare_stock_report_rc`

*   **描述:** 存储卖方（券商）发布的盈利预测数据。
*   **Tushare API:** `report_rc`
*   **列信息:**

| 列名           | 数据类型        | 约束       | 注释                                     |
|----------------|-----------------|------------|------------------------------------------|
| `ts_code`      | VARCHAR(10)     | NOT NULL   | Tushare股票代码                          |
| `report_date`  | DATE            | NOT NULL   | 报告日期                                 |
| `org_name`     | VARCHAR(100)    | NOT NULL   | 机构名称 (空值填充为'无')                |
| `author_name`  | VARCHAR(255)    | NOT NULL   | 分析师名称 (空值填充为'无')              |
| `quarter`      | VARCHAR(10)     | NOT NULL   | 报告对应的季度 (例如 '2023Q1')           |
| `name`         | VARCHAR(50)     |            | 股票名称                                 |
| `report_title` | TEXT            |            | 报告标题                                 |
| `report_type`  | VARCHAR(50)     |            | 报告类型                                 |
| `classify`     | VARCHAR(50)     |            | 报告分类                                 |
| `op_rt`        | NUMERIC(20,4)   |            | 营业收入同比增长率(%)                    |
| `op_pr`        | NUMERIC(20,4)   |            | 营业利润同比增长率(%)                    |
| `tp`           | NUMERIC(20,4)   |            | 目标价格                                 |
| `np`           | NUMERIC(20,4)   |            | 净利润(万元)                             |
| `eps`          | NUMERIC(20,4)   |            | 每股收益(元) - 预测值                    |
| `pe`           | NUMERIC(20,4)   |            | 市盈率(倍) - 预测值                      |
| `rd`           | NUMERIC(20,4)   |            | 研发支出(万元)                           |
| `roe`          | NUMERIC(20,4)   |            | 净资产收益率(%)                          |
| `ev_ebitda`    | NUMERIC(20,4)   |            | EV/EBITDA(倍)                           |
| `rating`       | VARCHAR(50)     |            | 投资评级                                 |
| `max_price`    | NUMERIC(20,4)   |            | 最高目标价                               |
| `min_price`    | NUMERIC(20,4)   |            | 最低目标价                               |
| `imp_dg`       | VARCHAR(50)     |            | 隐含评级变动                             |
| `create_time`  | TIMESTAMP       |            | Tushare记录创建时间                      |

### 4. `tushare_stock_adjfactor`

*   **描述:** 存储股票的复权因子，用于计算前复权和后复权价格。
*   **Tushare API:** `adj_factor`
*   **列信息:**

| 列名         | 数据类型        | 约束       | 注释               |
|--------------|-----------------|------------|--------------------|
| `ts_code`    | VARCHAR(10)     | NOT NULL   | Tushare股票代码    |
| `trade_date` | DATE            | NOT NULL   | 交易日期           |
| `adj_factor` | NUMERIC(18,6)   |            | 复权因子           |

### 5. `tushare_stock_dailybasic`

*   **描述:** 存储股票每日的基本面指标，如市盈率、市净率、换手率、市值等。
*   **Tushare API:** `daily_basic`
*   **注意:** 部分字段在数据库 schema 中定义，但未在 Tushare API 返回字段 (fields) 中列出，可能由程序计算或配置有误。
*   **列信息:**

| 列名               | 数据类型        | 约束       | 注释                                                         |
|--------------------|-----------------|------------|--------------------------------------------------------------|
| `ts_code`          | VARCHAR(10)     | NOT NULL   | Tushare股票代码                                              |
| `trade_date`       | DATE            | NOT NULL   | 交易日期                                                     |
| `close`            | NUMERIC(18,4)   |            | 当日收盘价                                                   |
| `turnover_rate`    | NUMERIC(18,4)   |            | 换手率（%）                                                  |
| `turnover_rate_f`  | NUMERIC(18,4)   |            | 换手率（自由流通股）                                         |
| `volume_ratio`     | NUMERIC(18,4)   |            | 量比                                                         |
| `pe`               | NUMERIC(18,4)   |            | 市盈率（总市值/净利润）                                      |
| `pe_ttm`           | NUMERIC(18,4)   |            | 市盈率（TTM）                                                |
| `pb`               | NUMERIC(18,4)   |            | 市净率（总市值/净资产）                                      |
| `ps`               | NUMERIC(18,4)   |            | 市销率                                                       |
| `ps_ttm`           | NUMERIC(18,4)   |            | 市销率（TTM）                                                |
| `dv_ratio`         | NUMERIC(18,4)   |            | 股息率（%）                                                  |
| `dv_ttm`           | NUMERIC(18,4)   |            | 股息率（TTM）（%）                                           |
| `total_share`      | NUMERIC(20,4)   |            | 总股本（万股）                                               |
| `float_share`      | NUMERIC(20,4)   |            | 流通股本（万股）                                             |
| `free_share`       | NUMERIC(20,4)   |            | 自由流通股本（万股）                                         |
| `total_mv`         | NUMERIC(20,4)   |            | 总市值（万元）                                               |
| `circ_mv`          | NUMERIC(20,4)   |            | 流通市值（万元）                                             |
| `free_mv`          | NUMERIC(20,4)   |            | 自由流通市值（万元）(注: schema 定义, API fields 未列出)     |
| `float_ratio`      | NUMERIC(18,4)   |            | 流通股本占比 (%)(注: schema 定义, API fields 未列出)         |
| `bp_ratio`         | NUMERIC(18,4)   |            | 账面价值/总市值 (注: schema 定义, API fields 未列出)         |
| `annual_div_yield` | NUMERIC(18,4)   |            | 年化股息率 (%)(注: schema 定义, API fields 未列出)           |

### 6. `tushare_index_basic`

*   **描述:** 存储指数的基础信息，如市场、发布方、基准日期等。全量更新。
*   **Tushare API:** `index_basic`
*   **列信息:**

| 列名         | 数据类型        | 约束       | 注释                                                   |
|--------------|-----------------|------------|--------------------------------------------------------|
| `ts_code`    | VARCHAR(30)     | NOT NULL   | Tushare指数代码                                        |
| `name`       | VARCHAR(100)    |            | 指数简称                                               |
| `market`     | VARCHAR(50)     |            | 市场 (例如: SSE, SZSE, CSI, CICC, SW, CNIS, OTH)       |
| `publisher`  | VARCHAR(100)    |            | 发布方                                                 |
| `category`   | VARCHAR(100)    |            | 指数类别                                               |
| `base_date`  | DATE            |            | 基期                                                   |
| `base_point` | FLOAT           |            | 基点                                                   |
| `list_date`  | DATE            |            | 发布日期 (空值填充为'1970-01-01')                      |

### 7. `tushare_index_swdaily`

*   **描述:** 存储申万行业指数的日线行情数据。增量更新。
*   **Tushare API:** `sw_daily`
*   **列信息:**

| 列名         | 数据类型        | 约束       | 注释                                     |
|--------------|-----------------|------------|------------------------------------------|
| `ts_code`    | VARCHAR(15)     | NOT NULL   | Tushare指数代码                          |
| `trade_date` | DATE            | NOT NULL   | 交易日期                                 |
| `name`       | VARCHAR(100)    |            | 指数名称                                 |
| `open`       | FLOAT           |            | 开盘点位                                 |
| `low`        | FLOAT           |            | 最低点位                                 |
| `high`       | FLOAT           |            | 最高点位                                 |
| `close`      | FLOAT           |            | 收盘点位                                 |
| `change`     | FLOAT           |            | 涨跌点位                                 |
| `pct_change` | FLOAT           |            | 涨跌幅 (%)                               |
| `volume`     | FLOAT           |            | 成交量（万股）(源自 Tushare `vol`)       |
| `amount`     | FLOAT           |            | 成交额（万元）                           |
| `pe`         | FLOAT           |            | 市盈率                                   |
| `pb`         | FLOAT           |            | 市净率                                   |
| `float_mv`   | FLOAT           |            | 流通市值（万元）                         |
| `total_mv`   | FLOAT           |            | 总市值（万元）                           |

### 8. `tushare_fina_indicator`

*   **描述:** 存储上市公司的各类财务指标数据。增量更新。包含非常多的财务指标。
*   **Tushare API:** `fina_indicator_vip`
*   **列信息 (关键列及概述):**

| 列名         | 数据类型        | 约束       | 注释                                                         |
|--------------|-----------------|------------|--------------------------------------------------------------|
| `ts_code`    | VARCHAR(10)     | NOT NULL   | Tushare股票代码                                              |
| `ann_date`   | DATE            | NOT NULL   | 公告日期 (空值填充为 end_date)                               |
| `end_date`   | DATE            | NOT NULL   | 报告期                                                       |
| `eps`        | NUMERIC(20,4)   |            | 基本每股收益                                                 |
| `bps`        | NUMERIC(20,4)   |            | 每股净资产                                                   |
| `roe`        | NUMERIC(20,4)   |            | 净资产收益率                                                 |
| `roa`        | NUMERIC(20,4)   |            | 总资产报酬率                                                 |
| ...          | NUMERIC(20,4)   |            | (其他众多财务指标, 如偿债能力、营运能力、成长能力等指标)     |
| `update_flag`| VARCHAR(1)      |            | 更新标识                                                     |

*(请参考 `alphahome/fetchers/tasks/finance/tushare_fina_indicator.py` 中的 `fields` 列表获取完整字段名)*

### 9. `tushare_fina_income`

*   **描述:** 存储上市公司的利润表数据。包含收入、成本、利润等众多项目。
*   **Tushare API:** `income_vip`
*   **列信息 (关键列及概述):**

| 列名             | 数据类型        | 约束       | 注释                                                         |
|------------------|-----------------|------------|--------------------------------------------------------------|
| `ts_code`        | VARCHAR(10)     | NOT NULL   | Tushare股票代码                                              |
| `ann_date`       | DATE            |            | 公告日期 (Tushare原始公告日期)                               |
| `f_ann_date`     | DATE            | NOT NULL   | 实际公告日期 (优先使用)                                      |
| `end_date`       | DATE            | NOT NULL   | 报告期截止日期                                               |
| `report_type`    | SMALLINT        |            | 报告类型 (1合并报表 2单季合并 等)                            |
| `comp_type`      | SMALLINT        |            | 公司类型 (1一般工商业 2银行 3保险 4证券)                     |
| `total_revenue`  | NUMERIC(20,4)   |            | 营业总收入                                                   |
| `n_income_attr_p`| NUMERIC(20,4)   |            | 归属于母公司所有者的净利润                                   |
| `basic_eps`      | NUMERIC(20,4)   |            | 基本每股收益                                                 |
| ...              | NUMERIC(20,4)   |            | (其他众多利润表项目)                                         |

*(请参考 `alphahome/fetchers/tasks/finance/tushare_fina_income.py` 中的 `fields` 列表获取完整字段名)*

### 10. `tushare_fina_balancesheet`

*   **描述:** 存储上市公司的资产负债表数据。包含资产、负债、所有者权益等众多项目。
*   **Tushare API:** `balancesheet_vip`
*   **列信息 (关键列及概述):**

| 列名                         | 数据类型        | 约束       | 注释                                                         |
|------------------------------|-----------------|------------|--------------------------------------------------------------|
| `ts_code`                    | VARCHAR(10)     | NOT NULL   | Tushare股票代码                                              |
| `ann_date`                   | DATE            |            | 公告日期 (Tushare原始公告日期)                               |
| `f_ann_date`                 | DATE            | NOT NULL   | 实际公告日期 (优先使用)                                      |
| `end_date`                   | DATE            | NOT NULL   | 报告期截止日期                                               |
| `report_type`                | SMALLINT        |            | 报告类型 (1合并报表 2单季合并 等)                            |
| `comp_type`                  | SMALLINT        |            | 公司类型 (1一般工商业 2银行 3保险 4证券)                     |
| `total_assets`               | NUMERIC(20,4)   |            | 资产总计                                                     |
| `total_liab`                 | NUMERIC(20,4)   |            | 负债合计                                                     |
| `total_hldr_eqy_inc_min_int` | NUMERIC(20,4)   |            | 所有者权益(或股东权益)合计(含少数股东权益)                   |
| ...                          | NUMERIC(20,4)   |            | (其他众多资产负债表项目)                                     |

*(请参考 `alphahome/fetchers/tasks/finance/tushare_fina_balancesheet.py` 中的 `fields` 列表获取完整字段名)*

### 11. `tushare_fina_cashflow`

*   **描述:** 存储上市公司的现金流量表数据。包含经营、投资、筹资活动现金流等众多项目。
*   **Tushare API:** `cashflow_vip`
*   **列信息 (关键列及概述):**

| 列名                     | 数据类型        | 约束       | 注释                                                         |
|--------------------------|-----------------|------------|--------------------------------------------------------------|
| `ts_code`                | VARCHAR(10)     | NOT NULL   | Tushare股票代码                                              |
| `ann_date`               | DATE            |            | 公告日期 (Tushare原始公告日期)                               |
| `f_ann_date`             | DATE            | NOT NULL   | 实际公告日期 (优先使用)                                      |
| `end_date`               | DATE            | NOT NULL   | 报告期截止日期                                               |
| `report_type`            | SMALLINT        |            | 报告类型 (1合并报表 2单季合并 等)                            |
| `comp_type`              | SMALLINT        |            | 公司类型 (1一般工商业 2银行 3保险 4证券)                     |
| `n_cashflow_act`         | NUMERIC(20,4)   |            | 经营活动产生的现金流量净额                                   |
| `n_cashflow_inv_act`     | NUMERIC(20,4)   |            | 投资活动产生的现金流量净额                                   |
| `n_cash_flows_fnc_act`   | NUMERIC(20,4)   |            | 筹资活动产生的现金流量净额                                   |
| `n_incr_cash_cash_equ`   | NUMERIC(20,4)   |            | 现金及现金等价物净增加额                                     |
| `c_cash_equ_end_period`  | NUMERIC(20,4)   |            | 期末现金及现金等价物余额                                     |
| ...                      | NUMERIC(20,4)   |            | (其他众多现金流量表项目)                                     |

*(请参考 `alphahome/fetchers/tasks/finance/tushare_fina_cashflow.py` 中的 `fields` 列表获取完整字段名)*

### 12. `tushare_fina_forecast`

*   **描述:** 存储上市公司的业绩预告数据。
*   **Tushare API:** `forecast_vip`
*   **列信息 (关键列):**

| 列名             | 数据类型        | 约束       | 注释                                    |
|------------------|-----------------|------------|-----------------------------------------|
| `ts_code`        | VARCHAR(10)     | NOT NULL   | Tushare股票代码                         |
| `ann_date`       | DATE            | NOT NULL   | 公告日期                                |
| `end_date`       | DATE            | NOT NULL   | 报告期截止日期                          |
| `type`           | VARCHAR(20)     |            | 预告类型 (预增/预减/扭亏等)             |
| `p_change_min`   | NUMERIC(20,4)   |            | 预告净利润变动幅度下限(%)               |
| `p_change_max`   | NUMERIC(20,4)   |            | 预告净利润变动幅度上限(%)               |
| `net_profit_min` | NUMERIC(20,4)   |            | 预告净利润下限（万元）                  |
| `net_profit_max` | NUMERIC(20,4)   |            | 预告净利润上限（万元）                  |
| `summary`        | TEXT            |            | 业绩预告摘要                            |
| `change_reason`  | TEXT            |            | 业绩变动原因                            |
| ...              | ...             |            | (其他字段如 `last_parent_net`, `first_ann_date`) |

### 13. `tushare_fina_express`

*   **描述:** 存储上市公司的业绩快报数据（财报发布前的初步数据）。
*   **Tushare API:** `express`
*   **列信息 (关键列):**

| 列名              | 数据类型        | 约束       | 注释                                     |
|-------------------|-----------------|------------|------------------------------------------|
| `ts_code`         | VARCHAR(10)     | NOT NULL   | Tushare股票代码                          |
| `ann_date`        | DATE            | NOT NULL   | 公告日期                                 |
| `end_date`        | DATE            | NOT NULL   | 报告期截止日期                           |
| `revenue`         | NUMERIC(20,4)   |            | 营业收入(元)                             |
| `n_income`        | NUMERIC(20,4)   |            | 净利润(元)                               |
| `total_assets`    | NUMERIC(20,4)   |            | 总资产(元)                               |
| `diluted_eps`     | NUMERIC(20,4)   |            | 基本每股收益(元)                         |
| `diluted_roe`     | NUMERIC(20,4)   |            | 净资产收益率(%)                          |
| `yoy_net_profit`  | NUMERIC(20,4)   |            | 净利润同比增长率(%)                      |
| `bps`             | NUMERIC(20,4)   |            | 每股净资产(元)                           |
| `perf_summary`    | TEXT            |            | 业绩简要说明                             |
| `is_audit`        | SMALLINT        |            | 是否审计：1是 0否                        |
| ...               | ...             |            | (其他同比增长率、上年同期值等字段)       |

### 14. `tushare_fund_basic`

*   **描述:** 存储公募基金的基础信息。全量更新。
*   **Tushare API:** `fund_basic`
*   **列信息 (关键列):**

| 列名           | 数据类型        | 约束       | 注释                                     |
|----------------|-----------------|------------|------------------------------------------|
| `ts_code`      | VARCHAR(15)     | NOT NULL   | Tushare基金代码                          |
| `name`         | VARCHAR(100)    |            | 基金简称                                 |
| `management`   | VARCHAR(100)    |            | 管理人                                   |
| `custodian`    | VARCHAR(100)    |            | 托管人                                   |
| `fund_type`    | VARCHAR(50)     |            | 投资类型                                 |
| `found_date`   | DATE            |            | 成立日期                                 |
| `list_date`    | DATE            |            | 上市日期                                 |
| `issue_amount` | FLOAT           |            | 发行份额(亿份)                           |
| `m_fee`        | FLOAT           |            | 管理费率(%)                              |
| `c_fee`        | FLOAT           |            | 托管费率(%)                              |
| `status`       | VARCHAR(1)      |            | 存续状态 (I发行 D存续期 L终止)           |
| `market`       | VARCHAR(1)      |            | 上市地点 (E场内 O场外)                   |
| ...            | ...             |            | (其他字段如 `due_date`, `benchmark`, `p_value` 等) |

### 15. `tushare_fund_daily`

*   **描述:** 存储场内基金（ETF、LOF等）的日线行情数据。
*   **Tushare API:** `fund_daily`
*   **列信息:**

| 列名       | 数据类型        | 约束       | 注释                                     |
|------------|-----------------|------------|------------------------------------------|
| `ts_code`  | VARCHAR(15)     | NOT NULL   | Tushare基金代码                          |
| `trade_date`| DATE            | NOT NULL   | 交易日期                                 |
| `open`     | FLOAT           |            | 开盘价(元)                               |
| `high`     | FLOAT           |            | 最高价(元)                               |
| `low`      | FLOAT           |            | 最低价(元)                               |
| `close`    | FLOAT           |            | 收盘价(元)                               |
| `pre_close`| FLOAT           |            | 昨收盘价(元)                             |
| `change`   | FLOAT           |            | 涨跌额(元)                               |
| `pct_chg`  | FLOAT           |            | 涨跌幅(%)                                |
| `volume`   | FLOAT           |            | 成交量(手) (源自 Tushare `vol`)          |
| `amount`   | FLOAT           |            | 成交额(千元)                             |

### 16. `tushare_fund_nav`

*   **描述:** 存储公募基金的净值数据。
*   **Tushare API:** `fund_nav`
*   **列信息:**

| 列名             | 数据类型        | 约束       | 注释                       |
|------------------|-----------------|------------|----------------------------|
| `ts_code`        | VARCHAR(15)     | NOT NULL   | Tushare基金代码            |
| `ann_date`       | DATE            |            | 公告日期                   |
| `nav_date`       | DATE            | NOT NULL   | 净值日期                   |
| `unit_nav`       | FLOAT           |            | 单位净值                   |
| `accum_nav`      | FLOAT           |            | 累计净值                   |
| `accum_div`      | FLOAT           |            | 累计分红                   |
| `net_asset`      | FLOAT           |            | 资产净值(万元)             |
| `total_netasset` | FLOAT           |            | 合计资产净值(万元)         |
| `adj_nav`        | FLOAT           |            | 复权单位净值               |

### 17. `tushare_fund_adjfactor`

*   **描述:** 存储公募基金的复权因子，用于计算后复权价格。
*   **Tushare API:** `fund_adj`
*   **列信息:**

| 列名         | 数据类型        | 约束       | 注释               |
|--------------|-----------------|------------|--------------------|
| `ts_code`    | VARCHAR(15)     | NOT NULL   | Tushare基金代码    |
| `trade_date` | DATE            | NOT NULL   | 交易日期           |
| `adj_factor` | FLOAT           |            | 复权因子           |

### 18. `tushare_fund_share`

*   **描述:** 存储基金规模数据，主要是 ETF 的份额变化。
*   **Tushare API:** `fund_share`
*   **列信息:**

| 列名         | 数据类型        | 约束       | 注释               |
|--------------|-----------------|------------|--------------------|
| `ts_code`    | VARCHAR(15)     | NOT NULL   | Tushare基金代码    |
| `trade_date` | DATE            | NOT NULL   | 交易日期           |
| `fd_share`   | FLOAT           |            | 基金份额（万份）   |

### 19. `tushare_fund_portfolio`

*   **描述:** 存储公募基金季度末的股票持仓明细。
*   **Tushare API:** `fund_portfolio`
*   **列信息:**

| 列名             | 数据类型        | 约束       | 注释                       |
|------------------|-----------------|------------|----------------------------|
| `ts_code`        | VARCHAR(15)     | NOT NULL   | Tushare基金代码            |
| `ann_date`       | DATE            | NOT NULL   | 公告日期                   |
| `end_date`       | DATE            | NOT NULL   | 报告期截止日期             |
| `symbol`         | VARCHAR(10)     | NOT NULL   | 股票代码                   |
| `mkv`            | FLOAT           |            | 持有股票市值(元)           |
| `amount`         | FLOAT           |            | 持有股票数量（股）         |
| `stk_mkv_ratio`  | FLOAT           |            | 占股票市值比(%)            |
| `stk_float_ratio`| FLOAT           |            | 占流通股本比例(%)          |

### 20. `tushare_index_cimember`

*   **描述:** 存储中信(CITIC)行业成分数据，包含历史信息。
*   **Tushare API:** `ci_index_member`
*   **列信息:**

| 列名      | 数据类型        | 约束       | 注释               |
|-----------|-----------------|------------|--------------------|
| `l1_code` | VARCHAR(20)     |            | 一级行业代码       |
| `l1_name` | VARCHAR(50)     |            | 一级行业名称       |
| `l2_code` | VARCHAR(20)     |            | 二级行业代码       |
| `l2_name` | VARCHAR(50)     |            | 二级行业名称       |
| `l3_code` | VARCHAR(20)     | NOT NULL   | 三级行业代码       |
| `l3_name` | VARCHAR(100)    |            | 三级行业名称       |
| `ts_code` | VARCHAR(30)     | NOT NULL   | Tushare股票代码    |
| `name`    | VARCHAR(100)    |            | 股票名称           |
| `in_date` | DATE            | NOT NULL   | 纳入日期           |
| `out_date`| DATE            |            | 剔除日期           |
| `is_new`  | VARCHAR(1)      |            | 是否最新Y/N        |

### 21. `tushare_index_swmember`

*   **描述:** 存储申万(SW)行业成分数据（分级），包含历史信息。
*   **Tushare API:** `index_member_all`
*   **列信息:**

| 列名      | 数据类型        | 约束       | 注释               |
|-----------|-----------------|------------|--------------------|
| `l1_code` | VARCHAR(20)     |            | 一级行业代码       |
| `l1_name` | VARCHAR(50)     |            | 一级行业名称       |
| `l2_code` | VARCHAR(20)     |            | 二级行业代码       |
| `l2_name` | VARCHAR(50)     |            | 二级行业名称       |
| `l3_code` | VARCHAR(20)     | NOT NULL   | 三级行业代码       |
| `l3_name` | VARCHAR(100)    |            | 三级行业名称       |
| `ts_code` | VARCHAR(30)     | NOT NULL   | Tushare股票代码    |
| `name`    | VARCHAR(100)    |            | 股票名称           |
| `in_date` | DATE            | NOT NULL   | 纳入日期           |
| `out_date`| DATE            |            | 剔除日期           |
| `is_new`  | VARCHAR(1)      |            | 是否最新Y/N        |

### 22. `tushare_index_cidaily`

*   **描述:** 存储中信(CITIC)行业指数的日线行情数据。
*   **Tushare API:** `ci_daily`
*   **列信息:**

| 列名         | 数据类型        | 约束       | 注释                                     |
|--------------|-----------------|------------|------------------------------------------|
| `ts_code`    | VARCHAR(15)     | NOT NULL   | Tushare指数代码                          |
| `trade_date` | DATE            | NOT NULL   | 交易日期                                 |
| `open`       | FLOAT           |            | 开盘点位                                 |
| `low`        | FLOAT           |            | 最低点位                                 |
| `high`       | FLOAT           |            | 最高点位                                 |
| `close`      | FLOAT           |            | 收盘点位                                 |
| `pre_close`  | FLOAT           |            | 昨收盘点位                               |
| `change`     | FLOAT           |            | 涨跌点位                                 |
| `pct_change` | FLOAT           |            | 涨跌幅(%)                                |
| `volume`     | FLOAT           |            | 成交量（万股）(源自 Tushare `vol`)       |
| `amount`     | FLOAT           |            | 成交额（万元）                           |

---

## 潜在/未识别的表

当前分析已覆盖 `alphahome/fetchers/tasks/` 目录下所有明确定义的任务文件。

可能还存在基于以下 Tushare API 的数据表，但需要检查项目中是否有对应的任务实现：

*   **交易日历:** (`trade_cal`)
*   **分红送股:** (`dividend`)
*   **其他 Tushare API:** (例如沪深股通、港股通、期权、期货等，需根据项目实际使用的 API 确定) 