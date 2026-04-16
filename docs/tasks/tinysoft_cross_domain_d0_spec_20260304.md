# TinySoft 跨域 D0 口径与模型规范（2026-03-04）

## 1. 交付物

- 重合矩阵（CSV）：`docs/tasks/tinysoft_cross_domain_overlap_matrix_20260304.csv`
- 本文档（口径字典 + 新表建模规范）：`docs/tasks/tinysoft_cross_domain_d0_spec_20260304.md`

## 2. 统一字段口径字典

## 2.1 代码与标识口径

| 业务对象 | Canonical 字段 | TinySoft 原始样式 | 统一规则 | 备注 |
|---|---|---|---|---|
| 股票 | `ts_code` | `SH600000` / `SZ000001` / `BJ430001` | 统一为 `600000.SH` / `000001.SZ` / `430001.BJ` | 与现有 `tinysoft_stock_minute.py` 一致 |
| 股票 | `tsl_code` | `SH600000` | 保留 TinySoft 原码（大写） | 用于追溯与排错 |
| 基金 | `ts_code` | `OF010031` / `SH510300` / `SZ159915` | 统一为 `010031.OF` / `510300.SH` / `159915.SZ` | 基金存在场外与场内双通道 |
| 基金 | `tsl_code` | `OF010031` 等 | 保留 TinySoft 原码（大写） | 与 `ts_code` 一一映射 |
| 指数/市场序列 | `index_code_raw` | `SH000001` / `HKHSI001` / `HSG000002` / `CBICBA00301` / `CSI000300` | 原样保留（大写） | 指数字段不强制转 `ts_code` |
| 指数（可映射时） | `index_ts_code` | `CSI000300` 等 | 能映射时转为 `000300.CSI`，否则置空 | 防止错误强制转换 |

## 2.2 交易所与通道口径

| 原始前缀/代码 | 语义 | 统一字段 |
|---|---|---|
| `SH` | 上交所 | `exchange='SH'` |
| `SZ` | 深交所 | `exchange='SZ'` |
| `BJ` | 北交所 | `exchange='BJ'` |
| `OF` | 场外公募基金代码 | `exchange='OF'` |
| `HG000001` | 港股通(沪) | `channel_code='HG000001'` |
| `HG000002` | 沪股通 | `channel_code='HG000002'` |
| `HG000003` | 港股通(深) | `channel_code='HG000003'` |
| `HG000004` | 深股通 | `channel_code='HG000004'` |
| `HSG000001` | 南向日历 | `market_code='HSG000001'` |
| `HSG000002` | 北向日历 | `market_code='HSG000002'` |
| `HKHSI001` | 港股市场日历 | `market_code='HKHSI001'` |
| `CBICBA00301` | 银行间债市日历 | `market_code='CBICBA00301'` |

## 2.3 日期字段语义

| 字段 | 语义 | 存储类型 | 规则 |
|---|---|---|---|
| `trade_date` | 交易发生日/统计截止日 | `DATE` | 与交易或口径采样日绑定 |
| `ann_date` | 公告/披露日 | `DATE` | 仅披露型表使用 |
| `report_date` | 报告期截止日 | `DATE` | 季报/年报口径表使用 |
| `start_date`/`end_date` | 区间起止 | `DATE` | 周收益、经理绩效等区间表使用 |
| `in_date`/`out_date` | 入选/剔除生效日 | `DATE` | 版本化分类与成分表使用 |
| `update_time` | 入库更新时间 | `TIMESTAMP` | 统一自动写入 |

补充：

- `fina_pit_ext` 继续使用现有规则：`trade_date = COALESCE(ann_date, report_date)`。
- 所有 `YYYYMMDD` 原始整数在入库前统一转换成 `DATE`。

## 3. 新表建模规范（主键/索引/分区/幂等）

说明：

- `upsert_rule` 统一为：`ON CONFLICT(pk) DO UPDATE` 非主键字段，`update_time` 由现有 `DBManager.upsert` 智能维护。
- 分区策略分两档：
  - `NONE`：普通堆表 + btree 索引。
  - `RANGE(trade_date|report_date, YEAR)`：年分区（高体量表）。

### 3.1 D1-D3 主落地（P0）

| 目标表 | TinySoft 表ID | 主键（PK） | 二级索引 | 分区策略 | upsert_rule |
|---|---|---|---|---|---|
| `tinysoft.stock_hsgt_daily` | 130 | `(channel_code, trade_date)` | `trade_date`, `channel_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_hsgt_top10` | 131 | `(channel_code, trade_date, rank_no, security_code_raw)` | `trade_date`, `security_code_raw`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_hsgt_hold` | 132 | `(channel_code, trade_date, security_code_raw)` | `trade_date`, `ts_code`, `channel_code` | `RANGE(trade_date,YEAR)` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_hsgt_short_balance` | 161 | `(channel_code, trade_date, security_code_raw)` | `trade_date`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_lending_summary` | 151（正文标注150，待实测确认） | `(ts_code, trade_date, tenor_days, declare_type, data_type)` | `trade_date`, `ts_code` | `RANGE(trade_date,YEAR)` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_lending_trade` | 152 | `(ts_code, trade_date, tenor_days)` | `trade_date`, `ts_code` | `RANGE(trade_date,YEAR)` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_lending_balance` | 153 | `(ts_code, trade_date)` | `trade_date`, `ts_code` | `RANGE(trade_date,YEAR)` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_style_ext` | 614 | `(ts_code, report_date)` | `report_date`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_risk_adj_ext` | 622 | `(ts_code, start_date, end_date, data_type)` | `end_date`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_category_index_ret` | 623 | `(category_code, start_date, end_date, data_type)` | `end_date`, `category_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_manager_perf_ext` | 625 | `(manager_code, start_date, end_date, category_code)` | `end_date`, `manager_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_manager_risk_ext` | 626 | `(manager_code, start_date, end_date, category_code)` | `end_date`, `manager_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.market_calendar_multi` | 753 | `(market_code, trade_date)` | `trade_date`, `market_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.index_financial_agg` | 754-763 | `(index_code_raw, report_date, metric_table_id, metric_name, agg_method, sample_scope)` | `report_date`, `index_code_raw`, `metric_name` | `RANGE(report_date,YEAR)` | `conflict=PK, update=all_non_pk` |

### 3.2 提质预留（P1）

| 目标表 | TinySoft 表ID | 主键（PK） | 二级索引 | 分区策略 | upsert_rule |
|---|---|---|---|---|---|
| `tinysoft.stock_public_trade_info` | 129 | `(ts_code, trade_date, abnormal_type, broker_short_name, trade_action)` | `trade_date`, `ts_code` | `RANGE(trade_date,YEAR)` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_unlock_schedule` | 154 | `(ts_code, unlock_date, lock_type)` | `unlock_date`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_holder_change_ext` | 157 | `(ts_code, ann_date, holder_name, change_direction, change_reason)` | `ann_date`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_repurchase_ext` | 160 | `(ts_code, ann_date, report_date, repurchase_type)` | `ann_date`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_pledge_summary` | 144 | `(market_code, trade_date)` | `trade_date`, `market_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_pledge_detail` | 145 | `(security_code_raw, trade_date)` | `trade_date`, `ts_code` | `RANGE(trade_date,YEAR)` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_pledge_balance` | 146 | `(security_code_raw, trade_date, data_source)` | `trade_date`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.stock_pledge_rate` | 147 | `(market_code, trade_date)` | `trade_date`, `market_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_holding_detail_ext` | 318 | `(ts_code, report_date, security_code_raw, rank_no)` | `report_date`, `ts_code` | `RANGE(report_date,YEAR)` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_industry_alloc_ext` | 320 | `(ts_code, report_date, industry_name)` | `report_date`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_asset_alloc_ext` | 322 | `(ts_code, report_date, asset_type)` | `report_date`, `ts_code` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_classification_info` | 355 | `(attr_code, level_no, in_date)` | `attr_code`, `in_date` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.fund_classification_member` | 356 | `(ts_code, attr_code, level_no, in_date)` | `ts_code`, `in_date` | `NONE` | `conflict=PK, update=all_non_pk` |
| `tinysoft.index_member_versioned` | 752 | `(index_code_raw, con_code_raw, in_date)` | `index_code_raw`, `con_code_raw`, `out_date` | `NONE` | `conflict=PK, update=all_non_pk` |

## 4. 关键实现约束（D1 起执行）

- 统一保留双码：`ts_code` + `tsl_code/index_code_raw`，禁止只存一种代码。
- 原始“金额/数量/比率”字段先保留原单位，再增加标准化字段（避免信息损失）。
- 对披露机制变化表（130/131/132/161），增加 `disclosure_cycle`、`is_disclosure_missing` 字段。
- 所有表默认 `validation_mode='report'`，关键主键字段空值过滤复用 BaseTask 现有逻辑。

## 5. 待确认项

- 数据字典在“转融通证券出借交易”章节存在 `表ID 151` 与正文 `表ID 150` 不一致，D1 实装前需以 `call(infoarray, ...)` 实测确认。

## 6. 依据

- `tmp_pdf_text.txt`（股票 3.2、基金 3.3、基金扩展 3.4、指数 3.9 相关表ID）。
- 现网代码口径：`alphahome/fetchers/tasks/stock/tinysoft_stock_minute.py`。
- 现网库结构与索引：`tinysoft` / `tushare` / `akshare` `information_schema` 结果（2026-03-04）。
