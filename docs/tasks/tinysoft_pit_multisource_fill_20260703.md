# Tinysoft 多源补缺 PIT 财务缺口方案（2026-07-03）

## 1. 背景

PIT 财务三表（income/balance/cashflow）100% 依赖 tushare schema，存在 tushare 上游漏采导致的 `source_missing` 缺口——重跑 PIT 无法补出（源头无数据）。本方案用 Tinysoft 表44/48 作为补充源填补此类缺口。

## 2. 缺口调研结论（2020+，相对另外两表并集）

| 缺口表 | 缺口期数 | 分布特征 | 可补性 |
|---|---|---|---|
| cashflow（表48补） | ~3855 | 2022/2023 高度集中在 Q2/Q4（2022Q2=779, 2023Q2=1387），Q1/Q3 极少 | **高**，疑似 tushare VIP 对半年报/年报现金流采集问题，天软表48有完整数据 |
| balance（表44补） | 2020/2021 各千余、2024 有 207 | 早期较多，近年大幅减少 | **高**，天软表44有完整资产负债表 |
| income | 各年 80-250，稳定 | 分散 | **低**，天软无独立利润表，仅表42少数指标可部分补 |

**现金流缺口季度集中性**（2022-2023）：

| 年 | Q1 | Q2 | Q3 | Q4 |
|---|---|---|---|---|
| 2022 | 25 | **779** | 29 | 470 |
| 2023 | 24 | **1387** | 16 | 281 |

Q2/Q4 集中、Q1/Q3 极少 → 系统性漏采（非个股偶发），批量补全价值高。

**缺口真实性核实**（2022-2023 cashflow）：tushare 源缺 3011 期 = PIT report 源缺 3011 期 = PIT 所有源缺 3010 期（1 个差异是已补的 002549.SZ）。即重跑 PIT 补不出，必须换源。

## 3. 可补前提（缺一不可）

1. tushare 源表对该股该报告期 source_missing（fina_cashflow/fina_balancesheet 双缺）
2. 同股同期 tushare **利润表**有该期（说明公司确已披露，非未披露）—— ann_date 也从 income 表复用
3. 天软表44/48 有该期数据（按"截止日"整数匹配，如 20220630）

## 4. 已落地能力

### 4.1 fina_pit_ext 任务多表 profiles

`alphahome/fetchers/tasks/stock/tinysoft_stock_fina_pit_ext.py` 的 `default_metric_profiles` 已扩展为 3 个 profile：

| finance_source | table_id | 表名 | metric_name 对齐 |
|---|---|---|---|
| report_42_main | 42 | 主要财务指标 | eps_diluted/bps/roe_diluted/netprofit_excl_nr |
| report_44_balancesheet | 44 | 合并资产负债表 | pit_balance 字段（tot_assets 等 6 个） |
| report_48_cashflow | 48 | 合并现金流量表 | pit_cashflow 字段（n_cashflow_act 等 10 个） |

表44/48 的 `metric_name` 与 PIT 字段名对齐，使校准/对账/补缺共享同一份字段定义。config.example.json 已同步更新示例。

### 4.2 补缺脚本

`scripts/analysis/pit_cashflow_tinysoft_fill.py`（通用版，字段映射从 fina_pit_ext 任务动态读取）：

```bash
# dry-run（默认）
python scripts/analysis/pit_cashflow_tinysoft_fill.py --ts-code 002549.SZ --end-date 2022-06-30 --table cashflow
python scripts/analysis/pit_cashflow_tinysoft_fill.py --ts-code 001391.SZ --end-date 2024-06-30 --table balance
# 实际写入
python scripts/analysis/pit_cashflow_tinysoft_fill.py --ts-code 002549.SZ --end-date 2022-06-30 --table cashflow --apply
```

辅助脚本：
- `scripts/analysis/tinysoft_cashflow48_probe.py` — 表48列名/覆盖探测
- `scripts/analysis/tinysoft_infotable_probe.py` — 通用 infotable 列名探测（表44/48等）
- `scripts/analysis/fina_pit_ext_profile_check.py` — 验证 fina_pit_ext 表44/48 profile 端到端取数

## 5. PIT 时点一致性原则（写库必须遵守）

- **数值**取自天软表44/48（按"截止日"匹配报告期）
- **ann_date 不用天软"公布日"**（那是数据更新日，如 20220630 期公布日是 20230830，远晚于实际披露），复用 `pit.pit_income_quarterly` 同股同期 ann_date，保证三表时点一致
- `data_source='tinysoft'`，主键 `(ts_code,end_date,ann_date,data_source)` 与 `'report'` 隔离不冲突
- 天软对未披露科目常填 0，补缺时 `v != 0` 过滤，保留 PIT 留空语义

## 6. 对账结论

天软表48 与 tushare fina_cashflow 在同一报告期数值完全一致（002549.SZ 2022Q1/Q3/Q4 三个净额字段零差异），单位/正负号/合并口径一致。表48"报告类型"=已审计对应 tushare report_type=1。表44 同理（资产=负债+权益 校验通过）。

## 7. 已处理案例

- 002549.SZ / 2022-06-30（Q2 半年报现金流，tushare 三表仅现金流缺）→ 已用天软表48补入 `pit.pit_cashflow_quarterly`，n_cashflow_act=124925392.36，ann_date=2022-07-27

## 8. 边界与限制

- **income 缺口**：天软无独立利润表，表42仅有少数指标，无法完整补 income 三表字段。income 缺口需另寻源（akshare 或人工）。
- **ann_date 依赖 income**：补 balance/cashflow 前提是该股同期 income 在 PIT 表有 ann_date。若 income 也缺，脚本会跳过（需先补 income 或用其他 ann_date 来源）。
- **效率**：`fetch_infotable_for_symbol_pairs` 当前 `select *`（表44=206列/表48=174列），大批量补缺时可传 fields 优化（待基类支持）。
- **批量补缺**：当前脚本是单股单期。全量 ~3855 现金流缺口需封装批量扫描+补缺（后续可做）。
