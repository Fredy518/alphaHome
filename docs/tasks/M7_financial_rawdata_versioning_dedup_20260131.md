# M7 财务源表版本/去重策略调研（2026-01-31）

范围：`rawdata.fina_income / rawdata.fina_balancesheet / rawdata.fina_cashflow / rawdata.fina_indicator / rawdata.fina_express / rawdata.fina_forecast`  
目的：给出**源表数据特征**与**去重/版本选择策略**，供评审讨论通过后再落地到 `features` 的 MV SQL（本文件不直接改动口径）。

> 背景：`rawdata` schema 是由脚本 `scripts/migrate_existing_tables_to_rawdata.py` 创建的“统一视图层”（tushare 优先），因此这里看到的数据特征本质上来自 `tushare.*` 源表。

## 1. 关键发现（数据特征）

### 1.1 rawdata.* 为 VIEW

在当前数据库快照中，`rawdata.fina_income` 等均为 `VIEW`（非实体表）。这意味着：
- “去重/版本选择”更适合在 **features MV 层（SQL）** 或 **上游写入约束/脚本** 中实现；
- 直接对 `rawdata` 做 DDL/清洗不合适（会影响视图映射语义）。

### 1.2 版本维度的核心字段是 `f_ann_date`（三大报表）

根据 repo 文档：`docs/business/data_sources.md` 明确指出财务三表 `f_ann_date` 是“实际公告日期（优先使用）”。  
本次调研也观测到：在 `fina_income / fina_balancesheet / fina_cashflow` 中，
- `(ts_code, end_date, f_ann_date)` **唯一**（无重复组）
- 但 `(ts_code, end_date, ann_date)` 会出现重复组，且所有重复组都伴随 **多个不同的 `f_ann_date`**

结论：对三大报表而言，若仍以 `ann_date` 作为 PIT 生效日期，会把多个版本“挤压”到同一天，产生歧义甚至未来函数风险；应以 `f_ann_date`（或 `COALESCE(f_ann_date, ann_date)`）作为 PIT 生效日期。

### 1.3 重复统计（当前 DB 快照）

统计口径：
- `dup_groups(ts_code,end_date)`：同一报告期存在多版本（正常现象：多次公告/修订）
- `dup_groups(ts_code,end_date,ann_date)`：同一公告日仍有多条（通常意味着 **ann_date 不足以区分版本**）

| 表 | 行数 | dup_groups(ts_code,end_date) / max | dup_groups(ts_code,end_date,ann_date) / max | dup_groups(ts_code,end_date,f_ann_date) / max |
|---|---:|---:|---:|---:|
| rawdata.fina_indicator | 348,261 | 18 / 3 | 0 / 1 | - |
| rawdata.fina_income | 339,652 | 1,299 / 4 | 905 / 2 | 0 / 1 |
| rawdata.fina_balancesheet | 333,446 | 1,710 / 4 | 1,166 / 3 | 0 / 1 |
| rawdata.fina_cashflow | 297,708 | 652 / 4 | 318 / 2 | 0 / 1 |
| rawdata.fina_express | 28,416 | 17 / 2 | 0 / 1 | - |
| rawdata.fina_forecast | 125,607 | 7,801 / 4 | 0 / 1 | - |

> 注：三大报表当前快照里 `report_type` 全为 1（`SMALLINT=1`），因此 `report_type` 对去重无区分度；`comp_type` 有 1/2/3/4 多种取值，但在重复组里未观察到 comp_type 分歧。

### 1.4 典型重复样例（三大报表）

以 `rawdata.fina_income` 为例：同一 `(ts_code,end_date,ann_date)` 下出现多条，`f_ann_date` 不同，且数值字段可能发生变化（例如利润被重述）。  
这进一步支持：`ann_date` 不是稳定的版本键，不能用于 PIT 窗口的唯一定位。

## 2. 建议策略（去重 / 版本选择）

### 2.1 统一“PIT 生效日期”字段：`pit_ann_date`

建议在 features MV 中引入统一口径（可以是 SQL 内部字段，不一定要落表）：

- **正式财报三表（income/balance/cashflow）**：`pit_ann_date = COALESCE(f_ann_date, ann_date)`
  - 原则：优先 `f_ann_date`（实际公告日期），回退到 `ann_date`
- **fina_indicator / fina_express / fina_forecast**：`pit_ann_date = ann_date`
  - forecast 可额外保留 `first_ann_date` 作为“首次披露日期”辅助分析，但不建议替代 `ann_date` 作主键，除非后续验证其 PIT 语义更强

### 2.2 去重规则（同一版本键内的重复）

对每张源表建议以“版本键 + 最新写入时间”去重（即便当前快照未出现，也建议做防御）：

- 版本键建议：
  - income/balance/cashflow：`(ts_code, end_date, pit_ann_date)`
  - indicator/express/forecast：`(ts_code, end_date, ann_date)`
- 选择规则（建议优先级）：
  1. `update_time` 最大（最新入库/更新）
  2. 若仍并列：保留数值字段非空更多的一条（可用 `num_nonnull_cols` 打分）
  3. 若仍并列：稳定排序（例如按主键全量字段做 deterministic tie-break）

### 2.3 版本选择：PIT“时间轴” vs “最新快照”

建议区分两类消费：

1. **PIT 时间轴（推荐用于因子/回测）**  
   输出保留多版本：同一 `report_period=end_date` 可以对应多个 `pit_ann_date`。  
   PIT 窗口字段：
   - `query_start_date = pit_ann_date`
   - `query_end_date = next(pit_ann_date) - 1 day`（同 ts_code，下一个不同 pit_ann_date 推导）

2. **最新快照（用于看板/最新财务画像）**  
   在消费侧或另建 MV 做压缩：
   - 对每个 `(ts_code, end_date)` 仅取 `pit_ann_date` 最新的一条（并以 `update_time` 作 tie-break）

## 3. 对 features MV 的影响评估（待评审后改动）

### 3.1 需要优先处理的 MV

当前 `features` 中存在“以 ann_date 作为 PIT 生效日期”的实现（以三大报表为主）。  
建议评审通过后，优先把以下 MV 切换到 `pit_ann_date` 口径：
- `alphahome/features/recipes/mv/stock/stock_income_quarterly.py`
- `alphahome/features/recipes/mv/stock/stock_balance_quarterly.py`
- （可选一致性补强）`alphahome/features/recipes/mv/stock/stock_cashflow_quarterly.py`：当前已使用 `COALESCE(f_ann_date, ann_date)`，但仍需确认是否应保留“多版本时间轴”或仅保留最新版本

### 3.2 风险与收益

- **收益**
  - 消除 `(ts_code,end_date,ann_date)` 重复导致的窗口歧义
  - 与文档“f_ann_date 优先使用”对齐，降低未来函数风险
  - 让 PIT 时间轴能表达“更正/重述”的真实发生时点
- **风险**
  - MV 输出行数会变化（ann_date→f_ann_date 会“拆开”版本）
  - 下游若假设 ann_date 唯一，可能需要联动调整 join key（建议显式使用 `query_start_date/query_end_date/report_period`）

## 4. 附录：本次调研 SQL（可复用）

### 4.1 统计重复组

```sql
-- dup_groups(ts_code,end_date)
SELECT COUNT(*) FILTER (WHERE cnt > 1) AS dup_groups, MAX(cnt) AS max_dup
FROM (
  SELECT ts_code, end_date, COUNT(*) AS cnt
  FROM rawdata.fina_income
  WHERE ts_code IS NOT NULL AND end_date IS NOT NULL
  GROUP BY ts_code, end_date
) g;

-- dup_groups(ts_code,end_date,ann_date)
SELECT COUNT(*) FILTER (WHERE cnt > 1) AS dup_groups, MAX(cnt) AS max_dup
FROM (
  SELECT ts_code, end_date, ann_date, COUNT(*) AS cnt
  FROM rawdata.fina_income
  WHERE ts_code IS NOT NULL AND end_date IS NOT NULL AND ann_date IS NOT NULL
  GROUP BY ts_code, end_date, ann_date
) g;

-- dup_groups(ts_code,end_date,f_ann_date)（三大报表）
SELECT COUNT(*) FILTER (WHERE cnt > 1) AS dup_groups, MAX(cnt) AS max_dup
FROM (
  SELECT ts_code, end_date, f_ann_date, COUNT(*) AS cnt
  FROM rawdata.fina_income
  WHERE ts_code IS NOT NULL AND end_date IS NOT NULL AND f_ann_date IS NOT NULL
  GROUP BY ts_code, end_date, f_ann_date
) g;
```

### 4.2 抽样查看重复明细（以 income 为例）

```sql
WITH d AS (
  SELECT ts_code, end_date, ann_date, COUNT(*) AS cnt
  FROM rawdata.fina_income
  GROUP BY ts_code, end_date, ann_date
  HAVING COUNT(*) > 1
  ORDER BY cnt DESC, ts_code, end_date, ann_date
  LIMIT 5
)
SELECT
  i.ts_code, i.end_date, i.ann_date, i.f_ann_date, i.update_time,
  i.revenue, i.n_income_attr_p
FROM rawdata.fina_income i
JOIN d USING (ts_code, end_date, ann_date)
ORDER BY i.ts_code, i.end_date, i.ann_date, i.f_ann_date;
```

