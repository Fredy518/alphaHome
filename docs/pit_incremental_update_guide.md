# PIT 数据增量更新指南

## 当前入口

PIT 现在是一等任务体系，任务注册在 `alphahome.pit.tasks`，GUI、生产脚本和后续调度都通过 `UnifiedTaskFactory` 执行同一批 `task_type="pit"` 任务。

兼容生产脚本仍位于：

```text
scripts/production/data_updaters/pit/
```

统一入口：

```bash
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
```

历史文档中的 `scripts/pit/*` 路径已废弃。旧入口路径保留，但内部已改为调用 PIT task registry，不再维护独立 manager 清单。

## 存储边界

- PIT 标准化落库统一使用 `pit` schema。
- P/G 因子结果统一使用 `factors` schema。
- `pgs_factors` 仅保留同名兼容视图，供旧查询过渡使用，不再作为生产写入目标。
- 若需要重放 schema 分拆或修复兼容视图，运行：

```bash
python scripts/database/split_pgs_factors_schema.py
```

## 支持对象

| target | 表 | 说明 |
| --- | --- | --- |
| `balance` | `pit.pit_balance_quarterly` | 资产负债表 PIT 数据 |
| `income` | `pit.pit_income_quarterly` | 利润表 PIT 数据 |
| `cashflow` | `pit.pit_cashflow_quarterly` | 现金流量表 PIT 数据，v1 仅正式财报 `report` |
| `financial_indicators` | `pit.pit_financial_indicators` | 基于 income/balance 计算的标准 PIT 财务指标，仅使用 `report` / `express` |
| `industry_classification` | `pit.pit_industry_classification` | 行业分类 PIT 快照 |
| `all` | 全部 | 按依赖关系执行 |

## Forecast 边界

- `pit.pit_income_quarterly` 可以保留 `forecast` 行，用于记录业绩预告披露本身。
- `forecast` 行必须带有 horizon 治理字段：`forecast_horizon_days`、`forecast_horizon_bucket`、`forecast_horizon_status`、`is_future_target`、`is_usable_forecast`。
- 可用性阈值：`0-190` 天为近端预告，`191-370` 天为长端预告，超过 `370` 天标记为 `horizon_outlier` 且 `is_usable_forecast=false`。
- `pit.pit_financial_indicators` 是标准会计指标层，不承载 `forecast`。历史遗留的 `forecast` 指标行应清理，后续计算入口也会默认只读取 `report` / `express`。

## 常用命令

```bash
# 日常增量
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental

# 只更新依赖表
python scripts/production/data_updaters/pit/pit_data_update_production.py --target balance income --mode incremental

# 只更新财务三表 PIT
python scripts/production/data_updaters/pit/pit_data_update_production.py --target income balance cashflow --mode incremental

# 只重算财务指标
python scripts/production/data_updaters/pit/pit_data_update_production.py --target financial_indicators --mode full

# 并行执行无依赖冲突的任务
python scripts/production/data_updaters/pit/pit_data_update_production.py --target balance income industry_classification --mode incremental --parallel --workers 3

# 调试日志
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --log-level DEBUG
```

`financial_indicators` 依赖 `income` 和 `balance`。如果同批包含依赖任务，协调器会禁用并行以保证顺序。

## 单表管理器

必要时可直接运行单表管理器：

```bash
python scripts/production/data_updaters/pit/pit_income_quarterly_manager.py --mode incremental --days 30
python scripts/production/data_updaters/pit/pit_balance_quarterly_manager.py --mode incremental --days 30
python scripts/production/data_updaters/pit/pit_cashflow_quarterly_manager.py --mode incremental --days 30
python scripts/production/data_updaters/pit/pit_financial_indicators_manager.py --mode incremental --days 30
python scripts/production/data_updaters/pit/pit_industry_classification_manager.py --mode incremental --months 3
```

常用参数：

| 参数 | 说明 |
| --- | --- |
| `--mode incremental` | 增量更新 |
| `--mode full-backfill` / `--mode full` | 历史回填或全量重算 |
| `--mode single-backfill` | 单股回填，部分管理器支持 |
| `--start-date` / `--end-date` | 指定公告日或观察日范围 |
| `--days` / `--months` | 增量检查窗口 |
| `--batch-size` | 分批大小 |
| `--status` | 查看表状态 |
| `--validate` | 执行数据校验 |
| `--ts-code` | 单股回填代码 |

## 调度建议

### Windows 任务计划程序

程序：

```text
python
```

参数：

```text
scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
```

起始位置：

```text
E:\CodePrograms\alphaHome
```

### Linux/macOS cron

```bash
0 2 * * * cd /path/to/alphaHome && python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
```

## 验证

```sql
SELECT 'income' AS table_name, MAX(ann_date) AS latest_date, COUNT(*) AS rows
FROM pit.pit_income_quarterly
UNION ALL
SELECT 'balance', MAX(ann_date), COUNT(*)
FROM pit.pit_balance_quarterly
UNION ALL
SELECT 'cashflow', MAX(ann_date), COUNT(*)
FROM pit.pit_cashflow_quarterly
UNION ALL
SELECT 'financial_indicators', MAX(ann_date), COUNT(*)
FROM pit.pit_financial_indicators;
```

也可以用管理器：

```bash
python scripts/production/data_updaters/pit/pit_income_quarterly_manager.py --status
python scripts/production/data_updaters/pit/pit_income_quarterly_manager.py --validate
```

## GUI 管理

GUI 新增 `PIT 管理` 页签，位于 `数据采集` 与 `特征更新` 之间。

- `刷新`：读取已注册 PIT 任务、最近审计快照和当前表状态。
- `增量更新` / `全量回填`：复用统一任务运行服务，运行状态继续显示在 `任务运行与状态` 页。
- `只审计`：不写业务 PIT 表，只写 `pit.pit_audit_snapshot`。
- `查看缺口`：按报告期展示财务 PIT 覆盖率矩阵。
- `单股诊断`：按股票代码展示 raw-vs-pit 报告期缺口、财务三表预期缺口、缺口原因和最近 PIT 记录。

## 缺口治理

单股诊断对财务类 PIT 使用 income、balance、cashflow 三表的报告期并集作为“预期报告期”基线。如果某张表在预期窗口内缺少报告期，审计服务会继续探查任务 contract 中声明的源表，并同时探查常见 rawdata 镜像表，例如 `tushare.fina_cashflow` 对应 `rawdata.fina_cashflow`。

缺口原因码：

| 原因码 | 含义 | 处理建议 |
| --- | --- | --- |
| `source_relation_missing` | 源表或 rawdata 镜像表不存在 | 先修复采集表/视图注册 |
| `source_missing` | 源表存在，但该股票该报告期没有源记录 | 标记为上游数据缺口；必要时换源或人工补源 |
| `source_not_eligible` | 源表有记录，但不符合 PIT 任务口径 | 检查 `report_type` 等准入过滤是否合理 |
| `source_empty_after_field_filter` | 源表有合格记录，但核心业务字段全空 | 作为源数据质量问题处理 |
| `pit_build_gap` | 源表存在有效记录，但 PIT 未落库 | 优先重跑对应 PIT 任务或检查 upsert/清洗逻辑 |

示例：`002549.SZ` 的 `2022-06-30` 现金流缺口属于 `source_missing`；`tushare.fina_cashflow` 与 `rawdata.fina_cashflow` 对该报告期均无记录，因此不是 `pit_cashflow_quarterly` 构建漏写。

## 审计快照

审计结果统一写入：

```text
pit.pit_audit_snapshot
```

核心字段包括 `snapshot_time`、`task_name`、`output_table`、`latest_pit_time`、`row_count`、`coverage_rate`、`gap_count`、`status`、`details_json`。

覆盖率默认口径：

- 财务类按 `end_date` 报告期统计当前上市股票覆盖。
- 行业类按 `obs_date` 月末快照统计当前上市股票覆盖。
- raw-vs-pit 缺口保存在 `details_json.raw_vs_pit`，不混入业务 PIT 表。

## 注意事项

- PIT 表用于避免未来函数，更新逻辑必须以 `ann_date` / `obs_date` 为时点边界。
- 全量回填前建议备份 `pit` 相关表；`pgs_factors` 仅保留兼容视图，不作为新数据落库位置。
- 同时运行多个 PIT 脚本可能造成锁等待或重复写入，日常调度优先使用统一协调器。
- 财务指标表依赖利润表和资产负债表，修复依赖表后需要重算指标。
