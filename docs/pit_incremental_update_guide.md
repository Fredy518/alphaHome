# PIT 数据增量更新指南

## 当前入口

PIT 生产脚本位于：

```text
scripts/production/data_updaters/pit/
```

统一入口：

```bash
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
```

历史文档中的 `scripts/pit/*` 路径已废弃。

## 支持对象

| target | 表 | 说明 |
| --- | --- | --- |
| `balance` | `pgs_factors.pit_balance_quarterly` | 资产负债表 PIT 数据 |
| `income` | `pgs_factors.pit_income_quarterly` | 利润表 PIT 数据 |
| `financial_indicators` | `pgs_factors.pit_financial_indicators` | 基于 income/balance 计算的 PIT 财务指标 |
| `industry_classification` | `pgs_factors.pit_industry_classification` | 行业分类 PIT 快照 |
| `all` | 全部 | 按依赖关系执行 |

## 常用命令

```bash
# 日常增量
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental

# 只更新依赖表
python scripts/production/data_updaters/pit/pit_data_update_production.py --target balance income --mode incremental

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
FROM pgs_factors.pit_income_quarterly
UNION ALL
SELECT 'balance', MAX(ann_date), COUNT(*)
FROM pgs_factors.pit_balance_quarterly
UNION ALL
SELECT 'financial_indicators', MAX(ann_date), COUNT(*)
FROM pgs_factors.pit_financial_indicators;
```

也可以用管理器：

```bash
python scripts/production/data_updaters/pit/pit_income_quarterly_manager.py --status
python scripts/production/data_updaters/pit/pit_income_quarterly_manager.py --validate
```

## 注意事项

- PIT 表用于避免未来函数，更新逻辑必须以 `ann_date` / `obs_date` 为时点边界。
- 全量回填前建议备份 `pgs_factors` 相关表。
- 同时运行多个 PIT 脚本可能造成锁等待或重复写入，日常调度优先使用统一协调器。
- 财务指标表依赖利润表和资产负债表，修复依赖表后需要重算指标。
