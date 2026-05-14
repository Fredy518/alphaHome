# 生产脚本说明

`scripts/production/` 保存日常可运行的生产脚本。所有命令默认在仓库根目录执行。

## 目录

```text
scripts/production/
├── refresh_market_timing_dependencies.py
├── config/
│   └── tushare_update_config.yaml
├── data_updaters/
│   ├── tushare/
│   │   └── data_collection_smart_update_production.py
│   └── pit/
│       ├── pit_data_update_production.py
│       ├── pit_balance_quarterly_manager.py
│       ├── pit_income_quarterly_manager.py
│       ├── pit_financial_indicators_manager.py
│       ├── pit_industry_classification_manager.py
│       ├── base/
│       ├── calculators/
│       └── database/
├── database/
│   └── migrate_bse_code_mapping.py
├── factor_calculators/
│   ├── batch_calculate_missing_factors.py
│   ├── batch_calculate_recent_missing_factors.py
│   ├── p_factor/
│   └── g_factor/
└── shared/
    └── verify_stats.py
```

## 数据采集更新

```bash
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3 --max_retries 5 --retry_delay 10
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 2 --dry-run
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --log_level DEBUG
```

参数：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--workers` | 3 | 脚本级并发任务数 |
| `--max_retries` | 3 | 任务失败后的脚本级重试次数 |
| `--retry_delay` | 5 | 脚本级重试等待秒数 |
| `--log_level` | INFO | 日志级别 |
| `--dry-run` | false | 只分析将执行的任务，不实际运行 |

脚本会发现所有 `task_type="fetch"` 的注册任务，覆盖 Tushare、AkShare、Tinysoft、Excel 等数据源。具体 API 并发仍受任务级 `concurrent_limit` 和数据源自身限制约束。

## PIT 更新

```bash
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
python scripts/production/data_updaters/pit/pit_data_update_production.py --target balance income --mode incremental
python scripts/production/data_updaters/pit/pit_data_update_production.py --target financial_indicators --mode full
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --log-level DEBUG
```

参数：

| 参数 | 说明 |
| --- | --- |
| `--target` | `balance`、`income`、`financial_indicators`、`industry_classification`、`all` |
| `--mode` | `incremental` 或 `full` |
| `--parallel` | 允许无依赖冲突任务并行 |
| `--workers` | 最大并发进程数 |
| `--log-level` | 日志级别 |

直接运行单表管理器：

```bash
python scripts/production/data_updaters/pit/pit_income_quarterly_manager.py --mode incremental --days 30
python scripts/production/data_updaters/pit/pit_balance_quarterly_manager.py --mode incremental --days 30
python scripts/production/data_updaters/pit/pit_financial_indicators_manager.py --mode incremental --days 30
python scripts/production/data_updaters/pit/pit_industry_classification_manager.py --mode incremental --months 3
```

## P/G 因子

### 单日或指定日期补算

```bash
python scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py --dates 2026-05-08
python scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py --dates 2026-05-08
```

可一次传多个日期：

```bash
python scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py --dates 2026-05-08 2026-05-11
```

### 缺失日期批量补算

```bash
python scripts/production/factor_calculators/batch_calculate_missing_factors.py --start-date 2024-01-01 --end-date 2024-12-31 --dry-run
python scripts/production/factor_calculators/batch_calculate_recent_missing_factors.py --months 3
```

### 年度/季度并行

```bash
python scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation.py --start_year 2020 --end_year 2024 --workers 10
python scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation_quarterly.py --start_year 2020 --end_year 2024 --workers 16

python scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation.py --start_year 2020 --end_year 2024 --workers 10
python scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation_quarterly.py --start_year 2020 --end_year 2024 --workers 16
```

G 因子依赖同日期已有 P 因子数据。

## 市场择时依赖刷新

```bash
python scripts/production/refresh_market_timing_dependencies.py --profile alphasniper --mode check
python scripts/production/refresh_market_timing_dependencies.py --profile betanavigator --mode refresh
python scripts/production/refresh_market_timing_dependencies.py --profile betanavigator --mode check --json
```

当前 profile：

- `alphasniper`
- `betanavigator`

## 数据库维护

北交所代码映射迁移：

```bash
python scripts/production/database/migrate_bse_code_mapping.py --dry-run
python scripts/production/database/migrate_bse_code_mapping.py --dry-run --verbose
python scripts/production/database/migrate_bse_code_mapping.py --tables stock_daily stock_dividend
```

执行实际迁移前必须先跑 `--dry-run` 并备份。

## 运行建议

- 生产脚本都应从仓库根目录执行。
- 大规模更新先降低 `--workers` 验证，再逐步提高。
- 全量 PIT、全量因子和数据库维护脚本执行前先备份。
- 同一目标表不要并发运行多个写入脚本。
- 日志异常先保留命令、时间、配置和完整 traceback，便于复盘。
