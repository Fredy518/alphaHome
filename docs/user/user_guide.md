# AlphaHome 用户指南

## 使用前准备

1. 按 [安装指南](../setup/installation.md) 安装项目。
2. 按 [配置指南](../setup/configuration.md) 配置 `~/.alphahome/config.json`。
3. 确认 PostgreSQL 可连接，Tushare Token 已配置。

```bash
python -c "from alphahome.common.db_manager import create_sync_manager; db=create_sync_manager(); print(db.test_connection())"
```

## GUI

启动：

```bash
python run.py
```

GUI 主要用于内部数据采集任务：

| 标签页 | 用途 |
| --- | --- |
| 数据采集 | 查看、筛选、选择已注册 fetch 任务 |
| 任务运行与状态 | 选择 SMART / MANUAL / FULL 模式并运行任务 |
| 任务日志 | 查看任务生命周期、批次执行、验证和保存日志 |
| Features 更新 | 查看和执行部分 feature/MV 更新能力 |
| 存储与设置 | 查看数据库信息、加载/保存 Tushare Token、测试连接 |

执行模式：

| 模式 | 说明 |
| --- | --- |
| SMART | 根据目标表最新日期自动增量更新，并按 `smart_lookback_days` 回看 |
| MANUAL | 使用用户指定的开始/结束日期 |
| FULL | 从任务 `default_start_date` 到当前日期全量拉取 |

## 生产脚本

生产脚本需要在仓库根目录执行。

### 数据采集

```bash
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3 --dry-run
```

该脚本会自动发现所有 `task_type="fetch"` 的任务，并按数据源做并发控制。

### PIT 数据

```bash
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
python scripts/production/data_updaters/pit/pit_data_update_production.py --target balance income --mode incremental
python scripts/production/data_updaters/pit/pit_data_update_production.py --target financial_indicators --mode full
```

PIT 当前支持：

- `pit_balance_quarterly`
- `pit_income_quarterly`
- `pit_financial_indicators`
- `pit_industry_classification`

`financial_indicators` 依赖 `income` 和 `balance`，同批执行时脚本会保护依赖顺序。

### P/G 因子

```bash
python scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py --dates 2026-05-08
python scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py --dates 2026-05-08

python scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation.py --start_year 2020 --end_year 2024 --workers 10
python scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation_quarterly.py --start_year 2020 --end_year 2024 --workers 16
```

G 因子依赖同日期已有 P 因子数据。

### Features / MV

```bash
python scripts/initialize_materialized_views.py
python scripts/features_init.py --help
python scripts/features_validate_pit.py --help
```

当前 features 目录以 `features/cards/*.yaml` 和 `features/recipes/` 为准。

### DolphinDB / Hikyuu 5min

```bash
python scripts/generate_hikyuu_5min_tickers.py --hikyuu-dir E:/stock --output-dir scripts/tickers
python -m alphahome.integrations.dolphindb.cli init-kline5m
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min --codes-file scripts/tickers/all.txt --incremental
.\scripts\import_all_hikyuu_to_ddb.ps1 -Incremental
```

## 任务系统

采集任务统一走以下生命周期：

```text
BaseTask.execute()
  -> _pre_execute()
  -> _fetch_data()
  -> process_data()
  -> _validate_data()
  -> _save_data()
  -> _post_execute()
```

数据源任务分层：

```text
BaseTask
└── FetcherTask
    ├── TushareTask
    ├── AkShareTask
    ├── TinySoftTask
    └── ExcelTask
```

新增任务请参考 [新任务开发指南](../new_task_development_guide.md)。

## 研究侧数据访问

`AlphaDataTool` 是当前推荐的轻量研究入口：

```python
from research.tools.context import ResearchContext

with ResearchContext() as context:
    data = context.data_tool.get_stock_data(
        ["000001.SZ"],
        "2024-01-01",
        "2024-12-31",
    )
```

更多说明见 [providers README](../../alphahome/providers/README.md)。

## 故障排查

- 配置问题：先确认 `~/.alphahome/config.json` 路径和 JSON 格式。
- 数据库问题：先用 `create_sync_manager().test_connection()` 验证。
- Tushare 问题：检查 token、接口权限、限流日志。
- 大批量脚本问题：降低 `--workers` 或任务级 `concurrent_limit`。
- 数据缺失：用 MANUAL 模式指定日期补拉，再检查任务日志和目标表主键。
