# Scripts Directory

`scripts/` 保存当前可直接运行的维护、生产和辅助脚本。所有命令默认在仓库根目录执行。

## 目录

```text
scripts/
├── features_init.py
├── features_validate_pit.py
├── initialize_materialized_views.py
├── analysis/
├── database/
├── maintenance/
└── production/
```

## 当前常用脚本

### Features / MV

```bash
python scripts/initialize_materialized_views.py
python scripts/features_init.py --help
python scripts/features_validate_pit.py --help
```

### 生产数据更新

```bash
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
```

### 因子补算

```bash
python scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py --dates 2026-05-08
python scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py --dates 2026-05-08
```

### 数据库维护

```bash
python scripts/production/database/migrate_bse_code_mapping.py --dry-run
python scripts/database/alphadb_nas_logical_sync.py --help
```

### 一次性维护

```bash
python scripts/maintenance/fix_stock_limitup_reason_ts_code.py --help
python scripts/maintenance/fix_tushare_fund_share_sz_trade_date.py --help
python scripts/maintenance/fix_g_factor_rankings_and_scores.py --help
```

## 目录说明

| 目录 | 用途 |
| --- | --- |
| `analysis/` | 数据口径校准、覆盖率分析、因子差异调查等分析脚本 |
| `database/` | AlphaDB / NAS 同步、恢复、逻辑复制和数据库级维护 |
| `maintenance/` | 一次性或低频数据修复 |
| `production/` | 日常生产脚本，详见 [production README](production/README.md) |

历史 `scripts/pit/` 入口已迁移到 `scripts/production/data_updaters/pit/`。
