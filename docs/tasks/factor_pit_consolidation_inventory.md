# Factor/PIT Consolidation Inventory

> Phase 0 inventory, generated on 2026-06-07.
> Scope: inventory and contracts only. No runtime code was changed in this phase.

## Baseline

- Command: `python -m pytest tests/unit/ -m "not requires_db and not requires_api" -q`
- Result: `317 passed, 1 warning`
- Worktree note before changes: existing untracked `docs/tasks/factor_pit_engine_consolidation_goal.md` and `tmp/` were present and are unrelated to this inventory.

## Current Fork Diff Summary

### P/G Factor Calculators

| Area | Research path | Scripts path | Diff / status | Authority decision |
| --- | --- | --- | --- | --- |
| P calculator | `research/pgs_factor/processors/production_p_factor_calculator.py` | `scripts/production/factor_calculators/p_factor/production_p_factor_calculator.py` | `git diff --no-index --stat`: 1 file, 293 insertions, 584 deletions from research to scripts; AST similarity ~0.571 | Use research-side numeric implementation as authority; merge scripts-side standalone DB/config construction as package adapter behavior. |
| G calculator | `research/pgs_factor/processors/production_g_factor_calculator.py` | no standalone scripts calculator | scripts G runners import research calculator directly | Use research-side implementation as authority; replace scripts imports with package import. |
| P runner | `research/pgs_factor/production_p_factor_runner.py` | `scripts/production/factor_calculators/p_factor/production_p_factor_runner.py` | CLI args match | Keep scripts runner as thin compatibility entry; research runner imports package. |
| G runner | `research/pgs_factor/production_g_factor_runner.py` | no scripts runner equivalent | research runner plus scripts parallel variants | Keep research runner as wrapper; scripts variants call package engine. |

P calculator differences to preserve:

- Research-only methods include `_get_mvp_precomputed_indicators_pit`, `_calculate_p_factors_from_mvp_indicators_pit`, `_save_p_factors_mvp`, `_calculate_p_score_vectorized_mvp`, `_validate_calculation_results`, `check_mvp_data_availability`, and `check_mvp_data_availability_pit`.
- Scripts-only methods include `_get_precomputed_indicators_pit`, `_calculate_p_factors_from_indicators_pit`, `_save_p_factors`, `_calculate_p_score_vectorized`, and `_is_financial_stock_by_code`.
- Research file contains two `calculate_p_factors_batch_pit` definitions; Python keeps the latter date-range signature. The package implementation must keep the effective date-range API and move the older list-based batch behavior under a non-colliding helper only if still needed.
- The scripts constructor currently owns `ConfigManager`/`DBManager` bootstrap with no `ResearchContext`. The package calculator must support both injected research context and standalone DB manager/config construction.

### PIT Managers

| Area | Research path | Scripts path | Similarity / status | Authority decision |
| --- | --- | --- | --- | --- |
| `PITConfig` | `research/pit_data/base/pit_config.py` | `scripts/production/data_updaters/pit/base/pit_config.py` | identical | Use scripts-side import paths as production authority, no algorithm change. |
| `PITTableManager` | `research/pit_data/base/pit_table_manager.py` | `scripts/production/data_updaters/pit/base/pit_table_manager.py` | ~0.984 | Use scripts-side version; preserve relative import compatibility. |
| `PITBalanceQuarterlyManager` | `research/pit_data/pit_balance_quarterly_manager.py` | `scripts/production/data_updaters/pit/pit_balance_quarterly_manager.py` | ~0.997 | Use scripts-side version. |
| `PITIncomeQuarterlyManager` | `research/pit_data/pit_income_quarterly_manager.py` | `scripts/production/data_updaters/pit/pit_income_quarterly_manager.py` | ~0.998 | Use scripts-side version. |
| `PITFinancialIndicatorsManager` | `research/pit_data/pit_financial_indicators_manager.py` | `scripts/production/data_updaters/pit/pit_financial_indicators_manager.py` | ~0.992 | Use scripts-side version. |
| `PITIndustryClassificationManager` | `research/pit_data/pit_industry_classification_manager.py` | `scripts/production/data_updaters/pit/pit_industry_classification_manager.py` | ~0.993 | Use scripts-side version. |
| `FinancialIndicatorsCalculator` | `research/pit_data/calculators/financial_indicators_calculator.py` | `scripts/production/data_updaters/pit/calculators/financial_indicators_calculator.py` | identical | Move to package once; both sides import package. |

PIT directory diff note: whole-directory `git diff --no-index --stat` is noisy because scripts side contains production entry files, `.bat`, and `__pycache__` artifacts while research side contains README/deprecation/test files. Core manager/calculator differences are small.

## Script Disposition

### Factor Scripts

All retained scripts become thin entrypoints or compatibility modules. CLI flags and defaults remain part of the public contract.

| Path | Disposition | Compatibility contract |
| --- | --- | --- |
| `scripts/production/factor_calculators/p_factor/production_p_factor_calculator.py` | Replace with thin import/export shim | `ProductionPFactorCalculator()` remains importable without explicit context. |
| `scripts/production/factor_calculators/p_factor/production_p_factor_runner.py` | Thin entry | Preserve `--start-date`, `--end-date`, `--mode`, log/dry-run/validation flags. |
| `scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py` | Thin entry | Preserve `--dates` and `--log-level`. |
| `scripts/production/factor_calculators/p_factor/p_factor_parallel_by_year.py` | Thin entry | Preserve `--start_year`, `--end_year`, `--worker_id`, `--total_workers`. |
| `scripts/production/factor_calculators/p_factor/p_factor_parallel_by_quarter.py` | Thin entry | Preserve `--worker_id`, `--total_workers`, `--quarter`, `--start_quarter`, `--end_quarter`. |
| `scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation.py` | Thin launcher | Preserve `--start_year`, `--end_year`, `--workers`, `--delay`. |
| `scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation_quarterly.py` | Thin launcher | Preserve quarter/year range validation, `--workers`, `--delay`. |
| `scripts/production/factor_calculators/p_factor/*.bat` | Keep / update command target only if needed | Existing Windows launch behavior preserved. |
| `scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py` | Thin entry | Preserve `--dates` and `--log-level`. |
| `scripts/production/factor_calculators/g_factor/g_factor_parallel_by_year.py` | Thin entry | Preserve `--start_year`, `--end_year`, `--worker_id`, `--total_workers`. |
| `scripts/production/factor_calculators/g_factor/g_factor_parallel_by_quarter.py` | Thin entry | Preserve `--worker_id`, `--total_workers`, `--quarter`, `--start_quarter`, `--end_quarter`. |
| `scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation.py` | Thin launcher | Preserve `--start_year`, `--end_year`, `--workers`, `--delay`. |
| `scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation_quarterly.py` | Thin launcher | Preserve quarter/year range validation, `--workers`, `--delay`. |
| `scripts/production/factor_calculators/g_factor/*.bat` | Keep / update command target only if needed | Existing Windows launch behavior preserved. |
| `scripts/production/factor_calculators/batch_calculate_missing_factors.py` | Thin entry | Preserve `--start-date`, `--end-date`, `--log-level`, `--dry-run`. |
| `scripts/production/factor_calculators/batch_calculate_recent_missing_factors.py` | Thin entry | Preserve `--months`, `--log-level`. |

### PIT Scripts

PIT implementation moves to `alphahome.pit` because PIT managers are data/PIT updaters rather than factor definitions. Scripts and research modules should import from this package.

| Path | Disposition | Compatibility contract |
| --- | --- | --- |
| `scripts/production/data_updaters/pit/base/*` | Move logic to `alphahome/pit/base`; scripts path becomes shim if import compatibility is needed | `PITConfig`, `PITTableManager` remain importable. |
| `scripts/production/data_updaters/pit/calculators/*` | Move to `alphahome/pit/calculators`; scripts path becomes shim | `FinancialIndicatorsCalculator` remains importable. |
| `scripts/production/data_updaters/pit/pit_balance_quarterly_manager.py` | Thin entry/import shim | Preserve manager class and CLI flags: `--mode`, `--start-date`, `--end-date`, `--days`, `--batch-size`, `--ts-code`, `--status`, `--validate`. |
| `scripts/production/data_updaters/pit/pit_income_quarterly_manager.py` | Thin entry/import shim | Preserve manager class and CLI flags: `--mode`, `--start-date`, `--end-date`, `--days`, `--batch-size`, `--status`, `--validate`, `--ts-code`. |
| `scripts/production/data_updaters/pit/pit_financial_indicators_manager.py` | Thin entry/import shim | Preserve manager class and existing CLI behavior. |
| `scripts/production/data_updaters/pit/pit_industry_classification_manager.py` | Thin entry/import shim | Preserve manager class and existing CLI behavior. |
| `scripts/production/data_updaters/pit/pit_data_update_production.py` | Thin coordinator entry | Preserve `--target`, `--mode`, `--parallel`, `--workers`, `--log-level`. |
| `scripts/production/data_updaters/pit/database/*.sql` | Move or reference from package resources | SQL DDL remains available to package managers. |
| `scripts/production/data_updaters/pit/start_pit_data_update.bat` | Keep / update target only if needed | Existing Windows launch behavior preserved. |

### One-Off Scripts To Archive In Phase 1

These should be moved with `git mv` to `archive/scripts/` before implementation cleanup.

| Path | Disposition |
| --- | --- |
| `scripts/analysis/audit_financial_table_versions.py` | Archive |
| `scripts/analysis/calendar_effect_analysis.py` | Archive |
| `scripts/analysis/compare_g_p_factor_stock_counts.py` | Archive |
| `scripts/analysis/investigate_g_factor_discrepancy.py` | Archive |
| `scripts/analysis/monitor_g_factor_progress.py` | Archive |
| `scripts/analysis/p_factor_coverage_analysis.py` | Archive |
| `scripts/analysis/tinysoft_fina_field_calibration.py` | Archive |
| `scripts/analysis/tinysoft_industry_field_calibration.py` | Archive |
| `scripts/analysis/tinysoft_suspend_field_calibration.py` | Archive |
| `scripts/maintenance/backfill_stock_limitlist_pre2020.py` | Archive |
| `scripts/maintenance/fix_g_factor_rankings.bat` | Archive |
| `scripts/maintenance/fix_g_factor_rankings_and_scores.py` | Archive |
| `scripts/maintenance/fix_stock_limitup_reason_ts_code.py` | Archive |
| `scripts/maintenance/fix_tushare_fund_share_sz_trade_date.py` | Archive |
| `scripts/maintenance/migrate_db_name.py` | Archive |
| `scripts/maintenance/recreate_stock_sharefloat_schedule.py` | Archive |
| `scripts/maintenance/README_G_FACTOR_FIX.md` | Archive with its G-factor fix script |
| `scripts/database/delete_g_factor_data.py` | Archive |
| `scripts/database/migrate_tushare_finance_update_time_and_ts_code.py` | Archive |
| `scripts/migrate_existing_tables_to_rawdata.py` | Archive |
| `scripts/run_tinysoft_infoarray_full.py` | Archive |
| `scripts/run_empty_tinysoft_full_opi.py` | Archive as same root-level Tinysoft one-off family |
| `scripts/production/database/migrate_bse_code_mapping.py` | Archive |
| `scripts/production/database/README.md` | Archive with BSE migration entry |

Scripts not in this archive batch:

- `scripts/database/alphadb_nas_*.py`, `scripts/database/README_NAS_LOGICAL_SYNC.md`, and `scripts/database/create_missing_functions.sql` are not included in the requested one-off glob and are documented as NAS sync/database support.
- `scripts/features_init.py`, `scripts/features_validate_pit.py`, `scripts/initialize_materialized_views.py`, `scripts/check_coverage.py`, production Tushare updater scripts, and shared verification scripts are outside this task's archive scope.

## Package Interface Contract

### `alphahome.factors.core`

`PFactorCalculator`

- Constructor: `PFactorCalculator(context=None, db_manager=None, config=None, database_url=None)`
- Backward-compatible alias: `ProductionPFactorCalculator = PFactorCalculator`
- Required methods:
  - `calculate_p_factors_pit(as_of_date: str, stock_codes: list[str]) -> dict`
  - `calculate_p_factors_batch_pit(start_date: str, end_date: str, mode: str | None = None) -> dict`
  - `detect_execution_mode(start_date: str, end_date: str) -> str`
  - `generate_calculation_dates(start_date: str, end_date: str, mode: str) -> list[str]`
  - `_filter_missing_dates(dates: list[str]) -> list[str]`
  - `check_mvp_data_availability(calc_date: str, stock_codes: list[str]) -> dict`
  - `check_mvp_data_availability_pit(as_of_date: str, stock_codes: list[str]) -> dict`

`GFactorCalculator`

- Constructor: `GFactorCalculator(context=None, db_manager=None, config=None, database_url=None)`
- Backward-compatible alias: `ProductionGFactorCalculator = GFactorCalculator`
- Required methods:
  - `calculate_g_factors_pit(as_of_date: str, stock_codes: list[str]) -> dict`
  - date/batch helpers should be provided by `FactorEngine` rather than duplicated in calculator scripts.

Implementation constraint: calculators must accept injected `ResearchContext` for research modules and standalone DB/config construction for production scripts. Numeric formulas and SQL predicates are moved, not changed.

### `alphahome.factors.pipelines`

`FactorEngineConfig`

- Fields: `factor_types`, `dates`, `start_date`, `end_date`, `start_year`, `end_year`, `quarter`, `start_quarter`, `end_quarter`, `worker_id`, `total_workers`, `workers`, `mode`, `missing_mode`, `months_back`, `dry_run`, `log_level`.
- Semantics:
  - `dates`: explicit date mode; mutually exclusive with year/quarter range modes.
  - `start_year/end_year`: generate full-year date ranges compatible with current yearly scripts.
  - `quarter/start_quarter/end_quarter`: generate quarter date ranges compatible with current quarterly scripts.
  - `worker_id/total_workers`: process shard mode for existing worker scripts.
  - `workers`: launcher/parallel orchestration mode for existing start scripts.
  - `missing_mode`: `none`, `batch_missing`, or `recent_missing`.

`FactorEngine`

- Constructor: `FactorEngine(config: FactorEngineConfig, p_calculator=None, g_calculator=None, context=None, db_manager=None)`
- Required methods:
  - `resolve_dates() -> list[str]`
  - `resolve_work_items() -> list[FactorWorkItem]`
  - `filter_missing_dates(factor_type: str, dates: list[str]) -> list[str]`
  - `run() -> dict`
  - `run_worker() -> dict`
  - `launch_workers(script_kind: str) -> int`
- Pure logic to unit test: date generation, quarter parsing/range expansion, explicit date validation, mode detection, missing-date filtering, worker sharding, and argument mutual exclusion.

### `alphahome.pit`

Core exports:

- `PITConfig`
- `PITTableManager`
- `PITBalanceQuarterlyManager`
- `PITIncomeQuarterlyManager`
- `PITFinancialIndicatorsManager`
- `PITIndustryClassificationManager`
- `PITDataUpdateCoordinator`
- `FinancialIndicatorsCalculator`

Required behavior:

- Preserve current CLI manager modes: `full-backfill`, `incremental`, `single-backfill` where currently available.
- Preserve production coordinator modes: `incremental` and `full`; targets `balance`, `income`, `financial_indicators`, `industry_classification`, and `all`.
- Preserve DDL availability for PIT manager table creation/index helpers.
- Research-side `research/pit_data/*` files become import shims or thin wrappers so old research imports do not fork implementation.

## Phase 0 Risks / Notes

- P research calculator's duplicate method name must be resolved during extraction to avoid silently dropping the older list-based batch API.
- Current scripts import research modules by mutating `sys.path` and using dynamic imports; package migration should remove those patterns from runtime paths while preserving CLI execution from repo root.
- PIT production scripts and research scripts are highly similar but use different relative imports. Moving to `alphahome.pit` should be mechanical, but CLI `--help` and import tests are required after shimming.
- DB/API-dependent validation is intentionally out of Phase 0. It remains a manual follow-up after package consolidation.
