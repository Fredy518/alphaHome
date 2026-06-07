# Factor/PIT Consolidation Report

> Completed on 2026-06-07. Refactor moved implementation only; no factor/PIT numeric formula change was intentionally introduced.

## Commit Summary

| Phase | Commit | Scope |
| --- | --- | --- |
| 0 | `7403b43` | Inventory and public contract report |
| 1 | `0b700ed` | Archive one-off scripts and remove public docs entries |
| 2 | `07747ea` | Move P/G calculators into `alphahome.factors.core`; add `FactorEngine` tests |
| 3 | `dec912a` | Thin factor scripts and research runners |
| 4 | `ddc42d5` | Move PIT managers/calculator/DDL into `alphahome.pit`; thin scripts/research imports |

## Implementation Boundary

| Domain | True implementation now lives in | Compatibility paths |
| --- | --- | --- |
| P factor | `alphahome/factors/core/p_factor_calculator.py` | `research/pgs_factor/processors/production_p_factor_calculator.py`, `scripts/production/factor_calculators/p_factor/production_p_factor_calculator.py` |
| G factor | `alphahome/factors/core/g_factor_calculator.py` | `research/pgs_factor/processors/production_g_factor_calculator.py` |
| Factor orchestration | `alphahome/factors/pipelines/factor_engine.py`, `alphahome/factors/pipelines/cli.py` | all factor scripts under `scripts/production/factor_calculators/` |
| PIT managers | `alphahome/pit/*.py`, `alphahome/pit/base/` | `scripts/production/data_updaters/pit/*.py`, `research/pit_data/*.py` |
| PIT financial calculator | `alphahome/pit/calculators/financial_indicators_calculator.py` | scripts/research calculator wrappers |
| PIT DDL | `alphahome/pit/database/*.sql` | package resource path used by managers |

## Line Count Change

| Area | Before | After |
| --- | --- | --- |
| P factor calculators | research ~1195 + scripts ~904 duplicated lines | package P calculator 1024 lines; scripts/research are wrappers |
| G factor calculators | research ~1084 lines, scripts imported research | package G calculator 928 lines; research is wrapper |
| Factor runners/helpers | 12+ scripts with local date/quarter/year/missing logic | `FactorEngine` 410 lines + `cli.py` 343 lines; scripts are argparse wrappers |
| PIT income manager | research ~2200 + scripts ~2204 duplicated lines | package manager 1905 lines; scripts/research are wrappers |
| PIT balance manager | research ~1142 + scripts ~1146 duplicated lines | package manager 988 lines; scripts/research are wrappers |
| PIT financial indicators manager | research ~653 + scripts ~658 duplicated lines | package manager 529 lines; scripts/research are wrappers |
| PIT industry manager | research ~562 + scripts ~566 duplicated lines | package manager 511 lines; scripts/research are wrappers |
| PIT financial calculator | research/scripts 1319 duplicated lines | package calculator 1145 lines; scripts/research are wrappers |

## Archived Scripts

Moved to `archive/scripts/` with `git mv`:

- `analysis/`: 9 analysis/calibration/factor investigation scripts
- `maintenance/`: G-factor fixes, stock/fund fixes, historical backfill, recreate/migrate helpers and G-factor fix README/BAT
- `database/`: G-factor delete and Tushare finance migration helpers
- root one-offs: `migrate_existing_tables_to_rawdata.py`, Tinysoft full-run helpers
- `production/database/`: BSE code mapping migration README/script

Active scripts intentionally retained:

- NAS sync scripts under `scripts/database/`
- feature/MV scripts under `scripts/`
- production Tushare updater
- factor and PIT compatibility entrypoints

## Retained Commands

Representative commands preserved:

```bash
python scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py --dates 2026-05-08
python scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py --dates 2026-05-08
python scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation.py --start_year 2020 --end_year 2024 --workers 10
python scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation_quarterly.py --start_year 2020 --end_year 2024 --workers 16
python scripts/production/factor_calculators/batch_calculate_missing_factors.py --start-date 2024-01-01 --end-date 2024-12-31 --dry-run
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
python scripts/production/data_updaters/pit/pit_income_quarterly_manager.py --mode incremental --days 30
```

## Verification

- Unit baseline before refactor: `317 passed, 1 warning`
- Unit suite after factor/PIT consolidation: `328 passed, 1 warning`
- Added tests: `tests/unit/test_factor_engine.py` with 11 tests covering date generation, quarter parsing/range expansion, worker sharding, mutual exclusion validation, missing-date filtering, recent-missing determinism, and mock calculator execution.
- `python -c "import alphahome"` passes.
- `python -m py_compile` over `alphahome`, `scripts`, and relevant research wrappers passes.
- Preserved factor and PIT script `--help` checks pass.

## Remaining DB Validation

Not run in this refactor because DB/API credentials and end-to-end writes are outside the automated gate:

- P factor one-date dry run and one limited real calculation against `pgs_factors.p_factor`.
- G factor one-date run after confirming same-date P data exists.
- PIT `--target all --mode incremental` with a small `--days`/manual limit path where available.
- Single-stock PIT balance/income/financial-indicators backfill for one known liquid stock.
- Verify PIT DDL loading from `alphahome/pit/database/*.sql` on an initialized AlphaDB.
- Confirm no row-count or score distribution drift on a sampled date set versus pre-refactor outputs.

## Notes

- Scripts and research files remain import-compatible, but they no longer contain true calculator/manager implementations.
- `research/hikyuu_source/` was not touched.
- Existing untracked `docs/tasks/factor_pit_engine_consolidation_goal.md` and `tmp/` were left untouched.
