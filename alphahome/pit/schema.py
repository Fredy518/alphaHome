"""Generate reviewed PIT DDL without connecting or migrating during execution."""

from pathlib import Path


def render_schema_sql(task_names=None):
    from .pit_data_update_production import PITDataUpdateCoordinator
    from .base.pit_config import PITConfig
    from .pit_income_quarterly_manager import PITIncomeQuarterlyManager
    from .pit_earnings_surprise_annual_manager import PITEarningsSurpriseAnnualManager

    contracts = PITDataUpdateCoordinator._registered_contracts()
    names = sorted(task_names or contracts)
    unknown = set(names) - set(contracts)
    if unknown:
        raise ValueError(f"Unknown PIT tasks: {sorted(unknown)}")
    directory = Path(__file__).with_name("database")
    statements = ["BEGIN;", "SET LOCAL lock_timeout = '5s';", "CREATE SCHEMA IF NOT EXISTS pit;"]
    seen = set()
    for name in names:
        manager = contracts[name].resolve_manager_class()()
        table = manager.table_name
        if table in seen:
            continue
        seen.add(table)
        ddl_path = directory / f"create_{table}_table.sql"
        if ddl_path.exists():
            statements.append(ddl_path.read_text(encoding="utf-8"))
        else:
            statements.append(manager._generate_create_table_sql(PITConfig.PIT_SCHEMA, table))
        additional = {}
        if table == "pit_income_quarterly":
            additional = {**PITIncomeQuarterlyManager.ANNUAL_ACTUAL_COLUMNS, **PITIncomeQuarterlyManager.FORECAST_HORIZON_COLUMNS}
        elif table == "pit_earnings_surprise_annual":
            additional = PITEarningsSurpriseAnnualManager.OUTPUT_MIGRATION_COLUMNS
        for column, sql_type in additional.items():
            statements.append(f'ALTER TABLE pit."{table}" ADD COLUMN IF NOT EXISTS "{column}" {sql_type};')
        if table == "pit_industry_fttm_monthly":
            statements.append("ALTER TABLE pit.pit_industry_fttm_monthly ALTER COLUMN aggregation_version TYPE varchar(64);")
        if table in {"pit_income_quarterly", "pit_balance_quarterly"}:
            # Remove only the documented old key; existing duplicates abort the transaction.
            statements.append(f'ALTER TABLE pit."{table}" DROP CONSTRAINT IF EXISTS "{table}_ts_code_end_date_ann_date_key";')
            statements.append(f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='pit.{table}'::regclass AND conname='{table}_uniq_with_source') THEN ALTER TABLE pit.{table} ADD CONSTRAINT {table}_uniq_with_source UNIQUE (ts_code,end_date,ann_date,data_source); END IF; END $$;")
            kind = "income" if "income" in table else "balance"
            statements.append((directory / f"create_pit_{kind}_indexes.sql").read_text(encoding="utf-8"))
    statements.append((directory / "create_pit_updated_at_triggers.sql").read_text(encoding="utf-8"))
    statements.append("COMMIT;")
    return "\n\n".join(statements) + "\n"


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description="Render PIT schema migration SQL; does not execute SQL")
    parser.add_argument("--task", action="append")
    args = parser.parse_args(argv)
    print(render_schema_sql(args.task))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
