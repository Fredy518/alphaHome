#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Split legacy pgs_factors tables into pit and factors schemas.

The legacy pgs_factors schema used to hold both normalized PIT panels and P/G
factor outputs. This migration moves base tables to their owning schemas and
keeps pgs_factors as compatibility views.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager
from alphahome.common.schema_names import (
    FACTOR_SCHEMA,
    FACTOR_TABLES,
    LEGACY_PGS_FACTORS_SCHEMA,
    PIT_SCHEMA,
    PIT_TABLES,
)


UPDATED_AT_TABLES = PIT_TABLES


def _qualified(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def _relation_kind(cursor, schema: str, table: str) -> str | None:
    cursor.execute(
        """
        SELECT c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _has_column(cursor, schema: str, table: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """,
        (schema, table, column),
    )
    return cursor.fetchone() is not None


def _row_count(cursor, schema: str, table: str) -> int | None:
    if _relation_kind(cursor, schema, table) not in {"r", "p", "v"}:
        return None
    cursor.execute(f"SELECT COUNT(*) FROM {_qualified(schema, table)}")
    return int(cursor.fetchone()[0])


def _move_table_if_needed(cursor, source_schema: str, target_schema: str, table: str, dry_run: bool) -> None:
    source_kind = _relation_kind(cursor, source_schema, table)
    target_kind = _relation_kind(cursor, target_schema, table)

    if target_kind in {"r", "p"}:
        return
    if source_kind in {"r", "p"}:
        print(f"move table: {source_schema}.{table} -> {target_schema}.{table}")
        if not dry_run:
            cursor.execute(f"ALTER TABLE {_qualified(source_schema, table)} SET SCHEMA {target_schema}")
        return
    if source_kind == "v":
        return
    print(f"skip missing base table: {source_schema}.{table}")


def _create_updated_at_function(cursor, dry_run: bool) -> None:
    sql = f"""
    CREATE OR REPLACE FUNCTION {PIT_SCHEMA}.update_updated_at_pit()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """
    print(f"ensure function: {PIT_SCHEMA}.update_updated_at_pit()")
    if not dry_run:
        cursor.execute(sql)


def _recreate_updated_at_triggers(cursor, dry_run: bool) -> None:
    for table in UPDATED_AT_TABLES:
        if _relation_kind(cursor, PIT_SCHEMA, table) not in {"r", "p"}:
            continue
        if not _has_column(cursor, PIT_SCHEMA, table, "updated_at"):
            continue
        trigger_name = f"trg_{table}_updated_at"
        print(f"recreate trigger: {PIT_SCHEMA}.{table}.{trigger_name}")
        if dry_run:
            continue
        cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {_qualified(PIT_SCHEMA, table)}")
        cursor.execute(
            f"""
            CREATE TRIGGER {trigger_name}
            BEFORE UPDATE ON {_qualified(PIT_SCHEMA, table)}
            FOR EACH ROW EXECUTE FUNCTION {PIT_SCHEMA}.update_updated_at_pit()
            """
        )


def _drop_legacy_updated_at_function(cursor, dry_run: bool) -> None:
    print(f"drop legacy function if unused: {LEGACY_PGS_FACTORS_SCHEMA}.update_updated_at_pit()")
    if not dry_run:
        cursor.execute(f"DROP FUNCTION IF EXISTS {LEGACY_PGS_FACTORS_SCHEMA}.update_updated_at_pit()")


def _create_compatibility_view(cursor, source_schema: str, table: str, dry_run: bool) -> None:
    target_kind = _relation_kind(cursor, source_schema, table)
    legacy_kind = _relation_kind(cursor, LEGACY_PGS_FACTORS_SCHEMA, table)

    if target_kind not in {"r", "p"}:
        return
    if legacy_kind in {"r", "p"}:
        raise RuntimeError(
            f"cannot create compatibility view while base table still exists: "
            f"{LEGACY_PGS_FACTORS_SCHEMA}.{table}"
        )
    print(f"compatibility view: {LEGACY_PGS_FACTORS_SCHEMA}.{table} -> {source_schema}.{table}")
    if not dry_run:
        if legacy_kind == "v":
            cursor.execute(f"DROP VIEW {_qualified(LEGACY_PGS_FACTORS_SCHEMA, table)}")
        cursor.execute(
            f"""
            CREATE VIEW {_qualified(LEGACY_PGS_FACTORS_SCHEMA, table)}
            AS SELECT * FROM {_qualified(source_schema, table)}
            """
        )


def _verify_no_legacy_base_tables(cursor, tables: Iterable[str]) -> None:
    leftovers = [
        table
        for table in tables
        if _relation_kind(cursor, LEGACY_PGS_FACTORS_SCHEMA, table) in {"r", "p"}
    ]
    if leftovers:
        raise RuntimeError(f"legacy base tables remain in {LEGACY_PGS_FACTORS_SCHEMA}: {leftovers}")


def _refresh_supporting_functions(cursor, dry_run: bool) -> None:
    sql_path = Path(__file__).with_name("create_missing_functions.sql")
    if not sql_path.exists():
        print(f"skip supporting functions refresh: missing {sql_path}")
        return
    print(f"refresh supporting functions: {sql_path.name}")
    if not dry_run:
        cursor.execute(sql_path.read_text(encoding="utf-8"))


def _print_counts(cursor, label: str) -> None:
    rows = []
    for schema, tables in (
        (PIT_SCHEMA, PIT_TABLES),
        (FACTOR_SCHEMA, FACTOR_TABLES),
        (LEGACY_PGS_FACTORS_SCHEMA, tuple(PIT_TABLES) + tuple(FACTOR_TABLES)),
    ):
        for table in tables:
            kind = _relation_kind(cursor, schema, table)
            count = _row_count(cursor, schema, table)
            if count is not None:
                rows.append((f"{schema}.{table}", kind, count))
    print(label)
    for name, kind, count in rows:
        kind_label = "view" if kind == "v" else "table"
        print(f"  {name} ({kind_label}): {count}")


def run_migration(dry_run: bool = False) -> None:
    database_url = ConfigManager().get_database_url()
    if not database_url:
        raise RuntimeError("database URL is not configured")

    db = DBManager(database_url, mode="sync")
    connection = db._get_sync_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {PIT_SCHEMA}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {FACTOR_SCHEMA}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {LEGACY_PGS_FACTORS_SCHEMA}")

            _print_counts(cursor, "before split counts:")

            for table in PIT_TABLES:
                _move_table_if_needed(cursor, LEGACY_PGS_FACTORS_SCHEMA, PIT_SCHEMA, table, dry_run)
            for table in FACTOR_TABLES:
                _move_table_if_needed(cursor, LEGACY_PGS_FACTORS_SCHEMA, FACTOR_SCHEMA, table, dry_run)

            _create_updated_at_function(cursor, dry_run)
            _recreate_updated_at_triggers(cursor, dry_run)
            _drop_legacy_updated_at_function(cursor, dry_run)

            for table in PIT_TABLES:
                _create_compatibility_view(cursor, PIT_SCHEMA, table, dry_run)
            for table in FACTOR_TABLES:
                _create_compatibility_view(cursor, FACTOR_SCHEMA, table, dry_run)

            _refresh_supporting_functions(cursor, dry_run)

            if not dry_run:
                _verify_no_legacy_base_tables(cursor, tuple(PIT_TABLES) + tuple(FACTOR_TABLES))
                _print_counts(cursor, "after split counts:")
                connection.commit()
            else:
                connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        db.close_sync()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Split legacy pgs_factors schema into pit and factors.")
    parser.add_argument("--dry-run", action="store_true", help="print intended actions and roll back")
    args = parser.parse_args(argv)

    run_migration(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
