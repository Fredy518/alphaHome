#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Fix common historical schema issues in tushare finance tables:

1) update_time was previously created as VARCHAR(10) in some environments.
   Smart incremental update inserts a datetime into update_time, which can
   trigger StringDataRightTruncationError on COPY.

2) Some finance tables used ts_code as VARCHAR(10). This script widens it.

Usage:
  python scripts/database/migrate_tushare_finance_update_time_and_ts_code.py
  python scripts/database/migrate_tushare_finance_update_time_and_ts_code.py --dry-run
  python scripts/database/migrate_tushare_finance_update_time_and_ts_code.py --schema tushare
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import create_async_manager


FINANCE_TABLES_TS_CODE_LEN = {
    "fina_balancesheet": 15,
    "fina_cashflow": 15,
    "fina_disclosure": 15,
    "fina_express": 15,
    "fina_forecast": 15,
    "fina_income": 15,
    "fina_indicator": 15,
    "fina_mainbz": 20,
}


async def table_exists(conn, schema: str, table: str) -> bool:
    sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
    """
    return bool(await conn.fetchval(sql, schema, table))


async def get_column_info(conn, schema: str, table: str, column: str) -> dict | None:
    sql = """
        SELECT
            column_name,
            data_type,
            udt_name,
            character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
    """
    row = await conn.fetchrow(sql, schema, table, column)
    return dict(row) if row else None


def safe_text_to_timestamp_using_expr(col: str) -> str:
    qcol = f'"{col}"'
    return f"""
        CASE
            WHEN {qcol} IS NULL OR btrim({qcol}) = '' THEN NULL
            WHEN {qcol} ~ '^[0-9]{{8}}$' THEN to_date({qcol}, 'YYYYMMDD')::timestamp
            WHEN {qcol} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN to_date({qcol}, 'YYYY-MM-DD')::timestamp
            WHEN {qcol} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}[ T][0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}(\\.[0-9]{{1,6}})?$' THEN {qcol}::timestamp
            ELSE NULL
        END
    """.strip()


async def get_relation_oid(conn, schema: str, name: str) -> int | None:
    sql = """
        SELECT c.oid
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
    """
    return await conn.fetchval(sql, schema, name)


async def get_direct_dependent_relations(conn, parent_oid: int):
    sql = """
        SELECT DISTINCT
            v.oid AS oid,
            nv.nspname AS schema,
            v.relname AS name,
            v.relkind AS kind
        FROM pg_depend d
        JOIN pg_rewrite r ON r.oid = d.objid
        JOIN pg_class v ON v.oid = r.ev_class
        JOIN pg_namespace nv ON nv.oid = v.relnamespace
        WHERE d.refobjid = $1
          AND v.relkind IN ('v','m')
    """
    return await conn.fetch(sql, parent_oid)


async def collect_dependent_views_closure(conn, base_schema: str, base_table: str):
    base_oid = await get_relation_oid(conn, base_schema, base_table)
    if base_oid is None:
        return base_oid, {}, {}

    # Graph: parent_oid -> set(child_oid)
    edges: dict[int, set[int]] = {base_oid: set()}
    rels: dict[int, dict] = {}
    queue = [base_oid]
    visited = {base_oid}

    while queue:
        current = queue.pop(0)
        edges.setdefault(current, set())
        for row in await get_direct_dependent_relations(conn, current):
            oid = row["oid"]
            kind = row["kind"]
            if isinstance(kind, (bytes, bytearray)):
                kind = kind.decode("utf-8")
            kind = str(kind)

            edges[current].add(oid)
            rels[oid] = {
                "oid": oid,
                "schema": row["schema"],
                "name": row["name"],
                "kind": kind,
            }

            edges.setdefault(oid, set())
            if oid not in visited:
                visited.add(oid)
                queue.append(oid)

    # Extract only views. If any matview appears, we abort (recreating it safely is not trivial).
    matviews = [r for r in rels.values() if r["kind"] == "m"]
    if matviews:
        names = ", ".join([f'{r["schema"]}.{r["name"]}' for r in matviews])
        raise RuntimeError(
            f"Found dependent materialized views: {names}. "
            "Please handle them manually before altering base table types."
        )

    # Post-order DFS for drop order (dependents first)
    drop_order: list[int] = []
    seen: set[int] = set()

    def dfs(node: int):
        if node in seen:
            return
        seen.add(node)
        for child in edges.get(node, set()):
            dfs(child)
        if node != base_oid and rels.get(node, {}).get("kind") == "v":
            drop_order.append(node)

    dfs(base_oid)

    # Fetch view definitions once
    view_defs: dict[int, str] = {}
    for oid in drop_order:
        view_defs[oid] = await conn.fetchval(
            "SELECT pg_get_viewdef($1::oid, true)", oid
        )

    return base_oid, rels, {"drop_order": drop_order, "view_defs": view_defs}


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def plan_migrations(conn, schema: str, table: str, desired_ts_code_len: int):
    full = f'{quote_ident(schema)}.{quote_ident(table)}'
    ddls: list[str] = []

    # update_time
    info_ut = await get_column_info(conn, schema, table, "update_time")
    if not info_ut:
        ddls.append(
            f'ALTER TABLE {full} ADD COLUMN IF NOT EXISTS "update_time" '
            f"TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP;"
        )
    else:
        dt = (info_ut.get("data_type") or "").lower()
        if dt != "timestamp without time zone":
            if dt in ("character varying", "text", "character"):
                using_expr = safe_text_to_timestamp_using_expr("update_time")
                ddls.append(
                    f'ALTER TABLE {full} ALTER COLUMN "update_time" '
                    f"TYPE TIMESTAMP WITHOUT TIME ZONE USING ({using_expr});"
                )
            elif dt == "date":
                ddls.append(
                    f'ALTER TABLE {full} ALTER COLUMN "update_time" '
                    f'TYPE TIMESTAMP WITHOUT TIME ZONE USING ("update_time"::timestamp);'
                )
            else:
                ddls.append(
                    f'ALTER TABLE {full} ALTER COLUMN "update_time" '
                    f'TYPE TIMESTAMP WITHOUT TIME ZONE USING ("update_time"::timestamp);'
                )

    # ts_code length
    info_tc = await get_column_info(conn, schema, table, "ts_code")
    if info_tc:
        dt = (info_tc.get("data_type") or "").lower()
        existing_len = info_tc.get("character_maximum_length")
        if dt == "character varying" and isinstance(existing_len, int) and existing_len < desired_ts_code_len:
            ddls.append(
                f'ALTER TABLE {full} ALTER COLUMN "ts_code" TYPE VARCHAR({desired_ts_code_len});'
            )

    return ddls


async def main(schema: str, dry_run: bool) -> int:
    db_url = get_database_url()
    if not db_url:
        print("Error: DATABASE_URL not configured")
        return 1

    db = create_async_manager(db_url)
    await db.connect()
    try:
        for table, ts_code_len in FINANCE_TABLES_TS_CODE_LEN.items():
            print(f"\n[{schema}.{table}]")
            async with db.transaction() as conn:
                if not await table_exists(conn, schema, table):
                    print("  [SKIP] table not found")
                    continue

                ddls = await plan_migrations(conn, schema, table, ts_code_len)
                if not ddls:
                    print("  [OK] no changes")
                    continue

                # Collect dependent views to temporarily drop/recreate
                _, rels, dep = await collect_dependent_views_closure(conn, schema, table)
                drop_order: list[int] = dep.get("drop_order", [])
                view_defs: dict[int, str] = dep.get("view_defs", {})

                dropped = 0
                for oid in drop_order:
                    r = rels[oid]
                    fq = f'{quote_ident(r["schema"])}.{quote_ident(r["name"])}'
                    sql = f"DROP VIEW IF EXISTS {fq};"
                    if dry_run:
                        print(f"  [DRY-RUN] {sql}")
                    else:
                        await conn.execute(sql)
                        dropped += 1

                applied = 0
                for sql in ddls:
                    if dry_run:
                        print(f"  [DRY-RUN] {sql}")
                    else:
                        await conn.execute(sql)
                        applied += 1

                # Recreate views in reverse drop order (base-first)
                recreated = 0
                for oid in reversed(drop_order):
                    r = rels[oid]
                    fq = f'{quote_ident(r["schema"])}.{quote_ident(r["name"])}'
                    view_def = view_defs.get(oid)
                    if not view_def:
                        raise RuntimeError(f"Missing view definition for {fq}")
                    if dry_run:
                        print(f"  [DRY-RUN] RECREATE VIEW {fq}")
                    else:
                        sql = f"CREATE OR REPLACE VIEW {fq} AS {view_def};"
                        await conn.execute(sql)
                        recreated += 1

                if not dry_run:
                    print(f"  [UPDATED] applied {applied} DDL, dropped/recreated {dropped}/{recreated} views")
    finally:
        await db.close()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate tushare finance update_time + ts_code varchar length"
    )
    parser.add_argument("--schema", default="tushare", help="Target schema name")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show SQL without executing"
    )
    args = parser.parse_args()

    raise SystemExit(asyncio.run(main(schema=args.schema, dry_run=args.dry_run)))
