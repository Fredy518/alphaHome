"""Certify retained-source equality inside the financial MV refresh snapshot."""
from __future__ import annotations

import hashlib

from alphahome.common.plan_inspection import qualified_relation

FINANCIAL_RECIPES = frozenset({"stock_income_quarterly", "stock_balance_quarterly", "stock_cashflow_quarterly"})


async def certify_financial_snapshot(connection, recipe):
    """Full bidirectional equality, including source version hashes and windows.

    This certifies the retained inputs of this committed refresh only. It never
    certifies vendor completeness, the system's first receipt, or another domain.
    The caller must retain REPEATABLE READ and commit this evidence with the MV.
    """
    if recipe.name not in FINANCIAL_RECIPES:
        raise ValueError("No financial snapshot consumption contract for this recipe")
    isolation = await connection.fetchval("SHOW transaction_isolation")
    if isolation != "repeatable read":
        raise RuntimeError("Financial consumption certification requires repeatable read")
    create_sql = recipe.get_create_sql()
    prefix = f"CREATE MATERIALIZED VIEW {recipe.full_name} AS "
    if not create_sql.startswith(prefix):
        raise ValueError("Unsupported financial projection SQL")
    query = create_sql[len(prefix):].rstrip().rstrip(";")
    target = qualified_relation(recipe.full_name)
    # All ranking and LEAD windows are partitioned by stock. Exhaustive stock
    # ranges therefore preserve every report/event window while keeping the
    # EXCEPT ALL sorts small. Include target-only stocks so extras cannot hide.
    relations = [*recipe.source_tables, recipe.full_name]
    collations = [await connection.fetchval(
        "SELECT attcollation::integer FROM pg_attribute WHERE attrelid=to_regclass($1) "
        "AND attname='ts_code' AND NOT attisdropped", relation
    ) for relation in relations]
    if None in collations or len(set(collations)) != 1:
        raise RuntimeError("Financial stock ranges require a shared ts_code collation")
    null_keys = await connection.fetchval(
        f"SELECT COUNT(*) FROM {target} WHERE ts_code IS NULL OR ann_date IS NULL OR report_period IS NULL"
    )
    if null_keys:
        raise RuntimeError("Financial target has null event keys")
    stock_query = " UNION ".join(
        f"SELECT ts_code FROM {qualified_relation(relation)} WHERE ts_code IS NOT NULL"
        for relation in relations
    )
    stocks = [row["ts_code"] for row in await connection.fetch(
        f"SELECT ts_code FROM ({stock_query}) codes ORDER BY ts_code"
    )]
    marker = "WHERE ts_code IS NOT NULL AND end_date IS NOT NULL"
    if query.count(marker) != len(recipe.source_tables):
        raise ValueError("Unsupported stock-partitioned financial projection")
    ranged_query = query.replace(marker, marker + " AND ts_code >= $1 AND ts_code <= $2")
    proof = {"expected_rows": 0, "actual_rows": 0, "missing_rows": 0, "extra_rows": 0}
    batches = []
    for offset in range(0, len(stocks), 200):
        first, last = stocks[offset], stocks[min(offset + 199, len(stocks) - 1)]
        row = await connection.fetchrow(f"""
            WITH expected AS MATERIALIZED ({ranged_query}),
            actual AS MATERIALIZED (
                SELECT * FROM {target} WHERE ts_code >= $1 AND ts_code <= $2
            ),
            missing AS (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual),
            extra AS (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
            SELECT (SELECT COUNT(*) FROM expected) AS expected_rows,
                   (SELECT COUNT(*) FROM actual) AS actual_rows,
                   (SELECT COUNT(*) FROM missing) AS missing_rows,
                   (SELECT COUNT(*) FROM extra) AS extra_rows
        """, first, last)
        counts = dict(row)
        batches.append({"first_stock": first, "last_stock": last, **counts})
        for key in proof:
            proof[key] += counts[key]
        if counts["missing_rows"] or counts["extra_rows"]:
            raise RuntimeError("Financial snapshot source/output equality failed")
    if proof["actual_rows"] != await connection.fetchval(f"SELECT COUNT(*) FROM {target}"):
        raise RuntimeError("Financial stock ranges omitted target rows")
    if proof["missing_rows"] or proof["extra_rows"] or proof["expected_rows"] != proof["actual_rows"]:
        raise RuntimeError("Financial snapshot source/output equality failed")
    sources = {}
    for source in recipe.source_tables:
        sources[source] = dict(await connection.fetchrow(
            f"SELECT COUNT(*) AS rows, MAX(update_time) AS observed_update_time FROM {qualified_relation(source)}"
        ))
    boundary = await connection.fetchrow("""SELECT pg_current_snapshot()::text AS snapshot,
        pg_snapshot_xmin(pg_current_snapshot())::text AS snapshot_xmin,
        transaction_timestamp() AS snapshot_observed_at""")
    limits = await connection.fetchrow("""SELECT
        current_setting('statement_timeout') AS statement_timeout,
        current_setting('transaction_timeout') AS transaction_timeout,
        current_setting('lock_timeout') AS lock_timeout,
        current_setting('work_mem') AS work_mem,
        current_setting('temp_file_limit') AS temp_file_limit,
        current_setting('max_parallel_workers_per_gather') AS max_parallel_workers_per_gather""")
    return {
        "contract_version": "financial_mv_snapshot_v1",
        "scope": "all_retained_source_versions_qualified_by_recipe",
        "source_consumption": "verified",
        "isolation": "repeatable_read",
        "projection_sha256": hashlib.sha256(create_sql.encode("utf-8")).hexdigest(),
        "sources": sources,
        "boundary": dict(boundary),
        "execution_limits": dict(limits),
        "equality": proof,
        "exhaustive_stock_ranges": {"stock_count": len(stocks), "stocks_per_batch": 200,
                                    "batches": batches, "target_only_stocks_included": True},
        "system_first_receipt_verified": False,
        "vendor_historical_completeness_verified": False,
        "later_source_commits": "outside_this_snapshot_require_next_refresh",
    }
