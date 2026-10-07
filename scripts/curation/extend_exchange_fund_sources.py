#!/usr/bin/env python
"""Explicit LOF facts migration/refresh; frozen plan, targeted backup, no DDL on refresh."""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import psycopg2
from psycopg2.extras import RealDictCursor
from alphahome.common.config_manager import get_database_url
from alphahome.curation.deepseek_candidate_client import sha256_json
from alphahome.curation.exchange_fund_sources import UNIFIED_FACTS_SQL
from alphahome.curation.etf_candidate_master import ENRICHED_VIEW_SQL
from alphahome.curation.etf_usable_pool import SCHEMA_SQL as USABLE_SCHEMA
from alphahome.features.recipes.mv.fund.lof_product_facts_current import (
    LOFProductFactsCurrentMV,
)


def main():
    p = argparse.ArgumentParser(__doc__)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--expected-plan-hash")
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(get_database_url())
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as q:
            q.execute(
                "SELECT to_regclass('features.mv_lof_product_facts_current') AS existing"
            )
            existing = q.fetchone()["existing"]
            sql = (
                ("" if existing else LOFProductFactsCurrentMV.create_sql + ";")
                + "\n"
                + ";\n".join(LOFProductFactsCurrentMV().get_post_create_sqls())
                + ";\n"
                + UNIFIED_FACTS_SQL
                + ENRICHED_VIEW_SQL
                + USABLE_SCHEMA
            )
            payload = {
                "contract": "exchange_fund_sources_v1",
                "existing_lof_mv": existing,
                "sql_hash": sha256_json(sql),
            }
            plan_hash = sha256_json(payload)
            if not args.execute:
                (args.output_dir / "schema.sql").write_text(sql, encoding="utf-8")
                (args.output_dir / "schema_plan.json").write_text(
                    json.dumps({**payload, "plan_hash": plan_hash}, indent=2),
                    encoding="utf-8",
                )
                print(json.dumps({**payload, "plan_hash": plan_hash}))
                return
            if args.expected_plan_hash != plan_hash:
                raise RuntimeError("schema plan changed")
            for view in (
                "etf_candidate_master_current_enriched",
                "etf_usable_pool_source_current",
                "etf_usable_pool_screening_current",
                "etf_usable_pool_current",
            ):
                q.execute(
                    "SELECT pg_get_viewdef(%s::regclass,true) definition",
                    ("fund_pool_on." + view,),
                )
                (args.output_dir / (view + "_before.sql")).write_text(
                    q.fetchone()["definition"], encoding="utf-8"
                )
            baseline = {}
            for table in (
                "etf_candidate_master_batch",
                "etf_candidate_master_snapshot",
                "etf_candidate_confirmation_audit",
                "etf_usable_pool_batch",
                "etf_usable_pool_snapshot",
                "etf_usable_pool_monthly_batch",
                "etf_usable_pool_monthly_snapshot",
            ):
                q.execute(
                    "SELECT to_jsonb(t) AS row FROM fund_pool_on."
                    + table
                    + " t ORDER BY to_jsonb(t)::text"
                )
                rows = [r["row"] for r in q.fetchall()]
                baseline[table] = {"rows": len(rows), "hash": sha256_json(rows)}
                with gzip.open(
                    args.output_dir / (table + "_before.json.gz"),
                    "wt",
                    encoding="utf-8",
                ) as f:
                    json.dump(rows, f, ensure_ascii=False, default=str)
            (args.output_dir / "baseline.json").write_text(
                json.dumps(baseline, indent=2), encoding="utf-8"
            )
            q.execute("SET LOCAL lock_timeout='5s'")
            q.execute(
                "SELECT pg_advisory_xact_lock(hashtext('alphahome_etf_candidate_master_write_v1'))"
            )
            q.execute(sql)
            q.execute("REFRESH MATERIALIZED VIEW features.mv_lof_product_facts_current")
            q.execute(
                "SELECT product_type,count(*) AS rows,count(*) FILTER(WHERE core_facts_complete) AS complete FROM features.exchange_fund_product_facts_current GROUP BY 1"
            )
            result = {
                "status": "success",
                "plan_hash": plan_hash,
                "facts": [dict(r) for r in q.fetchall()],
            }
    conn.close()
    (args.output_dir / "schema_result.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
