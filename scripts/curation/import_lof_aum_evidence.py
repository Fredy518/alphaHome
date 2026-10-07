#!/usr/bin/env python
"""Archive downloaded Tushare report evidence without rewriting daily NAV vintages."""
import argparse
import gzip
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import psycopg2
from psycopg2.extras import Json, execute_values
from alphahome.common.config_manager import get_database_url
from alphahome.curation.deepseek_candidate_client import sha256_json
from alphahome.curation.exchange_fund_sources import LOF_UNIVERSE_SQL


def main():
    p = argparse.ArgumentParser(__doc__)
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--as-of", type=date.fromisoformat, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--expected-plan-hash")
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with gzip.open(args.input, "rt", encoding="utf-8") as f:
        downloaded = json.load(f)
    conn = psycopg2.connect(get_database_url())
    with conn:
        with conn.cursor() as q:
            q.execute(LOF_UNIVERSE_SQL)
            codes = {r[0] for r in q.fetchall()}
            rows, rejected = [], []
            for row in downloaded:
                period, announced = date.fromisoformat(
                    row["nav_date"]
                ), date.fromisoformat(row["ann_date"])
                if (
                    row["ts_code"] not in codes
                    or not period < announced <= args.as_of
                    or not row["net_asset"] > 0
                ):
                    rejected.append(row)
                    continue
                rows.append(
                    (
                        row["ts_code"],
                        period,
                        announced,
                        row["net_asset"],
                        "tushare.fund_nav",
                        sha256_json(row),
                        row,
                    )
                )
            q.execute(
                "SELECT fund_code,report_date,ann_date,source_hash FROM fund_pool_on.lof_aum_report_evidence ORDER BY 1,2,3,4"
            )
            before = q.fetchall()
            payload = {
                "contract": "lof_report_evidence_import_v1",
                "as_of": str(args.as_of),
                "download_hash": sha256_json(downloaded),
                "rows_hash": sha256_json(rows),
                "existing_hash": sha256_json(before),
                "accepted": len(rows),
                "rejected": len(rejected),
                "existing_count": len(before),
                "rejection_reason": "unknown_exchange_LOF_or_report_announcement_not_after_NAV_date",
            }
            plan_hash = sha256_json(payload)
            if not args.execute:
                (args.output_dir / "reports_plan.json").write_text(
                    json.dumps({**payload, "plan_hash": plan_hash}, indent=2),
                    encoding="utf-8",
                )
                (args.output_dir / "reports_rejected.json").write_text(
                    json.dumps(rejected, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                print(json.dumps({**payload, "plan_hash": plan_hash}))
                return
            if args.expected_plan_hash != plan_hash or not rows:
                raise RuntimeError("report import plan changed or no valid reports")
            q.execute(
                "LOCK TABLE fund_pool_on.lof_aum_report_evidence IN SHARE ROW EXCLUSIVE MODE"
            )
            q.execute(
                "SELECT fund_code,report_date,ann_date,source_hash FROM fund_pool_on.lof_aum_report_evidence ORDER BY 1,2,3,4"
            )
            if sha256_json(q.fetchall()) != payload["existing_hash"]:
                raise RuntimeError("report archive changed under lock")
            execute_values(
                q,
                "INSERT INTO fund_pool_on.lof_aum_report_evidence (fund_code,report_date,ann_date,net_asset,source,source_hash,source_payload) VALUES %s ON CONFLICT DO NOTHING",
                [(*r[:-1], Json(r[-1])) for r in rows],
            )
            q.execute("SELECT count(*) FROM fund_pool_on.lof_aum_report_evidence")
            result = {
                "status": "success",
                "plan_hash": plan_hash,
                "total_rows": q.fetchone()[0],
            }
    conn.close()
    (args.output_dir / "reports_result.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
