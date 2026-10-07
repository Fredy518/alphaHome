#!/usr/bin/env python
# ruff: noqa: E402
"""Preview/apply monthly ETF pool schema and an immutable 2016+ reconstruction."""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from alphahome.common.config_manager import get_database_url
from alphahome.curation import etf_usable_pool_monthly as pool


def save(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )


def progress(value: dict) -> None:
    if value["month"][5:7] in ("01", "08", "12"):
        print(json.dumps({"progress": value}, ensure_ascii=False), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("schema", "backfill"))
    parser.add_argument("--start-month", default="2016-01")
    parser.add_argument("--end-month")
    parser.add_argument(
        "--as-of", default=datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--expected-plan-hash")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    if args.execute and not args.expected_plan_hash:
        parser.error("--execute requires --expected-plan-hash")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(get_database_url())
    try:
        if args.command == "schema":
            plan = pool.schema_plan(conn)
            conn.rollback()
            (args.output_dir / "schema.sql").write_text(
                pool.SCHEMA_SQL, encoding="utf-8"
            )
            result = (
                pool.apply_schema(conn, args.expected_plan_hash)
                if args.execute
                else plan
            )
            filename = "schema_result.json" if args.execute else "schema_plan.json"
        elif args.execute:
            payload = json.loads(
                (args.output_dir / "plan.json").read_text(encoding="utf-8")
            )
            with gzip.open(
                args.output_dir / "monthly_preview.jsonl.gz", "rt", encoding="utf-8"
            ) as source:
                months = [json.loads(line) for line in source]
            plan = pool.MonthlyPlan(payload, months)
            if (
                payload["start_month"] != args.start_month + "-01"
                or payload["as_of"] != args.as_of
                or args.end_month
                and payload["end_month"] != args.end_month + "-01"
            ):
                raise pool.MonthlyPoolError("CLI range differs from saved plan")
            result = pool.execute_monthly_plan(
                conn,
                plan,
                expected_plan_hash=args.expected_plan_hash,
                progress=progress,
            )
            filename = "result.json"
        else:
            conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
            plan = pool.build_monthly_plan(
                conn,
                start_month=date.fromisoformat(args.start_month + "-01"),
                end_month=(
                    date.fromisoformat(args.end_month + "-01")
                    if args.end_month
                    else None
                ),
                as_of=date.fromisoformat(args.as_of),
                progress=progress,
            )
            save(args.output_dir / "plan.json", plan.payload)
            with gzip.open(
                args.output_dir / "monthly_preview.jsonl.gz", "wt", encoding="utf-8"
            ) as target:
                for month in plan.months:
                    target.write(
                        json.dumps(month, ensure_ascii=False, separators=(",", ":"))
                        + "\n"
                    )
            save(
                args.output_dir / "monthly_summary.json",
                [
                    {
                        "maintenance_month": m["meta"]["maintenance_month"],
                        **m["meta"]["summary"],
                    }
                    for m in plan.months
                ],
            )
            result = plan.summary()
            filename = "plan_summary.json"
        save(args.output_dir / filename, result)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
