#!/usr/bin/env python
# ruff: noqa: E402
"""Preview/apply ETF usable-pool schema or a frozen deterministic selection."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from alphahome.common.config_manager import get_database_url
from alphahome.curation.etf_usable_pool import (
    SCHEMA_SQL,
    apply_schema,
    build_usable_pool_plan,
    execute_usable_pool_plan,
    schema_plan,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("schema", "refresh"))
    parser.add_argument(
        "--run-date", default=datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--expected-plan-hash")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.execute and not args.expected_plan_hash:
        parser.error("--execute requires --expected-plan-hash")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    connection = psycopg2.connect(get_database_url())
    try:
        if args.command == "schema":
            plan = schema_plan(connection)
            connection.rollback()
            (args.output_dir / "schema.sql").write_text(SCHEMA_SQL, encoding="utf-8")
            result = (
                apply_schema(connection, args.expected_plan_hash)
                if args.execute
                else plan
            )
            output_name = "schema_result.json" if args.execute else "schema_plan.json"
        else:
            from datetime import date

            plan = build_usable_pool_plan(
                connection, run_date=date.fromisoformat(args.run_date)
            )
            connection.rollback()
            if args.execute:
                result = execute_usable_pool_plan(
                    connection, plan, expected_plan_hash=args.expected_plan_hash
                )
                output_name = "result.json"
            else:
                for name, value in (
                    ("plan.json", plan.payload),
                    ("screening_preview.json", plan.rows),
                ):
                    (args.output_dir / name).write_text(
                        json.dumps(value, ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8",
                    )
                result = plan.summary()
                output_name = "plan_summary.json"
        (args.output_dir / output_name).write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
