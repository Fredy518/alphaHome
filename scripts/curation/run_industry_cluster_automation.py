#!/usr/bin/env python
# ruff: noqa: E402
"""Maintain a stable industry library and publish monthly clusters from AlphaDB."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from alphahome.common.config_manager import get_database_url
from alphahome.curation.industry_clusters import automation as auto


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("schema", "library", "clusters", "retire"))
    parser.add_argument(
        "--as-of", default=datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--expected-plan-hash")
    parser.add_argument("--index-code")
    parser.add_argument("--reason")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    url = get_database_url()
    day = datetime.fromisoformat(args.as_of).date()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.command == "retire":
        if not args.execute or not args.index_code or not args.reason:
            parser.error(
                "retire requires --execute, --index-code and a confirmed --reason"
            )
        result = auto.retire_index(url, args.index_code, args.reason)
    elif args.command == "schema":
        if args.execute:
            if not args.expected_plan_hash:
                parser.error("schema execution requires --expected-plan-hash")
            result = auto.apply_schema(url, args.expected_plan_hash)
        else:
            with auto._connection(url) as conn:
                conn.set_session(readonly=True)
                result = auto.schema_plan(conn)
            (args.output_dir / "schema.sql").write_text(
                auto.SCHEMA_SQL, encoding="utf-8"
            )
    else:
        plan_file = args.output_dir / (args.command + "_plan.json")
        if args.execute:
            if not args.expected_plan_hash:
                parser.error("execution requires --expected-plan-hash")
            plan = json.loads(plan_file.read_text(encoding="utf-8"))
            if plan.get("run_date") != day.isoformat():
                parser.error("date differs from saved plan")
            if args.command == "library":
                result = auto.execute_library_plan(url, plan, args.expected_plan_hash)
            else:
                result = auto.execute_cluster_plan(url, plan, args.expected_plan_hash)
        else:
            with auto._connection(url) as conn:
                conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
                result = (
                    auto.build_library_plan
                    if args.command == "library"
                    else auto.build_cluster_plan
                )(conn, day)
            plan_file.write_text(
                json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
            )
    filename = args.command + ("_result.json" if args.execute else "_preview.json")
    (args.output_dir / filename).write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    display = {
        k: v
        for k, v in result.items()
        if k not in {"members", "seed_members", "pending", "config"}
    }
    print(json.dumps(display, ensure_ascii=False, indent=2))
    return (
        0
        if result.get("status") in {"ready", "success", "no_op", "expected_no_data"}
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
