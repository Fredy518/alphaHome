#!/usr/bin/env python
# ruff: noqa: E402
"""Plan/apply the explicit ETF candidate AI-confirmation schema migration."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from alphahome.common.config_manager import get_database_url
from alphahome.curation.etf_candidate_confirmation import (
    MIGRATION_ID,
    apply_confirmation_migration,
    migration_plan_hash,
    missing_confirmation_schema,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan or apply the ETF candidate confirmation migration."
    )
    parser.add_argument("--database-url", help="override configured database URL")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="apply the migration; default is a read-only plan",
    )
    parser.add_argument(
        "--expected-plan-hash",
        help="required with --apply and must equal the current migration hash",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_url = args.database_url or get_database_url()
    if not database_url:
        raise RuntimeError("AlphaHome database URL is not configured")
    plan_hash = migration_plan_hash()
    if args.apply and args.expected_plan_hash != plan_hash:
        raise RuntimeError(
            "--expected-plan-hash must match the current migration plan hash"
        )

    connection = psycopg2.connect(database_url)
    try:
        before = missing_confirmation_schema(connection)
        connection.rollback()
        if args.apply:
            apply_confirmation_migration(connection)
            after = missing_confirmation_schema(connection)
            connection.rollback()
            if after:
                raise RuntimeError(
                    "migration acceptance failed; missing: " + ", ".join(after)
                )
            status = "applied"
        else:
            after = before
            status = "planned"
    finally:
        connection.close()

    print(
        json.dumps(
            {
                "status": status,
                "migration_id": MIGRATION_ID,
                "plan_hash": plan_hash,
                "missing_before": before,
                "missing_after": after,
                "write_performed": bool(args.apply),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
