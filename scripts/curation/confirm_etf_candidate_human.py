#!/usr/bin/env python
# ruff: noqa: E402
"""Approve or reject one ETF candidate and append an audit record."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from alphahome.common.config_manager import get_database_url
from alphahome.curation.etf_candidate_confirmation import review_candidate_human


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Approve or reject one candidate while retaining its AI provenance "
            "and an append-only before/after audit."
        )
    )
    parser.add_argument("--fund-code", required=True)
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--review-note")
    parser.add_argument(
        "--decision",
        choices=("approve", "reject"),
        default="approve",
        help="approve keeps the row active; reject preserves history but removes it from current views",
    )
    parser.add_argument(
        "--snapshot-id",
        help="target snapshot; default is the latest loaded candidate snapshot",
    )
    parser.add_argument("--database-url", help="override configured database URL")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_url = args.database_url or get_database_url()
    if not database_url:
        raise RuntimeError("AlphaHome database URL is not configured")
    connection = psycopg2.connect(database_url)
    try:
        result = review_candidate_human(
            connection,
            fund_code=args.fund_code,
            reviewer=args.reviewer,
            decision=args.decision,
            review_note=args.review_note,
            snapshot_id=args.snapshot_id,
        )
    finally:
        connection.close()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
