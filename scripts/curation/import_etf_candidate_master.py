#!/usr/bin/env python
# ruff: noqa: E402
"""Import a normalized ETF candidate-master snapshot into AlphaDB."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from alphahome.common.config_manager import get_database_url
from alphahome.curation.etf_candidate_master import (
    load_candidate_master_snapshot,
    read_and_validate_payload,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and idempotently import an ETF candidate-master JSON snapshot "
            "into fund_pool_on."
        )
    )
    parser.add_argument(
        "--input", required=True, type=Path, help="normalized snapshot JSON"
    )
    parser.add_argument(
        "--database-url", help="override AlphaHome configured database URL"
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="validate the handoff contract without changing the database",
    )
    parser.add_argument(
        "--skip-source-file-check",
        action="store_true",
        help="do not compare the JSON hash with a locally accessible source workbook",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = read_and_validate_payload(
        args.input,
        verify_source_file=not args.skip_source_file_check,
    )
    if args.validate_only:
        print(
            json.dumps(
                {
                    "status": "validated",
                    "snapshot_id": payload["snapshot_id"],
                    "rows": len(payload["records"]),
                    "exposures": payload["quality"]["exposure_count"],
                    "source_file_sha256": payload["source"]["source_file_sha256"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    database_url = args.database_url or get_database_url()
    if not database_url:
        raise RuntimeError("AlphaHome database URL is not configured")
    connection = psycopg2.connect(database_url)
    try:
        result = load_candidate_master_snapshot(
            connection,
            payload,
            verify_source_file=not args.skip_source_file_check,
        )
    finally:
        connection.close()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
