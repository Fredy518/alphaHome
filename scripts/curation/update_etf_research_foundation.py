#!/usr/bin/env python
# ruff: noqa: E402
"""Refresh reusable ETF facts and load one candidate-master snapshot."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import DBManager
from alphahome.curation.etf_research_foundation import (
    update_etf_research_foundation,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh reusable ETF/industry research facts, load an ETF candidate "
            "snapshot, then refresh its current index universe."
        )
    )
    parser.add_argument(
        "--candidate-snapshot",
        required=True,
        type=Path,
        help="normalized candidate-master JSON exported from the research workspace",
    )
    parser.add_argument(
        "--database-url", help="override AlphaHome configured database URL"
    )
    parser.add_argument(
        "--skip-source-file-check",
        action="store_true",
        help="skip comparison against a locally accessible source workbook",
    )
    return parser.parse_args()


async def run_update(args: argparse.Namespace) -> dict:
    database_url = args.database_url or get_database_url()
    if not database_url:
        raise RuntimeError("AlphaHome database URL is not configured")

    db_manager = DBManager(database_url)
    await db_manager.connect()
    try:
        return await update_etf_research_foundation(
            db_manager,
            database_url,
            args.candidate_snapshot,
            verify_source_file=not args.skip_source_file_check,
        )
    finally:
        await db_manager.close()


def main() -> int:
    result = asyncio.run(run_update(parse_args()))
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raise SystemExit(main())
