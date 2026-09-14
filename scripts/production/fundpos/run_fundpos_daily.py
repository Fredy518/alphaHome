"""Thin production entrypoint for the isolated fundpos calculation engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from alphahome.integrations.fundpos import (
    FundposProductionRunner,
    load_production_config,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the AlphaHome fundpos pipeline")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path.home() / ".alphahome" / "fundpos_production.json",
    )
    parser.add_argument("--mode", choices=("check", "shadow", "publish"))
    parser.add_argument("--date", default="latest")
    parser.add_argument("--cutoff")
    parser.add_argument(
        "--family",
        action="append",
        choices=("fixed_income_plus", "enhanced_index", "convertible_dominant"),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        runner = FundposProductionRunner(load_production_config(args.config))
        result = runner.run(
            mode=args.mode,
            date=args.date,
            cutoff=args.cutoff,
            families=args.family,
        )
    # Config-load failures occur before the runner can write its normal ledger.
    except Exception as exc:  # noqa: BLE001
        result = {
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") == "passed" else 2


if __name__ == "__main__":
    sys.exit(main())
