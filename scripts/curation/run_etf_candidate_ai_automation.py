#!/usr/bin/env python
# ruff: noqa: E402
"""Plan or execute monthly GLMS-assisted ETF candidate maintenance."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from alphahome.common.config_manager import get_database_url
from alphahome.curation.etf_candidate_monthly_maintenance import (
    build_candidate_monthly_maintenance_plan,
    execute_candidate_monthly_maintenance,
    DEFAULT_FULL_MAX_NEW_PRODUCTS,
    DEFAULT_MAX_NEW_PRODUCTS,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only plan by default. The AlphaHome GUI owns upstream refresh; "
            "execution validates current facts, calls GLMS when required, "
            "and atomically loads one versioned candidate snapshot."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--execute",
        action="store_true",
        help="execute a manually reviewed plan hash",
    )
    parser.add_argument(
        "--expected-plan-hash",
        help="required with --execute",
    )
    parser.add_argument("--database-url", help="override configured database URL")
    parser.add_argument("--model", help="override GLMS_MODEL")
    parser.add_argument(
        "--run-date",
        type=date.fromisoformat,
        help="logical run date in YYYY-MM-DD; default is today",
    )
    parser.add_argument(
        "--reconfirm-all",
        action="store_true",
        help="reconfirm all non-human candidates instead of only legacy/drift rows",
    )
    parser.add_argument("--batch-size", type=int, default=12)
    parser.add_argument(
        "--workers", type=int, default=1, help="parallel model requests (1-8)"
    )
    parser.add_argument(
        "--scope",
        choices=("incremental", "full", "lof_initial"),
        default="incremental",
        help="full screens all listed exchange ETFs, including old unseen products",
    )
    parser.add_argument(
        "--max-new-products",
        type=int,
        help="guard limit; default 50 for incremental, 3000 for explicit full screening",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="save complete read-only plan, execution result and resumable model batches",
    )
    parser.add_argument(
        "--not-before-day",
        type=int,
        default=5,
        help="monthly maintenance waits until this calendar day (default: 5)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.execute and not args.expected_plan_hash:
        raise RuntimeError("--execute requires --expected-plan-hash")
    if not args.execute and args.expected_plan_hash:
        raise RuntimeError("--expected-plan-hash is only valid with --execute")

    database_url = args.database_url or get_database_url()
    if not database_url:
        raise RuntimeError("AlphaHome database URL is not configured")
    effective_run_date = args.run_date or date.today()
    max_new_products = args.max_new_products
    if max_new_products is None:
        max_new_products = (
            DEFAULT_FULL_MAX_NEW_PRODUCTS
            if args.scope in ("full", "lof_initial")
            else DEFAULT_MAX_NEW_PRODUCTS
        )
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.execute:
        result = execute_candidate_monthly_maintenance(
            database_url,
            run_date=effective_run_date,
            model_requested=args.model,
            not_before_day=args.not_before_day,
            reconfirm_all=args.reconfirm_all,
            max_new_products=max_new_products,
            batch_size=args.batch_size,
            expected_plan_hash=args.expected_plan_hash,
            screening_scope=args.scope,
            checkpoint_dir=args.output_dir / "batches" if args.output_dir else None,
            progress_callback=lambda event: print(
                json.dumps({"progress": event}), flush=True
            ),
            max_workers=args.workers,
        )
    else:
        result = build_candidate_monthly_maintenance_plan(
            database_url,
            run_date=effective_run_date,
            model_requested=args.model,
            not_before_day=args.not_before_day,
            reconfirm_all=args.reconfirm_all,
            max_new_products=max_new_products,
            screening_scope=args.scope,
            include_plan_payload=bool(args.output_dir),
        )
    if args.output_dir:
        payload = result.pop("plan_payload", None)
        if payload is not None:
            (args.output_dir / "plan.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
        filename = "result.json" if args.execute else "plan_summary.json"
        (args.output_dir / filename).write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
