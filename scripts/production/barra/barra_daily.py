"""Barra daily wrapper.

Purpose: provide a production-friendly entrypoint that defaults to running the
latest trade date (equivalent to `scripts/run_barra_batch.py --last-n 1`).

This keeps `scripts/run_barra_batch.py` CLI contract unchanged while enabling
`ah prod run barra-daily` to be a single-command workflow.

Post-MVP: Added --mode flag to select between 'full' (multi-indicator) and 'mvp' factors.
"""

from __future__ import annotations

import argparse
import asyncio
import sys


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Barra daily pipeline (defaults to --last-n 1)",
    )
    parser.add_argument(
        "start_date",
        nargs="?",
        help="Start date (YYYY-MM-DD). If provided, end_date is required unless --last-n is set.",
    )
    parser.add_argument(
        "end_date",
        nargs="?",
        help="End date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--last-n",
        type=int,
        default=1,
        help="Run last N trade dates (default: 1)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1)",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "mvp"],
        default="full",
        help="Factor mode: 'full' (Post-MVP multi-indicator) or 'mvp' (original single-indicator)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-date output",
    )
    parser.add_argument(
        "--no-lag",
        action="store_true",
        help="Use same-day exposures (MVP mode). Default uses t-1 exposures (PIT mode).",
    )
    return parser.parse_args(argv)


def _build_run_barra_batch_args(args: argparse.Namespace):
    # Default: last-n 1
    if args.start_date and args.end_date:
        return {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "last_n": None,
            "parallel": args.parallel,
            "quiet": args.quiet,
            "no_lag": args.no_lag,
            "mode": args.mode,
        }

    return {
        "start_date": None,
        "end_date": None,
        "last_n": args.last_n,
        "parallel": args.parallel,
        "quiet": args.quiet,
        "no_lag": args.no_lag,
        "mode": args.mode,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if (args.start_date and not args.end_date) or (args.end_date and not args.start_date):
        print("Error: start_date and end_date must be provided together.")
        return 2

    from scripts.run_barra_batch import main as run_batch_main

    run_args_dict = _build_run_barra_batch_args(args)
    run_args = argparse.Namespace(**run_args_dict)

    asyncio.run(run_batch_main(run_args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
