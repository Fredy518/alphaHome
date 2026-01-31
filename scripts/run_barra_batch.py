#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Batch runner: execute Barra exposures + factor returns for a date range.

Usage:
  python scripts/run_barra_batch.py 2025-01-01 2025-12-31
  python scripts/run_barra_batch.py 2025-01-01 2025-12-31 --parallel 4
  python scripts/run_barra_batch.py --last-n 30
  python scripts/run_barra_batch.py --last-n 30 --no-lag  # Use same-day exposures (MVP mode)

By default, factor returns use t-1 exposures to avoid look-ahead bias.
Use --no-lag to use same-day exposures (for testing/debugging).

Requires:
- DATABASE_URL or ~/.alphahome/config.json
- scripts/initialize_barra_schema.py already executed
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date as dt_date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import create_async_manager

from alphahome.barra.tasks import (
    BarraExposuresDailyTask,
    BarraExposuresFullTask,
    BarraFactorReturnsDailyTask,
)


# Global mode flag (set from args)
_USE_FULL_MODE = True


def get_exposures_task(db):
    """Get the appropriate exposures task based on mode."""
    if _USE_FULL_MODE:
        return BarraExposuresFullTask(db)
    return BarraExposuresDailyTask(db)


async def get_trade_dates(db, start_date: dt_date, end_date: dt_date) -> List[dt_date]:
    """Fetch actual trade dates from database within the range."""
    sql = """
        SELECT DISTINCT trade_date
        FROM rawdata.stock_daily
        WHERE trade_date BETWEEN $1::date AND $2::date
        ORDER BY trade_date
    """
    rows = await db.fetch(sql, start_date, end_date)
    return [r["trade_date"] for r in rows] if rows else []


async def get_prev_trade_date_map(db, trade_dates: List[dt_date]) -> Dict[dt_date, Optional[dt_date]]:
    """Build a mapping from each trade_date to its previous trade date.
    
    Returns:
        Dict mapping trade_date -> prev_trade_date (or None if no prev available)
    """
    if not trade_dates:
        return {}
    
    # Get all trade dates in a wider range to find predecessors
    min_date = min(trade_dates) - timedelta(days=30)  # Look back 30 days
    max_date = max(trade_dates)
    
    sql = """
        SELECT DISTINCT trade_date
        FROM rawdata.stock_daily
        WHERE trade_date BETWEEN $1::date AND $2::date
        ORDER BY trade_date
    """
    rows = await db.fetch(sql, min_date, max_date)
    all_dates = [r["trade_date"] for r in rows] if rows else []
    
    # Build mapping
    prev_map: Dict[dt_date, Optional[dt_date]] = {}
    date_set = set(trade_dates)
    
    for i, d in enumerate(all_dates):
        if d in date_set:
            prev_map[d] = all_dates[i - 1] if i > 0 else None
    
    return prev_map


async def run_single_date(
    db, 
    trade_date: dt_date, 
    exposure_date: Optional[dt_date] = None,
    verbose: bool = True
) -> Tuple[bool, str]:
    """Run exposures + factor_returns for a single date.
    
    Args:
        db: Database connection
        trade_date: The date to compute factor returns for
        exposure_date: The date to use for exposures (t-1 for PIT). 
                       If None, uses same-day (MVP mode).
    
    Returns:
        (success: bool, message: str)
    """
    trade_date_str = str(trade_date)
    exposure_date_str = str(exposure_date) if exposure_date else None
    
    try:
        async def _has_exposures(d: dt_date) -> bool:
            rows = await db.fetch(
                "SELECT 1 FROM barra.exposures_daily WHERE trade_date = $1::date LIMIT 1",
                d,
            )
            return bool(rows)

        # Step 1: Exposures
        # In PIT mode, factor returns at t uses exposures from exposure_date (t-1).
        # Ensure exposure_date exposures exist even when running a truncated range.
        t1 = get_exposures_task(db)
        if exposure_date and exposure_date != trade_date:
            if not await _has_exposures(exposure_date):
                await t1.execute(trade_date=str(exposure_date))

        res1 = await t1.execute(trade_date=trade_date_str)
        exp_count = res1.get("exposures_count", 0) if res1 else 0

        # Step 2: Factor Returns 
        # If exposure_date is provided, use t-1 exposures to explain t returns (PIT mode)
        # Otherwise, use same-day exposures (MVP mode)
        t2 = BarraFactorReturnsDailyTask(db)
        execute_kwargs = {"trade_date": trade_date_str}
        if exposure_date_str:
            execute_kwargs["exposure_date"] = exposure_date_str
        
        res2 = await t2.execute(**execute_kwargs)
        r2_val = res2.get("r2", 0) if res2 else 0
        n_obs = res2.get("n_obs", 0) if res2 else 0
        
        pit_info = f"(exp={exposure_date})" if exposure_date else "(same-day)"
        msg = f"exposures={exp_count}, n_obs={n_obs}, R²={r2_val:.3f} {pit_info}"
        if verbose:
            print(f"  ✓ {trade_date}: {msg}")
        return True, msg

    except Exception as e:
        msg = str(e)[:100]
        if verbose:
            print(f"  ✗ {trade_date}: {msg}")
        return False, msg


async def run_batch_sequential(
    db, 
    trade_dates: List[dt_date],
    prev_date_map: Dict[dt_date, Optional[dt_date]],
    use_lag: bool = True,
    verbose: bool = True
) -> dict:
    """Run dates sequentially."""
    results = {"success": 0, "failed": 0, "failures": []}
    
    for i, td in enumerate(trade_dates, 1):
        if verbose:
            print(f"[{i}/{len(trade_dates)}] Processing {td}...")
        
        # Determine exposure_date based on lag setting
        exposure_date = prev_date_map.get(td) if use_lag else None
        
        # Skip if no previous date available and lag is required
        if use_lag and exposure_date is None:
            if verbose:
                print(f"  ⚠ {td}: Skipped (no previous trade date available)")
            continue
        
        success, msg = await run_single_date(db, td, exposure_date, verbose=False)
        if success:
            results["success"] += 1
            if verbose:
                print(f"  ✓ {td}: {msg}")
        else:
            results["failed"] += 1
            results["failures"].append((td, msg))
            if verbose:
                print(f"  ✗ {td}: {msg}")
    
    return results


async def run_batch_parallel(
    db_url: str,
    trade_dates: List[dt_date],
    prev_date_map: Dict[dt_date, Optional[dt_date]],
    use_lag: bool = True,
    parallel: int = 4,
    verbose: bool = True
) -> dict:
    """Run dates in parallel batches (each batch uses its own connection)."""
    results = {"success": 0, "failed": 0, "failures": []}
    
    semaphore = asyncio.Semaphore(parallel)
    
    async def run_with_own_connection(td: dt_date) -> Tuple[dt_date, bool, str]:
        async with semaphore:
            # Determine exposure_date
            exposure_date = prev_date_map.get(td) if use_lag else None
            
            # Skip if no previous date and lag is required
            if use_lag and exposure_date is None:
                return td, False, "Skipped (no previous trade date)"
            
            db = create_async_manager(db_url)
            await db.connect()
            try:
                success, msg = await run_single_date(db, td, exposure_date, verbose=False)
                return td, success, msg
            finally:
                await db.close()
    
    tasks = [run_with_own_connection(td) for td in trade_dates]
    
    for i, coro in enumerate(asyncio.as_completed(tasks), 1):
        td, success, msg = await coro
        if success:
            results["success"] += 1
            if verbose:
                print(f"[{i}/{len(trade_dates)}] ✓ {td}: {msg}")
        else:
            results["failed"] += 1
            results["failures"].append((td, msg))
            if verbose:
                print(f"[{i}/{len(trade_dates)}] ✗ {td}: {msg}")
    
    return results


async def main(args: argparse.Namespace) -> None:
    # Ensure mode flag works both when called as a script and when imported
    # (e.g., scripts/production/barra/barra_daily.py wrapper).
    global _USE_FULL_MODE
    _USE_FULL_MODE = getattr(args, "mode", "full") == "full"

    db_url = get_database_url()
    if not db_url:
        raise RuntimeError("No database URL configured")

    db = create_async_manager(db_url)
    await db.connect()
    
    use_lag = not args.no_lag  # Default: use t-1 exposures (PIT mode)

    try:
        # Determine date range
        if args.last_n:
            # Get last N trade dates from database
            sql = """
                SELECT DISTINCT trade_date
                FROM rawdata.stock_daily
                ORDER BY trade_date DESC
                LIMIT $1
            """
            rows = await db.fetch(sql, args.last_n)
            trade_dates = sorted([r["trade_date"] for r in rows])
            print(f"[barra-batch] Running last {len(trade_dates)} trade dates")
        else:
            start_date = pd.to_datetime(args.start_date).date()
            end_date = pd.to_datetime(args.end_date).date()
            trade_dates = await get_trade_dates(db, start_date, end_date)
            print(f"[barra-batch] Found {len(trade_dates)} trade dates from {start_date} to {end_date}")

        if not trade_dates:
            print("[barra-batch] No trade dates to process")
            return

        # Build prev_trade_date map for t-1 alignment
        prev_date_map = await get_prev_trade_date_map(db, trade_dates)
        lag_mode = "PIT (t-1 exposures)" if use_lag else "MVP (same-day exposures)"
        print(f"[barra-batch] Mode: {lag_mode}")

        # Run batch
        if args.parallel > 1:
            await db.close()  # Close main connection, parallel mode uses its own
            print(f"[barra-batch] Running with {args.parallel} parallel workers...")
            results = await run_batch_parallel(
                db_url, trade_dates, prev_date_map, use_lag, 
                args.parallel, verbose=not args.quiet
            )
        else:
            print("[barra-batch] Running sequentially...")
            results = await run_batch_sequential(
                db, trade_dates, prev_date_map, use_lag, 
                verbose=not args.quiet
            )

        # Summary
        print(f"\n[barra-batch] Completed: {results['success']} success, {results['failed']} failed")
        if results["failures"]:
            print("[barra-batch] Failed dates:")
            for td, msg in results["failures"][:10]:
                print(f"  - {td}: {msg}")
            if len(results["failures"]) > 10:
                print(f"  ... and {len(results['failures']) - 10} more")

    finally:
        if args.parallel <= 1:
            await db.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch run Barra exposures + factor returns")
    parser.add_argument("start_date", nargs="?", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", nargs="?", help="End date (YYYY-MM-DD)")
    parser.add_argument("--last-n", type=int, help="Run last N trade dates instead of date range")
    parser.add_argument("--parallel", type=int, default=1, help="Number of parallel workers (default: 1)")
    parser.add_argument("--mode", choices=["full", "mvp"], default="full",
                        help="Factor mode: 'full' (Post-MVP multi-indicator) or 'mvp' (original)")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-date output")
    parser.add_argument("--no-lag", action="store_true", 
                        help="Use same-day exposures (MVP mode). Default uses t-1 exposures (PIT mode).")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    if not args.last_n and (not args.start_date or not args.end_date):
        print("Usage: python scripts/run_barra_batch.py START_DATE END_DATE")
        print("       python scripts/run_barra_batch.py --last-n 30")
        print("       python scripts/run_barra_batch.py --last-n 30 --mode full")
        sys.exit(1)
    
    # Set global mode flag (module scope doesn't need 'global')
    _USE_FULL_MODE = getattr(args, "mode", "full") == "full"
    
    asyncio.run(main(args))
