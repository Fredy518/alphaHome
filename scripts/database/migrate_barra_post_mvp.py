#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Migrate Barra schema to Post-MVP (add new factor columns).

This script adds the new factor columns introduced in Post-MVP:
- Core factors renamed: style_mom_12m1m -> style_momentum, style_value_bp -> style_value
- Extended factors: style_nlsize, style_growth, style_leverage, style_dividend, style_earnings_quality

Run this script once to upgrade from MVP to Full Barra schema.

Usage:
  python scripts/database/migrate_barra_post_mvp.py
  python scripts/database/migrate_barra_post_mvp.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import create_async_manager

# New columns to add to exposures_daily
NEW_EXPOSURE_COLUMNS = [
    # Renamed/new core factors
    ("style_momentum", "DOUBLE PRECISION", "Multi-window momentum (was style_mom_12m1m)"),
    ("style_value", "DOUBLE PRECISION", "Multi-indicator value composite (was style_value_bp)"),
    # Extended factors
    ("style_nlsize", "DOUBLE PRECISION", "Non-linear size (Size³ orthogonalized to Size)"),
    ("style_growth", "DOUBLE PRECISION", "Multi-indicator growth composite"),
    ("style_leverage", "DOUBLE PRECISION", "Multi-indicator leverage composite"),
    ("style_dividend", "DOUBLE PRECISION", "Dividend yield"),
    ("style_earnings_quality", "DOUBLE PRECISION", "Earnings quality (accruals + OCF)"),
]

# New columns to add to factor_returns_daily
NEW_FR_COLUMNS = [
    ("fr_style_momentum", "DOUBLE PRECISION"),
    ("fr_style_value", "DOUBLE PRECISION"),
    ("fr_style_nlsize", "DOUBLE PRECISION"),
    ("fr_style_growth", "DOUBLE PRECISION"),
    ("fr_style_leverage", "DOUBLE PRECISION"),
    ("fr_style_dividend", "DOUBLE PRECISION"),
    ("fr_style_earnings_quality", "DOUBLE PRECISION"),
]

# New columns to add to portfolio_attribution_daily
NEW_ATTR_COLUMNS = [
    ("contrib_style_momentum", "DOUBLE PRECISION"),
    ("contrib_style_value", "DOUBLE PRECISION"),
    ("contrib_style_nlsize", "DOUBLE PRECISION"),
    ("contrib_style_growth", "DOUBLE PRECISION"),
    ("contrib_style_leverage", "DOUBLE PRECISION"),
    ("contrib_style_dividend", "DOUBLE PRECISION"),
    ("contrib_style_earnings_quality", "DOUBLE PRECISION"),
]


async def column_exists(db, schema: str, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    sql = """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
    """
    rows = await db.fetch(sql, schema, table, column)
    return bool(rows)


async def add_column_if_not_exists(
    db, 
    schema: str, 
    table: str, 
    column: str, 
    dtype: str,
    dry_run: bool = False,
) -> bool:
    """Add a column to a table if it doesn't exist."""
    exists = await column_exists(db, schema, table, column)
    if exists:
        print(f"  [SKIP] {schema}.{table}.{column} already exists")
        return False
    
    sql = f'ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS "{column}" {dtype};'
    
    if dry_run:
        print(f"  [DRY-RUN] Would execute: {sql}")
        return True
    
    await db.execute(sql)
    print(f"  [ADDED] {schema}.{table}.{column}")
    return True


async def migrate_exposures_daily(db, dry_run: bool = False) -> int:
    """Add new columns to barra.exposures_daily."""
    print("\n[1/3] Migrating barra.exposures_daily...")
    added = 0
    for col, dtype, comment in NEW_EXPOSURE_COLUMNS:
        if await add_column_if_not_exists(db, "barra", "exposures_daily", col, dtype, dry_run):
            added += 1
    return added


async def migrate_factor_returns_daily(db, dry_run: bool = False) -> int:
    """Add new columns to barra.factor_returns_daily."""
    print("\n[2/3] Migrating barra.factor_returns_daily...")
    added = 0
    for col, dtype in NEW_FR_COLUMNS:
        if await add_column_if_not_exists(db, "barra", "factor_returns_daily", col, dtype, dry_run):
            added += 1
    return added


async def migrate_portfolio_attribution_daily(db, dry_run: bool = False) -> int:
    """Add new columns to barra.portfolio_attribution_daily."""
    print("\n[3/3] Migrating barra.portfolio_attribution_daily...")
    added = 0
    for col, dtype in NEW_ATTR_COLUMNS:
        if await add_column_if_not_exists(db, "barra", "portfolio_attribution_daily", col, dtype, dry_run):
            added += 1
    return added


async def main(dry_run: bool = False):
    """Run the migration."""
    print("=" * 60)
    print("Barra Post-MVP Schema Migration")
    print("=" * 60)
    
    if dry_run:
        print("\n[DRY-RUN MODE] No changes will be made.\n")
    
    db_url = get_database_url()
    if not db_url:
        print("Error: DATABASE_URL not configured")
        return 1
    
    total_added = 0
    
    db = create_async_manager(db_url)
    await db.connect()
    try:
        # Check if barra schema exists
        rows = await db.fetch(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'barra'"
        )
        if not rows:
            print("Error: 'barra' schema does not exist. Run initialize_barra_schema.py first.")
            return 1

        # Run migrations
        total_added += await migrate_exposures_daily(db, dry_run)
        total_added += await migrate_factor_returns_daily(db, dry_run)
        total_added += await migrate_portfolio_attribution_daily(db, dry_run)
    finally:
        await db.close()
    
    print("\n" + "=" * 60)
    if dry_run:
        print(f"[DRY-RUN] Would add {total_added} new columns")
    else:
        print(f"Migration complete. Added {total_added} new columns.")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate Barra schema to Post-MVP")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    args = parser.parse_args()
    
    sys.exit(asyncio.run(main(dry_run=args.dry_run)))
