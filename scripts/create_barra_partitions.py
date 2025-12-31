#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Create yearly partitions for Barra tables.

This script creates RANGE partitions by year for the partitioned Barra tables.
Run this AFTER initialize_barra_schema.py and BEFORE bulk backfill.

Usage:
    python scripts/create_barra_partitions.py --start-year 2015 --end-year 2026
    python scripts/create_barra_partitions.py  # defaults: 2010-2030

Why partitions matter:
    - Without explicit partitions, all data goes into DEFAULT partition
    - This causes poor query performance and slow upserts during backfill
    - Yearly partitions provide good balance between partition count and data volume
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date

from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import create_async_manager
from alphahome.common.logging_utils import get_logger
from alphahome.barra.constants import BARRA_SCHEMA

logger = get_logger(__name__)

# Tables that use trade_date partitioning
PARTITIONED_TABLES = [
    "exposures_daily",
    "factor_returns_daily",
    "specific_returns_daily",
    "portfolio_attribution_daily",
]


def generate_partition_ddl(table_name: str, year: int) -> str:
    """Generate DDL for a yearly partition.
    
    Creates a partition for [year-01-01, year+1-01-01).
    """
    partition_name = f"{table_name}_y{year}"
    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"
    
    return f"""
CREATE TABLE IF NOT EXISTS {BARRA_SCHEMA}.{partition_name}
PARTITION OF {BARRA_SCHEMA}.{table_name}
FOR VALUES FROM ('{start_date}') TO ('{end_date}');
""".strip()


async def check_partition_exists(db, table_name: str, year: int) -> bool:
    """Check if a partition already exists."""
    partition_name = f"{table_name}_y{year}"
    sql = """
    SELECT 1 FROM pg_tables 
    WHERE schemaname = $1 AND tablename = $2
    LIMIT 1
    """
    rows = await db.fetch(sql, BARRA_SCHEMA, partition_name)
    return bool(rows)


async def check_parent_table_exists(db, table_name: str) -> bool:
    """Check if the parent partitioned table exists."""
    sql = """
    SELECT 1 FROM pg_tables 
    WHERE schemaname = $1 AND tablename = $2
    LIMIT 1
    """
    rows = await db.fetch(sql, BARRA_SCHEMA, table_name)
    return bool(rows)


async def create_partitions(
    db,
    start_year: int,
    end_year: int,
    dry_run: bool = False,
) -> dict:
    """Create yearly partitions for all Barra tables.
    
    Returns:
        Dict with counts of created/skipped/failed partitions
    """
    results = {"created": 0, "skipped": 0, "failed": 0, "details": []}
    
    for table_name in PARTITIONED_TABLES:
        # Check if parent table exists
        if not await check_parent_table_exists(db, table_name):
            logger.warning(f"Parent table {BARRA_SCHEMA}.{table_name} does not exist, skipping")
            results["details"].append((table_name, "all", "parent_missing"))
            continue
        
        for year in range(start_year, end_year + 1):
            partition_name = f"{table_name}_y{year}"
            
            # Check if already exists
            if await check_partition_exists(db, table_name, year):
                logger.debug(f"Partition {partition_name} already exists, skipping")
                results["skipped"] += 1
                continue
            
            ddl = generate_partition_ddl(table_name, year)
            
            if dry_run:
                logger.info(f"[DRY RUN] Would create: {partition_name}")
                print(ddl)
                print()
                results["created"] += 1
            else:
                try:
                    await db.execute(ddl)
                    logger.info(f"Created partition: {BARRA_SCHEMA}.{partition_name}")
                    results["created"] += 1
                    results["details"].append((table_name, year, "created"))
                except Exception as e:
                    logger.error(f"Failed to create {partition_name}: {e}")
                    results["failed"] += 1
                    results["details"].append((table_name, year, f"failed: {e}"))
    
    return results


async def show_partition_status(db) -> None:
    """Show current partition status for all Barra tables."""
    print("\n" + "=" * 60)
    print("Current Partition Status")
    print("=" * 60)
    
    for table_name in PARTITIONED_TABLES:
        if not await check_parent_table_exists(db, table_name):
            print(f"\n{BARRA_SCHEMA}.{table_name}: NOT CREATED")
            continue
        
        # Get all partitions for this table
        sql = """
        SELECT 
            child.relname as partition_name,
            pg_get_expr(child.relpartbound, child.oid) as partition_bound
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace ns ON parent.relnamespace = ns.oid
        WHERE ns.nspname = $1 AND parent.relname = $2
        ORDER BY child.relname
        """
        rows = await db.fetch(sql, BARRA_SCHEMA, table_name)
        
        print(f"\n{BARRA_SCHEMA}.{table_name}:")
        if not rows:
            print("  (no partitions found)")
        else:
            for r in rows:
                name = r["partition_name"]
                bound = r["partition_bound"] or "DEFAULT"
                # Shorten the bound display
                if "FOR VALUES" in bound:
                    bound = bound.replace("FOR VALUES FROM ", "").replace(" TO ", " → ")
                print(f"  - {name}: {bound}")


async def main(args: argparse.Namespace) -> None:
    db_url = get_database_url()
    if not db_url:
        raise RuntimeError("No database URL configured")

    db = create_async_manager(db_url)
    await db.connect()
    
    try:
        if args.status:
            await show_partition_status(db)
            return
        
        logger.info(f"Creating yearly partitions from {args.start_year} to {args.end_year}")
        if args.dry_run:
            logger.info("DRY RUN mode - no changes will be made")
        
        results = await create_partitions(
            db,
            start_year=args.start_year,
            end_year=args.end_year,
            dry_run=args.dry_run,
        )
        
        print("\n" + "=" * 60)
        print("Partition Creation Summary")
        print("=" * 60)
        print(f"Created: {results['created']}")
        print(f"Skipped (already exist): {results['skipped']}")
        print(f"Failed: {results['failed']}")
        
        if results["failed"] > 0:
            print("\nFailed partitions:")
            for table, year, status in results["details"]:
                if "failed" in str(status):
                    print(f"  - {table}_y{year}: {status}")
        
        # Show final status
        if not args.dry_run:
            await show_partition_status(db)
        
    finally:
        await db.close()


def parse_args() -> argparse.Namespace:
    current_year = date.today().year
    
    parser = argparse.ArgumentParser(
        description="Create yearly partitions for Barra tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Create partitions for 2015-2026
    python scripts/create_barra_partitions.py --start-year 2015 --end-year 2026
    
    # Dry run to see what would be created
    python scripts/create_barra_partitions.py --dry-run
    
    # Show current partition status
    python scripts/create_barra_partitions.py --status
        """
    )
    parser.add_argument(
        "--start-year", type=int, default=2010,
        help="First year to create partition for (default: 2010)"
    )
    parser.add_argument(
        "--end-year", type=int, default=current_year + 5,
        help=f"Last year to create partition for (default: {current_year + 5})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print DDL without executing"
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show current partition status and exit"
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))
