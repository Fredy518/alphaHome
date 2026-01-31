#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Debug runner: execute Barra exposures + factor returns for a single trade_date.

Usage:
  python scripts/debug_run_barra_day.py 2025-12-29

Requires:
- DATABASE_URL or ~/.alphahome/config.json
- scripts/initialize_barra_schema.py already executed
"""

from __future__ import annotations

import asyncio
import sys

import pandas as pd

from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import create_async_manager

# Import tasks so decorators register (optional)
from alphahome.barra.tasks import BarraExposuresDailyTask, BarraFactorReturnsDailyTask


async def main(trade_date: str) -> None:
    trade_date = str(pd.to_datetime(trade_date).date())
    db_url = get_database_url()
    if not db_url:
        raise RuntimeError("No database URL configured")

    db = create_async_manager(db_url)
    await db.connect()

    try:
        print(f"[barra] running exposures for {trade_date}...")
        t1 = BarraExposuresDailyTask(db)
        res1 = await t1.execute(trade_date=trade_date)
        print(f"[barra] exposures done: {res1}")

        print(f"[barra] running factor returns for {trade_date}...")
        t2 = BarraFactorReturnsDailyTask(db)
        res2 = await t2.execute(trade_date=trade_date)
        print(f"[barra] factor returns done: {res2}")

    finally:
        await db.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/debug_run_barra_day.py YYYY-MM-DD")
    asyncio.run(main(sys.argv[1]))
