#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Initialize Barra schema objects in AlphaDB.

Creates:
- schema barra
- view barra.pit_sw_industry_member_mv (PIT range using rawdata.index_swmember)
- dimension table barra.industry_l1_dim
- partitioned wide tables (trade_date partition key) with DEFAULT partitions:
  - barra.exposures_daily
  - barra.factor_returns_daily
  - barra.specific_returns_daily
  - barra.portfolio_attribution_daily

Design notes:
- Industry one-hot columns are generated from DISTINCT l1_code in rawdata.index_swmember.
- out_date is confirmed as the last effective day (query_end_date = out_date).

Usage:
  python scripts/initialize_barra_schema.py

Connection:
- DATABASE_URL, or ~/.alphahome/config.json (database.url)
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass

from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import create_async_manager
from alphahome.common.logging_utils import get_logger
from alphahome.barra.constants import BARRA_SCHEMA, STYLE_FACTOR_COLUMNS
from alphahome.barra.ddl import BarraDDL

logger = get_logger(__name__)


_IDENTIFIER_RE = re.compile(r"[^a-zA-Z0-9_]+")


def sanitize_industry_column_name(l1_code: str) -> str:
    """Convert SW l1_code (e.g. '801010.SI') to a safe SQL identifier.

    Output example: 'ind_801010_si'
    """
    if not l1_code:
        raise ValueError("l1_code is empty")

    raw = f"ind_{l1_code.strip().lower()}"
    raw = raw.replace(".", "_")
    raw = _IDENTIFIER_RE.sub("_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")

    # Ensure starts with a letter or underscore
    if not re.match(r"^[a-zA-Z_]", raw):
        raw = f"ind_{raw}"

    # Keep reasonably short
    return raw[:60]


@dataclass(frozen=True)
class IndustryL1:
    l1_code: str
    l1_name: str | None
    column_name: str


async def load_sw_l1_list(db) -> list[IndustryL1]:
    rows = await db.fetch(
        """
        SELECT
          l1_code,
          MAX(l1_name) as l1_name
        FROM rawdata.index_swmember
        WHERE l1_code IS NOT NULL
        GROUP BY l1_code
        ORDER BY l1_code;
        """
    )

    industries: list[IndustryL1] = []
    for row in rows:
        l1_code = (row.get("l1_code") or "").strip()
        if not l1_code:
            continue
        industries.append(
            IndustryL1(
                l1_code=l1_code,
                l1_name=(row.get("l1_name") or None),
                column_name=sanitize_industry_column_name(l1_code),
            )
        )

    if not industries:
        raise RuntimeError("No SW L1 industries found in rawdata.index_swmember")

    # Ensure uniqueness of column_name
    seen: set[str] = set()
    for ind in industries:
        if ind.column_name in seen:
            raise RuntimeError(f"Duplicate industry column_name generated: {ind.column_name}")
        seen.add(ind.column_name)

    return industries


def build_exposures_columns_sql(industries: list[IndustryL1]) -> str:
    base_cols = [
        "trade_date DATE NOT NULL",
        "ticker TEXT NOT NULL",
        "eligible_flag BOOLEAN",
        "ff_mcap DOUBLE PRECISION",
        "weight_wls DOUBLE PRECISION",
        "industry_l1_code TEXT",
    ]
    style_cols = [f"{c} DOUBLE PRECISION" for c in STYLE_FACTOR_COLUMNS]
    ind_cols = [f"{ind.column_name} SMALLINT" for ind in industries]

    # SMALLINT for one-hot 0/1
    all_cols = base_cols + style_cols + ind_cols
    return "\n  , ".join(all_cols)


def build_factor_returns_columns_sql(industries: list[IndustryL1]) -> str:
    base_cols = [
        "trade_date DATE NOT NULL",
        "n_obs INTEGER",
        "r2 DOUBLE PRECISION",
        "rmse DOUBLE PRECISION",
    ]
    style_cols = [f"fr_{c} DOUBLE PRECISION" for c in STYLE_FACTOR_COLUMNS]
    ind_cols = [f"fr_{ind.column_name} DOUBLE PRECISION" for ind in industries]
    all_cols = base_cols + style_cols + ind_cols
    return "\n  , ".join(all_cols)


def build_specific_returns_columns_sql() -> str:
    cols = [
        "trade_date DATE NOT NULL",
        "ticker TEXT NOT NULL",
        "raw_return DOUBLE PRECISION",
        "fitted_return DOUBLE PRECISION",
        "specific_return DOUBLE PRECISION",
        "weight_wls DOUBLE PRECISION",
    ]
    return "\n  , ".join(cols)


def build_portfolio_attr_columns_sql(industries: list[IndustryL1]) -> str:
    base_cols = [
        "trade_date DATE NOT NULL",
        "portfolio_id TEXT NOT NULL",
        "benchmark_id TEXT NOT NULL",
        "active_return DOUBLE PRECISION",  # True active return (portfolio - benchmark)
        "explained_return DOUBLE PRECISION",  # Model's explanation (factor + specific)
        "specific_contrib DOUBLE PRECISION",
        "recon_error DOUBLE PRECISION",  # active_return - explained_return
        "portfolio_return DOUBLE PRECISION",  # True portfolio return
        "benchmark_return DOUBLE PRECISION",  # True benchmark return
    ]
    style_cols = [f"contrib_{c} DOUBLE PRECISION" for c in STYLE_FACTOR_COLUMNS]
    ind_cols = [f"contrib_{ind.column_name} DOUBLE PRECISION" for ind in industries]
    all_cols = base_cols + style_cols + ind_cols
    return "\n  , ".join(all_cols)


def build_multi_period_attr_columns_sql(industries: list[IndustryL1]) -> str:
    """Columns for multi-period linked attribution table."""
    base_cols = [
        "start_date DATE NOT NULL",
        "end_date DATE NOT NULL",
        "portfolio_id TEXT NOT NULL",
        "benchmark_id TEXT NOT NULL",
        "n_periods INTEGER",
        "total_return DOUBLE PRECISION",
        "specific_contrib DOUBLE PRECISION",
        "recon_error DOUBLE PRECISION",
        "linking_method TEXT",
    ]
    style_cols = [f"contrib_{c} DOUBLE PRECISION" for c in STYLE_FACTOR_COLUMNS]
    ind_cols = [f"contrib_{ind.column_name} DOUBLE PRECISION" for ind in industries]
    all_cols = base_cols + style_cols + ind_cols
    return "\n  , ".join(all_cols)


async def upsert_industry_dim(db, industries: list[IndustryL1]):
    await db.execute(f"CREATE SCHEMA IF NOT EXISTS {BARRA_SCHEMA};")

    ddl = BarraDDL(schema=BARRA_SCHEMA)
    await db.execute(ddl.create_industry_dim_table_sql())

    sql = f"""
    INSERT INTO {BARRA_SCHEMA}.industry_l1_dim (l1_code, l1_name, column_name, updated_at)
    VALUES ($1, $2, $3, NOW())
    ON CONFLICT (l1_code) DO UPDATE SET
      l1_name = EXCLUDED.l1_name,
      column_name = EXCLUDED.column_name,
      updated_at = NOW();
    """

    for ind in industries:
        await db.execute(sql, ind.l1_code, ind.l1_name, ind.column_name)


async def initialize_barra_schema():
    db_url = get_database_url()
    if not db_url:
        raise RuntimeError("No database URL configured. Set DATABASE_URL or ~/.alphahome/config.json")

    db = create_async_manager(db_url)
    await db.connect()

    try:
        ddl = BarraDDL(schema=BARRA_SCHEMA)

        logger.info("Loading SW L1 industry list from rawdata.index_swmember...")
        industries = await load_sw_l1_list(db)
        logger.info(f"Found {len(industries)} SW L1 industries")

        logger.info("Creating barra schema + PIT SW view...")
        await db.execute(ddl.create_schema_sql())
        await db.execute(ddl.create_pit_sw_view_sql())

        logger.info("Upserting barra.industry_l1_dim...")
        await upsert_industry_dim(db, industries)

        logger.info("Creating partitioned wide tables (with DEFAULT partitions)...")

        # exposures_daily
        exposures_cols = build_exposures_columns_sql(industries)
        await db.execute(
            ddl.create_partitioned_table_sql(
                table_name="exposures_daily",
                columns_sql="  " + exposures_cols,
                pk_sql="  PRIMARY KEY (trade_date, ticker)",
            )
        )
        await db.execute(ddl.create_default_partition_sql("exposures_daily"))

        # factor_returns_daily
        fr_cols = build_factor_returns_columns_sql(industries)
        await db.execute(
            ddl.create_partitioned_table_sql(
                table_name="factor_returns_daily",
                columns_sql="  " + fr_cols,
                pk_sql="  PRIMARY KEY (trade_date)",
            )
        )
        await db.execute(ddl.create_default_partition_sql("factor_returns_daily"))

        # specific_returns_daily
        sr_cols = build_specific_returns_columns_sql()
        await db.execute(
            ddl.create_partitioned_table_sql(
                table_name="specific_returns_daily",
                columns_sql="  " + sr_cols,
                pk_sql="  PRIMARY KEY (trade_date, ticker)",
            )
        )
        await db.execute(ddl.create_default_partition_sql("specific_returns_daily"))

        # portfolio_attribution_daily
        pa_cols = build_portfolio_attr_columns_sql(industries)
        await db.execute(
            ddl.create_partitioned_table_sql(
                table_name="portfolio_attribution_daily",
                columns_sql="  " + pa_cols,
                pk_sql="  PRIMARY KEY (trade_date, portfolio_id, benchmark_id)",
            )
        )
        await db.execute(ddl.create_default_partition_sql("portfolio_attribution_daily"))

        # multi_period_attribution (non-partitioned, smaller volume)
        mpa_cols = build_multi_period_attr_columns_sql(industries)
        mpa_sql = f"""
        CREATE TABLE IF NOT EXISTS {BARRA_SCHEMA}.multi_period_attribution (
          {mpa_cols}
          , PRIMARY KEY (start_date, end_date, portfolio_id, benchmark_id)
        );
        """
        await db.execute(mpa_sql)

        # factor_covariance (risk model output)
        fc_sql = f"""
        CREATE TABLE IF NOT EXISTS {BARRA_SCHEMA}.factor_covariance (
          as_of_date DATE NOT NULL,
          factor1 TEXT NOT NULL,
          factor2 TEXT NOT NULL,
          covariance DOUBLE PRECISION,
          correlation DOUBLE PRECISION,
          vol1 DOUBLE PRECISION,
          vol2 DOUBLE PRECISION,
          PRIMARY KEY (as_of_date, factor1, factor2)
        );
        """
        await db.execute(fc_sql)

        # specific_variance_daily (risk model output)
        sv_sql = f"""
        CREATE TABLE IF NOT EXISTS {BARRA_SCHEMA}.specific_variance_daily (
          as_of_date DATE NOT NULL,
          ticker TEXT NOT NULL,
          specific_var DOUBLE PRECISION,
          n_obs INTEGER,
          PRIMARY KEY (as_of_date, ticker)
        );
        """
        await db.execute(sv_sql)

        logger.info("✅ Barra schema initialized (7 tables)")

    finally:
        await db.close()


def main():
    asyncio.run(initialize_barra_schema())


if __name__ == "__main__":
    main()
