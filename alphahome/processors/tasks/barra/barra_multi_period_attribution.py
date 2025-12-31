#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Compute and persist multi-period portfolio Barra attribution with linking.

Links single-period attributions using Carino/Menchero methods to produce
consistent multi-period attribution results.

Multi-period linking ensures:
  Σ linked_contrib_k ≈ geometric_total_return

Supported methods:
- carino: Carino (1999) logarithmic smoothing
- menchero: Menchero (2000) optimized linking
- simple: Naive additive (no compounding adjustment)
"""

from __future__ import annotations

from datetime import date as dt_date
from typing import Any, Dict, List, Optional

import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ....barra.constants import BARRA_SCHEMA, STYLE_FACTOR_COLUMNS
from ....barra.linking import MultiPeriodLinker, link_attribution_series
from ...utils.serialization import save_dataframe as serializer_save_dataframe


@task_register()
class BarraMultiPeriodAttributionTask(ProcessorTaskBase):
    """Compute multi-period linked attribution from daily attributions.
    
    This task reads from portfolio_attribution_daily and computes
    geometrically-linked attribution for a date range.
    
    Required kwargs:
    - start_date: str or date
    - end_date: str or date
    - portfolio_id: str
    - benchmark_id: str
    - method: str ("carino", "menchero", or "simple"), default "carino"
    """
    
    name = "barra_multi_period_attribution"
    table_name = "multi_period_attribution"
    data_source = BARRA_SCHEMA
    description = "Barra 多期归因链接"

    source_tables = ["barra.portfolio_attribution_daily"]
    primary_keys = ["start_date", "end_date", "portfolio_id", "benchmark_id"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        self._industry_columns: Optional[List[str]] = None

    async def _ensure_industry_columns_loaded(self) -> None:
        if self._industry_columns is not None:
            return
        rows = await self.db.fetch(
            f"SELECT column_name FROM {BARRA_SCHEMA}.industry_l1_dim ORDER BY l1_code"
        )
        cols = [str(r.get("column_name")).strip() for r in (rows or []) if r.get("column_name")]
        if not cols:
            raise RuntimeError(f"{BARRA_SCHEMA}.industry_l1_dim is empty")
        self._industry_columns = cols

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """Fetch daily attributions for the date range."""
        start_date_raw = kwargs.get("start_date")
        end_date_raw = kwargs.get("end_date")
        portfolio_id = kwargs.get("portfolio_id")
        benchmark_id = kwargs.get("benchmark_id")

        if not all([start_date_raw, end_date_raw, portfolio_id, benchmark_id]):
            raise ValueError("start_date, end_date, portfolio_id, benchmark_id are required")

        start_date: dt_date = pd.to_datetime(start_date_raw).date()
        end_date: dt_date = pd.to_datetime(end_date_raw).date()

        await self._ensure_industry_columns_loaded()

        sql = f"""
            SELECT *
            FROM {BARRA_SCHEMA}.portfolio_attribution_daily
            WHERE trade_date BETWEEN $1::date AND $2::date
              AND portfolio_id = $3
              AND benchmark_id = $4
            ORDER BY trade_date
        """
        rows = await self.db.fetch(sql, start_date, end_date, portfolio_id, benchmark_id)
        
        if not rows:
            self.logger.warning(f"No attributions found for {portfolio_id}/{benchmark_id} in [{start_date}, {end_date}]")
            return pd.DataFrame()

        df = pd.DataFrame([dict(r) for r in rows])
        
        # Store params for process_data
        self._start_date = start_date
        self._end_date = end_date
        self._portfolio_id = portfolio_id
        self._benchmark_id = benchmark_id
        self._method = kwargs.get("method", "carino")
        
        return df

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        if data is None or data.empty:
            return pd.DataFrame()

        start_date = getattr(self, "_start_date", None)
        end_date = getattr(self, "_end_date", None)
        portfolio_id = getattr(self, "_portfolio_id", "")
        benchmark_id = getattr(self, "_benchmark_id", "")
        method = getattr(self, "_method", "carino")

        # Use the linking function (now includes specific_contrib in linking)
        result = link_attribution_series(
            attribution_df=data,
            return_col="active_return",
            contrib_prefix="contrib_",
            specific_contrib_col="specific_contrib",
            method=method,
        )

        linked_contribs = result.get("linked_contributions", {})
        
        # Build output row
        out = {
            "start_date": start_date,
            "end_date": end_date,
            "portfolio_id": portfolio_id,
            "benchmark_id": benchmark_id,
            "n_periods": result.get("n_periods", 0),
            "total_return": result.get("total_return", 0.0),
            "specific_contrib": result.get("specific_contrib", 0.0),  # Now properly linked
            "recon_error": result.get("recon_error", 0.0),
            "linking_method": method,
        }

        # Add factor contributions with prefix
        for factor, contrib in linked_contribs.items():
            out[f"contrib_{factor}"] = contrib

        result_df = pd.DataFrame([out])
        result_df["start_date"] = pd.to_datetime(result_df["start_date"])
        result_df["end_date"] = pd.to_datetime(result_df["end_date"])
        
        return result_df

    async def save_result(self, data: pd.DataFrame, **kwargs):
        if data is None or data.empty:
            self.logger.warning("没有数据需要保存")
            return

        await serializer_save_dataframe(
            df=data,
            table_name=self.table_name,
            db_connection=self.db,
            primary_keys=self.primary_keys,
            schema=BARRA_SCHEMA,
            if_exists="upsert",
        )


async def compute_rolling_attribution(
    db,
    portfolio_id: str,
    benchmark_id: str,
    as_of_date: dt_date,
    lookback_days: int = 252,
    method: str = "carino",
) -> Dict[str, Any]:
    """Convenience function to compute rolling multi-period attribution.
    
    Args:
        db: Database connection
        portfolio_id: Portfolio identifier
        benchmark_id: Benchmark identifier
        as_of_date: End date for the rolling window
        lookback_days: Number of calendar days to look back (default: 252 ≈ 1 year)
        method: Linking method
    
    Returns:
        Linked attribution result dict
    """
    from datetime import timedelta
    
    start_date = as_of_date - timedelta(days=lookback_days)
    
    task = BarraMultiPeriodAttributionTask(db)
    result = await task.execute(
        start_date=str(start_date),
        end_date=str(as_of_date),
        portfolio_id=portfolio_id,
        benchmark_id=benchmark_id,
        method=method,
    )
    
    return result
