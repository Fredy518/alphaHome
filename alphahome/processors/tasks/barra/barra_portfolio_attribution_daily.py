#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Compute and persist daily portfolio Barra attribution.

Single-period attribution formula (relative to benchmark):
  active_return ≈ Δx' * f + a' * u

Where:
  Δx = portfolio exposures - benchmark exposures (factor vector)
  f  = factor returns (from factor_returns_daily)
  a  = active weight = portfolio weight - benchmark weight (per stock)
  u  = specific return (from specific_returns_daily)

Recon error:
  recon_error = true_active_return - (factor_contrib + specific_contrib)
  
  This measures the attribution's accuracy. A non-zero recon_error indicates
  model incompleteness (e.g., missing factors, weight changes, trading effects).

Note:
- This implementation computes true portfolio/benchmark returns from weights and stock returns.
- Multi-period linking is handled separately by barra_multi_period_attribution.py
"""

from __future__ import annotations

from datetime import date as dt_date
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ....barra.constants import BARRA_SCHEMA, STYLE_FACTOR_COLUMNS
from ...utils.serialization import save_dataframe as serializer_save_dataframe


@task_register()
class BarraPortfolioAttributionDailyTask(ProcessorTaskBase):
    name = "barra_portfolio_attribution_daily"
    table_name = "portfolio_attribution_daily"
    data_source = BARRA_SCHEMA
    description = "Barra 日度组合归因（MVP）"

    source_tables = [
        "barra.exposures_daily",
        "barra.factor_returns_daily",
        "barra.specific_returns_daily",
    ]

    primary_keys = ["trade_date", "portfolio_id", "benchmark_id"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        self._industry_columns: Optional[list[str]] = None

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
        """Fetch exposures, factor returns, specific returns for the trade_date.

        kwargs expected:
        - trade_date: required
        - portfolio_weights: pd.DataFrame with columns [ticker, weight]
        - benchmark_weights: pd.DataFrame with columns [ticker, weight]
        - portfolio_id: str
        - benchmark_id: str
        """
        trade_date_raw = kwargs.get("trade_date")
        if not trade_date_raw:
            raise ValueError("trade_date is required")

        trade_date: dt_date = pd.to_datetime(trade_date_raw).date()

        await self._ensure_industry_columns_loaded()
        assert self._industry_columns is not None

        # Load exposures
        exp_cols = ["ticker", "eligible_flag", *STYLE_FACTOR_COLUMNS, *self._industry_columns]
        exp_sql = f"SELECT {', '.join(exp_cols)} FROM {BARRA_SCHEMA}.exposures_daily WHERE trade_date = $1::date"
        exp_rows = await self.db.fetch(exp_sql, trade_date)
        if not exp_rows:
            return pd.DataFrame()
        exp_df = pd.DataFrame([dict(r) for r in exp_rows])

        # Load factor returns
        fr_sql = f"SELECT * FROM {BARRA_SCHEMA}.factor_returns_daily WHERE trade_date = $1::date"
        fr_rows = await self.db.fetch(fr_sql, trade_date)
        if not fr_rows:
            return pd.DataFrame()
        fr_row = dict(fr_rows[0])

        # Load specific returns (also contains raw_return for computing true returns)
        sr_sql = f"""
            SELECT ticker, specific_return, raw_return 
            FROM {BARRA_SCHEMA}.specific_returns_daily 
            WHERE trade_date = $1::date
        """
        sr_rows = await self.db.fetch(sr_sql, trade_date)
        sr_df = pd.DataFrame([dict(r) for r in sr_rows]) if sr_rows else pd.DataFrame(
            columns=["ticker", "specific_return", "raw_return"]
        )

        # Pass everything via self for process_data
        self._exp_df = exp_df
        self._fr_row = fr_row
        self._sr_df = sr_df
        self._trade_date = trade_date

        # Return a dummy non-empty DF so process_data runs
        return pd.DataFrame({"_dummy": [1]})

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        exp_df = getattr(self, "_exp_df", None)
        fr_row = getattr(self, "_fr_row", None)
        sr_df = getattr(self, "_sr_df", None)
        trade_date = getattr(self, "_trade_date", None)

        if exp_df is None or exp_df.empty or fr_row is None:
            return pd.DataFrame()

        portfolio_weights = kwargs.get("portfolio_weights")
        benchmark_weights = kwargs.get("benchmark_weights")
        portfolio_id = kwargs.get("portfolio_id", "portfolio")
        benchmark_id = kwargs.get("benchmark_id", "benchmark")

        if portfolio_weights is None or benchmark_weights is None:
            self.logger.warning("portfolio_weights and benchmark_weights are required")
            return pd.DataFrame()

        pw = portfolio_weights.set_index("ticker")["weight"]
        bw = benchmark_weights.set_index("ticker")["weight"]

        # Align to common universe
        universe = exp_df["ticker"].tolist()
        pw = pw.reindex(universe).fillna(0.0)
        bw = bw.reindex(universe).fillna(0.0)

        # Compute portfolio/benchmark exposures (weighted sum)
        exp_df = exp_df.set_index("ticker")
        factor_cols = list(STYLE_FACTOR_COLUMNS) + list(self._industry_columns or [])

        x_p = {}
        x_b = {}
        for c in factor_cols:
            x = exp_df[c].fillna(0.0).astype(float)
            x_p[c] = float((pw * x).sum())
            x_b[c] = float((bw * x).sum())

        delta_x = {c: x_p[c] - x_b[c] for c in factor_cols}

        # Factor contribution = Δx * f
        factor_contrib = {}
        total_factor_contrib = 0.0
        for c in factor_cols:
            fr_key = f"fr_{c}"
            f_val = fr_row.get(fr_key)
            if f_val is None:
                f_val = 0.0
            contrib = float(delta_x[c]) * float(f_val)
            factor_contrib[f"contrib_{c}"] = contrib
            total_factor_contrib += contrib

        # Specific contribution = a' * u
        a = pw - bw
        if sr_df is not None and not sr_df.empty:
            sr = sr_df.set_index("ticker")["specific_return"].reindex(universe).fillna(0.0).astype(float)
            stock_returns = sr_df.set_index("ticker")["raw_return"].reindex(universe).fillna(0.0).astype(float)
        else:
            sr = pd.Series(0.0, index=universe)
            stock_returns = pd.Series(0.0, index=universe)
        specific_contrib = float((a * sr).sum())

        # Compute TRUE portfolio and benchmark returns
        # true_portfolio_return = sum(w_i * r_i)
        # true_benchmark_return = sum(b_i * r_i)
        # true_active_return = true_portfolio_return - true_benchmark_return
        true_portfolio_return = float((pw * stock_returns).sum())
        true_benchmark_return = float((bw * stock_returns).sum())
        true_active_return = true_portfolio_return - true_benchmark_return

        # Explained active return (from attribution model)
        explained_active_return = total_factor_contrib + specific_contrib

        # Recon error: difference between true active return and model explanation
        # This measures attribution accuracy
        # A non-zero recon_error indicates model incompleteness
        recon_error = true_active_return - explained_active_return

        out = {
            "trade_date": trade_date,
            "portfolio_id": portfolio_id,
            "benchmark_id": benchmark_id,
            "active_return": true_active_return,  # Use TRUE active return
            "explained_return": explained_active_return,  # Model's explanation
            "specific_contrib": specific_contrib,
            "recon_error": recon_error,
            "portfolio_return": true_portfolio_return,
            "benchmark_return": true_benchmark_return,
            **factor_contrib,
        }

        result_df = pd.DataFrame([out])
        result_df["trade_date"] = pd.to_datetime(result_df["trade_date"])
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
