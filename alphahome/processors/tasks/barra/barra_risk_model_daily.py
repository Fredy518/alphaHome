#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Compute and persist Barra risk model estimates.

Estimates factor covariance matrix and specific variances from historical
factor returns and specific returns.

Output tables:
- barra.factor_covariance: K × K factor covariance matrix (annualized)
- barra.specific_variance_daily: Per-stock specific variance estimates
"""

from __future__ import annotations

from datetime import date as dt_date, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ....barra.constants import BARRA_SCHEMA, STYLE_FACTOR_COLUMNS
from ....barra.risk_model import (
    RiskModel,
    RiskModelConfig,
    estimate_factor_covariance,
    estimate_specific_variance,
)
from ...utils.serialization import save_dataframe as serializer_save_dataframe


@task_register()
class BarraRiskModelDailyTask(ProcessorTaskBase):
    """Compute daily risk model estimates (factor cov + specific var).
    
    Required kwargs:
    - trade_date: The as-of date for risk model estimation
    - cov_window: Rolling window size (default: 252)
    - half_life: Exponential decay half-life (default: 126)
    
    The task fetches historical factor returns and specific returns
    up to trade_date and estimates the risk model.
    """
    
    name = "barra_risk_model_daily"
    table_name = "factor_covariance"
    data_source = BARRA_SCHEMA
    description = "Barra 日度风险模型估计"

    source_tables = [
        "barra.factor_returns_daily",
        "barra.specific_returns_daily",
    ]
    
    primary_keys = ["as_of_date"]

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
        """Fetch historical factor returns and specific returns."""
        trade_date_raw = kwargs.get("trade_date")
        if not trade_date_raw:
            raise ValueError("trade_date is required")

        trade_date: dt_date = pd.to_datetime(trade_date_raw).date()
        
        cov_window = kwargs.get("cov_window", 252)
        half_life = kwargs.get("half_life", 126)
        
        await self._ensure_industry_columns_loaded()
        
        # Fetch factor returns for the window
        fr_sql = f"""
            SELECT *
            FROM {BARRA_SCHEMA}.factor_returns_daily
            WHERE trade_date <= $1::date
            ORDER BY trade_date DESC
            LIMIT $2
        """
        fr_rows = await self.db.fetch(fr_sql, trade_date, cov_window)
        
        if not fr_rows:
            self.logger.warning(f"No factor returns found up to {trade_date}")
            return pd.DataFrame()
        
        fr_df = pd.DataFrame([dict(r) for r in fr_rows])
        fr_df = fr_df.sort_values("trade_date")
        
        # Fetch specific returns for the window
        start_date = trade_date - timedelta(days=int(cov_window * 1.5))
        sr_sql = f"""
            SELECT trade_date, ticker, specific_return
            FROM {BARRA_SCHEMA}.specific_returns_daily
            WHERE trade_date BETWEEN $1::date AND $2::date
        """
        sr_rows = await self.db.fetch(sr_sql, start_date, trade_date)
        sr_df = pd.DataFrame([dict(r) for r in sr_rows]) if sr_rows else pd.DataFrame()
        
        # Store for process_data
        self._fr_df = fr_df
        self._sr_df = sr_df
        self._trade_date = trade_date
        self._cov_window = cov_window
        self._half_life = half_life
        
        # Return dummy to trigger process_data
        return pd.DataFrame({"_dummy": [1]})

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        fr_df = getattr(self, "_fr_df", None)
        sr_df = getattr(self, "_sr_df", None)
        trade_date = getattr(self, "_trade_date", None)
        cov_window = getattr(self, "_cov_window", 252)
        half_life = getattr(self, "_half_life", 126)
        
        if fr_df is None or fr_df.empty:
            return pd.DataFrame()
        
        # Configure risk model
        config = RiskModelConfig(
            cov_window=cov_window,
            half_life=half_life,
            min_observations=30,
            newey_west_lags=2,
            specific_var_shrinkage=0.2,
        )
        
        # Estimate factor covariance
        try:
            factor_cov, cov_diag = estimate_factor_covariance(fr_df, config)
        except ValueError as e:
            self.logger.warning(f"Factor covariance estimation failed: {e}")
            return pd.DataFrame()
        
        # Store factor covariance in long format for database
        # Columns: as_of_date, factor1, factor2, covariance, correlation
        cov_records = []
        vols = np.sqrt(np.diag(factor_cov.values))
        
        for i, f1 in enumerate(factor_cov.index):
            for j, f2 in enumerate(factor_cov.columns):
                if i <= j:  # Upper triangle only (symmetric)
                    cov_val = factor_cov.loc[f1, f2]
                    corr_val = cov_val / (vols[i] * vols[j]) if vols[i] > 0 and vols[j] > 0 else 0
                    cov_records.append({
                        "as_of_date": trade_date,
                        "factor1": f1,
                        "factor2": f2,
                        "covariance": float(cov_val),
                        "correlation": float(corr_val),
                        "vol1": float(vols[i]),
                        "vol2": float(vols[j]),
                    })
        
        cov_result = pd.DataFrame(cov_records)
        cov_result["as_of_date"] = pd.to_datetime(cov_result["as_of_date"])
        
        # Store for save_result
        self._cov_result = cov_result
        self._cov_diag = cov_diag
        
        # Estimate specific variance
        if sr_df is not None and not sr_df.empty:
            try:
                spec_var, spec_diag = estimate_specific_variance(sr_df, config)
                spec_var["as_of_date"] = trade_date
                spec_var["as_of_date"] = pd.to_datetime(spec_var["as_of_date"])
                self._spec_var = spec_var
                self._spec_diag = spec_diag
            except ValueError as e:
                self.logger.warning(f"Specific variance estimation failed: {e}")
                self._spec_var = None
        else:
            self._spec_var = None
        
        return cov_result

    async def save_result(self, data: pd.DataFrame, **kwargs):
        cov_result = getattr(self, "_cov_result", None)
        spec_var = getattr(self, "_spec_var", None)
        
        if cov_result is not None and not cov_result.empty:
            await serializer_save_dataframe(
                df=cov_result,
                table_name="factor_covariance",
                db_connection=self.db,
                primary_keys=["as_of_date", "factor1", "factor2"],
                schema=BARRA_SCHEMA,
                if_exists="upsert",
            )
            self.logger.info(f"Saved {len(cov_result)} factor covariance entries")
        
        if spec_var is not None and not spec_var.empty:
            await serializer_save_dataframe(
                df=spec_var,
                table_name="specific_variance_daily",
                db_connection=self.db,
                primary_keys=["as_of_date", "ticker"],
                schema=BARRA_SCHEMA,
                if_exists="upsert",
            )
            self.logger.info(f"Saved {len(spec_var)} specific variance entries")
