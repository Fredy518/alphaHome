#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Compute and persist daily Barra factor returns via cross-sectional WLS.

MVP scope:
- Inputs:
  - barra.exposures_daily (style exposures + industry one-hot + weight_wls)
  - rawdata.stock_daily (pct_chg as raw return)
- Method:
  - Winsorize returns (1%/99%)
  - WLS with weights = weight_wls (typically sqrt(mcap))
  - Industry sum-to-zero via re-parameterization
- Outputs:
  - barra.factor_returns_daily (one row per trade_date)
  - barra.specific_returns_daily (one row per ticker per trade_date)

此版本独立于 processors 模块，直接继承 common/task_system 的 BaseTask。
"""

from __future__ import annotations

from datetime import date as dt_date
from typing import Any, Dict, Optional, List

import numpy as np
import pandas as pd

from alphahome.common.task_system import BaseTask, task_register
from alphahome.barra.constants import ALL_STYLE_FACTOR_COLUMNS, BARRA_SCHEMA
from .serialization import save_dataframe


@task_register()
class BarraFactorReturnsDailyTask(BaseTask):
    """Barra 日度因子收益（WLS + 行业sum-to-zero）。"""
    
    name = "barra_factor_returns_daily"
    table_name = "factor_returns_daily"
    data_source = BARRA_SCHEMA
    description = "Barra 日度因子收益（WLS + 行业sum-to-zero）"
    task_type = "barra"

    source_tables = [
        "barra.exposures_daily",
        "barra.industry_l1_dim",
        "rawdata.stock_daily",
    ]

    primary_keys = ["trade_date"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        self._industry_columns: Optional[list[str]] = None
        self._exposure_factor_columns: Optional[list[str]] = None
        self._factor_returns_table_columns: Optional[set[str]] = None

    async def _ensure_exposure_factor_columns_loaded(self) -> None:
        if self._exposure_factor_columns is not None:
            return

        rows = await self.db.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            BARRA_SCHEMA,
            "exposures_daily",
        )
        cols = {str(r.get("column_name")).strip() for r in (rows or []) if r.get("column_name")}
        self._exposure_factor_columns = [c for c in ALL_STYLE_FACTOR_COLUMNS if c in cols]

    async def _ensure_factor_returns_table_columns_loaded(self) -> None:
        if self._factor_returns_table_columns is not None:
            return

        rows = await self.db.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            BARRA_SCHEMA,
            self.table_name,
        )
        self._factor_returns_table_columns = {
            str(r.get("column_name")).strip() for r in (rows or []) if r.get("column_name")
        }

    async def _ensure_industry_columns_loaded(self) -> None:
        if self._industry_columns is not None:
            return
        rows = await self.db.fetch(
            f"SELECT column_name FROM {BARRA_SCHEMA}.industry_l1_dim ORDER BY l1_code"
        )
        cols = [str(r.get("column_name")).strip() for r in (rows or []) if r.get("column_name")]
        if not cols:
            raise RuntimeError(f"{BARRA_SCHEMA}.industry_l1_dim is empty; run scripts/initialize_barra_schema.py")
        self._industry_columns = cols

    @staticmethod
    def _winsorize_series(s: pd.Series, lower_q: float = 0.01, upper_q: float = 0.99) -> pd.Series:
        s = pd.to_numeric(s, errors="coerce")
        valid = s.dropna()
        if valid.empty:
            return s
        lo = float(valid.quantile(lower_q))
        hi = float(valid.quantile(upper_q))
        return s.clip(lower=lo, upper=hi)

    @staticmethod
    def _wls_solve(x: np.ndarray, y: np.ndarray, w: np.ndarray) -> np.ndarray:
        """Solve WLS via weighted least squares on transformed system."""
        w = np.asarray(w, dtype=float)
        x = np.asarray(x, dtype=float)
        y = np.asarray(y, dtype=float)

        w = np.where(np.isfinite(w) & (w > 0), w, 0.0)
        
        sqrt_w = np.sqrt(w)
        xw = x * sqrt_w[:, None]
        yw = y * sqrt_w

        coef, *_ = np.linalg.lstsq(xw, yw, rcond=None)
        return coef

    async def _fetch_data(self, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        """Fetch exposures and returns data."""
        trade_date_raw = kwargs.get("trade_date")
        if not trade_date_raw:
            raise ValueError("trade_date is required (YYYY-MM-DD or YYYYMMDD)")

        trade_date: dt_date = pd.to_datetime(trade_date_raw).date()

        # Support strict PIT: use exposures from previous trade day (t-1)
        exposure_date_raw = kwargs.get("exposure_date")
        if exposure_date_raw:
            exposure_date: dt_date = pd.to_datetime(exposure_date_raw).date()
        else:
            exposure_date = trade_date

        await self._ensure_industry_columns_loaded()
        assert self._industry_columns is not None

        await self._ensure_exposure_factor_columns_loaded()
        assert self._exposure_factor_columns is not None

        cols = [
            "e.trade_date as exposure_date",
            "e.ticker",
            "e.eligible_flag",
            "e.weight_wls",
            "sd.pct_chg::double precision as pct_chg",
        ]
        cols += [f"e.{c}" for c in self._exposure_factor_columns]
        cols += [f"e.{c}" for c in self._industry_columns]

        sql = f"""
        SELECT
          $2::date as trade_date,
          {', '.join(cols)}
        FROM {BARRA_SCHEMA}.exposures_daily e
        JOIN rawdata.stock_daily sd
          ON sd.ts_code = e.ticker AND sd.trade_date = $2::date
        WHERE e.trade_date = $1::date;
        """

        rows = await self.db.fetch(sql, exposure_date, trade_date)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        """Perform WLS regression to compute factor returns."""
        if data is None or data.empty:
            return pd.DataFrame()

        # Note: Need industry columns loaded (will be set by _fetch_data)
        if self._industry_columns is None or self._exposure_factor_columns is None:
            # This shouldn't happen in normal flow
            return pd.DataFrame()

        df = data.copy()

        # Filter eligible + required fields
        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
        df["weight_wls"] = pd.to_numeric(df["weight_wls"], errors="coerce")
        df = df[df["eligible_flag"] == True]  # noqa: E712
        df = df[df["pct_chg"].notna() & df["weight_wls"].notna() & (df["weight_wls"] > 0)]
        if df.empty:
            return pd.DataFrame()

        # y: decimal return
        y = (df["pct_chg"] / 100.0).astype(float)
        y = self._winsorize_series(y, 0.01, 0.99).astype(float)

        # style matrix
        style_used = [c for c in self._exposure_factor_columns if (c in df.columns and df[c].notna().any())]
        x_style = df[style_used].apply(pd.to_numeric, errors="coerce").fillna(0.0).astype(float)

        # industry: sum-to-zero via re-param
        ind_cols = list(self._industry_columns)
        if len(ind_cols) < 2:
            raise RuntimeError("Need >=2 industries to apply sum-to-zero re-parameterization")

        x_ind = df[ind_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).astype(float)
        ref_col = ind_cols[-1]
        other_cols = ind_cols[:-1]
        x_ind_diff = x_ind[other_cols].subtract(x_ind[ref_col], axis=0)

        x = pd.concat([x_style, x_ind_diff], axis=1)
        if x.shape[1] == 0:
            return pd.DataFrame()

        w = df["weight_wls"].astype(float).to_numpy()
        coef = self._wls_solve(x.to_numpy(), y.to_numpy(), w)

        fitted = x.to_numpy() @ coef
        resid = y.to_numpy() - fitted

        # Compute R² and RMSE
        wsum = float(np.sum(w))
        if wsum <= 0:
            r2 = None
            rmse = None
        else:
            ybar = float(np.sum(w * y.to_numpy()) / wsum)
            sse = float(np.sum(w * resid**2))
            sst = float(np.sum(w * (y.to_numpy() - ybar) ** 2))
            r2 = None if sst <= 0 else float(1.0 - sse / sst)
            rmse = float(np.sqrt(sse / wsum))

        coef_series = pd.Series(coef, index=list(x.columns))

        out: dict[str, Any] = {
            "trade_date": df["trade_date"].iloc[0],
            "n_obs": int(len(df)),
            "r2": r2,
            "rmse": rmse,
        }

        # Style factor returns (will be filled during save)
        self._style_used = style_used
        self._coef_series = coef_series
        self._other_cols = other_cols
        self._ref_col = ref_col
        self._ind_cols = ind_cols

        # Store for save
        self._out = out
        self._df = df
        self._y = y
        self._fitted = fitted
        self._resid = resid
        self._w = w

        result_df = pd.DataFrame([out])
        result_df["trade_date"] = pd.to_datetime(result_df["trade_date"])
        return result_df

    async def _save_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Dict[str, Any]:
        """Save factor returns and specific returns."""
        if data is None or data.empty:
            self.logger.warning("没有数据需要保存")
            return {"rows_saved": 0}

        await self._ensure_factor_returns_table_columns_loaded()
        assert self._factor_returns_table_columns is not None

        out = self._out
        coef_series = self._coef_series
        style_used = self._style_used
        other_cols = self._other_cols
        ref_col = self._ref_col
        ind_cols = self._ind_cols

        # Style factor returns
        for c in self._exposure_factor_columns:
            key = f"fr_{c}"
            if key in self._factor_returns_table_columns:
                out[key] = float(coef_series.get(c)) if c in style_used else None

        # Industry factor returns: recover full J returns from J-1 params
        g = {col: float(coef_series.get(col, 0.0)) for col in other_cols}
        f_ref = -float(sum(g.values()))
        if f"fr_{ref_col}" in self._factor_returns_table_columns:
            out[f"fr_{ref_col}"] = f_ref
        for col in other_cols:
            key = f"fr_{col}"
            if key in self._factor_returns_table_columns:
                out[key] = g[col]

        # Verify sum-to-zero
        industry_returns = [out.get(f"fr_{col}", 0.0) or 0.0 for col in ind_cols]
        sum_industry = sum(industry_returns)
        if abs(sum_industry) > 1e-10:
            self.logger.warning(
                "Industry sum-to-zero check not close to 0: %.6e (trade_date=%s)",
                float(sum_industry),
                str(out["trade_date"]),
            )

        # Specific returns
        specific_df = pd.DataFrame(
            {
                "trade_date": self._df["trade_date"],
                "ticker": self._df["ticker"],
                "raw_return": self._y.to_numpy(),
                "fitted_return": self._fitted,
                "specific_return": self._resid,
                "weight_wls": self._w,
            }
        )
        specific_df["trade_date"] = pd.to_datetime(specific_df["trade_date"])

        # Save factor returns
        result_df = pd.DataFrame([out])
        result_df["trade_date"] = pd.to_datetime(result_df["trade_date"])

        await save_dataframe(
            df=result_df,
            table_name=self.table_name,
            db_connection=self.db,
            primary_keys=self.primary_keys,
            schema=BARRA_SCHEMA,
            if_exists="upsert",
        )

        # Save specific returns
        await save_dataframe(
            df=specific_df,
            table_name="specific_returns_daily",
            db_connection=self.db,
            primary_keys=["trade_date", "ticker"],
            schema=BARRA_SCHEMA,
            if_exists="upsert",
        )

        return {
            "r2": out.get("r2"),
            "n_obs": out.get("n_obs"),
        }
