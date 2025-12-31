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
  - Industry sum-to-zero via re-parameterization:
      f_ref = -sum_{j!=ref} g_j
      r = sum_{j!=ref} (I_j - I_ref) * g_j + sum_k X_k * f_k + u
- Outputs:
  - barra.factor_returns_daily (one row per trade_date)
  - barra.specific_returns_daily (one row per ticker per trade_date)

Note:
- To avoid look-ahead, production should use exposures from t-1 to explain returns at t.
  MVP uses same-date join to keep the pipeline runnable.
"""

from __future__ import annotations

from datetime import date as dt_date
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ....barra.constants import ALL_STYLE_FACTOR_COLUMNS, BARRA_SCHEMA
from ...utils.serialization import save_dataframe as serializer_save_dataframe


@task_register()
class BarraFactorReturnsDailyTask(ProcessorTaskBase):
    name = "barra_factor_returns_daily"
    table_name = "factor_returns_daily"
    data_source = BARRA_SCHEMA
    description = "Barra 日度因子收益（WLS + 行业sum-to-zero）"

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
        # Only request the factor columns that actually exist in the exposures table.
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
        """Solve WLS via weighted least squares on transformed system.
        
        Minimizes: sum_i w_i * (y_i - x_i @ beta)^2
        
        To achieve this, we multiply both X and y by sqrt(w), then solve OLS:
            X_tilde = sqrt(w) * X
            y_tilde = sqrt(w) * y
            beta = argmin ||y_tilde - X_tilde @ beta||^2
        
        Note: w is expected to be the actual WLS weight (e.g., sqrt(mcap) from exposures),
        so we apply sqrt(w) here to get the correct transformation.
        """
        w = np.asarray(w, dtype=float)
        x = np.asarray(x, dtype=float)
        y = np.asarray(y, dtype=float)

        # Ensure valid positive weights
        w = np.where(np.isfinite(w) & (w > 0), w, 0.0)
        
        # sqrt(w) transformation for WLS: minimizes sum w_i * (y_i - x_i @ beta)^2
        sqrt_w = np.sqrt(w)
        xw = x * sqrt_w[:, None]
        yw = y * sqrt_w

        coef, *_ = np.linalg.lstsq(xw, yw, rcond=None)
        return coef

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        trade_date_raw = kwargs.get("trade_date")
        if not trade_date_raw:
            raise ValueError("trade_date is required (YYYY-MM-DD or YYYYMMDD)")

        trade_date: dt_date = pd.to_datetime(trade_date_raw).date()

        # Support strict PIT: use exposures from previous trade day (t-1) to explain returns on t.
        # If exposure_date is explicitly passed, use it; otherwise default to same day (MVP mode).
        exposure_date_raw = kwargs.get("exposure_date")
        if exposure_date_raw:
            exposure_date: dt_date = pd.to_datetime(exposure_date_raw).date()
        else:
            exposure_date = trade_date  # MVP: same-day join

        await self._ensure_industry_columns_loaded()
        assert self._industry_columns is not None

        await self._ensure_exposure_factor_columns_loaded()
        assert self._exposure_factor_columns is not None

        # Pull exposures from exposure_date + returns from trade_date.
        # pct_chg is in percent; convert to decimal later.
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

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        if data is None or data.empty:
            return pd.DataFrame()

        await self._ensure_industry_columns_loaded()
        assert self._industry_columns is not None

        await self._ensure_exposure_factor_columns_loaded()
        assert self._exposure_factor_columns is not None

        await self._ensure_factor_returns_table_columns_loaded()
        assert self._factor_returns_table_columns is not None

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

        # style matrix: keep columns that have at least some non-null values
        style_used = [c for c in self._exposure_factor_columns if (c in df.columns and df[c].notna().any())]
        x_style = df[style_used].apply(pd.to_numeric, errors="coerce").fillna(0.0).astype(float)

        # industry: sum-to-zero via re-param using last industry as reference
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

        # Compute weighted R² and RMSE using the same weights as WLS
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

        # Map coefficients back
        coef_series = pd.Series(coef, index=list(x.columns))

        out: dict[str, Any] = {
            "trade_date": df["trade_date"].iloc[0],
            "n_obs": int(len(df)),
            "r2": r2,
            "rmse": rmse,
        }

        # Style factor returns
        for c in self._exposure_factor_columns:
            key = f"fr_{c}"
            if key in self._factor_returns_table_columns:
                out[key] = float(coef_series.get(c)) if c in style_used else None

        # Industry factor returns: recover full J returns from J-1 params
        # 
        # Mathematical background (sum-to-zero via re-parameterization):
        # We use the C-matrix transformation from PLAN:
        #   C = [I_{J-1}; -1^T] ∈ R^{J×(J-1)}
        #   f_ind = C @ g, where g ∈ R^{J-1}
        #
        # The difference design X_ind @ C is equivalent to:
        #   X_diff[:, k] = I_k - I_ref (for k = 0..J-2)
        #
        # After regression we get g, and recover f_ind = C @ g:
        #   f_j = g_j for j ∈ {0..J-2}
        #   f_ref = -sum(g)
        #
        # This ensures sum(f_ind) = sum(g) + (-sum(g)) = 0 (sum-to-zero constraint)
        g = {col: float(coef_series.get(col, 0.0)) for col in other_cols}
        f_ref = -float(sum(g.values()))
        if f"fr_{ref_col}" in self._factor_returns_table_columns:
            out[f"fr_{ref_col}"] = f_ref
        for col in other_cols:
            key = f"fr_{col}"
            if key in self._factor_returns_table_columns:
                out[key] = g[col]
        
        # Diagnostic: verify sum-to-zero (should be ~0 within numerical precision)
        industry_returns = [out.get(f"fr_{col}", 0.0) or 0.0 for col in ind_cols]
        sum_industry = sum(industry_returns)
        # NOTE: do not persist this diagnostic column by default, because the
        # wide table schema may not include it. Keep it as a runtime check.
        if abs(sum_industry) > 1e-10:
            self.logger.warning(
                "Industry sum-to-zero check not close to 0: %.6e (trade_date=%s)",
                float(sum_industry),
                str(df["trade_date"].iloc[0]),
            )

        # Save specific returns as a side output (one row per ticker)
        specific_df = pd.DataFrame(
            {
                "trade_date": df["trade_date"],
                "ticker": df["ticker"],
                "raw_return": y.to_numpy(),
                "fitted_return": fitted,
                "specific_return": resid,
                "weight_wls": w,
            }
        )
        # Ensure trade_date is datetime64 for asyncpg (not object/date)
        specific_df["trade_date"] = pd.to_datetime(specific_df["trade_date"])

        # Stash for save_result
        self._specific_df = specific_df

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

        specific_df = getattr(self, "_specific_df", None)
        if isinstance(specific_df, pd.DataFrame) and (not specific_df.empty):
            await serializer_save_dataframe(
                df=specific_df,
                table_name="specific_returns_daily",
                db_connection=self.db,
                primary_keys=["trade_date", "ticker"],
                schema=BARRA_SCHEMA,
                if_exists="upsert",
            )
