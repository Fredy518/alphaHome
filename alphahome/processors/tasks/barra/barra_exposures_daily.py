#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Compute and persist daily Barra exposures (MVP skeleton).

This task currently focuses on:
- pulling required inputs from rawdata
- shaping the output to match barra.exposures_daily wide table

Actual factor computations (beta/momentum/resvol etc.) will be implemented
incrementally; for now this provides the integration points.
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
class BarraExposuresDailyTask(ProcessorTaskBase):
    name = "barra_exposures_daily"
    table_name = "exposures_daily"
    data_source = BARRA_SCHEMA
    description = "Barra 日截面因子暴露（MVP）"

    # Inputs confirmed available in current alphadb
    source_tables = [
        "rawdata.stock_daily",
        "rawdata.stock_dailybasic",
        "barra.pit_sw_industry_member_mv",
        "rawdata.stock_st",
    ]

    primary_keys = ["trade_date", "ticker"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        self.result_table = (config or {}).get("result_table", self.table_name)
        self.table_name = self.result_table

        self._industry_code_to_column: Optional[dict[str, str]] = None
        self._industry_columns: Optional[list[str]] = None

    async def _ensure_industry_dim_loaded(self) -> None:
        if self._industry_code_to_column is not None and self._industry_columns is not None:
            return

        rows = await self.db.fetch(
            f"SELECT l1_code, column_name FROM {BARRA_SCHEMA}.industry_l1_dim ORDER BY l1_code"
        )
        code_to_col: dict[str, str] = {}
        columns: list[str] = []
        for r in rows or []:
            l1_code = (r.get("l1_code") or "").strip()
            col = (r.get("column_name") or "").strip()
            if not l1_code or not col:
                continue
            code_to_col[l1_code] = col
            columns.append(col)

        if not columns:
            raise RuntimeError(
                f"{BARRA_SCHEMA}.industry_l1_dim is empty; run scripts/initialize_barra_schema.py"
            )

        self._industry_code_to_column = code_to_col
        self._industry_columns = columns

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
    def _weighted_zscore(x: pd.Series, w: pd.Series) -> pd.Series:
        x = pd.to_numeric(x, errors="coerce")
        w = pd.to_numeric(w, errors="coerce")
        mask = x.notna() & w.notna() & (w > 0)
        if mask.sum() < 2:
            return x * np.nan

        xv = x[mask].astype(float)
        wv = w[mask].astype(float)
        wsum = float(wv.sum())
        if wsum <= 0:
            return x * np.nan

        mean = float((xv * wv).sum() / wsum)
        var = float(((xv - mean) ** 2 * wv).sum() / wsum)
        std = float(np.sqrt(var))
        if not np.isfinite(std) or std <= 0:
            return x * np.nan

        out = (x - mean) / std
        return out

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        trade_date_raw = kwargs.get("trade_date")
        if not trade_date_raw:
            raise ValueError("trade_date is required (YYYY-MM-DD or YYYYMMDD)")

        trade_date: dt_date = pd.to_datetime(trade_date_raw).date()

        # Keep SQL simple: join price + dailybasic + PIT industry as-of trade_date
        query = """
        WITH base AS (
          SELECT
            sd.trade_date::date as trade_date,
            sd.ts_code as ticker,
            sd.pct_chg::double precision as pct_chg,
            sdb.circ_mv::double precision as ff_mcap,
            sdb.turnover_rate_f::double precision as turnover_rate_f,
            sdb.pb::double precision as pb
          FROM rawdata.stock_daily sd
          LEFT JOIN rawdata.stock_dailybasic sdb
            ON sdb.ts_code = sd.ts_code AND sdb.trade_date = sd.trade_date
          WHERE sd.trade_date = $1::date
        ), ind AS (
                    SELECT DISTINCT ON (ts_code)
                        ts_code,
                        l1_code
                    FROM barra.pit_sw_industry_member_mv
                    WHERE $1::date BETWEEN query_start_date AND query_end_date
                    ORDER BY ts_code, query_start_date DESC
        )
        SELECT
          b.trade_date,
          b.ticker,
          b.ff_mcap,
          SQRT(NULLIF(b.ff_mcap, 0)) as weight_wls,
          i.l1_code as industry_l1_code,
          b.turnover_rate_f,
          b.pb
        FROM base b
        LEFT JOIN ind i ON i.ts_code = b.ticker;
        """

        rows = await self.db.fetch(query, trade_date)
        if not rows:
            return pd.DataFrame()

        return pd.DataFrame([dict(r) for r in rows])

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        if data is None or data.empty:
            return pd.DataFrame()

        df = data.copy()
        df["eligible_flag"] = df["ff_mcap"].notna()

        # MVP: style_size, style_value, style_liquidity
        df["style_size"] = df["ff_mcap"].apply(
            lambda x: None if pd.isna(x) or x <= 0 else float(np.log(x))
        )
        df["style_value"] = df["pb"].apply(
            lambda x: None if pd.isna(x) or x == 0 else float(1.0 / x)
        )
        df["style_liquidity"] = pd.to_numeric(df["turnover_rate_f"], errors="coerce").astype(float)

        # Unimplemented MVP style factors
        for col in STYLE_FACTOR_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # 1) Winsorize + (mcap-weighted) z-score for implemented style factors
        w = df["ff_mcap"]
        for col in ["style_size", "style_value", "style_liquidity"]:
            s = df.loc[df["eligible_flag"], col]
            s = self._winsorize_series(s, lower_q=0.01, upper_q=0.99)
            z = self._weighted_zscore(s, w.loc[df["eligible_flag"]])
            df.loc[df["eligible_flag"], col] = z

        # 2) Expand SW L1 industry one-hot columns (0/1)
        await self._ensure_industry_dim_loaded()
        assert self._industry_code_to_column is not None
        assert self._industry_columns is not None

        for c in self._industry_columns:
            if c not in df.columns:
                df[c] = 0

        df["_ind_col"] = df["industry_l1_code"].map(self._industry_code_to_column)
        if df["_ind_col"].notna().any():
            for col_name, idx in df[df["_ind_col"].notna()].groupby("_ind_col").groups.items():
                if col_name in df.columns:
                    df.loc[idx, col_name] = 1

        df.drop(columns=["_ind_col"], inplace=True)

        keep_cols = [
            "trade_date",
            "ticker",
            "eligible_flag",
            "ff_mcap",
            "weight_wls",
            "industry_l1_code",
            *STYLE_FACTOR_COLUMNS,
            *self._industry_columns,
        ]
        result = df[keep_cols].copy()
        # Ensure trade_date is datetime64 for asyncpg (not object/date)
        result["trade_date"] = pd.to_datetime(result["trade_date"])
        # Drop duplicates on primary key (PIT join may yield dups if multiple industry records)
        result = result.drop_duplicates(subset=["trade_date", "ticker"], keep="last")
        result = result.reset_index(drop=True)
        return result

    async def save_result(self, data: pd.DataFrame, **kwargs):
        if data is None or data.empty:
            self.logger.warning("没有数据需要保存")
            return

        # Save to barra.exposures_daily (partitioned table). Upsert by PK.
        await serializer_save_dataframe(
            df=data,
            table_name=self.table_name,
            db_connection=self.db,
            primary_keys=self.primary_keys,
            schema=BARRA_SCHEMA,
            if_exists="upsert",
        )
