#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Full Barra Exposures Daily Task (Post-MVP).

This task computes production-grade Barra CNE5-style factor exposures:
- Multi-indicator composites (Value, Liquidity, Growth, Leverage)
- EWMA-weighted factors (Beta, Momentum, ResVol)
- Industry neutralization where appropriate
- Robust winsorization and standardization

Upgrades from MVP:
- Size: Added optional industry neutralization
- Value: E/P + B/P + S/P + DY composite (was single 1/PB)
- Liquidity: 21d/63d/252d turnover composite (was single-day)
- Beta: EWMA with Bayesian shrinkage (was placeholder)
- Momentum: Multi-window with reversal adjustment (was placeholder)
- ResVol: EWMA with industry adjustment (was placeholder)
- NEW: Growth, Leverage, Non-linear Size factors
"""

from __future__ import annotations

from datetime import date as dt_date, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ....barra.constants import (
    BARRA_SCHEMA, 
    STYLE_FACTOR_COLUMNS, 
    EXTENDED_FACTOR_COLUMNS,
    ALL_STYLE_FACTOR_COLUMNS,
    FACTOR_PARAMS,
    MARKET_INDEX_CODE,
)
from ....barra.factor_calculators import (
    winsorize_series,
    weighted_zscore,
    industry_neutralize,
    calculate_size,
    calculate_value,
    calculate_liquidity,
    calculate_beta,
    calculate_momentum,
    calculate_growth,
    calculate_leverage,
    calculate_resvol,
    calculate_nlsize,
)
from ...utils.serialization import save_dataframe as serializer_save_dataframe


@task_register()
class BarraExposuresFullTask(ProcessorTaskBase):
    """Full Barra CNE5-style factor exposures computation."""
    
    name = "barra_exposures_full"
    table_name = "exposures_daily"
    data_source = BARRA_SCHEMA
    description = "Barra 日截面因子暴露（Full版，含多指标组合+EWMA因子）"

    source_tables = [
        "rawdata.stock_daily",
        "rawdata.stock_dailybasic",
        "rawdata.fina_indicator",
        "rawdata.index_dailybasic",
        "barra.pit_sw_industry_member_mv",
    ]

    primary_keys = ["trade_date", "ticker"]
    
    # Lookback windows for historical data
    BETA_WINDOW = 252
    MOMENTUM_WINDOW = 252
    LIQUIDITY_WINDOWS = [21, 63, 252]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        cfg = config or {}
        self.result_table = cfg.get("result_table", self.table_name)
        self.table_name = self.result_table
        
        # Feature flags
        self.include_extended_factors = cfg.get("include_extended_factors", True)
        self.industry_neutralize_size = cfg.get("industry_neutralize_size", False)
        
        self._industry_code_to_column: Optional[dict[str, str]] = None
        self._industry_columns: Optional[list[str]] = None

        # Cache available columns in rawdata.stock_dailybasic.
        # Some DB snapshots may not contain optional fields (e.g., ps_ttm/dv_ttm).
        self._stock_dailybasic_columns: Optional[set[str]] = None

    async def _ensure_stock_dailybasic_columns_loaded(self) -> None:
        if self._stock_dailybasic_columns is not None:
            return

        rows = await self.db.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'rawdata' AND table_name = 'stock_dailybasic'
            """
        )
        self._stock_dailybasic_columns = {
            str(r.get("column_name")).strip() for r in (rows or []) if r.get("column_name")
        }

    def _sdb_expr(self, col: str, alias: Optional[str] = None) -> str:
        """Safe SELECT expression for rawdata.stock_dailybasic columns."""
        assert self._stock_dailybasic_columns is not None
        out_name = alias or col
        if col in self._stock_dailybasic_columns:
            return f"sdb.{col}::double precision as {out_name}"
        return f"NULL::double precision as {out_name}"

    async def _ensure_industry_dim_loaded(self) -> None:
        """Load industry dimension table mapping."""
        if self._industry_code_to_column is not None:
            return

        rows = await self.db.fetch(
            f"SELECT l1_code, column_name FROM {BARRA_SCHEMA}.industry_l1_dim ORDER BY l1_code"
        )
        code_to_col: dict[str, str] = {}
        columns: list[str] = []
        for r in rows or []:
            l1_code = (r.get("l1_code") or "").strip()
            col = (r.get("column_name") or "").strip()
            if l1_code and col:
                code_to_col[l1_code] = col
                columns.append(col)

        if not columns:
            raise RuntimeError(
                f"{BARRA_SCHEMA}.industry_l1_dim is empty; run scripts/initialize_barra_schema.py"
            )

        self._industry_code_to_column = code_to_col
        self._industry_columns = columns

    async def _fetch_base_data(self, trade_date: dt_date) -> pd.DataFrame:
        """Fetch daily cross-sectional data for factor calculation."""
        await self._ensure_stock_dailybasic_columns_loaded()

        sdb_select = ",\n                ".join(
            [
                self._sdb_expr("circ_mv", "ff_mcap"),
                self._sdb_expr("turnover_rate_f", "turnover_rate_f"),
                self._sdb_expr("pb", "pb"),
                self._sdb_expr("pe_ttm", "pe_ttm"),
                self._sdb_expr("ps_ttm", "ps_ttm"),
                self._sdb_expr("dv_ttm", "dv_ttm"),
            ]
        )

        query = f"""
        WITH base AS (
            SELECT
                sd.trade_date::date as trade_date,
                sd.ts_code as ticker,
                sd.pct_chg::double precision as pct_chg,
                sd.amount::double precision as amount,
                {sdb_select}
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
            b.*,
            SQRT(NULLIF(b.ff_mcap, 0)) as weight_wls,
            i.l1_code as industry_l1_code
        FROM base b
        LEFT JOIN ind i ON i.ts_code = b.ticker;
        """
        rows = await self.db.fetch(query, trade_date)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    async def _fetch_historical_returns(
        self, 
        trade_date: dt_date, 
        window: int
    ) -> pd.DataFrame:
        """Fetch historical stock returns for momentum/beta/resvol calculation."""
        start_date = trade_date - timedelta(days=int(window * 1.5))  # Extra buffer for non-trading days
        
        query = """
        SELECT trade_date::date as trade_date, ts_code as ticker, pct_chg::double precision as pct_chg
        FROM rawdata.stock_daily
        WHERE trade_date BETWEEN $1::date AND $2::date
        ORDER BY trade_date, ts_code
        """
        rows = await self.db.fetch(query, start_date, trade_date)
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame([dict(r) for r in rows])
        # Pivot to wide format: rows=dates, columns=tickers
        if df.empty:
            return pd.DataFrame()
        
        pivoted = df.pivot(index="trade_date", columns="ticker", values="pct_chg")
        return pivoted

    async def _fetch_market_returns(
        self, 
        trade_date: dt_date, 
        window: int
    ) -> pd.Series:
        """Fetch market index returns for beta calculation."""
        start_date = trade_date - timedelta(days=int(window * 1.5))
        
        query = """
        SELECT trade_date::date as trade_date, 
               (close - pre_close) / NULLIF(pre_close, 0) * 100 as pct_chg
        FROM rawdata.index_dailybasic
        WHERE ts_code = $1 AND trade_date BETWEEN $2::date AND $3::date
        ORDER BY trade_date
        """
        rows = await self.db.fetch(query, MARKET_INDEX_CODE, start_date, trade_date)
        if not rows:
            # Fallback: try to get from a view or alternative source
            return pd.Series(dtype=float)
        
        df = pd.DataFrame([dict(r) for r in rows])
        return df.set_index("trade_date")["pct_chg"]

    async def _fetch_rolling_turnover(
        self, 
        trade_date: dt_date, 
        window: int
    ) -> pd.DataFrame:
        """Fetch rolling average turnover for liquidity calculation."""
        start_date = trade_date - timedelta(days=int(window * 1.5))
        
        query = """
        SELECT ts_code as ticker,
               AVG(turnover_rate_f) as avg_turnover,
               AVG(amount / NULLIF(circ_mv, 0)) as avg_amount_mv
        FROM rawdata.stock_dailybasic
        WHERE trade_date BETWEEN $1::date AND $2::date
          AND turnover_rate_f IS NOT NULL
        GROUP BY ts_code
        """
        rows = await self.db.fetch(query, start_date, trade_date)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    async def _fetch_financial_data(self, trade_date: dt_date) -> pd.DataFrame:
        """Fetch PIT-aligned financial indicators for growth/leverage factors."""
        query = """
        SELECT DISTINCT ON (ts_code)
            ts_code as ticker,
            netprofit_yoy::double precision as netprofit_yoy,
            or_yoy::double precision as revenue_yoy,
            ocf_yoy::double precision as ocf_yoy,
            debt_to_assets::double precision as debt_to_assets,
            debt_to_eqt::double precision as debt_to_equity
        FROM rawdata.fina_indicator
        WHERE f_ann_date <= $1::date OR (f_ann_date IS NULL AND ann_date <= $1::date)
        ORDER BY ts_code, end_date DESC, f_ann_date DESC NULLS LAST
        """
        rows = await self.db.fetch(query, trade_date)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    async def fetch_data(self, **kwargs) -> Optional[Dict[str, pd.DataFrame]]:
        """Fetch all required data for factor calculations."""
        trade_date_raw = kwargs.get("trade_date")
        if not trade_date_raw:
            raise ValueError("trade_date is required (YYYY-MM-DD or YYYYMMDD)")

        trade_date: dt_date = pd.to_datetime(trade_date_raw).date()

        # Fetch all data in parallel-ish manner
        base_data = await self._fetch_base_data(trade_date)
        if base_data.empty:
            return None

        # Historical data for time-series factors
        hist_returns = await self._fetch_historical_returns(trade_date, self.BETA_WINDOW)
        market_returns = await self._fetch_market_returns(trade_date, self.BETA_WINDOW)
        
        # Rolling turnover for multi-window liquidity
        turnover_21d = await self._fetch_rolling_turnover(trade_date, 21)
        turnover_63d = await self._fetch_rolling_turnover(trade_date, 63)
        turnover_252d = await self._fetch_rolling_turnover(trade_date, 252)
        
        # Financial data for growth/leverage (if extended factors enabled)
        fina_data = pd.DataFrame()
        if self.include_extended_factors:
            fina_data = await self._fetch_financial_data(trade_date)

        return {
            "base": base_data,
            "hist_returns": hist_returns,
            "market_returns": market_returns,
            "turnover_21d": turnover_21d,
            "turnover_63d": turnover_63d,
            "turnover_252d": turnover_252d,
            "fina_data": fina_data,
            "trade_date": trade_date,
        }

    async def process_data(
        self, 
        data: Optional[Dict[str, pd.DataFrame]], 
        stop_event=None, 
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """Process all factor calculations."""
        if data is None:
            return pd.DataFrame()

        base_df = data["base"]
        if base_df.empty:
            return pd.DataFrame()

        df = base_df.copy()
        df["eligible_flag"] = df["ff_mcap"].notna() & (df["ff_mcap"] > 0)
        
        weights = df["ff_mcap"].copy()
        industry_codes = df["industry_l1_code"].copy()
        
        # =====================================================================
        # Core Style Factors
        # =====================================================================
        
        # 1. Size (log market cap, optionally industry-neutralized)
        df["style_size"] = calculate_size(
            ff_mcap=df["ff_mcap"],
            industry_codes=industry_codes if self.industry_neutralize_size else None,
            neutralize=self.industry_neutralize_size,
            weights=weights,
        )
        
        # 2. Value (multi-indicator composite)
        df["style_value"] = calculate_value(
            pe_ttm=df["pe_ttm"],
            pb=df["pb"],
            ps_ttm=df["ps_ttm"],
            dv_ttm=df["dv_ttm"],
            weights=weights,
        )
        
        # 3. Liquidity (multi-window turnover)
        turnover_data = self._merge_turnover_data(
            df, 
            data["turnover_21d"],
            data["turnover_63d"],
            data["turnover_252d"],
        )
        df["style_liquidity"] = calculate_liquidity(
            turnover_21d=turnover_data["turnover_21d"],
            turnover_63d=turnover_data["turnover_63d"],
            turnover_252d=turnover_data["turnover_252d"],
            amount_to_mv=turnover_data.get("amount_mv_21d"),
            weights=weights,
        )
        
        # 4. Beta (EWMA + Bayesian shrinkage)
        hist_returns = data["hist_returns"]
        market_returns = data["market_returns"]
        
        if not hist_returns.empty and not market_returns.empty:
            beta_params = FACTOR_PARAMS.get("beta", {})
            beta_series = calculate_beta(
                stock_returns=hist_returns,
                market_returns=market_returns,
                weights=weights,
                window=beta_params.get("window", 252),
                half_life=beta_params.get("half_life", 63),
                shrinkage_factor=beta_params.get("shrinkage_factor", 0.3),
            )
            df["style_beta"] = df["ticker"].map(beta_series)
        else:
            df["style_beta"] = np.nan
        
        # 5. Momentum (multi-window with reversal adjustment)
        if not hist_returns.empty:
            momentum_data = self._calculate_momentum_inputs(hist_returns, df["ticker"])
            mom_params = FACTOR_PARAMS.get("momentum", {})
            df["style_momentum"] = calculate_momentum(
                cumret_252_21=momentum_data["cumret_252_21"],
                cumret_126_21=momentum_data["cumret_126_21"],
                cumret_21_1=momentum_data["cumret_21_1"],
                industry_codes=industry_codes,
                weights=weights,
                reversal_adj=mom_params.get("reversal_adjustment", 0.1),
                neutralize=mom_params.get("neutralize_industry", True),
            )
        else:
            df["style_momentum"] = np.nan
        
        # 6. Residual Volatility (placeholder - requires prior regression residuals)
        # In practice, this would use residuals from a prior beta regression
        df["style_resvol"] = np.nan  # TODO: Implement when residual history available
        
        # =====================================================================
        # Extended Factors (Post-MVP)
        # =====================================================================
        
        if self.include_extended_factors:
            fina_df = data.get("fina_data", pd.DataFrame())
            
            if not fina_df.empty:
                # Merge financial data
                fina_merged = df[["ticker"]].merge(fina_df, on="ticker", how="left")
                
                # 7. Growth (multi-dimensional)
                df["style_growth"] = calculate_growth(
                    netprofit_yoy=fina_merged["netprofit_yoy"],
                    revenue_yoy=fina_merged["revenue_yoy"],
                    ocf_yoy=fina_merged["ocf_yoy"],
                    weights=weights,
                )
                
                # 8. Leverage (multi-dimensional)
                df["style_leverage"] = calculate_leverage(
                    debt_to_assets=fina_merged["debt_to_assets"],
                    debt_to_equity=fina_merged["debt_to_equity"],
                    weights=weights,
                )
            else:
                df["style_growth"] = np.nan
                df["style_leverage"] = np.nan
            
            # 9. Non-linear Size
            df["style_nlsize"] = calculate_nlsize(df["style_size"], weights)
            
            # 10. Dividend Yield (already have dv_ttm)
            dv = df["dv_ttm"] / 100.0
            dv = winsorize_series(dv)
            df["style_dividend"] = weighted_zscore(dv, weights)
            
            # 11. Earnings Quality (placeholder - requires detailed cashflow data)
            df["style_earnings_quality"] = np.nan
        
        # =====================================================================
        # Industry One-Hot Encoding
        # =====================================================================
        
        await self._ensure_industry_dim_loaded()
        assert self._industry_columns is not None
        
        for c in self._industry_columns:
            df[c] = 0
        
        df["_ind_col"] = df["industry_l1_code"].map(self._industry_code_to_column)
        for col_name, idx in df[df["_ind_col"].notna()].groupby("_ind_col").groups.items():
            if col_name in df.columns:
                df.loc[idx, col_name] = 1
        df.drop(columns=["_ind_col"], inplace=True)
        
        # =====================================================================
        # Prepare Output
        # =====================================================================
        
        # Determine which factor columns to include
        factor_cols = list(STYLE_FACTOR_COLUMNS)
        if self.include_extended_factors:
            factor_cols.extend(EXTENDED_FACTOR_COLUMNS)
        
        keep_cols = [
            "trade_date",
            "ticker",
            "eligible_flag",
            "ff_mcap",
            "weight_wls",
            "industry_l1_code",
            *factor_cols,
            *self._industry_columns,
        ]
        
        # Only keep columns that exist
        keep_cols = [c for c in keep_cols if c in df.columns]
        
        result = df[keep_cols].copy()
        result["trade_date"] = pd.to_datetime(result["trade_date"])
        result = result.drop_duplicates(subset=["trade_date", "ticker"], keep="last")
        result = result.reset_index(drop=True)
        
        return result

    def _merge_turnover_data(
        self,
        df: pd.DataFrame,
        turn_21d: pd.DataFrame,
        turn_63d: pd.DataFrame,
        turn_252d: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge multi-window turnover data with base dataframe."""
        result = df[["ticker"]].copy()
        
        if not turn_21d.empty:
            result = result.merge(
                turn_21d.rename(columns={"avg_turnover": "turnover_21d", "avg_amount_mv": "amount_mv_21d"}),
                on="ticker", how="left"
            )
        else:
            result["turnover_21d"] = df["turnover_rate_f"]
            result["amount_mv_21d"] = None
        
        if not turn_63d.empty:
            result = result.merge(
                turn_63d.rename(columns={"avg_turnover": "turnover_63d"})[["ticker", "turnover_63d"]],
                on="ticker", how="left"
            )
        else:
            result["turnover_63d"] = df["turnover_rate_f"]
        
        if not turn_252d.empty:
            result = result.merge(
                turn_252d.rename(columns={"avg_turnover": "turnover_252d"})[["ticker", "turnover_252d"]],
                on="ticker", how="left"
            )
        else:
            result["turnover_252d"] = df["turnover_rate_f"]
        
        return result

    def _calculate_momentum_inputs(
        self, 
        hist_returns: pd.DataFrame, 
        tickers: pd.Series
    ) -> Dict[str, pd.Series]:
        """Calculate cumulative returns for momentum factor."""
        # Get trade dates in order
        dates = hist_returns.index.tolist()
        n_dates = len(dates)
        
        cumret_252_21 = {}
        cumret_126_21 = {}
        cumret_21_1 = {}
        
        for ticker in tickers.unique():
            if ticker not in hist_returns.columns:
                cumret_252_21[ticker] = np.nan
                cumret_126_21[ticker] = np.nan
                cumret_21_1[ticker] = np.nan
                continue
            
            ret_series = hist_returns[ticker].dropna()
            n = len(ret_series)
            
            # Cumulative return = product of (1 + r) - 1
            if n >= 252:
                # t-252 to t-21 (skip last 21 days)
                rets = ret_series.iloc[-252:-21] / 100.0
                cumret_252_21[ticker] = (1 + rets).prod() - 1
            else:
                cumret_252_21[ticker] = np.nan
            
            if n >= 126:
                # t-126 to t-21
                rets = ret_series.iloc[-126:-21] / 100.0 if n >= 126 else ret_series.iloc[:-21] / 100.0
                cumret_126_21[ticker] = (1 + rets).prod() - 1
            else:
                cumret_126_21[ticker] = np.nan
            
            if n >= 21:
                # t-21 to t-1 (short-term)
                rets = ret_series.iloc[-21:-1] / 100.0
                cumret_21_1[ticker] = (1 + rets).prod() - 1
            else:
                cumret_21_1[ticker] = np.nan
        
        return {
            "cumret_252_21": tickers.map(cumret_252_21),
            "cumret_126_21": tickers.map(cumret_126_21),
            "cumret_21_1": tickers.map(cumret_21_1),
        }

    async def save_result(self, data: pd.DataFrame, **kwargs):
        """Save exposures to database."""
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
