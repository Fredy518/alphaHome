"""Reusable monthly EXPMA atoms for the self-built all-A index."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from alphahome.features.registry import feature_register

from .all_a_index_daily import (
    SERIES_IDS,
    _AuditedAtomicFullRefreshFeature,
    _records_to_frame,
    _require,
)

METHOD_VERSION = "all_a_expma_monthly_12_120_v1"


def build_monthly_expma_atoms(
    daily: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    *,
    calculation_run_id: uuid.UUID,
    calculated_at: datetime,
) -> pd.DataFrame:
    """Build complete-month OHLC and 12/120-span EMA atoms for each series."""

    required = {
        "series_id",
        "variant",
        "trade_date",
        "open",
        "close",
        "is_seed",
        "source_data_as_of",
        "calculation_run_id",
    }
    _require(
        required.issubset(daily.columns), "daily all-A source schema is incomplete"
    )
    daily = daily.copy()
    daily["trade_date"] = pd.to_datetime(daily["trade_date"])
    calendar = pd.DatetimeIndex(calendar).sort_values()
    _require(not calendar.has_duplicates, "duplicate exchange calendar")

    schedule = pd.DataFrame({"trade_date": calendar, "period": calendar.to_period("M")})
    expected_counts = schedule.groupby("period").size()
    expected_ends = schedule.groupby("period")["trade_date"].max()
    records: list[pd.DataFrame] = []

    for series_id, group in daily.groupby("series_id", sort=True):
        group = group.sort_values("trade_date").reset_index(drop=True)
        _require(
            not group["trade_date"].duplicated().any(), f"duplicate dates: {series_id}"
        )
        _require(group["close"].gt(0).all(), f"nonpositive index level: {series_id}")
        _require(group["is_seed"].sum() == 1, f"invalid seed count: {series_id}")
        seed_period = group.loc[group["is_seed"], "trade_date"].iloc[0].to_period("M")
        group["period"] = group["trade_date"].dt.to_period("M")

        observed = group.groupby("period").size()
        complete_periods = [
            period
            for period, count in observed.items()
            if period > seed_period
            and period in expected_counts.index
            and count == expected_counts.loc[period]
            and expected_ends.loc[period] <= group["trade_date"].max()
        ]
        _require(bool(complete_periods), f"no complete monthly data: {series_id}")
        subset = group[group["period"].isin(complete_periods)].copy()
        grouped = subset.groupby("period", sort=True)
        monthly = grouped.agg(
            signal_date=("trade_date", "max"),
            open=("open", "first"),
            close=("close", "last"),
            observed_days=("trade_date", "size"),
            variant=("variant", "first"),
            source_data_as_of=("source_data_as_of", "max"),
            source_index_run_id=("calculation_run_id", "first"),
        ).reset_index(names="month_period")
        monthly["month"] = monthly["month_period"].astype(str)
        monthly["expected_days"] = monthly["month_period"].map(expected_counts)
        _require(
            monthly["observed_days"].eq(monthly["expected_days"]).all(),
            f"incomplete month admitted: {series_id}",
        )
        _require(
            monthly["open"].notna().all() & monthly["open"].gt(0).all(),
            f"missing monthly open: {series_id}",
        )
        monthly["bar_number"] = np.arange(1, len(monthly) + 1)
        monthly["ema12"] = monthly["close"].ewm(span=12, adjust=False).mean()
        monthly["ema120"] = monthly["close"].ewm(span=120, adjust=False).mean()
        previous_close = monthly["close"].shift(1)
        previous_ema12 = monthly["ema12"].shift(1)
        previous_ema120 = monthly["ema120"].shift(1)
        monthly["cross_above_ema12"] = (monthly["close"] > monthly["ema12"]) & (
            previous_close <= previous_ema12
        )
        monthly["cross_below_ema12"] = (monthly["close"] < monthly["ema12"]) & (
            previous_close >= previous_ema12
        )
        monthly["cross_above_ema120"] = (monthly["close"] > monthly["ema120"]) & (
            previous_close <= previous_ema120
        )
        monthly["cross_below_ema120"] = (monthly["close"] < monthly["ema120"]) & (
            previous_close >= previous_ema120
        )
        monthly["monthly_bearish"] = monthly["close"] < monthly["open"]
        monthly["is_complete"] = True
        monthly["series_id"] = series_id
        monthly["methodology_version"] = METHOD_VERSION
        monthly["calculation_run_id"] = calculation_run_id
        monthly["calculated_at"] = calculated_at
        records.append(monthly)

    output = pd.concat(records, ignore_index=True)
    columns = [
        "series_id",
        "variant",
        "signal_date",
        "month",
        "open",
        "close",
        "ema12",
        "ema120",
        "cross_above_ema12",
        "cross_below_ema12",
        "cross_above_ema120",
        "cross_below_ema120",
        "monthly_bearish",
        "observed_days",
        "expected_days",
        "bar_number",
        "is_complete",
        "source_data_as_of",
        "source_index_run_id",
        "methodology_version",
        "calculation_run_id",
        "calculated_at",
    ]
    output = output[columns].sort_values(["series_id", "signal_date"])
    output["signal_date"] = pd.to_datetime(output["signal_date"]).dt.date
    output["source_data_as_of"] = pd.to_datetime(output["source_data_as_of"]).dt.date
    return output.reset_index(drop=True)


@feature_register
class AllAExpmaMonthlyFeature(_AuditedAtomicFullRefreshFeature):
    """Complete-month EXPMA atoms; no portfolio state or budget fields."""

    name = "all_a_index_expma_monthly"
    materialized_view_name = "all_a_expma_monthly"
    description = "自建全A完整月K与EXPMA12/120技术原子（月频）"
    category = "index"
    source_tables = [
        "features.all_a_index_daily",
        "rawdata.others_calendar",
    ]
    quality_checks = {
        "primary_key": ["series_id", "signal_date"],
        "complete_months_only": True,
        "expma_spans": [12, 120],
        "strategy_state_excluded": True,
    }

    def get_create_sql(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS features.all_a_expma_monthly (
            series_id VARCHAR(48) NOT NULL,
            variant VARCHAR(32) NOT NULL,
            signal_date DATE NOT NULL,
            month CHAR(7) NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            ema12 DOUBLE PRECISION NOT NULL,
            ema120 DOUBLE PRECISION NOT NULL,
            cross_above_ema12 BOOLEAN NOT NULL,
            cross_below_ema12 BOOLEAN NOT NULL,
            cross_above_ema120 BOOLEAN NOT NULL,
            cross_below_ema120 BOOLEAN NOT NULL,
            monthly_bearish BOOLEAN NOT NULL,
            observed_days INTEGER NOT NULL,
            expected_days INTEGER NOT NULL,
            bar_number INTEGER NOT NULL,
            is_complete BOOLEAN NOT NULL,
            source_data_as_of DATE NOT NULL,
            source_index_run_id UUID NOT NULL,
            methodology_version VARCHAR(64) NOT NULL,
            calculation_run_id UUID NOT NULL,
            calculated_at TIMESTAMP WITH TIME ZONE NOT NULL,
            PRIMARY KEY (series_id, signal_date),
            UNIQUE (series_id, month),
            CONSTRAINT all_a_expma_complete_check CHECK (is_complete),
            CONSTRAINT all_a_expma_days_check CHECK (observed_days = expected_days),
            CONSTRAINT all_a_expma_level_check
                CHECK (open > 0 AND close > 0 AND ema12 > 0 AND ema120 > 0)
        )
        """.strip()

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_all_a_expma_monthly_signal_date "
            "ON features.all_a_expma_monthly (signal_date DESC)",
            "COMMENT ON TABLE features.all_a_expma_monthly IS "
            "'自建全A完整月技术原子；不包含预热资格、红绿灯状态或ETF预算'",
        ]

    async def compute(self, start_date: str, end_date: str) -> pd.DataFrame:
        del start_date, end_date
        daily = _records_to_frame(
            await self._db_manager.fetch(
                """
                SELECT series_id, variant, trade_date, open, close, is_seed,
                       source_data_as_of, calculation_run_id
                FROM features.all_a_index_daily
                ORDER BY series_id, trade_date
                """
            )
        )
        _require(not daily.empty, "features.all_a_index_daily is empty")
        _require(
            daily["calculation_run_id"].nunique() == 1,
            "daily all-A source contains multiple calculation runs",
        )
        _require(
            daily["source_data_as_of"].nunique() == 1,
            "daily all-A source contains multiple data watermarks",
        )
        for column in ("open", "close"):
            daily[column] = pd.to_numeric(daily[column], errors="coerce")
        min_date = pd.to_datetime(daily["trade_date"]).min().date()
        max_date = pd.to_datetime(daily["trade_date"]).max().date()
        calendar = _records_to_frame(
            await self._db_manager.fetch(
                """
                SELECT cal_date
                FROM rawdata.others_calendar
                WHERE exchange = 'SSE' AND is_open = 1
                  AND cal_date >= $1 AND cal_date <= $2
                ORDER BY cal_date
                """,
                min_date,
                max_date + timedelta(days=62),
            )
        )
        _require(not calendar.empty, "exchange calendar is empty")
        run_id = uuid.uuid4()
        calculated_at = datetime.now(timezone.utc)
        output = build_monthly_expma_atoms(
            daily,
            pd.DatetimeIndex(pd.to_datetime(calendar["cal_date"])),
            calculation_run_id=run_id,
            calculated_at=calculated_at,
        )
        _require(
            set(output["series_id"]) == set(SERIES_IDS.values()),
            "monthly output does not cover all index series",
        )
        counts = output.groupby("series_id").size()
        _require(counts.nunique() == 1, "monthly series have inconsistent coverage")
        output_digest = hashlib.sha256(
            pd.util.hash_pandas_object(
                output[["series_id", "signal_date", "close", "ema12", "ema120"]],
                index=False,
            ).values.tobytes()
        ).hexdigest()
        self._build_details = {
            "calculation_run_id": str(run_id),
            "methodology_version": METHOD_VERSION,
            "source_index_run_ids": sorted(
                {str(value) for value in output["source_index_run_id"]}
            ),
            "source_data_as_of": str(output["source_data_as_of"].max()),
            "first_signal_date": str(output["signal_date"].min()),
            "last_signal_date": str(output["signal_date"].max()),
            "rows_per_series": int(counts.iloc[0]),
            "expma_spans": [12, 120],
            "output_sha256": output_digest,
            "excludes": [
                "warmup eligibility",
                "portfolio state",
                "target budget",
                "drawdown correction",
            ],
        }
        return output
