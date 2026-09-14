from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from alphahome.features.recipes.python.index.all_a_expma_monthly import (
    AllAExpmaMonthlyFeature,
    build_monthly_expma_atoms,
)
from alphahome.features.recipes.python.index.all_a_index_daily import (
    AllAIndexDailyFeature,
    _day_step,
    _non_seed_month_starts,
)
from alphahome.features.storage.database_init import CREATE_MV_REFRESH_LOG_TABLE_SQL
from alphahome.features.storage.refresh_log import log_mv_refresh


def test_daily_object_uses_plain_features_table_name():
    feature = AllAIndexDailyFeature()
    assert feature.full_name == "features.all_a_index_daily"
    assert (
        "CREATE TABLE IF NOT EXISTS features.all_a_index_daily"
        in feature.get_create_sql()
    )
    assert feature.refresh_strategy == "full"


def test_expma_object_excludes_strategy_state():
    feature = AllAExpmaMonthlyFeature()
    ddl = feature.get_create_sql()
    assert feature.full_name == "features.all_a_expma_monthly"
    assert "ema12" in ddl and "ema120" in ddl
    assert "target_budget" not in ddl
    assert "state_after" not in ddl


def test_day_step_uses_lagged_cap_weights():
    result = _day_step(
        previous_price=np.array([10.0, 20.0]),
        previous_shares=np.array([2.0, 1.0]),
        previous_float=np.array([2.0, 1.0]),
        eligible=np.array([True, True]),
        data_eligible=np.array([True, True]),
        close=np.array([11.0, 18.0]),
        pre_close=np.array([10.0, 20.0]),
        free_share=np.array([4.0, 1.0]),
        float_share=np.array([4.0, 1.0]),
        cash=np.zeros(2),
        bonus=np.zeros(2),
    )
    # Both prior free-float caps are 20, so the aggregate return is
    # 0.5 * 10% + 0.5 * -10% = 0.  Same-day share changes cannot affect it.
    assert result["reference_return"] == pytest.approx(0.0)
    np.testing.assert_allclose(result["weights"], [0.5, 0.5])
    np.testing.assert_allclose(result["next_shares"], [4.0, 1.0])


def test_monthly_open_check_excludes_the_entire_seed_month():
    daily = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["1991-01-02", "1991-01-03", "1991-02-01"]),
            "open": [np.nan, np.nan, 1010.0],
            "is_seed": [True, False, False],
        }
    )
    result = _non_seed_month_starts(daily)
    assert result["trade_date"].tolist() == [pd.Timestamp("1991-02-01")]
    assert result["open"].notna().all()


def test_monthly_atoms_drop_seed_month_and_only_keep_complete_months():
    calendar = pd.bdate_range("2020-01-01", "2020-04-30")
    source_run = uuid.uuid4()
    rows = []
    for i, trade_date in enumerate(calendar[calendar <= "2020-03-31"]):
        period_days = calendar[calendar.to_period("M") == trade_date.to_period("M")]
        is_first = trade_date == period_days.min()
        rows.append(
            {
                "series_id": "TEST",
                "variant": "strict_free",
                "trade_date": trade_date,
                "open": 100.0 + i if is_first and trade_date.month > 1 else np.nan,
                "close": 100.0 + i,
                "is_seed": i == 0,
                "source_data_as_of": pd.Timestamp("2020-03-31"),
                "calculation_run_id": source_run,
            }
        )
    daily = pd.DataFrame(rows)
    result = build_monthly_expma_atoms(
        daily,
        calendar,
        calculation_run_id=uuid.uuid4(),
        calculated_at=datetime.now(timezone.utc),
    )
    assert result["month"].tolist() == ["2020-02", "2020-03"]
    assert result["is_complete"].all()
    assert result["observed_days"].equals(result["expected_days"])
    assert result.iloc[0]["ema12"] == pytest.approx(result.iloc[0]["close"])
    assert "target_budget" not in result.columns


def test_refresh_log_schema_migrates_details_for_existing_tables():
    assert "details JSONB" in CREATE_MV_REFRESH_LOG_TABLE_SQL
    assert "ADD COLUMN IF NOT EXISTS details JSONB" in CREATE_MV_REFRESH_LOG_TABLE_SQL


@pytest.mark.asyncio
async def test_refresh_log_serializes_build_details():
    class RecordingDB:
        def __init__(self):
            self.call = None

        async def execute(self, sql, *params):
            self.call = (sql, params)

    db = RecordingDB()
    await log_mv_refresh(
        db,
        view_name="all_a_index_daily",
        schema_name="features",
        refresh_strategy="full_atomic",
        success=True,
        duration_seconds=1.25,
        row_count=2,
        details={"source_data_as_of": pd.Timestamp("2026-09-11")},
    )

    sql, params = db.call
    assert "$8::jsonb" in sql
    assert json.loads(params[-1]) == {"source_data_as_of": "2026-09-11 00:00:00"}
