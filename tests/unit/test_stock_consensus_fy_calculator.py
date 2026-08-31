from __future__ import annotations

import pandas as pd
import pytest

from alphahome.pit.calculators.stock_consensus_fy_calculator import (
    StockConsensusFYCalculator,
)


def _forecast(
    report_date: str,
    org_name: str,
    np_value: float,
    *,
    quarter: str = "2024Q4",
    eps: float = 1.0,
) -> dict:
    return {
        "ts_code": "000001.SZ",
        "org_name": org_name,
        "author_name": "分析师",
        "report_date": report_date,
        "quarter": quarter,
        "np": np_value,
        "eps": eps,
    }


def test_latest_broker_forecast_and_fixed_fiscal_year_median():
    forecasts = pd.DataFrame(
        [
            _forecast("2025-01-05", "A", 90.0),
            _forecast("2025-01-20", "A", 120.0),
            _forecast("2025-01-15", "B", 100.0),
            _forecast("2025-01-16", "C", 110.0),
            _forecast("2025-02-01", "D", 1000.0),
            _forecast("2025-01-10", "E", 999.0, quarter="2025Q1"),
        ]
    )

    result = StockConsensusFYCalculator().calculate(forecasts, ["2025-01-31"])
    row = result.iloc[0]

    assert row.target_year == 2024
    assert row.org_count == 3
    assert row.np_org_count == 3
    assert row.np_consensus_median == pytest.approx(110.0)
    assert row.latest_report_date == pd.Timestamp("2025-01-20")
    assert bool(row.is_eligible) is True


def test_revision_counts_only_matched_broker_and_target_year():
    forecasts = pd.DataFrame(
        [
            _forecast("2025-01-10", "A", 100.0),
            _forecast("2025-01-10", "B", 100.0),
            _forecast("2025-01-10", "C", 100.0),
            _forecast("2025-02-10", "A", 110.0),
            _forecast("2025-02-10", "D", 500.0),
        ]
    )

    result = StockConsensusFYCalculator().calculate(
        forecasts, ["2025-01-31", "2025-02-28"]
    )
    february = result.loc[result.obs_date.eq(pd.Timestamp("2025-02-28"))].iloc[0]

    assert february.revision_matched_org_count_1m == 3
    assert february.revision_revised_org_count_1m == 1
    assert february.revision_up_org_count_1m == 1
    assert february.revision_down_org_count_1m == 0
    assert february.revision_activity_rate_1m == pytest.approx(1 / 3)
    assert february.revision_up_org_rate_1m == pytest.approx(1.0)


def test_non_month_end_observation_is_rejected():
    with pytest.raises(ValueError, match="自然月末"):
        StockConsensusFYCalculator().calculate(
            pd.DataFrame([_forecast("2025-01-10", "A", 100.0)]),
            ["2025-01-30"],
        )
