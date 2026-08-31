from __future__ import annotations

import pandas as pd
import pytest

from alphahome.pit.calculators.annual_earnings_surprise_calculator import (
    AnnualEarningsSurpriseCalculator,
)


def _actual(np_yuan: float = 1_150_000.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "end_date": "2024-12-31",
                "ann_date": "2025-02-28",
                "actual_np_yuan": np_yuan,
                "actual_basic_eps": 1.15,
                "actual_diluted_eps": 1.14,
                "actual_source_row_count": 1,
                "actual_source_value_conflict": False,
                "actual_source_update_time": "2025-03-01 07:00:00",
                "actual_source_selection_basis": "latest_update_fann_stable_hash_v1",
                "source_income_updated_at": "2025-03-01 08:00:00",
            }
        ]
    )


def _consensus() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "obs_date": obs_date,
                "ts_code": "000001.SZ",
                "target_year": 2024,
                "np_consensus_median": np_value,
                "eps_consensus_median": eps,
                "org_count": 4,
                "np_org_count": 4,
                "np_dispersion_rate": 0.1,
                "is_eligible": True,
                "availability_basis": "report_date_reconstructed",
                "source_max_report_date": source_date,
            }
            for obs_date, np_value, eps, source_date in [
                ("2025-01-31", 110.0, 1.10, "2025-01-20"),
                # Same-day snapshot must not be visible at announcement time.
                ("2025-02-28", 200.0, 2.00, "2025-02-27"),
            ]
        ]
    )


def test_uses_strictly_prior_snapshot_and_converts_yuan_to_10k():
    row = AnnualEarningsSurpriseCalculator().calculate(_actual(), _consensus()).iloc[0]

    assert row.consensus_obs_date == pd.Timestamp("2025-01-31")
    assert row.actual_np_10k == pytest.approx(115.0)
    assert row.consensus_np_10k == pytest.approx(110.0)
    assert row.np_surprise_abs_10k == pytest.approx(5.0)
    assert row.np_surprise_rate == pytest.approx(5.0 / 110.0)
    assert row.eps_surprise_rate == pytest.approx(0.05 / 1.10)
    assert bool(row.is_eligible) is True


def test_missing_prior_consensus_is_retained_but_ineligible():
    consensus = _consensus().assign(obs_date="2025-02-28")

    row = AnnualEarningsSurpriseCalculator().calculate(_actual(), consensus).iloc[0]

    assert pd.isna(row.consensus_obs_date)
    assert bool(row.is_eligible) is False
    assert "missing_prior_consensus" in row.quality_reasons
    assert "missing_consensus_np" in row.quality_reasons


def test_np_sign_change_is_explicit_and_rate_uses_absolute_denominator():
    consensus = _consensus().iloc[[0]].copy()
    consensus["np_consensus_median"] = -100.0

    row = (
        AnnualEarningsSurpriseCalculator()
        .calculate(_actual(np_yuan=500_000.0), consensus)
        .iloc[0]
    )

    assert bool(row.is_np_sign_change) is True
    assert row.np_surprise_rate == pytest.approx(1.5)


def test_conflicting_actual_source_is_retained_but_ineligible():
    actual = _actual()
    actual["actual_source_row_count"] = 2
    actual["actual_source_value_conflict"] = True

    row = AnnualEarningsSurpriseCalculator().calculate(actual, _consensus()).iloc[0]

    assert bool(row.is_eligible) is False
    assert "conflicting_actual_source" in row.quality_reasons
