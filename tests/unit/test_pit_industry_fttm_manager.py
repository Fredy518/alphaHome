from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from alphahome.pit.pit_industry_fttm_manager import PITIndustryFTTMManager


def test_bounded_industry_replay_propagates_to_immediate_next_month(monkeypatch):
    manager = PITIndustryFTTMManager()
    captured = {}
    monkeypatch.setattr(manager, "_latest_available_month", lambda: date(2024, 3, 31))
    monkeypatch.setattr(
        manager,
        "_run_months",
        lambda months, batch_size, result_key: captured.update(months=months)
        or {result_key: 0},
    )

    manager.full_backfill(start_date="2024-01-01", end_date="2024-01-31")

    assert captured["months"] == [date(2024, 1, 31), date(2024, 2, 29)]


def test_industry_incremental_uses_eight_month_window_capped_by_classification(monkeypatch):
    manager = PITIndustryFTTMManager()
    captured = {}
    monkeypatch.setattr(manager, "_latest_available_month", lambda: date(2026, 7, 31))
    monkeypatch.setattr(
        manager,
        "_run_months",
        lambda months, batch_size, result_key: captured.update(months=months)
        or {result_key: 0},
    )

    manager.incremental_update(months=1)

    assert len(captured["months"]) == 8
    assert captured["months"][-1] == date(2026, 7, 31)


def test_dependency_guard_rejects_missing_stock_month():
    classifications = pd.DataFrame(
        {
            "obs_date": [pd.Timestamp("2024-01-31")],
            "industry_code1": ["L1"],
            "industry_code2": ["L2"],
        }
    )
    stock_fttm = pd.DataFrame(columns=["obs_date"])

    with pytest.raises(RuntimeError, match="pit_stock_fttm_monthly"):
        PITIndustryFTTMManager._validate_dependencies(
            classifications, stock_fttm, [date(2024, 1, 31)]
        )


def test_dependency_guard_rejects_missing_classification_month():
    classifications = pd.DataFrame(
        columns=["obs_date", "industry_code1", "industry_code2"]
    )
    stock_fttm = pd.DataFrame({"obs_date": [pd.Timestamp("2024-01-31")]})

    with pytest.raises(RuntimeError, match="pit_industry_classification"):
        PITIndustryFTTMManager._validate_dependencies(
            classifications, stock_fttm, [date(2024, 1, 31)]
        )


def test_dependency_guard_requires_both_sw_levels():
    classifications = pd.DataFrame(
        {
            "obs_date": [pd.Timestamp("2024-01-31")],
            "industry_code1": ["L1"],
            "industry_code2": [None],
        }
    )
    stock_fttm = pd.DataFrame({"obs_date": [pd.Timestamp("2024-01-31")]})

    with pytest.raises(RuntimeError, match="L2=2024-01-31"):
        PITIndustryFTTMManager._validate_dependencies(
            classifications, stock_fttm, [date(2024, 1, 31)]
        )


def test_missing_anchor_is_recomputed_read_only_and_not_required_in_target_table(
    monkeypatch,
):
    manager = PITIndustryFTTMManager()
    persisted = pd.DataFrame(
        {
            "obs_date": [date(2024, 1, 31)],
            "ts_code": ["000001.SZ"],
            "org_name": ["机构A"],
            "fttm_np": [100.0],
            "formula_version": ["fttm_q4_report_event_linear_v1"],
            "selected_report_date": [pd.Timestamp("2024-01-20")],
        }
    )
    transient = persisted.copy()
    transient["obs_date"] = pd.Timestamp("2023-12-31")
    monkeypatch.setattr(
        manager, "_calculate_transient_stock_month", lambda anchor: transient
    )

    combined, source = manager._ensure_stock_anchor(
        persisted, date(2023, 12, 31)
    )

    assert source == "recomputed_from_raw_read_only"
    assert set(combined["obs_date"]) == {
        pd.Timestamp("2023-12-31"),
        pd.Timestamp("2024-01-31"),
    }
    assert pd.api.types.is_datetime64_any_dtype(combined["obs_date"])


def test_existing_anchor_is_reused_without_raw_recalculation(monkeypatch):
    manager = PITIndustryFTTMManager()
    persisted = pd.DataFrame(
        {
            "obs_date": [pd.Timestamp("2023-12-31")],
            "ts_code": ["000001.SZ"],
            "org_name": ["机构A"],
            "fttm_np": [100.0],
        }
    )
    monkeypatch.setattr(
        manager,
        "_calculate_transient_stock_month",
        lambda anchor: pytest.fail("persisted anchor should be reused"),
    )

    combined, source = manager._ensure_stock_anchor(
        persisted, date(2023, 12, 31)
    )

    assert source == "persisted_pit_stock_fttm"
    pd.testing.assert_frame_equal(combined, persisted)
