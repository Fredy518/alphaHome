from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from alphahome.pit.pit_index_fttm_manager import (
    ALL_A_CODE,
    IMPORTANT_INDEX_SPECS,
    PITIndexFTTMManager,
    configured_universe_count,
)


def test_configured_universe_count_respects_first_valid_weight_dates():
    assert configured_universe_count("2014-01-31") == 6
    assert configured_universe_count("2026-07-31") == 1 + len(IMPORTANT_INDEX_SPECS)


def test_bounded_index_replay_propagates_to_immediate_next_month(monkeypatch):
    manager = PITIndexFTTMManager()
    captured = {}
    monkeypatch.setattr(
        manager,
        "_latest_available_month",
        lambda cutoff_date=None: date(2024, 3, 31),
    )
    monkeypatch.setattr(
        manager,
        "_run_months",
        lambda months, batch_size, result_key: captured.update(months=months)
        or {result_key: 0},
    )

    manager.full_backfill(start_date="2024-01-01", end_date="2024-01-31")

    assert captured["months"] == [date(2024, 1, 31), date(2024, 2, 29)]


def test_index_incremental_uses_at_least_eight_complete_months(monkeypatch):
    manager = PITIndexFTTMManager()
    captured = {}
    monkeypatch.setattr(
        manager,
        "_latest_available_month",
        lambda cutoff_date=None: date(2026, 7, 31),
    )
    monkeypatch.setattr(
        manager,
        "_run_months",
        lambda months, batch_size, result_key: captured.update(months=months)
        or {result_key: 0},
    )

    manager.incremental_update(months=1)

    assert len(captured["months"]) == 8
    assert captured["months"][-1] == date(2026, 7, 31)


def test_dependency_guard_rejects_missing_stock_fttm_month():
    members = pd.DataFrame(
        {
            "obs_date": [pd.Timestamp("2024-01-31")],
            "universe_type": ["all_a"],
            "universe_code": [ALL_A_CODE],
        }
    )
    stock_fttm = pd.DataFrame(columns=["obs_date"])

    with pytest.raises(RuntimeError, match="pit_stock_fttm_monthly"):
        PITIndexFTTMManager._validate_dependencies(
            members, stock_fttm, [date(2024, 1, 31)]
        )


def test_dependency_guard_rejects_missing_configured_index_weight():
    month = pd.Timestamp("2026-07-31")
    rows = [
        {"obs_date": month, "universe_type": "all_a", "universe_code": ALL_A_CODE}
    ]
    rows.extend(
        {
            "obs_date": month,
            "universe_type": "index",
            "universe_code": spec.code,
        }
        for spec in IMPORTANT_INDEX_SPECS[1:]
    )
    members = pd.DataFrame(rows)
    stock_fttm = pd.DataFrame({"obs_date": [month]})

    with pytest.raises(RuntimeError, match=IMPORTANT_INDEX_SPECS[0].code):
        PITIndexFTTMManager._validate_dependencies(
            members, stock_fttm, [date(2026, 7, 31)]
        )


def test_missing_anchor_is_recomputed_read_only(monkeypatch):
    manager = PITIndexFTTMManager()
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

    combined, source = manager._ensure_stock_anchor(persisted, date(2023, 12, 31))

    assert source == "recomputed_from_raw_read_only"
    assert set(combined["obs_date"]) == {
        pd.Timestamp("2023-12-31"),
        pd.Timestamp("2024-01-31"),
    }


def test_member_loader_uses_pit_weight_dates_and_explicit_staleness_windows():
    manager = PITIndexFTTMManager()
    captured = {}

    class _Context:
        def query_dataframe(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return pd.DataFrame()

    manager.context = _Context()
    manager._load_members([date(2026, 7, 31)])

    assert "s.weight_trade_date <= r.obs_date" in captured["sql"]
    assert "INTERVAL '65 days'" in captured["sql"]
    assert "INTERVAL '31 days'" in captured["sql"]
    assert "HAVING SUM(w.weight) BETWEEN 99 AND 101" in captured["sql"]
    assert len(captured["params"]) == len(IMPORTANT_INDEX_SPECS) * 3 + 3
