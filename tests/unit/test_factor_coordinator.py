from dataclasses import replace
from datetime import date
from types import SimpleNamespace

from alphahome.factors.coordinator import FactorCoordinator


class _ConstructionDB:
    def _get_sync_connection(self):
        raise AssertionError("unexpected connection")

    def fetch_sync(self, *_args, **_kwargs):
        return []


class _PlanRepository:
    def snapshot_xmin(self):
        return 100

    def __init__(self, missing=None):
        self.missing = missing or {}

    def table_date_stats(self, _contract):
        return {
            "first_calc_date": date(2025, 1, 3),
            "latest_date_row_count": 100,
        }

    def first_source_date(self, _contract):
        return date(2025, 1, 3)

    def missing_dates(self, contract, _start, _end):
        return list(self.missing.get(contract.task_name, []))

    def dirty_start_date(self, _contract, _watermarks):
        return None

    def source_watermarks(self, _contract):
        return {}

    def readiness(self, _contract, _cutoff, _dates, _dependency_plans):
        return []

    def row_counts_by_date(self, _contract, dates):
        return {value: 80 for value in dates}


def _coordinator(missing=None, max_dates=26):
    coordinator = FactorCoordinator(_ConstructionDB(), max_automatic_dates=max_dates)
    coordinator.governance = SimpleNamespace(
        schema_issues=lambda: [],
        latest_source_watermarks=lambda _task: {"verified_fixture": "2026-09-01", "_snapshot_xmin": 90},
    )
    coordinator.repository = _PlanRepository(missing)
    return coordinator


def test_selecting_g_expands_p_and_propagates_changed_p_for_730_days():
    changed = date(2026, 1, 2)
    coordinator = _coordinator({"factor_p": [changed], "factor_g": []}, 200)

    plan = coordinator.plan(
        ["factor_g"],
        mode="smart",
        batch_started_at=date(2026, 9, 12),
    )

    assert plan.task_names == ["factor_p", "factor_g"]
    assert plan.task_plans[0].dates == [changed]
    assert plan.task_plans[1].dates[0] == changed
    assert plan.task_plans[1].dates[-1] == date(2026, 9, 11)
    assert all(value.weekday() == 4 for value in plan.task_plans[1].dates)


def test_selecting_p_does_not_expand_pit_tasks():
    coordinator = _coordinator({"factor_p": []})
    plan = coordinator.plan(
        ["factor_p"], mode="smart", batch_started_at=date(2026, 9, 12)
    )
    assert plan.task_names == ["factor_p"]
    assert plan.task_plans[0].readiness_dependencies == [
        "pit_financial_indicators",
        "pit_industry_classification",
    ]


def test_smart_mode_refuses_partial_write_when_one_task_exceeds_cap():
    dates = [
        date(2026, 1, 2).fromordinal(date(2026, 1, 2).toordinal() + 7 * i)
        for i in range(27)
    ]
    coordinator = _coordinator({"factor_p": dates})
    plan = coordinator.plan(
        ["factor_p"], mode="smart", batch_started_at=date(2026, 9, 12)
    )
    assert plan.status == "needs_manual_backfill"
    assert "factor_p=27" in plan.message


def test_preflight_exposes_replacement_estimate():
    target = date(2026, 9, 11)
    coordinator = _coordinator({"factor_p": [target]})
    plan = coordinator.plan(
        ["factor_p"], mode="smart", batch_started_at=date(2026, 9, 12)
    )
    task = plan.task_plans[0]
    assert task.existing_rows_to_replace == 80
    assert task.estimated_output_rows == 100


def test_cancellation_is_observed_between_dates_only():
    class Calculator:
        def __init__(self, **kwargs):
            self.config = kwargs.get("config") or {}

        def _get_trading_stock_codes(self, _calc_date):
            return ["000001.SZ"]

        def calculate_p_factors_pit(self, _calc_date, _codes):
            return {"status": "success", "success_count": 1}

    records = []
    coordinator = _coordinator()
    coordinator.governance = SimpleNamespace(
        record_date=lambda *args, **kwargs: records.append((args, kwargs))
    )
    contract = replace(coordinator.contracts()["factor_p"], calculator_class=Calculator)
    dates = [date(2026, 8, 28), date(2026, 9, 4), date(2026, 9, 11)]
    checks = iter([False, True])

    result = coordinator._run_task_dates(
        "00000000-0000-0000-0000-000000000001",
        contract,
        dates,
        stop_requested=lambda: next(checks),
    )

    assert result["successful_dates"] == 1
    assert result["skipped_dates"] == 2
    assert result["failed_dates"] == 0
    assert result["status"] == "cancelled"
    assert len(records) == 2


def test_g_missing_same_day_p_counts_as_failure_not_skip():
    class Calculator:
        def __init__(self, **kwargs):
            self.config = kwargs.get("config") or {}

        def _get_trading_stock_codes(self, _calc_date):
            return []

    coordinator = _coordinator()
    coordinator.governance = SimpleNamespace(record_date=lambda *_a, **_k: None)
    contract = replace(coordinator.contracts()["factor_g"], calculator_class=Calculator)
    result = coordinator._run_task_dates(
        "00000000-0000-0000-0000-000000000001",
        contract,
        [date(2026, 9, 11)],
        stop_requested=None,
    )
    assert result["failed_dates"] == 1
    assert result["skipped_dates"] == 0
    assert result["status"] == "error"
