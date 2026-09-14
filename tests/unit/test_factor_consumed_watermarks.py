from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from alphahome.factors.coordinator import FactorCoordinator, FactorRunPlan, FactorTaskPlan
from alphahome.factors.source_boundary import consumed_watermarks


T1 = datetime(2026, 9, 11, 10, tzinfo=timezone.utc)
T2 = datetime(2026, 9, 11, 11, tzinfo=timezone.utc)
SOURCE = "pit.pit_financial_indicators"


def task_result(status="success", watermark=T1):
    return {"status": status, "successful_dates": 1, "failed_dates": int(status == "error"),
            "skipped_dates": 0, "output_count": 1,
            "dates": {"2026-09-11": {"status": status}},
            "source_snapshot": {"consistent": True, "xmin": 100, "watermarks": {SOURCE: watermark}}}


@pytest.mark.parametrize("planned, snapshot, expected", [(T1, T2, T1), (T2, T1, T1), (T1, T1, T1)])
def test_consumed_ceiling_never_exceeds_plan_or_snapshot(planned, snapshot, expected):
    assert consumed_watermarks(task_result(watermark=snapshot), {SOURCE: planned}, 90) == {SOURCE: expected, "_snapshot_xmin": 90}


@pytest.mark.parametrize("status", ["no_op", "cancelled", "error", "partial_success"])
def test_unfinished_or_noop_tasks_cannot_advance_consumption(status):
    assert consumed_watermarks(task_result(status), {SOURCE: T1}) == {}


@pytest.mark.parametrize("mode, start, expected", [("smart", None, True), ("full", None, True), ("manual", "2026-09-11", False), ("smart", "2026-09-11", False)])
def test_run_stores_end_observation_separately_and_limits_scope(monkeypatch, mode, start, expected):
    db = SimpleNamespace(fetch_sync=Mock(), _get_sync_connection=Mock())
    coordinator = FactorCoordinator(db)
    coordinator.governance = SimpleNamespace(start_run=Mock(return_value="run-1"), finish_run=Mock(), record_public_status=Mock(), current_dates_for_run=lambda *_: {date(2026, 9, 11)})
    coordinator.repository.source_watermarks = lambda _: {SOURCE: T2}
    plan = FactorRunPlan(["factor_p"], mode, date(2026, 9, 11), [FactorTaskPlan("factor_p", dates=[date(2026, 9, 11)], source_watermarks={SOURCE: T1}, source_xmin=90)])
    monkeypatch.setattr(coordinator, "plan", lambda *args, **kwargs: plan)
    monkeypatch.setattr(coordinator, "_run_task_dates", lambda *args, **kwargs: task_result())
    result = coordinator.run(["factor_p"], mode=mode, start_date=start, batch_started_at=date(2026, 9, 14))
    assert coordinator.governance.start_run.call_args.kwargs["source_watermarks"] == {}
    finished = coordinator.governance.finish_run.call_args.kwargs
    assert finished["source_watermarks"] == ({"factor_p": {SOURCE: T1, "_snapshot_xmin": 90}} if expected else {})
    assert result.details["end_observed_watermarks"] == {"factor_p": {SOURCE: T2}}


def test_existing_outputs_without_verified_watermarks_require_revalidation(monkeypatch):
    db = SimpleNamespace(fetch_sync=Mock(), _get_sync_connection=Mock())
    coordinator = FactorCoordinator(db)
    coordinator.governance = SimpleNamespace(schema_issues=lambda: [], latest_source_watermarks=lambda _: {})
    repository = SimpleNamespace(
        snapshot_xmin=lambda: 100,
        first_source_date=lambda _: date(2024, 1, 5),
        source_watermarks=lambda _: {SOURCE: T1},
        table_date_stats=lambda _: {"first_calc_date": date(2024, 1, 5)},
        missing_dates=lambda *_: [], readiness=lambda *_: [], row_counts_by_date=lambda *_: {},
    )
    coordinator.repository = repository
    result = coordinator.plan(["factor_p"], batch_started_at=date(2026, 9, 14))
    assert result.status == "needs_manual_backfill"
    assert result.total_dates > 26


def test_no_mvcc_snapshot_cannot_be_certified():
    assert consumed_watermarks(task_result(), {SOURCE: T1}) == {}


def test_projection_change_during_compute_prevents_consumption_promotion():
    from alphahome.factors.source_boundary import STOCK_MASTER_PROJECTION

    result = task_result()
    before = {SOURCE: T1, "tushare.stock_basic": STOCK_MASTER_PROJECTION + "a"*32}
    result["source_snapshot"]["watermarks"] = {**before, "tushare.stock_basic": STOCK_MASTER_PROJECTION + "b"*32}
    result["source_snapshot"]["xmin"] = 100
    assert consumed_watermarks(result, before, planned_xmin=90) == {}


def test_uncertified_short_source_history_revalidates_only_available_dates():
    from test_factor_coordinator import _coordinator

    coordinator = _coordinator()
    coordinator.governance.latest_source_watermarks = lambda _: {}
    coordinator.repository.first_source_date = lambda _: date(2026, 9, 4)
    plan = coordinator.plan(["factor_p"], batch_started_at=date(2026, 9, 14))
    assert plan.task_plans[0].dates == [date(2026, 9, 4), date(2026, 9, 11)]


def test_expired_mvcc_cursor_fails_before_source_scan():
    from alphahome.factors.repository import FactorRepository, FactorSourceQueryError

    db = SimpleNamespace(fetch_sync=Mock(), fetch_val_sync=Mock(return_value=str(2**31 + 10)))
    with pytest.raises(FactorSourceQueryError, match="expired"):
        FactorRepository(db).dirty_start_date(FactorCoordinator.contracts()["factor_p"], {"_snapshot_xmin": 5, SOURCE: T1})
