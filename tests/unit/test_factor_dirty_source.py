from dataclasses import replace
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from psycopg2.errors import UndefinedColumn

from alphahome.factors.coordinator import FactorCoordinator
from alphahome.factors.repository import FactorRepository, FactorSourceQueryError


SOURCE = "pit.pit_financial_indicators"
WATERMARK = datetime(2026, 9, 1)


class DirtyDB:
    def __init__(self, value=None, error=None, exists=True):
        self.value, self.error, self.exists = value, error, exists
        self.queries = []

    def _get_sync_connection(self):
        raise AssertionError("No database writes or real connections in this test")

    def fetch_sync(self, *args, **kwargs):
        raise AssertionError("Unexpected source read")

    def fetch_val_sync(self, sql, params=()):
        self.queries.append((sql, params))
        if "to_regclass" in sql:
            return self.exists
        if self.error:
            raise self.error
        return self.value


@pytest.fixture
def contract():
    return replace(FactorCoordinator.contracts()["factor_p"], source_tables=(SOURCE,))


@pytest.mark.parametrize("error_type", [TimeoutError, PermissionError, UndefinedColumn])
def test_dirty_query_errors_propagate_with_safe_source_identity(contract, error_type):
    db = DirtyDB(error=error_type("test-sensitive-driver-details"))

    with pytest.raises(FactorSourceQueryError) as exc:
        FactorRepository(db).dirty_start_date(contract, {SOURCE: WATERMARK})

    assert exc.value.source == SOURCE
    assert exc.value.reason == error_type.__name__
    assert "test-sensitive-driver-details" not in str(exc.value)


@pytest.mark.parametrize(
    "value, expected",
    [(None, None), (date(2026, 8, 28), date(2026, 8, 28)), (datetime(2026, 8, 28), date(2026, 8, 28))],
)
def test_no_changes_and_real_dirty_dates_remain_distinct(contract, value, expected):
    db = DirtyDB(value=value)

    assert FactorRepository(db).dirty_start_date(contract, {SOURCE: WATERMARK}) == expected
    assert db.queries[-1][1] == (WATERMARK,)


def test_no_previous_watermark_does_not_run_dirty_query(contract):
    db = DirtyDB(error=AssertionError("no baseline"))

    assert FactorRepository(db).dirty_start_date(contract, {}) is None
    assert db.queries == []


@pytest.mark.parametrize("db, reason", [(DirtyDB(exists=False), "missing_relation"), (DirtyDB(value="invalid"), "invalid_date_result")])
def test_missing_or_drifted_source_is_not_treated_as_no_changes(contract, db, reason):
    with pytest.raises(FactorSourceQueryError, match=reason):
        FactorRepository(db).dirty_start_date(contract, {SOURCE: WATERMARK})


def test_plan_failure_never_starts_run_or_persists_watermarks(monkeypatch, contract):
    coordinator = FactorCoordinator(DirtyDB(error=TimeoutError("test timeout")))
    governance = SimpleNamespace(
        ensure_schema=Mock(),
        latest_source_watermarks=lambda name: {name: {SOURCE: WATERMARK}},
        start_run=Mock(),
        finish_run=Mock(),
    )
    coordinator.governance = governance
    monkeypatch.setattr(coordinator, "contracts", lambda: {"factor_p": contract})
    monkeypatch.setattr(coordinator.repository, "table_date_stats", lambda _: {"first_calc_date": date(2025, 1, 3)})
    monkeypatch.setattr(coordinator.repository, "missing_dates", lambda *args: [])
    execute = Mock()
    monkeypatch.setattr(coordinator, "_run_task_dates", execute)

    with pytest.raises(FactorSourceQueryError, match="TimeoutError"):
        coordinator.run(["factor_p"], batch_started_at=date(2026, 9, 14))

    governance.start_run.assert_not_called()
    governance.finish_run.assert_not_called()
    execute.assert_not_called()
