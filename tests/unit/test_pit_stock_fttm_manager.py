from __future__ import annotations

from datetime import date

import pandas as pd

from alphahome.pit.base import monthly_snapshot_manager
from alphahome.pit.base.monthly_snapshot_manager import PITMonthlySnapshotManager
from alphahome.pit.pit_stock_fttm_manager import PITStockFTTMManager


def test_stock_incremental_enforces_minimum_eight_complete_months(monkeypatch):
    manager = PITStockFTTMManager()
    captured = {}
    monkeypatch.setattr(
        manager,
        "_run_months",
        lambda months, batch_size, result_key: captured.update(
            months=months, result_key=result_key
        )
        or {result_key: 0},
    )
    monkeypatch.setattr(manager, "latest_complete_month", lambda today=None: date(2026, 7, 31))

    manager.incremental_update(months=2)

    assert len(captured["months"]) == 8
    assert captured["months"][0] == date(2025, 12, 31)
    assert captured["months"][-1] == date(2026, 7, 31)
    assert captured["result_key"] == "updated_records"


class _Cursor:
    def __init__(self):
        self.queries = []
        self.inserted_count = 0
        self._last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, query, params=None):
        self.queries.append((query, params))
        self._last_query = query

    def fetchone(self):
        if "duplicate_keys" in self._last_query:
            return (0,)
        return (self.inserted_count,)


class _Connection:
    def __init__(self):
        self.cursor_instance = _Cursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class _DB:
    def __init__(self):
        self.connection = _Connection()

    def _get_sync_connection(self):
        return self.connection


def test_empty_recalculation_still_deletes_old_month_atomically(monkeypatch):
    manager = PITStockFTTMManager()
    manager.context = type("Context", (), {"db_manager": _DB()})()
    monkeypatch.setattr(
        monthly_snapshot_manager,
        "execute_values",
        lambda *args, **kwargs: None,
    )

    rows = manager._atomic_replace_months(
        pd.DataFrame(),
        [date(2026, 7, 31)],
        columns=["ts_code", "org_name", "obs_date"],
        primary_keys=["ts_code", "org_name", "obs_date"],
    )

    connection = manager.context.db_manager.connection
    sql = "\n".join(query for query, _ in connection.cursor_instance.queries)
    assert rows == 0
    assert "DELETE FROM \"pit\".\"pit_stock_fttm_monthly\" WHERE obs_date = ANY" in sql
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_month_end_planner_uses_natural_months():
    assert PITMonthlySnapshotManager.month_ends("2024-01-02", "2024-03-10") == [
        date(2024, 1, 31),
        date(2024, 2, 29),
        date(2024, 3, 31),
    ]
