from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from alphahome.pit.pit_etf_index_a_share_proxy_fapi_manager import (
    PITETFIndexAShareProxyFAPIMonthlyManager,
)
from alphahome.pit.pit_etf_index_a_share_proxy_members_manager import (
    PITETFIndexAShareProxyMembersMonthlyManager,
)
from alphahome.pit.pit_etf_index_fapi_manager import PITETFIndexFAPIMonthlyManager
from alphahome.pit.pit_etf_index_members_manager import (
    PITETFIndexMembersMonthlyManager,
)


MANAGERS = (
    PITETFIndexMembersMonthlyManager,
    PITETFIndexFAPIMonthlyManager,
    PITETFIndexAShareProxyMembersMonthlyManager,
    PITETFIndexAShareProxyFAPIMonthlyManager,
)


class _Cursor:
    def __init__(self, staged_count=1):
        self.staged_count = staged_count
        self.statements = []
        self._fetchone = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.statements.append((" ".join(sql.split()), params))
        if "duplicate_keys" in sql:
            self._fetchone = (0,)
        elif "SELECT COUNT(*) FROM" in sql:
            self._fetchone = (self.staged_count,)

    def fetchone(self):
        return self._fetchone


class _Connection:
    def __init__(self, staged_count=1):
        self.cursor_instance = _Cursor(staged_count)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class _DB:
    def __init__(self, connection):
        self.connection = connection
        self.connection_requests = 0

    def _get_sync_connection(self):
        self.connection_requests += 1
        return self.connection


def _columns(manager):
    return (
        manager.calculator.INDEX_OUTPUT_COLUMNS
        if "fapi" in manager.table_name
        else manager.calculator.OUTPUT_COLUMNS
    )


def _valid_frame(manager, month):
    row = {column: "value" for column in _columns(manager)}
    for column in row:
        if column == "obs_date" or column.endswith("_date"):
            row[column] = month
    row.update(index_code="IDX", method_version=manager.calculator.METHOD_VERSION)
    if "ts_code" in row:
        row["ts_code"] = "000001.SZ"
    if "benchmark_code" in row:
        row["benchmark_code"] = "000300.SH"
    return pd.DataFrame([row])


@pytest.mark.parametrize("manager_type", MANAGERS)
def test_empty_etf_month_never_deletes_or_commits(manager_type):
    manager = manager_type()
    connection = _Connection(staged_count=0)
    manager.context = SimpleNamespace(db_manager=_DB(connection))
    month = date(2026, 8, 31)

    with pytest.raises(ValueError, match="pit_incomplete_months"):
        manager._atomic_replace_scope(
            pd.DataFrame(columns=_columns(manager)), [month], ["IDX"]
        )

    assert connection.commits == 0
    assert connection.rollbacks == 0
    assert connection.cursor_instance.statements == []
    assert getattr(manager, "_verified_replacement_months", []) == []


@pytest.mark.parametrize("manager_type", MANAGERS)
def test_partial_etf_index_scope_never_deletes_or_commits(manager_type):
    manager = manager_type()
    connection = _Connection(staged_count=1)
    manager.context = SimpleNamespace(db_manager=_DB(connection))
    month = date(2026, 8, 31)

    with pytest.raises(ValueError, match="pit_incomplete_scope"):
        manager._atomic_replace_scope(
            _valid_frame(manager, month), [month], ["IDX", "MISSING_IDX"]
        )

    assert connection.commits == 0
    assert connection.rollbacks == 0
    assert connection.cursor_instance.statements == []
    assert getattr(manager, "_verified_replacement_months", []) == []


@pytest.mark.parametrize("manager_type", MANAGERS)
def test_etf_month_commit_records_completion(manager_type, monkeypatch):
    manager = manager_type()
    connection = _Connection(staged_count=1)
    manager.context = SimpleNamespace(db_manager=_DB(connection))
    month = date(2026, 8, 31)
    inserted = []
    monkeypatch.setattr(
        "alphahome.pit.base.monthly_snapshot_manager.execute_values",
        lambda cursor, sql, records, page_size: inserted.extend(records),
    )

    assert manager._atomic_replace_scope(
        _valid_frame(manager, month), [month], ["IDX"]
    ) == 1

    statements = [sql for sql, _ in connection.cursor_instance.statements]
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert len(inserted) == 1
    assert any("DELETE FROM" in sql and "method_version" in sql for sql in statements)
    assert manager._verified_replacement_months == [month]
