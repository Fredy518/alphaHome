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


def _source_backed_manager(monkeypatch, *, published_missing=False):
    manager = PITETFIndexMembersMonthlyManager()
    manager.logger = SimpleNamespace(info=lambda *_args, **_kwargs: None)
    month = date(2026, 8, 31)
    row = {column: None for column in manager.calculator.OUTPUT_COLUMNS}
    row.update(
        obs_date=month,
        index_code="GOOD",
        ts_code="000001.SZ",
        weight=1.0,
        raw_weight=100.0,
        source_available_date=month,
        method_version=manager.calculator.METHOD_VERSION,
    )
    calculated = pd.DataFrame([row])
    manager.context = SimpleNamespace(
        query_dataframe=lambda *_args, **_kwargs: (
            pd.DataFrame({"index_code": ["MISSING"]})
            if published_missing
            else pd.DataFrame(columns=["index_code"])
        )
    )
    monkeypatch.setattr(manager, "_resolve_index_codes", lambda _codes: ["GOOD", "MISSING"])
    monkeypatch.setattr(manager, "_ensure_table_exists", lambda: None)
    monkeypatch.setattr(
        manager,
        "_load_sources",
        lambda _months, _codes: {
            "official_weights": pd.DataFrame(),
            "fund_holdings": pd.DataFrame(),
        },
    )

    def calculate(*_args):
        manager.calculator.last_audit = {
            "source_pair_counts": {
                "official_index_weight": 1,
                "etf_disclosed_holding": 0,
                "unavailable": 1,
            }
        }
        return calculated

    monkeypatch.setattr(manager.calculator, "calculate", calculate)
    monkeypatch.setattr(manager, "_dependency_freshness", lambda _codes: {})
    return manager, month


def test_source_backed_scope_is_visible_in_preview_and_committed_result(monkeypatch):
    manager, month = _source_backed_manager(monkeypatch)
    committed = []
    monkeypatch.setattr(
        manager,
        "_atomic_replace_scope",
        lambda frame, months, codes: committed.append((list(months), list(codes))) or len(frame),
    )

    preview = manager.preview_source_scope([month])
    assert preview[month.isoformat()]["selected_index_count"] == 1
    assert preview[month.isoformat()]["unavailable_index_codes"] == ["MISSING"]
    manager._planned_source_scope_by_month = preview

    result = manager._run_months(
        [month], batch_size=None, index_codes=None, result_key="backfilled_records"
    )

    assert committed == [([month], ["GOOD"])]
    assert result["backfilled_records"] == 1
    assert result["source_gap_count"] == 1
    assert result["coverage_status"] == "source_gaps"
    assert result["source_scope_by_month"] == preview


@pytest.mark.parametrize("fault", ["changed_plan", "published_source_disappeared"])
def test_source_scope_failure_happens_before_any_replacement(monkeypatch, fault):
    manager, month = _source_backed_manager(
        monkeypatch, published_missing=fault == "published_source_disappeared"
    )
    committed = []
    monkeypatch.setattr(
        manager,
        "_atomic_replace_scope",
        lambda *_args: committed.append(True),
    )
    if fault == "changed_plan":
        manager._planned_source_scope_by_month = {
            month.isoformat(): {"selected_index_count": 2}
        }

    expected = (
        "pit_etf_member_source_scope_changed"
        if fault == "changed_plan"
        else "pit_published_etf_source_disappeared"
    )
    with pytest.raises(ValueError, match=expected):
        manager._run_months(
            [month], batch_size=None, index_codes=None, result_key="backfilled_records"
        )
    assert committed == []


def test_explicit_index_repair_keeps_strict_requested_scope(monkeypatch):
    manager, month = _source_backed_manager(monkeypatch)
    scopes = []
    monkeypatch.setattr(
        manager,
        "_atomic_replace_scope",
        lambda _frame, _months, codes: scopes.append(list(codes)) or 1,
    )

    manager._run_months(
        [month], batch_size=None, index_codes=["GOOD", "MISSING"],
        result_key="backfilled_records",
    )
    assert scopes == [["GOOD", "MISSING"]]
