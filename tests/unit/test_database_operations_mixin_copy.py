import pandas as pd
import pytest
from datetime import date, datetime, timezone
from decimal import Decimal

from alphahome.common.db_components.database_operations_mixin import DatabaseOperationsMixin


class _FakeLogger:
    def debug(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None


class _FakeResolver:
    def get_schema_and_table(self, target):
        return "public", "target_table"


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self):
        self.copy_calls = []
        self.execute_calls = []
        self.records = []

    def transaction(self):
        return _FakeTransaction()

    async def execute(self, sql, *args, timeout=None):
        self.execute_calls.append({"sql": sql, "timeout": timeout})
        return "OK"

    async def copy_records_to_table(self, table, *, records, columns, timeout):
        count = 0
        async for _record in records:
            count += 1
            self.records.append(_record)
        self.copy_calls.append(
            {
                "table": table,
                "columns": list(columns),
                "timeout": timeout,
                "count": count,
            }
        )
        return f"COPY {count}"


class _FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _FakeAcquire(self.connection)


class _CopyHarness(DatabaseOperationsMixin):
    def __init__(self, connection):
        self.pool = _FakePool(connection)
        self.resolver = _FakeResolver()
        self.logger = _FakeLogger()
        self.copy_records_chunk_size = 2
        self.copy_records_timeout_seconds = 11
        self.bulk_execute_timeout_seconds = 22


@pytest.mark.asyncio
async def test_copy_from_dataframe_chunks_copy_but_merges_once():
    connection = _FakeConnection()
    harness = _CopyHarness(connection)
    data = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50],
        }
    )

    copied = await harness.copy_from_dataframe(
        data,
        target="target_table",
        conflict_columns=["id"],
        update_columns=["value"],
    )

    assert copied == 5
    assert [call["count"] for call in connection.copy_calls] == [2, 2, 1]
    assert {call["timeout"] for call in connection.copy_calls} == {11}

    merge_calls = [
        call
        for call in connection.execute_calls
        if "ON CONFLICT" in call["sql"]
    ]
    assert len(merge_calls) == 1
    assert merge_calls[0]["timeout"] == 22


@pytest.mark.asyncio
async def test_replace_from_dataframe_stages_then_replaces_in_one_transaction():
    connection = _FakeConnection()
    harness = _CopyHarness(connection)
    data = pd.DataFrame({"id": [1, 2], "value": [10, 20]})

    copied = await harness.replace_from_dataframe(data, target="target_table")

    assert copied == 2
    statements = [call["sql"] for call in connection.execute_calls]
    lock_index = next(i for i, sql in enumerate(statements) if "LOCK TABLE" in sql)
    delete_index = next(i for i, sql in enumerate(statements) if "DELETE FROM" in sql)
    insert_index = next(
        i
        for i, sql in enumerate(statements)
        if "INSERT INTO" in sql and "ON CONFLICT" not in sql
    )
    assert lock_index < delete_index < insert_index
    assert "ON CONFLICT" not in statements[insert_index]


@pytest.mark.asyncio
async def test_bulk_copy_preserves_dates_timestamps_numbers_and_missing_values():
    connection = _FakeConnection()
    harness = _CopyHarness(connection)
    harness._get_date_and_timestamp_columns_from_target = lambda target: ({"day"}, {"stamp"})
    stamp = datetime(2026, 9, 30, 12, 34, tzinfo=timezone.utc)
    data = pd.DataFrame({
        "day": [pd.Timestamp("2026-09-30"), pd.NaT],
        "stamp": [stamp, None],
        "value": [Decimal("12.3400"), None],
        "count": [10, 20],
    })
    assert await harness.copy_from_dataframe(data, target="target_table") == 2
    assert connection.records == [(date(2026, 9, 30), stamp, Decimal("12.3400"), 10), (None, None, None, 20)]


@pytest.mark.asyncio
async def test_opt_in_unchanged_guard_is_null_safe_and_ignores_ingestion_time():
    connection = _FakeConnection()
    harness = _CopyHarness(connection)
    harness.skip_unchanged_upserts = True
    harness.sort_bulk_conflict_keys = True
    data = pd.DataFrame({"id": [2, 1], "value": [None, 10], "update_time": [pd.NaT, pd.NaT]})
    assert await harness.copy_from_dataframe(data, target="target_table", conflict_columns=["id"], timestamp_column="update_time") == 2
    merge = next(x["sql"] for x in connection.execute_calls if "ON CONFLICT" in x["sql"])
    assert 'ORDER BY "id"' in merge
    guard = merge.split(" WHERE ", 1)[1]
    assert '"value" IS DISTINCT FROM EXCLUDED."value"' in guard
    assert "update_time" not in guard


@pytest.mark.asyncio
async def test_default_bulk_upsert_keeps_its_existing_update_policy():
    connection = _FakeConnection()
    harness = _CopyHarness(connection)
    await harness.copy_from_dataframe(pd.DataFrame({"id": [1], "value": [10]}), target="target_table", conflict_columns=["id"])
    merge = next(x["sql"] for x in connection.execute_calls if "ON CONFLICT" in x["sql"])
    assert " WHERE " not in merge
    assert "ORDER BY" not in merge


@pytest.mark.asyncio
@pytest.mark.parametrize("provided_timestamp", [False, True])
async def test_explicit_payload_update_columns_also_maintain_requested_timestamp(provided_timestamp):
    connection = _FakeConnection()
    harness = _CopyHarness(connection)
    payload = {"id": [1], "value": [None]}
    if provided_timestamp:
        payload["update_time"] = [datetime(2026, 1, 1)]
    data = pd.DataFrame(payload)
    updates = ["value"]
    await harness.copy_from_dataframe(
        data, target="target_table", conflict_columns=["id"],
        update_columns=updates, timestamp_column="update_time",
    )
    merge = next(x["sql"] for x in connection.execute_calls if "ON CONFLICT" in x["sql"])
    assert '"update_time" = CASE WHEN' in merge
    assert '"value" IS DISTINCT FROM EXCLUDED."value"' in merge
    assert "THEN CURRENT_TIMESTAMP" in merge
    selection = merge.split('ON CONFLICT', 1)[0]
    assert ('CURRENT_TIMESTAMP AS "update_time"' in selection) is (not provided_timestamp)
    assert updates == ["value"]
    assert list(data.columns) == list(payload)


@pytest.mark.asyncio
async def test_explicit_empty_update_columns_keep_do_nothing_with_timestamp():
    connection = _FakeConnection()
    await _CopyHarness(connection).copy_from_dataframe(
        pd.DataFrame({"id": [1], "value": [10]}), target="target_table",
        conflict_columns=["id"], update_columns=[], timestamp_column="update_time",
    )
    merge = next(x["sql"] for x in connection.execute_calls if "ON CONFLICT" in x["sql"])
    assert "DO NOTHING" in merge
