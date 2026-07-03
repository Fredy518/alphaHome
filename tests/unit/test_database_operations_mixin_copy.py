import pandas as pd
import pytest

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

    def transaction(self):
        return _FakeTransaction()

    async def execute(self, sql, *args, timeout=None):
        self.execute_calls.append({"sql": sql, "timeout": timeout})
        return "OK"

    async def copy_records_to_table(self, table, *, records, columns, timeout):
        count = 0
        async for _record in records:
            count += 1
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
