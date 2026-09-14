import logging

import pytest

from alphahome.common.db_components.db_manager_core import DBManagerCore


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _Pool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


class _Connection:
    def __init__(self, schema_oid):
        self.schema_oid = schema_oid
        self.executed = []

    async def fetchval(self, query):
        assert query == "SELECT to_regnamespace('rawdata')"
        return self.schema_oid

    async def execute(self, query):
        self.executed.append(query)


def _manager(connection):
    manager = object.__new__(DBManagerCore)
    manager.pool = _Pool(connection)
    manager.logger = logging.getLogger("test.rawdata_init")
    return manager


@pytest.mark.asyncio
async def test_existing_rawdata_schema_initialization_is_read_only():
    connection = _Connection(schema_oid=123)

    await _manager(connection)._initialize_rawdata_schema()

    assert connection.executed == []


@pytest.mark.asyncio
async def test_missing_rawdata_schema_is_created_and_documented():
    connection = _Connection(schema_oid=None)

    await _manager(connection)._initialize_rawdata_schema()

    assert len(connection.executed) == 2
    assert connection.executed[0] == 'CREATE SCHEMA "rawdata"'
    assert "COMMENT ON SCHEMA rawdata" in connection.executed[1]
