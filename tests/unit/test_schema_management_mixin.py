import pytest

from alphahome.common.db_components.schema_management_mixin import SchemaManagementMixin


@pytest.fixture
def mixin() -> SchemaManagementMixin:
    return SchemaManagementMixin()


@pytest.mark.parametrize(
    "raw_columns, expected",
    [
        ("period_end_date", ["period_end_date"]),
        ("series_id, period_end_date", ["series_id", "period_end_date"]),
        (' "series_id" , "period_end_date" ', ["series_id", "period_end_date"]),
        (["series_id", " period_end_date "], ["series_id", "period_end_date"]),
    ],
)
def test_normalize_index_columns_valid(
    mixin: SchemaManagementMixin, raw_columns, expected
) -> None:
    assert mixin._normalize_index_columns(raw_columns) == expected


@pytest.mark.parametrize(
    "raw_columns",
    [
        None,
        123,
        [],
        ["series_id", 1],
        " , ",
    ],
)
def test_normalize_index_columns_invalid_returns_none(
    mixin: SchemaManagementMixin, raw_columns
) -> None:
    assert mixin._normalize_index_columns(raw_columns) is None


def test_build_update_time_index_sql_uses_canonical_single_column_index(
    mixin: SchemaManagementMixin,
) -> None:
    index_name, sql = mixin._build_update_time_index_sql(
        "tushare.stock_daily",
        "stock_daily",
    )

    assert index_name == "idx_stock_daily_update_time"
    assert sql == (
        'CREATE INDEX IF NOT EXISTS "idx_stock_daily_update_time" '
        'ON tushare.stock_daily ("update_time");'
    )


class _AsyncContext:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _PrimaryKeyConnection:
    def __init__(self):
        self.executed = []

    def transaction(self):
        return _AsyncContext()

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "OK"

    async def fetchrow(self, sql, *args):
        return {
            "conname": "fina_mainbz_pkey",
            "columns": ["ts_code", "end_date", "bz_item"],
        }

    async def fetchval(self, sql, *args):
        if "SELECT EXISTS" in sql:
            return False
        return 0


class _PrimaryKeyPool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _AsyncContext(self.connection)


@pytest.mark.asyncio
async def test_opt_in_primary_key_migration_is_atomic_and_explicit(
    mixin: SchemaManagementMixin,
) -> None:
    connection = _PrimaryKeyConnection()
    mixin.pool = _PrimaryKeyPool(connection)

    class _Target:
        migrate_primary_key_on_schema_check = True
        primary_keys = ["ts_code", "end_date", "bz_type", "bz_item"]
        primary_key_migration_defaults = {"bz_type": "U"}

    actions = await mixin._ensure_opt_in_primary_key_compatible(
        _Target(),
        schema="tushare",
        table="fina_mainbz",
    )

    statements = [sql for sql, _ in connection.executed]
    assert statements[0] == (
        'LOCK TABLE "tushare"."fina_mainbz" IN ACCESS EXCLUSIVE MODE;'
    )
    assert any('DROP CONSTRAINT "fina_mainbz_pkey"' in sql for sql in statements)
    assert any(
        'ADD PRIMARY KEY ("ts_code", "end_date", "bz_type", "bz_item")' in sql
        for sql in statements
    )
    assert actions == [
        "migrate_primary_key:ts_code,end_date,bz_item->"
        "ts_code,end_date,bz_type,bz_item"
    ]
