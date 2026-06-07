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
