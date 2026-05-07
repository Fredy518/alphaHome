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
