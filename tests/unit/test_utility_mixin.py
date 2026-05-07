import logging
from datetime import datetime

import pytest

from alphahome.common.db_components.utility_mixin import UtilityMixin


class _Resolver:
    @staticmethod
    def get_schema_and_table(target):
        return "akshare", "missing_table"


class _UtilityMixinStub(UtilityMixin):
    def __init__(self, *, table_exists_result, fetch_val_result=None):
        self.resolver = _Resolver()
        self.logger = logging.getLogger("test.utility_mixin")
        self._table_exists_result = table_exists_result
        self._fetch_val_result = fetch_val_result
        self.fetch_val_calls = 0

    async def table_exists(self, target):
        return self._table_exists_result

    async def fetch_val(self, query):
        self.fetch_val_calls += 1
        return self._fetch_val_result


@pytest.mark.asyncio
async def test_get_latest_date_returns_none_when_table_missing():
    mixin = _UtilityMixinStub(table_exists_result=False)

    result = await mixin.get_latest_date(object(), "period_end_date")

    assert result is None
    assert mixin.fetch_val_calls == 0


@pytest.mark.asyncio
async def test_get_latest_date_queries_existing_table():
    expected = datetime(2024, 3, 31)
    mixin = _UtilityMixinStub(table_exists_result=True, fetch_val_result=expected)

    result = await mixin.get_latest_date(object(), "period_end_date")

    assert result == expected
    assert mixin.fetch_val_calls == 1
