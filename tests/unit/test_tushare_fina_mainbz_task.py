import pandas as pd
import pytest

from alphahome.fetchers.tasks.finance.tushare_fina_mainbz import (
    TushareFinaMainbzTask,
)


class _MainBusinessAPI:
    def __init__(self):
        self.calls = []

    async def query(
        self,
        api_name,
        fields,
        limit,
        max_pages=None,
        stop_event=None,
        **params,
    ):
        self.calls.append(
            {
                "api_name": api_name,
                "fields": fields,
                "limit": limit,
                "max_pages": max_pages,
                "params": params,
            }
        )
        return pd.DataFrame(
            {
                "ts_code": ["300382.SZ"],
                "end_date": ["20260630"],
                "bz_code": ["001"],
                "bz_item": ["易拉盖高速生产设备及系统改造"],
            }
        )


def test_fina_mainbz_schema_preserves_business_type_dimension():
    assert TushareFinaMainbzTask.primary_keys == [
        "ts_code",
        "end_date",
        "bz_type",
        "bz_item",
    ]
    assert TushareFinaMainbzTask.schema_def["bz_type"]["constraints"] == "NOT NULL"
    assert TushareFinaMainbzTask.migrate_primary_key_on_schema_check is True
    assert TushareFinaMainbzTask.primary_key_migration_defaults == {"bz_type": "U"}


@pytest.mark.asyncio
async def test_fina_mainbz_fetch_batch_attaches_requested_business_type():
    api = _MainBusinessAPI()
    task = TushareFinaMainbzTask(
        db_connection=object(),
        api_token="test-token",
        api=api,
    )

    result = await task.fetch_batch({"period": "20260630", "type": "D"})

    assert result is not None
    assert result["bz_type"].tolist() == ["D"]
    assert api.calls[0]["params"] == {"period": "20260630", "type": "D"}


@pytest.mark.asyncio
async def test_fina_mainbz_fetch_batch_rejects_unknown_business_type():
    task = TushareFinaMainbzTask(
        db_connection=object(),
        api_token="test-token",
        api=_MainBusinessAPI(),
    )

    with pytest.raises(ValueError, match="无效的主营业务类型"):
        await task.fetch_batch({"period": "20260630", "type": "X"})
