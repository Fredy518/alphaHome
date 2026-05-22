import pytest

from alphahome.fetchers.tasks.cbond.tushare_cbond_price_chg import TushareCBondPriceChgTask


class _FakeDB:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    async def fetch(self, query):
        self.queries.append(query)
        return self.rows


class _FakeAPI:
    pass


@pytest.mark.asyncio
async def test_cbond_price_chg_batches_use_multi_value_ts_code():
    task = TushareCBondPriceChgTask(
        db_connection=_FakeDB(
            [
                {"ts_code": "110001.SH"},
                {"ts_code": "110002.SH"},
                {"ts_code": "123001.SZ"},
            ]
        ),
        api_token="dummy-token",
        api=_FakeAPI(),
        task_config={"code_batch_size": 2},
    )

    batches = await task.get_batch_list()

    assert batches == [
        {"ts_code": "110001.SH,110002.SH"},
        {"ts_code": "123001.SZ"},
    ]


@pytest.mark.asyncio
async def test_cbond_price_chg_invalid_batch_size_falls_back_to_default():
    task = TushareCBondPriceChgTask(
        db_connection=_FakeDB([{"ts_code": f"{i:06d}.SH"} for i in range(301)]),
        api_token="dummy-token",
        api=_FakeAPI(),
        task_config={"code_batch_size": "bad"},
    )

    batches = await task.get_batch_list()

    assert len(batches) == 2
    assert len(batches[0]["ts_code"].split(",")) == task.default_code_batch_size
