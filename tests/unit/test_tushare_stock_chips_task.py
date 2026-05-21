import pytest

from alphahome.fetchers.sources.tushare.batch_utils import generate_stock_code_batches
from alphahome.fetchers.tasks.stock.tushare_stock_chips import TushareStockChipsTask


class _FakeDB:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    async def fetch(self, query):
        self.queries.append(query)
        return self.rows


class _FakeAPI:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []

    async def query(self, **kwargs):
        self.calls.append(kwargs)
        import pandas as pd

        return pd.DataFrame(self.rows)


@pytest.mark.asyncio
async def test_stock_chips_batches_use_ts_code_date_window_for_all_codes():
    db = _FakeDB(
        [
            {"ts_code": "000001.SZ"},
            {"ts_code": "600000.SH"},
        ]
    )
    task = TushareStockChipsTask(
        db_connection=db,
        api_token="dummy-token",
        api=_FakeAPI(),
    )

    batches = await task.get_batch_list(
        start_date="20260518",
        end_date="20260520",
    )

    assert len(batches) == 2
    assert {batch["ts_code"] for batch in batches} == {"000001.SZ", "600000.SH"}
    assert all(batch["start_date"] == "20260518" for batch in batches)
    assert all(batch["end_date"] == "20260520" for batch in batches)
    assert all("trade_date" not in batch for batch in batches)
    assert db.queries


@pytest.mark.asyncio
async def test_stock_chips_single_code_does_not_query_stock_basic():
    db = _FakeDB([{"ts_code": "000001.SZ"}])
    task = TushareStockChipsTask(
        db_connection=db,
        api_token="dummy-token",
        api=_FakeAPI(),
    )

    batches = await task.get_batch_list(
        start_date="20260518",
        end_date="20260520",
        ts_code="600000.SH",
    )

    assert batches == [
        {
            "fields": ",".join(task.fields or []),
            "start_date": "20260518",
            "end_date": "20260520",
            "ts_code": "600000.SH",
        }
    ]
    assert db.queries == []


@pytest.mark.asyncio
async def test_stock_code_batches_api_fallback_passes_list_status_directly():
    api = _FakeAPI(rows=[{"ts_code": "000001.SZ"}])

    batches = await generate_stock_code_batches(
        db_connection=_FakeDB([]),
        api_instance=api,
        additional_params={"start_date": "20260518", "end_date": "20260520"},
    )

    assert batches == [
        {
            "start_date": "20260518",
            "end_date": "20260520",
            "ts_code": "000001.SZ",
        }
    ]
    assert api.calls[0]["list_status"] == "L"
    assert "params" not in api.calls[0]
