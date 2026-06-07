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
    def __init__(self, rows=None, fail_multi=False):
        self.rows = rows or []
        self.fail_multi = fail_multi
        self.calls = []

    async def query(self, **kwargs):
        self.calls.append(kwargs)
        import pandas as pd

        if self.fail_multi and "," in str(kwargs.get("ts_code") or ""):
            raise ValueError("Tushare API 返回错误 (cyq_perf): Code: 50101, Msg: 查询数据失败，请确认参数！")
        if kwargs.get("api_name") == "cyq_perf":
            ts_code = kwargs.get("ts_code")
            return pd.DataFrame([{"ts_code": ts_code, "trade_date": "20260520"}])
        return pd.DataFrame(self.rows)


@pytest.mark.asyncio
async def test_stock_chips_batches_group_listed_codes_with_date_window():
    db = _FakeDB(
        [
            {"ts_code": "000001.SZ"},
            {"ts_code": "600000.SH"},
            {"ts_code": "600036.SH"},
        ]
    )
    task = TushareStockChipsTask(
        db_connection=db,
        api_token="dummy-token",
        api=_FakeAPI(),
        task_config={"code_batch_size": 2},
    )

    batches = await task.get_batch_list(
        start_date="20260518",
        end_date="20260520",
    )

    assert len(batches) == 2
    assert [batch["ts_code"] for batch in batches] == [
        "000001.SZ,600000.SH",
        "600036.SH",
    ]
    assert all(batch["start_date"] == "20260518" for batch in batches)
    assert all(batch["end_date"] == "20260520" for batch in batches)
    assert all("trade_date" not in batch for batch in batches)
    assert "list_status = 'L'" in db.queries[0]


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
async def test_stock_chips_long_range_auto_reduces_code_batch_size():
    db = _FakeDB(
        [
            {"ts_code": "000001.SZ"},
            {"ts_code": "000002.SZ"},
            {"ts_code": "600000.SH"},
        ]
    )
    task = TushareStockChipsTask(
        db_connection=db,
        api_token="dummy-token",
        api=_FakeAPI(),
    )

    batches = await task.get_batch_list(
        start_date="20180101",
        end_date="20260520",
    )

    assert [batch["ts_code"] for batch in batches] == [
        "000001.SZ,000002.SZ",
        "600000.SH",
    ]


@pytest.mark.asyncio
async def test_stock_chips_multi_code_fallback_splits_on_tushare_param_error():
    task = TushareStockChipsTask(
        db_connection=_FakeDB([]),
        api_token="dummy-token",
        api=_FakeAPI(fail_multi=True),
    )

    data = await task.fetch_batch(
        {
            "start_date": "20260518",
            "end_date": "20260520",
            "ts_code": "000001.SZ,600000.SH",
        }
    )

    assert data["ts_code"].tolist() == ["000001.SZ", "600000.SH"]
    cyq_calls = [call for call in task.api.calls if call["api_name"] == "cyq_perf"]
    assert [call["ts_code"] for call in cyq_calls] == [
        "000001.SZ,600000.SH",
        "000001.SZ",
        "600000.SH",
    ]


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
