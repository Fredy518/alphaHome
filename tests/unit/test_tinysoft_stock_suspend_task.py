import pandas as pd
import pytest

from alphahome.fetchers.tasks.stock.tinysoft_stock_suspend import TinySoftStockSuspendTask


class _DummyApi:
    async def call_dataframe(self, *args, **kwargs):
        return pd.DataFrame()


class _FakeDB:
    def __init__(self, rows=None):
        self._rows = rows or []

    async def get_column_names(self, target):
        return ["ts_code", "list_status"]

    async def fetch(self, query, *args, **kwargs):
        return self._rows


class _MultiSymbolApi:
    async def call_dataframe(self, func, table_id, **kwargs):
        stock = kwargs.get("stock")
        assert func == "infoarray"
        assert table_id == 127
        return pd.DataFrame(
            {
                "停牌开始日": [20260302],
                "停牌开始时间": ["09:30:00"],
                "停牌截止日": [20260302],
                "停牌截止时间": ["10:30:00"],
                "停牌期限": ["1小时"],
                "停牌原因": ["临时停牌"],
                "StockID": [stock],
            }
        )


def _make_task(task_config=None, db=None, api=None):
    config = task_config or {}
    return TinySoftStockSuspendTask(
        db_connection=db if db is not None else object(),
        api=api or _DummyApi(),
        tinysoft_config={},
        task_config=config,
    )


def test_process_data_extracts_suspend_fields():
    task = _make_task()
    raw = pd.DataFrame(
        {
            "停牌开始日": [20260302],
            "停牌开始时间": ["09:30:00"],
            "停牌截止日": [20260302],
            "停牌截止时间": ["10:30:00"],
            "停牌期限": ["1小时"],
            "停牌原因": ["临时停牌"],
            "StockID": ["SZ000001"],
        }
    )
    processed = task.process_data(raw)
    assert not processed.empty
    row = processed.iloc[0]
    assert row["ts_code"] == "000001.SZ"
    assert str(row["trade_date"]) == "2026-03-02"
    assert row["suspend_term"] == "1小时"
    assert row["event_text"] == "临时停牌"
    assert row["event_type"] == "suspend"


def test_process_data_filters_empty_event_by_default():
    task = _make_task()
    raw = pd.DataFrame(
        {
            "停牌开始日": [20260302],
            "停牌原因": [" "],
            "StockID": ["SZ000001"],
        }
    )
    processed = task.process_data(raw)
    assert processed.empty


@pytest.mark.asyncio
async def test_resolve_symbols_defaults_to_market_scope():
    task = _make_task(
        db=_FakeDB(rows=[{"ts_code": "000001.SZ"}, {"ts_code": "600000.SH"}]),
        task_config={
            "ts_codes": ["000001.SZ"],
            "max_symbols": 1,
        },
    )
    symbols = await task._resolve_symbols()
    assert symbols == ["000001.SZ", "600000.SH"]


@pytest.mark.asyncio
async def test_get_batch_list_uses_infoarray_table_id():
    task = _make_task(
        db=_FakeDB(rows=[{"ts_code": "000001.SZ"}, {"ts_code": "600000.SH"}]),
        task_config={"symbol_batch_size": 2},
    )
    batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260302",
        infoarray_table_id=127,
    )
    assert len(batches) == 1
    assert batches[0]["infoarray_table_id"] == 127
    assert len(batches[0]["symbol_pairs"]) == 2


@pytest.mark.asyncio
async def test_fetch_batch_supports_symbol_pairs():
    task = _make_task(
        db=_FakeDB(),
        api=_MultiSymbolApi(),
        task_config={"skip_failed_symbols": False},
    )
    df = await task.fetch_batch(
        {
            "symbol_pairs": [
                {"ts_code": "000001.SZ", "stock": "SZ000001"},
                {"ts_code": "600000.SH", "stock": "SH600000"},
            ],
            "infoarray_table_id": 127,
            "service": "",
            "timeout_ms": 45000,
        }
    )
    assert df is not None
    assert len(df) == 2
    assert set(df["StockID"]) == {"SZ000001", "SH600000"}
