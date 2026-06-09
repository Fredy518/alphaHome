import pandas as pd
import pytest

from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import (
    TinySoftStockMinuteTask,
    normalize_ts_code,
    tinysoft_symbol_to_ts_code,
    ts_code_to_tinysoft_symbol,
)


class _DummyApi:
    async def query(self, **kwargs):
        return pd.DataFrame()


class _PanelApi:
    def __init__(self, *, panel_has_identifier=True):
        self.panel_has_identifier = panel_has_identifier
        self.panel_calls = []
        self.query_calls = []

    async def query_panel(self, **kwargs):
        self.panel_calls.append(kwargs)
        if self.panel_has_identifier:
            return pd.DataFrame(
                {
                    "date": ["2026-03-02 09:31:00", "2026-03-02 09:31:00"],
                    "StockID": list(kwargs["stocks"]),
                    "close": [10.0, 20.0],
                }
            )
        return pd.DataFrame({"date": ["2026-03-02 09:31:00"], "close": [10.0]})

    async def query(self, **kwargs):
        self.query_calls.append(kwargs)
        return pd.DataFrame(
            {
                "date": ["2026-03-02 09:31:00"],
                "StockID": [kwargs["stock"]],
                "close": [10.0],
            }
        )


def _make_task():
    return TinySoftStockMinuteTask(
        db_connection=object(),
        api=_DummyApi(),
        tinysoft_config={},
        task_config={"ts_codes": ["000001.SZ"]},
    )


def test_ts_code_and_tinysoft_symbol_conversion_roundtrip():
    assert normalize_ts_code("000001.sz") == "000001.SZ"
    assert ts_code_to_tinysoft_symbol("600000.SH") == "SH600000"
    assert tinysoft_symbol_to_ts_code("SZ000001") == "000001.SZ"
    assert tinysoft_symbol_to_ts_code("INVALID") is None


def test_process_data_maps_columns_and_builds_trade_date():
    task = _make_task()
    raw = pd.DataFrame(
        {
            "date": ["2026-02-27 09:31:00", "2026-02-27 09:32:00"],
            "StockID": ["SZ000001", "SZ000001"],
            "open": [10.1, 10.2],
            "high": [10.3, 10.4],
            "low": [10.0, 10.1],
            "close": [10.2, 10.3],
            "vol": [1000, 1200],
            "amount": [100000, 120000],
        }
    )

    processed = task.process_data(raw)

    assert not processed.empty
    assert "ts_code" in processed.columns
    assert "trade_time" in processed.columns
    assert "trade_date" in processed.columns
    assert "volume" in processed.columns
    assert processed["ts_code"].iloc[0] == "000001.SZ"
    assert str(processed["trade_date"].iloc[0]) == "2026-02-27"


@pytest.mark.asyncio
async def test_get_batch_list_groups_symbols_for_panel_query():
    task = TinySoftStockMinuteTask(
        db_connection=object(),
        api=_DummyApi(),
        tinysoft_config={},
        task_config={},
    )

    batches = await task.get_batch_list(
        start_date="20260302",
        end_date="20260302",
        ts_codes=["000001.SZ", "600000.SH", "000002.SZ"],
        symbol_batch_size=2,
    )

    assert len(batches) == 2
    assert [pair["stock"] for pair in batches[0]["symbol_pairs"]] == ["SZ000001", "SH600000"]
    assert [pair["stock"] for pair in batches[1]["symbol_pairs"]] == ["SZ000002"]
    assert batches[1]["stock"] == "SZ000002"


@pytest.mark.asyncio
async def test_fetch_batch_prefers_panel_query_for_symbol_pairs():
    api = _PanelApi()
    task = TinySoftStockMinuteTask(
        db_connection=object(),
        api=api,
        tinysoft_config={},
        task_config={},
    )

    df = await task.fetch_batch(
        {
            "symbol_pairs": [
                {"ts_code": "000001.SZ", "stock": "SZ000001"},
                {"ts_code": "600000.SH", "stock": "SH600000"},
            ],
            "cycle": "1分钟线",
            "begin_time": "2026-03-02 09:30:00",
            "end_time": "2026-03-02 15:00:00",
            "fields": ["date", "StockID", "close"],
        }
    )

    assert len(api.panel_calls) == 1
    assert api.query_calls == []
    assert set(df["StockID"]) == {"SZ000001", "SH600000"}


@pytest.mark.asyncio
async def test_fetch_batch_falls_back_when_panel_lacks_identifier():
    api = _PanelApi(panel_has_identifier=False)
    task = TinySoftStockMinuteTask(
        db_connection=object(),
        api=api,
        tinysoft_config={},
        task_config={},
    )

    df = await task.fetch_batch(
        {
            "symbol_pairs": [
                {"ts_code": "000001.SZ", "stock": "SZ000001"},
                {"ts_code": "600000.SH", "stock": "SH600000"},
            ],
            "cycle": "1分钟线",
            "begin_time": "2026-03-02 09:30:00",
            "end_time": "2026-03-02 15:00:00",
            "fields": ["date", "StockID", "close"],
        }
    )

    assert len(api.panel_calls) == 1
    assert len(api.query_calls) == 2
    assert set(df["StockID"]) == {"SZ000001", "SH600000"}
