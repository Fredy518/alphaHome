import pandas as pd

from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import (
    TinySoftStockMinuteTask,
    normalize_ts_code,
    tinysoft_symbol_to_ts_code,
    ts_code_to_tinysoft_symbol,
)


class _DummyApi:
    async def query(self, **kwargs):
        return pd.DataFrame()


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
