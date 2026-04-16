import pandas as pd
import pytest

from alphahome.fetchers.tasks.index.tinysoft_index_minute import (
    TinySoftIndexMinuteTask,
    index_ts_code_to_tinysoft_symbol,
    normalize_index_ts_code,
    normalize_tinysoft_index_symbol,
    tinysoft_index_symbol_to_ts_code,
)


class _DummyApi:
    async def query(self, **kwargs):
        return pd.DataFrame()


class _IndexDB:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.queries = []

    async def get_column_names(self, target):
        if target in {"tushare.index_basic", "rawdata.index_basic"}:
            return ["ts_code", "market"]
        return []

    async def fetch(self, query, *args, **kwargs):
        self.queries.append(str(query))
        return self._rows


def test_index_code_conversion_roundtrip():
    assert normalize_index_ts_code("000300.csi") == "000300.CSI"
    assert index_ts_code_to_tinysoft_symbol("000001.SH") == "SH000001"
    assert index_ts_code_to_tinysoft_symbol("H30351.CSI") == "CSIH30351"
    assert normalize_tinysoft_index_symbol("csi000300") == "CSI000300"
    assert tinysoft_index_symbol_to_ts_code("SH000001") == "000001.SH"
    assert tinysoft_index_symbol_to_ts_code("CSI000300") == "000300.CSI"


def test_process_data_maps_index_codes_and_trade_date():
    task = TinySoftIndexMinuteTask(
        db_connection=object(),
        api=_DummyApi(),
        tinysoft_config={},
        task_config={},
    )
    raw = pd.DataFrame(
        {
            "date": ["2026-03-02 09:31:00"],
            "StockID": ["CSI000300"],
            "open": [3900.0],
            "high": [3910.0],
            "low": [3895.0],
            "close": [3905.0],
            "vol": [1000],
            "amount": [100000],
        }
    )

    processed = task.process_data(raw)

    assert not processed.empty
    assert processed["index_code_raw"].iloc[0] == "CSI000300"
    assert processed["index_ts_code"].iloc[0] == "000300.CSI"
    assert str(processed["trade_date"].iloc[0]) == "2026-03-02"
    assert processed["volume"].iloc[0] == pytest.approx(10.0)
    assert processed["amount"].iloc[0] == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_resolve_symbol_pairs_defaults_to_market_scope():
    task = TinySoftIndexMinuteTask(
        db_connection=_IndexDB(
            rows=[
                {"ts_code": "000001.SH"},
                {"ts_code": "000300.CSI"},
                {"ts_code": "HSI.HI"},
            ]
        ),
        api=_DummyApi(),
        tinysoft_config={},
        task_config={
            "ts_codes": ["000001.SH"],
            "max_symbols": 1,
        },
    )

    pairs = await task._resolve_symbol_pairs()

    assert [pair["stock"] for pair in pairs] == ["SH000001", "CSI000300"]


@pytest.mark.asyncio
async def test_get_batch_list_accepts_mixed_runtime_formats():
    task = TinySoftIndexMinuteTask(
        db_connection=_IndexDB(),
        api=_DummyApi(),
        tinysoft_config={},
        task_config={},
    )

    batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260301",
        ts_codes=["000001.SH", "CSI000300"],
        batch_days=1,
        symbol_batch_size=1,
        use_trade_day_batches=False,
        all_symbols_in_one_group=False,
    )

    assert len(batches) == 2
    assert batches[0]["symbol_pairs"][0]["stock"] == "SH000001"
    assert batches[1]["symbol_pairs"][0]["stock"] == "CSI000300"
