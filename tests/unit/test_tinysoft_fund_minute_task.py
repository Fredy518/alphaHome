import pandas as pd
import pytest

from alphahome.fetchers.tasks.fund.tinysoft_fund_minute import TinySoftFundMinuteTask


class _DummyApi:
    async def query(self, **kwargs):
        return pd.DataFrame()


class _FundDB:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.queries = []

    async def get_column_names(self, target):
        if target in {"tushare.fund_basic", "rawdata.fund_basic"}:
            return ["ts_code", "market", "status"]
        if target in {"tushare.fund_etf_basic", "rawdata.fund_etf_basic"}:
            return ["ts_code"]
        return []

    async def fetch(self, query, *args, **kwargs):
        self.queries.append(str(query))
        return self._rows


@pytest.mark.asyncio
async def test_resolve_symbols_defaults_to_exchange_funds():
    db = _FundDB(
        rows=[
            {"ts_code": "510300.SH"},
            {"ts_code": "160706.SZ"},
            {"ts_code": "000001.OF"},
        ]
    )
    task = TinySoftFundMinuteTask(
        db_connection=db,
        api=_DummyApi(),
        tinysoft_config={},
        task_config={
            "ts_codes": ["510300.SH"],
            "max_symbols": 1,
        },
    )

    symbols = await task._resolve_symbols()

    assert symbols == ["510300.SH", "160706.SZ"]
    assert any("market = 'E'" in query for query in db.queries)
    assert any("status = 'L'" in query for query in db.queries)


def test_process_data_maps_columns_for_exchange_fund():
    task = TinySoftFundMinuteTask(
        db_connection=object(),
        api=_DummyApi(),
        tinysoft_config={},
        task_config={},
    )
    raw = pd.DataFrame(
        {
            "date": ["2026-03-02 09:31:00"],
            "StockID": ["SH510300"],
            "open": [4.5],
            "high": [4.6],
            "low": [4.4],
            "close": [4.55],
            "vol": [1000],
            "amount": [100000],
        }
    )

    processed = task.process_data(raw)

    assert not processed.empty
    assert processed["ts_code"].iloc[0] == "510300.SH"
    assert str(processed["trade_date"].iloc[0]) == "2026-03-02"
    assert processed["volume"].iloc[0] == pytest.approx(10.0)
    assert processed["amount"].iloc[0] == pytest.approx(100.0)

