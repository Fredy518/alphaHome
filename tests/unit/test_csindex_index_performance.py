from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.sources.csindex.csindex_api import CsindexAPI, CsindexAPIError
from alphahome.fetchers.tasks.index.csindex_index_performance import (
    CsindexIndexPerformanceTask,
)


class _MockDB:
    def __init__(self, calendar=None):
        self.calendar = (
            {
                date(2026, 8, 1): 0,
                date(2026, 8, 2): 0,
                date(2026, 8, 3): 1,
                date(2026, 8, 4): 1,
            }
            if calendar is None
            else calendar
        )

    async def table_exists(self, target):
        return False

    async def get_column_names(self, target):
        return []

    async def fetch(self, query, exchange, start_date, end_date):
        assert "rawdata.others_calendar" in query
        assert exchange == "SSE"
        return [
            {"cal_date": cal_date, "is_open": is_open}
            for cal_date, is_open in self.calendar.items()
            if start_date <= cal_date <= end_date
        ]


class _FakeAPI:
    def __init__(self, frame: pd.DataFrame):
        self.frame = frame
        self.calls = []

    async def fetch_performance(
        self, index_code, start_date, end_date, stop_event=None
    ):
        self.calls.append((index_code, start_date, end_date))
        return self.frame.copy()


def _task(api, db=None, **kwargs):
    start_date = kwargs.pop("start_date", "2026-08-01")
    end_date = kwargs.pop("end_date", "2026-08-04")
    return CsindexIndexPerformanceTask(
        db_connection=db or _MockDB(),
        api=api,
        update_type=UpdateTypes.MANUAL,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def test_task_contract_defaults_to_dividend_total_return():
    task = _task(_FakeAPI(pd.DataFrame()))
    assert task.name == "csindex_index_performance"
    assert task.data_source == "csindex"
    assert task.primary_keys == ["index_code", "trade_date"]
    assert task.index_codes == ["H00922"]


@pytest.mark.asyncio
async def test_fetch_and_process_removes_official_chart_anchor():
    raw = pd.DataFrame(
        [
            {
                "tradeDate": "20260801",
                "indexCode": "H00922",
                "indexNameEn": "CSI Dividend TRI",
                "indexNameEnAll": "CSI Dividend Total Return Index",
                "close": 12000.51,
                "change": -41.17,
                "changePct": -0.34,
                "tradingVol": 6657603932,
                "tradingValue": 691.82,
                "consNumber": 100,
                "peg": 8.54,
            },
            {
                "tradeDate": "20260803",
                "indexCode": "H00922",
                "indexNameEn": "CSI Dividend TRI",
                "indexNameEnAll": "CSI Dividend Total Return Index",
                "close": 12000.51,
                "change": -41.17,
                "changePct": -0.34,
                "tradingVol": 6657603932,
                "tradingValue": 691.82,
                "consNumber": 100,
                "peg": 8.54,
            },
            {
                "tradeDate": "20260804",
                "indexCode": "H00922",
                "indexNameEn": "CSI Dividend TRI",
                "indexNameEnAll": "CSI Dividend Total Return Index",
                "close": 11806.28,
                "change": -194.23,
                "changePct": -1.62,
                "tradingVol": 7000000000,
                "tradingValue": 720.0,
                "consNumber": 100,
                "peg": 8.50,
            },
        ]
    )
    fake = _FakeAPI(raw)
    task = _task(fake)
    task._effective_start_date = "20260801"
    task._effective_end_date = "20260804"

    fetched = await task.fetch_batch(
        {"index_code": "H00922", "start_date": "20260801", "end_date": "20260804"}
    )
    processed = task.process_data(fetched)

    assert fake.calls == [("H00922", "20260801", "20260804")]
    assert processed["trade_date"].tolist() == [date(2026, 8, 3), date(2026, 8, 4)]
    assert processed["close"].tolist() == [12000.51, 11806.28]
    assert processed["source_url"].str.contains("csindex.com.cn").all()


@pytest.mark.asyncio
async def test_fetch_removes_every_anchor_across_multiday_market_closure():
    raw = pd.DataFrame(
        [
            {
                "tradeDate": trade_date,
                "indexCode": "H00922",
                "close": close,
                "change": change,
                "changePct": change_pct,
                "tradingVol": volume,
                "tradingValue": value,
                "consNumber": 100,
            }
            for trade_date, close, change, change_pct, volume, value in [
                ("20180616", 6475.67, 0.0, 0.0, 0.0, 0.0),
                ("20180618", 6475.67, 0.0, 0.0, 0.0, 0.0),
                ("20180619", 6230.33, -245.34, -3.79, 5875638724.0, 651.2),
                ("20180620", 6266.95, 36.62, 0.59, 5023411000.0, 608.4),
            ]
        ]
    )
    calendar = {
        date(2018, 6, 16): 0,
        date(2018, 6, 17): 0,
        date(2018, 6, 18): 0,
        date(2018, 6, 19): 1,
        date(2018, 6, 20): 1,
    }
    task = _task(
        _FakeAPI(raw),
        db=_MockDB(calendar),
        start_date="2018-06-16",
        end_date="2018-06-20",
    )
    task._effective_start_date = "20180616"
    task._effective_end_date = "20180620"

    fetched = await task.fetch_batch(
        {"index_code": "H00922", "start_date": "20180616", "end_date": "20180620"}
    )
    processed = task.process_data(fetched)

    assert processed["trade_date"].tolist() == [date(2018, 6, 19), date(2018, 6, 20)]


def test_process_data_returns_empty_frame_when_all_rows_are_invalid():
    task = _task(_FakeAPI(pd.DataFrame()))
    raw = pd.DataFrame(
        [
            {
                "trade_date": "20260804",
                "index_code": "H00922",
                "close": None,
                "source_url": "https://www.csindex.com.cn/",
            }
        ]
    )

    result = task.process_data(raw)

    assert result.empty
    assert list(result.columns) == list(task.schema_def)


@pytest.mark.asyncio
async def test_fetch_fails_closed_when_calendar_does_not_cover_source_date():
    raw = pd.DataFrame(
        [{"tradeDate": "20260803", "indexCode": "H00922", "close": 12000.51}]
    )
    task = _task(_FakeAPI(raw), db=_MockDB(calendar={}))

    with pytest.raises(ValueError, match="交易日历缺少中证返回日期"):
        await task.fetch_batch(
            {"index_code": "H00922", "start_date": "20260801", "end_date": "20260804"}
        )


@pytest.mark.asyncio
async def test_task_rejects_mismatched_index_code():
    raw = pd.DataFrame(
        [{"tradeDate": "20260803", "indexCode": "000300", "close": 4000}]
    )
    task = _task(_FakeAPI(raw))
    with pytest.raises(ValueError, match="指数代码不一致"):
        await task.fetch_batch(
            {"index_code": "H00922", "start_date": "20260801", "end_date": "20260804"}
        )


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = "failure"

    def json(self):
        return self._payload


@pytest.mark.asyncio
async def test_api_parses_success_payload(monkeypatch):
    payload = {
        "success": True,
        "data": [{"tradeDate": "20260803", "indexCode": "H00922", "close": 12000.51}],
    }
    monkeypatch.setattr(
        "alphahome.fetchers.sources.csindex.csindex_api.requests.get",
        lambda *args, **kwargs: _FakeResponse(payload),
    )
    api = CsindexAPI(max_retries=1)
    frame = await api.fetch_performance("H00922", "20260801", "20260804")
    assert frame.iloc[0]["indexCode"] == "H00922"


@pytest.mark.asyncio
async def test_api_raises_after_failed_payload(monkeypatch):
    monkeypatch.setattr(
        "alphahome.fetchers.sources.csindex.csindex_api.requests.get",
        lambda *args, **kwargs: _FakeResponse(
            {"success": False, "code": 500, "msg": "bad"}
        ),
    )
    api = CsindexAPI(max_retries=1)
    with pytest.raises(CsindexAPIError, match="经 1 次请求仍失败"):
        await api.fetch_performance("H00922", "20260801", "20260804")
