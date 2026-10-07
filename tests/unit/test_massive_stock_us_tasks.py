import asyncio
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.sources.massive import MassiveAPIError, MassiveTask
from alphahome.fetchers.tasks.stock.massive_stock_us_basic import (
    MassiveStockUsBasicTask,
)
from alphahome.fetchers.tasks.stock.massive_stock_us_daily import (
    MassiveStockUsDailyTask,
)
from alphahome.fetchers.tasks.stock.massive_stock_us_split import (
    MassiveStockUsSplitTask,
)

TASKS = (MassiveStockUsBasicTask, MassiveStockUsDailyTask, MassiveStockUsSplitTask)


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch):
    monkeypatch.setattr(
        MassiveTask,
        "now",
        staticmethod(lambda: datetime(2026, 9, 28, 8, tzinfo=timezone.utc)),
    )


def make_task(cls=MassiveStockUsDailyTask, **kwargs):
    db = Mock()
    db.get_latest_date = AsyncMock(return_value=None)
    api = Mock(request=AsyncMock(), list_records=AsyncMock())
    config = {"min_daily_records": 1, "min_basic_records": 1}
    config.update(kwargs.pop("task_config", {}))
    return cls(db, api=api, task_config=config, **kwargs)


def bar(ticker="BRK.B", day="2026-09-25"):
    timestamp = pd.Timestamp(day, tz="America/New_York").value // 1_000_000
    return {"T": ticker, "o": 10, "h": 12, "l": 9, "c": 11, "v": 123.5, "t": timestamp}


def payload(rows):
    return {
        "status": "OK",
        "adjusted": False,
        "results": rows,
        "resultsCount": len(rows),
    }


@pytest.mark.parametrize("cls", TASKS)
def test_registration_and_chronological_fail_closed_defaults(cls):
    task = make_task(
        cls,
        task_config={"concurrent_limit": 8, "continue_on_stream_batch_failure": True},
    )
    assert UnifiedTaskFactory._task_registry[task.name] is cls
    assert task.concurrent_limit == 1
    assert task.max_retries == 1
    assert (
        task._continue_on_stream_batch_failure(
            {"continue_on_stream_batch_failure": True}
        )
        is False
    )
    assert task.get_full_table_name() == f"massive.{task.table_name}"


@pytest.mark.asyncio
async def test_holidays_weekends_and_new_york_date_are_respected(monkeypatch):
    task = make_task()
    batches = await task.get_batch_list(start_date="20260702", end_date="20260706")
    assert batches == [{"date": "2026-07-02"}, {"date": "2026-07-06"}]
    # Friday evening ET is already Saturday in China, but Basic must wait.
    monkeypatch.setattr(
        MassiveTask,
        "now",
        staticmethod(lambda: datetime(2026, 9, 26, 2, tzinfo=timezone.utc)),
    )
    assert task.latest_complete_session() == date(2026, 9, 24)
    monkeypatch.setattr(
        MassiveTask,
        "now",
        staticmethod(lambda: datetime(2026, 9, 26, 8, tzinfo=timezone.utc)),
    )
    assert task.latest_complete_session() == date(2026, 9, 25)


@pytest.mark.asyncio
async def test_window_modes_are_bounded_and_manual_outside_entitlement_fails():
    task = make_task(update_type=UpdateTypes.FULL)
    assert await task._determine_date_range() == {
        "start_date": "20240929",
        "end_date": "20260925",
    }
    task = make_task()
    assert await task._determine_date_range() == {
        "start_date": "20260919",
        "end_date": "20260925",
    }
    task.db.get_latest_date.return_value = date(2026, 9, 23)
    assert await task._determine_date_range() == {
        "start_date": "20260921",
        "end_date": "20260925",
    }
    task = make_task(
        update_type=UpdateTypes.MANUAL, start_date="20240920", end_date="20240930"
    )
    with pytest.raises(ValueError, match="历史权限"):
        await task._determine_date_range()


@pytest.mark.asyncio
async def test_full_basic_is_one_complete_latest_universe():
    task = make_task(MassiveStockUsBasicTask, update_type=UpdateTypes.FULL)
    assert await task._get_effective_batch_list() == [{"date": "2026-09-25"}]


@pytest.mark.asyncio
async def test_daily_preserves_raw_units_and_symbols():
    task = make_task()
    task.api.request.return_value = payload([bar()])
    result = task.process_data(await task.fetch_batch({"date": "2026-09-25"}))
    assert result.iloc[0]["ticker"] == "BRK.B"
    assert result.iloc[0]["volume"] == 123.5
    assert result.iloc[0]["trade_date"] == date(2026, 9, 25)
    assert not result.iloc[0]["adjusted"]
    assert pd.isna(result.iloc[0]["transactions"])
    assert pd.isna(result.iloc[0]["vwap"])
    assert task.api.request.call_args.args[1] == {
        "adjusted": "false",
        "include_otc": "false",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "variant",
    [
        "empty",
        "wrong_date",
        "adjusted",
        "duplicate",
        "negative_volume",
        "bad_ohlc",
        "missing_close",
        "otc",
        "infinity",
    ],
)
async def test_bad_market_day_is_never_silently_filtered(variant):
    task = make_task()
    body = payload([bar()])
    row = body["results"][0]
    if variant == "empty":
        body["results"] = []
    elif variant == "wrong_date":
        body["results"] = [bar(day="2026-09-24")]
    elif variant == "adjusted":
        body["adjusted"] = True
    elif variant == "duplicate":
        body["results"] = [bar(), bar()]
    elif variant == "negative_volume":
        row["v"] = -1
    elif variant == "bad_ohlc":
        row["h"] = 1
    elif variant == "missing_close":
        del row["c"]
    elif variant == "otc":
        row["otc"] = True
    elif variant == "infinity":
        row["c"] = float("inf")
    task.api.request.return_value = body
    with pytest.raises(MassiveAPIError):
        task.process_data(await task.fetch_batch({"date": "2026-09-25"}))


@pytest.mark.asyncio
async def test_security_types_are_preserved_and_historical_date_is_explicit():
    task = make_task(MassiveStockUsBasicTask)
    task.api.list_records.return_value = [
        {
            "ticker": code,
            "name": code,
            "type": kind,
            "primary_exchange": exchange,
            "active": True,
            "locale": "us",
            "market": "stocks",
        }
        for code, kind, exchange in [
            ("A", "CS", "XNYS"),
            ("SPY", "ETF", "ARCX"),
            ("BABA", "ADRC", "XNYS"),
            ("OTC", "CS", None),
        ]
    ]
    result = task.process_data(await task.fetch_batch({"date": "2026-09-24"}))
    assert result["security_type"].tolist() == ["CS", "ETF", "ADRC"]
    assert set(result["snapshot_date"]) == {date(2026, 9, 24)}
    assert task.api.list_records.call_args.args[1]["date"] == "2026-09-24"
    assert set(result["observed_at"].dt.date) == {date(2026, 9, 28)}


@pytest.mark.asyncio
async def test_splits_preserve_event_ratio_and_reject_out_of_window():
    task = make_task(MassiveStockUsSplitTask)
    task.api.list_records.return_value = [
        {
            "id": "event1",
            "ticker": "A",
            "execution_date": "2026-09-24",
            "split_from": 1,
            "split_to": 10,
            "adjustment_type": "forward_split",
            "historical_adjustment_factor": 0.01,
        }
    ]
    params = {"start_date": "2026-09-24", "end_date": "2026-09-25"}
    result = task.process_data(await task.fetch_batch(params))
    assert result.iloc[0]["split_to"] / result.iloc[0]["split_from"] == 10
    assert "historical_adjustment_factor" not in result
    assert task.api.list_records.call_args.args[0] == "/stocks/v1/splits"
    with pytest.raises(MassiveAPIError, match="窗口"):
        await task.fetch_batch({"start_date": "2026-09-25", "end_date": "2026-09-25"})


@pytest.mark.asyncio
async def test_streaming_failure_does_not_fetch_later_day_or_advance_past_failure():
    task = make_task(
        update_type=UpdateTypes.MANUAL, start_date="20260923", end_date="20260925"
    )
    task.api.request.side_effect = [
        payload([bar(day="2026-09-23")]),
        MassiveAPIError("HTTP 403"),
    ]
    task._save_data = AsyncMock(return_value={"rows": 1})
    result = await task.execute()
    assert result["status"] == "error"
    assert task._save_data.await_count == 1
    assert task.api.request.await_count == 2


@pytest.mark.asyncio
async def test_snapshot_replace_is_bounded_and_cancellation_occurs_inside_transaction():
    task = make_task()
    task.api.request.return_value = payload([bar()])
    data = task.process_data(await task.fetch_batch({"date": "2026-09-25"}))
    connection = Mock(execute=AsyncMock(), copy_records_to_table=AsyncMock())
    transaction = AsyncMock()
    connection.transaction.return_value = transaction
    acquire = AsyncMock()
    acquire.__aenter__.return_value = connection
    task.db.pool.acquire.return_value = acquire
    assert await task._save_to_database(data) == 1
    delete = connection.execute.call_args_list[1]
    assert 'WHERE "trade_date" = ANY($1::date[])' in delete.args[0]
    assert delete.args[1] == [date(2026, 9, 25)]
    assert connection.copy_records_to_table.call_args.kwargs["records"][0][0] == "BRK.B"
    event = asyncio.Event()
    connection.copy_records_to_table.side_effect = lambda *args, **kw: event.set()
    with pytest.raises(asyncio.CancelledError):
        await task._save_to_database(data, stop_event=event)
    assert transaction.__aexit__.call_args.args[0] is asyncio.CancelledError


@pytest.mark.asyncio
async def test_vendor_nanosecond_timestamp_is_explicitly_normalized_to_db_precision():
    task = make_task(MassiveStockUsBasicTask)
    task.api.list_records.return_value = [
        {
            "ticker": "A",
            "name": "A",
            "type": "CS",
            "primary_exchange": "XNYS",
            "active": True,
            "locale": "us",
            "market": "stocks",
            "last_updated_utc": "2026-09-24T12:34:56.123456789Z",
        }
    ]
    result = task.process_data(await task.fetch_batch({"date": "2026-09-25"}))
    value = result.iloc[0]["source_updated_at"]
    assert value == pd.Timestamp("2026-09-24T12:34:56.123456Z")
    assert value.nanosecond == 0


@pytest.mark.asyncio
async def test_historical_unclassified_issues_are_retained_without_inventing_type():
    task = make_task(MassiveStockUsBasicTask)
    common = {
        "name": "Listed issue",
        "primary_exchange": "XNYS",
        "active": True,
        "locale": "us",
        "market": "stocks",
    }
    task.api.list_records.return_value = [
        {**common, "ticker": "A", "type": "CS"},
        {**common, "ticker": "B"},
        {**common, "ticker": "C", "type": None},
        {**common, "ticker": "D", "type": " "},
    ]
    result = task.process_data(await task.fetch_batch({"date": "2024-09-30"}))
    assert result["ticker"].tolist() == ["A", "B", "C", "D"]
    assert result["security_type"].tolist() == ["CS", "UNKNOWN", "UNKNOWN", "UNKNOWN"]
    assert result.query("security_type == 'CS'")["ticker"].tolist() == ["A"]
    # Loss of the entire classification field still indicates a broken schema.
    task.api.list_records.return_value = [{**common, "ticker": "B"}]
    with pytest.raises(MassiveAPIError, match="缺少必需分类字段"):
        await task.fetch_batch({"date": "2024-09-30"})
