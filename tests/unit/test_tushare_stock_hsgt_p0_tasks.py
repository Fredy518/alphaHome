from datetime import date, datetime

import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.tasks.stock.tushare_stock_hk_hold import TushareStockHkHoldTask
from alphahome.fetchers.tasks.stock.tushare_stock_hsgt_top10 import TushareStockHsgtTop10Task
from alphahome.fetchers.tasks.stock.tushare_stock_moneyflow_hsgt import (
    TushareStockMoneyflowHsgtTask,
)


class _FakeDb:
    def __init__(self, latest_date=None):
        self.latest_date = latest_date

    async def get_latest_date(self, task, date_column):
        return self.latest_date


P0_TASK_CLASSES = [
    TushareStockMoneyflowHsgtTask,
    TushareStockHsgtTop10Task,
    TushareStockHkHoldTask,
]


def _make_task(task_cls, **kwargs):
    return task_cls(
        db_connection=_FakeDb(kwargs.pop("latest_date", None)),
        api_token="test-token",
        api=object(),
        **kwargs,
    )


async def _run_fetch_date_resolution(task):
    captured = {}

    async def fake_get_batch_list(**kwargs):
        captured.update(kwargs)
        return [{"trade_date": kwargs["start_date"]}]

    async def fake_prepare_params(batch):
        return batch

    async def fake_fetch_batch(params, stop_event=None):
        return pd.DataFrame({"trade_date": [params["trade_date"]]})

    task.get_batch_list = fake_get_batch_list
    task.prepare_params = fake_prepare_params
    task.fetch_batch = fake_fetch_batch

    data = await task._fetch_data()
    return captured, data


def test_p0_tasks_are_registered():
    for task_cls in P0_TASK_CLASSES:
        assert task_cls.name in UnifiedTaskFactory._task_registry


@pytest.mark.asyncio
@pytest.mark.parametrize("task_cls", P0_TASK_CLASSES)
async def test_p0_tasks_support_gui_manual_mode(task_cls):
    task = _make_task(
        task_cls,
        update_type=UpdateTypes.MANUAL,
        start_date="20240501",
        end_date="20240506",
    )

    captured, data = await _run_fetch_date_resolution(task)

    assert captured["update_type"] == UpdateTypes.MANUAL
    assert captured["start_date"] == "20240501"
    assert captured["end_date"] == "20240506"
    assert data.iloc[0]["trade_date"] == "20240501"


@pytest.mark.asyncio
@pytest.mark.parametrize("task_cls", P0_TASK_CLASSES)
async def test_p0_tasks_support_gui_full_mode(task_cls):
    task = _make_task(task_cls, update_type=UpdateTypes.FULL)

    captured, _ = await _run_fetch_date_resolution(task)

    assert captured["update_type"] == UpdateTypes.FULL
    assert captured["start_date"] == task_cls.default_start_date
    assert captured["end_date"] == datetime.now().strftime("%Y%m%d")


@pytest.mark.asyncio
@pytest.mark.parametrize("task_cls", P0_TASK_CLASSES)
async def test_p0_tasks_support_gui_smart_mode(task_cls):
    task = _make_task(
        task_cls,
        update_type=UpdateTypes.SMART,
        latest_date=date(2024, 5, 6),
    )

    captured, _ = await _run_fetch_date_resolution(task)

    assert captured["update_type"] == UpdateTypes.SMART
    assert captured["start_date"] == "20240504"
    assert captured["end_date"] == datetime.now().strftime("%Y%m%d")


@pytest.mark.asyncio
async def test_moneyflow_hsgt_generates_date_range_batches(monkeypatch):
    from alphahome.fetchers.tasks.stock import tushare_stock_moneyflow_hsgt as module

    captured = {}

    async def fake_generate_trade_day_batches(**kwargs):
        captured.update(kwargs)
        return [{"start_date": kwargs["start_date"], "end_date": kwargs["end_date"]}]

    monkeypatch.setattr(module, "generate_trade_day_batches", fake_generate_trade_day_batches)
    task = _make_task(TushareStockMoneyflowHsgtTask)

    batches = await task.get_batch_list(start_date="20240506", end_date="20240506")

    assert batches == [{"start_date": "20240506", "end_date": "20240506"}]
    assert captured["batch_size"] == task.batch_trade_days
    assert captured["start_date"] == "20240506"
    assert captured["end_date"] == "20240506"


@pytest.mark.asyncio
async def test_hsgt_top10_preserves_optional_api_filters(monkeypatch):
    from alphahome.fetchers.tasks.stock import tushare_stock_hsgt_top10 as module

    captured = {}

    async def fake_generate_trade_day_batches(**kwargs):
        captured.update(kwargs)
        return [
            {
                "start_date": kwargs["start_date"],
                "end_date": kwargs["end_date"],
                **kwargs["additional_params"],
            }
        ]

    monkeypatch.setattr(module, "generate_trade_day_batches", fake_generate_trade_day_batches)
    task = _make_task(TushareStockHsgtTop10Task)

    batches = await task.get_batch_list(
        start_date="20240506",
        end_date="20240506",
        market_type="1",
        ts_code="600519.SH",
    )

    assert batches == [
        {
            "start_date": "20240506",
            "end_date": "20240506",
            "market_type": "1",
            "ts_code": "600519.SH",
        }
    ]
    assert captured["batch_size"] == task.batch_trade_days
    assert captured["additional_params"] == {"market_type": "1", "ts_code": "600519.SH"}


@pytest.mark.asyncio
async def test_hk_hold_preserves_optional_api_filters(monkeypatch):
    from alphahome.fetchers.tasks.stock import tushare_stock_hk_hold as module

    captured = {}

    async def fake_generate_single_date_batches(**kwargs):
        captured.update(kwargs)
        return [{"trade_date": kwargs["start_date"], **kwargs["additional_params"]}]

    monkeypatch.setattr(module, "generate_single_date_batches", fake_generate_single_date_batches)
    task = _make_task(TushareStockHkHoldTask)

    batches = await task.get_batch_list(
        start_date="20240506",
        end_date="20240506",
        ts_code="600519.SH",
        hold_exchange="SH",
    )

    assert batches == [{"trade_date": "20240506", "ts_code": "600519.SH", "exchange": "SH"}]
    assert captured["date_field"] == "trade_date"
    assert captured["additional_params"] == {"ts_code": "600519.SH", "exchange": "SH"}
