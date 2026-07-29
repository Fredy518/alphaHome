import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.tasks import discover_tasks
from alphahome.fetchers.tasks.stock.tinysoft_stock_margin import (
    TinySoftStockMarginDetailTask,
    TinySoftStockMarginTask,
)


class _DummyApi:
    async def call_dataframe_for_stocks(self, *args, **kwargs):
        return pd.DataFrame()

    async def call_dataframe(self, *args, **kwargs):
        return pd.DataFrame()


class _RecordLike:
    def __init__(self, **values):
        self._values = values

    def __getitem__(self, key):
        return self._values[key]


class _CodeDB:
    def __init__(self):
        self.queries = []

    async def get_column_names(self, target):
        return ["ts_code", "list_status"]

    async def fetch(self, query, *args, **kwargs):
        self.queries.append(query)
        if '"fund_basic"' in query:
            return [_RecordLike(ts_code="510300.SH")]
        if '"fund_etf_basic"' in query:
            return [_RecordLike(ts_code="159901.SZ")]
        if '"stock_margindetail"' in query:
            return [_RecordLike(ts_code="000022.SZ")]
        return [_RecordLike(ts_code="000001.SZ"), _RecordLike(ts_code="600000.SH")]


def _make_task(task_cls, *, db=None):
    return task_cls(
        db_connection=db if db is not None else object(),
        api=_DummyApi(),
        tinysoft_config={},
        task_config={},
    )


def test_margin_summary_processes_exchange_and_tushare_compatible_fields():
    task = _make_task(TinySoftStockMarginTask)
    raw = pd.DataFrame(
        {
            "StockID": ["RZRQ000001", "RZRQ000002", "RZRQ000003"],
            "截止日": [20260728, 20260728, 20260728],
            "融资买入额": [1, 2, 3],
            "融资偿还额": [4, 5, 6],
            "融资余额": [7, 8, 9],
            "融券卖出量": [10, 11, 12],
            "融券偿还量": [13, 14, 15],
            "融券余量": [16, 17, 18],
            "融券余额": [19, 20, 21],
            "融资融券余额": [26, 28, 30],
        }
    )

    processed = task.process_data(raw)

    assert processed["exchange_id"].tolist() == ["SSE", "SZSE", "BSE"]
    assert processed["trade_date"].astype(str).tolist() == ["2026-07-28"] * 3
    assert processed["rzmre"].tolist() == [1.0, 2.0, 3.0]
    assert processed["rqchl"].tolist() == [13.0, 14.0, 15.0]
    assert "raw_json" not in processed.columns


@pytest.mark.asyncio
async def test_margin_summary_uses_all_three_exchange_codes_in_one_batch():
    task = _make_task(TinySoftStockMarginTask)

    batches = await task.get_batch_list(
        start_date="20100331",
        end_date="20260728",
        update_type=UpdateTypes.FULL,
    )

    assert len(batches) == 1
    assert batches[0]["codes"] == ["RZRQ000001", "RZRQ000002", "RZRQ000003"]
    assert batches[0]["infoarray_table_id"] == 165


def test_margin_detail_processes_stock_code_and_tushare_compatible_fields():
    task = _make_task(TinySoftStockMarginDetailTask)
    raw = pd.DataFrame(
        {
            "StockID": ["SH600000"],
            "截止日": [20260728],
            "融资买入额": ["100.25"],
            "融资偿还额": ["80.25"],
            "融资余额": ["1000.00"],
            "融券卖出量": ["50"],
            "融券偿还量": ["20"],
            "融券余量": ["100"],
            "融券余额": ["500.00"],
            "融资融券余额": ["1500.00"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    assert processed.loc[0, "ts_code"] == "600000.SH"
    assert processed.loc[0, "tsl_code"] == "SH600000"
    assert processed.loc[0, "rzrqye"] == pytest.approx(1500.0)
    assert processed.loc[0, "rqchl"] == pytest.approx(20.0)


@pytest.mark.asyncio
async def test_margin_detail_loads_listed_and_delisted_stock_codes():
    db = _CodeDB()
    task = _make_task(TinySoftStockMarginDetailTask, db=db)

    batches = await task.get_batch_list(
        start_date="20100331",
        end_date="20260728",
        update_type=UpdateTypes.FULL,
    )

    assert [code for batch in batches for code in batch["codes"]] == [
        "SZ000001",
        "SH600000",
        "SH510300",
        "SZ159901",
        "SZ000022",
    ]
    assert all("list_status = 'L'" not in query for query in db.queries)


def test_margin_tasks_are_discoverable_without_replacing_tushare_tasks():
    discover_tasks(force_reload=True)

    assert UnifiedTaskFactory._task_registry["tinysoft_stock_margin"] is TinySoftStockMarginTask
    assert (
        UnifiedTaskFactory._task_registry["tinysoft_stock_margindetail"]
        is TinySoftStockMarginDetailTask
    )
    assert "tushare_stock_margin" in UnifiedTaskFactory._task_registry
    assert "tushare_stock_margindetail" in UnifiedTaskFactory._task_registry
