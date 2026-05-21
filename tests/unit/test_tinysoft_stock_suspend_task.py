import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
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
    def __init__(self):
        self.last_where_clause = "NOT_CALLED"

    async def call_dataframe(self, func, table_id, **kwargs):
        stock = kwargs.get("stock")
        self.last_where_clause = kwargs.get("where_clause")
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


class _BatchInfoTableApi:
    def __init__(self):
        self.batch_calls = 0
        self.single_calls = 0
        self.last_where_clause = "NOT_CALLED"
        self.last_stocks = None

    async def call_dataframe_for_stocks(self, func, table_id, **kwargs):
        stock_list = list(kwargs.get("stocks"))
        self.batch_calls += 1
        self.last_stocks = stock_list
        self.last_where_clause = kwargs.get("where_clause")
        assert func == "infoarray"
        assert table_id == 127
        return pd.DataFrame(
            {
                "停牌开始日": [20260302 for _ in stock_list],
                "停牌开始时间": ["09:30:00" for _ in stock_list],
                "停牌截止日": [20260302 for _ in stock_list],
                "停牌截止时间": ["10:30:00" for _ in stock_list],
                "停牌期限": ["1小时" for _ in stock_list],
                "停牌原因": ["临时停牌" for _ in stock_list],
                "StockID": stock_list,
            }
        )

    async def call_dataframe(self, *args, **kwargs):
        self.single_calls += 1
        return pd.DataFrame()


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
async def test_smart_mode_uses_larger_symbol_batches_without_changing_full_batches():
    rows = [{"ts_code": f"{i:06d}.SZ"} for i in range(1, 451)]
    task = _make_task(
        db=_FakeDB(rows=rows),
        task_config={"symbol_batch_size": 50, "smart_symbol_batch_size": 200},
    )

    full_batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.FULL,
    )
    smart_batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )

    assert [len(batch["symbol_pairs"]) for batch in full_batches] == [50] * 9
    assert [len(batch["symbol_pairs"]) for batch in smart_batches] == [200, 200, 50]


@pytest.mark.asyncio
async def test_get_batch_list_defaults_start_date_when_missing():
    task = _make_task(
        db=_FakeDB(rows=[{"ts_code": "000001.SZ"}]),
        task_config={"symbol_batch_size": 1},
    )
    batches = await task.get_batch_list()
    assert len(batches) == 1
    assert batches[0]["start_date"] == task.default_start_date


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


@pytest.mark.asyncio
async def test_fetch_batch_prefers_batch_infotable_for_symbol_pairs():
    api = _BatchInfoTableApi()
    task = _make_task(db=_FakeDB(), api=api, task_config={"skip_failed_symbols": False})
    df = await task.fetch_batch(
        {
            "symbol_pairs": [
                {"ts_code": "000001.SZ", "stock": "SZ000001"},
                {"ts_code": "600000.SH", "stock": "SH600000"},
            ],
            "infoarray_table_id": 127,
            "start_date": "20260301",
            "service": "",
            "timeout_ms": 45000,
        }
    )
    assert df is not None
    assert api.batch_calls == 1
    assert api.single_calls == 0
    assert api.last_stocks == ["SZ000001", "SH600000"]
    assert api.last_where_clause == '["停牌开始日"]>=20260301'
    assert set(df["StockID"]) == {"SZ000001", "SH600000"}


# ---------- WHERE clause tests ----------


def test_build_where_clause_with_valid_date():
    clause = TinySoftStockSuspendTask._build_where_clause("20260301")
    assert clause == '["停牌开始日"]>=20260301'


def test_build_where_clause_with_none():
    assert TinySoftStockSuspendTask._build_where_clause(None) is None


def test_build_where_clause_with_empty_string():
    assert TinySoftStockSuspendTask._build_where_clause("") is None


@pytest.mark.asyncio
async def test_fetch_batch_passes_where_clause_when_start_date_present():
    """SMART/MANUAL: start_date -> WHERE 条件应传到 call_dataframe。"""
    api = _MultiSymbolApi()
    task = _make_task(db=_FakeDB(), api=api, task_config={"skip_failed_symbols": False})
    await task.fetch_batch(
        {
            "symbol_pairs": [{"ts_code": "000001.SZ", "stock": "SZ000001"}],
            "infoarray_table_id": 127,
            "start_date": "20260301",
            "service": "",
            "timeout_ms": 45000,
        }
    )
    assert api.last_where_clause == '["停牌开始日"]>=20260301'


@pytest.mark.asyncio
async def test_fetch_batch_no_where_clause_when_no_start_date():
    """低层 fetch_batch 未给 start_date 时不传 where_clause。"""
    api = _MultiSymbolApi()
    task = _make_task(db=_FakeDB(), api=api, task_config={"skip_failed_symbols": False})
    await task.fetch_batch(
        {
            "symbol_pairs": [{"ts_code": "000001.SZ", "stock": "SZ000001"}],
            "infoarray_table_id": 127,
            "service": "",
            "timeout_ms": 45000,
        }
    )
    assert api.last_where_clause is None


def test_process_data_uses_effective_window_when_kwargs_missing():
    task = _make_task()
    task._effective_start_date = "20260301"
    task._effective_end_date = "20260331"
    raw = pd.DataFrame(
        {
            "停牌开始日": [20260228, 20260302],
            "停牌开始时间": ["09:30:00", "09:30:00"],
            "停牌截止日": [20260228, 20260302],
            "停牌截止时间": ["10:30:00", "10:30:00"],
            "停牌期限": ["1小时", "1小时"],
            "停牌原因": ["临时停牌", "临时停牌"],
            "StockID": ["SZ000001", "SZ000001"],
        }
    )
    processed = task.process_data(raw)
    assert len(processed) == 1
    assert str(processed.iloc[0]["trade_date"]) == "2026-03-02"


def test_suspend_smart_lookback_covers_late_corrections():
    assert TinySoftStockSuspendTask.smart_lookback_days >= 90
