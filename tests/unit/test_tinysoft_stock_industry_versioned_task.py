import pandas as pd
import pytest

from alphahome.fetchers.tasks.stock.tinysoft_stock_industry_versioned import (
    TinySoftStockIndustryVersionedTask,
)


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
        assert table_id == 139
        return pd.DataFrame(
            {
                "证券代码": [stock, stock, stock],
                "属性代码": ["SWHY440000", "SWHY440100", "SWHY440101"],
                "属性名称": ["申万金融服务", "申万银行", "申万股份制银行"],
                "级数": [1, 2, 3],
                "入选日期": [20260301, 20260301, 20260301],
                "剔除日期": [0, 0, 0],
                "最新标识": [1, 1, 1],
                "所属属性代码": ["SWHY", "SWHY", "SWHY"],
                "所属属性名称": ["申万行业", "申万行业", "申万行业"],
            }
        )


def _make_task(task_config=None, db=None, api=None):
    return TinySoftStockIndustryVersionedTask(
        db_connection=db if db is not None else object(),
        api=api or _DummyApi(),
        tinysoft_config={},
        task_config=task_config or {},
    )


def test_process_data_maps_industry_fields():
    task = _make_task()
    raw = pd.DataFrame(
        {
            "证券代码": ["SZ000001", "SZ000001", "SZ000001"],
            "属性代码": ["SWHY440000", "SWHY440100", "SWHY440101"],
            "属性名称": ["申万金融服务", "申万银行", "申万股份制银行"],
            "级数": [1, 2, 3],
            "入选日期": [20260301, 20260301, 20260301],
            "剔除日期": [0, 0, 0],
            "最新标识": [1, 1, 1],
            "所属属性代码": ["SWHY", "SWHY", "SWHY"],
            "所属属性名称": ["申万行业", "申万行业", "申万行业"],
        }
    )

    processed = task.process_data(raw, start_date="20260301", end_date="20260302")
    assert not processed.empty
    row = processed.iloc[0]
    assert row["ts_code"] == "000001.SZ"
    assert row["industry_source"] == "SWHY"
    assert row["industry_l1"] == "申万金融服务"
    assert row["industry_l2"] == "申万银行"
    assert row["industry_l3"] == "申万股份制银行"
    assert row["industry_code"] == "SWHY440101"


def test_process_data_filters_empty_records_by_default():
    task = _make_task(task_config={"include_empty_records": False})
    raw = pd.DataFrame(
        {
            "证券代码": ["SZ000001"],
            "属性代码": [None],
            "属性名称": [None],
            "级数": [1],
            "入选日期": [20260301],
            "剔除日期": [0],
            "最新标识": [1],
            "所属属性代码": ["SWHY"],
            "所属属性名称": ["申万行业"],
        }
    )
    processed = task.process_data(raw, start_date="20260301", end_date="20260302")
    assert processed.empty


@pytest.mark.asyncio
async def test_get_batch_list_with_table_id_and_source_codes():
    task = _make_task(
        db=_FakeDB(rows=[{"ts_code": "000001.SZ"}, {"ts_code": "600000.SH"}]),
        task_config={"symbol_batch_size": 2},
    )
    batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260302",
        source_codes=["SWHY"],
    )
    assert len(batches) == 1
    assert batches[0]["infoarray_table_id"] == 139
    assert batches[0]["source_codes"] == ["SWHY"]
    assert len(batches[0]["symbol_pairs"]) == 2


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
            "infoarray_table_id": 139,
            "service": "",
            "timeout_ms": 45000,
        }
    )
    assert df is not None
    assert len(df) == 6
    assert set(df["证券代码"]) == {"SZ000001", "SH600000"}


# ---------- WHERE clause tests ----------


@pytest.mark.asyncio
async def test_fetch_batch_never_passes_where_clause():
    """行业分类任务不做服务端过滤，where_clause 始终为 None。"""
    api = _MultiSymbolApi()
    task = _make_task(db=_FakeDB(), api=api, task_config={"skip_failed_symbols": False})
    await task.fetch_batch(
        {
            "symbol_pairs": [{"ts_code": "000001.SZ", "stock": "SZ000001"}],
            "infoarray_table_id": 139,
            "start_date": "20260301",
            "end_date": "20260302",
            "service": "",
            "timeout_ms": 45000,
        }
    )
    assert api.last_where_clause is None, (
        "行业分类任务不应传递 where_clause（需要完整上下文重建快照）"
    )


def test_process_data_uses_effective_window_when_kwargs_missing():
    task = _make_task()
    task._effective_start_date = "20260302"
    task._effective_end_date = "20260302"
    raw = pd.DataFrame(
        {
            "证券代码": ["SZ000001", "SZ000001", "SZ000001", "SZ000001", "SZ000001", "SZ000001"],
            "属性代码": ["SWHY440000", "SWHY440100", "SWHY440101", "SWHY440000", "SWHY440100", "SWHY440101"],
            "属性名称": ["申万金融服务", "申万银行", "申万股份制银行", "申万金融服务", "申万银行", "申万股份制银行"],
            "级数": [1, 2, 3, 1, 2, 3],
            "入选日期": [20260301, 20260301, 20260301, 20260302, 20260302, 20260302],
            "剔除日期": [20260301, 20260301, 20260301, 0, 0, 0],
            "最新标识": [1, 1, 1, 1, 1, 1],
            "所属属性代码": ["SWHY", "SWHY", "SWHY", "SWHY", "SWHY", "SWHY"],
            "所属属性名称": ["申万行业", "申万行业", "申万行业", "申万行业", "申万行业", "申万行业"],
        }
    )
    processed = task.process_data(raw)
    assert len(processed) == 1
    assert str(processed.iloc[0]["trade_date"]) == "2026-03-02"
