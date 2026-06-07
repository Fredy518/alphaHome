import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.tasks import discover_tasks
from alphahome.fetchers.tasks.cbond.tinysoft_cbond_price_chg import TinySoftCBondPriceChgTask
from alphahome.fetchers.tasks.cbond.tushare_cbond_price_chg import TushareCBondPriceChgTask


class _DummyApi:
    async def call_dataframe_for_stocks(self, *args, **kwargs):
        return pd.DataFrame()

    async def call_dataframe(self, *args, **kwargs):
        return pd.DataFrame()

    async def call_dataframe_table(self, *args, **kwargs):
        return pd.DataFrame()


class _TransportFailApi:
    def __init__(self):
        self.batch_calls = 0
        self.single_calls = 0

    async def call_dataframe_for_stocks(self, *args, **kwargs):
        self.batch_calls += 1
        raise OSError("Cannot connect to host opi.tinysoft.com.cn:443 ssl:default [信号灯超时时间已到]")

    async def call_dataframe(self, *args, **kwargs):
        self.single_calls += 1
        raise AssertionError("transport batch failure should not fall back to single-code calls")


class _CodeDB:
    def __init__(self, rows=None, columns=None):
        self.rows = rows or []
        self.columns = columns or ["ts_code"]
        self.queries = []

    async def get_column_names(self, target):
        return self.columns

    async def fetch(self, query, *args, **kwargs):
        self.queries.append(query)
        return self.rows


class _MultiTableDB:
    def __init__(self, table_rows=None, table_columns=None):
        self.table_rows = table_rows or {}
        self.table_columns = table_columns or {}
        self.queries = []

    async def get_column_names(self, target):
        return self.table_columns.get(target, [])

    async def fetch(self, query, *args, **kwargs):
        self.queries.append(query)
        for table, rows in self.table_rows.items():
            schema, table_name = table.split(".", 1)
            if f'"{schema}"."{table_name}"' in query:
                return rows
        return []


class _RawdataViewDB(_CodeDB):
    def __init__(self):
        super().__init__()
        self.schemas = []
        self.executed = []

    async def ensure_schema_exists(self, schema):
        self.schemas.append(schema)

    async def execute(self, query, *args, **kwargs):
        self.executed.append(query)


def _make_task(db=None, api=None, **kwargs):
    return TinySoftCBondPriceChgTask(
        db_connection=db or _CodeDB(),
        tinysoft_config={"mode": "opi", "user": "dummy", "password": "dummy", "opi_url": "http://127.0.0.1"},
        api=api or _DummyApi(),
        **kwargs,
    )


def test_tinysoft_cbond_price_chg_query_fields_are_projected():
    task = _make_task()

    fields = task._resolve_query_fields({})

    assert fields == [
        "StockID",
        "生效日",
        "执行日",
        "公布日",
        "转股价",
        "价格变动类型",
        "未来不修正开始日",
        "未来不修正截止日",
    ]


def test_tinysoft_cbond_price_chg_defaults_to_non_streaming():
    task = _make_task(update_type=UpdateTypes.FULL)

    assert task._should_stream_batches({}) is False


@pytest.mark.asyncio
async def test_tinysoft_cbond_price_chg_batches_load_cbond_codes():
    db = _CodeDB(
        rows=[
            {"ts_code": "113556.SH"},
            {"ts_code": "128114.SZ"},
            {"ts_code": "128110.SZ"},
        ],
        columns=["ts_code"],
    )
    task = _make_task(db=db, update_type=UpdateTypes.FULL, task_config={"code_batch_size": 2})

    batches = await task.get_batch_list(start_date="19900101", end_date="20260522", update_type=UpdateTypes.FULL)

    assert [batch["codes"] for batch in batches] == [["SH113556", "SZ128114"], ["SZ128110"]]
    assert {batch["infoarray_table_id"] for batch in batches} == {504}


@pytest.mark.asyncio
async def test_tinysoft_cbond_price_chg_prefers_cbond_sources_over_all_bonds():
    db = _MultiTableDB(
        table_columns={
            "tushare.cbond_basic": ["ts_code"],
            "tinysoft.bond_basic_ext": ["bond_code_raw", "bond_type"],
        },
        table_rows={
            "tushare.cbond_basic": [{"ts_code": "113556.SH"}],
            "tinysoft.bond_basic_ext": [{"bond_code_raw": "BK112010540", "bond_type": "金融债"}],
        },
    )
    task = _make_task(db=db, update_type=UpdateTypes.FULL, task_config={"code_batch_size": 500})

    batches = await task.get_batch_list(start_date="19900101", end_date="20260522", update_type=UpdateTypes.FULL)

    assert [batch["codes"] for batch in batches] == [["SH113556"]]
    assert not any('"tinysoft"."bond_basic_ext"' in query for query in db.queries)


@pytest.mark.asyncio
async def test_tinysoft_cbond_price_chg_bond_basic_fallback_is_filtered():
    db = _MultiTableDB(
        table_columns={
            "tushare.cbond_basic": ["ts_code"],
            "rawdata.cbond_basic": ["ts_code"],
            "tushare.cbond_price_chg": ["ts_code"],
            "rawdata.cbond_price_chg": ["ts_code"],
            "tinysoft.bond_basic_ext": ["bond_code_raw", "bond_type", "bond_short_name"],
        },
        table_rows={
            "tinysoft.bond_basic_ext": [{"bond_code_raw": "SH113556", "bond_type": "可转债", "bond_short_name": "至纯转债"}],
        },
    )
    task = _make_task(db=db, update_type=UpdateTypes.FULL)

    codes = await task._load_codes_from_db()

    assert codes == ["SH113556"]
    assert any('"tinysoft"."bond_basic_ext"' in query and "ILIKE" in query for query in db.queries)


@pytest.mark.asyncio
async def test_tinysoft_cbond_price_chg_transport_error_does_not_select_all_or_single_fallback():
    api = _TransportFailApi()
    task = _make_task(api=api)

    with pytest.raises(OSError):
        await task.fetch_batch(
            {
                "codes": ["SH113556", "SZ128114"],
                "infoarray_table_id": 504,
                "timeout_ms": 1000,
            }
        )

    assert api.batch_calls == 1
    assert api.single_calls == 0


def test_tinysoft_cbond_price_chg_processes_tinysoft_504_rows():
    task = _make_task()
    task._effective_start_date = "19900101"
    task._effective_end_date = "20260522"
    task._bond_name_map = {"113556.SH": "至纯转债"}
    raw = pd.DataFrame(
        [
            {
                "StockID": "SH113556",
                "生效日": 20191217,
                "执行日": 20200626,
                "公布日": 20191217,
                "转股价": 29.47,
                "价格变动类型": "初始转股价",
                "未来不修正开始日": 0,
                "未来不修正截止日": 0,
            },
            {
                "StockID": "SH113556",
                "生效日": 20200629,
                "执行日": 20200629,
                "公布日": 20200619,
                "转股价": 29.38,
                "价格变动类型": "调整转股价",
                "未来不修正开始日": 0,
                "未来不修正截止日": 0,
            },
        ]
    )

    processed = task.process_data(raw)

    assert list(processed["ts_code"]) == ["113556.SH", "113556.SH"]
    assert list(processed["bond_short_name"]) == ["至纯转债", "至纯转债"]
    assert list(processed["convert_price_initial"]) == [29.47, 29.47]
    assert pd.isna(processed.iloc[0]["convertprice_bef"])
    assert processed.iloc[1]["convertprice_bef"] == 29.47
    assert processed.iloc[1]["convertprice_aft"] == 29.38
    assert processed.iloc[1]["change_type"] == "调整转股价"


def test_tinysoft_cbond_price_chg_smart_filter_keeps_derived_history_context():
    task = _make_task()
    task._effective_start_date = "20200601"
    task._effective_end_date = "20260522"
    raw = pd.DataFrame(
        [
            {"StockID": "SH113556", "生效日": 20191217, "执行日": 20200626, "公布日": 20191217, "转股价": 29.47, "价格变动类型": "初始转股价"},
            {"StockID": "SH113556", "生效日": 20200629, "执行日": 20200629, "公布日": 20200619, "转股价": 29.38, "价格变动类型": "调整转股价"},
        ]
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    assert processed.iloc[0]["change_date"].strftime("%Y%m%d") == "20200629"
    assert processed.iloc[0]["convert_price_initial"] == 29.47
    assert processed.iloc[0]["convertprice_bef"] == 29.47


def test_tushare_cbond_price_chg_is_archived_and_not_registered():
    UnifiedTaskFactory._task_registry.pop("tushare_cbond_price_chg", None)
    discover_tasks(force_reload=True)

    assert TushareCBondPriceChgTask.archived is True
    assert TushareCBondPriceChgTask.archived_replacement == "tinysoft_cbond_price_chg"
    assert "tushare_cbond_price_chg" not in UnifiedTaskFactory._task_registry
    assert UnifiedTaskFactory._task_registry["tinysoft_cbond_price_chg"] is TinySoftCBondPriceChgTask


@pytest.mark.asyncio
async def test_tinysoft_cbond_price_chg_forces_rawdata_view_to_tinysoft():
    db = _RawdataViewDB()
    task = _make_task(db=db)

    await task._create_rawdata_view_if_needed()

    assert db.schemas == ["rawdata"]
    create_sql = db.executed[0]
    assert 'CREATE OR REPLACE VIEW rawdata."cbond_price_chg"' in create_sql
    assert 'FROM "tinysoft"."cbond_price_chg"' in create_sql
    assert create_sql.index('"publish_date"') < create_sql.index('"source_code"')
    assert create_sql.index('"convertprice_aft"') < create_sql.index('"update_time"')
    assert "compatible_prefix=tushare.cbond_price_chg" in db.executed[1]
