import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.sources.tinysoft import TinySoftTask
from alphahome.fetchers.tasks.tinysoft_p0_base import TinySoftP0InfoArrayTask
from alphahome.fetchers.tasks.fund.tinysoft_fund_p0_ext import (
    TinySoftFundAssetAllocTask,
    TinySoftFundBasicExtTask,
    TinySoftFundBondHoldingDetailTask,
    TinySoftFundBrokerSeatTask,
    TinySoftFundCbondHoldingDetailTask,
    TinySoftFundClassificationInfoTask,
    TinySoftFundClassificationMemberTask,
    TinySoftFundFinancialQuarterlyExtTask,
    TinySoftFundFofHoldingDetailTask,
    TinySoftFundHolderStructureTask,
    TinySoftFundIndustryAllocTask,
    TinySoftFundManagerExtTask,
    TinySoftFundStockHoldingDetailTask,
    TinySoftFundStockTradeSummaryTask,
)
from alphahome.fetchers.tasks.index.tinysoft_index_p0_cross_domain import (
    TinySoftIndexBasicExtTask,
    TinySoftIndexMemberVersionedTask,
    TinySoftMarketCalendarMultiTask,
)
from alphahome.fetchers.tasks.stock.tinysoft_stock_p0_cross_domain import (
    TinySoftStockBasicExtTask,
    TinySoftStockHsgtHoldTask,
    TinySoftStockHsgtShortBalanceTask,
    TinySoftStockHsgtTop10Task,
    TinySoftStockLendingBalanceTask,
    TinySoftStockPledgeBalanceTask,
    TinySoftStockPledgeRateTask,
    TinySoftStockPublicTradeInfoTask,
    TinySoftStockRepurchaseExtTask,
    TinySoftStockUnlockScheduleTask,
)
from alphahome.fetchers.tasks.cbond.tinysoft_bond_basic_ext import TinySoftBondBasicExtTask
from alphahome.fetchers.tasks.future.tinysoft_future_basic_ext import (
    TinySoftFutureBasicExtTask,
    TinySoftFutureProductMappingExtTask,
)
from alphahome.fetchers.tasks.option.tinysoft_option_basic_ext import TinySoftOptionBasicDailyExtTask


class _DummyApi:
    async def call_dataframe_for_stocks(self, *args, **kwargs):
        return pd.DataFrame()

    async def call_dataframe(self, *args, **kwargs):
        return pd.DataFrame()

    async def call_dataframe_table(self, *args, **kwargs):
        return pd.DataFrame()


class _RecordLike:
    def __init__(self, **values):
        self._values = values

    def __getitem__(self, key):
        return self._values[key]


class _CodeDB:
    def __init__(self, rows=None, columns=None):
        self._rows = rows or []
        self._columns = columns or ["ts_code", "list_status"]

    async def get_column_names(self, target):
        return self._columns

    async def fetch(self, query, *args, **kwargs):
        return self._rows


class _BatchApi:
    def __init__(self):
        self.batch_calls = 0
        self.single_calls = 0
        self.table_calls = 0
        self.last_stocks = None
        self.last_where_clause = None
        self.last_fields = None

    async def call_dataframe_for_stocks(self, func, table_id, **kwargs):
        stocks = list(kwargs.get("stocks") or [])
        self.batch_calls += 1
        self.last_stocks = stocks
        self.last_where_clause = kwargs.get("where_clause")
        self.last_fields = kwargs.get("fields")
        if table_id == 625:
            return pd.DataFrame(
                {
                    "StockID": stocks,
                    "StockName": ["测试基金" for _ in stocks],
                    "基金经理代码": ["M001" for _ in stocks],
                    "基金经理": ["张三" for _ in stocks],
                    "开始日": [20260101 for _ in stocks],
                    "截止日": [0 for _ in stocks],
                    "实际开始日": [20260101 for _ in stocks],
                    "实际截止日": [20260331 for _ in stocks],
                    "同类别基金": ["TSJJ0209" for _ in stocks],
                }
            )
        return pd.DataFrame(
            {
                "StockID": stocks,
                "截止日": [20260301 for _ in stocks],
                "余量": [100 for _ in stocks],
                "余额": [200 for _ in stocks],
            }
        )

    async def call_dataframe(self, func, table_id, **kwargs):
        self.single_calls += 1
        self.last_where_clause = kwargs.get("where_clause")
        self.last_fields = kwargs.get("fields")
        if table_id == 625:
            stock = kwargs.get("stock")
            return pd.DataFrame(
                {
                    "StockID": [stock],
                    "StockName": ["测试基金"],
                    "基金经理代码": ["M001"],
                    "基金经理": ["张三"],
                    "开始日": [20260101],
                    "截止日": [0],
                    "实际开始日": [20260101],
                    "实际截止日": [20260331],
                    "同类别基金": ["TSJJ0209"],
                }
            )
        return pd.DataFrame()

    async def call_dataframe_table(self, func, table_id, **kwargs):
        self.table_calls += 1
        self.last_where_clause = kwargs.get("where_clause")
        self.last_fields = kwargs.get("fields")
        return pd.DataFrame(
            {
                "基金经理代码": ["M001"],
                "基金经理": ["张三"],
                "开始日": [20260101],
                "截止日": [20260331],
                "同类别基金": ["TSJJ0209"],
            }
        )


class _ProjectionFailBatchApi(_BatchApi):
    async def call_dataframe_for_stocks(self, func, table_id, **kwargs):
        if kwargs.get("fields") is not None:
            self.batch_calls += 1
            self.last_fields = kwargs.get("fields")
            raise RuntimeError("unknown projected field")
        return await super().call_dataframe_for_stocks(func, table_id, **kwargs)


class _FofHoldingApi:
    def __init__(self):
        self.calls = []

    async def call_dataframe_for_stocks(self, func, table_id, **kwargs):
        stocks = list(kwargs.get("stocks") or [])
        self.calls.append({"table_id": table_id, "stocks": stocks, "where_clause": kwargs.get("where_clause")})
        if table_id == 302:
            rows = []
            for stock in stocks:
                rows.append(
                    {
                        "StockID": stock,
                        "基金名称": f"{stock}基金中基金(FOF)",
                        "投资风格": "FOF",
                        "不同收费模式基金主代码": "OF006872" if stock in {"OF006872", "OF006873"} else stock,
                        "母基金代码": "",
                    }
                )
            return pd.DataFrame(rows)
        if table_id == 349:
            rows = []
            for stock in stocks:
                rows.append(
                    {
                        "StockID": stock,
                        "StockName": f"{stock}主基金",
                        "截止日": 20260331,
                        "名称": "测试持有基金",
                        "代码": "OF485019",
                        "数量": "1000.5",
                        "市值": "123456.78",
                        "占净值比例(%)": "2.34",
                        "市值排名": "1",
                        "是否属于关联基金": "是",
                    }
                )
            return pd.DataFrame(rows)
        return pd.DataFrame()

    async def call_dataframe(self, func, table_id, **kwargs):
        return await self.call_dataframe_for_stocks(
            func,
            table_id,
            stocks=[kwargs.get("stock")],
            where_clause=kwargs.get("where_clause"),
        )


class _SelectiveFofMetadataApi(_FofHoldingApi):
    async def call_dataframe_for_stocks(self, func, table_id, **kwargs):
        stocks = list(kwargs.get("stocks") or [])
        self.calls.append({"table_id": table_id, "stocks": stocks, "where_clause": kwargs.get("where_clause")})
        if table_id == 302:
            rows = []
            for stock in stocks:
                if stock == "OF000001":
                    rows.append(
                        {
                            "StockID": stock,
                            "基金名称": "普通混合基金",
                            "投资类型": "混合型",
                            "不同收费模式基金主代码": stock,
                            "母基金代码": "",
                        }
                    )
                else:
                    rows.append(
                        {
                            "StockID": stock,
                            "基金名称": "养老目标日期基金",
                            "投资类型": "基金中基金",
                            "不同收费模式基金主代码": "OF000002",
                            "母基金代码": "",
                        }
                    )
            return pd.DataFrame(rows)
        if table_id == 349:
            return pd.DataFrame(
                {
                    "StockID": stocks,
                    "StockName": [f"{stock}主基金" for stock in stocks],
                    "截止日": [20260331 for _ in stocks],
                    "名称": ["测试持有基金" for _ in stocks],
                    "代码": ["OF485019" for _ in stocks],
                    "数量": ["1000.5" for _ in stocks],
                    "市值": ["123456.78" for _ in stocks],
                    "占净值比例(%)": ["2.34" for _ in stocks],
                    "市值排名": ["1" for _ in stocks],
                    "是否属于关联基金": ["是" for _ in stocks],
                }
            )
        return pd.DataFrame()


class _FundMainCodeReportApi(_FofHoldingApi):
    async def call_dataframe_for_stocks(self, func, table_id, **kwargs):
        stocks = list(kwargs.get("stocks") or [])
        self.calls.append({"table_id": table_id, "stocks": stocks, "where_clause": kwargs.get("where_clause")})
        if table_id == 302:
            return pd.DataFrame(
                {
                    "StockID": stocks,
                    "不同收费模式基金主代码": ["OF006872" if stock in {"OF006872", "OF006873"} else stock for stock in stocks],
                    "母基金代码": ["" for _ in stocks],
                }
            )
        if table_id == 342:
            return pd.DataFrame(
                {
                    "StockID": stocks,
                    "StockName": [f"{stock}主基金" for stock in stocks],
                    "截止日": [20260331 for _ in stocks],
                    "公布日": [0 for _ in stocks],
                    "名称": ["25国债01" for _ in stocks],
                    "代码": ["SH019766" for _ in stocks],
                    "数量": ["360400" for _ in stocks],
                    "市值": ["36442028.67" for _ in stocks],
                    "占净值比例(%)": ["1.46" for _ in stocks],
                    "市值排名": ["2" for _ in stocks],
                    "债券类型": ["" for _ in stocks],
                    "是否处于转股期": ["否" for _ in stocks],
                    "备注": ["" for _ in stocks],
                }
            )
        return pd.DataFrame()


class _SaveDB(_CodeDB):
    def __init__(self, rows=None, columns=None):
        super().__init__(rows=rows, columns=columns)
        self.saved_chunks = []

    async def table_exists(self, target):
        return True

    async def ensure_table_schema_compatible(self, target):
        return None

    async def check_table_exists(self, schema, table):
        return False

    async def create_rawdata_view(self, *args, **kwargs):
        return None

    async def upsert(self, df, target, conflict_columns, update_columns, timestamp_column=None):
        self.saved_chunks.append(df.copy())
        return len(df)


def _make_task(cls, task_config=None, db=None, api=None):
    return cls(
        db_connection=db if db is not None else object(),
        api=api or _DummyApi(),
        tinysoft_config={},
        task_config=task_config or {},
    )


@pytest.mark.asyncio
async def test_p0_resolve_codes_ignores_config_codes_when_disabled():
    task = _make_task(
        TinySoftStockLendingBalanceTask,
        db=_CodeDB(rows=[_RecordLike(ts_code="000001.SZ"), _RecordLike(ts_code="600000.SH")]),
        task_config={"ts_codes": ["000002.SZ"], "max_codes": 1},
    )

    codes = await task._resolve_codes()

    assert codes == ["SZ000001", "SH600000"]


@pytest.mark.asyncio
async def test_p0_resolve_codes_uses_config_codes_when_enabled():
    task = _make_task(
        TinySoftStockLendingBalanceTask,
        db=_CodeDB(rows=[{"ts_code": "000001.SZ"}]),
        task_config={"use_config_codes": True, "ts_codes": ["000002.SZ", "SH600000"], "max_codes": 1},
    )

    codes = await task._resolve_codes()

    assert codes == ["SZ000002"]


@pytest.mark.asyncio
async def test_p0_fetch_batch_prefers_batch_query_with_date_window():
    api = _BatchApi()
    task = _make_task(TinySoftStockLendingBalanceTask, api=api)

    df = await task.fetch_batch(
        {
            "codes": ["SZ000001", "SH600000"],
            "infoarray_table_id": 153,
            "start_date": "20260301",
            "end_date": "20260302",
            "service": "",
            "timeout_ms": 45000,
        }
    )

    assert df is not None
    assert api.batch_calls == 1
    assert api.single_calls == 0
    assert api.last_stocks == ["SZ000001", "SH600000"]
    assert api.last_where_clause == '["截止日"]>=20260301 and ["截止日"]<=20260302'


@pytest.mark.asyncio
async def test_p0_fetch_batch_projects_declared_fields_by_default():
    api = _BatchApi()
    task = _make_task(TinySoftStockLendingBalanceTask, api=api)

    await task.fetch_batch(
        {
            "codes": ["SZ000001", "SH600000"],
            "infoarray_table_id": 153,
            "start_date": "20260301",
            "end_date": "20260302",
        }
    )

    assert api.last_fields == ["StockID", "证券代码", "截止日", "余量", "余额"]


@pytest.mark.asyncio
async def test_p0_fetch_batch_can_disable_field_projection():
    api = _BatchApi()
    task = _make_task(
        TinySoftStockLendingBalanceTask,
        api=api,
        task_config={"use_field_projection": False},
    )

    await task.fetch_batch(
        {
            "codes": ["SZ000001", "SH600000"],
            "infoarray_table_id": 153,
            "start_date": "20260301",
            "end_date": "20260302",
        }
    )

    assert api.last_fields is None


@pytest.mark.asyncio
async def test_p0_fetch_batch_falls_back_to_select_all_on_projection_error():
    api = _ProjectionFailBatchApi()
    task = _make_task(TinySoftStockLendingBalanceTask, api=api)

    df = await task.fetch_batch(
        {
            "codes": ["SZ000001", "SH600000"],
            "infoarray_table_id": 153,
            "start_date": "20260301",
            "end_date": "20260302",
        }
    )

    assert df is not None
    assert api.batch_calls == 2
    assert api.last_fields is None


@pytest.mark.asyncio
async def test_p0_smart_mode_uses_larger_code_batches_without_changing_full_batches():
    rows = [_RecordLike(ts_code=f"{i:06d}.SZ") for i in range(1, 451)]
    task = _make_task(
        TinySoftStockLendingBalanceTask,
        db=_CodeDB(rows=rows),
        task_config={"code_batch_size": 50, "smart_code_batch_size": 200},
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

    assert [len(batch["codes"]) for batch in full_batches] == [50] * 9
    assert [len(batch["codes"]) for batch in smart_batches] == [200, 200, 50]


def test_stock_hsgt_top10_processes_disclosure_change_fields():
    task = _make_task(TinySoftStockHsgtTop10Task)
    raw = pd.DataFrame(
        {
            "request_code": ["HG000002"],
            "截止日": [20240820],
            "股票代码": ["600000"],
            "股票名称": ["浦发银行"],
            "排名": ["1"],
            "买入金额": [None],
            "卖出金额": [None],
            "买入及卖出金额": [1000],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["channel_code"] == "HG000002"
    assert row["ts_code"] == "600000.SH"
    assert row["rank_no"] == 1
    assert bool(row["is_disclosure_missing"]) is True
    assert "raw_json" in processed.columns


def test_stock_hsgt_hold_and_short_balance_process_codes():
    hold_task = _make_task(TinySoftStockHsgtHoldTask)
    hold_raw = pd.DataFrame(
        {
            "request_code": ["HG000004"],
            "截止日": [20240930],
            "股票代码": ["000001"],
            "股票名称": ["平安银行"],
            "股数": ["123456"],
            "占总股本比例(%)": ["1.23"],
        }
    )

    hold = hold_task.process_data(hold_raw)

    assert len(hold) == 1
    assert hold.iloc[0]["ts_code"] == "000001.SZ"
    assert hold.iloc[0]["disclosure_cycle"] == "quarterly"
    assert hold.iloc[0]["holding_volume"] == pytest.approx(123456)

    short_task = _make_task(TinySoftStockHsgtShortBalanceTask)
    short_raw = pd.DataFrame(
        {
            "request_code": ["HG000002"],
            "截止日": [20260331],
            "股票代码": ["600000"],
            "可供卖空股数余额": ["8888"],
        }
    )

    short = short_task.process_data(short_raw)

    assert len(short) == 1
    assert short.iloc[0]["ts_code"] == "600000.SH"
    assert short.iloc[0]["short_balance_volume"] == pytest.approx(8888)


def test_stock_p1_event_and_pledge_tasks_process_core_fields():
    public_task = _make_task(TinySoftStockPublicTradeInfoTask)
    public_raw = pd.DataFrame(
        {
            "StockID": ["SH600000"],
            "截止日": [20260331],
            "交易动作": ["买入"],
            "营业部简称": ["测试营业部"],
            "异动类型": ["1"],
            "买入金额": ["1000"],
        }
    )
    public = public_task.process_data(public_raw)
    assert len(public) == 1
    assert public.iloc[0]["ts_code"] == "600000.SH"
    assert public.iloc[0]["abnormal_type"] == 1

    unlock_task = _make_task(TinySoftStockUnlockScheduleTask)
    unlock = unlock_task.process_data(
        pd.DataFrame(
            {
                "StockID": ["SZ000001"],
                "解禁日": [20261231],
                "解禁数量": ["100"],
                "实际可流通数量": ["90"],
                "限售类型": ["首发原股东限售股份"],
            }
        )
    )
    assert len(unlock) == 1
    assert unlock.iloc[0]["ts_code"] == "000001.SZ"
    assert unlock.iloc[0]["actual_float_volume"] == pytest.approx(90)

    repo_task = _make_task(TinySoftStockRepurchaseExtTask)
    repo = repo_task.process_data(
        pd.DataFrame(
            {
                "StockID": ["SH600000"],
                "首次信息发布日": [20260101],
                "截止日": [20260331],
                "公布日": [20260401],
                "回购类型": ["集中竞价"],
                "累计回购数量": ["1000"],
            }
        )
    )
    assert len(repo) == 1
    assert repo.iloc[0]["repurchase_type"] == "集中竞价"

    pledge_balance_task = _make_task(TinySoftStockPledgeBalanceTask)
    pledge_balance = pledge_balance_task.process_data(
        pd.DataFrame(
            {
                "StockID": ["SH600000"],
                "代码": ["SH600000"],
                "截止日": [20260331],
                "余量": ["10000"],
                "数据来源": ["上海证券交易所"],
            }
        )
    )
    assert len(pledge_balance) == 1
    assert pledge_balance.iloc[0]["ts_code"] == "600000.SH"

    pledge_rate_task = _make_task(TinySoftStockPledgeRateTask)
    pledge_rate = pledge_rate_task.process_data(
        pd.DataFrame(
            {
                "request_code": ["SH000001"],
                "截止日": [20260331],
                "无限售条件股份质押率(%)": ["45.6"],
                "有限售条件股份质押率(%)": ["50.1"],
            }
        )
    )
    assert len(pledge_rate) == 1
    assert pledge_rate.iloc[0]["market_code"] == "SH000001"
    assert pledge_rate.iloc[0]["unrestricted_pledge_rate_pct"] == pytest.approx(45.6)


@pytest.mark.asyncio
async def test_fund_fof_holding_detail_maps_share_classes_to_main_codes():
    api = _FofHoldingApi()
    task = _make_task(
        TinySoftFundFofHoldingDetailTask,
        api=api,
        db=_CodeDB(
            rows=[
                _RecordLike(ts_code="006872.OF", name="长信颐天平衡养老(FOF)A"),
                _RecordLike(ts_code="006873.OF", name="长信颐天平衡养老(FOF)C"),
            ],
            columns=["ts_code", "name", "status"],
        ),
    )

    batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )
    df = await task.fetch_batch(batches[0])
    processed = task.process_data(df)

    assert batches[0]["codes"] == ["OF006872"]
    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["ts_code"] == "006872.OF"
    assert str(row["report_date"]) == "2026-03-31"
    assert row["holding_code_raw"] == "OF485019"
    assert row["holding_ts_code"] == "485019.OF"
    assert row["quantity"] == pytest.approx(1000.5)
    assert row["market_value"] == pytest.approx(123456.78)
    assert row["nav_ratio_pct"] == pytest.approx(2.34)
    assert row["rank_no"] == 1
    assert bool(row["is_related_fund"]) is True
    assert api.calls[-1]["table_id"] == 349
    assert api.calls[-1]["where_clause"] == '["截止日"]>=20260301 and ["截止日"]<=20260331'


@pytest.mark.asyncio
async def test_fund_fof_holding_detail_identifies_fof_from_tinysoft_302_metadata():
    api = _SelectiveFofMetadataApi()
    task = _make_task(
        TinySoftFundFofHoldingDetailTask,
        api=api,
        db=_CodeDB(
            rows=[
                _RecordLike(ts_code="000001.OF", name="普通混合基金", status="L"),
                _RecordLike(ts_code="000002.OF", name="养老目标日期基金A", status="L"),
                _RecordLike(ts_code="000003.OF", name="养老目标日期基金C", status="D"),
            ],
            columns=["ts_code", "name", "status"],
        ),
    )

    batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.FULL,
    )

    assert [call["table_id"] for call in api.calls[:1]] == [302]
    assert api.calls[0]["stocks"] == ["OF000001", "OF000002", "OF000003"]
    assert [batch["codes"] for batch in batches] == [["OF000002"]]


@pytest.mark.asyncio
async def test_fund_fof_holding_detail_smart_uses_large_batches():
    api = _FofHoldingApi()
    rows = [
        _RecordLike(ts_code=f"{i:06d}.OF", name=f"测试FOF{i}")
        for i in range(1, 451)
    ]
    task = _make_task(
        TinySoftFundFofHoldingDetailTask,
        api=api,
        db=_CodeDB(rows=rows, columns=["ts_code", "name"]),
    )

    batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )

    assert [len(batch["codes"]) for batch in batches] == [450]


def test_fund_fof_holding_detail_full_start_covers_first_public_fof_report():
    # 首只公募FOF南方全天候策略FOF成立于2017-10-19，FULL至少要覆盖2017Q4报告期。
    assert TinySoftFundFofHoldingDetailTask.default_start_date <= "20170101"


@pytest.mark.asyncio
async def test_fund_bond_holding_detail_maps_main_codes_and_processes_fields():
    api = _FundMainCodeReportApi()
    task = _make_task(
        TinySoftFundBondHoldingDetailTask,
        api=api,
        db=_CodeDB(
            rows=[
                _RecordLike(ts_code="006872.OF"),
                _RecordLike(ts_code="006873.OF"),
            ],
            columns=["ts_code", "status"],
        ),
    )

    batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )
    df = await task.fetch_batch(batches[0])
    processed = task.process_data(df)

    assert batches[0]["codes"] == ["OF006872"]
    assert api.calls[-1]["table_id"] == 342
    assert api.calls[-1]["where_clause"] == '["截止日"]>=20260301 and ["截止日"]<=20260331'
    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["ts_code"] == "006872.OF"
    assert row["bond_code_raw"] == "SH019766"
    assert row["bond_ts_code"] == "019766.SH"
    assert row["market_value"] == pytest.approx(36442028.67)
    assert bool(row["is_convertible_period"]) is False


def test_fund_asset_alloc_processes_full_asset_bucket_fields():
    task = _make_task(TinySoftFundAssetAllocTask)
    raw = pd.DataFrame(
        {
            "StockID": ["OF013300"],
            "StockName": ["工银价值稳健"],
            "截止日": [20260331],
            "股票市值": ["13050"],
            "基金市值": ["2382548000"],
            "债券市值": ["137496816.33"],
            "银行存款和清算备付金市值": ["3483731.87"],
            "资产净值": ["2542196000"],
            "资产总值": ["2553816000"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["ts_code"] == "013300.OF"
    assert row["fund_market_value"] == pytest.approx(2382548000)
    assert row["bond_market_value"] == pytest.approx(137496816.33)
    assert row["net_asset_value"] == pytest.approx(2542196000)


def test_fund_classification_tasks_process_versioned_rows():
    info_task = _make_task(TinySoftFundClassificationInfoTask)
    info_raw = pd.DataFrame(
        {
            "request_code": ["TSJJ02"],
            "属性代码": ["TSJJ0201"],
            "属性名称": ["股票型"],
            "级数": ["1"],
            "上级属性代码": ["TSJJ02"],
            "入选日期": [0],
            "剔除日期": [20251231],
            "最新标识": ["0"],
            "所属属性代码": ["TSJJ02"],
        }
    )

    info = info_task.process_data(info_raw)

    assert len(info) == 1
    assert str(info.iloc[0]["in_date"]) == "1900-01-01"
    assert str(info.iloc[0]["latest_change_date"]) == "2025-12-31"

    member_task = _make_task(TinySoftFundClassificationMemberTask)
    member_raw = pd.DataFrame(
        {
            "StockID": ["OF010031"],
            "属性代码": ["TSJJ0201"],
            "属性名称": ["股票型"],
            "级数": ["1"],
            "入选日期": [20200101],
            "剔除日期": [0],
            "最新标识": ["1"],
            "所属属性代码": ["TSJJ02"],
            "所属属性名称": ["基金投资风格"],
        }
    )

    member = member_task.process_data(member_raw)

    assert len(member) == 1
    assert member.iloc[0]["ts_code"] == "010031.OF"
    assert str(member.iloc[0]["latest_change_date"]) == "2020-01-01"


@pytest.mark.asyncio
async def test_new_fund_report_tasks_use_large_smart_batches():
    rows = [_RecordLike(ts_code=f"{i:06d}.OF") for i in range(1, 751)]
    db = _CodeDB(rows=rows, columns=["ts_code"])
    for cls in [
        TinySoftFundStockHoldingDetailTask,
        TinySoftFundIndustryAllocTask,
        TinySoftFundAssetAllocTask,
        TinySoftFundBondHoldingDetailTask,
        TinySoftFundCbondHoldingDetailTask,
        TinySoftFundStockTradeSummaryTask,
        TinySoftFundBrokerSeatTask,
    ]:
        task = _make_task(cls, api=_FundMainCodeReportApi(), db=db)
        batches = await task.get_batch_list(
            start_date="20260301",
            end_date="20260331",
            update_type=UpdateTypes.SMART,
        )
        assert len(batches[0]["codes"]) >= 300

    for cls in [TinySoftFundHolderStructureTask, TinySoftFundClassificationMemberTask]:
        task = _make_task(cls, db=db)
        batches = await task.get_batch_list(
            start_date="20260301",
            end_date="20260331",
            update_type=UpdateTypes.SMART,
        )
        assert len(batches[0]["codes"]) >= 750


def test_p2_fund_basic_ext_processes_static_fields_and_code_links():
    task = _make_task(TinySoftFundBasicExtTask)
    raw = pd.DataFrame(
        {
            "StockID": ["OF010031"],
            "基金名称": ["华泰柏瑞生物医药灵活配置混合型证券投资基金C"],
            "基金简称": ["华泰柏瑞生物医药C"],
            "基金类型": ["混合型"],
            "投资类型": ["灵活配置型"],
            "设立日": [20200601],
            "上市日": [0],
            "清算日": [0],
            "标的指数代码": ["CSI000300"],
            "是否ETF联接": ["是"],
            "ETF连接目标代码": ["OF510300"],
            "不同收费模式基金主代码": ["OF010030"],
            "母基金代码": [""],
            "分级基金分拆比例": ["1.25"],
            "募集总金额": ["123456789.12"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["ts_code"] == "010031.OF"
    assert row["fee_mode_main_ts_code"] == "010030.OF"
    assert row["tracking_index_ts_code"] == "000300.CSI"
    assert row["etf_target_ts_code"] == "510300.OF"
    assert bool(row["is_etf_feeder"]) is True
    assert str(row["latest_change_date"]) == "2020-06-01"
    assert row["structured_split_ratio"] == pytest.approx(1.25)


def test_p2_fund_manager_ext_processes_resume_fields_with_manager_key():
    task = _make_task(TinySoftFundManagerExtTask)
    raw = pd.DataFrame(
        {
            "StockID": ["OF010031"],
            "StockName": ["华泰柏瑞生物医药C"],
            "公布日": [20240220],
            "信息来源": ["基金公告"],
            "姓名": ["张三"],
            "性别": ["男"],
            "国籍": ["中国"],
            "出生年份": ["1985"],
            "年龄": ["41"],
            "职务": ["基金经理"],
            "学历": ["硕士"],
            "证券从业经历": ["多年证券从业经历"],
            "任职日": [20240219],
            "离职日": [0],
            "在任与否": ["是"],
            "简历": ["测试简历"],
            "基金经理代码": ["M001"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["ts_code"] == "010031.OF"
    assert row["manager_key"] == "M001"
    assert row["age"] == 41
    assert bool(row["is_current"]) is True
    assert str(row["begin_date"]) == "2024-02-19"


def test_p2_fund_financial_quarterly_ext_processes_quarterly_financials():
    task = _make_task(TinySoftFundFinancialQuarterlyExtTask)
    raw = pd.DataFrame(
        {
            "StockID": ["OF006872"],
            "StockName": ["长信颐天平衡养老(FOF)A"],
            "截止日": [20260331],
            "公布日": [20260422],
            "净收益": ["100.5"],
            "本期利润": ["200.25"],
            "资产总值": ["3000000000.12"],
            "资产净值": ["2500000000.34"],
            "资产净值收益率(%)": ["2.34"],
            "备注": ["季报"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["ts_code"] == "006872.OF"
    assert str(row["report_date"]) == "2026-03-31"
    assert str(row["ann_date"]) == "2026-04-22"
    assert row["net_income"] == pytest.approx(100.5)
    assert row["net_asset"] == pytest.approx(2500000000.34)
    assert row["net_asset_return_pct"] == pytest.approx(2.34)


def test_p2_index_basic_ext_processes_static_index_fields():
    task = _make_task(TinySoftIndexBasicExtTask)
    raw = pd.DataFrame(
        {
            "request_code": ["CSI000300"],
            "指数简称": ["沪深300"],
            "指数全称": ["沪深300指数"],
            "指数类型": ["规模指数"],
            "指数标的": ["股票"],
            "指数所属公司": ["中证指数有限公司"],
            "开始日期": [20050408],
            "成立日期": [20050408],
            "停用日期": [0],
            "指数起始点数": ["1000"],
            "加权方式": ["自由流通市值加权"],
            "样本个数": ["300"],
            "指数一级分类": ["股票指数"],
            "指数主代码": ["CSI000300"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["index_code_raw"] == "CSI000300"
    assert row["index_ts_code"] == "000300.CSI"
    assert row["main_index_ts_code"] == "000300.CSI"
    assert str(row["latest_change_date"]) == "2005-04-08"
    assert row["base_point"] == pytest.approx(1000)
    assert row["sample_count"] == 300


def test_tinysoft_stock_basic_ext_processes_company_and_industry_fields():
    task = _make_task(TinySoftStockBasicExtTask)
    raw = pd.DataFrame(
        {
            "StockID": ["SH600000"],
            "公司中文全称": ["上海浦东发展银行股份有限公司"],
            "公司中文简称": ["浦发银行"],
            "注册资本": ["18653471415"],
            "成立日期": [19921019],
            "当前状态": ["上市"],
            "所属市场": ["主板"],
            "申万一级行业代码": ["SW801780"],
            "中证四级行业代码": ["40101010"],
            "H股代码": [""],
            "股本单位": ["股"],
            "转换比例": ["1"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["ts_code"] == "600000.SH"
    assert row["company_short_name"] == "浦发银行"
    assert row["registered_capital"] == pytest.approx(18653471415)
    assert str(row["establish_date"]) == "1992-10-19"
    assert row["sw_industry_l1_code"] == "SW801780"


def test_tinysoft_bond_basic_ext_processes_rating_and_convertible_fields():
    task = _make_task(TinySoftBondBasicExtTask)
    raw = pd.DataFrame(
        {
            "StockID": ["SH113001"],
            "债券代码": ["SH113001"],
            "债券全称": ["中行转债"],
            "债券简称": ["中行转债"],
            "发行起始日": [20100602],
            "上市日": [20100618],
            "到期日": [20160602],
            "发行额": ["40000000000"],
            "债券种类": ["可转换债券"],
            "信用等级": ["AAA"],
            "正股代码": ["SH601988"],
            "摘牌日": [20150313],
            "转股开始日": [20101202],
            "停止转股日": [20150312],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["bond_code_raw"] == "SH113001"
    assert row["bond_ts_code"] == "113001.SH"
    assert row["underlying_ts_code"] == "601988.SH"
    assert row["credit_rating"] == "AAA"
    assert str(row["maturity_date"]) == "2016-06-02"


def test_tinysoft_future_basic_ext_processes_contract_fields():
    task = _make_task(TinySoftFutureBasicExtTask)
    raw = pd.DataFrame(
        {
            "StockID": ["AU2512"],
            "合约代码": ["au2512"],
            "变动日": [20241118],
            "交易代码": ["au"],
            "交割年份": ["2025"],
            "交割月份": ["12"],
            "交易品种": ["黄金"],
            "合约乘数": ["1000"],
            "报价单位": ["元/克"],
            "最小变动价位": ["0.02"],
            "最后交易日": [20251215],
            "最后交割日": [20251217],
            "上市地": ["上海期货交易所"],
            "期货类别": ["商品期货"],
            "商品期货类别": ["金属期货"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["contract_code_raw"] == "AU2512"
    assert row["ts_code"] == "AU2512.SHF"
    assert row["product_code"] == "AU"
    assert row["contract_multiplier"] == pytest.approx(1000)
    assert str(row["last_trade_date"]) == "2025-12-15"


def test_tinysoft_future_product_mapping_ext_processes_main_contracts():
    task = _make_task(TinySoftFutureProductMappingExtTask)
    raw = pd.DataFrame(
        {
            "StockID": ["AU"],
            "品种代码": ["au"],
            "变动日": [20260520],
            "品种名称": ["黄金"],
            "主力代码": ["au2512"],
            "主力代码2": ["au2602"],
            "次主力代码": ["au2606"],
            "连续代码": ["au00"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["product_code"] == "AU"
    assert row["main_contract_code"] == "AU2512"
    assert row["continuous_contract_code"] == "AU00"
    assert str(row["change_date"]) == "2026-05-20"


def test_tinysoft_option_basic_daily_ext_processes_daily_status_fields():
    task = _make_task(TinySoftOptionBasicDailyExtTask)
    raw = pd.DataFrame(
        {
            "StockID": ["IO2608-C-4600", "AU2608-C-460"],
            "截止日": [20260521, 20260521],
            "合约交易代码": ["IO2608-C-4600", "AU2608-C-460"],
            "合约简称": ["沪深300指数2608购4600", "黄金期权2608购460"],
            "标的证券代码": ["CSI000300", "AU"],
            "标的证券名称": ["沪深300", "黄金"],
            "标的证券类型": ["指数", "商品期货"],
            "行权方式": ["欧式", "美式"],
            "期权类型": ["认购", "认购"],
            "合约单位": ["100", "1000"],
            "行权价": ["4600", "460"],
            "首个交易日": [20260518, 20260518],
            "最后交易日": [20260821, 20260821],
            "到期日": [20260821, 20260821],
            "合约未平仓数": ["0", "10"],
            "合约前收盘价": ["12.34", "1.23"],
            "期权合约状态信息": ["正常", "正常"],
            "开仓状态": ["允许开仓", "允许开仓"],
            "上市地": ["中国金融期货交易所", "上海期货交易所"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["contract_code_raw"] == "IO2608-C-4600"
    assert row["ts_code"] == "IO2608-C-4600.CFX"
    assert str(row["trade_date"]) == "2026-05-21"
    assert row["exercise_price"] == pytest.approx(4600)
    assert row["open_interest"] == pytest.approx(0)
    assert pd.isna(row["raw_json"])


@pytest.mark.asyncio
async def test_tinysoft_option_basic_daily_ext_loads_only_financial_option_codes():
    task = _make_task(
        TinySoftOptionBasicDailyExtTask,
        db=_CodeDB(
            rows=[
                _RecordLike(ts_code="IO2608-C-4600.CFX"),
                _RecordLike(ts_code="10000001.SH"),
                _RecordLike(ts_code="510050C2406M02500.SH"),
                _RecordLike(ts_code="AU2608-C-460.SHF"),
                _RecordLike(ts_code="M2608-P-3000.DCE"),
            ],
            columns=["ts_code"],
        ),
    )

    codes = await task._load_codes_from_db()

    assert codes == ["IO2608-C-4600", "10000001", "510050C2406M02500"]


@pytest.mark.asyncio
async def test_p2_static_tasks_use_larger_smart_batches():
    fund_rows = [_RecordLike(ts_code=f"{i:06d}.OF") for i in range(1, 1002)]
    fund_task = _make_task(TinySoftFundBasicExtTask, db=_CodeDB(rows=fund_rows, columns=["ts_code"]))
    manager_task = _make_task(TinySoftFundManagerExtTask, db=_CodeDB(rows=fund_rows, columns=["ts_code"]))
    index_rows = [_RecordLike(ts_code=f"{i:06d}.CSI") for i in range(1, 502)]
    index_task = _make_task(TinySoftIndexBasicExtTask, db=_CodeDB(rows=index_rows, columns=["ts_code"]))
    stock_rows = [_RecordLike(ts_code=f"{i:06d}.SZ") for i in range(1, 1202)]
    stock_task = _make_task(TinySoftStockBasicExtTask, db=_CodeDB(rows=stock_rows, columns=["ts_code"]))

    fund_batches = await fund_task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )
    manager_batches = await manager_task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )
    index_batches = await index_task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )
    stock_batches = await stock_task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )

    assert [len(batch["codes"]) for batch in fund_batches] == [1000, 1]
    assert [len(batch["codes"]) for batch in manager_batches] == [1000, 1]
    assert [len(batch["codes"]) for batch in index_batches] == [500, 1]
    assert [len(batch["codes"]) for batch in stock_batches] == [1201]


@pytest.mark.asyncio
async def test_tinysoft_derivative_basic_tasks_use_large_smart_batches():
    future_rows = [_RecordLike(ts_code=f"AU{i:04d}.SHF") for i in range(1, 1202)]
    future_task = _make_task(TinySoftFutureBasicExtTask, db=_CodeDB(rows=future_rows, columns=["ts_code"]))
    product_rows = [_RecordLike(fut_code=f"P{i}") for i in range(1, 702)]
    product_task = _make_task(TinySoftFutureProductMappingExtTask, db=_CodeDB(rows=product_rows, columns=["fut_code"]))
    option_rows = [_RecordLike(ts_code=f"IO2608-C-{4000 + i}.CFX") for i in range(1, 1502)]
    option_task = _make_task(TinySoftOptionBasicDailyExtTask, db=_CodeDB(rows=option_rows, columns=["ts_code"]))
    bond_rows = [_RecordLike(ts_code=f"{110000 + i}.SH") for i in range(1, 1802)]
    bond_task = _make_task(TinySoftBondBasicExtTask, db=_CodeDB(rows=bond_rows, columns=["ts_code"]))

    future_batches = await future_task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )
    product_batches = await product_task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )
    option_batches = await option_task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )
    bond_batches = await bond_task.get_batch_list(
        start_date="20260301",
        end_date="20260331",
        update_type=UpdateTypes.SMART,
    )

    assert [len(batch["codes"]) for batch in future_batches] == [1000, 201]
    assert [len(batch["codes"]) for batch in product_batches] == [500, 201]
    assert [len(batch["codes"]) for batch in option_batches] == [1000, 501]
    assert [len(batch["codes"]) for batch in bond_batches] == [1801]


def test_market_calendar_multi_handles_text_booleans():
    task = _make_task(TinySoftMarketCalendarMultiTask)
    raw = pd.DataFrame(
        {
            "request_code": ["SH000001", "SH000001"],
            "截止日": [20260302, 20260303],
            "是否交易日": ["是", "否"],
            "交易日类别": ["正常交易", "休市"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 2
    assert processed["is_trade_day"].tolist() == [True, False]
    assert processed["market_name"].tolist() == ["A股市场", "A股市场"]


def test_market_calendar_multi_accepts_stockid_from_batch_fetch():
    task = _make_task(TinySoftMarketCalendarMultiTask)
    raw = pd.DataFrame(
        {
            "StockID": ["SH000001", "HSG000002"],
            "截止日": [20260302, 20260303],
            "是否交易日": ["是", "否"],
            "交易日类别": ["正常交易", "休市"],
        }
    )

    processed = task.process_data(raw)

    assert processed["market_code"].tolist() == ["SH000001", "HSG000002"]
    assert processed["market_name"].tolist() == ["A股市场", "北向交易日历"]
    assert processed["is_trade_day"].tolist() == [True, False]


def test_index_member_versioned_processes_event_dates():
    task = _make_task(TinySoftIndexMemberVersionedTask)
    raw = pd.DataFrame(
        {
            "request_code": ["CSI000300"],
            "证券代码": ["SH600000"],
            "入选日期": [20200101],
            "剔除日期": [20251231],
            "成份标志": ["0"],
            "入选公布日": [20191215],
            "剔除公布日": [20251215],
            "入选调整类型": ["定期调整"],
            "剔除调整类型": ["定期调整"],
        }
    )

    processed = task.process_data(raw)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["index_code_raw"] == "CSI000300"
    assert row["index_ts_code"] == "000300.CSI"
    assert row["con_ts_code"] == "600000.SH"
    assert str(row["latest_change_date"]) == "2025-12-31"


def test_revision_sensitive_tinysoft_p0_tasks_use_wider_smart_lookback():
    assert TinySoftBondBasicExtTask.smart_lookback_days >= 30
    assert TinySoftFutureBasicExtTask.smart_lookback_days >= 30
    assert TinySoftFutureProductMappingExtTask.smart_lookback_days >= 30
    assert TinySoftOptionBasicDailyExtTask.smart_lookback_days >= 30
    assert TinySoftStockBasicExtTask.smart_lookback_days >= 30
    assert TinySoftStockLendingBalanceTask.smart_lookback_days >= 30
    assert TinySoftMarketCalendarMultiTask.smart_lookback_days >= 370
    assert TinySoftFundAssetAllocTask.smart_lookback_days >= 370
    assert TinySoftFundBasicExtTask.smart_lookback_days >= 370
    assert TinySoftFundBondHoldingDetailTask.smart_lookback_days >= 370
    assert TinySoftFundClassificationInfoTask.smart_lookback_days >= 370
    assert TinySoftFundClassificationMemberTask.smart_lookback_days >= 370
    assert TinySoftFundFinancialQuarterlyExtTask.smart_lookback_days >= 370
    assert TinySoftFundFofHoldingDetailTask.smart_lookback_days >= 370
    assert TinySoftFundHolderStructureTask.smart_lookback_days >= 370
    assert TinySoftFundManagerExtTask.smart_lookback_days >= 370
    assert TinySoftFundStockHoldingDetailTask.smart_lookback_days >= 370
    assert TinySoftIndexBasicExtTask.smart_lookback_days >= 370


def test_tinysoft_tasks_stream_batches_by_default():
    assert TinySoftTask.default_stream_batches is True
    assert TinySoftP0InfoArrayTask.default_stream_batches is True
    assert TinySoftOptionBasicDailyExtTask.default_stream_batches is True
    assert TinySoftOptionBasicDailyExtTask.default_include_raw_json is False
