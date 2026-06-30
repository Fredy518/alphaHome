#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.tasks.fund.akshare_fund_fee_em import AkShareFundFeeEmTask
from alphahome.fetchers.tasks.fund.akshare_fund_fee_utils import current_snapshot_date
from alphahome.fetchers.tasks.fund.akshare_fund_overview_em import AkShareFundOverviewEmTask
from alphahome.fetchers.tasks.fund.akshare_fund_individual_detail_info_xq import (
    AkShareFundIndividualDetailInfoXqTask,
)


class _MockDB:
    async def get_column_names(self, target):
        return []

    async def fetch(self, query, *args, **kwargs):
        return [{"ts_code": "000001.OF"}, {"ts_code": "000003.OF"}]

    async def get_latest_date(self, target, date_column):
        return None

    async def table_exists(self, target):
        return False

    async def get_latest_update_time(self, target):
        return None


class _ResumeMockDB(_MockDB):
    def __init__(self, existing_fee_rows=None, existing_rows_by_table=None):
        self.month_args = None
        self.month_args_by_table = {}
        self.existing_rows_by_table = existing_rows_by_table or {}
        if existing_fee_rows is not None:
            self.existing_rows_by_table["fund_fee_em"] = existing_fee_rows

    async def table_exists(self, target):
        return True

    async def fetch(self, query, *args, **kwargs):
        if "FROM tushare.fund_basic" in query:
            return [{"ts_code": "000001.OF"}, {"ts_code": "000003.OF"}]
        for table_name, rows in self.existing_rows_by_table.items():
            if "FROM" in query and table_name in query:
                self.month_args = args
                self.month_args_by_table[table_name] = args
                return rows
        return []


def _current_month_anchor():
    return pd.to_datetime(current_snapshot_date()).date().replace(day=1)


def _run_pipeline(task, raw_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    transformed = task.data_transformer.process_data(raw_df.copy())
    return task.process_data(transformed, **kwargs)


def test_fund_fee_purchase_schedule_parses_amount_and_discount_rates():
    task = AkShareFundFeeEmTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw = pd.DataFrame(
        {
            "适用金额": [
                "小于100万元",
                "大于等于100万元，小于500万元",
                "大于等于1000万元",
            ],
            "原费率|天天基金优惠费率": ["1.50% | 0.15%", "1.20% | 0.12%", "每笔1000元"],
        }
    )

    processed = _run_pipeline(task, raw, fund_code="000001", indicator="申购费率（前端）")

    assert len(processed) == 3
    first = processed.iloc[0]
    second = processed.iloc[1]
    third = processed.iloc[2]
    assert first["rule_type"] == "amount_fee"
    assert first["condition_max_amount_wan"] == 100.0
    assert first["original_rate_pct"] == 1.5
    assert first["discount_rate_pct"] == 0.15
    assert second["condition_min_amount_wan"] == 100.0
    assert second["condition_max_amount_wan"] == 500.0
    assert third["flat_fee_amount_yuan"] == 1000.0
    assert third["fee_unit"] == "yuan_per_txn"


def test_fund_fee_redeem_schedule_parses_holding_days():
    task = AkShareFundFeeEmTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw = pd.DataFrame({"适用期限": ["小于7天", "大于等于7天"], "赎回费率": ["1.50%", "0.50%"]})

    processed = _run_pipeline(task, raw, fund_code="000001", indicator="赎回费率")

    assert processed["rule_type"].tolist() == ["holding_period_fee", "holding_period_fee"]
    assert processed.iloc[0]["condition_max_holding_days"] == 7.0
    assert processed.iloc[1]["condition_min_holding_days"] == 7.0
    assert processed.iloc[1]["fee_rate_pct"] == 0.5


def test_fund_fee_operation_normalizes_wide_rows():
    task = AkShareFundFeeEmTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw = pd.DataFrame([[ "管理费率", "1.20%（每年）", "托管费率", "0.20%（每年）", "销售服务费率", "---" ]])

    processed = _run_pipeline(task, raw, fund_code="000001", indicator="运作费用")

    assert len(processed) == 3
    assert processed["item_name"].tolist() == ["管理费率", "托管费率", "销售服务费率"]
    assert processed.iloc[0]["fee_rate_pct"] == 1.2
    assert processed.iloc[0]["operation_period"] == "每年"
    assert pd.isna(processed.iloc[2]["fee_rate_pct"])


@pytest.mark.asyncio
async def test_fund_fee_explicit_code_batches_cross_join_default_indicators():
    task = AkShareFundFeeEmTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)

    batches = await task.get_batch_list(fund_codes=["000001.OF", "000003"])

    assert len(batches) == 6
    assert batches[0] == {"fund_code": "000001", "symbol": "000001", "indicator": "申购费率（前端）"}


def test_fund_code_tasks_stream_smart_by_default():
    task = AkShareFundFeeEmTask(db_connection=_MockDB(), update_type=UpdateTypes.SMART)

    assert task.stream_batches is True
    assert UpdateTypes.SMART in task.stream_update_types
    assert task.continue_on_stream_batch_failure is True


def test_fund_code_tasks_use_fast_defaults():
    fee = AkShareFundFeeEmTask(db_connection=_MockDB(), update_type=UpdateTypes.SMART)
    overview = AkShareFundOverviewEmTask(db_connection=_MockDB(), update_type=UpdateTypes.SMART)
    detail = AkShareFundIndividualDetailInfoXqTask(db_connection=_MockDB(), update_type=UpdateTypes.SMART)

    assert fee.request_interval == 0.10
    assert fee.concurrent_limit == 6
    assert fee.max_retries == 1
    assert fee.retry_delay == 1
    assert fee.stream_save_batch_size == 3000

    assert overview.request_interval == 0.10
    assert overview.concurrent_limit == 6
    assert overview.max_retries == 1
    assert overview.stream_save_batch_size == 2000

    assert detail.request_interval == 0.15
    assert detail.concurrent_limit == 4
    assert detail.max_retries == 1
    assert detail.stream_save_batch_size == 2000


def test_fund_fee_text_schema_allows_long_raw_fee_text():
    assert AkShareFundFeeEmTask.schema_def["fee_text"]["type"] == "TEXT"


@pytest.mark.asyncio
async def test_fund_fee_smart_first_month_run_keeps_all_batches():
    db = _ResumeMockDB(existing_fee_rows=[])
    task = AkShareFundFeeEmTask(db_connection=db, update_type=UpdateTypes.SMART)

    batches = await task.get_batch_list()

    assert len(batches) == 6
    assert all(batch["snapshot_date"] == current_snapshot_date() for batch in batches)
    assert hasattr(db.month_args[0], "toordinal")
    assert hasattr(db.month_args[1], "toordinal")
    assert db.month_args[0].day == 1
    assert db.month_args[0] < db.month_args[1]


@pytest.mark.asyncio
async def test_fund_fee_smart_batches_skip_existing_month_pairs_and_anchor_snapshot():
    anchor = _current_month_anchor()
    db = _ResumeMockDB(
        existing_fee_rows=[
            {"fund_code": "000001", "indicator": "申购费率（前端）", "first_snapshot_date": anchor},
            {"fund_code": "000001", "indicator": "赎回费率", "first_snapshot_date": anchor},
        ]
    )
    task = AkShareFundFeeEmTask(db_connection=db, update_type=UpdateTypes.SMART)

    batches = await task.get_batch_list()

    assert {
        "fund_code": "000001",
        "symbol": "000001",
        "indicator": "申购费率（前端）",
        "snapshot_date": str(anchor),
    } not in batches
    assert {
        "fund_code": "000001",
        "symbol": "000001",
        "indicator": "赎回费率",
        "snapshot_date": str(anchor),
    } not in batches
    assert {
        "fund_code": "000001",
        "symbol": "000001",
        "indicator": "运作费用",
        "snapshot_date": str(anchor),
    } in batches
    assert len(batches) == 4
    assert {batch["snapshot_date"] for batch in batches} == {str(anchor)}
    assert hasattr(db.month_args[0], "toordinal")


def test_fund_fee_process_data_uses_snapshot_date_override():
    task = AkShareFundFeeEmTask(db_connection=_MockDB(), update_type=UpdateTypes.SMART)
    raw = pd.DataFrame({"适用期限": ["小于7天"], "赎回费率": ["1.50%"]})

    processed = _run_pipeline(
        task,
        raw,
        fund_code="000001",
        indicator="赎回费率",
        snapshot_date="2026-06-01",
    )

    assert processed["snapshot_date"].tolist() == ["2026-06-01"]


@pytest.mark.asyncio
async def test_fund_fee_optional_purchase_indicator_keyerror_is_no_data(monkeypatch):
    from alphahome.fetchers.tasks.fund import akshare_fund_fee_em as module

    class _FakeAk:
        @staticmethod
        def fund_fee_em(symbol, indicator):
            raise KeyError(indicator)

    monkeypatch.setattr(module, "ak", _FakeAk())
    monkeypatch.setattr(AkShareFundFeeEmTask, "_read_fund_fee_table_by_title", staticmethod(lambda *args: None))
    task = AkShareFundFeeEmTask(
        db_connection=_MockDB(),
        update_type=UpdateTypes.SMART,
        task_config={"request_interval": 0},
    )

    data = await task.fetch_batch(
        {"fund_code": "004340", "symbol": "004340", "indicator": "申购费率（前端）"}
    )

    assert data is None


@pytest.mark.asyncio
async def test_fund_fee_optional_purchase_indicator_falls_back_to_purchase_title(monkeypatch):
    from alphahome.fetchers.tasks.fund import akshare_fund_fee_em as module

    class _FakeAk:
        @staticmethod
        def fund_fee_em(symbol, indicator):
            raise KeyError(indicator)

    fallback_raw = pd.DataFrame(
        {
            "适用金额": ["小于100万元"],
            "原费率|天天基金优惠费率": ["1.20% | 0.12%"],
        }
    )
    monkeypatch.setattr(module, "ak", _FakeAk())
    monkeypatch.setattr(
        AkShareFundFeeEmTask,
        "_read_fund_fee_table_by_title",
        staticmethod(lambda symbol, title: fallback_raw),
    )
    task = AkShareFundFeeEmTask(
        db_connection=_MockDB(),
        update_type=UpdateTypes.SMART,
        task_config={"request_interval": 0},
    )

    data = await task.fetch_batch(
        {"fund_code": "000190", "symbol": "000190", "indicator": "申购费率（前端）"}
    )

    assert len(data) == 1
    row = data.iloc[0]
    assert row["fund_code"] == "000190"
    assert row["indicator"] == "申购费率（前端）"
    assert row["original_rate_pct"] == 1.2
    assert row["discount_rate_pct"] == 0.12


@pytest.mark.asyncio
async def test_fund_overview_smart_batches_skip_existing_month_codes_and_anchor_snapshot():
    anchor = _current_month_anchor()
    db = _ResumeMockDB(
        existing_rows_by_table={
            "fund_overview_em": [
                {"fund_code": "000001", "first_snapshot_date": anchor},
            ],
        }
    )
    task = AkShareFundOverviewEmTask(db_connection=db, update_type=UpdateTypes.SMART)

    batches = await task.get_batch_list()

    assert batches == [{"fund_code": "000003", "symbol": "000003", "snapshot_date": str(anchor)}]
    assert hasattr(db.month_args_by_table["fund_overview_em"][0], "toordinal")


def test_fund_overview_process_data_uses_snapshot_date_override():
    task = AkShareFundOverviewEmTask(db_connection=_MockDB(), update_type=UpdateTypes.SMART)
    raw = pd.DataFrame(
        [
            {
                "基金简称": "华夏成长混合",
                "管理费率": "1.20%（每年）",
            }
        ]
    )

    processed = _run_pipeline(task, raw, fund_code="000001", snapshot_date="2026-06-01")

    assert processed["snapshot_date"].tolist() == ["2026-06-01"]


def test_fund_overview_parses_fee_rates():
    task = AkShareFundOverviewEmTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw = pd.DataFrame(
        [
            {
                "基金全称": "华夏成长证券投资基金",
                "基金简称": "华夏成长混合",
                "基金代码": "000001（前端）、000002（后端）",
                "基金类型": "混合型-灵活",
                "管理费率": "1.20%（每年）",
                "托管费率": "0.20%（每年）",
                "销售服务费率": "---（每年）",
                "最高认购费率": "1.00%（前端）",
            }
        ]
    )

    processed = _run_pipeline(task, raw, fund_code="000001")

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["fund_code"] == "000001"
    assert row["fund_name"] == "华夏成长混合"
    assert row["management_fee_rate_pct"] == 1.2
    assert row["custodian_fee_rate_pct"] == 0.2
    assert pd.isna(row["sales_service_fee_rate_pct"])
    assert row["max_subscription_fee_rate_pct"] == 1.0


@pytest.mark.asyncio
async def test_fund_detail_xq_smart_batches_skip_existing_month_codes_and_anchor_snapshot():
    anchor = _current_month_anchor()
    db = _ResumeMockDB(
        existing_rows_by_table={
            "fund_individual_detail_info_xq": [
                {"fund_code": "000003", "first_snapshot_date": anchor},
            ],
        }
    )
    task = AkShareFundIndividualDetailInfoXqTask(db_connection=db, update_type=UpdateTypes.SMART)

    batches = await task.get_batch_list()

    assert batches == [
        {"fund_code": "000001", "symbol": "000001", "timeout": 10.0, "snapshot_date": str(anchor)}
    ]
    assert hasattr(db.month_args_by_table["fund_individual_detail_info_xq"][0], "toordinal")


def test_fund_detail_xq_process_data_uses_snapshot_date_override():
    task = AkShareFundIndividualDetailInfoXqTask(db_connection=_MockDB(), update_type=UpdateTypes.SMART)
    raw = pd.DataFrame([{"费用类型": "其他费用", "条件或名称": "管理费率", "费用": 1.2}])

    processed = _run_pipeline(task, raw, fund_code="000001", snapshot_date="2026-06-01")

    assert processed["snapshot_date"].tolist() == ["2026-06-01"]


@pytest.mark.asyncio
async def test_fund_detail_xq_missing_data_error_is_no_data():
    task = AkShareFundIndividualDetailInfoXqTask(db_connection=_MockDB(), update_type=UpdateTypes.SMART)

    class _FakeApi:
        async def call(self, **kwargs):
            from alphahome.fetchers.sources.akshare.akshare_api import AkShareAPIError

            raise AkShareAPIError("akshare.fund_individual_detail_info_xq 调用失败: 'data'")

    task.api = _FakeApi()

    data = await task.fetch_batch({"fund_code": "000014", "symbol": "000014", "timeout": 10.0})

    assert data is None


def test_fund_detail_xq_parses_buy_sell_rules_and_fee_unit_hint():
    task = AkShareFundIndividualDetailInfoXqTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw = pd.DataFrame(
        [
            {"费用类型": "买入规则", "条件或名称": "0.0万<买入金额<100.0万", "费用": 1.5},
            {"费用类型": "买入规则", "条件或名称": "1000.0万<=买入金额", "费用": 1000.0},
            {"费用类型": "卖出规则", "条件或名称": "0.0天<持有期限<7.0天", "费用": 1.5},
            {"费用类型": "其他费用", "条件或名称": "管理费率", "费用": 1.2},
        ]
    )

    processed = _run_pipeline(task, raw, fund_code="000001")

    assert len(processed) == 4
    assert processed.iloc[0]["condition_min_amount_wan"] == 0.0
    assert processed.iloc[0]["condition_max_amount_wan"] == 100.0
    assert processed.iloc[1]["fee_unit_hint"] == "yuan_per_txn"
    assert processed.iloc[2]["condition_min_holding_days"] == 0.0
    assert processed.iloc[2]["condition_max_holding_days"] == 7.0
    assert processed.iloc[3]["fee_unit_hint"] == "pct_per_year"
