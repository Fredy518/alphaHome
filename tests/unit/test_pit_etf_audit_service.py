"""Regression coverage for ETF task denominators and shared-table isolation."""

from datetime import date, timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from alphahome.pit import audit_service
from alphahome.pit.audit_service import PITAuditService
from alphahome.pit.calculators.etf_index_a_share_proxy_fapi_calculator import (
    ETFIndexAShareProxyFAPICalculator,
)
from alphahome.pit.calculators.etf_index_a_share_proxy_members_calculator import (
    ETFIndexAShareProxyMembersCalculator,
)
from alphahome.pit.calculators.etf_index_fapi_calculator import ETFIndexFAPICalculator
from alphahome.pit.calculators.etf_index_members_calculator import (
    ETFIndexMembersCalculator,
)
from alphahome.pit.pit_etf_index_a_share_proxy_members_manager import (
    PITETFIndexAShareProxyMembersMonthlyManager,
)
from alphahome.pit.tasks.etf_index_a_share_proxy import (
    PITETFIndexAShareProxyFAPIMonthlyTask,
    PITETFIndexAShareProxyMembersMonthlyTask,
)
from alphahome.pit.tasks.etf_index_fapi import PITETFIndexFAPIMonthlyTask
from alphahome.pit.tasks.etf_index_members import PITETFIndexMembersMonthlyTask
from alphahome.fetchers.tasks.fund.tushare_fund_portfolio import (
    TushareFundPortfolioTask,
)
from alphahome.fetchers.tasks.index.tushare_index_weight import (
    TushareIndexWeightTask,
)

OBS_DATE = date(2026, 2, 28)
ETF_TASKS = (
    (PITETFIndexMembersMonthlyTask, ETFIndexMembersCalculator),
    (PITETFIndexFAPIMonthlyTask, ETFIndexFAPICalculator),
    (PITETFIndexAShareProxyMembersMonthlyTask, ETFIndexAShareProxyMembersCalculator),
    (PITETFIndexAShareProxyFAPIMonthlyTask, ETFIndexAShareProxyFAPICalculator),
)


@pytest.mark.parametrize("task,calculator", ETF_TASKS)
@pytest.mark.parametrize("table_exists", [False, True])
@pytest.mark.asyncio
async def test_missing_or_empty_etf_output_does_not_warn_unknown_denominator(
    monkeypatch, task, calculator, table_exists
):
    db = AsyncMock()
    db.fetch_one.return_value = {"row_count": 0, "latest_pit_time": None}
    service = PITAuditService(db)
    monkeypatch.setattr(
        service, "_relation_exists", AsyncMock(return_value=table_exists)
    )
    monkeypatch.setattr(service, "_get_columns", AsyncMock(return_value=set()))
    monkeypatch.setattr(audit_service.logger, "warning", Mock())

    stats = await service._table_stats(task.contract)

    assert stats["status"] == (
        "migration_required" if table_exists else "missing_table"
    )
    assert stats["row_count"] == 0
    assert stats["coverage_rate"] is None
    audit_service.logger.warning.assert_not_called()


def _official(index_code="IDX", when=OBS_DATE - timedelta(days=1)):
    return [
        {
            "index_code": index_code,
            "index_name": index_code,
            "weight_trade_date": when,
            "ts_code": f"{stock:06d}.SZ",
            "raw_weight": 20.0,
        }
        for stock in range(1, 6)
    ]


def _holdings(index_code="IDX"):
    return [
        {
            "index_code": index_code,
            "index_name": index_code,
            "etf_code": "510000.SH",
            "ann_date": date(2026, 1, 20),
            "end_date": date(2025, 12, 31),
            "ts_code": f"{stock:06d}.SZ",
            "raw_weight": 10.0,
        }
        for stock in range(10, 20)
    ]


class _SourceDB:
    def __init__(self, official=(), holdings=(), codes=("IDX",)):
        self.official = list(official)
        self.holdings = list(holdings)
        self.codes = codes
        self.queries = []

    async def fetch(self, query, *args):
        self.queries.append((query, args))
        if "FROM rawdata.index_weight" in query:
            codes, start, end = args
            assert "trade_date BETWEEN $2 AND $3" in query
            return [
                row
                for row in self.official
                if row["index_code"] in codes
                and start <= row["weight_trade_date"] <= end
            ]
        if "WITH latest AS" in query:
            codes, start, end = args
            assert "p.ann_date <= $3" in query
            assert "p.end_date BETWEEN $2 AND $3" in query
            return [
                row
                for row in self.holdings
                if row["index_code"] in codes
                and start <= row["end_date"] <= end
                and row["ann_date"] <= end
            ]
        assert "FROM rawdata.fund_etf_basic" in query
        assert "status = 'L' AND etf_type = '纯境内'" in query
        return [{"index_code": code} for code in self.codes]


@pytest.mark.parametrize(
    "official,expected",
    [
        (_official(), 5),
        (_official(when=OBS_DATE + timedelta(days=1)), 10),
        (_official(when=OBS_DATE - timedelta(days=66)), 10),
        (_official()[:4], 10),
    ],
    ids=[
        "official_priority",
        "no_future_weights",
        "stale_weights",
        "incomplete_weights",
    ],
)
@pytest.mark.asyncio
async def test_member_denominator_reconstructs_visible_sources_independently_of_output(
    monkeypatch, official, expected
):
    holdings = _holdings()
    holdings += [dict(holdings[0], ts_code="000999.SZ", ann_date=date(2026, 3, 1))]
    db = _SourceDB(official, holdings)
    service = PITAuditService(db)
    monkeypatch.setattr(service, "_relation_exists", AsyncMock(return_value=True))

    count = await service._denominator_count(
        PITETFIndexMembersMonthlyTask.contract, OBS_DATE
    )

    assert count == expected
    assert all("pit.pit_etf_index_members_monthly" not in sql for sql, _ in db.queries)


@pytest.mark.asyncio
async def test_members_mix_official_and_fallback_sources_without_double_counting(
    monkeypatch,
):
    db = _SourceDB(
        _official(), _holdings() + _holdings("FALLBACK"), codes=("IDX", "FALLBACK")
    )
    service = PITAuditService(db)
    monkeypatch.setattr(service, "_relation_exists", AsyncMock(return_value=True))

    count = await service._denominator_count(
        PITETFIndexMembersMonthlyTask.contract, OBS_DATE
    )

    assert count == 15
    holding_args = next(args for sql, args in db.queries if "WITH latest AS" in sql)
    assert holding_args[0] == ["FALLBACK"]


@pytest.mark.asyncio
async def test_proxy_member_denominator_counts_a_shares_after_full_snapshot_validation(
    monkeypatch,
):
    rows = _official("931238.CSI")
    for row in rows:
        row["raw_weight"] = 14.0
    rows += [
        dict(rows[0], ts_code=code, raw_weight=15.0)
        for code in ("00178.HK", "178.HK", "02899.HK")
    ]
    rows += _official("UNREGISTERED")
    db = _SourceDB(rows)
    service = PITAuditService(db)
    monkeypatch.setattr(service, "_relation_exists", AsyncMock(return_value=True))

    count = await service._denominator_count(
        PITETFIndexAShareProxyMembersMonthlyTask.contract, OBS_DATE
    )

    assert count == 5
    assert len(db.queries) == 1
    assert db.queries[0][1][0] == ["931238.CSI"]


@pytest.mark.asyncio
async def test_fapi_denominator_counts_upstream_indices_in_matching_month_and_version(
    monkeypatch,
):
    db = AsyncMock()
    db.fetch_one.return_value = {"cnt": 2}
    service = PITAuditService(db)
    monkeypatch.setattr(service, "_relation_exists", AsyncMock(return_value=True))

    count = await service._denominator_count(
        PITETFIndexFAPIMonthlyTask.contract, OBS_DATE
    )

    assert count == 2
    sql, *args = db.fetch_one.call_args.args
    assert "COUNT(DISTINCT index_code)" in sql
    assert "FROM pit.pit_etf_index_members_monthly" in sql
    assert "obs_date = $1 AND method_version = $2" in sql
    assert args == [OBS_DATE, ETFIndexMembersCalculator.METHOD_VERSION]


@pytest.mark.asyncio
async def test_proxy_fapi_denominator_uses_registered_universe_even_when_output_is_incomplete(
    monkeypatch,
):
    monkeypatch.setattr(
        PITETFIndexAShareProxyMembersMonthlyManager,
        "DEFAULT_PROXY_INDEX_CODES",
        ("931238.CSI", "SECOND", "931238.CSI"),
    )
    count = await PITAuditService(AsyncMock())._denominator_count(
        PITETFIndexAShareProxyFAPIMonthlyTask.contract, OBS_DATE
    )
    assert count == 2


@pytest.mark.parametrize("task,calculator", ETF_TASKS[:3])
@pytest.mark.asyncio
async def test_missing_source_tables_return_zero(monkeypatch, task, calculator):
    db = AsyncMock()
    service = PITAuditService(db)
    monkeypatch.setattr(service, "_relation_exists", AsyncMock(return_value=False))
    assert await service._denominator_count(task.contract, OBS_DATE) == 0
    db.fetch.assert_not_called()
    db.fetch_one.assert_not_called()


@pytest.mark.parametrize("task,calculator", ETF_TASKS)
@pytest.mark.asyncio
async def test_all_stats_queries_scope_shared_table_to_task_version(
    monkeypatch, task, calculator
):
    db = AsyncMock()
    db.fetch_one.side_effect = [
        {"row_count": 4, "latest_pit_time": OBS_DATE},
        {"coverage_period": OBS_DATE},
        {"coverage_count": 4},
    ]
    service = PITAuditService(db)
    monkeypatch.setattr(service, "_relation_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(
        service, "_get_columns", AsyncMock(return_value=set(task.contract.primary_keys))
    )
    monkeypatch.setattr(service, "_denominator_count", AsyncMock(return_value=5))

    stats = await service._table_stats(task.contract)

    assert stats["row_count"] == 4
    assert stats["latest_pit_time"] == OBS_DATE
    assert stats["coverage_rate"] == 0.8
    assert stats["gap_count"] == 1
    expected_scope = f"t.method_version = '{calculator.METHOD_VERSION}'"
    for call in db.fetch_one.call_args_list:
        sql, *_ = call.args
        assert expected_scope in sql


@pytest.mark.asyncio
async def test_etf_coverage_fallback_preserves_date_and_scope_parameters(monkeypatch):
    contract = PITETFIndexMembersMonthlyTask.contract
    previous = date(2026, 1, 31)
    db = AsyncMock()
    db.fetch_one.side_effect = [
        {"coverage_period": OBS_DATE},
        {"coverage_count": 0},
        {"coverage_period": previous},
        {"coverage_count": 5},
    ]
    service = PITAuditService(db)
    monkeypatch.setattr(
        service, "_get_columns", AsyncMock(return_value=set(contract.primary_keys))
    )

    result = await service._coverage_for_latest(contract, "obs_date")

    assert result == {"coverage_period": previous, "coverage_count": 5}
    sql, *args = db.fetch_one.call_args_list[2].args
    assert 't."obs_date" < $1' in sql
    assert (
        f"t.method_version = '{ETFIndexMembersCalculator.METHOD_VERSION}'" in sql
    )
    assert args == [OBS_DATE]


@pytest.mark.asyncio
async def test_every_registered_pit_contract_has_a_supported_audit_denominator(
    monkeypatch,
):
    service = PITAuditService(AsyncMock())
    monkeypatch.setattr(service, "_relation_exists", AsyncMock(return_value=False))
    monkeypatch.setattr(service, "_current_listed_count", AsyncMock(return_value=0))
    monkeypatch.setattr(audit_service.logger, "warning", Mock())

    for task in service._pit_task_classes().values():
        await service._denominator_count(task.contract, None)

    audit_service.logger.warning.assert_not_called()


def test_etf_source_tasks_declare_audit_query_indexes():
    def declared_columns(task):
        return {
            tuple(
                part.strip()
                for part in str(index["columns"]).split(",")
            )
            for index in task.indexes
        }

    assert ("index_code", "trade_date") in declared_columns(
        TushareIndexWeightTask
    )
    assert ("ts_code", "end_date") in declared_columns(
        TushareFundPortfolioTask
    )
