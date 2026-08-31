import asyncio
from datetime import date
from unittest.mock import AsyncMock

import pytest

from alphahome.pit.audit_service import PITAuditService
from alphahome.pit.base.pit_task import PITTaskContract
from alphahome.pit import audit_service


class _FakeManager:
    pass


class _FakeAuditTask:
    task_type = "pit"
    contract = PITTaskContract(
        task_name="fake_task",
        domain="financials",
        source_tables=("tushare.fake_financial",),
        output_table="pit.fake_pit",
        pit_time_key="ann_date",
        primary_keys=("ts_code", "end_date", "ann_date", "data_source"),
        dependencies=(),
        supported_modes=("incremental", "audit_only"),
        manager_class=_FakeManager,
    )


class _SecondFakeAuditTask:
    task_type = "pit"
    contract = PITTaskContract(
        task_name="second_fake_task",
        domain="financials",
        source_tables=("tushare.second_fake_financial",),
        output_table="pit.second_fake_pit",
        pit_time_key="ann_date",
        primary_keys=("ts_code", "end_date", "ann_date", "data_source"),
        dependencies=(),
        supported_modes=("incremental", "audit_only"),
        manager_class=_FakeManager,
    )


class _FakeCashflowManager:
    pass


_CashflowTaskContract = PITTaskContract(
    task_name="pit_cashflow_quarterly",
    domain="financials",
    source_tables=("tushare.fina_cashflow",),
    output_table="pit.pit_cashflow_quarterly",
    pit_time_key="ann_date",
    primary_keys=("ts_code", "end_date", "ann_date", "data_source"),
    dependencies=(),
    supported_modes=("incremental", "audit_only"),
    manager_class=_FakeCashflowManager,
)


class _FakeDB:
    def __init__(self):
        self.executed = []

    async def execute(self, query, *args, **kwargs):
        self.executed.append((query, args))
        return "OK"

    async def fetch_one(self, query, *args, **kwargs):
        if "information_schema.tables" in query:
            return {"exists": True}
        if "COUNT(*)::bigint AS row_count" in query:
            return {"row_count": 100, "latest_pit_time": date(2026, 4, 30)}
        if "pit_coverage_candidate_period" in query:
            return {"coverage_period": date(2025, 12, 31)}
        if "pit_coverage_count" in query:
            return {"coverage_count": 8}
        if "COUNT(DISTINCT ts_code)::bigint AS cnt" in query:
            return {"cnt": 10}
        if "WITH raw_codes" in query:
            return {"raw_count": 9, "pit_count": 8, "raw_missing_in_pit": 1}
        return None

    async def fetch(self, query, *args, **kwargs):
        if "information_schema.columns" in query:
            schema, table = args
            if (schema, table) == ("pit", "fake_pit"):
                return [
                    {"column_name": "ts_code"},
                    {"column_name": "end_date"},
                    {"column_name": "ann_date"},
                    {"column_name": "data_source"},
                ]
            if (schema, table) == ("tushare", "fake_financial"):
                return [{"column_name": "ts_code"}, {"column_name": "end_date"}]
            if (schema, table) == ("rawdata", "stock_basic"):
                return [{"column_name": "ts_code"}, {"column_name": "list_status"}]
        return []


@pytest.mark.asyncio
async def test_pit_audit_task_returns_coverage_and_persists_snapshot(monkeypatch):
    monkeypatch.setattr(
        audit_service.UnifiedTaskFactory,
        "get_tasks_by_type",
        lambda task_type: {"fake_task": _FakeAuditTask},
    )
    db = _FakeDB()
    service = PITAuditService(db)

    result = await service.audit_task("fake_task", persist=True)

    assert result["status"] == "healthy"
    assert result["row_count"] == 100
    assert result["coverage_rate"] == 0.8
    assert result["gap_count"] == 2
    assert result["details"]["raw_vs_pit"]["raw_missing_in_pit"] == 1
    assert any("pit.pit_audit_snapshot" in query for query, _ in db.executed)


@pytest.mark.asyncio
async def test_raw_gap_compares_like_for_like_accounting_sources():
    class _CaptureDB(_FakeDB):
        async def fetch_one(self, query, *args, **kwargs):
            self.last_fetch_query = query
            return await super().fetch_one(query, *args, **kwargs)

    contract = PITTaskContract(
        task_name="pit_financial_indicators",
        domain="financials",
        source_tables=("pit.pit_income_quarterly", "pit.pit_balance_quarterly"),
        output_table="pit.pit_financial_indicators",
        pit_time_key="ann_date",
        primary_keys=("ts_code", "end_date", "ann_date", "data_source"),
        dependencies=("pit_income_quarterly", "pit_balance_quarterly"),
        supported_modes=("audit_only",),
        manager_class=_FakeManager,
    )
    db = _CaptureDB()
    service = PITAuditService(db)
    service._relation_exists = AsyncMock(return_value=True)

    async def _get_columns(relation):
        columns = {"ts_code", "end_date", "ann_date", "data_source"}
        if relation == "pit.pit_income_quarterly":
            return columns | {"conversion_status"}
        if relation == "pit.pit_balance_quarterly":
            return columns | {"tot_assets"}
        return columns

    service._get_columns = AsyncMock(side_effect=_get_columns)
    service._listed_join_sql = AsyncMock(return_value="")

    await service._raw_gap_summary(contract, date(2026, 6, 30))

    query = db.last_fetch_query
    assert "s.data_source IN ('report', 'express')" in query
    assert "p.data_source IN ('report', 'express')" in query
    assert "COALESCE(s.conversion_status, '') <> 'RPT_ORIG'" in query
    assert 'FROM "pit"."pit_balance_quarterly" b' in query
    assert "b.ann_date <= s.ann_date" in query
    assert "b.tot_assets IS NOT NULL" in query


@pytest.mark.asyncio
async def test_pit_audit_service_falls_back_to_registry_when_factory_uninitialized(monkeypatch):
    def _raise_uninitialized(task_type):
        raise RuntimeError("UnifiedTaskFactory 尚未初始化，请先调用 initialize() 方法")

    monkeypatch.setattr(audit_service.UnifiedTaskFactory, "get_tasks_by_type", _raise_uninitialized)
    monkeypatch.setattr(audit_service.UnifiedTaskFactory, "_task_registry", {"fake_task": _FakeAuditTask})

    db = _FakeDB()
    service = PITAuditService(db)

    result = await service.audit_task("fake_task", persist=False)

    assert result["status"] == "healthy"
    assert result["row_count"] == 100


@pytest.mark.asyncio
async def test_list_pit_tasks_uses_live_stats_and_separates_execution_from_audit(monkeypatch):
    monkeypatch.setattr(
        audit_service.UnifiedTaskFactory,
        "get_tasks_by_type",
        lambda task_type: {"fake_task": _FakeAuditTask},
    )
    service = PITAuditService(_FakeDB())

    async def _table_stats(contract):
        return {
            "latest_pit_time": date(2026, 8, 12),
            "row_count": 123,
            "coverage_rate": 0.95,
            "gap_count": 5,
            "status": "healthy",
        }

    async def _latest_snapshot(task_name):
        return {
            "last_audit_time": "2026-07-03T13:39:22+08:00",
            "audited_latest_date": date(2026, 6, 13),
            "audited_row_count": 100,
            "audited_coverage_rate": 0.8,
            "audited_gap_count": 20,
            "audit_status": "healthy",
        }

    async def _latest_execution(task_name):
        return {
            "last_execution_time": "2026-08-14T14:15:56+08:00",
            "last_execution_status": "success",
            "last_execution_details": "处理完成 (行数: 160)",
        }

    monkeypatch.setattr(service, "_table_stats", _table_stats)
    monkeypatch.setattr(service, "_latest_snapshot_for_task", _latest_snapshot)
    monkeypatch.setattr(service, "_latest_task_execution_for_task", _latest_execution)

    result = (await service.list_pit_tasks())[0]

    assert result["latest_date"] == date(2026, 8, 12)
    assert result["row_count"] == 123
    assert result["coverage_rate"] == 0.95
    assert result["last_execution_status"] == "success"
    assert result["last_execution_time"] == "2026-08-14T14:15:56+08:00"
    assert result["last_audit_time"] == "2026-07-03T13:39:22+08:00"
    assert result["audited_latest_date"] == date(2026, 6, 13)
    assert result["recent_status"] == "success"
    assert result["last_run_time"] == result["last_execution_time"]


@pytest.mark.asyncio
async def test_list_pit_tasks_loads_independent_task_stats_concurrently(monkeypatch):
    monkeypatch.setattr(
        audit_service.UnifiedTaskFactory,
        "get_tasks_by_type",
        lambda task_type: {
            "fake_task": _FakeAuditTask,
            "second_fake_task": _SecondFakeAuditTask,
        },
    )
    service = PITAuditService(_FakeDB())
    active = 0
    max_active = 0

    async def _table_stats(contract):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0)
        active -= 1
        return {
            "latest_pit_time": date(2026, 8, 12),
            "row_count": 123,
            "coverage_rate": 0.95,
            "gap_count": 5,
            "status": "healthy",
        }

    async def _empty_lookup(task_name):
        return None

    monkeypatch.setattr(service, "_table_stats", _table_stats)
    monkeypatch.setattr(service, "_latest_snapshot_for_task", _empty_lookup)
    monkeypatch.setattr(service, "_latest_task_execution_for_task", _empty_lookup)

    tasks = await service.list_pit_tasks()

    assert [task["name"] for task in tasks] == ["fake_task", "second_fake_task"]
    assert max_active == 2


class _LatestPeriodCoverageDB:
    def __init__(self):
        self.fetch_queries = []
        self.fetch_one_queries = []

    async def fetch(self, query, *args, **kwargs):
        self.fetch_queries.append((query, args))
        if "information_schema.columns" in query:
            return [
                {"column_name": "ts_code"},
                {"column_name": "end_date"},
                {"column_name": "ann_date"},
                {"column_name": "data_source"},
            ]
        return []

    async def fetch_one(self, query, *args, **kwargs):
        self.fetch_one_queries.append((query, args))
        if "pit_coverage_candidate_period" in query:
            if not args:
                return {"coverage_period": date(2026, 6, 30)}
            if args[0] == date(2026, 6, 30):
                return {"coverage_period": date(2025, 12, 31)}
            return {"coverage_period": None}
        if "pit_coverage_count" in query:
            period = args[0]
            counts = {
                date(2026, 6, 30): 2,
                date(2025, 12, 31): 8,
            }
            return {"coverage_count": counts[period]}
        return None


@pytest.mark.asyncio
async def test_coverage_for_latest_counts_only_candidate_periods_until_threshold():
    db = _LatestPeriodCoverageDB()
    service = PITAuditService(db)

    coverage = await service._coverage_for_latest(
        _FakeAuditTask.contract,
        "end_date",
        min_coverage_count=5,
    )

    assert coverage == {
        "coverage_period": date(2025, 12, 31),
        "coverage_count": 8,
    }
    count_queries = [
        (query, args)
        for query, args in db.fetch_one_queries
        if "pit_coverage_count" in query
    ]
    assert [args[0] for _, args in count_queries] == [
        date(2026, 6, 30),
        date(2025, 12, 31),
    ]
    assert all('t."end_date" = $1' in query for query, _ in count_queries)
    assert all("GROUP BY t.\"end_date\"" not in query for query, _ in count_queries)


class _SourceMissingDB:
    async def fetch_one(self, query, *args, **kwargs):
        if "COUNT(*)::bigint AS cnt" in query:
            return {"cnt": 0}
        return None


@pytest.mark.asyncio
async def test_financial_gap_diagnosis_marks_missing_cashflow_source(monkeypatch):
    service = PITAuditService(_SourceMissingDB())

    async def _relation_exists(relation):
        return relation in {"pit.pit_cashflow_quarterly", "tushare.fina_cashflow", "rawdata.fina_cashflow"}

    async def _get_columns(relation):
        if relation == "pit.pit_cashflow_quarterly":
            return {"ts_code", "end_date", "ann_date", "data_source", "n_cashflow_act"}
        if relation in {"tushare.fina_cashflow", "rawdata.fina_cashflow"}:
            return {"ts_code", "end_date", "ann_date", "f_ann_date", "report_type", "n_cashflow_act"}
        return set()

    monkeypatch.setattr(service, "_relation_exists", _relation_exists)
    monkeypatch.setattr(service, "_get_columns", _get_columns)

    result = await service._diagnose_financial_gap_reasons(
        _CashflowTaskContract,
        "002549.SZ",
        [date(2022, 6, 30)],
    )

    assert result[0]["period"] == date(2022, 6, 30)
    assert result[0]["reason"] == "source_missing"
    assert {item["source_table"] for item in result[0]["source_checks"]} == {
        "tushare.fina_cashflow",
        "rawdata.fina_cashflow",
    }


class _IndustryAuditDB:
    def __init__(self, state):
        self.state = state
        self.queries = []

    async def execute(self, query, *args, **kwargs):
        return "OK"

    async def fetch_one(self, query, *args, **kwargs):
        self.queries.append(query)
        if "information_schema.tables" in query:
            schema, table = args
            if (schema, table) == ("pit", "pit_industry_fttm_monthly"):
                return {"exists": self.state != "missing"}
            return {"exists": True}
        if "COUNT(*)::bigint AS row_count" in query:
            if self.state == "empty":
                return {"row_count": 0, "latest_pit_time": None}
            return {"row_count": 2, "latest_pit_time": date(2026, 7, 31)}
        if "pit_coverage_candidate_period" in query:
            if self.state == "empty":
                return {"coverage_period": None}
            return {"coverage_period": date(2026, 7, 31)}
        if "pit_coverage_count" in query:
            return {"coverage_count": 2}
        if "WITH structural AS" in query:
            return {"cnt": 2}
        if "inconsistent_code_name_groups" in query:
            return {"inconsistent_code_name_groups": 0}
        return None

    async def fetch(self, query, *args, **kwargs):
        self.queries.append(query)
        if "information_schema.columns" in query:
            return [
                {"column_name": column}
                for column in (
                    "obs_date",
                    "classification_source",
                    "industry_level",
                    "industry_code",
                    "weight_basis",
                    "industry_name",
                    "industry_fttm_np",
                    "is_eligible",
                    "quality_reasons",
                    "source_max_report_date",
                    "diffusion_up",
                    "matched_org_count",
                    "up_org_count",
                    "down_or_flat_org_count",
                    "covered_mv_rate",
                    "org_count",
                    "median_org_mv_coverage",
                    "p25_org_mv_coverage",
                )
            ]
        if "structural_industries" in query:
            valued = 0 if self.state == "structure_only" else 1
            return [
                {
                    "industry_level": level,
                    "structural_industries": 1,
                    "valued_industries": valued,
                    "eligible_industries": valued,
                }
                for level in ("L1", "L2")
            ]
        return []


class _IndustryAuditTask:
    task_type = "pit"
    contract = PITTaskContract(
        task_name="pit_industry_fttm_monthly",
        domain="industry_fttm",
        source_tables=("pit.pit_industry_classification",),
        output_table="pit.pit_industry_fttm_monthly",
        pit_time_key="obs_date",
        primary_keys=(
            "obs_date",
            "classification_source",
            "industry_level",
            "industry_code",
            "weight_basis",
        ),
        dependencies=(),
        supported_modes=("audit_only",),
        manager_class=_FakeManager,
        audit_entity_keys=(
            "classification_source",
            "industry_level",
            "industry_code",
            "weight_basis",
        ),
        audit_denominator="pit_time_structural_industries",
    )


@pytest.mark.parametrize(
    ("state", "expected_status"),
    [
        ("missing", "missing_table"),
        ("empty", "empty"),
        ("structure_only", "structure_only"),
        ("normal", "healthy"),
    ],
)
@pytest.mark.asyncio
async def test_industry_audit_handles_all_table_states_without_fake_ts_code(
    monkeypatch, state, expected_status
):
    monkeypatch.setattr(
        audit_service.UnifiedTaskFactory,
        "get_tasks_by_type",
        lambda task_type: {"pit_industry_fttm_monthly": _IndustryAuditTask},
    )
    db = _IndustryAuditDB(state)

    result = await PITAuditService(db).audit_task(
        "pit_industry_fttm_monthly", persist=False
    )

    assert result["status"] == expected_status
    assert all("t.ts_code" not in query for query in db.queries)
    if state in {"structure_only", "normal"}:
        assert result["coverage_rate"] == 1.0
        assert {row["industry_level"] for row in result["details"]["domain_metrics"]["levels"]} == {
            "L1",
            "L2",
        }


class _StockPITTimeAuditDB:
    def __init__(self):
        self.queries = []

    async def fetch_one(self, query, *args, **kwargs):
        self.queries.append(query)
        if "information_schema.tables" in query:
            return {"exists": True}
        if "COUNT(*)::bigint AS row_count" in query:
            return {"row_count": 20, "latest_pit_time": date(2020, 1, 31)}
        if "pit_coverage_candidate_period" in query:
            return {"coverage_period": date(2020, 1, 31)}
        if "pit_coverage_count" in query:
            return {"coverage_count": 4}
        if "COUNT(DISTINCT ts_code)::bigint AS cnt" in query:
            return {"cnt": 5}
        return None

    async def fetch(self, query, *args, **kwargs):
        self.queries.append(query)
        if "information_schema.columns" in query:
            return [
                {"column_name": "obs_date"},
                {"column_name": "ts_code"},
                {"column_name": "org_name"},
            ]
        return []


@pytest.mark.asyncio
async def test_stock_fttm_coverage_uses_active_stocks_at_audited_obs_date():
    contract = PITTaskContract(
        task_name="stock_fttm",
        domain="stock_fttm",
        source_tables=("rawdata.stock_report_rc",),
        output_table="pit.stock_fttm",
        pit_time_key="obs_date",
        primary_keys=("ts_code", "org_name", "obs_date"),
        dependencies=(),
        supported_modes=("audit_only",),
        manager_class=_FakeManager,
        audit_entity_keys=("ts_code",),
        audit_denominator="pit_time_active_stocks",
    )
    db = _StockPITTimeAuditDB()

    stats = await PITAuditService(db)._table_stats(contract)

    assert stats["coverage_rate"] == 0.8
    coverage_sql = next(query for query in db.queries if "pit_coverage_count" in query)
    assert "active.list_date <= t.\"obs_date\"" in coverage_sql
    assert "active.delist_date > t.\"obs_date\"" in coverage_sql


@pytest.mark.asyncio
async def test_stock_diagnosis_skips_non_stock_industry_rows_without_ts_code_sql():
    db = _IndustryAuditDB("normal")
    rows = await PITAuditService(db)._latest_rows_for_stock(
        _IndustryAuditTask.contract, "000001.SZ"
    )

    assert rows == []
    assert all("WHERE ts_code =" not in query for query in db.queries)


@pytest.mark.asyncio
async def test_index_fttm_denominator_counts_configured_indices_and_all_a():
    contract = PITTaskContract(
        task_name="pit_index_fttm_monthly",
        domain="index_fttm",
        source_tables=("pit.pit_stock_fttm_monthly", "rawdata.index_weight"),
        output_table="pit.pit_index_fttm_monthly",
        pit_time_key="obs_date",
        primary_keys=(
            "obs_date",
            "universe_type",
            "universe_code",
            "weight_basis",
        ),
        dependencies=("pit_stock_fttm_monthly",),
        supported_modes=("audit_only",),
        manager_class=_FakeManager,
        audit_entity_keys=("universe_type", "universe_code"),
        audit_denominator="configured_index_fttm_universes",
    )

    count = await PITAuditService(_FakeDB())._denominator_count(
        contract, date(2026, 7, 31)
    )

    assert count == 14


class _IndustryFAPIAuditDB:
    def __init__(self, valued_count: int):
        self.valued_count = valued_count

    async def fetch(self, query, *args, **kwargs):
        if "structural_industries" in query:
            return [
                {
                    "industry_level": level,
                    "structural_industries": 1,
                    "valued_industries": self.valued_count,
                    "eligible_industries": self.valued_count,
                    "ratio_eligible_industries": self.valued_count,
                    "late_source_count": 0,
                    "fapi_out_of_range_count": 0,
                    "spread_denominator_mismatch_count": 0,
                    "ratio_denominator_mismatch_count": 0,
                }
                for level in ("L1", "L2")
            ]
        return []

    async def fetch_one(self, query, *args, **kwargs):
        if "inconsistent_code_name_groups" in query:
            return {"inconsistent_code_name_groups": 0}
        return None


class _IndustryFAPIAuditTask:
    task_type = "pit"
    contract = PITTaskContract(
        task_name="pit_industry_fapi_monthly",
        domain="industry_fapi",
        source_tables=("pit.pit_stock_fttm_monthly",),
        output_table="pit.pit_industry_fapi_monthly",
        pit_time_key="obs_date",
        primary_keys=(
            "obs_date",
            "classification_source",
            "industry_level",
            "industry_code",
            "benchmark_code",
            "method_version",
        ),
        dependencies=(),
        supported_modes=("audit_only",),
        manager_class=_FakeManager,
        audit_entity_keys=(
            "classification_source",
            "industry_level",
            "industry_code",
        ),
        audit_denominator="pit_time_structural_industries",
    )


@pytest.mark.parametrize(
    ("valued_count", "expected_status"),
    [(0, "structure_only"), (1, "healthy")],
)
@pytest.mark.asyncio
async def test_industry_fapi_audit_distinguishes_structure_from_valued_rows(
    monkeypatch, valued_count, expected_status
):
    monkeypatch.setattr(
        audit_service.UnifiedTaskFactory,
        "get_tasks_by_type",
        lambda task_type: {"pit_industry_fapi_monthly": _IndustryFAPIAuditTask},
    )
    service = PITAuditService(_IndustryFAPIAuditDB(valued_count))
    monkeypatch.setattr(service, "_relation_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(
        service,
        "_table_stats",
        AsyncMock(
            return_value={
                "coverage_period": date(2026, 7, 31),
                "coverage_count": 2,
                "listed_stock_count": None,
                "denominator_name": "pit_time_structural_industries",
                "denominator_count": 2,
                "latest_pit_time": date(2026, 7, 31),
                "row_count": 2,
                "coverage_rate": 1.0,
                "gap_count": 0,
                "status": "healthy",
            }
        ),
    )
    monkeypatch.setattr(service, "_raw_gap_summary", AsyncMock(return_value={}))

    result = await service.audit_task("pit_industry_fapi_monthly", persist=False)

    assert result["status"] == expected_status
    assert {
        row["industry_level"]
        for row in result["details"]["domain_metrics"]["levels"]
    } == {"L1", "L2"}
