from datetime import date

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
        if "period_counts AS" in query:
            return {"coverage_period": date(2025, 12, 31), "coverage_count": 8}
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
