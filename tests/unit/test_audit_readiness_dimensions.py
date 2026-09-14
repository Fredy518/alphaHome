from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from alphahome.common.audit_models import AuditDimensions
from alphahome.factors.audit_service import FactorAuditService
from alphahome.factors.coordinator import FactorCoordinator
from alphahome.factors.governance import SCHEMA_COLUMNS
from alphahome.factors.repair import FactorRepairService
from alphahome.pit.audit_service import PITAuditService
from test_factor_readonly_preflight import ReadOnlyDB
from test_pit_audit_service import _FakeAuditTask, _FakeDB


DAY = date(2026, 9, 11)


class AuditDB:
    def __init__(self, consumption="unverified", missing_schema=False):
        self.consumption, self.missing_schema = consumption, missing_schema

    async def execute(self, *args):
        raise AssertionError("Read-only audit attempted a write")

    async def fetch(self, sql, *args):
        if "pg_catalog.pg_attribute" in sql:
            return [] if self.missing_schema else [
                {"table_schema": table.split(".")[0], "table_name": table.split(".")[1], "column_name": column}
                for table, columns in SCHEMA_COLUMNS.items() for column in columns
            ]
        return []

    async def fetch_one(self, sql, *args):
        if "AS distinct_date_count" in sql:
            return {"row_count": 10, "distinct_date_count": 1, "first_calc_date": DAY,
                    "actual_latest_date": DAY, "nonstandard_date_count": 0}
        if "source_watermarks" in sql:
            if self.consumption == "unverified":
                return None
            return {"run_id": "fixture", "finished_at": datetime(2026, 9, 11, tzinfo=timezone.utc),
                    "source_watermarks": {"factor_g": {"factors.p_factor": "2026-09-11T00:00:00+00:00", "_snapshot_xmin": 100}}}
        if "FROM public.task_status" in sql:
            return {"status": "success", "update_time": datetime(2026, 9, 1, tzinfo=timezone.utc)}
        return {}

    async def fetch_val(self, sql, *args):
        if "to_regclass" in sql:
            return True
        if "pg_snapshot_xmax" in sql:
            return "110"
        if "MIN(calc_date)" in sql:
            return DAY if self.consumption == "changed" else None
        return 10


@pytest.mark.asyncio
@pytest.mark.parametrize("consumption,status", [("unverified", "consumption_unverified"), ("changed", "source_unconsumed"), ("current", "healthy")])
async def test_factor_health_requires_separate_consumption_proof(consumption, status):
    service = FactorAuditService(AuditDB(consumption))
    contract = FactorCoordinator.contracts()["factor_g"]
    service._contracts = lambda: {"factor_g": (SimpleNamespace(description="fixture"), contract)}
    service.date_policy = SimpleNamespace(automatic_cutoff=lambda: DAY)
    service._dependency_status = AsyncMock(return_value=("ready", {}))
    service._coverage_denominator = AsyncMock(return_value=10)
    result = await service.audit_task("factor_g", persist=False)
    assert result["status"] == status
    assert result["dimensions"]["dates"] == "complete"
    assert result["dimensions"]["source_consumption"] == consumption
    assert not result["persisted"]
    assert result["last_execution_time"] < result["audited_at"]


@pytest.mark.asyncio
async def test_factor_missing_audit_schema_returns_migration_without_ddl():
    service = FactorAuditService(AuditDB(missing_schema=True))
    result = await service.audit_task("factor_p", persist=False)
    assert result["status"] == "migration_required"
    assert result["dimensions"]["structure"] == "migration_required"


def test_repair_preview_never_installs_missing_schema():
    db = ReadOnlyDB(missing_table="factors.factor_run")
    result = FactorRepairService(db).plan(DAY)
    assert result["status"] == "migration_required"


@pytest.mark.asyncio
async def test_pit_persistence_requires_an_explicit_schema_migration(monkeypatch):
    db = _FakeDB()
    service = PITAuditService(db)
    service._pit_task_classes = lambda: {"fake_task": _FakeAuditTask}
    original = service._relation_exists

    async def exists(name):
        return False if name == "pit.pit_audit_snapshot" else await original(name)

    service._relation_exists = exists
    with pytest.raises(RuntimeError, match="migration_required"):
        await service.audit_task("fake_task", persist=True)
    assert db.executed == []


def test_unknown_dimensions_are_not_healthy():
    assert not AuditDimensions(structure="ready", dates="complete", coverage="complete").healthy
    assert AuditDimensions("ready", "complete", "current", "complete", "qualified").healthy
