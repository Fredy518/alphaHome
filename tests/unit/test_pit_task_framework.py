import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.tasks import discover_tasks


class _FakeManager:
    calls = []
    exit_stats = []

    def __init__(self):
        self.stats = {
            "processed_records": 0,
            "success_records": 0,
            "error_records": 0,
            "skipped_records": 0,
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit_stats.append(dict(self.stats))
        return False

    def incremental_update(self, days=None, batch_size=None):
        self.calls.append(("incremental_update", {"days": days, "batch_size": batch_size}))
        return {"updated_records": 3}

    def full_backfill(self, start_date=None, end_date=None, batch_size=None):
        self.calls.append(
            (
                "full_backfill",
                {"start_date": start_date, "end_date": end_date, "batch_size": batch_size},
            )
        )
        return {"backfilled_records": 5}


class _FakePITTask(PITTask):
    contract = PITTaskContract(
        task_name="fake_pit_task",
        domain="financials",
        source_tables=("tushare.fake",),
        output_table="pit.fake_pit_task",
        pit_time_key="ann_date",
        primary_keys=("ts_code", "end_date", "ann_date", "data_source"),
        dependencies=(),
        supported_modes=("incremental", "full_backfill", "manual_range", "audit_only"),
        manager_class=_FakeManager,
    )


def test_pit_task_contract_serializes_manager_class_path():
    contract = _FakePITTask.contract

    payload = contract.to_dict()
    restored = PITTaskContract.from_dict(payload)

    assert payload["manager_class"].endswith("._FakeManager")
    assert restored.task_name == "fake_pit_task"
    assert restored.primary_keys == ("ts_code", "end_date", "ann_date", "data_source")
    assert restored.audit_entity_keys == ()
    assert restored.audit_denominator == "current_listed_stocks"


def test_pit_task_contract_round_trips_explicit_audit_entity_contract():
    contract = PITTaskContract(
        task_name="aggregate",
        domain="industry_fttm",
        source_tables=("pit.source",),
        output_table="pit.aggregate",
        pit_time_key="obs_date",
        primary_keys=("obs_date", "industry_level", "industry_code"),
        dependencies=("source",),
        supported_modes=("incremental", "audit_only"),
        manager_class=_FakeManager,
        audit_entity_keys=("industry_level", "industry_code"),
        audit_denominator="pit_time_structural_industries",
    )

    restored = PITTaskContract.from_dict(contract.to_dict())

    assert restored.audit_entity_keys == ("industry_level", "industry_code")
    assert restored.audit_denominator == "pit_time_structural_industries"


def test_pit_task_discovery_finds_all_fttm_tasks_without_manual_import():
    discover_tasks(force_reload=True)

    registry = UnifiedTaskFactory._task_registry
    assert "pit_stock_fttm_monthly" in registry
    assert "pit_industry_fttm_monthly" in registry
    assert "pit_industry_fapi_monthly" in registry
    assert "pit_index_fttm_monthly" in registry
    assert "pit_stock_consensus_fy_monthly" in registry
    assert "pit_earnings_surprise_annual" in registry
    assert "pit_etf_index_a_share_proxy_members_monthly" in registry
    assert "pit_etf_index_a_share_proxy_fapi_monthly" in registry

    surprise = registry["pit_earnings_surprise_annual"].contract
    assert surprise.dependencies == (
        "pit_income_quarterly",
        "pit_stock_consensus_fy_monthly",
    )
    assert surprise.audit_denominator == "annual_report_events_at_ann_date"

    fapi = registry["pit_industry_fapi_monthly"].contract
    assert fapi.dependencies == (
        "pit_stock_fttm_monthly",
        "pit_industry_classification",
    )
    assert fapi.domain == "industry_fapi"
    assert fapi.audit_entity_keys == (
        "classification_source",
        "industry_level",
        "industry_code",
    )

    proxy_fapi = registry["pit_etf_index_a_share_proxy_fapi_monthly"].contract
    assert proxy_fapi.dependencies == (
        "pit_etf_index_a_share_proxy_members_monthly",
        "pit_stock_fttm_monthly",
    )
    assert proxy_fapi.output_table == "pit.pit_etf_index_fapi_monthly"


@pytest.mark.asyncio
async def test_pit_task_dispatches_smart_to_incremental():
    _FakeManager.calls = []
    _FakeManager.exit_stats = []
    task = _FakePITTask(object(), update_type=UpdateTypes.SMART, task_config={"days": 9, "batch_size": 2})

    result = await task.execute()

    assert result["status"] == "success"
    assert result["rows"] == 3
    assert _FakeManager.calls == [("incremental_update", {"days": 9, "batch_size": 2})]
    assert _FakeManager.exit_stats == [
        {
            "processed_records": 3,
            "success_records": 3,
            "error_records": 0,
            "skipped_records": 0,
        }
    ]


@pytest.mark.asyncio
async def test_pit_task_dispatches_manual_range_to_full_backfill():
    _FakeManager.calls = []
    task = _FakePITTask(
        object(),
        update_type=UpdateTypes.MANUAL,
        start_date="2025-01-01",
        end_date="2025-12-31",
        task_config={"batch_size": 4},
    )

    result = await task.execute()

    assert result["status"] == "success"
    assert result["rows"] == 5
    assert _FakeManager.calls == [
        (
            "full_backfill",
            {"start_date": "2025-01-01", "end_date": "2025-12-31", "batch_size": 4},
        )
    ]
