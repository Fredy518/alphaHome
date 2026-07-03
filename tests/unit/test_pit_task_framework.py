import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.pit.base.pit_task import PITTask, PITTaskContract


class _FakeManager:
    calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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


@pytest.mark.asyncio
async def test_pit_task_dispatches_smart_to_incremental():
    _FakeManager.calls = []
    task = _FakePITTask(object(), update_type=UpdateTypes.SMART, task_config={"days": 9, "batch_size": 2})

    result = await task.execute()

    assert result["status"] == "success"
    assert result["rows"] == 3
    assert _FakeManager.calls == [("incremental_update", {"days": 9, "batch_size": 2})]


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
