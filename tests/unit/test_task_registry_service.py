from unittest.mock import AsyncMock
from datetime import datetime

import pytest

from alphahome.gui.services import task_registry_service
from alphahome.gui.utils.common import format_datetime_for_display


class _VisibleFetchTask:
    task_type = "fetch"
    data_source = "tinysoft"
    description = "visible task"
    table_name = "visible_table"

    def get_business_domain(self):
        return "fund"


class _HiddenFetchTask(_VisibleFetchTask):
    hide_from_gui = True
    description = "hidden task"
    table_name = "hidden_table"


@pytest.mark.asyncio
async def test_collection_task_list_hides_tasks_marked_hide_from_gui(monkeypatch):
    events = []

    monkeypatch.setattr(
        task_registry_service,
        "get_tasks_by_type",
        lambda task_type: {
            "visible_task": _VisibleFetchTask,
            "hidden_task": _HiddenFetchTask,
        },
    )
    get_task = AsyncMock(return_value=_VisibleFetchTask())
    monkeypatch.setattr(task_registry_service.UnifiedTaskFactory, "get_task", get_task)
    monkeypatch.setattr(
        task_registry_service,
        "_update_tasks_with_latest_timestamp",
        AsyncMock(),
    )

    task_registry_service._collection_task_cache = []
    task_registry_service.initialize_task_registry(lambda event, payload: events.append((event, payload)))

    await task_registry_service.handle_get_collection_tasks()

    task_names = [task["name"] for task in task_registry_service.get_cached_collection_tasks()]
    assert task_names == ["visible_task"]
    get_task.assert_awaited_once_with("visible_task")
    assert any(event == "COLLECTION_TASK_LIST_UPDATE" for event, _ in events)


@pytest.mark.asyncio
async def test_update_tasks_with_latest_timestamp_uses_batch_query(monkeypatch):
    latest_time = datetime(2026, 5, 22, 13, 30, 0)

    class _Resolver:
        def get_schema_and_table(self, target):
            return target.get("data_source") or "public", target["table_name"]

    class _DB:
        resolver = _Resolver()

        def __init__(self):
            self.fetch_calls = []

        async def fetch(self, query, *args):
            self.fetch_calls.append((query, args))
            if "information_schema.columns" in query:
                return [
                    {
                        "schema_name": "tinysoft",
                        "table_name": "present_table",
                        "table_exists": True,
                        "has_update_time": True,
                    },
                    {
                        "schema_name": "tinysoft",
                        "table_name": "missing_table",
                        "table_exists": False,
                        "has_update_time": False,
                    },
                    {
                        "schema_name": "tinysoft",
                        "table_name": "no_update_time_table",
                        "table_exists": True,
                        "has_update_time": False,
                    },
                ]
            return [{"idx": 0, "latest_time": latest_time}]

    db = _DB()
    monkeypatch.setattr(task_registry_service.UnifiedTaskFactory, "get_db_manager", lambda: db)
    cache = [
        {"name": "present_a", "data_source": "tinysoft", "table_name": "present_table"},
        {"name": "present_b", "data_source": "tinysoft", "table_name": "present_table"},
        {"name": "missing", "data_source": "tinysoft", "table_name": "missing_table"},
        {"name": "no_update", "data_source": "tinysoft", "table_name": "no_update_time_table"},
        {"name": "no_table", "data_source": "tinysoft", "table_name": None},
    ]

    await task_registry_service._update_tasks_with_latest_timestamp(cache)

    assert cache[0]["latest_update_time"] == format_datetime_for_display(latest_time)
    assert cache[1]["latest_update_time"] == format_datetime_for_display(latest_time)
    assert cache[2]["latest_update_time"] == "无数据"
    assert cache[3]["latest_update_time"] == "无数据"
    assert cache[4]["latest_update_time"] == "无对应表"
    assert len(db.fetch_calls) == 2
