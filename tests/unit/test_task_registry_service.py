from unittest.mock import AsyncMock

import pytest

from alphahome.gui.services import task_registry_service


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
