import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from alphahome.pit.base.pit_table_manager import PITTableManager
from alphahome.pit.context import PITContext
from test_pit_task_framework import _FakeManager, _FakePITTask


def test_borrowed_context_does_not_close_external_manager():
    db = Mock()
    with PITContext(db_manager=db) as context:
        assert context.db_manager is db
    context.close()
    db.close_sync.assert_not_called()


def test_context_rejects_cross_thread_use_and_close():
    context = PITContext(db_manager=Mock())
    with ThreadPoolExecutor(1) as executor:
        for action in (lambda: context.db_manager, context.close):
            with pytest.raises(RuntimeError, match="creating thread"):
                executor.submit(action).result()
    context.close()


class _ProbeManager(PITTableManager):
    def __init__(self):
        super().__init__("pit_income_quarterly")

    def full_backfill(self, **kwargs):
        return {}

    def incremental_update(self, **kwargs):
        return {}


@pytest.mark.parametrize("failure", ["enter", "exit", None])
def test_owned_context_cleanup_and_no_implicit_ddl(monkeypatch, failure):
    db = Mock()
    factory = Mock(return_value=db)
    monkeypatch.setattr(PITContext, "_create_db_manager", factory)
    manager = _ProbeManager().bind_database(database_url="explicit-target")
    if failure == "enter":
        manager._setup_logging = Mock(side_effect=RuntimeError("enter failed"))
    elif failure == "exit":
        manager._log_execution_stats = Mock(side_effect=RuntimeError("exit failed"))
    if failure:
        with pytest.raises(RuntimeError, match="failed"):
            with manager:
                pass
    else:
        with manager:
            pass
    factory.assert_called_once_with("explicit-target")
    db.close_sync.assert_called_once()
    db.execute_sync.assert_not_called()


@pytest.mark.asyncio
async def test_entire_manager_lifetime_runs_in_one_worker_with_explicit_target():
    events = []

    class Manager(_FakeManager):
        def __init__(self):
            super().__init__()
            events.append(("construct", threading.get_ident()))

        def bind_database(self, **kwargs):
            assert kwargs == {"database_url": "explicit-target"}
            events.append(("bind", threading.get_ident()))

        def __enter__(self):
            events.append(("enter", threading.get_ident()))
            return self

        def incremental_update(self, **kwargs):
            events.append(("run", threading.get_ident()))
            return {"updated_records": 2}

        def __exit__(self, *args):
            events.append(("exit", threading.get_ident()))

    task = _FakePITTask(SimpleNamespace(connection_string="explicit-target"))
    task.contract = replace(task.contract, manager_class=Manager)
    result = await task.execute()
    assert result["committed_rows"] == 2
    assert [event for event, _ in events] == ["construct", "bind", "enter", "run", "exit"]
    assert len({thread for _, thread in events}) == 1
    assert events[0][1] != threading.get_ident()


@pytest.mark.asyncio
async def test_cancel_waits_for_worker_commit_and_close():
    started, release, closed = (threading.Event() for _ in range(3))

    class Manager(_FakeManager):
        def incremental_update(self, **kwargs):
            started.set()
            assert release.wait(5)
            return {"updated_records": 2}

        def __exit__(self, *args):
            closed.set()

    task = _FakePITTask(object())
    task.contract = replace(task.contract, manager_class=Manager)
    execution = asyncio.create_task(task.execute())
    try:
        for _ in range(200):
            if started.is_set():
                break
            await asyncio.sleep(0.005)
        assert started.is_set()
        execution.cancel()
        await asyncio.sleep(0.02)
        execution.cancel()
        await asyncio.sleep(0.02)
        assert not execution.done()
        assert not closed.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await execution
    assert closed.is_set()
    assert task.last_execution_result["committed_rows"] == 2
    assert task.last_execution_result["cancel_requested"]
