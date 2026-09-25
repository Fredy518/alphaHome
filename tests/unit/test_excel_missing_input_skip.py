from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.sources.excel.input_file import missing_excel_input_reason
from alphahome.fetchers.tasks.fastrategy.excel_fastrategy_basic import (
    ExcelFastategyBasicTask,
)
from alphahome.gui.services import task_execution_service
from scripts.production.data_updaters.tushare import (
    data_collection_smart_update_production as production,
)


@pytest.mark.asyncio
async def test_excel_task_missing_file_skips_before_fetch(tmp_path, monkeypatch):
    missing = tmp_path / "missing.xlsx"
    task = ExcelFastategyBasicTask(
        db_connection=object(),
        excel_file_path=str(missing),
        update_type=UpdateTypes.FULL,
    )
    fetch = AsyncMock()
    monkeypatch.setattr(task, "_fetch_data", fetch)

    result = await task.execute()

    assert result["status"] == "expected_skip"
    assert str(missing) in result["reason"]
    fetch.assert_not_awaited()


def test_missing_input_resolves_macro_workbook_and_ignores_existing_file(tmp_path):
    missing = tmp_path / "macro.xlsx"
    macro = SimpleNamespace(data_source="excel", workbook_path=missing)
    assert str(missing) in missing_excel_input_reason(macro)

    missing.touch()
    assert missing_excel_input_reason(macro) is None
    assert missing_excel_input_reason(SimpleNamespace(data_source="excel")) is None


@pytest.mark.asyncio
async def test_gui_missing_excel_is_skipped_without_execute(tmp_path, monkeypatch):
    missing = tmp_path / "missing.xlsx"
    task = SimpleNamespace(
        data_source="excel", workbook_path=missing, execute=AsyncMock()
    )
    record = AsyncMock()
    monkeypatch.setattr(task_execution_service, "_is_running", False)
    monkeypatch.setattr(
        task_execution_service, "_ensure_task_status_table_exists", AsyncMock()
    )
    monkeypatch.setattr(task_execution_service, "_record_task_status", record)
    monkeypatch.setattr(task_execution_service, "get_all_task_status", AsyncMock())
    monkeypatch.setattr(
        task_execution_service.UnifiedTaskFactory,
        "create_task_instance",
        AsyncMock(return_value=task),
    )

    await task_execution_service.run_tasks(
        object(), [{"task_name": "excel_missing"}], None, None, "全量更新"
    )

    task.execute.assert_not_awaited()
    assert any(
        call.args[1:3] == ("excel_missing", "skipped")
        for call in record.await_args_list
    )
    assert not any(call.args[2] == "running" for call in record.await_args_list)


@pytest.mark.asyncio
async def test_production_missing_macro_workbook_skips_without_retry(
    tmp_path, monkeypatch
):
    task = SimpleNamespace(
        data_source="excel",
        workbook_path=tmp_path / "macro.xlsx",
        supports_incremental_update=lambda: True,
        execute=AsyncMock(),
    )
    monkeypatch.setattr(
        production.UnifiedTaskFactory,
        "create_task_instance",
        AsyncMock(return_value=task),
    )
    updater = production.DataCollectionProductionUpdater(max_workers=1, max_retries=2)
    try:
        result = await updater.execute_task_with_retry("excel_macro_dr007_history")
        assert result["status"] == "expected_skip"
        assert result["attempts"] == 1
        assert updater.evaluate_batch(["excel_macro_dr007_history"], [result])
        task.execute.assert_not_awaited()
    finally:
        updater.executor.shutdown(wait=True)
