from types import SimpleNamespace
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.tasks.stock.tushare_stock_thsindex import (
    TushareStockThsIndexTask,
)


class _SnapshotDB:
    def __init__(self):
        self.snapshot = pd.DataFrame({"ts_code": ["883399.TI"]})
        self.replace_calls = []

    async def replace_from_dataframe(self, **kwargs):
        self.replace_calls.append(kwargs)
        self.snapshot = kwargs["df"].copy(deep=True)
        return len(self.snapshot)


@pytest.fixture
def snapshot_data():
    return pd.DataFrame(
        {
            "ts_code": ["883400.TI", "883401.TI"],
            "name": ["测试指数甲", "测试指数乙"],
            "count": [3, 5],
            "exchange": ["A", "A"],
            "list_date": [pd.Timestamp("2020-01-01")] * 2,
            "type": ["N", "N"],
        }
    )


def _make_task(monkeypatch, data):
    db = _SnapshotDB()
    api = SimpleNamespace(query=AsyncMock(return_value=data))
    task = TushareStockThsIndexTask(
        db_connection=db,
        api_token="test-token",
        api=api,
        update_type=UpdateTypes.FULL,
        task_config={"stream_batches": True},
    )
    monkeypatch.setattr(task, "_ensure_table_exists", AsyncMock())
    return task, db, api


@pytest.mark.parametrize("validation_mode", ["report", "filter"])
@pytest.mark.parametrize("problem", ["negative_count", "empty_name", "missing_column"])
@pytest.mark.asyncio
async def test_thsindex_execute_preserves_snapshot_on_validation_failure(
    monkeypatch, snapshot_data, validation_mode, problem
):
    if problem == "negative_count":
        snapshot_data.loc[0, "count"] = -1
    elif problem == "empty_name":
        snapshot_data.loc[0, "name"] = " "
    else:
        snapshot_data = snapshot_data.drop(columns="exchange")
    task, db, api = _make_task(monkeypatch, snapshot_data)
    task.validation_mode = validation_mode
    previous_snapshot = db.snapshot.copy(deep=True)

    result = await task.execute(stream_batches=True)

    assert result["status"] == "error"
    assert "完整快照校验失败" in result["error"]
    assert "保留旧快照" in result["error"]
    assert db.replace_calls == []
    pd.testing.assert_frame_equal(db.snapshot, previous_snapshot)
    task._ensure_table_exists.assert_not_awaited()
    api.query.assert_awaited_once()


@pytest.mark.parametrize("unknown_count", [False, True])
@pytest.mark.asyncio
async def test_thsindex_execute_replaces_valid_snapshot_once(
    monkeypatch, snapshot_data, unknown_count
):
    if unknown_count:
        snapshot_data.loc[0, "count"] = np.nan
    task, db, api = _make_task(monkeypatch, snapshot_data)

    result = await task.execute(stream_batches=True)

    assert result["status"] == "success"
    assert result["validation"] is True
    assert result["rows"] == len(snapshot_data)
    assert len(db.replace_calls) == 1
    pd.testing.assert_frame_equal(db.snapshot, snapshot_data)
    task._ensure_table_exists.assert_awaited_once()
    api.query.assert_awaited_once()


@pytest.mark.parametrize("data", [None, pd.DataFrame()], ids=["none", "empty"])
@pytest.mark.asyncio
async def test_thsindex_execute_preserves_snapshot_when_source_is_empty(
    monkeypatch, data
):
    task, db, api = _make_task(monkeypatch, data)
    previous_snapshot = db.snapshot.copy(deep=True)

    result = await task.execute(stream_batches=True)

    assert result["status"] == "no_data"
    assert db.replace_calls == []
    pd.testing.assert_frame_equal(db.snapshot, previous_snapshot)
    task._ensure_table_exists.assert_not_awaited()
    api.query.assert_awaited_once()
