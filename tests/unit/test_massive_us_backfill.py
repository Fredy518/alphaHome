from unittest.mock import AsyncMock, Mock

import pytest

from scripts.production import backfill_massive_us as backfill


def test_completed_dates_does_not_hide_old_holes_or_invalid_snapshots():
    rows = {
        "2024-09-30": {"rows": 14000, "duplicate_keys": 0, "invalid_rows": 0},
        "2024-10-01": {"rows": 14000, "duplicate_keys": 0, "invalid_rows": 1},
        "2024-10-02": {"rows": 20, "duplicate_keys": 0, "invalid_rows": 0},
        "2024-10-03": {"rows": 14000, "duplicate_keys": 1, "invalid_rows": 0},
        "2026-09-25": {"rows": 14000, "duplicate_keys": 0, "invalid_rows": 0},
    }
    assert backfill.completed_dates(rows, list(rows)) == {"2024-09-30", "2026-09-25"}
    assert backfill.completed_dates(rows, ["2024-09-30"]) == {"2024-09-30"}


def test_frozen_plan_rejects_tampering_and_changed_code(monkeypatch):
    monkeypatch.setattr(backfill, "code_hashes", lambda: {"collector.py": "original"})
    plan = {"code_hashes": backfill.code_hashes(), "end_date": "2026-09-25"}
    plan["plan_hash"] = backfill.digest(plan)
    backfill.validate_plan(plan, plan["plan_hash"])
    with pytest.raises(RuntimeError, match="plan_hash_mismatch"):
        backfill.validate_plan(plan, "wrong")
    altered = {**plan, "end_date": "2026-09-26"}
    with pytest.raises(RuntimeError, match="plan_hash_mismatch"):
        backfill.validate_plan(altered, plan["plan_hash"])
    monkeypatch.setattr(backfill, "code_hashes", lambda: {"collector.py": "changed"})
    with pytest.raises(RuntimeError, match="source_changed"):
        backfill.validate_plan(plan, plan["plan_hash"])


def task_factory(monkeypatch, result, reason=None):
    task = Mock(name="collector")
    task.name = "massive_stock_us_daily"
    task.table_name = "stock_us_daily"
    task.date_column = "trade_date"
    task.execute = AsyncMock(return_value=result)
    task._smart_skip_reason = reason
    factory = Mock(return_value=task)
    monkeypatch.setitem(backfill.TASKS, "daily", factory)
    monkeypatch.setitem(backfill.TASKS, "split", factory)
    return factory


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["partial_success", "error", "cancelled", "no_data"])
async def test_unsuccessful_day_cannot_be_receipted(monkeypatch, status):
    task_factory(monkeypatch, {"status": status, "rows": 14000})
    db = Mock(fetch_val=AsyncMock(return_value=14000))
    with pytest.raises(RuntimeError, match="failed"):
        await backfill.collect_one(db, "daily", "2024-09-30", "2024-09-30")
    db.fetch_val.assert_not_awaited()


@pytest.mark.asyncio
async def test_successful_task_requires_actual_committed_count(monkeypatch):
    task_factory(monkeypatch, {"status": "success", "rows": 14000})
    db = Mock(fetch_val=AsyncMock(return_value=13999))
    with pytest.raises(RuntimeError, match="row count mismatch"):
        await backfill.collect_one(db, "daily", "2024-09-30", "2024-09-30")
    db.fetch_val.return_value = 14000
    receipt = await backfill.collect_one(db, "daily", "2024-09-30", "2024-09-30")
    assert receipt["rows"] == 14000
    assert receipt["start"] == "2024-09-30"


@pytest.mark.asyncio
async def test_empty_event_window_requires_explicit_source_empty_reason(monkeypatch):
    task_factory(monkeypatch, {"status": "no_data", "rows": 0})
    db = Mock(fetch_val=AsyncMock(return_value=0))
    with pytest.raises(RuntimeError):
        await backfill.collect_one(db, "split", "2024-09-30", "2024-09-30")
    task_factory(
        monkeypatch,
        {"status": "no_data", "rows": 0},
        "Massive 成功返回该窗口无拆股事件",
    )
    result = await backfill.collect_one(db, "split", "2024-09-30", "2024-09-30")
    assert result["rows"] == 0


@pytest.mark.asyncio
async def test_missing_backup_blocks_resume_with_receipts(tmp_path):
    (tmp_path / "receipts.jsonl").write_text("{}\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Backup missing"):
        await backfill.backup_targets(Mock(), tmp_path, {"plan_hash": "frozen"})


@pytest.mark.asyncio
async def test_changed_snapshot_blocks_backup_and_execution(tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "coverage", AsyncMock(return_value={"changed": True}))
    with pytest.raises(RuntimeError, match="changed_since_preview"):
        await backfill.backup_targets(
            Mock(), tmp_path, {"plan_hash": "frozen", "initial_coverage": {}}
        )
