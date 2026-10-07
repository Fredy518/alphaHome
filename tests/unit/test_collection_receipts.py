import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts.production.data_updaters.tushare import data_collection_smart_update_production as production


def test_embedded_entrypoint_records_receipts_by_default():
    updater = production.DataCollectionProductionUpdater(max_workers=1)
    try:
        assert updater.receipt_dir == production.PROJECT_ROOT / "logs" / "collection_runs"
        assert updater.trigger_origin == "unspecified"
    finally:
        updater.executor.shutdown(wait=True)


def configured_updater(monkeypatch, tmp_path, results, **options):
    updater = production.DataCollectionProductionUpdater(
        max_workers=1, receipt_dir=tmp_path, trigger_origin="manual", **options
    )
    updater.initialize = AsyncMock(return_value=True)
    updater.get_fetch_tasks = AsyncMock(return_value=["input_a", "input_b"])
    updater.execute_tasks_parallel = AsyncMock(return_value=results)
    monkeypatch.setattr(updater, "print_execution_summary", lambda _: None)
    monkeypatch.setattr(production.UnifiedTaskFactory, "_task_registry", {
        name: SimpleNamespace(data_source="fixture") for name in ("input_a", "input_b")
    })
    return updater


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [("success", True), ("partial_success", False), ("cancelled", False)])
async def test_receipt_records_whole_required_batch(monkeypatch, tmp_path, status, expected):
    updater = configured_updater(monkeypatch, tmp_path, [
        {"task_name": "input_a", "status": "success", "attempts": 2},
        {"task_name": "input_b", "status": status},
    ])
    assert await updater.run_production_update() is expected
    receipt = json.loads(updater.receipt_path.read_text(encoding="utf-8"))
    assert receipt["terminal"] is True
    assert receipt["exit_code"] == int(not expected)
    assert receipt["collection_success"] is expected
    assert receipt["trigger_origin"] == "manual"
    assert receipt["trigger_origin_verified"] is False
    assert receipt["batch_outcome"]["required_tasks"] == ["input_a", "input_b"]
    assert receipt["results"][0]["attempts"] == 2
    assert receipt["source_consumption"] == "unverified"
    assert receipt["started_at"].endswith("+00:00")
    assert receipt["finished_at"] >= receipt["started_at"]


@pytest.mark.asyncio
async def test_missing_optional_result_does_not_certify_complete_batch(monkeypatch, tmp_path):
    updater = configured_updater(monkeypatch, tmp_path,
        [{"task_name": "input_a", "status": "success"}], optional_tasks=["input_b"])
    assert await updater.run_production_update() is False
    record = json.loads(updater.receipt_path.read_text(encoding="utf-8"))
    assert record["batch_outcome"]["blocking_tasks"] == ["input_b"]


@pytest.mark.asyncio
async def test_dry_run_is_never_collection_success(monkeypatch, tmp_path):
    updater = configured_updater(monkeypatch, tmp_path, [
        {"task_name": name, "status": "skipped_dry_run"} for name in ("input_a", "input_b")
    ], dry_run=True)
    assert await updater.run_production_update() is True
    record = json.loads(updater.receipt_path.read_text(encoding="utf-8"))
    assert record["batch_outcome"]["status"] == "dry_run"
    assert record["collection_success"] is False
    assert record["target_fingerprint"] is None


@pytest.mark.asyncio
async def test_initialization_failure_and_cancellation_leave_terminal_receipt(monkeypatch, tmp_path):
    updater = configured_updater(monkeypatch, tmp_path, [])
    updater.initialize = AsyncMock(return_value=False)
    assert await updater.run_production_update() is False
    record = json.loads(updater.receipt_path.read_text(encoding="utf-8"))
    assert record["terminal"] and record["batch_outcome"]["status"] == "failed"
    updater = configured_updater(monkeypatch, tmp_path, [])
    updater.execute_tasks_parallel = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await updater.run_production_update()
    record = json.loads(updater.receipt_path.read_text(encoding="utf-8"))
    assert record["batch_outcome"]["status"] == "cancelled"
    assert record["collection_success"] is False


@pytest.mark.asyncio
async def test_receipt_failure_does_not_abort_or_rollback_collection(monkeypatch, tmp_path):
    updater = configured_updater(monkeypatch, tmp_path, [
        {"task_name": name, "status": "success"} for name in ("input_a", "input_b")
    ])
    monkeypatch.setattr(production, "write_receipt", lambda *_: (_ for _ in ()).throw(PermissionError()))
    assert await updater.run_production_update() is True
    updater.execute_tasks_parallel.assert_awaited_once()
    assert updater.receipt_recorded is False
    assert updater.receipt_error == "PermissionError"


@pytest.mark.asyncio
async def test_receipt_excludes_credentials_and_arbitrary_payload(monkeypatch, tmp_path):
    updater = configured_updater(monkeypatch, tmp_path, [
        {"task_name": "input_a", "status": "expected_no_data", "result": {
            "reason": "already covered token=abcsecret postgresql://u:secret@host/db",
            "raw_api_payload": {"password": "privatevalue"},
        }},
        {"task_name": "input_b", "status": "error", "error": "password=hiddenvalue"},
    ])
    assert await updater.run_production_update() is False
    receipt_text = updater.receipt_path.read_text(encoding="utf-8")
    for secret in ("abcsecret", "u:secret", "privatevalue", "hiddenvalue", "raw_api_payload"):
        assert secret not in receipt_text
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.asyncio
async def test_cleanup_failure_is_not_success(monkeypatch, tmp_path):
    updater = configured_updater(monkeypatch, tmp_path, [
        {"task_name": name, "status": "success"} for name in ("input_a", "input_b")
    ])
    updater.db_manager = SimpleNamespace(close=AsyncMock(side_effect=RuntimeError("secret")))
    updater._owns_factory = True
    assert await updater.run_production_update() is False
    record = json.loads(updater.receipt_path.read_text(encoding="utf-8"))
    assert record["batch_outcome"]["reason_code"] == "cleanup_failed"
    assert record["collection_success"] is False
