from __future__ import annotations

import pytest

from alphahome.gui.handlers.etf_research_handler import _format_status_detail
from alphahome.gui.services import etf_research_service


def test_status_detail_exposes_coverage_and_authority_boundary():
    text = _format_status_detail(
        {
            "candidate_batch": {"snapshot_id": "snapshot-1"},
            "candidate_current": {
                "row_count": 146,
                "exposure_count": 120,
                "formal_candidate_count": 133,
                "conditional_candidate_count": 4,
                "watch_count": 9,
                "live_product_fact_count": 146,
                "live_complete_count": 146,
            },
            "index_coverage": {
                "tracking_index_count": 133,
                "technical_index_count": 103,
                "direct_valuation_index_count": 3,
            },
            "authority": {
                "capital_authority": False,
                "order_authority": False,
            },
        }
    )

    assert "产品/暴露: 146 / 120" in text
    assert "技术 103/133；直接估值 3/133" in text
    assert "资金权限: 否" in text
    assert "下单权限: 否" in text


@pytest.mark.asyncio
async def test_service_rejects_non_json_snapshot_before_database_access(monkeypatch):
    responses = []
    etf_research_service.initialize_etf_research_service(
        lambda command, data: responses.append((command, data))
    )

    def fail_if_called():
        raise AssertionError("database must not be accessed")

    monkeypatch.setattr(
        etf_research_service.UnifiedTaskFactory,
        "get_db_manager",
        fail_if_called,
    )

    await etf_research_service.handle_update("candidate.xlsx")

    assert responses == [
        (
            "ETF_RESEARCH_UPDATE_COMPLETE",
            {"status": "error", "error": "请选择标准化候选母表 JSON 快照。"},
        )
    ]


@pytest.mark.asyncio
async def test_service_emits_progress_and_completion(monkeypatch, tmp_path):
    snapshot = tmp_path / "candidate.json"
    snapshot.write_text("{}", encoding="utf-8")
    responses = []
    etf_research_service.initialize_etf_research_service(
        lambda command, data: responses.append((command, data))
    )
    monkeypatch.setattr(
        etf_research_service, "get_database_url", lambda: "postgresql://example"
    )
    monkeypatch.setattr(
        etf_research_service.UnifiedTaskFactory,
        "get_db_manager",
        lambda: "db",
    )

    async def fake_update(
        db_manager,
        database_url,
        candidate_snapshot,
        *,
        verify_source_file,
        progress_callback,
    ):
        assert db_manager == "db"
        assert database_url == "postgresql://example"
        assert candidate_snapshot == snapshot
        assert verify_source_file is True
        progress_callback({"stage": "validate", "message": "正在校验"})
        return {
            "status": "success",
            "snapshot_id": "snapshot-1",
            "authority": {
                "capital_authority": False,
                "order_authority": False,
            },
        }

    monkeypatch.setattr(
        etf_research_service, "update_etf_research_foundation", fake_update
    )

    await etf_research_service.handle_update(str(snapshot))

    assert responses == [
        (
            "ETF_RESEARCH_PROGRESS",
            {"stage": "validate", "message": "正在校验"},
        ),
        (
            "ETF_RESEARCH_UPDATE_COMPLETE",
            {
                "status": "success",
                "snapshot_id": "snapshot-1",
                "authority": {
                    "capital_authority": False,
                    "order_authority": False,
                },
            },
        ),
    ]
