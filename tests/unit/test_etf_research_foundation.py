from __future__ import annotations

from datetime import date

import pytest

from alphahome.curation import etf_research_foundation as foundation


class StatusDatabase:
    def __init__(self, *, refresh_log_exists: bool = True):
        self.refresh_log_exists = refresh_log_exists
        self.queries: list[str] = []

    async def fetch(self, query, *args):
        normalized = " ".join(query.split())
        self.queries.append(normalized)
        if "to_regclass" in normalized:
            return [
                {
                    key: self.refresh_log_exists if key == "refresh_log" else True
                    for key in foundation.RELATION_KEYS
                }
            ]
        if "FROM features.mv_refresh_log" in normalized:
            return [
                {
                    "view_name": item["relation"].rsplit(".", 1)[1],
                    "finished_at": "2026-09-13 10:00:00",
                    "row_count": index * 100,
                    "success": True,
                }
                for index, item in enumerate(foundation.FACT_OBJECTS, start=1)
            ]
        if "MAX(as_of_date)" in normalized:
            return [{"watermark": date(2026, 9, 11)}]
        if "MAX(trade_date)" in normalized:
            return [{"watermark": date(2026, 9, 11)}]
        if "MAX(obs_date)" in normalized:
            return [{"watermark": date(2026, 8, 31)}]
        if "FROM fund_pool_on.etf_candidate_master_latest_batch" in normalized:
            return [
                {
                    "snapshot_id": "etf_candidate_master_20260905_5d8767b2d4fa",
                    "source_file_name": "ETF候选池总表_20260905_分组排序版.xlsx",
                    "source_file_sha256": "5d8767b2d4fa" + "0" * 52,
                    "workbook_generated_on": date(2026, 9, 5),
                    "product_facts_as_of": date(2026, 9, 4),
                    "row_count": 146,
                    "exposure_count": 120,
                    "capital_authority": False,
                    "order_authority": False,
                }
            ]
        if "FROM fund_pool_on.etf_candidate_master_current_enriched" in normalized:
            return [
                {
                    "row_count": 146,
                    "exposure_count": 120,
                    "formal_candidate_count": 133,
                    "conditional_candidate_count": 4,
                    "watch_count": 9,
                    "ai_confirmed_count": 146,
                    "ai_review_required_count": 0,
                    "human_confirmed_count": 0,
                    "legacy_imported_count": 0,
                    "live_product_fact_count": 146,
                    "live_complete_count": 146,
                    "live_auxiliary_state_change_count": 0,
                    "live_facts_as_of": date(2026, 9, 11),
                }
            ]
        if "FROM fund_pool_on.etf_candidate_ai_run" in normalized:
            return [
                {
                    "ai_run_id": "etf_ai_202609_test",
                    "run_month": date(2026, 9, 1),
                    "facts_as_of": date(2026, 9, 11),
                    "model_requested": "deepseek-flash",
                    "prompt_version": "etf_candidate_confirmation_v2",
                    "status": "SUCCEEDED",
                    "decision_count": 146,
                    "output_snapshot_id": "etf_candidate_master_ai_202609_test",
                }
            ]
        if "FROM fund_pool_on.etf_candidate_index_coverage_current" in normalized:
            return [
                {
                    "tracking_index_count": 133,
                    "technical_index_count": 103,
                    "direct_valuation_index_count": 3,
                    "technical_latest_date": date(2026, 9, 11),
                    "direct_valuation_latest_date": date(2026, 9, 11),
                }
            ]
        raise AssertionError(f"unexpected query: {normalized}")


@pytest.mark.asyncio
async def test_status_reports_managed_objects_coverage_and_no_authority():
    db = StatusDatabase()

    result = await foundation.get_etf_research_foundation_status(db)

    assert result["status"] == "success"
    assert result["candidate_current"]["row_count"] == 146
    assert result["candidate_current"]["exposure_count"] == 120
    assert result["index_coverage"]["technical_index_count"] == 103
    assert result["index_coverage"]["direct_valuation_index_count"] == 3
    assert len(result["facts"]) == 4
    assert all(item["exists"] for item in result["facts"])
    assert result["authority"] == {
        "capital_authority": False,
        "order_authority": False,
    }


@pytest.mark.asyncio
async def test_status_can_open_before_feature_refresh_log_exists():
    db = StatusDatabase(refresh_log_exists=False)

    result = await foundation.get_etf_research_foundation_status(db)

    assert len(result["facts"]) == 4
    assert all(item["row_count"] is None for item in result["facts"])
    assert not any("FROM features.mv_refresh_log" in query for query in db.queries)


@pytest.mark.asyncio
async def test_update_uses_fixed_dependency_order(monkeypatch, tmp_path):
    events: list[str] = []
    snapshot = tmp_path / "candidate.json"
    snapshot.write_text("{}", encoding="utf-8")

    def fake_read(path, *, verify_source_file):
        assert path == snapshot
        assert verify_source_file is True
        events.append("validate")
        return {"snapshot_id": "snapshot-1"}

    class FakeInitializer:
        def __init__(self, *, db_manager, schema):
            assert db_manager == "db"
            assert schema == "features"

        async def ensure_initialized(self):
            events.append("initialize")

    async def fake_refresh(db_manager, recipe_class, progress_callback=None):
        assert db_manager == "db"
        events.append(recipe_class.name)
        return {"name": recipe_class.name, "status": "success"}

    def fake_load(database_url, payload, *, verify_source_file):
        assert database_url == "postgresql://example"
        assert payload == {"snapshot_id": "snapshot-1"}
        assert verify_source_file is True
        events.append("candidate_import")
        return {"row_count": 146}

    def fake_coverage(database_url):
        assert database_url == "postgresql://example"
        events.append("coverage")

    async def fake_status(db_manager):
        events.append("status")
        return {
            "authority": {"capital_authority": False, "order_authority": False},
            "candidate_batch": {},
            "candidate_current": {},
            "index_coverage": {},
            "watermarks": {},
            "facts": [],
        }

    monkeypatch.setattr(foundation, "read_and_validate_payload", fake_read)
    monkeypatch.setattr(foundation, "FeaturesDatabaseInit", FakeInitializer)
    monkeypatch.setattr(foundation, "_ensure_and_refresh", fake_refresh)
    monkeypatch.setattr(foundation, "_load_snapshot", fake_load)
    monkeypatch.setattr(foundation, "_ensure_coverage_view", fake_coverage)
    monkeypatch.setattr(foundation, "get_etf_research_foundation_status", fake_status)

    result = await foundation.update_etf_research_foundation(
        "db",
        "postgresql://example",
        snapshot,
    )

    assert events == [
        "validate",
        "initialize",
        *[recipe.name for recipe in foundation.PRE_IMPORT_RECIPES],
        "candidate_import",
        *[recipe.name for recipe in foundation.POST_IMPORT_RECIPES],
        "coverage",
        "status",
    ]
    assert result["snapshot_id"] == "snapshot-1"
    assert result["authority"] == {
        "capital_authority": False,
        "order_authority": False,
    }
