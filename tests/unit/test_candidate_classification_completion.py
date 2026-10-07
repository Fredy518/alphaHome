from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from alphahome.curation import etf_candidate_ai_automation as automation
from alphahome.curation import etf_candidate_classification_completion as completion
from alphahome.curation.deepseek_candidate_client import (
    DeepSeekCandidateError,
    validate_decisions,
)
from test_etf_candidate_ai_automation import (
    AutomationConnection,
    _capture_publication,
    _current_record,
    _fact,
    _model_result,
)


@pytest.fixture
def db(monkeypatch):
    db = AutomationConnection()
    row = db.records[0]
    row.update(region_market=None, level1_group=None, level2_group=None,
               confirmation_status="AI_REVIEW_REQUIRED", ai_run_id="original-review",
               human_review_note="其他疑点保留", confirmation_actor="deepseek:previous-model",
               confirmation_at="2026-09-10T00:00:00+00:00", ai_model="previous-model",
               ai_confidence=0.8, ai_decision_hash="a" * 64)
    monkeypatch.setattr(automation, "missing_confirmation_schema", lambda _: [])
    monkeypatch.setattr(completion, "_read_completion_evidence", lambda _: {
        row["fund_code"]: {"rawdata.fund_etf_index": {"index_name": "沪深300"}}
    })
    return db


def plan(db, count=1):
    return completion.build_candidate_classification_completion_plan(
        db, model_requested="test-model", run_date=date(2026, 9, 16),
        expected_target_count=count, web_evidence={"products": {
            "510300.SH": {"profile": {"url": "https://example.org/510300", "fields": {"基金代码": "510300"}}}
        }},
    )


def test_pdf_controls_are_removed_before_evidence_is_frozen():
    source = {"products": [{"text": "投资\x00范围\x01\n股票\t债券", "sha256": "a" * 64}]}
    assert completion.sanitize_public_evidence(source) == {
        "products": [{"text": "投资范围\n股票\t债券", "sha256": "a" * 64}]
    }
    assert source["products"][0]["text"].startswith("投资\x00")


def result(code="510300.SH", **changes):
    out = _model_result([{"fund_code": code}])
    out.decisions[0].update(
        action="UPDATE", classification_patch={"region_market": "中国A股", "level1_group": "A股核心", "level2_group": "全市场综合"},
        evidence=[f"{code} 沪深300投资于A股", "https://example.org/510300 公共资料支持宽基"],
    )
    out.decisions[0].update(changes)
    return out


def envelopes(out):
    return [automation.DecisionEnvelope(decision=d, result=out) for d in out.decisions]


def snapshot(p, out):
    return automation.build_ai_snapshot_payload(
        p, ai_run_id="completion-test",
        envelopes=completion.resolve_completion_consistency(p, envelopes(out)),
        confirmed_at=datetime.now(timezone.utc),
    )[0]


def test_confirmed_and_pending_gaps_are_targets_but_human_records_are_preserved(db):
    db.records[0]["confirmation_status"] = "AI_CONFIRMED"
    assert plan(db).target_items[0]["missing_classification_fields"] == ["region_market", "level1_group", "level2_group"]
    assert not plan(db, 420).executable
    db.records[0]["confirmation_status"] = "HUMAN_CONFIRMED"
    assert plan(db, 0).target_items == []


def test_completion_applies_nulls_preserves_identity_membership_and_prior_review(db):
    p = plan(db)
    before = deepcopy(db.records[0])
    row = snapshot(p, result())["records"][0]
    for field in ("fund_code", "tracking_index_code", "exposure_id", "candidate_status", "product_role",
                  "confirmation_status", "ai_run_id", "human_review_note"):
        assert row[field] == before[field]
    assert row["level1_group"] == "A股核心"
    assert row["level2_group"] == "全市场综合"
    assert row["region_market"] == "中国A股"


@pytest.mark.parametrize("field", ["exposure_id", "candidate_status", "asset_class", "allocation_module"])
def test_completion_rejects_frozen_fields(db, field):
    out = result()
    out.decisions[0]["classification_patch"][field] = "changed"
    with pytest.raises(automation.CandidateAutomationError, match="frozen"):
        snapshot(plan(db), out)


def test_nonempty_classification_is_frozen(db):
    db.records[0]["region_market"] = "中国A股"
    with pytest.raises(automation.CandidateAutomationError, match="non-empty"):
        snapshot(plan(db), result())


@pytest.mark.parametrize("changes", [{"confidence": 0.84}, {"requires_human_review": True}])
def test_insufficient_evidence_does_not_publish_proposed_labels(db, changes):
    row = snapshot(plan(db), result(**changes))["records"][0]
    assert row["region_market"] is None
    assert row["confirmation_status"] == "AI_REVIEW_REQUIRED"


def test_partial_or_invalid_hierarchy_not_applied(db):
    out = result()
    out.decisions[0]["classification_patch"].pop("level2_group")
    assert snapshot(plan(db), out)["records"][0]["level1_group"] is None
    out.decisions[0]["classification_patch"]["level2_group"] = "半导体"
    assert snapshot(plan(db), out)["records"][0]["level1_group"] is None


def test_same_index_conflict_keeps_nulls(db):
    db.records.append({**db.records[0], **{k:v for k,v in _current_record("AI_CONFIRMED").items() if k in ("region_market","level1_group","level2_group")}, "fund_code": "510330.SH", "source_rank": 2, "source_row_number": 8})
    db.facts.append({**_fact(), "fund_code": "510330.SH"})
    out = result()
    out.decisions[0]["classification_patch"].update(level1_group="科技", level2_group="半导体")
    assert snapshot(plan(db), out)["records"][0]["level1_group"] is None


def test_ordinary_incremental_trigger_finds_confirmed_missing_archive(db):
    db.records[0]["confirmation_status"] = "AI_CONFIRMED"
    assert automation._target_reason(db.records[0], _fact(), reconfirm_all=False, thresholds={}) == "classification_incomplete"


def test_client_cannot_confirm_incomplete_keep(db):
    p = plan(db)
    out = result(action="KEEP", classification_patch={})
    with pytest.raises(DeepSeekCandidateError, match="incomplete"):
        validate_decisions({"decisions": out.decisions}, input_items=p.target_items, taxonomy=p.taxonomy)
    out.decisions[0]["requires_human_review"] = True
    validate_decisions({"decisions": out.decisions}, input_items=p.target_items, taxonomy=p.taxonomy)


def test_executor_revalidates_completion_evidence_and_preserves_non_targets(db, monkeypatch, tmp_path):
    _capture_publication(monkeypatch, db)
    p = plan(db)
    client = SimpleNamespace(
        prompt_sha256=completion.COMPLETION_PROMPT_SHA256,
        generation_settings=completion.COMPLETION_GENERATION_SETTINGS,
        confirm_batch=lambda **_: result(),
    )
    outcome = automation.execute_candidate_automation(db, p, client=client, checkpoint_dir=tmp_path)
    assert outcome["screening_scope"] == "classification_completion"
    assert db.published[0]["records"][0]["ai_run_id"] == "original-review"
    assert db.published[0]["records"][0]["region_market"] == "中国A股"
