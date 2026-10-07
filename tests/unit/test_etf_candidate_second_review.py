from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from alphahome.curation import etf_candidate_ai_automation as automation
from alphahome.curation import etf_candidate_second_review as review
from test_etf_candidate_ai_automation import (
    AutomationConnection,
    _current_record,
    _fact,
    _model_result,
    _capture_publication,
)


@pytest.fixture
def db(monkeypatch):
    db = AutomationConnection()
    db.records[0]["confirmation_status"] = "AI_REVIEW_REQUIRED"
    db.records[0]["candidate_status"] = "观察"
    db.records[0]["research_permission"] = "仅观察"
    monkeypatch.setattr(automation, "missing_confirmation_schema", lambda _: [])
    monkeypatch.setattr(
        review,
        "_read_review_evidence",
        lambda _: {
            r["fund_code"]: {
                "previous_decision": {"uncertainty": ["分类待核对"]},
                "rawdata.fund_etf_index": {"index_name": "沪深300"},
            }
            for r in db.records
            if r["confirmation_status"] == "AI_REVIEW_REQUIRED"
        },
    )
    return db


def plan(db, count=None):
    return review.build_candidate_second_review_plan(
        db,
        model_requested="test-model",
        run_date=date(2026, 9, 16),
        expected_target_count=count,
    )


def result_for(items):
    result = _model_result(items)
    for decision in result.decisions:
        decision["evidence"] = [
            f"基金名录 {decision['fund_code']} 跟踪指数 000300.SH 一致",
            "指数名录全名沪深300支持当前分类",
        ]
    return result


def envelope(code="510300.SH", **changes):
    result = result_for([{"fund_code": code}])
    decision = deepcopy(result.decisions[0])
    decision.update(changes)
    return automation.DecisionEnvelope(decision=decision, result=result)


def test_plan_selects_only_review_candidates_and_enriches_evidence(db):
    db.records.append(
        {
            **_current_record("AI_CONFIRMED"),
            "fund_code": "510330.SH",
            "source_rank": 2,
            "source_row_number": 8,
        }
    )
    db.facts.append({**_fact(), "fund_code": "510330.SH"})
    p = plan(db, 1)
    assert p.executable
    assert [i["fund_code"] for i in p.target_items] == ["510300.SH"]
    assert len(p.target_items[0]["same_index_products"]) == 2
    assert p.target_items[0]["review_evidence"]["previous_decision"]
    assert p.plan_payload["screening_scope"] == "review"
    assert p.plan_payload["new_product_count"] == 0
    assert "candidate_status" not in p.taxonomy["allowed_patch_fields"]
    assert "exposure_id" not in p.taxonomy["allowed_patch_fields"]
    assert not plan(db, 694).executable


def test_prior_evidence_change_invalidates_frozen_review_plan(db, monkeypatch):
    original = plan(db)
    monkeypatch.setattr(
        review,
        "_read_review_evidence",
        lambda _: {"510300.SH": {"previous_decision": {"uncertainty": ["新的疑点"]}}},
    )
    assert plan(db).plan_hash != original.plan_hash


@pytest.mark.parametrize(
    "field", ["candidate_status", "exposure_id", "parent_fund_code"]
)
def test_review_cannot_change_frozen_grade_or_identity(db, field):
    with pytest.raises(automation.CandidateAutomationError, match="frozen"):
        review.resolve_review_consistency(
            plan(db),
            [envelope(action="UPDATE", classification_patch={field: "changed"})],
        )


def test_review_confirmation_preserves_observation_grade(db):
    p = plan(db)
    decisions = review.resolve_review_consistency(p, [envelope()])
    payload, _ = automation.build_ai_snapshot_payload(
        p,
        ai_run_id="test-review",
        envelopes=decisions,
        confirmed_at=datetime.now(timezone.utc),
    )
    row = payload["records"][0]
    assert row["confirmation_status"] == "AI_CONFIRMED"
    assert row["candidate_status"] == "观察"
    assert row["research_permission"] == "仅观察"
    assert payload["source"]["source_version"] == "AI_REVIEW_202609"


def test_low_confidence_stays_pending(db):
    resolved = review.resolve_review_consistency(plan(db), [envelope(confidence=0.84)])
    assert resolved[0].decision["requires_human_review"]
    assert resolved[0].raw_decision["requires_human_review"] is False


def test_review_correction_must_agree_with_untouched_same_index_peer(db):
    db.records.append({**_current_record("AI_CONFIRMED"), "fund_code": "510330.SH"})
    db.facts.append({**_fact(), "fund_code": "510330.SH"})
    resolved = review.resolve_review_consistency(
        plan(db),
        [
            envelope(
                action="UPDATE", classification_patch={"allocation_module": "风格因子"}
            )
        ],
    )
    assert resolved[0].decision["requires_human_review"]
    assert "同指数" in resolved[0].decision["uncertainty"][-1]


def test_consistency_rechecks_after_a_whole_patch_is_blocked(db):
    for code in ("510330.SH", "510350.SH"):
        db.records.append({**_current_record("AI_REVIEW_REQUIRED"), "fund_code": code})
        db.facts.append({**_fact(), "fund_code": code})
    resolved = review.resolve_review_consistency(
        plan(db),
        [
            envelope(
                action="UPDATE", classification_patch={"allocation_module": "风格因子"}
            ),
            envelope(
                "510330.SH",
                action="UPDATE",
                classification_patch={
                    "allocation_module": "风格因子",
                    "budget_scope": "其他预算",
                },
            ),
            envelope(
                "510350.SH",
                action="UPDATE",
                classification_patch={"allocation_module": "风格因子"},
            ),
        ],
    )
    assert all(e.decision["requires_human_review"] for e in resolved)


def test_real_executor_revalidates_review_plan_and_preserves_non_targets(
    db, monkeypatch, tmp_path
):
    untouched = {
        **_current_record("AI_CONFIRMED"),
        "fund_code": "510330.SH",
        "fund_name": _fact()["fund_name"],
        "source_rank": 2,
        "source_row_number": 8,
        "confirmation_actor": "deepseek:previous-model",
        "confirmation_at": "2026-09-10T00:00:00+00:00",
        "ai_run_id": "previous-run",
        "ai_model": "previous-model",
        "ai_confidence": 0.9,
        "ai_decision_hash": "d" * 64,
    }
    db.records.append(untouched)
    db.facts.append({**_fact(), "fund_code": "510330.SH"})
    _capture_publication(monkeypatch, db)
    p = plan(db, 1)
    client = SimpleNamespace(
        prompt_sha256=review.REVIEW_PROMPT_SHA256,
        generation_settings=review.REVIEW_GENERATION_SETTINGS,
        confirm_batch=lambda **kw: result_for(kw["items"]),
    )
    result = automation.execute_candidate_automation(
        db, p, client=client, checkpoint_dir=tmp_path
    )
    assert result["ai_confirmed_count"] == 1
    assert result["screening_scope"] == "review"
    after = {r["fund_code"]: r for r in db.published[0]["records"]}
    assert after["510330.SH"]["confirmation_status"] == "AI_CONFIRMED"
    assert after["510330.SH"]["ai_run_id"] == untouched["ai_run_id"]
    assert after["510300.SH"]["candidate_status"] == "观察"


def test_executor_rejects_wrong_model_prompt_before_call(db):
    with pytest.raises(automation.CandidateAutomationError, match="prompt"):
        automation.execute_candidate_automation(
            db, plan(db), client=SimpleNamespace(prompt_sha256="wrong")
        )
