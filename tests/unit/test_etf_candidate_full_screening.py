from datetime import date
from types import SimpleNamespace

import pytest

from alphahome.curation import etf_candidate_ai_automation as automation
from alphahome.curation import etf_candidate_monthly_maintenance as monthly
from alphahome.curation.deepseek_candidate_client import DeepSeekBatchResult
from test_etf_candidate_ai_automation import (
    AutomationConnection,
    _build_plan,
    _fact,
    _current_record,
    _model_result,
    _capture_publication,
)


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr(automation, "missing_confirmation_schema", lambda _: [])
    return AutomationConnection()


def test_old_unseen_growth_etf_is_discovered_in_both_scopes(db):
    db.facts.append({**_fact(), "fund_code": "159259.SZ", "list_date": "2025-08-28"})
    for scope in ("incremental", "full"):
        plan = _build_plan(db, screening_scope=scope)
        assert plan.executable
        assert plan.plan_payload["new_product_count"] == 1
        assert plan.plan_payload["coverage"]["159259.SZ"]["state"] == "model_target_new"
        assert "159259.SZ" in {row["fund_code"] for row in plan.target_items}


def test_full_inventory_accounts_for_alias_unlisted_missing_and_rejected(db):
    db.facts.extend(
        [
            {**_fact(), "fund_code": "159259.OF"},
            {**_fact(), "fund_code": "159259.SZ", "list_date": "2026-10-01"},
            {**_fact(), "fund_code": "511990.SH"},
        ]
    )
    db.records.append(
        {
            **_current_record("HUMAN_REJECTED"),
            "fund_code": "511990.SH",
            "include_in_candidate_pool": False,
        }
    )
    db.inventory = [
        {
            "fund_code": "158041.SZ",
            "fund_name": "算力ETF",
            "status": "L",
            "list_date": "2026-09-10",
        }
    ]
    plan = _build_plan(db, screening_scope="full")
    states = {code: row["state"] for code, row in plan.plan_payload["coverage"].items()}
    assert states == {
        "510300.SH": "model_target_existing",
        "159259.OF": "out_of_scope_code",
        "159259.SZ": "not_listed_as_of",
        "511990.SH": "human_rejected",
        "158041.SZ": "deferred_product_facts",
    }
    assert plan.plan_payload["guards"]["deferred_new_product_fact_issues"] == {
        "158041.SZ": ["missing_product_facts"]
    }
    assert (
        sum(v for k, v in plan.plan_payload["coverage_counts"].items() if k != "total")
        == 5
    )


def test_incremental_retries_changed_exclusion_and_full_always_rescreens(db):
    fact = {**_fact(), "fund_code": "159259.SZ", "list_date": "2025-08-28"}
    db.facts.append(fact)
    db.previous_exclusions = [
        {
            "fund_code": "159259.SZ",
            "decision_action": "EXCLUDE_NEW",
            "screening_fingerprint": automation._screening_fingerprint(
                fact, db.source_batch["thresholds"]
            ),
        }
    ]
    assert _build_plan(db).plan_payload["new_product_count"] == 0
    assert (
        _build_plan(db, screening_scope="full").plan_payload["new_product_count"] == 1
    )
    fact["fund_name"] = "新名称"
    assert _build_plan(db).plan_payload["new_product_count"] == 1


def test_stale_prelisting_etf_directory_cannot_hide_listed_fund(db):
    db.inventory = [
        {
            "fund_code": "158026.SZ",
            "status": "P",
            "list_date": None,
            "basic_status": "L",
            "basic_list_date": "2026-09-10",
        }
    ]
    plan = _build_plan(db, screening_scope="full")
    assert (
        plan.plan_payload["coverage"]["158026.SZ"]["state"] == "deferred_product_facts"
    )


def test_full_scope_is_separate_from_already_successful_month(monkeypatch):
    calls = []
    connection = SimpleNamespace(rollback=lambda: None, close=lambda: None)
    monkeypatch.setattr(monthly.psycopg2, "connect", lambda _: connection)

    def existing(_db, _month, scope="incremental"):
        calls.append(scope)
        return (
            {"status": "skipped_already_succeeded"} if scope == "incremental" else None
        )

    monkeypatch.setattr(monthly, "get_existing_month_result", existing)
    monkeypatch.setattr(
        monthly,
        "build_candidate_automation_plan",
        lambda *a, **kw: SimpleNamespace(
            target_items=[], executable=True, summary=lambda: {"plan_hash": "test"}
        ),
    )
    result = monthly.build_candidate_monthly_maintenance_plan(
        "unused", run_date=date(2026, 9, 2), screening_scope="full"
    )
    assert result["status"] == "ready"
    assert calls == ["full"]


def test_checkpoint_reuse_and_tampering_fail_closed(db, monkeypatch, tmp_path):
    _capture_publication(monkeypatch, db)
    plan = _build_plan(db, screening_scope="full")
    calls = []

    def confirm(**kwargs):
        calls.append(1)
        return _model_result(kwargs["items"])

    for _ in range(2):
        automation.execute_candidate_automation(
            db,
            plan,
            client=SimpleNamespace(confirm_batch=confirm),
            checkpoint_dir=tmp_path,
        )
    assert len(calls) == 1
    cache = next(tmp_path.glob("*.json"))
    cache.write_text(
        cache.read_text(encoding="utf-8").replace('"test-response"', '"tampered"'),
        encoding="utf-8",
    )
    with pytest.raises(automation.CandidateAutomationError, match="checkpoint hash"):
        automation.execute_candidate_automation(
            db,
            plan,
            client=SimpleNamespace(confirm_batch=confirm),
            checkpoint_dir=tmp_path,
        )
    assert len(db.published) == 2


def test_failed_batch_preserves_checkpoints_without_partial_publication(
    db, monkeypatch, tmp_path
):
    db.records.append(
        {
            **_current_record(),
            "fund_code": "510050.SH",
            "source_rank": 2,
            "source_row_number": 8,
        }
    )
    db.facts.append({**_fact(), "fund_code": "510050.SH"})
    plan = _build_plan(db, screening_scope="full")
    _capture_publication(monkeypatch, db)
    calls = []

    def confirm(**kwargs):
        code = kwargs["items"][0]["fund_code"]
        calls.append(code)
        if code == "510050.SH":
            raise RuntimeError("provider unavailable")
        return _model_result(kwargs["items"])

    with pytest.raises(RuntimeError, match="provider unavailable"):
        automation.execute_candidate_automation(
            db,
            plan,
            client=SimpleNamespace(confirm_batch=confirm),
            batch_size=1,
            checkpoint_dir=tmp_path,
        )
    assert not db.published
    assert len(list(tmp_path.glob("*.json"))) == 1
    replay_calls = []

    def resume(**kwargs):
        replay_calls.append(kwargs["items"][0]["fund_code"])
        return _model_result(kwargs["items"])

    automation.execute_candidate_automation(
        db,
        plan,
        client=SimpleNamespace(confirm_batch=resume),
        batch_size=1,
        checkpoint_dir=tmp_path,
        max_workers=2,
    )
    assert replay_calls == ["510050.SH"]
    assert [row["fund_code"] for row in db.published[0]["records"]] == [
        "510300.SH",
        "510050.SH",
    ]


def test_new_exposure_ids_cannot_collide_across_model_batches(db):
    plan = _build_plan(db, screening_scope="full")
    envelopes = []
    for code, index_code, name in [
        ("159259.SZ", "980080.CNI", "成长100"),
        ("159999.SZ", "399997.SZ", "芯片产业"),
        ("159998.SZ", "980080.CNI", "成长100"),
    ]:
        plan.facts_by_code[code] = {
            **_fact(),
            "fund_code": code,
            "tracking_index_code": index_code,
            "tracking_index_name": name,
        }
        result = _model_result([{"fund_code": code}])
        decision = {
            **result.decisions[0],
            "action": "ADD",
            "classification_patch": {
                "exposure_id": "IND_063",
                "exposure_name": name,
                "asset_class": "境内权益",
                "allocation_module": "核心宽基",
            },
        }
        envelopes.append(automation.DecisionEnvelope(decision=decision, result=result))
    resolved = automation._resolve_exposure_identities(plan, envelopes)
    ids = [e.decision["classification_patch"]["exposure_id"] for e in resolved]
    assert ids[0] == ids[2]
    assert ids[0] != ids[1]
    assert all(
        e.raw_decision["classification_patch"]["exposure_id"] == "IND_063"
        for e in resolved
    )
    assert envelopes[0].decision["classification_patch"]["exposure_id"] == "IND_063"


def test_existing_dictionary_cannot_be_split_by_model_and_same_index_uses_reference(db):
    plan = _build_plan(db, screening_scope="full")
    result = _model_result([{"fund_code": "510300.SH"}])
    update = {
        **result.decisions[0],
        "action": "UPDATE",
        "classification_patch": {"exposure_id": "LLM_NEW_ID"},
    }
    plan.facts_by_code["510330.SH"] = {**_fact(), "fund_code": "510330.SH"}
    add = {
        **result.decisions[0],
        "fund_code": "510330.SH",
        "action": "ADD",
        "classification_patch": {
            "exposure_id": "LLM_NEW_ID",
            "exposure_name": "新名",
            "asset_class": "境内权益",
            "allocation_module": "核心宽基",
        },
    }
    resolved = automation._resolve_exposure_identities(
        plan,
        [
            automation.DecisionEnvelope(decision=update, result=result),
            automation.DecisionEnvelope(decision=add, result=result),
        ],
    )
    assert resolved[0].decision["requires_human_review"]
    assert resolved[1].decision["classification_patch"]["exposure_id"] == "CN_CORE_300"
    assert (
        resolved[1].decision["classification_patch"]["parent_fund_code"] == "510300.SH"
    )


def test_disagreement_within_new_index_requires_review(db):
    plan = _build_plan(db, screening_scope="full")
    envelopes = []
    for code, asset in [("159259.SZ", "境内权益"), ("159999.SZ", "跨境权益")]:
        plan.facts_by_code[code] = {
            **_fact(),
            "fund_code": code,
            "tracking_index_code": "980080.CNI",
        }
        result = _model_result([{"fund_code": code}])
        decision = {
            **result.decisions[0],
            "action": "ADD",
            "classification_patch": {
                "exposure_id": "GROWTH",
                "exposure_name": "成长",
                "asset_class": asset,
                "allocation_module": "核心宽基",
            },
        }
        envelopes.append(automation.DecisionEnvelope(decision=decision, result=result))
    resolved = automation._resolve_exposure_identities(plan, envelopes)
    assert all(e.decision["requires_human_review"] for e in resolved)
    assert (
        len({e.decision["classification_patch"]["asset_class"] for e in resolved}) == 1
    )
