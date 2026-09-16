from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from alphahome.curation import etf_candidate_ai_automation as automation
from alphahome.curation.deepseek_candidate_client import DeepSeekBatchResult
from alphahome.curation.etf_candidate_ai_automation import (
    CandidateAutomationPlan,
    CandidateAutomationError,
    DecisionEnvelope,
    _taxonomy,
    build_ai_snapshot_payload,
)
from alphahome.curation.etf_candidate_master import SNAPSHOT_COLUMNS


def _current_record(confirmation_status="LEGACY_IMPORTED"):
    record = {column: None for column in SNAPSHOT_COLUMNS}
    record.update(
        {
            "snapshot_id": "etf_candidate_master_20260905_aaaaaaaaaaaa",
            "source_row_number": 7,
            "source_rank": 1,
            "asset_class": "境内权益",
            "allocation_module": "核心宽基",
            "allocation_role": "核心锚",
            "region_market": "中国A股",
            "level1_group": "A股核心",
            "level2_group": "全市场综合",
            "exposure_name": "沪深300",
            "exposure_id": "CN_CORE_300",
            "fund_code": "510300.SH",
            "fund_name": "旧名称",
            "tracking_index_code": "000300.SH",
            "tracking_index_name": "沪深300",
            "product_role": "主工具",
            "candidate_status": "正式候选",
            "exposure_relationship": "核心锚主实现",
            "budget_scope": "A股核心锚共享预算",
            "snapshot_aum_100m": 100.0,
            "snapshot_amount_20d_100m": 5.0,
            "snapshot_age_months": 100.0,
            "snapshot_total_fee_pct": 0.2,
            "snapshot_product_auxiliary_state": "强",
            "snapshot_premium_observation_label": "不适用",
            "product_facts_as_of": "2026-09-04",
            "manual_review_status": "待复核",
            "inclusion_reason": "核心宽基实现工具",
            "risk_boundary": "仅候选研究",
            "update_frequency": "月度",
            "execution_check": "执行前复核",
            "research_permission": "候选池研究",
            "duplicate_check": "OK",
            "data_source_id": "legacy",
            "confirmation_status": confirmation_status,
            "include_in_candidate_pool": True,
            "confirmation_actor": (
                "reviewer" if confirmation_status == "HUMAN_CONFIRMED" else None
            ),
            "confirmation_at": (
                "2026-09-10T00:00:00+00:00"
                if confirmation_status == "HUMAN_CONFIRMED"
                else None
            ),
        }
    )
    return record


def _fact():
    return {
        "as_of_date": "2026-09-15",
        "fund_code": "510300.SH",
        "fund_name": "沪深300ETF",
        "tracking_index_code": "000300.SH",
        "aum_100m": 120.0,
        "amount_20d_100m": 8.0,
        "amount_20d_days": 20,
        "price_date": "2026-09-15",
        "nav_date": "2026-09-14",
        "share_date": "2026-09-15",
        "core_facts_complete": True,
        "list_date": "2012-05-28",
        "age_months": 110.0,
        "total_fee_pct": 0.2,
        "mean_abs_premium_60d": None,
    }


def _plan(record, target_items):
    taxonomy = _taxonomy([record])
    return CandidateAutomationPlan(
        plan_hash="a" * 64,
        plan_payload={
            "run_month": "2026-09-01",
            "run_date": "2026-09-16",
            "facts_as_of": "2026-09-15",
            "executable": True,
            "guards": {
                "minimum_fact_dates": {
                    "price_date": "2026-09-15",
                    "nav_date": "2026-09-11",
                    "share_date": "2026-09-11",
                },
            },
        },
        source_batch={
            "snapshot_id": record["snapshot_id"],
            "structure_baseline_as_of": "2026-08-31",
            "thresholds": {
                "strong_aum_100m": 20,
                "strong_amount_20d_100m": 1,
                "minimum_age_months": 12,
                "usable_aum_100m": 5,
                "usable_amount_20d_100m": 0.3,
                "low_premium_upper": 0.04,
                "medium_premium_upper": 0.08,
                "premium_is_candidate_gate": False,
            },
        },
        current_records=[record],
        facts_by_code={"510300.SH": _fact()},
        target_items=target_items,
        taxonomy=taxonomy,
    )


def test_ai_confirmation_refreshes_computed_facts_and_provenance():
    record = _current_record()
    item = {"kind": "existing", "fund_code": "510300.SH"}
    decision = {
        "fund_code": "510300.SH",
        "action": "KEEP",
        "include_in_candidate_pool": True,
        "confidence": 0.92,
        "classification_patch": {},
        "decision_summary": "分类一致",
        "evidence": ["跟踪指数代码一致"],
        "uncertainty": [],
        "requires_human_review": False,
    }
    result = DeepSeekBatchResult(
        decisions=[decision],
        response_id="r1",
        actual_model="deepseek-flash",
        system_fingerprint=None,
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        input_hash="b" * 64,
        output_hash="c" * 64,
    )
    payload, manifest_hash = build_ai_snapshot_payload(
        _plan(record, [item]),
        ai_run_id="etf_ai_202609_test",
        envelopes=[DecisionEnvelope(decision=decision, result=result)],
        confirmed_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
    )

    output = payload["records"][0]
    assert output["confirmation_status"] == "AI_CONFIRMED"
    assert output["ai_run_id"] == "etf_ai_202609_test"
    assert output["fund_name"] == "沪深300ETF"
    assert output["snapshot_aum_100m"] == 120.0
    assert output["product_facts_as_of"] == "2026-09-15"
    assert len(output["ai_decision_hash"]) == 64
    assert manifest_hash == payload["source"]["source_file_sha256"]
    assert payload["governance"]["capital_authority"] is False
    assert payload["governance"]["order_authority"] is False


def test_monthly_copy_preserves_human_confirmation():
    record = _current_record("HUMAN_CONFIRMED")
    payload, _ = build_ai_snapshot_payload(
        _plan(record, []),
        ai_run_id="etf_ai_202609_test",
        envelopes=[],
        confirmed_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
    )

    output = payload["records"][0]
    assert output["confirmation_status"] == "HUMAN_CONFIRMED"
    assert output["confirmation_actor"] == "reviewer"
    assert output["manual_review_status"] == "人工确认"


def test_uncertain_model_result_routes_to_human_review_without_failing_snapshot():
    record = _current_record()
    decision = {
        "fund_code": "510300.SH",
        "action": "KEEP",
        "include_in_candidate_pool": True,
        "confidence": 0.88,
        "classification_patch": {},
        "decision_summary": "分类基本一致但简称存在歧义",
        "evidence": ["跟踪指数代码一致"],
        "uncertainty": ["基金简称不足以独立确认全部角色"],
        "requires_human_review": True,
    }
    result = DeepSeekBatchResult(
        decisions=[decision],
        response_id="r2",
        actual_model="deepseek-flash",
        system_fingerprint=None,
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        input_hash="b" * 64,
        output_hash="c" * 64,
    )
    payload, _ = build_ai_snapshot_payload(
        _plan(record, [{"kind": "existing", "fund_code": "510300.SH"}]),
        ai_run_id="etf_ai_202609_review",
        envelopes=[DecisionEnvelope(decision=decision, result=result)],
        confirmed_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
    )

    output = payload["records"][0]
    assert output["confirmation_status"] == "AI_REVIEW_REQUIRED"
    assert output["manual_review_status"] == "AI待人工复核"


class AutomationConnection:
    """Exercise real plan/execute code against mutable candidate state, without DB writes."""

    autocommit = False

    def __init__(self):
        self.records = [_current_record()]
        self.facts = [_fact()]
        self.source_batch = _plan(self.records[0], []).source_batch
        self.source_batch["product_facts_as_of"] = "2026-09-04"
        self.trade_dates = [date(2026, 9, 15), date(2026, 9, 14), date(2026, 9, 11)]
        self.last_success = None
        self.write_locked = False
        self.events = []
        self.published = []

    def cursor(self, **_kwargs):
        return AutomationCursor(self)

    def commit(self):
        self.events.append("commit")
        self.write_locked = False

    def rollback(self):
        self.events.append("rollback")
        self.write_locked = False


class AutomationCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        db = self.connection
        normalized = " ".join(sql.split())
        db.events.append(normalized)
        if "FROM fund_pool_on.etf_candidate_master_latest_batch" in sql:
            self.rows = [{"payload": deepcopy(db.source_batch)}]
        elif "FROM fund_pool_on.etf_candidate_master_current" in sql:
            self.rows = [
                {"payload": deepcopy(record)}
                for record in db.records
                if record["include_in_candidate_pool"]
            ]
        elif "FROM fund_pool_on.etf_candidate_master_snapshot" in sql:
            self.rows = [
                {"fund_code": row["fund_code"]}
                for row in db.records
                if not row["include_in_candidate_pool"]
            ]
        elif "FROM features.mv_etf_product_facts_current" in sql:
            self.rows = [{"payload": deepcopy(fact)} for fact in db.facts]
        elif "FROM rawdata.others_calendar" in sql:
            self.rows = [{"trade_date": day} for day in db.trade_dates]
        elif "last_facts_as_of" in sql:
            self.rows = [deepcopy(db.last_success)] if db.last_success else []
        elif "FROM fund_pool_on.etf_candidate_ai_run" in sql:
            self.rows = []
        elif sql == "SHOW transaction_isolation":
            self.rows = [("read committed",)]
        elif "pg_advisory_xact_lock" in sql:
            db.write_locked = True
            self.rows = [(None,)]
        elif "pg_try_advisory_lock" in sql or "pg_advisory_unlock" in sql:
            self.rows = [(True,)]
        elif "INSERT INTO fund_pool_on.etf_candidate_ai_run" in sql:
            assert not db.write_locked
        elif "UPDATE fund_pool_on.etf_candidate_ai_run" in sql:
            if "'SUCCEEDED'" in sql:
                assert db.write_locked
        else:
            raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


@pytest.fixture
def automation_db(monkeypatch):
    monkeypatch.setattr(automation, "missing_confirmation_schema", lambda _db: [])
    return AutomationConnection()


def _build_plan(db, run_date=date(2026, 9, 16), **kwargs):
    return automation.build_candidate_automation_plan(
        db, model_requested="test-model", run_date=run_date, **kwargs
    )


@pytest.mark.parametrize(
    "changes, expected_issue",
    [
        ({"price_date": "2026-08-01"}, "price_date_stale"),
        ({"nav_date": "2026-08-01"}, "nav_date_stale"),
        ({"share_date": "2026-08-01"}, "share_date_stale"),
        ({"nav_date": None}, "nav_date_missing"),
        ({"price_date": "2026-09-16"}, "price_date_after_as_of"),
        ({"core_facts_complete": False}, "core_facts_incomplete"),
        ({"amount_20d_days": 19}, "amount_20d_history_incomplete"),
        ({"aum_100m": None}, "aum_100m_missing_or_invalid"),
        ({"aum_100m": 0}, "aum_100m_invalid"),
        ({"amount_20d_100m": float("nan")}, "amount_20d_100m_invalid"),
        ({"total_fee_pct": None}, "total_fee_pct_missing_or_invalid"),
    ],
)
def test_plan_rejects_individual_bad_facts_despite_fresh_global_date(
    automation_db, changes, expected_issue
):
    automation_db.records[0] = _current_record("HUMAN_CONFIRMED")
    automation_db.facts[0].update(changes)
    plan = _build_plan(automation_db)

    assert plan.plan_payload["guards"]["product_facts_fresh"]
    assert not plan.executable
    issues = plan.plan_payload["guards"]["current_product_fact_issues"]["510300.SH"]
    assert expected_issue in issues
    with pytest.raises(CandidateAutomationError, match="blocked plan"):
        build_ai_snapshot_payload(
            plan,
            ai_run_id="test",
            envelopes=[],
            confirmed_at=datetime.now(timezone.utc),
        )


def test_payload_rechecks_product_facts_before_claiming_quality_success(automation_db):
    automation_db.records[0] = _current_record("HUMAN_CONFIRMED")
    plan = _build_plan(automation_db)
    assert plan.executable
    plan.facts_by_code["510300.SH"]["aum_100m"] = None
    with pytest.raises(CandidateAutomationError, match="aum_100m_missing_or_invalid"):
        build_ai_snapshot_payload(
            plan,
            ai_run_id="test",
            envelopes=[],
            confirmed_at=datetime.now(timezone.utc),
        )


def test_invalid_fact_is_reported_before_change_detection(automation_db):
    automation_db.records[0] = _current_record("AI_CONFIRMED")
    automation_db.records[0]["fund_name"] = _fact()["fund_name"]
    automation_db.facts[0]["aum_100m"] = "not-a-number"
    plan = _build_plan(automation_db)
    assert not plan.executable
    assert plan.plan_payload["guards"]["current_product_fact_issues"]["510300.SH"] == [
        "aum_100m_missing_or_invalid"
    ]
    assert not plan.target_items


def test_freshness_tolerance_uses_trading_calendar_including_holidays(automation_db):
    automation_db.trade_dates = [
        date(2026, 10, 8),
        date(2026, 9, 30),
        date(2026, 9, 29),
    ]
    automation_db.facts[0].update(
        as_of_date="2026-10-08",
        price_date="2026-10-08",
        nav_date="2026-09-29",
        share_date="2026-09-30",
    )
    plan = _build_plan(automation_db, date(2026, 10, 9))
    assert plan.executable
    assert plan.plan_payload["guards"]["minimum_fact_dates"]["nav_date"] == "2026-09-29"
    automation_db.facts[0]["nav_date"] = "2026-09-28"
    assert not _build_plan(automation_db, date(2026, 10, 9)).executable


def test_incomplete_new_product_is_deferred_and_retried_after_discovery_cutoff(
    automation_db,
):
    new_fact = {
        **_fact(),
        "fund_code": "159999.SZ",
        "list_date": "2026-09-10",
        "amount_20d_days": 4,
        "core_facts_complete": False,
    }
    automation_db.facts.append(new_fact)
    plan = _build_plan(automation_db)
    assert plan.executable
    assert plan.plan_payload["new_product_count"] == 0
    assert plan.summary()["deferred_new_product_count"] == 1
    assert all(item["fund_code"] != "159999.SZ" for item in plan.target_items)

    automation_db.last_success = {
        "last_facts_as_of": date(2026, 9, 15),
        "deferred_new_product_fact_codes": plan.plan_payload["guards"][
            "deferred_new_product_fact_codes"
        ],
    }
    # Even a temporarily absent fact row must stay in the retry ledger.
    automation_db.facts.pop()
    missing_plan = _build_plan(automation_db)
    assert missing_plan.plan_payload["guards"]["deferred_new_product_fact_issues"] == {
        "159999.SZ": ["missing_product_facts"]
    }
    automation_db.facts.append(new_fact)
    automation_db.trade_dates = [
        date(2026, 10, 14),
        date(2026, 10, 13),
        date(2026, 10, 12),
    ]
    for fact in automation_db.facts:
        fact.update(
            as_of_date="2026-10-14",
            price_date="2026-10-14",
            nav_date="2026-10-14",
            share_date="2026-10-14",
            core_facts_complete=True,
            amount_20d_days=20,
        )
    next_plan = _build_plan(automation_db, date(2026, 10, 15))
    assert next_plan.executable
    assert next_plan.plan_payload["new_product_count"] == 1
    assert next_plan.plan_payload["deferred_new_product_count"] == 0
    assert any(item["fund_code"] == "159999.SZ" for item in next_plan.target_items)


def test_rejected_recent_product_is_not_rediscovered_as_new(automation_db):
    rejected = {
        **_current_record(),
        "fund_code": "159999.SZ",
        "confirmation_status": "HUMAN_REJECTED",
        "include_in_candidate_pool": False,
    }
    automation_db.records.append(rejected)
    automation_db.facts.append(
        {**_fact(), "fund_code": "159999.SZ", "list_date": "2026-09-10"}
    )
    plan = _build_plan(automation_db)
    assert plan.plan_payload["new_product_count"] == 0
    assert all(item["fund_code"] != "159999.SZ" for item in plan.target_items)


def _model_result(items):
    decisions = [
        {
            "fund_code": item["fund_code"],
            "action": "KEEP",
            "include_in_candidate_pool": True,
            "confidence": 0.9,
            "classification_patch": {},
            "decision_summary": "分类一致",
            "evidence": ["指数一致"],
            "uncertainty": [],
            "requires_human_review": False,
        }
        for item in items
    ]
    return DeepSeekBatchResult(
        decisions=decisions,
        response_id="test-response",
        actual_model="test-model",
        system_fingerprint=None,
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        input_hash="b" * 64,
        output_hash="c" * 64,
    )


def _capture_publication(monkeypatch, db):
    def load(connection, payload, **kwargs):
        assert connection is db
        assert db.write_locked, "publication must share the revalidation transaction"
        assert kwargs == {"verify_source_file": False, "commit": False}
        db.published.append(payload)
        db.events.append("publish")
        return {"load_status": "loaded"}

    def insert_decisions(cursor, *_args, **_kwargs):
        assert cursor.connection.write_locked
        cursor.connection.events.append("insert_decisions")

    monkeypatch.setattr(automation, "load_candidate_master_snapshot", load)
    monkeypatch.setattr(automation, "execute_values", insert_decisions)


@pytest.mark.parametrize(
    "concurrent_change", ["approve", "reject", "new_snapshot", "facts", "thresholds"]
)
def test_model_completion_cannot_publish_stale_plan(
    monkeypatch, automation_db, concurrent_change
):
    db = automation_db
    second = {
        **_current_record("HUMAN_CONFIRMED"),
        "fund_code": "510050.SH",
        "source_row_number": 8,
        "source_rank": 2,
    }
    db.records.append(second)
    db.facts.append({**_fact(), "fund_code": "510050.SH"})
    plan = _build_plan(db)
    _capture_publication(monkeypatch, db)

    def confirm_batch(*, items, taxonomy):
        assert (
            not db.write_locked
        ), "human review must remain possible during model calls"
        if concurrent_change in {"approve", "reject"}:
            db.records[0].update(
                confirmation_status=(
                    "HUMAN_CONFIRMED"
                    if concurrent_change == "approve"
                    else "HUMAN_REJECTED"
                ),
                include_in_candidate_pool=concurrent_change == "approve",
                confirmation_actor="human",
                confirmation_at="2026-09-16T10:00:00+00:00",
            )
        elif concurrent_change == "new_snapshot":
            db.source_batch["snapshot_id"] = "newer_import"
        elif concurrent_change == "facts":
            db.facts[0]["aum_100m"] += 1
        else:
            db.source_batch["thresholds"]["low_premium_upper"] = 0.03
        return _model_result(items)

    with pytest.raises(
        CandidateAutomationError, match="plan changed before publication"
    ):
        automation.execute_candidate_automation(
            db,
            plan,
            client=SimpleNamespace(confirm_batch=confirm_batch),
            expected_plan_hash=plan.plan_hash,
        )
    assert not db.published
    assert "insert_decisions" not in db.events
    assert any("status = 'FAILED'" in event for event in db.events)
    assert not db.write_locked
    if concurrent_change == "reject":
        assert db.records[0]["include_in_candidate_pool"] is False


@pytest.mark.parametrize("human_confirmed", [False, True])
def test_unchanged_plan_revalidates_under_lock_then_publishes_atomically(
    monkeypatch, automation_db, human_confirmed
):
    db = automation_db
    if human_confirmed:
        db.records[0] = _current_record("HUMAN_CONFIRMED")
    plan = _build_plan(db)
    _capture_publication(monkeypatch, db)

    def confirm_batch(*, items, taxonomy):
        assert not db.write_locked
        return _model_result(items)

    result = automation.execute_candidate_automation(
        db,
        plan,
        client=(
            None if human_confirmed else SimpleNamespace(confirm_batch=confirm_batch)
        ),
    )
    assert result["status"] == "succeeded"
    assert len(db.published) == 1
    expected_status = "HUMAN_CONFIRMED" if human_confirmed else "AI_CONFIRMED"
    assert db.published[0]["records"][0]["confirmation_status"] == expected_status
    lock_index = next(
        i for i, event in enumerate(db.events) if "pg_advisory_xact_lock" in event
    )
    publish_index = db.events.index("publish")
    assert any(
        "etf_candidate_master_current" in event
        for event in db.events[lock_index:publish_index]
    )
    assert "commit" not in db.events[lock_index:publish_index]
    assert not db.write_locked
