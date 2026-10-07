from copy import deepcopy
from datetime import date
from decimal import Decimal

import pytest

from alphahome.curation import etf_usable_pool as pool


def source(
    code="510300.SH",
    *,
    confirmation="AI_REVIEW_REQUIRED",
    grade="观察",
    index="000300.SH",
    exposure="CN_CORE_HS300",
    amount=1.0,
    fee=0.2,
):
    return {
        "fund_code": code,
        "input_fingerprint": code,
        "candidate": {
            "fund_code": code,
            "tracking_index_code": index,
            "exposure_id": exposure,
            "candidate_status": grade,
            "confirmation_status": confirmation,
            "include_in_candidate_pool": True,
        },
        "facts": {
            "as_of_date": "2026-09-28",
            "tracking_index_code": index,
            "core_facts_complete": True,
            "amount_20d_days": 20,
            "price_date": "2026-09-28",
            "nav_date": "2026-09-28",
            "share_date": "2026-09-24",
            "age_months": 36,
            "aum_100m": 10,
            "amount_20d_100m": amount,
            "total_fee_pct": fee,
        },
        "identity": {
            "fund_code": code,
            "tracking_index_code": index,
            "status": "L",
            "list_date": "2023-09-01",
        },
        "review": {"uncertainty": ["与其他指数重合度待研究"]},
    }


def screen(*items):
    return pool.screen_products(
        list(items),
        facts_as_of="2026-09-28",
        minimum_fact_dates={
            "price_date": "2026-09-24",
            "nav_date": "2026-09-22",
            "share_date": "2026-09-22",
        },
    )


def test_pending_observation_enters_selection_without_becoming_ai_confirmed():
    original = source()
    before = deepcopy(original)
    row = screen(original)[0]
    assert row["selection_status"] == "PRIMARY"
    assert row["classification_review_required"]
    assert row["research_notes"] == ["与其他指数重合度待研究"]
    assert row["source_record"]["candidate_status"] == "观察"
    assert row["source_record"]["confirmation_status"] == "AI_REVIEW_REQUIRED"
    assert original == before


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("aum_100m", 4.99, "aum_below_5_100m"),
        ("amount_20d_100m", 0.299, "amount20d_below_0_3_100m"),
    ],
)
def test_hard_thresholds_apply_even_to_confirmed_formal_candidates(
    field, value, reason
):
    item = source(confirmation="AI_CONFIRMED", grade="正式候选")
    item["facts"][field] = value
    row = screen(item)[0]
    assert row["selection_status"] == "INELIGIBLE"
    assert reason in row["blocking_reasons"]


def test_exact_boundaries_pass():
    item = source(amount=0.3)
    item["facts"].update(age_months=3, aum_100m=5)
    item["identity"]["list_date"] = "2026-06-28"
    assert screen(item)[0]["selection_status"] == "PRIMARY"


@pytest.mark.parametrize(
    "listed,expected_age,status",
    [
        ("2026-06-29", 2, "INELIGIBLE"),
        ("2026-06-28", 3, "PRIMARY"),
        ("2026-06-27", 3, "PRIMARY"),
        ("2026-04-28", 5, "PRIMARY"),
        ("2025-10-28", 11, "PRIMARY"),
    ],
)
def test_three_complete_months_use_listing_date_not_rounded_fact_age(
    listed, expected_age, status
):
    item = source()
    item["facts"]["age_months"] = 3.0
    item["identity"]["list_date"] = listed
    row = screen(item)[0]
    assert row["selection_status"] == status
    assert row["diagnostics"]["listing_age_months"] == expected_age
    assert ("age_below_3_months" in row["blocking_reasons"]) == (expected_age < 3)


@pytest.mark.parametrize("listed", [None, "bad-date", "2026-10-01"])
def test_missing_invalid_or_future_listing_date_still_blocks(listed):
    item = source()
    item["identity"]["list_date"] = listed
    row = screen(item)[0]
    assert row["selection_status"] == "BLOCKED"
    assert "listing_date_missing_or_future" in row["blocking_reasons"]


@pytest.mark.parametrize("value", [Decimal("4.99"), "4.99"])
def test_numeric_representations_cannot_bypass_product_threshold(value):
    item = source()
    item["facts"]["aum_100m"] = value
    assert screen(item)[0]["selection_status"] == "INELIGIBLE"


@pytest.mark.parametrize(
    "field,value",
    [
        ("price_date", "2026-09-23"),
        ("nav_date", None),
        ("amount_20d_days", 19),
        ("total_fee_pct", float("nan")),
        ("aum_100m", None),
    ],
)
def test_incomplete_stale_or_invalid_facts_block(field, value):
    item = source()
    item["facts"][field] = value
    assert screen(item)[0]["selection_status"] == "BLOCKED"


def test_real_index_identity_conflict_blocks_even_with_ai_confirmation():
    item = source(confirmation="AI_CONFIRMED")
    item["identity"]["tracking_index_code"] = "000905.SH"
    row = screen(item)[0]
    assert row["selection_status"] == "BLOCKED"
    assert "tracking_index_identity_conflict" in row["blocking_reasons"]


def test_human_rejected_cannot_reenter():
    item = source(confirmation="HUMAN_REJECTED")
    assert screen(item)[0]["selection_status"] == "BLOCKED"


def test_primary_and_backup_are_deterministic_and_same_index():
    a = source("510300.SH", amount=4)
    b = source("510310.SH", amount=3, index="000905.SH")
    c = source("510330.SH", amount=2)
    d = source("510350.SH", amount=1)
    rows = screen(d, c, b, a)
    assert {r["fund_code"]: r["selection_status"] for r in rows} == {
        "510300.SH": "PRIMARY",
        "510310.SH": "RESERVE",
        "510330.SH": "BACKUP",
        "510350.SH": "RESERVE",
    }
    assert rows == screen(a, b, c, d)
    assert (
        rows[1]["diagnostics"]["selection_reason"] == "different_index_variant_reserved"
    )


def test_liquidity_tie_prefers_lower_fee_then_code_without_ai_bias():
    a = source("510300.SH", confirmation="AI_CONFIRMED", fee=0.6)
    b = source("510330.SH", fee=0.2)
    c = source("510310.SH", fee=0.2)
    rows = screen(a, b, c)
    assert (
        next(r for r in rows if r["selection_status"] == "PRIMARY")["fund_code"]
        == "510310.SH"
    )
    assert (
        next(r for r in rows if r["selection_status"] == "BACKUP")["fund_code"]
        == "510330.SH"
    )


def test_product_can_leave_usable_pool_without_leaving_archive():
    item = source()
    assert screen(item)[0]["selection_status"] == "PRIMARY"
    item["facts"]["amount_20d_100m"] = 0.1
    assert screen(item)[0]["selection_status"] == "INELIGIBLE"
    assert item["candidate"]["include_in_candidate_pool"] is True


def test_executable_rejects_different_plan_hash_before_database_writes():
    class Connection:
        def rollback(self):
            self.rolled_back = True

    connection = Connection()
    with pytest.raises(pool.UsablePoolError, match="hash/guards"):
        pool.execute_usable_pool_plan(
            connection,
            pool.UsablePoolPlan({"executable": True}, []),
            expected_plan_hash="wrong",
        )
    assert connection.rolled_back


def test_source_drift_aborts_before_inserting_snapshot(monkeypatch):
    class Connection:
        def cursor(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, sql):
            assert "pg_advisory_xact_lock" in sql

        def rollback(self):
            self.rolled_back = True

    connection = Connection()
    original = pool.UsablePoolPlan(
        {"executable": True, "run_date": "2026-09-28", "source": "old", "result_hash": pool.sha256_json([])}, []
    )
    changed = pool.UsablePoolPlan(
        {"executable": True, "run_date": "2026-09-28", "source": "changed"}, []
    )
    monkeypatch.setattr(pool, "schema_plan", lambda _: {"missing_objects": []})
    monkeypatch.setattr(pool, "lock_candidate_master", lambda _: None)
    monkeypatch.setattr(pool, "build_usable_pool_plan", lambda *_args, **_kw: changed)
    with pytest.raises(pool.UsablePoolError, match="source changed"):
        pool.execute_usable_pool_plan(
            connection, original, expected_plan_hash=original.plan_hash
        )
    assert connection.rolled_back


def test_changed_preview_rows_are_rejected_before_database_access():
    rows = [{"fund_code": "510300.SH", "selection_status": "BLOCKED"}]
    plan = pool.UsablePoolPlan({"executable": True, "result_hash": pool.sha256_json(rows)}, rows)
    frozen_hash = plan.plan_hash
    rows[0]["selection_status"] = "PRIMARY"

    class Connection:
        def rollback(self):
            pass
        def cursor(self, *args, **kwargs):
            raise AssertionError("mutated rows must fail before database access")

    with pytest.raises(pool.UsablePoolError, match="frozen result hash"):
        pool.execute_usable_pool_plan(Connection(), plan, expected_plan_hash=frozen_hash)


@pytest.mark.asyncio
async def test_gui_usable_pool_rebuild_runs_without_model_and_honors_stop(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from alphahome.gui.services import daily_update_service as service

    calls = []

    def refresh(database_url, *, run_date):
        calls.append(run_date)
        return {"status": "no_op"}

    monkeypatch.setattr(service.usable_pool, "refresh_usable_pool", refresh)
    group = {"key": "etf_usable_pool", "task_names": [pool.TASK_NAME]}
    db = SimpleNamespace(connection_string="postgresql://test")
    stop = asyncio.Event()
    result = await service._execute_group(group, db, stop, date(2026, 9, 28))
    assert result["status"] == "no_op"
    stop.set()
    assert (await service._execute_group(group, db, stop, date(2026, 9, 28)))[
        "status"
    ] == "cancelled"
    assert calls == [date(2026, 9, 28)]
