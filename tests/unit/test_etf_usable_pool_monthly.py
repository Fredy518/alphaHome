from copy import deepcopy
from datetime import date, datetime, timedelta

import pytest

from alphahome.curation import etf_usable_pool_monthly as pool


def dates():
    sessions = [
        date(2015, 12, 1) + timedelta(days=i)
        for i in range(150)
        if (date(2015, 12, 1) + timedelta(days=i)).weekday() < 5
    ]
    return pool.schedule(date(2016, 1, 1), sessions)


def source(code="510300.SH", *, amount=1, classification=None):
    d = dates()
    return {
        "identity": {
            "fund_code": code,
            "fund_name_reference": "Current name",
            "list_date": "2012-01-01",
            "delist_date": None,
            "current_index_reference": "FUTURE_INDEX",
        },
        "facts": {
            "price_date": d["facts_cutoff"],
            "amount_days": 20,
            "observed_dates": d["days"],
            "invalid_amount_days": 0,
            "invalid_price_days": 0,
            "amount_20d_100m": amount,
            "aum_100m": 10,
            "aum_source": "same_date_nav_times_shares",
            "nav_date": d["facts_cutoff"],
            "nav_ann_date": d["decision_cutoff"][:10],
            "share_date": d["facts_cutoff"],
        },
        "classification": classification or {},
    }


def classification(index="000300.SH", status="AI_REVIEW_REQUIRED"):
    return {
        "available_from": "2015-12-01T12:00:00+08:00",
        "snapshot_id": "known",
        "tracking_index_code": index,
        "exposure_id": "HS300",
        "confirmation_status": status,
    }


def test_month_range_has_128_complete_months_and_excludes_september():
    end = pool.last_complete_month(date(2026, 9, 29))
    assert end == date(2026, 8, 1)
    assert len(pool.month_range(date(2016, 1, 1), end)) == 128


def test_decision_before_first_open_and_effective_on_first_exchange_session():
    d = dates()
    assert d["facts_cutoff"] == "2016-01-29"
    assert d["decision_cutoff"] == "2016-02-01T09:00:00+08:00"
    assert d["scheduled_effective_at"] == "2016-02-01T09:30:00+08:00"
    assert len(d["days"]) == 20


def test_calendar_gap_fails():
    with pytest.raises(pool.MonthlyPoolError, match="calendar incomplete"):
        pool.schedule(date(2016, 1, 1), [date(2016, 1, 29)])


def test_unmapped_products_remain_standalone_without_current_index_grouping():
    a, b = source(), source("510330.SH")
    before = deepcopy([a, b])
    rows = pool.screen_month([a, b], dates())
    assert {r["selection_status"] for r in rows} == {"STANDALONE"}
    assert len({r["group_id"] for r in rows}) == 2
    assert all(r["tracking_index_code"] is None for r in rows)
    assert [a, b] == before


def test_historical_mapping_selects_primary_and_only_same_index_backup():
    rows = pool.screen_month(
        [
            source("510300.SH", amount=4, classification=classification()),
            source("510310.SH", amount=3, classification=classification("000905.SH")),
            source("510330.SH", amount=2, classification=classification()),
            source("510350.SH", amount=1, classification=classification()),
        ],
        dates(),
    )
    assert [r["selection_status"] for r in rows] == [
        "PRIMARY",
        "RESERVE",
        "BACKUP",
        "RESERVE",
    ]
    assert "classification_review_required" in rows[0]["quality_flags"]


def test_future_classification_is_rejected_not_backdated():
    c = classification()
    c["available_from"] = "2026-09-28T10:00:00+08:00"
    with pytest.raises(pool.MonthlyPoolError, match="future classification"):
        pool.screen_month([source(classification=c)], dates())


def test_rejection_only_applies_when_it_was_known():
    row = pool.screen_month(
        [source(classification=classification(status="HUMAN_REJECTED"))], dates()
    )[0]
    assert row["selection_status"] == "BLOCKED"
    assert "human_rejected_as_of_decision" in row["reasons"]


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("aum_100m", 4.99, "aum_below_5_100m"),
        ("amount_20d_100m", 0.299, "amount20d_below_0_3_100m"),
    ],
)
def test_fixed_thresholds_apply_to_historical_values(field, value, reason):
    item = source()
    item["facts"][field] = value
    row = pool.screen_month([item], dates())[0]
    assert row["selection_status"] == "INELIGIBLE"
    assert reason in row["reasons"]


def test_exact_thresholds_and_listing_anniversary():
    item = source(amount=0.3)
    item["facts"]["aum_100m"] = 5
    item["identity"]["list_date"] = "2015-10-29"
    assert pool.screen_month([item], dates())[0]["selection_status"] == "STANDALONE"
    item["identity"]["list_date"] = "2015-10-30"
    row = pool.screen_month([item], dates())[0]
    assert row["selection_status"] == "INELIGIBLE"
    assert "listing_age_below_3_months" in row["reasons"]


@pytest.mark.parametrize(
    "listed,cutoff,expected",
    [
        (date(2024, 11, 30), date(2025, 2, 27), 2),
        (date(2024, 11, 30), date(2025, 2, 28), 3),
        (date(2023, 11, 30), date(2024, 2, 28), 2),
        (date(2023, 11, 30), date(2024, 2, 29), 3),
        (date(2024, 8, 31), date(2025, 2, 27), 5),
        (date(2024, 8, 31), date(2025, 2, 28), 6),
        (date(2024, 2, 29), date(2024, 8, 28), 5),
        (date(2024, 2, 29), date(2024, 8, 29), 6),
    ],
)
def test_month_end_and_leap_day_anniversaries(listed, cutoff, expected):
    assert pool.listing_months(listed, cutoff) == expected


def test_daily_and_monthly_use_same_listing_calendar_and_three_month_policy():
    from alphahome.curation import etf_usable_pool as daily

    assert daily.listing_months is pool.listing_months
    assert (
        daily.POLICY["minimum_age_months"] == pool.POLICY["minimum_listing_months"] == 3
    )


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"amount_days": 19}, "incomplete_20_exchange_sessions"),
        ({"price_date": "2016-01-28"}, "price_not_at_month_end"),
        ({"nav_ann_date": "2016-02-02"}, "nav_not_announced_by_decision"),
        ({"nav_date": "2016-01-20"}, "nav_missing_or_stale"),
        ({"share_date": "2016-01-28"}, "nav_share_date_mismatch"),
        ({"aum_100m": "NaN"}, "historical_aum_unavailable"),
        ({"invalid_price_days": 1}, "invalid_market_observation"),
    ],
)
def test_historical_facts_fail_closed(change, reason):
    item = source()
    item["facts"].update(change)
    row = pool.screen_month([item], dates())[0]
    assert row["selection_status"] == "BLOCKED"
    assert reason in row["reasons"]


def test_missing_calendar_bar_cannot_be_replaced_with_older_bar():
    item = source()
    item["facts"]["observed_dates"] = ["2015-12-01", *dates()["days"][1:]]
    assert pool.screen_month([item], dates())[0]["selection_status"] == "BLOCKED"


def test_inventory_includes_later_delisted_excludes_future_and_already_delisted():
    rows = [
        {
            "fund_code": "a",
            "list_date": date(2010, 1, 1),
            "delist_date": date(2020, 1, 1),
        },
        {"fund_code": "b", "list_date": date(2017, 1, 1)},
        {
            "fund_code": "c",
            "list_date": date(2010, 1, 1),
            "delist_date": date(2015, 1, 1),
        },
    ]
    assert [r["fund_code"] for r in pool.active_inventory(rows, date(2016, 1, 29))] == [
        "a"
    ]


def test_wrong_hash_never_touches_database():
    plan = pool.MonthlyPlan({"executable": True}, [])

    class Connection:
        def rollback(self):
            pass

        def cursor(self, *a, **k):
            raise AssertionError("must not query")

    with pytest.raises(pool.MonthlyPoolError, match="hash/guards"):
        pool.execute_monthly_plan(Connection(), plan, expected_plan_hash="wrong")


def test_partial_month_cannot_be_built():
    with pytest.raises(pool.MonthlyPoolError, match="complete months"):
        pool.build_monthly_plan(
            None, start_month=date(2026, 9, 1), as_of=date(2026, 9, 29)
        )


def test_source_drift_rolls_back_before_any_insert(monkeypatch):
    payload = {
        "executable": True,
        "start_month": "2016-01-01",
        "end_month": "2016-01-01",
        "as_of": "2026-09-29",
        "record_kind": "HISTORICAL_RECONSTRUCTION",
    }
    plan = pool.MonthlyPlan(payload, [])
    changed = pool.MonthlyPlan({**payload, "source_changed": True}, [])
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, *args):
            statements.append(sql)

    class Connection:
        rolled_back = False

        def cursor(self, *args, **kwargs):
            return Cursor()

        def rollback(self):
            self.rolled_back = True

        def commit(self):
            raise AssertionError("must not publish")

    conn = Connection()
    monkeypatch.setattr(pool, "schema_plan", lambda _conn: {"missing_objects": []})
    monkeypatch.setattr(pool, "lock_candidate_master", lambda _conn: None)
    monkeypatch.setattr(pool, "build_monthly_plan", lambda *args, **kwargs: changed)
    with pytest.raises(pool.MonthlyPoolError, match="source changed"):
        pool.execute_monthly_plan(conn, plan, expected_plan_hash=plan.plan_hash)
    assert conn.rolled_back
    assert not any("INSERT" in sql for sql in statements)


@pytest.mark.asyncio
async def test_gui_monthly_due_and_skipped_preview(monkeypatch):
    from types import SimpleNamespace
    from alphahome.gui.services import daily_update_service as service

    monkeypatch.setattr(service, "_database_url", lambda _db: "test")
    for preview, expected in [
        (
            {
                "status": "ready",
                "through_month": "2026-09-01",
                "missing_months": ["2026-09-01"],
            },
            "ready",
        ),
        (
            {"status": "no_op", "through_month": "2026-08-01", "missing_months": []},
            "skipped_policy",
        ),
        ({"status": "backfill_required", "missing_months": ["2016-01-01"]}, "blocked"),
    ]:
        monkeypatch.setattr(pool, "preview_monthly", lambda *args, **kwargs: preview)
        group = await service._build_usable_pool_monthly_group(
            SimpleNamespace(), date(2026, 10, 9), order=7
        )
        assert group["status"] == expected
        assert group["depends_on"] == ["features"]


@pytest.mark.asyncio
async def test_gui_monthly_execution_and_cancellation(monkeypatch):
    import asyncio
    from alphahome.gui.services import daily_update_service as service

    monkeypatch.setattr(service, "_database_url", lambda _db: "test")
    calls = []

    def refresh(*args, **kwargs):
        calls.append(kwargs["run_date"])
        return {"status": "success"}

    monkeypatch.setattr(pool, "refresh_monthly", refresh)
    group = {"key": pool.TASK_NAME, "task_names": [pool.TASK_NAME]}
    stop = asyncio.Event()
    assert (await service._execute_group(group, None, stop, date(2026, 10, 9)))[
        "status"
    ] == "success"
    stop.set()
    assert (await service._execute_group(group, None, stop, date(2026, 10, 9)))[
        "status"
    ] == "cancelled"
    assert len(calls) == 1
