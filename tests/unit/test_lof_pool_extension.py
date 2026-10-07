from copy import deepcopy

import pytest

from alphahome.curation import etf_candidate_ai_automation as automation
from alphahome.curation import etf_usable_pool_monthly as monthly
from alphahome.curation.exchange_fund_sources import LOF_UNIVERSE_SQL, lof_aum_sql
from test_etf_usable_pool import source, screen
from test_etf_usable_pool_monthly import source as historical_source, dates
from test_etf_candidate_ai_automation import (
    AutomationConnection,
    _build_plan,
    _fact,
    _model_result,
)


def lof(code="163406.SZ"):
    item = source(code, index=None, exposure="SHARED_BENCHMARK")
    item["facts"].update(
        product_type="LOF",
        share_date=None,
        aum_date="2026-06-30",
        aum_known_date="2026-09-03",
        aum_source="fund_overview_observed_net_asset",
    )
    item["identity"]["product_type"] = "LOF"
    return item


def test_lof_does_not_require_daily_shares_or_tracking_index():
    row = screen(lof())[0]
    assert row["selection_status"] == "PRIMARY"
    assert row["exposure_id"] == "LOF_PRODUCT_163406_SZ"


def test_same_benchmark_lofs_never_become_same_index_backups():
    rows = screen(lof(), lof("163402.SZ"))
    assert [r["selection_status"] for r in rows] == ["PRIMARY", "PRIMARY"]
    assert len({r["exposure_id"] for r in rows}) == 2


@pytest.mark.parametrize(
    "patch,reason",
    [
        ({"aum_known_date": "2026-09-29"}, "lof_aum_not_known_as_of"),
        ({"aum_date": "2025-12-31"}, "lof_aum_report_stale"),
        ({"aum_known_date": None}, "lof_aum_availability_missing"),
        ({"aum_date": "2026-09-30"}, "lof_aum_not_known_as_of"),
        ({"price_date": "2026-09-01"}, "price_date_stale"),
        ({"nav_date": "2026-09-01"}, "nav_date_stale"),
    ],
)
def test_lof_fail_closed_on_unavailable_or_stale_facts(patch, reason):
    item = lof()
    item["facts"].update(patch)
    row = screen(item)[0]
    assert row["selection_status"] == "BLOCKED"
    assert reason in row["blocking_reasons"]


def test_lof_fabricated_index_is_blocked():
    item = lof()
    item["candidate"]["tracking_index_code"] = "000300.SH"
    assert "tracking_index_identity_conflict" in screen(item)[0]["blocking_reasons"]


def test_lof_low_liquidity_is_screened_out_even_if_classification_pending():
    item = lof()
    item["facts"]["amount_20d_100m"] = 0.29
    row = screen(item)[0]
    assert row["selection_status"] == "INELIGIBLE"
    assert row["classification_review_required"]


def historical_lof():
    item = historical_source("161226.SZ")
    item["identity"]["product_type"] = "LOF"
    item["facts"].update(
        product_type="LOF",
        share_date=None,
        aum_date="2015-12-31",
        aum_known_date="2016-01-20",
        aum_source="fund_nav_reported_net_asset",
    )
    return item


def test_historical_lof_periodic_report_passes_without_matching_daily_shares():
    row = monthly.screen_month([historical_lof()], dates())[0]
    assert row["selection_status"] == "STANDALONE"
    assert row["group_id"] == "LOF_PRODUCT_161226_SZ"


@pytest.mark.parametrize(
    "patch,reason",
    [
        ({"aum_known_date": "2026-09-03"}, "lof_report_not_known_by_decision"),
        ({"aum_known_date": None}, "lof_report_availability_missing"),
        ({"aum_date": "2015-01-01"}, "lof_report_stale_or_future"),
    ],
)
def test_monthly_lof_never_backdates_observation_or_uses_stale_report(patch, reason):
    item = historical_lof()
    item["facts"].update(patch)
    row = monthly.screen_month([item], dates())[0]
    assert row["selection_status"] == "BLOCKED"
    assert reason in row["reasons"]


def test_sql_has_explicit_report_availability_and_excludes_split_classes():
    sql = lof_aum_sql("c.code", "c.cutoff", "c.known")
    assert "n.ann_date>n.nav_date" in sql
    assert "o.snapshot_date<=c.known" in sql
    assert "fund_share" not in sql
    assert (
        "market='E'" in LOF_UNIVERSE_SQL and "NOT LIKE '%%分级%%'" in LOF_UNIVERSE_SQL
    )


def test_lof_bootstrap_targets_lof_without_reconfirming_existing_etfs(monkeypatch):
    monkeypatch.setattr(automation, "missing_confirmation_schema", lambda _: [])
    db = AutomationConnection()
    fact = {
        **lof()["facts"],
        **_fact(),
        "product_type": "LOF",
        "tracking_index_code": None,
        "share_date": None,
        "fund_code": "163406.SZ",
        "fund_name": "兴全合润混合-A",
        "list_date": "2021-01-28",
    }
    db.facts.append(fact)
    db.facts.append({**_fact(), "fund_code": "159259.SZ"})
    plan = _build_plan(db, screening_scope="lof_initial")
    assert plan.executable
    assert [r["fund_code"] for r in plan.target_items] == ["163406.SZ"]
    assert plan.target_items[0]["live_product_facts"]["product_type"] == "LOF"


def test_unverified_lof_index_gets_product_identity_without_forced_index_review(
    monkeypatch,
):
    monkeypatch.setattr(automation, "missing_confirmation_schema", lambda _: [])
    db = AutomationConnection()
    db.facts.append(
        {
            **lof()["facts"],
            **_fact(),
            "product_type": "LOF",
            "tracking_index_code": None,
            "share_date": None,
            "fund_code": "163406.SZ",
            "list_date": "2021-01-28",
        }
    )
    plan = _build_plan(db, screening_scope="lof_initial")
    result = _model_result(plan.target_items)
    result.decisions[0].update(
        action="ADD",
        classification_patch={
            "exposure_name": "主动混合",
            "asset_class": "A股权益",
            "allocation_module": "主动",
        },
    )
    env = automation.DecisionEnvelope(result.decisions[0], result)
    before = deepcopy(env.decision)
    resolved = automation._resolve_exposure_identities(plan, [env])[0]
    assert (
        resolved.decision["classification_patch"]["exposure_id"]
        == "LOF_PRODUCT_163406_SZ"
    )
    assert env.decision == before
