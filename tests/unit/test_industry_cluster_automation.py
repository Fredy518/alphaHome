from datetime import date, datetime
import gzip

import numpy as np
import pandas as pd
import pytest

from alphahome.curation.industry_clusters import automation as auto
from alphahome.curation.industry_clusters.engine import ClusterConfig, build_features
from alphahome.curation.industry_clusters.maintenance import (
    advance_snapshot,
    stable_hash,
)


def source(code="I1", **changes):
    row = {
        "fund_code": "512000.SH",
        "index_code": code,
        "index_name": "测试行业",
        "region_market": "中国A股",
        "allocation_module": "行业板块",
        "confirmation_status": "AI_CONFIRMED",
        "include_in_candidate_pool": True,
        "verified_index_code": code,
        "list_date": date(2020, 1, 1),
        "official_index_name": None,
        "pool_selection_status": "STANDALONE",
    }
    return {**row, **changes}


SEEN = datetime(2026, 9, 30, 8, tzinfo=auto.TZ)


@pytest.mark.parametrize("quality", ["invalid", "low", "ambiguous_same_start_classification", None])
def test_rejected_industry_quality_cannot_supply_known_weight(quality):
    config = ClusterConfig(windows=(5,), minimum_days=(3,))
    members = pd.DataFrame([dict(index_code="I1", ts_code="000001.SZ", weight=1.0)])
    classification = pd.DataFrame([dict(ts_code="000001.SZ", industry_code1="L1",
                                         industry_code2="L2", data_quality=quality)])
    returns = pd.DataFrame({"I1": [0.01, 0.02, -0.01, 0.01, 0.0],
                            config.benchmark: [0.02, 0.01, -0.01, 0.0, 0.01]},
                           index=pd.bdate_range("2026-09-21", periods=5))
    result = build_features(members, classification, returns, "2026-09-30", config)
    assert result.coverage.known_l1_weight.eq(0).all()
    assert result.coverage.known_l2_weight.eq(0).all()
    strict = ClusterConfig(windows=(5,), minimum_days=(3,), allow_partial_classification=False)
    with pytest.raises(ValueError, match="unclassified"):
        build_features(members, classification, returns, "2026-09-30", strict)
    for accepted in ("normal", "high"):
        classification["data_quality"] = accepted
        checked = build_features(members, classification, returns, "2026-09-30", strict)
        assert checked.coverage.known_l1_weight.eq(1).all()


@pytest.mark.parametrize(
    "present_views",
    [(), (auto.SELECTION_VIEWS[0],), (auto.SELECTION_VIEWS[1],), auto.SELECTION_VIEWS],
)
def test_schema_plan_detects_legacy_selection_columns(monkeypatch, present_views):
    def query(_conn, sql, args=()):
        if "to_regclass" in sql:
            return [{"relation": args[0]}]
        if "to_regprocedure" in sql:
            return [{"function": "industry_cluster_managed_as_of(timestamptz)"}]
        if "information_schema.columns" in sql:
            return [{"table_name": name} for name in present_views]
        if "pg_get_constraintdef" in sql:
            return [{"definition": "CHECK (record_kind IN ('historical_reconstruction','observed'))"}]
        raise AssertionError(sql)

    monkeypatch.setattr(auto, "_query", query)
    plan = auto.schema_plan(None)
    expected = [
        name + ".selection_confidence"
        for name in auto.SELECTION_VIEWS
        if name not in present_views
    ]
    assert plan["missing_objects"] == []
    assert plan["missing_columns"] == expected
    assert plan["status"] == ("ready" if expected else "no_op")


def test_qualified_library_is_independent_of_cluster_readiness():
    rows = [source(confirmation_status="AI_REVIEW_REQUIRED", price_ready=False)]
    members, events, pending = auto.reconcile_members([], rows, SEEN)
    assert [m["index_code"] for m in members] == ["I1"]
    assert members[0]["classification_review_required"] is True
    assert [e["event"] for e in events] == ["index_added"]
    assert pending == []


def test_disappeared_and_reclassified_fund_does_not_remove_established_index():
    old, _, _ = auto.reconcile_members([], [source()], SEEN)
    for rows in [
        [],
        [source(allocation_module="核心宽基")],
        [source(include_in_candidate_pool=False)],
    ]:
        members, events, _ = auto.reconcile_members(old, rows, SEEN)
        assert members == old
        assert events == []


@pytest.mark.parametrize(
    "change",
    [
        {"region_market": "中国香港"},
        {"allocation_module": "核心宽基"},
        {"allocation_module": "风格因子"},
    ],
)
def test_library_discovery_respects_industry_scope(change):
    assert auto.reconcile_members([], [source(**change)], SEEN)[0] == []


@pytest.mark.parametrize(
    "change",
    [
        {"verified_index_code": "I2"},
        {"list_date": date(2026, 10, 1)},
        {"confirmation_status": "HUMAN_REJECTED"},
    ],
)
def test_conflicting_future_or_rejected_new_index_is_pending(change):
    members, _, pending = auto.reconcile_members([], [source(**change)], SEEN)
    assert members == []
    assert len(pending) == 1


def test_retired_index_is_never_silently_reactivated():
    old, _, _ = auto.reconcile_members([], [source()], SEEN)
    old[0].update(status="retired", retired_at=SEEN.isoformat())
    new, events, _ = auto.reconcile_members(old, [source()], SEEN)
    assert new == old
    assert events == []


def test_same_discovery_is_idempotent_and_preserves_first_known_time():
    old, _, _ = auto.reconcile_members([], [source()], SEEN)
    new, events, _ = auto.reconcile_members(
        old, [source()], SEEN.replace(day=1, month=10)
    )
    assert new == old
    assert events == []


def test_saved_plan_cannot_be_tampered():
    body = {"members": [{"index_code": "I1"}], "asof": "2026-09-30"}
    digest = stable_hash(body)
    plan = {**body, "plan_hash": digest, "status": "ready"}
    auto._validate_plan(plan, digest)
    plan["members"][0]["index_code"] = "I2"
    with pytest.raises(auto.AutomationError, match="content or hash"):
        auto._validate_plan(plan, digest)


@pytest.mark.parametrize(
    "publication,expected",
    [
        ("2026-10-09T09:15:00+08:00", "2026-10-09T09:30:00+08:00"),
        ("2026-10-09T09:31:00+08:00", "2026-10-12T09:30:00+08:00"),
    ],
)
def test_late_publication_moves_to_real_next_open(monkeypatch, publication, expected):
    monkeypatch.setattr(
        auto,
        "_query",
        lambda *_args: [{"day": date(2026, 10, 9)}, {"day": date(2026, 10, 12)}],
    )
    result = auto.first_available_open(
        None,
        datetime(2026, 10, 9, 9, 30, tzinfo=auto.TZ),
        datetime.fromisoformat(publication),
    )
    assert result == datetime.fromisoformat(expected)


def test_library_expansion_keeps_existing_cluster_identity_and_confirmation_state():
    cfg = ClusterConfig(windows=(20, 40), minimum_days=(15, 30))
    rng = np.random.default_rng(34)
    days = pd.bdate_range("2026-06-01", "2026-09-30")
    market = rng.normal(0, 0.01, len(days))
    sector = market + rng.normal(0, 0.005, len(days))
    returns = pd.DataFrame(
        {cfg.benchmark: market, "I1": sector, "I2": sector, "I3": sector}, index=days
    )
    classification = pd.DataFrame(
        [{"ts_code": "000001.SZ", "industry_code1": "L1", "industry_code2": "L2"}]
    )

    def features(codes, day):
        members = pd.DataFrame(
            [
                {"index_code": code, "ts_code": "000001.SZ", "weight": 1.0}
                for code in codes
            ]
        )
        return build_features(members, classification, returns, day, cfg)

    prior = advance_snapshot(
        features(["I1", "I2"], "2026-08-31"), universe=["I1", "I2"]
    )
    old_id = prior["members"][0]["cluster_id"]
    nxt = advance_snapshot(
        features(["I1", "I2", "I3"], "2026-09-30"), prior["state"], ["I1", "I2", "I3"]
    )
    assert {
        r["cluster_id"] for r in nxt["members"] if r["index_code"] in {"I1", "I2"}
    } == {old_id}
    assert len(nxt["members"]) == 3
    assert nxt["state"]["merge_confirmations"]
    with pytest.raises(ValueError, match="later month"):
        advance_snapshot(
            features(["I1", "I2", "I3"], "2026-09-30"), nxt["state"], ["I1", "I2", "I3"]
        )


def cluster_plan_dependencies(monkeypatch):
    config = ClusterConfig().to_dict()
    pub = {
        "maintenance_month": date(2026, 8, 1),
        "cluster_batch_id": "old",
        "config": config,
    }

    def query(_conn, sql, args=()):
        if "publication p" in sql:
            return [pub]
        return [{"day": date(2026, 9, 30)}]

    monkeypatch.setattr(auto, "_ready", lambda _conn: None)
    monkeypatch.setattr(auto, "_query", query)
    monkeypatch.setattr(
        auto,
        "_schedule",
        lambda *_args: {
            "decision_cutoff": "2026-10-09T09:00:00+08:00",
            "scheduled_effective_at": "2026-10-09T09:30:00+08:00",
            "effective_to": "2026-11-02T09:30:00+08:00",
        },
    )
    return pub


def test_monthly_repeat_is_noop(monkeypatch):
    cluster_plan_dependencies(monkeypatch)
    plan = auto.build_cluster_plan(None, date(2026, 9, 30), now=SEEN)
    assert plan["status"] == "no_op"


def test_monthly_cannot_start_before_first_session_cutoff(monkeypatch):
    cluster_plan_dependencies(monkeypatch)
    plan = auto.build_cluster_plan(
        None, date(2026, 10, 9), now=datetime(2026, 10, 9, 8, 59, tzinfo=auto.TZ)
    )
    assert plan["status"] == "expected_no_data"


def test_monthly_uses_matching_pool_month_library_known_before_actual_fill(monkeypatch):
    cluster_plan_dependencies(monkeypatch)
    cutoffs = []

    def revision(_conn, month, known_at):
        assert month == date(2026, 9, 1)
        cutoffs.append(known_at)
        return {
            "revision_id": "frozen",
            "members": [
                {"index_code": "I1", "index_name": "行业", "status": "active"},
                {"index_code": "I2", "index_name": "退出", "status": "retired"},
            ],
        }

    monkeypatch.setattr(auto, "_revision_for_pool_month", revision)
    plan = auto.build_cluster_plan(
        None, date(2026, 10, 9), now=datetime(2026, 10, 9, 9, 1, tzinfo=auto.TZ)
    )
    assert plan["status"] == "ready"
    assert plan["members"] == [{"index_code": "I1", "index_name": "行业"}]
    assert cutoffs == [datetime(2026, 10, 9, 9, 1, tzinfo=auto.TZ)]


def test_source_drift_ignores_gzip_metadata_but_detects_changed_rows(tmp_path):
    names = ("members", "classification", "quotes", "calendar", "index_metadata")
    for folder, mtime in [("a", 1), ("b", 2)]:
        path = tmp_path / folder
        path.mkdir()
        for name in names:
            (path / (name + ".csv.gz")).write_bytes(
                gzip.compress(b"code,value\nI1,1\n", mtime=mtime)
            )
    a, b = {"cache_dir": str(tmp_path / "a")}, {"cache_dir": str(tmp_path / "b")}
    assert auto._source_frame_hash(a) == auto._source_frame_hash(b)
    (tmp_path / "b/quotes.csv.gz").write_bytes(
        gzip.compress(b"code,value\nI1,2\n", mtime=2)
    )
    assert auto._source_frame_hash(a) != auto._source_frame_hash(b)
