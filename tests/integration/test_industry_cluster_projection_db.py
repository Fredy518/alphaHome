"""Exercise the legacy-view upgrade and the real SQL-to-strategy boundary."""

from contextlib import contextmanager
import re

import pandas as pd
import psycopg2
from psycopg2.extras import Json
import pytest

from alphahome.curation.industry_clusters import automation as auto
from alphahome.curation.industry_clusters import store
from alphahome.curation.industry_clusters.service import select_projection

pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


def legacy_schema(sql):
    """Restore the previous projections while preserving their column order."""
    return re.sub(
        r",\n\s*m\.detail->>'selection_confidence'(?: AS selection_confidence)?",
        "",
        sql,
    )


@pytest.fixture
def legacy_cluster_database(isolated_database_url):
    conn = psycopg2.connect(isolated_database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regnamespace('fund_pool_on')")
            if cur.fetchone()[0] is not None:
                pytest.skip("This migration fixture requires an unused fund_pool_on schema")
            cur.execute(legacy_schema(store.SCHEMA_SQL))
            cur.execute(legacy_schema(auto.SCHEMA_SQL))
            members = [
                # The highest score is not always an admissible representative.
                ("I1", "C1", "confirmed", "S1", "multiview", True, True, "confirmed"),
                ("I2", "C1", "confirmed", "S1", "multiview", True, False, "confirmed"),
                ("I3", "C3", "classification_pending", "S2", "stock_fallback", True, True, "fallback"),
                ("I4", "C4", "price_pending", "S2", "stock_fallback", True, True, "fallback"),
                ("I5", "C5", "price_pending", "S3", "unresolved_singleton", False, False, "fallback"),
            ]
            universe = [{"index_code": row[0]} for row in members]
            cur.execute(
                """INSERT INTO fund_pool_on.industry_cluster_universe
                (universe_id,source_description,members,universe_hash)
                VALUES ('projection_test','test fixture',%s,'fixture')""",
                (Json(universe),),
            )
            cur.execute(
                """INSERT INTO fund_pool_on.industry_cluster_batch
                (batch_id,universe_id,asof_date,algorithm_version,config_hash,config,
                 input_hash,result_hash,record_kind,summary,maintenance_state,recorded_at)
                VALUES ('batch_test','projection_test','2026-09-30','industry_minimax_v2',
                        'fixture','{}','fixture','fixture','observed','{}','{}',
                        '2026-10-01 09:10+08')"""
            )
            for code, cluster, status, group, method, eligible, representative, confidence in members:
                detail = {
                    "selection_group_id": group,
                    "selection_method": method,
                    "selection_eligible": eligible,
                    "can_represent_selection_group": representative,
                    "selection_confidence": confidence,
                }
                cur.execute(
                    """INSERT INTO fund_pool_on.industry_cluster_member
                    (batch_id,index_code,cluster_id,representative_index_code,status,
                     price_ready,can_represent_cluster,is_central_representative,detail)
                    VALUES ('batch_test',%s,%s,'I1',%s,true,%s,%s,%s)""",
                    (code, cluster, status, representative, representative, Json(detail)),
                )
            cur.execute(
                """INSERT INTO fund_pool_on.industry_index_library
                (library_id,seed_batch_id,policy) VALUES ('library_test','batch_test',%s)""",
                (Json(auto.POLICY),),
            )
            cur.execute(
                """INSERT INTO fund_pool_on.industry_index_library_revision
                (revision_id,library_id,members,events,source_hash,plan_hash)
                VALUES ('revision_test','library_test',%s,'[]','fixture','fixture')""",
                (Json(universe),),
            )
            cur.execute(
                """INSERT INTO fund_pool_on.industry_cluster_monthly_publication
                (library_id,maintenance_month,revision_id,cluster_batch_id,plan_hash,
                 decision_cutoff,scheduled_effective_at,available_from,effective_to,
                 record_kind,recorded_at)
                VALUES ('library_test','2026-09-01','revision_test','batch_test','fixture',
                        '2026-10-01 09:00+08','2026-10-01 09:30+08',
                        '2026-10-01 09:30+08','2026-11-02 09:30+08',
                        'observed','2026-10-01 09:15+08')"""
            )
        yield conn
    finally:
        conn.rollback()
        conn.close()


def read_membership(conn, source):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM " + source + " ORDER BY index_code")
        return pd.DataFrame(cur.fetchall(), columns=[column.name for column in cur.description])


def test_legacy_upgrade_restores_all_database_projection_paths(
    legacy_cluster_database, isolated_database_url, monkeypatch
):
    conn = legacy_cluster_database
    sources = [
        "fund_pool_on.industry_cluster_current",
        "fund_pool_on.industry_cluster_managed_current",
        "fund_pool_on.industry_cluster_managed_as_of('2026-10-01 09:30+08')",
    ]
    candidates = pd.DataFrame({
        "index_code": ["I1", "I2", "I3", "I4", "I5"],
        "used_score": [10.0, 100.0, 20.0, 19.0, 200.0],
        "etf_code": ["510001.SH", "510002.SH", "510003.SH", "510004.SH", "510005.SH"],
    })
    previous_columns = {}
    for source in sources:
        membership = read_membership(conn, source)
        previous_columns[source] = list(membership.columns)
        with pytest.raises(ValueError, match="adaptive selection requires V2 membership"):
            select_projection(candidates, membership, slots=3)

    plan = auto.schema_plan(conn)
    assert plan["status"] == "ready"
    assert plan["missing_objects"] == []
    assert plan["missing_columns"] == [
        "industry_cluster_current.selection_confidence",
        "industry_cluster_managed_current.selection_confidence",
    ]

    @contextmanager
    def migration_connection(_url):
        # Keep the real migration inside the fixture's rollback-only transaction.
        yield conn

    monkeypatch.setattr(auto, "_connection", migration_connection)
    result = auto.apply_schema(isolated_database_url, plan["plan_hash"])
    assert result["status"] == "success"
    upgraded = auto.schema_plan(conn)
    assert upgraded["status"] == "no_op"
    assert upgraded["missing_columns"] == []

    for source in sources:
        membership = read_membership(conn, source)
        assert list(membership.columns) == previous_columns[source] + ["selection_confidence"]
        selected = select_projection(candidates, membership, slots=3)
        assert selected.index_code.tolist() == ["I3", "I1"]
        assert selected.selection_confidence.tolist() == ["fallback", "confirmed"]
        assert selected.target_weight.tolist() == pytest.approx([1 / 3, 1 / 3])

    assert read_membership(
        conn, "fund_pool_on.industry_cluster_managed_as_of('2026-10-01 09:29:59+08')"
    ).empty
    assert read_membership(
        conn, "fund_pool_on.industry_cluster_managed_as_of('2026-11-02 09:30+08')"
    ).empty
    assert auto.apply_schema(isolated_database_url, upgraded["plan_hash"]) == result
