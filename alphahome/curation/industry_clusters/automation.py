"""Stable industry library discovery and once-per-month cluster publication."""

from __future__ import annotations

from datetime import date, datetime, time
from contextlib import contextmanager
import gzip
from hashlib import sha256
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from psycopg2.extras import Json

from .. import etf_usable_pool_monthly as monthly_pool
from .data import load_inputs
from .engine import ClusterConfig
from .maintenance import stable_hash
from .service import run_history, save_json
from .store import CURRENT_VIEW_SQL, publish_series

TZ = ZoneInfo("Asia/Shanghai")
LIBRARY_ID = "a_share_industry_v1"
SEED_UNIVERSE = "industry_usable_202608_v1"
LIBRARY_TASK = "industry_index_library"
CLUSTER_TASK = "industry_clusters_monthly"
OUTPUT_ROOT = Path(__file__).resolve().parents[3] / "outputs/industry_clusters/managed"
LEGACY_CANDIDATE_POLICY = {
    "version": "industry_library_monthly_v1",
    "region": "中国A股",
    "module": "行业板块",
    "seed_universe": SEED_UNIVERSE,
    "remove_on_etf_ineligibility": False,
    "configuration": "industry_minimax_v2",
    "discovery": "candidate_archive_with_verified_tracking_identity",
}
POLICY = {
    **LEGACY_CANDIDATE_POLICY,
    "version": "industry_library_monthly_v2",
    "discovery": "qualified_monthly_usable_pool_with_verified_tracking_identity",
    "usable_pool_policy_version": monthly_pool.POLICY["version"],
}
QUALIFIED_STATUSES = ("PRIMARY", "BACKUP", "STANDALONE")

SCHEMA_SQL = CURRENT_VIEW_SQL + """
ALTER TABLE fund_pool_on.industry_cluster_batch
 DROP CONSTRAINT IF EXISTS industry_cluster_batch_record_kind_check;
ALTER TABLE fund_pool_on.industry_cluster_batch
 ADD CONSTRAINT industry_cluster_batch_record_kind_check
 CHECK(record_kind IN ('historical_reconstruction','observed'));
CREATE TABLE IF NOT EXISTS fund_pool_on.industry_index_library (
 library_id text PRIMARY KEY, seed_batch_id text NOT NULL
 REFERENCES fund_pool_on.industry_cluster_batch(batch_id),
 policy jsonb NOT NULL, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS fund_pool_on.industry_index_library_revision (
 revision_id text PRIMARY KEY, library_id text NOT NULL
 REFERENCES fund_pool_on.industry_index_library(library_id),
 previous_revision_id text REFERENCES fund_pool_on.industry_index_library_revision(revision_id),
 members jsonb NOT NULL CHECK(jsonb_array_length(members)>0),
 events jsonb NOT NULL, source_hash text NOT NULL, plan_hash text NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS industry_library_revision_time
 ON fund_pool_on.industry_index_library_revision(library_id,recorded_at);
CREATE OR REPLACE VIEW fund_pool_on.industry_index_library_latest AS
 SELECT DISTINCT ON (library_id) * FROM fund_pool_on.industry_index_library_revision
 ORDER BY library_id,recorded_at DESC,revision_id DESC;
CREATE OR REPLACE VIEW fund_pool_on.industry_index_library_current AS
 SELECT r.library_id,r.revision_id,r.recorded_at AS revision_available_at,
 m->>'index_code' AS index_code,m->>'index_name' AS index_name,
 m->>'status' AS status,(m->>'first_seen_at')::timestamptz AS first_seen_at,
 (m->>'retired_at')::timestamptz AS retired_at,m AS detail
 FROM fund_pool_on.industry_index_library_latest r
 CROSS JOIN LATERAL jsonb_array_elements(r.members) m;
CREATE TABLE IF NOT EXISTS fund_pool_on.industry_cluster_monthly_publication (
 library_id text NOT NULL REFERENCES fund_pool_on.industry_index_library(library_id),
 maintenance_month date NOT NULL,
 revision_id text NOT NULL REFERENCES fund_pool_on.industry_index_library_revision(revision_id),
 cluster_batch_id text NOT NULL REFERENCES fund_pool_on.industry_cluster_batch(batch_id),
 plan_hash text NOT NULL, decision_cutoff timestamptz NOT NULL,
 scheduled_effective_at timestamptz NOT NULL, available_from timestamptz NOT NULL,
 effective_to timestamptz NOT NULL,
 record_kind text NOT NULL CHECK(record_kind IN ('historical_reconstruction','observed')),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(available_from>=scheduled_effective_at),
 PRIMARY KEY(library_id,maintenance_month)
);
CREATE OR REPLACE VIEW fund_pool_on.industry_cluster_managed_current AS
 WITH latest AS (
 SELECT DISTINCT ON (library_id) * FROM fund_pool_on.industry_cluster_monthly_publication
 ORDER BY library_id,maintenance_month DESC
 )
 SELECT p.library_id,p.maintenance_month,p.revision_id,p.available_from,p.effective_to,
 p.record_kind AS publication_kind,b.universe_id,b.asof_date,b.algorithm_version,
 b.recorded_at AS cluster_recorded_at,m.*,
 m.detail->>'selection_group_id' AS selection_group_id,
 m.detail->>'selection_method' AS selection_method,
 (m.detail->>'selection_eligible')::boolean AS selection_eligible,
 (m.detail->>'can_represent_selection_group')::boolean AS can_represent_selection_group,
 m.detail->>'selection_confidence' AS selection_confidence
 FROM latest p JOIN fund_pool_on.industry_cluster_batch b ON b.batch_id=p.cluster_batch_id
 JOIN fund_pool_on.industry_cluster_member m ON m.batch_id=b.batch_id;
CREATE OR REPLACE FUNCTION fund_pool_on.industry_cluster_managed_as_of(p_at timestamptz)
RETURNS SETOF fund_pool_on.industry_cluster_managed_current LANGUAGE sql STABLE AS $body$
 SELECT p.library_id,p.maintenance_month,p.revision_id,p.available_from,p.effective_to,
 p.record_kind,b.universe_id,b.asof_date,b.algorithm_version,b.recorded_at,m.*,
 m.detail->>'selection_group_id',m.detail->>'selection_method',
 (m.detail->>'selection_eligible')::boolean,
 (m.detail->>'can_represent_selection_group')::boolean,
 m.detail->>'selection_confidence'
 FROM fund_pool_on.industry_cluster_monthly_publication p
 JOIN fund_pool_on.industry_cluster_batch b ON b.batch_id=p.cluster_batch_id
 JOIN fund_pool_on.industry_cluster_member m ON m.batch_id=b.batch_id
 WHERE p.record_kind='observed' AND p.recorded_at<=p_at
 AND b.recorded_at<=p_at AND p.available_from<=p_at AND p_at<p.effective_to;
$body$;
"""
OBJECTS = (
    "industry_index_library",
    "industry_index_library_revision",
    "industry_index_library_current",
    "industry_cluster_monthly_publication",
    "industry_cluster_managed_current",
)
SELECTION_VIEWS = ("industry_cluster_current", "industry_cluster_managed_current")


class AutomationError(RuntimeError):
    pass


@contextmanager
def _connection(database_url):
    conn = psycopg2.connect(database_url)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def _query(conn, sql, args=()):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return [
            dict(zip([c.name for c in cur.description], row)) for row in cur.fetchall()
        ]


def _clock(now=None):
    return now or datetime.now(TZ)


def _check_day(run_date, now):
    if run_date > now.astimezone(TZ).date():
        raise AutomationError("maintenance date cannot be in the future")


def schema_plan(conn):
    missing = [
        name
        for name in OBJECTS
        if not _query(
            conn, "SELECT to_regclass(%s) AS relation", ("fund_pool_on." + name,)
        )[0]["relation"]
    ]
    if not _query(
        conn,
        "SELECT to_regprocedure('fund_pool_on.industry_cluster_managed_as_of(timestamptz)') AS function",
    )[0]["function"]:
        missing.append("industry_cluster_managed_as_of(timestamptz)")
    confidence_columns = _query(
        conn,
        """SELECT table_name FROM information_schema.columns
        WHERE table_schema='fund_pool_on' AND table_name=ANY(%s)
          AND column_name='selection_confidence' AND data_type='text'""",
        (list(SELECTION_VIEWS),),
    )
    present = {row["table_name"] for row in confidence_columns}
    missing_columns = [
        name + ".selection_confidence" for name in SELECTION_VIEWS if name not in present
    ]
    constraints = _query(
        conn,
        """SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint
       WHERE conrelid='fund_pool_on.industry_cluster_batch'::regclass
       AND conname='industry_cluster_batch_record_kind_check'""",
    )
    constraint = constraints[0]["definition"] if constraints else ""
    body = {
        "migration": "industry_cluster_automation_v2",
        "missing_objects": missing,
        "missing_columns": missing_columns,
        "observed_kind_supported": "observed" in constraint,
        "sql_hash": stable_hash(SCHEMA_SQL),
    }
    return {
        **body,
        "plan_hash": stable_hash(body),
        "status": "ready" if missing or missing_columns or "observed" not in constraint else "no_op",
    }


def apply_schema(database_url, expected_plan_hash):
    with _connection(database_url) as conn:
        plan = schema_plan(conn)
        if plan["plan_hash"] != expected_plan_hash:
            raise AutomationError("schema plan changed")
        if plan["status"] == "ready":
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
        return {"status": "success", "migration": plan["migration"]}


def _ready(conn, *, allow_legacy=False):
    plan = schema_plan(conn)
    if plan["status"] != "no_op":
        raise AutomationError("migration_required")
    registered = _query(
        conn,
        "SELECT policy FROM fund_pool_on.industry_index_library WHERE library_id=%s",
        (LIBRARY_ID,),
    )
    policy = registered[0]["policy"] if registered else None
    if policy not in (None, POLICY) and not (
        allow_legacy and policy == LEGACY_CANDIDATE_POLICY
    ):
        raise AutomationError("library policy change requires a new library identity")
    return policy


def _latest_revision(conn, before=None):
    extra = " AND recorded_at<=%s" if before is not None else ""
    args = (LIBRARY_ID, before) if before is not None else (LIBRARY_ID,)
    rows = _query(
        conn,
        "SELECT * FROM fund_pool_on.industry_index_library_revision "
        "WHERE library_id=%s"
        + extra
        + " ORDER BY recorded_at DESC,revision_id DESC LIMIT 1",
        args,
    )
    return rows[0] if rows else None


def _seed(conn):
    complete_month = monthly_pool.last_complete_month(_clock().astimezone(TZ).date())
    cutoff = (pd.Timestamp(complete_month) + pd.offsets.MonthEnd(0)).date()
    rows = _query(
        conn,
        """SELECT b.*,u.members FROM fund_pool_on.industry_cluster_batch b
       JOIN fund_pool_on.industry_cluster_universe u USING(universe_id)
       WHERE b.universe_id=%s AND b.algorithm_version='industry_minimax_v2'
       AND b.asof_date<=%s ORDER BY b.asof_date DESC,b.recorded_at DESC,b.batch_id DESC LIMIT 1""",
        (SEED_UNIVERSE, cutoff),
    )
    if len(rows) != 1:
        raise AutomationError("seed_baseline_required")
    return rows[0]


def _revision_pool_basis(revision):
    if revision:
        for event in reversed(revision.get("events", [])):
            if event["event"] == "usable_pool_basis":
                return event["pool_basis"]
    return None


def _pool_basis(conn, known_at, previous_month=None):
    """Consume published, qualified complete-month snapshots in month order."""
    complete = monthly_pool.last_complete_month(known_at.astimezone(TZ).date())
    versions = _query(
        conn,
        """SELECT DISTINCT ON (maintenance_month) snapshot_id,maintenance_month,
          facts_cutoff,decision_cutoff,effective_from,recorded_at,record_kind,
          policy->>'version' AS policy_version
        FROM fund_pool_on.etf_usable_pool_monthly_batch
        WHERE recorded_at<=%s AND maintenance_month<=%s
          AND policy->>'version'=%s
        ORDER BY maintenance_month,recorded_at DESC,snapshot_id DESC""",
        (known_at, complete, POLICY["usable_pool_policy_version"]),
    )
    if not versions:
        raise AutomationError("qualified_monthly_usable_pool_required")
    if previous_month:
        prior = date.fromisoformat(previous_month)
        following = [r for r in versions if r["maintenance_month"] > prior]
        if following:
            selected = following[0]
            expected = (pd.Timestamp(prior) + pd.offsets.MonthBegin(1)).date()
            if selected["maintenance_month"] != expected:
                raise AutomationError("qualified_monthly_usable_pool_month_gap")
        else:
            matching = [r for r in versions if r["maintenance_month"] == prior]
            if not matching:
                raise AutomationError("previous_qualified_monthly_pool_missing")
            selected = matching[0]
    else:
        selected = versions[-1]
    return {
        k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in selected.items()
    }


def _sources(conn, known_at, pool_basis=None):
    # The monthly usable pool supplies eligibility; the archive only supplies labels.
    basis = pool_basis or _pool_basis(conn, known_at)
    return _query(
        conn,
        """SELECT c.fund_code,c.tracking_index_code AS index_code,
        c.tracking_index_name AS index_name,c.region_market,c.allocation_module,
        c.confirmation_status,c.include_in_candidate_pool,c.loaded_at,c.confirmation_at,
        e.index_code AS verified_index_code,e.list_date,
        b.name AS official_index_name,b.list_date AS index_list_date,
        p.selection_status AS pool_selection_status,p.snapshot_id AS pool_snapshot_id,
        p.aum_100m AS pool_aum_100m,p.amount_20d_100m AS pool_amount_20d_100m,
        p.listing_age_months AS pool_listing_age_months
       FROM fund_pool_on.etf_usable_pool_monthly_snapshot p
       JOIN fund_pool_on.etf_candidate_master_current_enriched c USING(fund_code)
       LEFT JOIN rawdata.fund_etf_basic e ON e.ts_code=c.fund_code
       LEFT JOIN rawdata.index_basic b ON b.ts_code=c.tracking_index_code
       WHERE p.snapshot_id=%s AND p.selection_status=ANY(%s)
         AND c.loaded_at<=%s AND (c.confirmation_at IS NULL OR c.confirmation_at<=%s)
         AND (p.delist_date IS NULL OR p.delist_date>%s)
       ORDER BY c.fund_code""",
        (
            basis["snapshot_id"],
            list(QUALIFIED_STATUSES),
            known_at,
            known_at,
            known_at.astimezone(TZ).date(),
        ),
    )


def reconcile_members(previous, sources, seen_at):
    """Retain established identities; admit only scoped, verified new indices."""
    members = {m["index_code"]: dict(m) for m in previous}
    evidence, events, pending = {}, [], []
    for row in sources:
        if (
            row["region_market"] != POLICY["region"]
            or row["allocation_module"] != POLICY["module"]
        ):
            continue
        code = row["index_code"]
        reason = None
        if row.get("pool_selection_status") not in QUALIFIED_STATUSES:
            reason = "not_in_qualified_monthly_usable_pool"
        elif (
            not row["include_in_candidate_pool"]
            or row["confirmation_status"] == "HUMAN_REJECTED"
        ):
            reason = "candidate_rejected"
        elif not code or row["verified_index_code"] != code:
            reason = "tracking_identity_missing_or_conflicting"
        elif (
            not row["list_date"]
            or pd.Timestamp(row["list_date"]).date() > seen_at.date()
        ):
            reason = "listing_not_yet_known"
        if reason:
            pending.append(
                {"fund_code": row["fund_code"], "index_code": code, "reason": reason}
            )
            continue
        item = evidence.setdefault(
            code,
            {
                "index_name": row["official_index_name"] or row["index_name"] or code,
                "source_funds": [],
                "classification_review_required": False,
            },
        )
        item["source_funds"].append(row["fund_code"])
        item["classification_review_required"] |= (
            row["confirmation_status"] == "AI_REVIEW_REQUIRED"
        )
    for code, item in sorted(evidence.items()):
        if code not in members:
            members[code] = {
                "index_code": code,
                "status": "active",
                "first_seen_at": seen_at.isoformat(),
                "retired_at": None,
                **item,
            }
            events.append({"event": "index_added", "index_code": code, **item})
        elif (
            members[code]["status"] == "active"
            and members[code]["index_name"] != item["index_name"]
        ):
            events.append(
                {
                    "event": "index_name_changed",
                    "index_code": code,
                    "before": members[code]["index_name"],
                    "after": item["index_name"],
                }
            )
            members[code]["index_name"] = item["index_name"]
    # A disappeared carrier or changed AI category never silently retires an index.
    return sorted(members.values(), key=lambda x: x["index_code"]), events, pending


def build_library_plan(conn, run_date, *, observed_at=None):
    observed_at = _clock(observed_at)
    _check_day(run_date, observed_at)
    registered_policy = _ready(conn, allow_legacy=True)
    correcting_policy = registered_policy == LEGACY_CANDIDATE_POLICY
    latest = _latest_revision(conn)
    seed = _seed(conn) if latest is None else None
    known_at = min(observed_at, datetime.combine(run_date, time.max, TZ))
    prior_basis = _revision_pool_basis(latest)
    pool_basis = _pool_basis(
        conn, known_at, prior_basis["maintenance_month"] if prior_basis else None
    )
    source = _sources(conn, known_at, pool_basis)
    previous = (
        latest["members"]
        if latest
        else [
            {
                **m,
                "status": "active",
                "first_seen_at": seed["recorded_at"].isoformat(),
                "retired_at": None,
                "source_funds": [],
                "classification_review_required": False,
            }
            for m in seed["members"]
        ]
    )
    correction_events = []
    if correcting_policy:
        original = _query(
            conn,
            """SELECT members FROM fund_pool_on.industry_index_library_revision
            WHERE library_id=%s AND previous_revision_id IS NULL
            ORDER BY recorded_at,revision_id LIMIT 1""",
            (LIBRARY_ID,),
        )
        if not latest or not original:
            raise AutomationError("original_library_seed_required_for_correction")
        seed_codes = {m["index_code"] for m in original[0]["members"]}
        qualified, _, _ = reconcile_members([], source, known_at)
        qualified_codes = {m["index_code"] for m in qualified}
        withdrawn = [
            m for m in previous if m["index_code"] not in seed_codes | qualified_codes
        ]
        previous = [m for m in previous if m not in withdrawn]
        correction_events = [
            {
                "event": "discovery_policy_corrected",
                "from_policy": registered_policy,
                "to_policy": POLICY,
                "reason": "ETF策略母库新增准入必须来自达标月度可用池",
            },
            *[
                {
                    "event": "index_admission_retracted",
                    "index_code": m["index_code"],
                    "index_name": m["index_name"],
                    "source_funds": m.get("source_funds", []),
                    "reason": "candidate_archive_admission_without_qualified_usable_etf",
                }
                for m in withdrawn
            ],
        ]
    members, events, pending = reconcile_members(previous, source, known_at)
    events = correction_events + events
    source_hash = stable_hash(json.loads(json.dumps(source, default=str)))
    if (
        not latest
        or events
        or latest["source_hash"] != source_hash
        or prior_basis != pool_basis
    ):
        events.append({"event": "usable_pool_basis", "pool_basis": pool_basis})
    body = {
        "library_id": LIBRARY_ID,
        "policy": POLICY,
        "observed_at": observed_at.isoformat(),
        "run_date": run_date.isoformat(),
        "source_hash": source_hash,
        "pool_basis": pool_basis,
        "correcting_policy": correcting_policy,
        "previous_revision_id": latest["revision_id"] if latest else None,
        "seed_batch_id": seed["batch_id"] if seed else None,
        "seed_members": previous if seed else None,
        "members": members,
        "events": events,
        "pending": pending,
        "bootstrap": latest is None,
    }
    return {
        **body,
        "plan_hash": stable_hash(body),
        "status": "ready" if latest is None or events else "no_op",
    }


def _insert_revision(conn, members, events, source_hash, plan_hash, previous_id):
    revision_id = (
        "IL_" + stable_hash([LIBRARY_ID, previous_id, members, source_hash])[:24]
    )
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO fund_pool_on.industry_index_library_revision
          (revision_id,library_id,previous_revision_id,members,events,source_hash,plan_hash)
          VALUES(%s,%s,%s,%s,%s,%s,%s)""",
            (
                revision_id,
                LIBRARY_ID,
                previous_id,
                Json(members),
                Json(events),
                source_hash,
                plan_hash,
            ),
        )
    return revision_id


def _schedule(conn, maintenance_month):
    return monthly_pool.schedule(
        maintenance_month,
        monthly_pool._sessions(conn, maintenance_month, maintenance_month),
    )


def _validate_plan(plan, expected_plan_hash):
    body = {k: v for k, v in plan.items() if k not in {"plan_hash", "status"}}
    if (
        plan.get("plan_hash") != expected_plan_hash
        or stable_hash(body) != expected_plan_hash
    ):
        raise AutomationError("saved plan content or hash changed")


def execute_library_plan(database_url, plan, expected_plan_hash):
    _validate_plan(plan, expected_plan_hash)
    if plan.get("policy") != POLICY:
        raise AutomationError("saved plan uses superseded discovery policy; re-preview")
    with _connection(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (LIBRARY_ID,))
        reused = _query(
            conn,
            """SELECT revision_id,members FROM fund_pool_on.industry_index_library_revision
           WHERE library_id=%s AND plan_hash=%s ORDER BY recorded_at DESC,revision_id DESC LIMIT 1""",
            (LIBRARY_ID, expected_plan_hash),
        )
        if reused:
            return {
                "status": "no_op",
                "revision_id": reused[0]["revision_id"],
                "indices": len(reused[0]["members"]),
                "reused_plan": True,
            }
        fresh = build_library_plan(
            conn,
            date.fromisoformat(plan["run_date"]),
            observed_at=datetime.fromisoformat(plan["observed_at"]),
        )
        if fresh["plan_hash"] != expected_plan_hash:
            raise AutomationError("library source or state changed; re-preview")
        if fresh["status"] == "no_op":
            return {
                "status": "no_op",
                "revision_id": fresh["previous_revision_id"],
                "indices": len(fresh["members"]),
                "pending": len(fresh["pending"]),
            }
        previous = fresh["previous_revision_id"]
        if fresh["correcting_policy"]:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE fund_pool_on.industry_index_library SET policy=%s
                    WHERE library_id=%s AND policy=%s""",
                    (Json(POLICY), LIBRARY_ID, Json(LEGACY_CANDIDATE_POLICY)),
                )
                if cur.rowcount != 1:
                    raise AutomationError("discovery correction predecessor changed")
        if fresh["bootstrap"]:
            seed = _seed(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fund_pool_on.industry_index_library(library_id,seed_batch_id,policy) VALUES(%s,%s,%s)",
                    (LIBRARY_ID, seed["batch_id"], Json(POLICY)),
                )
            previous = _insert_revision(
                conn,
                fresh["seed_members"],
                [{"event": "seed_imported", "batch_id": seed["batch_id"]}],
                seed["input_hash"],
                expected_plan_hash,
                None,
            )
            month = seed["asof_date"].replace(day=1)
            timing = _schedule(conn, month)
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO fund_pool_on.industry_cluster_monthly_publication
                  (library_id,maintenance_month,revision_id,cluster_batch_id,plan_hash,decision_cutoff,
                   scheduled_effective_at,available_from,effective_to,record_kind)
                  VALUES(%s,%s,%s,%s,%s,%s,%s,GREATEST(%s::timestamptz,clock_timestamp()),%s,'historical_reconstruction')""",
                    (
                        LIBRARY_ID,
                        month,
                        previous,
                        seed["batch_id"],
                        expected_plan_hash,
                        timing["decision_cutoff"],
                        timing["scheduled_effective_at"],
                        timing["scheduled_effective_at"],
                        timing["effective_to"],
                    ),
                )
        revision = previous
        if fresh["events"]:
            revision = _insert_revision(
                conn,
                fresh["members"],
                fresh["events"],
                fresh["source_hash"],
                expected_plan_hash,
                previous,
            )
        return {
            "status": "success",
            "revision_id": revision,
            "indices": len(fresh["members"]),
            "added": sum(e["event"] == "index_added" for e in fresh["events"]),
            "retracted": sum(
                e["event"] == "index_admission_retracted" for e in fresh["events"]
            ),
            "pool_month": fresh["pool_basis"]["maintenance_month"],
            "pending": len(fresh["pending"]),
            "bootstrap": fresh["bootstrap"],
        }


def preview_library(database_url, *, run_date):
    with _connection(database_url) as conn:
        conn.set_session(readonly=True)
        if schema_plan(conn)["status"] != "no_op":
            return {"status": "migration_required"}
        plan = build_library_plan(conn, run_date)
        return {
            "status": plan["status"],
            "indices": len(plan["members"]),
            "added": sum(e["event"] == "index_added" for e in plan["events"]),
            "retracted": sum(
                e["event"] == "index_admission_retracted" for e in plan["events"]
            ),
            "pool_month": plan["pool_basis"]["maintenance_month"],
            "pending": len(plan["pending"]),
            "bootstrap": plan["bootstrap"],
        }


def refresh_library(database_url, *, run_date):
    completed = []
    while True:
        result = _refresh_library_once(database_url, run_date=run_date)
        if result["status"] == "no_op":
            if not completed:
                return result
            return {
                **completed[-1],
                "added": sum(r.get("added", 0) for r in completed),
                "retracted": sum(r.get("retracted", 0) for r in completed),
                "completed_revisions": completed,
            }
        completed.append(result)


def _refresh_library_once(database_url, *, run_date):
    with _connection(database_url) as conn:
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        plan = build_library_plan(conn, run_date)
    if plan["status"] == "no_op":
        return {
            "status": "no_op",
            "revision_id": plan["previous_revision_id"],
            "indices": len(plan["members"]),
            "added": 0,
            "pending": len(plan["pending"]),
        }
    folder = OUTPUT_ROOT / "library" / plan["plan_hash"][:20]
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "plan.json").write_text(
        json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    result = execute_library_plan(database_url, plan, plan["plan_hash"])
    (folder / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


def _revision_for_pool_month(conn, month, known_at):
    rows = _query(
        conn,
        """SELECT r.* FROM fund_pool_on.industry_index_library_revision r
        WHERE library_id=%s AND recorded_at<=%s
          AND EXISTS (
            SELECT 1 FROM jsonb_array_elements(r.events) e
            WHERE e->>'event'='usable_pool_basis'
              AND e->'pool_basis'->>'maintenance_month'=%s
          )
        ORDER BY recorded_at DESC,revision_id DESC LIMIT 1""",
        (LIBRARY_ID, known_at, month.isoformat()),
    )
    return rows[0] if rows else None


def build_cluster_plan(conn, run_date, *, now=None):
    now = _clock(now)
    _check_day(run_date, now)
    _ready(conn)
    publications = _query(
        conn,
        """SELECT p.*,b.maintenance_state,b.config,b.config_hash
       FROM fund_pool_on.industry_cluster_monthly_publication p
       JOIN fund_pool_on.industry_cluster_batch b ON b.batch_id=p.cluster_batch_id
       WHERE p.library_id=%s ORDER BY p.maintenance_month DESC LIMIT 1""",
        (LIBRARY_ID,),
    )
    if not publications:
        return {"status": "bootstrap_required"}
    previous = publications[0]
    end = monthly_pool.last_complete_month(run_date)
    month = (
        pd.Timestamp(previous["maintenance_month"]) + pd.offsets.MonthBegin(1)
    ).date()
    if month > end:
        return {
            "status": "no_op",
            "through_month": previous["maintenance_month"].isoformat(),
        }
    timing = _schedule(conn, month)
    cutoff = datetime.fromisoformat(timing["decision_cutoff"])
    if now < cutoff:
        return {
            "status": "expected_no_data",
            "reason": "decision_window_not_complete",
            "decision_cutoff": cutoff.isoformat(),
        }
    # The pool is computed after its 09:00 facts window. A library computed before
    # the actual fill can serve this month; its source month must match exactly.
    revision = _revision_for_pool_month(conn, month, now)
    if revision is None:
        return {
            "status": "blocked",
            "reason": "library_pool_month_not_ready",
            "required_pool_month": month.isoformat(),
        }
    asof = (pd.Timestamp(month) + pd.offsets.MonthEnd(0)).date()
    last_session = _query(
        conn,
        "SELECT max(cal_date) AS day FROM rawdata.others_calendar WHERE exchange='SSE' AND is_open=1 AND cal_date<=%s",
        (asof,),
    )[0]["day"]
    benchmark = _query(
        conn,
        "SELECT max(trade_date) AS day FROM rawdata.index_factor_pro WHERE ts_code=%s AND trade_date<=%s AND close>0",
        (previous["config"]["benchmark"], asof),
    )[0]["day"]
    if benchmark != last_session:
        return {
            "status": "blocked",
            "reason": "benchmark_month_end_not_ready",
            "required_date": str(last_session),
            "price_date": str(benchmark),
        }
    members = [
        {"index_code": m["index_code"], "index_name": m["index_name"]}
        for m in revision["members"]
        if m["status"] == "active"
    ]
    if not members:
        raise AutomationError("empty industry library")
    config = ClusterConfig.from_dict(previous["config"])
    body = {
        "library_id": LIBRARY_ID,
        "maintenance_month": month.isoformat(),
        "asof_date": asof.isoformat(),
        "revision_id": revision["revision_id"],
        "members": members,
        "previous_batch_id": previous["cluster_batch_id"],
        "config": config.to_dict(),
        "timing": timing,
        "run_date": run_date.isoformat(),
        "universe_id": LIBRARY_ID + "__" + stable_hash(members)[:16],
    }
    return {**body, "plan_hash": stable_hash(body), "status": "ready"}


def preview_clusters(database_url, *, run_date):
    with _connection(database_url) as conn:
        conn.set_session(readonly=True)
        if schema_plan(conn)["status"] != "no_op":
            return {"status": "migration_required"}
        plan = build_cluster_plan(conn, run_date)
        return {k: v for k, v in plan.items() if k not in {"members", "config"}}


def first_available_open(conn, scheduled, published_at):
    threshold = max(scheduled, published_at)
    sessions = _query(
        conn,
        "SELECT cal_date AS day FROM rawdata.others_calendar WHERE exchange='SSE' AND is_open=1 AND cal_date>=%s ORDER BY cal_date LIMIT 10",
        (threshold.astimezone(TZ).date(),),
    )
    for row in sessions:
        opening = datetime.combine(row["day"], time(9, 30), TZ)
        if opening >= threshold:
            return opening
    raise AutomationError("future exchange calendar missing")


def _source_frame_hash(inputs):
    """Ignore gzip timestamps when comparing actual source rows across fresh reads."""
    hashes = {}
    for name in ("members", "classification", "quotes", "calendar", "index_metadata"):
        digest = sha256()
        with gzip.open(Path(inputs["cache_dir"]) / (name + ".csv.gz"), "rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        hashes[name] = digest.hexdigest()
    return stable_hash(hashes)


def execute_cluster_plan(
    database_url, plan, expected_plan_hash, *, stop_requested=None
):
    _validate_plan(plan, expected_plan_hash)
    folder = (
        OUTPUT_ROOT
        / "monthly"
        / (plan["maintenance_month"][:7] + "_" + expected_plan_hash[:12])
    )
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "plan.json").write_text(
        json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if stop_requested and stop_requested():
        return {"status": "cancelled"}
    with _connection(database_url) as conn:
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        prior_publication = _query(
            conn,
            """SELECT cluster_batch_id,available_from
           FROM fund_pool_on.industry_cluster_monthly_publication
           WHERE library_id=%s AND maintenance_month=%s""",
            (LIBRARY_ID, plan["maintenance_month"]),
        )
        if prior_publication:
            return {
                "status": "no_op",
                "cluster_batch_id": prior_publication[0]["cluster_batch_id"],
                "available_from": prior_publication[0]["available_from"].isoformat(),
            }
        fresh = build_cluster_plan(conn, date.fromisoformat(plan["run_date"]))
        if fresh.get("status") == "no_op":
            return fresh
        if fresh.get("plan_hash") != expected_plan_hash:
            raise AutomationError("cluster plan or predecessor changed")
        prior = _query(
            conn,
            "SELECT batch_id,maintenance_state FROM fund_pool_on.industry_cluster_batch WHERE batch_id=%s",
            (plan["previous_batch_id"],),
        )[0]
        config = ClusterConfig.from_dict(plan["config"])
        universe = pd.DataFrame(plan["members"])
        # Fresh per-attempt cache: reruns do not accidentally consume yesterday's source cache.
        from uuid import uuid4

        attempt = folder / ("attempt_" + uuid4().hex[:12])
        inputs = load_inputs(
            conn,
            universe,
            plan["asof_date"],
            plan["asof_date"],
            attempt / "inputs",
            config,
        )
        summary = run_history(
            inputs,
            universe,
            plan["asof_date"],
            plan["asof_date"],
            config,
            attempt,
            plan["universe_id"],
            "Managed stable A-share industry library",
            {"batch_id": prior["batch_id"], "state": prior["maintenance_state"]},
        )
    if stop_requested and stop_requested():
        return {"status": "cancelled", "output_dir": str(attempt)}
    with gzip.open(attempt / "series.json.gz", "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    payload["record_kind"] = "observed"
    payload["automation_plan_hash"] = expected_plan_hash
    payload["library_revision_id"] = plan["revision_id"]
    save_json(attempt / "publication_payload.json.gz", payload)
    with _connection(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (LIBRARY_ID,))
        existing = _query(
            conn,
            "SELECT * FROM fund_pool_on.industry_cluster_monthly_publication WHERE library_id=%s AND maintenance_month=%s",
            (LIBRARY_ID, plan["maintenance_month"]),
        )
        if existing:
            return {
                "status": "no_op",
                "cluster_batch_id": existing[0]["cluster_batch_id"],
            }
        fresh = build_cluster_plan(conn, date.fromisoformat(plan["run_date"]))
        if fresh.get("plan_hash") != expected_plan_hash:
            raise AutomationError(
                "cluster predecessor or library changed before publication"
            )
        verified_inputs = load_inputs(
            conn,
            universe,
            plan["asof_date"],
            plan["asof_date"],
            attempt / "publication_source_check",
            config,
        )
        if _source_frame_hash(verified_inputs) != _source_frame_hash(inputs):
            raise AutomationError(
                "cluster sources changed during calculation; re-preview"
            )
        if stop_requested and stop_requested():
            return {"status": "cancelled", "output_dir": str(attempt)}
        published = publish_series(
            conn, payload, manage_transaction=False, create_schema=False
        )
        now = _query(conn, "SELECT clock_timestamp() AS now")[0]["now"]
        available = first_available_open(
            conn, datetime.fromisoformat(plan["timing"]["scheduled_effective_at"]), now
        )
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO fund_pool_on.industry_cluster_monthly_publication
             (library_id,maintenance_month,revision_id,cluster_batch_id,plan_hash,decision_cutoff,
              scheduled_effective_at,available_from,effective_to,record_kind)
             VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,'observed')""",
                (
                    LIBRARY_ID,
                    plan["maintenance_month"],
                    plan["revision_id"],
                    published["last_batch_id"],
                    expected_plan_hash,
                    plan["timing"]["decision_cutoff"],
                    plan["timing"]["scheduled_effective_at"],
                    available,
                    plan["timing"]["effective_to"],
                ),
            )
    result = {
        "status": "success",
        "maintenance_month": plan["maintenance_month"],
        "cluster_batch_id": published["last_batch_id"],
        "revision_id": plan["revision_id"],
        "available_from": available.isoformat(),
        "scheduled_effective_at": plan["timing"]["scheduled_effective_at"],
        "indices": len(plan["members"]),
        "summary": summary["latest"],
        "output_dir": str(attempt),
    }
    (folder / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


def refresh_clusters(database_url, *, run_date, stop_requested=None):
    completed = []
    while True:
        with _connection(database_url) as conn:
            conn.set_session(readonly=True)
            plan = build_cluster_plan(conn, run_date)
        if plan["status"] != "ready":
            if plan["status"] in {"blocked", "bootstrap_required"}:
                return {
                    "status": "error",
                    "error": plan.get("reason", plan["status"]),
                    "completed_months": completed,
                    "preview": plan,
                }
            return {
                "status": (
                    "success"
                    if completed and plan["status"] == "no_op"
                    else plan["status"]
                ),
                "completed_months": completed,
                "preview": plan,
            }
        result = execute_cluster_plan(
            database_url, plan, plan["plan_hash"], stop_requested=stop_requested
        )
        if result["status"] not in {"success", "no_op"}:
            return result
        completed.append(result)


def retire_index(database_url, index_code, reason):
    """Explicit confirmed exit. Disappearance from the ETF pool is not an exit."""
    if not reason.strip():
        raise AutomationError("confirmed exit reason required")
    with _connection(database_url) as conn:
        _ready(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (LIBRARY_ID,))
        latest = _latest_revision(conn)
        if not latest:
            raise AutomationError("bootstrap_required")
        members = latest["members"]
        found = [m for m in members if m["index_code"] == index_code]
        if not found:
            raise AutomationError("unknown index")
        if found[0]["status"] == "retired":
            return {"status": "no_op", "revision_id": latest["revision_id"]}
        found[0].update(
            status="retired", retired_at=_clock().isoformat(), retirement_reason=reason
        )
        event = {"event": "index_retired", "index_code": index_code, "reason": reason}
        digest = stable_hash([latest["revision_id"], members, event])
        events = [event]
        basis = _revision_pool_basis(latest)
        if basis:
            events.append({"event": "usable_pool_basis", "pool_basis": basis})
        revision = _insert_revision(
            conn, members, events, digest, digest, latest["revision_id"]
        )
        return {"status": "success", "revision_id": revision}
