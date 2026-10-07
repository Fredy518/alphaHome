"""Deterministic ETF product eligibility and representative selection.

Candidate classification/grade remain an independent research archive. Pending
classification does not prevent objective screening or manufacture AI approval.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

from .deepseek_candidate_client import sha256_json
from .etf_candidate_ai_automation import FACT_MAX_LAG_TRADE_DAYS, _fact_quality_issues
from .etf_candidate_master import lock_candidate_master
from .exchange_fund_sources import listing_months

TASK_NAME = "etf_usable_pool"
POLICY = {
    "version": "exchange_fund_usable_pool_v4",
    "product_types": ["ETF", "LOF"],
    "lof_aum_maximum_age_days": 183,
    "lof_without_verified_index": "independent_product_no_same_index_backup",
    "minimum_age_months": 3,
    "age_basis": "complete_calendar_months_from_verified_listing_date",
    "minimum_aum_100m": 5,
    "minimum_amount_20d_100m": 0.3,
    "required_amount_days": 20,
    "group_by": "exposure_id",
    "primary_per_exposure": 1,
    "backup_per_exposure": 1,
    "backup_requires_same_index": True,
    "ranking": [
        "amount_20d_desc",
        "total_fee_asc",
        "aum_desc",
        "listing_age_desc",
        "fund_code_asc",
    ],
    "candidate_grade_is_gate": False,
    "ai_confirmation_is_gate": False,
    "premium_is_gate": False,
    "fact_max_lag_trade_days": FACT_MAX_LAG_TRADE_DAYS,
}

SOURCE_SQL = """
    SELECT c.fund_code, to_jsonb(c) AS candidate, to_jsonb(f) AS facts,
           jsonb_build_object(
               'fund_code', CASE WHEN f.product_type='LOF' THEN b.ts_code ELSE e.ts_code END,
               'tracking_index_code', e.index_code, 'product_type', f.product_type,
               'status', coalesce(b.status, e.status),
               'list_date', coalesce(b.list_date, e.list_date),
               'benchmark', e.benchmark
           ) AS identity,
           coalesce(d.decision_payload, '{}'::jsonb) AS review
    FROM fund_pool_on.etf_candidate_master_current c
    LEFT JOIN features.exchange_fund_product_facts_current f USING (fund_code)
    LEFT JOIN rawdata.fund_etf_basic e ON e.ts_code=c.fund_code
    LEFT JOIN rawdata.fund_basic b ON b.ts_code=c.fund_code
    LEFT JOIN fund_pool_on.etf_candidate_ai_decision d
      ON d.ai_run_id=c.ai_run_id AND d.fund_code=c.fund_code
"""
SOURCE_WITH_HASH_SQL = f"""
    SELECT x.*, md5((to_jsonb(x) - 'fund_code')::text) AS input_fingerprint
    FROM ({SOURCE_SQL}) x
"""

SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS fund_pool_on.etf_usable_pool_batch (
    selection_id text PRIMARY KEY,
    plan_hash text NOT NULL UNIQUE,
    source_snapshot_id text NOT NULL,
    facts_as_of date NOT NULL,
    evaluated_on date NOT NULL,
    available_from timestamptz NOT NULL DEFAULT clock_timestamp(),
    policy jsonb NOT NULL,
    summary jsonb NOT NULL,
    row_count integer NOT NULL CHECK (row_count > 0),
    capital_authority boolean NOT NULL DEFAULT false CHECK (NOT capital_authority),
    order_authority boolean NOT NULL DEFAULT false CHECK (NOT order_authority)
);
CREATE TABLE IF NOT EXISTS fund_pool_on.etf_usable_pool_snapshot (
    selection_id text NOT NULL REFERENCES fund_pool_on.etf_usable_pool_batch(selection_id),
    fund_code text NOT NULL,
    exposure_id text NOT NULL,
    selection_status text NOT NULL CHECK
      (selection_status IN ('PRIMARY','BACKUP','RESERVE','INELIGIBLE','BLOCKED')),
    exposure_rank integer,
    classification_review_required boolean NOT NULL,
    blocking_reasons jsonb NOT NULL,
    research_notes jsonb NOT NULL,
    input_fingerprint text NOT NULL,
    source_record jsonb NOT NULL,
    product_facts jsonb NOT NULL,
    diagnostics jsonb NOT NULL,
    PRIMARY KEY(selection_id, fund_code),
    CHECK (selection_status NOT IN ('PRIMARY','BACKUP') OR exposure_rank >= 1)
);
CREATE UNIQUE INDEX IF NOT EXISTS etf_usable_pool_one_role_per_exposure
ON fund_pool_on.etf_usable_pool_snapshot(selection_id, exposure_id, selection_status)
WHERE selection_status IN ('PRIMARY','BACKUP');

CREATE OR REPLACE VIEW fund_pool_on.etf_usable_pool_source_current AS
{SOURCE_WITH_HASH_SQL};

CREATE OR REPLACE VIEW fund_pool_on.etf_usable_pool_latest_batch AS
SELECT * FROM fund_pool_on.etf_usable_pool_batch
ORDER BY available_from DESC, selection_id DESC LIMIT 1;

CREATE OR REPLACE VIEW fund_pool_on.etf_usable_pool_history AS
SELECT s.selection_id, s.fund_code, s.exposure_id,
       s.source_record->>'exposure_name' AS exposure_name,
       s.source_record->>'fund_name' AS fund_name,
       s.product_facts->>'tracking_index_code' AS tracking_index_code,
       s.source_record->>'candidate_status' AS candidate_status,
       s.source_record->>'confirmation_status' AS confirmation_status,
       s.selection_status, s.exposure_rank, s.classification_review_required,
       s.blocking_reasons, s.research_notes,
       (s.product_facts->>'aum_100m')::numeric AS aum_100m,
       (s.product_facts->>'amount_20d_100m')::numeric AS amount_20d_100m,
       (s.product_facts->>'age_months')::numeric AS age_months,
       (s.product_facts->>'total_fee_pct')::numeric AS total_fee_pct,
       s.source_record, s.product_facts, s.diagnostics,
       b.source_snapshot_id, b.facts_as_of, b.evaluated_on, b.available_from,
       b.policy->>'version' AS policy_version,
       b.capital_authority, b.order_authority
FROM fund_pool_on.etf_usable_pool_snapshot s
JOIN fund_pool_on.etf_usable_pool_batch b USING(selection_id);

CREATE OR REPLACE VIEW fund_pool_on.etf_usable_pool_screening_current AS
WITH latest AS MATERIALIZED (SELECT * FROM fund_pool_on.etf_usable_pool_latest_batch),
inputs AS MATERIALIZED (
    SELECT fund_code, input_fingerprint FROM fund_pool_on.etf_usable_pool_source_current
),
calendar AS (
    SELECT array_agg(cal_date ORDER BY cal_date DESC) AS dates FROM (
        SELECT DISTINCT cal_date::date FROM rawdata.others_calendar
        WHERE exchange='SSE' AND is_open=1
          AND cal_date::date < (current_timestamp AT TIME ZONE 'Asia/Shanghai')::date
        ORDER BY cal_date DESC LIMIT 3
    ) d
),
source_state AS MATERIALIZED (
    SELECT b.selection_id,
       b.source_snapshot_id=(SELECT snapshot_id FROM fund_pool_on.etf_candidate_master_latest_batch)
       AND b.row_count=(SELECT count(*) FROM inputs)
       AND NOT EXISTS (
           SELECT 1 FROM fund_pool_on.etf_usable_pool_snapshot s
           LEFT JOIN inputs x USING(fund_code)
           WHERE s.selection_id=b.selection_id
             AND s.input_fingerprint IS DISTINCT FROM x.input_fingerprint
       ) AS source_unchanged
    FROM latest b
)
SELECT h.*, coalesce(v.source_unchanged AND cardinality(c.dates)=3
    AND h.facts_as_of BETWEEN c.dates[1] AND (current_timestamp AT TIME ZONE 'Asia/Shanghai')::date
    AND (h.product_facts->>'price_date')::date >= c.dates[1]
    AND (h.product_facts->>'nav_date')::date >= c.dates[3]
    AND CASE WHEN h.product_facts->>'product_type'='LOF' THEN
        (h.product_facts->>'aum_date')::date >= c.dates[1]-183
        AND (h.product_facts->>'aum_known_date')::date <= h.facts_as_of
        ELSE (h.product_facts->>'share_date')::date >= c.dates[3] END, false) AS is_current
FROM fund_pool_on.etf_usable_pool_history h
JOIN source_state v USING(selection_id) CROSS JOIN calendar c;

CREATE OR REPLACE VIEW fund_pool_on.etf_usable_pool_current AS
SELECT * FROM fund_pool_on.etf_usable_pool_screening_current
WHERE selection_status IN ('PRIMARY','BACKUP') AND is_current;

CREATE OR REPLACE FUNCTION fund_pool_on.etf_usable_pool_as_of(p_available_at timestamptz)
RETURNS SETOF fund_pool_on.etf_usable_pool_history LANGUAGE sql STABLE AS $body$
    WITH calendar AS (
        SELECT array_agg(day ORDER BY day DESC) AS dates FROM (
            SELECT DISTINCT cal_date::date AS day FROM rawdata.others_calendar
            WHERE exchange='SSE' AND is_open=1
              AND cal_date::date < (p_available_at AT TIME ZONE 'Asia/Shanghai')::date
            ORDER BY day DESC LIMIT 3
        ) x
    )
    SELECT h.* FROM fund_pool_on.etf_usable_pool_history h
    JOIN fund_pool_on.etf_candidate_master_as_of(p_available_at) c
      ON c.fund_code=h.fund_code AND c.snapshot_id=h.source_snapshot_id
    CROSS JOIN calendar cal
    WHERE h.selection_id=(
        SELECT selection_id FROM fund_pool_on.etf_usable_pool_batch
        WHERE available_from <= p_available_at
        ORDER BY available_from DESC, selection_id DESC LIMIT 1
    ) AND h.selection_status IN ('PRIMARY','BACKUP')
    AND cardinality(cal.dates)=3
    AND h.facts_as_of BETWEEN cal.dates[1] AND (p_available_at AT TIME ZONE 'Asia/Shanghai')::date
    AND (h.product_facts->>'price_date')::date >= cal.dates[1]
    AND (h.product_facts->>'nav_date')::date >= cal.dates[3]
    AND CASE WHEN h.product_facts->>'product_type'='LOF' THEN
        (h.product_facts->>'aum_date')::date >= cal.dates[1]-183
        AND (h.product_facts->>'aum_known_date')::date <= h.facts_as_of
        ELSE (h.product_facts->>'share_date')::date >= cal.dates[3] END;
$body$;

COMMENT ON VIEW fund_pool_on.etf_usable_pool_current IS
'ETF/LOF产品可用池：ETF每暴露一主一同指数备份；无核实指数的LOF独立筛选；源漂移或过期时关闭';
COMMENT ON VIEW fund_pool_on.etf_usable_pool_screening_current IS
'全候选逐只筛选结果及阻断原因；is_current=false 时须重新筛选';
"""

OBJECTS = (
    "etf_usable_pool_batch",
    "etf_usable_pool_snapshot",
    "etf_usable_pool_source_current",
    "etf_usable_pool_latest_batch",
    "etf_usable_pool_history",
    "etf_usable_pool_screening_current",
    "etf_usable_pool_current",
)


class UsablePoolError(RuntimeError):
    pass


@dataclass(frozen=True)
class UsablePoolPlan:
    payload: dict[str, Any]
    rows: list[dict[str, Any]]

    @property
    def plan_hash(self) -> str:
        return sha256_json(self.payload)

    def summary(self) -> dict[str, Any]:
        return {
            "status": "planned",
            "plan_hash": self.plan_hash,
            **{
                k: self.payload[k]
                for k in (
                    "run_date",
                    "facts_as_of",
                    "source_snapshot_id",
                    "summary",
                    "executable",
                    "guards",
                )
            },
        }


def schema_plan(connection: Any) -> dict[str, Any]:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            "SELECT name, to_regclass('fund_pool_on.' || name)::text AS relation FROM unnest(%s::text[]) name "
            "UNION ALL SELECT 'etf_usable_pool_as_of(timestamptz)', "
            "to_regprocedure('fund_pool_on.etf_usable_pool_as_of(timestamptz)')::text",
            (list(OBJECTS),),
        )
        missing = [r["name"] for r in cursor.fetchall() if r["relation"] is None]
    payload = {
        "migration": "20260928_etf_usable_pool_v1",
        "sql_hash": sha256_json(SCHEMA_SQL),
        "missing_objects": missing,
    }
    return {**payload, "plan_hash": sha256_json(payload)}


def apply_schema(connection: Any, expected_plan_hash: str) -> dict[str, Any]:
    try:
        plan = schema_plan(connection)
        if plan["plan_hash"] != expected_plan_hash:
            raise UsablePoolError("usable pool schema plan changed")
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext('alphahome_etf_usable_pool_v1'))"
            )
            cursor.execute(SCHEMA_SQL)
        connection.commit()
        return {"status": "success", **plan}
    except Exception:
        connection.rollback()
        raise


def _rank(row: dict[str, Any]) -> tuple:
    f = row["product_facts"]
    return (
        -float(f["amount_20d_100m"]),
        float(f["total_fee_pct"]),
        -float(f["aum_100m"]),
        -row["diagnostics"]["listing_age_months"],
        row["fund_code"],
    )


def screen_products(
    inputs: list[dict[str, Any]],
    *,
    facts_as_of: str,
    minimum_fact_dates: dict[str, str | None],
) -> list[dict[str, Any]]:
    """Screen all candidates, including observations and AI_REVIEW_REQUIRED rows."""
    rows = []
    groups: dict[str, list[dict]] = defaultdict(list)
    for item in inputs:
        c, f, identity = item["candidate"], item["facts"], item["identity"]
        review = item.get("review") or {}
        code = item["fund_code"]
        hard = _fact_quality_issues(
            f, facts_as_of=facts_as_of, minimum_fact_dates=minimum_fact_dates
        )
        if (
            not c.get("include_in_candidate_pool")
            or c.get("confirmation_status") == "HUMAN_REJECTED"
        ):
            hard.append("human_rejected_or_not_candidate")
        if identity.get("fund_code") != code or not code.endswith((".SH", ".SZ")):
            hard.append("fund_identity_unverified")
        index = (f or {}).get("tracking_index_code")
        is_lof = (f or {}).get("product_type") == "LOF"
        if (
            (not index and not is_lof)
            or index != c.get("tracking_index_code")
            or index != identity.get("tracking_index_code")
        ):
            hard.append("tracking_index_identity_conflict")
        if identity.get("status") != "L":
            hard.append("not_listed")
        try:
            listed = date.fromisoformat(str(identity.get("list_date"))[:10])
        except (TypeError, ValueError):
            listed = None
        cutoff = date.fromisoformat(facts_as_of)
        age = listing_months(listed, cutoff) if listed else None
        if not listed or listed > cutoff:
            hard.append("listing_date_missing_or_future")
        if not c.get("exposure_id"):
            hard.append("exposure_identity_missing")
        eligibility = []
        for field, minimum, reason in (
            (
                "age_months",
                POLICY["minimum_age_months"],
                f"age_below_{POLICY['minimum_age_months']}_months",
            ),
            ("aum_100m", POLICY["minimum_aum_100m"], "aum_below_5_100m"),
            (
                "amount_20d_100m",
                POLICY["minimum_amount_20d_100m"],
                "amount20d_below_0_3_100m",
            ),
        ):
            # Product facts keep the provider/recipe age for audit, but the gate
            # uses the verified listing anniversary, never rounded days / 30.4375.
            value = age if field == "age_months" else (f or {}).get(field)
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue  # Missing/invalid values are already hard blockers above.
            if (
                not isinstance(value, bool)
                and math.isfinite(number)
                and number < minimum
            ):
                eligibility.append(reason)
        pending = c.get("confirmation_status") not in (
            "AI_CONFIRMED",
            "HUMAN_CONFIRMED",
        )
        notes = list(review.get("uncertainty") or []) if pending else []
        if pending and not notes:
            notes.append("分类映射尚未确认；产品资格独立按源事实筛选")
        row = {
            "fund_code": code,
            "exposure_id": (
                "LOF_PRODUCT_" + code.replace(".", "_")
                if is_lof and not index
                else c.get("exposure_id") or ""
            ),
            "selection_status": (
                "BLOCKED" if hard else "INELIGIBLE" if eligibility else "RESERVE"
            ),
            "exposure_rank": None,
            "classification_review_required": pending,
            "blocking_reasons": hard + eligibility,
            "research_notes": notes,
            "input_fingerprint": item["input_fingerprint"],
            "source_record": c,
            "product_facts": f or {},
            "diagnostics": {
                "listing_age_months": age,
                "source_identity_verified": not any(
                    r in hard
                    for r in (
                        "fund_identity_unverified",
                        "tracking_index_identity_conflict",
                    )
                ),
                "selection_reason": (
                    "hard_fact_gate"
                    if hard
                    else "product_threshold" if eligibility else "eligible_reserve"
                ),
            },
        }
        rows.append(row)
        if not hard and not eligibility:
            groups[row["exposure_id"]].append(row)
    for group in groups.values():
        ordered = sorted(group, key=_rank)
        primary = ordered[0]
        backup_assigned = False
        for rank, row in enumerate(ordered, 1):
            row["exposure_rank"] = rank
            same_index = bool(
                row["product_facts"].get("tracking_index_code")
                and row["product_facts"]["tracking_index_code"]
                == primary["product_facts"]["tracking_index_code"]
            )
            if rank == 1:
                row["selection_status"] = "PRIMARY"
            elif same_index and not backup_assigned:
                row["selection_status"] = "BACKUP"
                backup_assigned = True
            row["diagnostics"].update(
                primary_fund_code=primary["fund_code"],
                selection_reason=(
                    "ranked_primary"
                    if rank == 1
                    else (
                        "ranked_same_index_backup"
                        if row["selection_status"] == "BACKUP"
                        else (
                            "different_index_variant_reserved"
                            if not same_index
                            else "lower_rank_reserved"
                        )
                    )
                ),
            )
    return sorted(rows, key=lambda r: r["fund_code"])


def build_usable_pool_plan(connection: Any, *, run_date: date) -> UsablePoolPlan:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            "SELECT to_jsonb(b) AS payload FROM fund_pool_on.etf_candidate_master_latest_batch b"
        )
        source = cursor.fetchone()
        if source is None:
            raise UsablePoolError("no candidate snapshot")
        source = source["payload"]
        cursor.execute(SOURCE_WITH_HASH_SQL + " ORDER BY fund_code")
        inputs = [dict(r) for r in cursor.fetchall()]
        cursor.execute(
            "SELECT DISTINCT cal_date::date AS day FROM rawdata.others_calendar WHERE exchange='SSE' AND is_open=1 AND cal_date::date < %s ORDER BY day DESC LIMIT 3",
            (run_date,),
        )
        days = [r["day"].isoformat() for r in cursor.fetchall()]
    fact_dates = sorted({r["facts"]["as_of_date"] for r in inputs if r["facts"]})
    facts_as_of = max(fact_dates) if fact_dates else run_date.isoformat()
    minima = {
        field: days[lag] if len(days) > lag else None
        for field, lag in FACT_MAX_LAG_TRADE_DAYS.items()
    }
    rows = screen_products(inputs, facts_as_of=facts_as_of, minimum_fact_dates=minima)
    counts = Counter(r["selection_status"] for r in rows)
    pending_counts = Counter(
        r["selection_status"] for r in rows if r["classification_review_required"]
    )
    guards = {
        "candidate_source_present": bool(inputs),
        "unique_candidate_codes": len({r["fund_code"] for r in inputs}) == len(inputs),
        "calendar_complete": len(days) == 3,
        "single_fact_date": len(fact_dates) == 1,
        "facts_fresh": bool(days and days[0] <= facts_as_of <= run_date.isoformat()),
    }
    payload = {
        "contract": "etf_usable_pool_plan_v1",
        "policy": POLICY,
        "run_date": run_date.isoformat(),
        "facts_as_of": facts_as_of,
        "source_snapshot_id": source["snapshot_id"],
        "source_fingerprints": {r["fund_code"]: r["input_fingerprint"] for r in inputs},
        "minimum_fact_dates": minima,
        "result_hash": sha256_json(rows),
        "summary": {
            "screened_count": len(rows),
            "status_counts": dict(counts),
            "selected_count": counts["PRIMARY"] + counts["BACKUP"],
            "eligible_count": counts["PRIMARY"] + counts["BACKUP"] + counts["RESERVE"],
            "pending_classification_count": sum(pending_counts.values()),
            "pending_classification_results": dict(pending_counts),
        },
        "guards": guards,
        "executable": all(guards.values()),
    }
    return UsablePoolPlan(payload=payload, rows=rows)


def execute_usable_pool_plan(
    connection: Any, plan: UsablePoolPlan, *, expected_plan_hash: str
) -> dict[str, Any]:
    try:
        if expected_plan_hash != plan.plan_hash or not plan.payload["executable"]:
            raise UsablePoolError("usable pool plan hash/guards failed")
        if sha256_json(plan.rows) != plan.payload.get("result_hash"):
            raise UsablePoolError("usable pool rows differ from the frozen result hash")
        if schema_plan(connection)["missing_objects"]:
            raise UsablePoolError("usable pool migration_required")
        lock_candidate_master(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext('alphahome_etf_usable_pool_v1'))"
            )
        fresh = build_usable_pool_plan(
            connection, run_date=date.fromisoformat(plan.payload["run_date"])
        )
        if fresh.plan_hash != plan.plan_hash or not fresh.payload["executable"]:
            raise UsablePoolError("usable pool source changed; generate a new plan")
        plan = fresh
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT selection_id FROM fund_pool_on.etf_usable_pool_batch WHERE plan_hash=%s",
                (plan.plan_hash,),
            )
            existing = cursor.fetchone()
            if existing:
                connection.rollback()
                return {
                    **plan.summary(),
                    "status": "no_op",
                    "selection_id": existing["selection_id"],
                }
            selection_id = (
                "etf_usable_"
                + plan.payload["run_date"].replace("-", "")
                + "_"
                + plan.plan_hash[:16]
            )
            cursor.execute(
                """INSERT INTO fund_pool_on.etf_usable_pool_batch
                (selection_id,plan_hash,source_snapshot_id,facts_as_of,evaluated_on,policy,summary,row_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    selection_id,
                    plan.plan_hash,
                    plan.payload["source_snapshot_id"],
                    plan.payload["facts_as_of"],
                    plan.payload["run_date"],
                    Json(POLICY),
                    Json(plan.payload["summary"]),
                    len(plan.rows),
                ),
            )
            values = [
                (
                    selection_id,
                    r["fund_code"],
                    r["exposure_id"],
                    r["selection_status"],
                    r["exposure_rank"],
                    r["classification_review_required"],
                    Json(r["blocking_reasons"]),
                    Json(r["research_notes"]),
                    r["input_fingerprint"],
                    Json(r["source_record"]),
                    Json(r["product_facts"]),
                    Json(r["diagnostics"]),
                )
                for r in plan.rows
            ]
            execute_values(
                cursor,
                """INSERT INTO fund_pool_on.etf_usable_pool_snapshot
                (selection_id,fund_code,exposure_id,selection_status,exposure_rank,classification_review_required,
                 blocking_reasons,research_notes,input_fingerprint,source_record,product_facts,diagnostics) VALUES %s""",
                values,
            )
        connection.commit()
        return {**plan.summary(), "status": "success", "selection_id": selection_id}
    except Exception:
        connection.rollback()
        raise


def preview_usable_pool(database_url: str, *, run_date: date) -> dict[str, Any]:
    connection = psycopg2.connect(database_url)
    try:
        connection.set_session(readonly=True)
        migration = schema_plan(connection)
        if migration["missing_objects"]:
            return {
                "status": "migration_required",
                "missing_objects": migration["missing_objects"],
            }
        return build_usable_pool_plan(connection, run_date=run_date).summary()
    finally:
        connection.close()


def refresh_usable_pool(database_url: str, *, run_date: date) -> dict[str, Any]:
    connection = psycopg2.connect(database_url)
    try:
        plan = build_usable_pool_plan(connection, run_date=run_date)
        connection.rollback()
        return execute_usable_pool_plan(
            connection, plan, expected_plan_hash=plan.plan_hash
        )
    finally:
        connection.close()
