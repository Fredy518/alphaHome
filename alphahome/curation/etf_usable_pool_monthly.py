"""Monthly ETF eligibility, preserving observation time and reconstruction limits.

Historical membership is reconstructed from the full exchange ETF inventory.
Only candidate mappings actually published by the decision cutoff may merge
products. An eligible product without such a mapping remains STANDALONE.
"""

from __future__ import annotations

import json
import math
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

from .deepseek_candidate_client import sha256_json
from .etf_candidate_master import lock_candidate_master
from .exchange_fund_sources import (
    LOF_UNIVERSE_SQL,
    LOF_MAX_AUM_AGE_DAYS,
    lof_aum_sql,
    listing_months,
)

TZ = ZoneInfo("Asia/Shanghai")
TASK_NAME = "etf_usable_pool_monthly"
START_MONTH = date(2016, 1, 1)
SELECTED = ("PRIMARY", "BACKUP", "STANDALONE")
POLICY = {
    "version": "exchange_fund_usable_pool_monthly_v5",
    "product_types": ["ETF", "LOF"],
    "lof_aum_maximum_age_days": LOF_MAX_AUM_AGE_DAYS,
    "lof_aum": "reported_net_asset_announced_after_report_date_or_observed_overview_snapshot",
    "minimum_listing_months": 3,
    "age_basis": "complete_calendar_months_from_verified_listing_date",
    "minimum_aum_100m": 5,
    "minimum_amount_20d_100m": 0.3,
    "amount_window": "last_20_exchange_sessions_through_month_end",
    "maximum_aum_lag_sessions": 2,
    "decision_cutoff": "first_exchange_session_after_month_end_090000_Asia_Shanghai",
    "scheduled_effective": "first_exchange_session_after_month_end_0930_Asia_Shanghai",
    "snapshot_basis": "month_end_facts_completed_before_first_exchange_open",
    "share_availability": "ASSUMED_T_PLUS_1_PRE_OPEN_NO_SOURCE_ANNOUNCEMENT_TIME",
    "nav_availability": "ann_date_through_decision_date_same_day_assumed_pre_open",
    "aum": "reported_net_assets_or_same_date_unit_nav_times_exchange_shares",
    "grouping": "candidate_mapping_available_by_decision_cutoff_else_standalone",
    "ranking": ["amount_20d_desc", "aum_desc", "listing_age_desc", "fund_code_asc"],
    "fee_ranking": "omitted_no_historical_fee_versions",
    "candidate_grade_is_gate": False,
    "ai_confirmation_is_gate": False,
    "source_revision_history_verified": False,
    "historical_universe_completeness_verified": False,
}

ETF_UNIVERSE_SQL = r"""
WITH codes AS (
    SELECT ts_code FROM rawdata.fund_etf_basic
    WHERE ts_code ~ '^[0-9]{6}[.](SH|SZ)$'
    UNION
    SELECT ts_code FROM rawdata.fund_basic
    WHERE ts_code ~ '^(15|51|52|56|58)[0-9]{4}[.](SH|SZ)$'
      AND name LIKE '%%ETF%%' AND name NOT LIKE '%%联接%%' AND name NOT LIKE '%%LOF%%'
)
SELECT c.ts_code AS fund_code, coalesce(e.name,b.name) AS fund_name_reference,
       coalesce(b.list_date,e.list_date) AS list_date,
       coalesce(b.found_date,e.found_date) AS found_date,
       b.delist_date, coalesce(b.status,e.status) AS current_status_reference,
       e.index_code AS current_index_reference,
       CASE WHEN e.ts_code IS NULL THEN 'fund_basic_etf_supplement'
            ELSE 'fund_etf_basic' END AS inventory_source
FROM codes c LEFT JOIN rawdata.fund_etf_basic e USING(ts_code)
LEFT JOIN rawdata.fund_basic b USING(ts_code)
ORDER BY c.ts_code
"""
UNIVERSE_SQL = f"""
SELECT e.*, 'ETF'::text AS product_type FROM ({ETF_UNIVERSE_SQL}) e
UNION ALL {LOF_UNIVERSE_SQL} ORDER BY fund_code
"""

# All 20 calendar dates are explicit: missing bars cannot be replaced with older
# trades. Net assets and shares are matched to the same NAV date. Revised vendor
# history is recorded as reconstruction, never as historical ingestion evidence.
FACTS_SQL = """
WITH codes AS (SELECT unnest(%(codes)s::text[]) AS fund_code)
SELECT c.fund_code, d.price_date, d.amount_20d_100m, d.amount_days,
       d.invalid_amount_days, d.invalid_price_days, d.observed_dates,
       n.nav_date, n.ann_date AS nav_ann_date, n.unit_nav,
       n.share_date, n.fd_share, n.aum_100m, n.aum_source
FROM codes c
LEFT JOIN LATERAL (
    SELECT max(trade_date) AS price_date,
           avg(amount)/100000.0 AS amount_20d_100m,
           count(DISTINCT trade_date) FILTER(WHERE amount IS NOT NULL) AS amount_days,
           count(*) FILTER(WHERE amount<0 OR amount='NaN'::numeric) AS invalid_amount_days,
           count(*) FILTER(WHERE close IS NULL OR close<=0 OR close='NaN'::numeric) AS invalid_price_days,
           array_agg(DISTINCT trade_date ORDER BY trade_date) AS observed_dates
    FROM rawdata.fund_daily
    WHERE ts_code=c.fund_code AND trade_date=ANY(%(days)s::date[])
) d ON true
LEFT JOIN LATERAL (
    SELECT v.nav_date, v.ann_date, v.unit_nav, s.trade_date AS share_date, s.fd_share,
           CASE WHEN v.total_netasset>0 THEN v.total_netasset/100000000.0
                WHEN v.net_asset>0 THEN v.net_asset/100000000.0
                WHEN v.unit_nav>0 AND s.fd_share>0 THEN v.unit_nav*s.fd_share/10000.0
           END AS aum_100m,
           CASE WHEN v.total_netasset>0 THEN 'reported_total_netasset'
                WHEN v.net_asset>0 THEN 'reported_net_asset'
                WHEN v.unit_nav>0 AND s.fd_share>0 THEN 'same_date_nav_times_shares'
           END AS aum_source
    FROM rawdata.fund_nav v
    LEFT JOIN rawdata.fund_share s ON s.ts_code=v.ts_code AND s.trade_date=v.nav_date
    WHERE v.ts_code=c.fund_code AND v.nav_date BETWEEN %(aum_min_date)s AND %(facts_cutoff)s
      AND v.ann_date IS NOT NULL AND v.ann_date<=%(decision_date)s
      AND v.ann_date>=v.nav_date
      AND (v.total_netasset>0 OR v.net_asset>0 OR (v.unit_nav>0 AND s.fd_share>0))
    ORDER BY v.nav_date DESC, v.ann_date DESC LIMIT 1
) n ON true
ORDER BY c.fund_code
"""

LOF_FACTS_SQL = f"""
WITH codes AS (SELECT unnest(%(codes)s::text[]) AS fund_code)
SELECT c.fund_code, 'LOF'::text AS product_type,
       d.price_date,d.amount_20d_100m,d.amount_days,
       d.invalid_amount_days,d.invalid_price_days,d.observed_dates,
       n.nav_date,n.ann_date AS nav_ann_date,n.unit_nav,
       NULL::date AS share_date,NULL::numeric AS fd_share,
       a.aum_100m,a.aum_source,a.aum_date,a.aum_known_date,a.aum_scope
FROM codes c
LEFT JOIN LATERAL (
    SELECT max(trade_date) AS price_date,avg(amount)/100000.0 AS amount_20d_100m,
           count(DISTINCT trade_date) FILTER(WHERE amount IS NOT NULL) AS amount_days,
           count(*) FILTER(WHERE amount<0 OR amount='NaN'::numeric) AS invalid_amount_days,
           count(*) FILTER(WHERE close IS NULL OR close<=0 OR close='NaN'::numeric) AS invalid_price_days,
           array_agg(DISTINCT trade_date ORDER BY trade_date) AS observed_dates
    FROM rawdata.fund_daily WHERE ts_code=c.fund_code AND trade_date=ANY(%(days)s::date[])
) d ON true
LEFT JOIN LATERAL (
    SELECT nav_date,ann_date,unit_nav FROM rawdata.fund_nav
    WHERE ts_code=c.fund_code AND nav_date BETWEEN %(aum_min_date)s AND %(facts_cutoff)s
      AND ann_date<=%(decision_date)s AND ann_date>=nav_date AND unit_nav>0
    ORDER BY nav_date DESC,ann_date DESC LIMIT 1
) n ON true
LEFT JOIN LATERAL ({lof_aum_sql('c.fund_code','%(facts_cutoff)s','%(decision_date)s',include_report_archive=True)}) a ON true
ORDER BY c.fund_code
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fund_pool_on.lof_aum_report_evidence (
    fund_code text NOT NULL, report_date date NOT NULL, ann_date date NOT NULL,
    net_asset numeric NOT NULL CHECK(net_asset>0),
    source text NOT NULL CHECK(source='tushare.fund_nav'),
    source_hash text NOT NULL, source_payload jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(fund_code,report_date,ann_date,source_hash),
    CHECK(ann_date>report_date)
);
CREATE TABLE IF NOT EXISTS fund_pool_on.etf_usable_pool_monthly_run (
    run_id text PRIMARY KEY, plan_hash text NOT NULL UNIQUE,
    start_month date NOT NULL, end_month date NOT NULL,
    record_kind text NOT NULL CHECK(record_kind IN ('HISTORICAL_RECONSTRUCTION','OBSERVED')),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    plan jsonb NOT NULL, summary jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS fund_pool_on.etf_usable_pool_monthly_batch (
    snapshot_id text PRIMARY KEY, month_hash text NOT NULL UNIQUE,
    run_id text NOT NULL REFERENCES fund_pool_on.etf_usable_pool_monthly_run(run_id),
    maintenance_month date NOT NULL CHECK(extract(day FROM maintenance_month)=1),
    facts_cutoff date NOT NULL, decision_cutoff timestamptz NOT NULL,
    scheduled_effective_at timestamptz NOT NULL, effective_from timestamptz NOT NULL,
    effective_to timestamptz NOT NULL, recorded_at timestamptz NOT NULL,
    record_kind text NOT NULL CHECK(record_kind IN ('HISTORICAL_RECONSTRUCTION','OBSERVED')),
    policy jsonb NOT NULL, source_hash text NOT NULL, result_hash text NOT NULL,
    summary jsonb NOT NULL, row_count integer NOT NULL CHECK(row_count>0),
    CHECK(facts_cutoff>=maintenance_month AND facts_cutoff<maintenance_month+interval '1 month'),
    CHECK(decision_cutoff>facts_cutoff::timestamp AT TIME ZONE 'Asia/Shanghai'),
    CHECK(scheduled_effective_at>decision_cutoff),
    CHECK(effective_from>=scheduled_effective_at),
    CHECK(record_kind='HISTORICAL_RECONSTRUCTION' OR effective_from>=recorded_at),
    CHECK(effective_to>effective_from)
);
CREATE TABLE IF NOT EXISTS fund_pool_on.etf_usable_pool_monthly_snapshot (
    snapshot_id text NOT NULL REFERENCES fund_pool_on.etf_usable_pool_monthly_batch(snapshot_id),
    fund_code text NOT NULL CHECK(fund_code ~ '^[0-9]{6}[.](SH|SZ)$'),
    fund_name_reference text, list_date date, delist_date date,
    selection_status text NOT NULL CHECK(selection_status IN
       ('PRIMARY','BACKUP','STANDALONE','RESERVE','INELIGIBLE','BLOCKED')),
    group_id text NOT NULL, exposure_rank integer, historical_mapping_available boolean NOT NULL,
    tracking_index_code text, candidate_snapshot_id text, candidate_available_from timestamptz,
    aum_100m numeric, amount_20d_100m numeric, listing_age_months integer,
    reasons jsonb NOT NULL, quality_flags jsonb NOT NULL,
    product_facts jsonb NOT NULL, classification_as_of jsonb NOT NULL,
    input_fingerprint text NOT NULL,
    PRIMARY KEY(snapshot_id,fund_code)
);
CREATE UNIQUE INDEX IF NOT EXISTS etf_usable_monthly_one_role
ON fund_pool_on.etf_usable_pool_monthly_snapshot(snapshot_id,group_id,selection_status)
WHERE selection_status IN ('PRIMARY','BACKUP');
CREATE INDEX IF NOT EXISTS etf_usable_monthly_month
ON fund_pool_on.etf_usable_pool_monthly_batch(maintenance_month,recorded_at DESC);
CREATE OR REPLACE VIEW fund_pool_on.etf_usable_pool_monthly_history AS
SELECT b.maintenance_month,b.facts_cutoff,b.decision_cutoff,b.scheduled_effective_at,
       b.effective_from,b.effective_to,b.recorded_at,b.record_kind,
       b.policy->>'version' AS policy_version,s.*
FROM fund_pool_on.etf_usable_pool_monthly_batch b
JOIN fund_pool_on.etf_usable_pool_monthly_snapshot s USING(snapshot_id);
CREATE OR REPLACE VIEW fund_pool_on.etf_usable_pool_monthly_screening AS
SELECT h.* FROM fund_pool_on.etf_usable_pool_monthly_history h
JOIN (SELECT DISTINCT ON (maintenance_month) maintenance_month,snapshot_id
      FROM fund_pool_on.etf_usable_pool_monthly_batch
      ORDER BY maintenance_month,recorded_at DESC,snapshot_id DESC) b USING(snapshot_id,maintenance_month);
CREATE OR REPLACE VIEW fund_pool_on.etf_usable_pool_monthly_membership AS
SELECT * FROM fund_pool_on.etf_usable_pool_monthly_screening
WHERE selection_status IN ('PRIMARY','BACKUP','STANDALONE');
CREATE OR REPLACE VIEW fund_pool_on.etf_usable_pool_monthly_current AS
SELECT * FROM fund_pool_on.etf_usable_pool_monthly_membership
WHERE current_timestamp>=effective_from AND current_timestamp<effective_to
  AND (delist_date IS NULL OR (current_timestamp AT TIME ZONE 'Asia/Shanghai')::date<delist_date);
CREATE OR REPLACE FUNCTION fund_pool_on.etf_usable_pool_monthly_reconstructed_as_of(
    p_effective_at timestamptz, p_known_at timestamptz DEFAULT current_timestamp)
RETURNS SETOF fund_pool_on.etf_usable_pool_monthly_history LANGUAGE sql STABLE AS $body$
    WITH versions AS (
        SELECT DISTINCT ON(maintenance_month) snapshot_id
        FROM fund_pool_on.etf_usable_pool_monthly_batch
        WHERE recorded_at<=p_known_at
        ORDER BY maintenance_month,recorded_at DESC,snapshot_id DESC
    )
    SELECT h.* FROM fund_pool_on.etf_usable_pool_monthly_history h JOIN versions USING(snapshot_id)
    WHERE h.effective_from<=p_effective_at AND p_effective_at<h.effective_to
      AND h.selection_status IN ('PRIMARY','BACKUP','STANDALONE')
      AND (h.delist_date IS NULL OR (p_effective_at AT TIME ZONE 'Asia/Shanghai')::date<h.delist_date);
$body$;
CREATE OR REPLACE FUNCTION fund_pool_on.etf_usable_pool_monthly_as_of(p_available_at timestamptz)
RETURNS SETOF fund_pool_on.etf_usable_pool_monthly_history LANGUAGE sql STABLE AS $body$
    WITH versions AS (
        SELECT DISTINCT ON(maintenance_month) snapshot_id
        FROM fund_pool_on.etf_usable_pool_monthly_batch
        WHERE recorded_at<=p_available_at AND record_kind='OBSERVED'
        ORDER BY maintenance_month,recorded_at DESC,snapshot_id DESC
    )
    SELECT h.* FROM fund_pool_on.etf_usable_pool_monthly_history h JOIN versions USING(snapshot_id)
    WHERE h.effective_from<=p_available_at AND p_available_at<h.effective_to
      AND h.selection_status IN ('PRIMARY','BACKUP','STANDALONE')
      AND (h.delist_date IS NULL OR (p_available_at AT TIME ZONE 'Asia/Shanghai')::date<h.delist_date);
$body$;
COMMENT ON VIEW fund_pool_on.etf_usable_pool_monthly_current IS
'月度成员在有效区间内保持稳定；record_kind区分历史重建与实际维护；日常检查独立，不因新行情抹去月度成员';
COMMENT ON FUNCTION fund_pool_on.etf_usable_pool_monthly_reconstructed_as_of(timestamptz,timestamptz) IS
'研究重建查询，明确允许事后重建；份额公告时间、供应商修订和历史宇宙完整性未获独立认证';
"""

OBJECTS = (
    "lof_aum_report_evidence",
    "etf_usable_pool_monthly_run",
    "etf_usable_pool_monthly_batch",
    "etf_usable_pool_monthly_snapshot",
    "etf_usable_pool_monthly_history",
    "etf_usable_pool_monthly_screening",
    "etf_usable_pool_monthly_membership",
    "etf_usable_pool_monthly_current",
)


class MonthlyPoolError(RuntimeError):
    pass


def normalized(value: Any) -> Any:
    return json.loads(
        json.dumps(value, ensure_ascii=False, default=str, allow_nan=False)
    )


def month_start(day: date) -> date:
    return day.replace(day=1)


def next_month(day: date) -> date:
    return (month_start(day) + timedelta(days=32)).replace(day=1)


def last_complete_month(as_of: date) -> date:
    return month_start(month_start(as_of) - timedelta(days=1))


def month_range(start: date, end: date) -> list[date]:
    months = []
    day = month_start(start)
    while day <= month_start(end):
        months.append(day)
        day = next_month(day)
    return months


def schedule(month: date, sessions: list[date]) -> dict[str, Any]:
    end = next_month(month) - timedelta(days=1)
    ix = bisect_right(sessions, end)
    following_end = next_month(next_month(month)) - timedelta(days=1)
    nx = bisect_right(sessions, following_end)
    if ix < 20 or ix >= len(sessions) or nx >= len(sessions):
        raise MonthlyPoolError(f"calendar incomplete for {month}")
    return {
        "maintenance_month": month.isoformat(),
        "facts_cutoff": sessions[ix - 1].isoformat(),
        "days": [d.isoformat() for d in sessions[ix - 20 : ix]],
        "aum_min_date": sessions[ix - 3].isoformat(),
        "decision_cutoff": datetime.combine(
            sessions[ix], time(9, 0), TZ
        ).isoformat(),
        "scheduled_effective_at": datetime.combine(
            sessions[ix], time(9, 30), TZ
        ).isoformat(),
        "effective_to": datetime.combine(sessions[nx], time(9, 30), TZ).isoformat(),
    }


def _number(value: Any) -> float | None:
    try:
        number = float(value)
        return number if not isinstance(value, bool) and math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def active_inventory(inventory: list[dict], cutoff: date) -> list[dict]:
    result = []
    for item in inventory:
        listed, found, delisted = (
            item.get(k) for k in ("list_date", "found_date", "delist_date")
        )
        if listed and listed > cutoff:
            continue
        if not listed and (not found or found > cutoff):
            continue
        if delisted and delisted <= cutoff:
            continue
        result.append(item)
    return result


def screen_month(inputs: list[dict], dates: dict[str, Any]) -> list[dict]:
    cutoff = date.fromisoformat(dates["facts_cutoff"])
    rows, groups = [], defaultdict(list)
    for item in inputs:
        identity, facts, classification = (
            item["identity"],
            item["facts"],
            item["classification"],
        )
        code = identity["fund_code"]
        hard, below = [], []
        listed = (
            date.fromisoformat(identity["list_date"])
            if identity.get("list_date")
            else None
        )
        age = listing_months(listed, cutoff) if listed else None
        if not listed or listed > cutoff:
            hard.append("listing_date_missing_or_future")
        if identity.get("current_status_reference") == "D" and not identity.get(
            "delist_date"
        ):
            hard.append("delisting_date_unknown")
        if (
            identity.get("delist_date")
            and identity["delist_date"] <= dates["facts_cutoff"]
        ):
            hard.append("already_delisted")
        if facts.get("amount_days") != 20 or set(
            facts.get("observed_dates") or []
        ) != set(dates["days"]):
            hard.append("incomplete_20_exchange_sessions")
        if facts.get("price_date") != dates["facts_cutoff"]:
            hard.append("price_not_at_month_end")
        if facts.get("invalid_amount_days") or facts.get("invalid_price_days"):
            hard.append("invalid_market_observation")
        aum, amount = _number(facts.get("aum_100m")), _number(
            facts.get("amount_20d_100m")
        )
        if aum is None or aum <= 0:
            hard.append("historical_aum_unavailable")
        is_lof = identity.get("product_type") == "LOF"
        if is_lof:
            aum_date, known_date = facts.get("aum_date"), facts.get("aum_known_date")
            if not aum_date or not known_date:
                hard.append("lof_report_availability_missing")
            elif not aum_date <= known_date <= dates["decision_cutoff"][:10]:
                hard.append("lof_report_not_known_by_decision")
            elif (
                not (cutoff - timedelta(days=LOF_MAX_AUM_AGE_DAYS)).isoformat()
                <= aum_date
                <= dates["facts_cutoff"]
            ):
                hard.append("lof_report_stale_or_future")
        if amount is None or amount < 0:
            hard.append("historical_amount_unavailable")
        if (
            not facts.get("nav_date")
            or not dates["aum_min_date"] <= facts["nav_date"] <= dates["facts_cutoff"]
        ):
            hard.append("nav_missing_or_stale")
        if (
            not facts.get("nav_ann_date")
            or facts["nav_ann_date"] > dates["decision_cutoff"][:10]
        ):
            hard.append("nav_not_announced_by_decision")
        if facts.get("aum_source") == "same_date_nav_times_shares" and facts.get(
            "share_date"
        ) != facts.get("nav_date"):
            hard.append("nav_share_date_mismatch")
        # The as-of function should enforce this too; fail explicitly on bad inputs.
        if classification and (
            not classification.get("available_from")
            or datetime.fromisoformat(classification["available_from"])
            > datetime.fromisoformat(dates["decision_cutoff"])
        ):
            raise MonthlyPoolError(
                "future classification cannot enter a historical month"
            )
        if classification.get("confirmation_status") == "HUMAN_REJECTED":
            hard.append("human_rejected_as_of_decision")
        if age is not None and age < POLICY["minimum_listing_months"]:
            below.append(f"listing_age_below_{POLICY['minimum_listing_months']}_months")
        if aum is not None and aum < 5:
            below.append("aum_below_5_100m")
        if amount is not None and amount < 0.3:
            below.append("amount20d_below_0_3_100m")
        mapped = bool(
            classification.get("exposure_id")
            and classification.get("tracking_index_code")
        )
        group = (
            classification["exposure_id"]
            if mapped
            else f"{'LOF' if is_lof else 'ETF'}_PRODUCT_{code.replace('.','_')}"
        )
        flags = [
            "vendor_revision_history_unverified",
            "inventory_history_reconstructed",
            "fund_name_is_current_reference",
            "historical_fee_not_used",
        ]
        if facts.get("aum_source") == "same_date_nav_times_shares":
            flags.append("share_publication_time_assumed_t_plus_1_pre_open")
        if facts.get("nav_ann_date") == dates["decision_cutoff"][:10]:
            flags.append("nav_same_day_publication_time_assumed_pre_open")
        if is_lof:
            flags.append("lof_periodic_aum_not_daily_exchange_share_size")
            if facts.get("aum_source") == "fund_overview_observed_net_asset":
                flags.append("report_available_from_observed_snapshot_only")
        if not mapped:
            flags.append("no_historical_mapping_standalone")
        if classification.get("confirmation_status") == "AI_REVIEW_REQUIRED":
            flags.append("classification_review_required")
        row = {
            "fund_code": code,
            "fund_name_reference": identity.get("fund_name_reference"),
            "list_date": identity.get("list_date"),
            "delist_date": identity.get("delist_date"),
            "selection_status": (
                "BLOCKED" if hard else "INELIGIBLE" if below else "RESERVE"
            ),
            "group_id": group,
            "exposure_rank": None,
            "historical_mapping_available": mapped,
            "tracking_index_code": (
                classification.get("tracking_index_code") if mapped else None
            ),
            "candidate_snapshot_id": classification.get("snapshot_id"),
            "candidate_available_from": classification.get("available_from"),
            "aum_100m": aum,
            "amount_20d_100m": amount,
            "listing_age_months": age,
            "reasons": hard + below,
            "quality_flags": flags,
            "product_facts": facts,
            "classification_as_of": classification,
            "input_fingerprint": sha256_json(item),
        }
        rows.append(row)
        if not hard and not below:
            groups[group].append(row)
    for group in groups.values():
        ordered = sorted(
            group,
            key=lambda r: (
                -r["amount_20d_100m"],
                -r["aum_100m"],
                -r["listing_age_months"],
                r["fund_code"],
            ),
        )
        primary, backup = ordered[0], False
        for rank, row in enumerate(ordered, 1):
            row["exposure_rank"] = rank
            if not row["historical_mapping_available"]:
                row["selection_status"] = "STANDALONE"
            elif rank == 1:
                row["selection_status"] = "PRIMARY"
            elif (
                not backup
                and row["tracking_index_code"] == primary["tracking_index_code"]
            ):
                row["selection_status"] = "BACKUP"
                backup = True
    return sorted(rows, key=lambda r: r["fund_code"])


def schema_plan(connection: Any) -> dict:
    with connection.cursor() as q:
        q.execute(
            "SELECT name FROM unnest(%s::text[]) name WHERE to_regclass('fund_pool_on.'||name) IS NULL",
            (list(OBJECTS),),
        )
        missing = [r[0] for r in q.fetchall()]
        for signature in (
            "etf_usable_pool_monthly_as_of(timestamptz)",
            "etf_usable_pool_monthly_reconstructed_as_of(timestamptz,timestamptz)",
        ):
            q.execute("SELECT to_regprocedure(%s)", ("fund_pool_on." + signature,))
            if q.fetchone()[0] is None:
                missing.append(signature)
    payload = {
        "migration": "20260929_etf_usable_pool_monthly_v1",
        "sql_hash": sha256_json(SCHEMA_SQL),
        "missing_objects": missing,
    }
    return {**payload, "plan_hash": sha256_json(payload)}


def apply_schema(connection: Any, expected_plan_hash: str) -> dict:
    try:
        plan = schema_plan(connection)
        if plan["plan_hash"] != expected_plan_hash:
            raise MonthlyPoolError("monthly schema plan changed")
        with connection.cursor() as q:
            q.execute(
                "SELECT pg_advisory_xact_lock(hashtext('alphahome_etf_usable_pool_monthly_v1'))"
            )
            q.execute(SCHEMA_SQL)
        connection.commit()
        return {"status": "success", **plan}
    except Exception:
        connection.rollback()
        raise


@dataclass(frozen=True)
class MonthlyPlan:
    payload: dict
    months: list[dict]

    @property
    def plan_hash(self) -> str:
        return sha256_json(self.payload)

    def summary(self) -> dict:
        return {
            "status": "planned",
            "plan_hash": self.plan_hash,
            **{
                k: self.payload[k]
                for k in (
                    "start_month",
                    "end_month",
                    "record_kind",
                    "summary",
                    "executable",
                    "guards",
                )
            },
        }


def _sessions(connection: Any, start: date, end: date) -> list[date]:
    with connection.cursor() as q:
        q.execute(
            "SELECT DISTINCT cal_date::date FROM rawdata.others_calendar WHERE exchange='SSE' AND is_open=1 AND cal_date BETWEEN %s AND %s ORDER BY 1",
            (
                start - timedelta(days=90),
                next_month(next_month(end)) + timedelta(days=40),
            ),
        )
        return [r[0] for r in q.fetchall()]


def build_monthly_plan(
    connection: Any,
    *,
    start_month: date = START_MONTH,
    as_of: date,
    end_month: date | None = None,
    record_kind: str = "HISTORICAL_RECONSTRUCTION",
    progress: Callable[[dict], None] | None = None,
) -> MonthlyPlan:
    start = month_start(start_month)
    end = month_start(end_month) if end_month else last_complete_month(as_of)
    if start < START_MONTH or end > last_complete_month(as_of) or start > end:
        raise MonthlyPoolError(
            "range must contain complete months starting no earlier than 2016-01"
        )
    if record_kind not in ("HISTORICAL_RECONSTRUCTION", "OBSERVED"):
        raise MonthlyPoolError("invalid record_kind")
    sessions = _sessions(connection, start, end)
    with connection.cursor(cursor_factory=RealDictCursor) as q:
        q.execute(UNIVERSE_SQL)
        inventory = [dict(r) for r in q.fetchall()]
        q.execute("SELECT clock_timestamp() AS now")
        now = q.fetchone()["now"]
    months = []
    for month in month_range(start, end):
        dates = schedule(month, sessions)
        if datetime.fromisoformat(dates["decision_cutoff"]) > now:
            raise MonthlyPoolError(f"decision window not yet complete: {month}")
        if (
            record_kind == "OBSERVED"
            and datetime.fromisoformat(dates["effective_to"]) <= now
        ):
            raise MonthlyPoolError(
                "expired historical month requires HISTORICAL_RECONSTRUCTION"
            )
        active = active_inventory(inventory, date.fromisoformat(dates["facts_cutoff"]))
        with connection.cursor(cursor_factory=RealDictCursor) as q:
            facts = {}
            for product_type, sql in (("ETF", FACTS_SQL), ("LOF", LOF_FACTS_SQL)):
                q.execute(
                    sql,
                    {
                        "codes": [
                            r["fund_code"]
                            for r in active
                            if r.get("product_type", "ETF") == product_type
                        ],
                        "days": dates["days"],
                        "aum_min_date": dates["aum_min_date"],
                        "facts_cutoff": dates["facts_cutoff"],
                        "decision_date": dates["decision_cutoff"][:10],
                    },
                )
                facts.update({r["fund_code"]: dict(r) for r in q.fetchall()})
            q.execute(
                "SELECT to_jsonb(c) AS payload FROM fund_pool_on.etf_candidate_master_as_of(%s::timestamptz,true) c",
                (dates["decision_cutoff"],),
            )
            classifications = {
                r["payload"]["fund_code"]: r["payload"] for r in q.fetchall()
            }
        inputs = normalized(
            [
                {
                    "identity": r,
                    "facts": facts[r["fund_code"]],
                    "classification": classifications.get(r["fund_code"], {}),
                }
                for r in active
            ]
        )
        rows = screen_month(inputs, dates)
        counts = Counter(r["selection_status"] for r in rows)
        summary = {
            "screened_count": len(rows),
            "selected_count": sum(counts[s] for s in SELECTED),
            "eligible_count": sum(counts[s] for s in (*SELECTED, "RESERVE")),
            "status_counts": dict(counts),
            "historically_mapped_count": sum(
                r["historical_mapping_available"] for r in rows
            ),
            "subsequently_delisted_count": sum(bool(r["delist_date"]) for r in rows),
            "reasons": dict(Counter(reason for r in rows for reason in r["reasons"])),
        }
        meta = {
            **dates,
            "source_hash": sha256_json(inputs),
            "result_hash": sha256_json(rows),
            "summary": summary,
            "record_kind": record_kind,
            "policy": POLICY,
            "sql_hash": sha256_json(UNIVERSE_SQL + FACTS_SQL + LOF_FACTS_SQL),
        }
        months.append({"meta": meta, "month_hash": sha256_json(meta), "rows": rows})
        if progress:
            progress({"month": month.isoformat(), **summary})
    guards = {
        "months_contiguous": len(months) == len(month_range(start, end)),
        "nonempty_month_universes": all(m["rows"] for m in months),
        "unique_month_fund_keys": all(
            len(m["rows"]) == len({r["fund_code"] for r in m["rows"]}) for m in months
        ),
    }
    payload = {
        "contract": "etf_usable_pool_monthly_plan_v1",
        "start_month": start.isoformat(),
        "end_month": end.isoformat(),
        "as_of": as_of.isoformat(),
        "record_kind": record_kind,
        "policy": POLICY,
        "schema_hash": sha256_json(SCHEMA_SQL),
        "months": [{"month_hash": m["month_hash"], **m["meta"]} for m in months],
        "summary": {
            "month_count": len(months),
            "screened_rows": sum(len(m["rows"]) for m in months),
            "selected_rows": sum(
                m["meta"]["summary"]["selected_count"] for m in months
            ),
        },
        "guards": guards,
        "executable": all(guards.values()),
    }
    return MonthlyPlan(payload, months)


def execute_monthly_plan(
    connection: Any,
    plan: MonthlyPlan,
    *,
    expected_plan_hash: str,
    progress: Callable[[dict], None] | None = None,
) -> dict:
    try:
        if expected_plan_hash != plan.plan_hash or not plan.payload["executable"]:
            raise MonthlyPoolError("monthly plan hash/guards failed")
        with connection.cursor() as q:
            # Revalidate and publish one MVCC snapshot. Source collectors can
            # commit while this bounded historical computation is running.
            q.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            q.execute("SET LOCAL lock_timeout='5s'")
            q.execute("SET LOCAL statement_timeout='60s'")
        if schema_plan(connection)["missing_objects"]:
            raise MonthlyPoolError("monthly migration_required")
        lock_candidate_master(connection)
        with connection.cursor() as q:
            q.execute(
                "SELECT pg_advisory_xact_lock(hashtext('alphahome_etf_usable_pool_monthly_v1'))"
            )
        fresh = build_monthly_plan(
            connection,
            start_month=date.fromisoformat(plan.payload["start_month"]),
            end_month=date.fromisoformat(plan.payload["end_month"]),
            as_of=date.fromisoformat(plan.payload["as_of"]),
            record_kind=plan.payload["record_kind"],
            progress=progress,
        )
        if fresh.plan_hash != plan.plan_hash:
            raise MonthlyPoolError("monthly source changed; regenerate plan")
        with connection.cursor(cursor_factory=RealDictCursor) as q:
            q.execute(
                "SELECT run_id FROM fund_pool_on.etf_usable_pool_monthly_run WHERE plan_hash=%s",
                (plan.plan_hash,),
            )
            old = q.fetchone()
            if old:
                connection.rollback()
                return {**plan.summary(), "status": "no_op", "run_id": old["run_id"]}
            q.execute("SELECT clock_timestamp() AS recorded_at")
            recorded_at = q.fetchone()["recorded_at"]
            q.execute(
                "SELECT min(cal_date::date::timestamp AT TIME ZONE 'Asia/Shanghai'+interval '9 hours 30 minutes') AS next_open FROM rawdata.others_calendar WHERE exchange='SSE' AND is_open=1 AND (cal_date::date::timestamp AT TIME ZONE 'Asia/Shanghai'+interval '9 hours 30 minutes')>%s",
                (recorded_at,),
            )
            next_open = q.fetchone()["next_open"]
            run_id = "etf_monthly_" + plan.plan_hash[:20]
            q.execute(
                "INSERT INTO fund_pool_on.etf_usable_pool_monthly_run(run_id,plan_hash,start_month,end_month,record_kind,recorded_at,plan,summary) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    run_id,
                    plan.plan_hash,
                    plan.payload["start_month"],
                    plan.payload["end_month"],
                    plan.payload["record_kind"],
                    recorded_at,
                    Json(plan.payload),
                    Json(plan.payload["summary"]),
                ),
            )
            inserted = 0
            for month in fresh.months:
                meta = month["meta"]
                q.execute(
                    "SELECT snapshot_id FROM fund_pool_on.etf_usable_pool_monthly_batch WHERE month_hash=%s",
                    (month["month_hash"],),
                )
                if q.fetchone():
                    continue
                snapshot_id = (
                    "etf_month_"
                    + meta["maintenance_month"][:7].replace("-", "")
                    + "_"
                    + month["month_hash"][:16]
                )
                effective = datetime.fromisoformat(meta["scheduled_effective_at"])
                if plan.payload["record_kind"] == "OBSERVED":
                    if not next_open:
                        raise MonthlyPoolError("next open unavailable")
                    effective = max(effective, next_open)
                if effective >= datetime.fromisoformat(meta["effective_to"]):
                    raise MonthlyPoolError(
                        "monthly validity interval expired before publication"
                    )
                q.execute(
                    """INSERT INTO fund_pool_on.etf_usable_pool_monthly_batch
                    (snapshot_id,month_hash,run_id,maintenance_month,facts_cutoff,decision_cutoff,scheduled_effective_at,
                     effective_from,effective_to,recorded_at,record_kind,policy,source_hash,result_hash,summary,row_count)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        snapshot_id,
                        month["month_hash"],
                        run_id,
                        meta["maintenance_month"],
                        meta["facts_cutoff"],
                        meta["decision_cutoff"],
                        meta["scheduled_effective_at"],
                        effective,
                        meta["effective_to"],
                        recorded_at,
                        meta["record_kind"],
                        Json(POLICY),
                        meta["source_hash"],
                        meta["result_hash"],
                        Json(meta["summary"]),
                        len(month["rows"]),
                    ),
                )
                columns = (
                    "fund_code",
                    "fund_name_reference",
                    "list_date",
                    "delist_date",
                    "selection_status",
                    "group_id",
                    "exposure_rank",
                    "historical_mapping_available",
                    "tracking_index_code",
                    "candidate_snapshot_id",
                    "candidate_available_from",
                    "aum_100m",
                    "amount_20d_100m",
                    "listing_age_months",
                    "reasons",
                    "quality_flags",
                    "product_facts",
                    "classification_as_of",
                    "input_fingerprint",
                )
                json_columns = {
                    "reasons",
                    "quality_flags",
                    "product_facts",
                    "classification_as_of",
                }
                values = [
                    (
                        snapshot_id,
                        *(Json(r[k]) if k in json_columns else r[k] for k in columns),
                    )
                    for r in month["rows"]
                ]
                execute_values(
                    q,
                    "INSERT INTO fund_pool_on.etf_usable_pool_monthly_snapshot(snapshot_id,"
                    + ",".join(columns)
                    + ") VALUES %s",
                    values,
                    page_size=1000,
                )
                inserted += 1
        connection.commit()
        return {
            **plan.summary(),
            "status": "success",
            "run_id": run_id,
            "inserted_months": inserted,
            "recorded_at": recorded_at.isoformat(),
        }
    except Exception:
        connection.rollback()
        raise


def preview_monthly(database_url: str, *, run_date: date) -> dict:
    connection = psycopg2.connect(database_url)
    try:
        connection.set_session(readonly=True)
        migration = schema_plan(connection)
        if migration["missing_objects"]:
            return {
                "status": "migration_required",
                "missing_objects": migration["missing_objects"],
            }
        with connection.cursor() as q:
            q.execute(
                "SELECT DISTINCT maintenance_month FROM fund_pool_on.etf_usable_pool_monthly_batch WHERE policy->>'version'=%s",
                (POLICY["version"],),
            )
            existing = {r[0] for r in q.fetchall()}
        end = last_complete_month(run_date)
        missing = [m for m in month_range(START_MONTH, end) if m not in existing]
        if not missing:
            return {
                "status": "no_op",
                "through_month": end.isoformat(),
                "missing_months": [],
            }
        if any(m < end for m in missing):
            return {
                "status": "backfill_required",
                "missing_months": [m.isoformat() for m in missing],
            }
        dates = schedule(end, _sessions(connection, end, end))
        if datetime.fromisoformat(dates["decision_cutoff"]) >= datetime.now(TZ):
            return {
                "status": "expected_no_data",
                "reason": "decision_window_not_complete",
                "through_month": end.isoformat(),
            }
        return {
            "status": "ready",
            "missing_months": [end.isoformat()],
            "through_month": end.isoformat(),
        }
    finally:
        connection.close()


def refresh_monthly(database_url: str, *, run_date: date) -> dict:
    preview = preview_monthly(database_url, run_date=run_date)
    if preview["status"] in ("no_op", "expected_no_data"):
        return preview
    if preview["status"] != "ready":
        raise MonthlyPoolError(
            preview["status"] + ": explicit schema/backfill required"
        )
    connection = psycopg2.connect(database_url)
    try:
        plan = build_monthly_plan(
            connection,
            start_month=date.fromisoformat(preview["missing_months"][0]),
            as_of=run_date,
            record_kind="OBSERVED",
        )
        connection.rollback()
        return execute_monthly_plan(connection, plan, expected_plan_hash=plan.plan_hash)
    finally:
        connection.close()
