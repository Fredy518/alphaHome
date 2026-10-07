"""LLM 参与的 ETF 候选母表月度自动维护。

流程将可重算产品事实与 LLM 分类分开：数值字段由 AlphaHome 计算，
LLM 只确认/修正分类或判断新产品是否进入候选池。任何结构错误或数据陈旧
都会失败关闭，不写入候选快照。
"""

from __future__ import annotations

import math
import re
import json
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable
from uuid import uuid4

from psycopg2.extras import Json, RealDictCursor, execute_values

from alphahome.curation.candidate_llm_config import resolve_candidate_llm_config
from alphahome.curation.deepseek_candidate_client import (
    PROMPT_SHA256,
    PROMPT_VERSION,
    DeepSeekBatchResult,
    CandidateLLMClient,
    canonical_json,
    sha256_json,
    validate_decisions,
)
from alphahome.curation.etf_candidate_confirmation import (
    missing_confirmation_schema,
)
from alphahome.curation.etf_candidate_taxonomy import (
    CLASSIFICATION_FIELDS,
    CLASSIFICATION_RULES,
    LEVEL2_BY_LEVEL1,
    TAXONOMY_VERSION,
    classification_values,
    missing_classification_fields,
)
from alphahome.curation.etf_candidate_master import (
    CONFIRMATION_STATUS_AI,
    CONFIRMATION_STATUS_AI_REVIEW,
    CONFIRMATION_STATUS_HUMAN,
    CONFIRMATION_STATUS_LEGACY,
    CONTRACT_VERSION,
    SNAPSHOT_COLUMNS,
    STATUS_PERMISSION,
    load_candidate_master_snapshot,
    lock_candidate_master,
    validate_payload,
)


CURATED_PATCH_FIELDS = (
    "asset_class",
    "allocation_module",
    "allocation_role",
    "region_market",
    "level1_group",
    "level2_group",
    "exposure_name",
    "exposure_id",
    "tracking_index_name",
    "product_role",
    "candidate_status",
    "exposure_relationship",
    "parent_fund_code",
    "budget_scope",
    "source_supplement",
    "inclusion_reason",
    "risk_boundary",
    "update_frequency",
    "execution_check",
)

ADD_REQUIRED_FIELDS = {
    "asset_class",
    "allocation_module",
    "exposure_name",
    "exposure_id",
    "product_role",
    "candidate_status",
    "budget_scope",
    "inclusion_reason",
    "risk_boundary",
    "update_frequency",
    "execution_check",
    *CLASSIFICATION_FIELDS,
}

PROMPT_CURRENT_FIELDS = (
    "asset_class",
    "allocation_module",
    "allocation_role",
    "region_market",
    "level1_group",
    "level2_group",
    "exposure_name",
    "exposure_id",
    "fund_name",
    "tracking_index_code",
    "tracking_index_name",
    "product_role",
    "candidate_status",
    "exposure_relationship",
    "parent_fund_code",
    "budget_scope",
    "inclusion_reason",
    "risk_boundary",
    "update_frequency",
    "execution_check",
)

PROMPT_FACT_FIELDS = (
    "product_type",
    "benchmark",
    "invest_type",
    "fund_type",
    "aum_date",
    "aum_known_date",
    "aum_source",
    "aum_scope",
    "as_of_date",
    "fund_code",
    "fund_name",
    "market",
    "etf_type",
    "tracking_index_code",
    "tracking_index_name",
    "found_date",
    "list_date",
    "status",
    "price_date",
    "nav_date",
    "share_date",
    "aum_100m",
    "amount_20d_100m",
    "amount_20d_days",
    "age_months",
    "total_fee_pct",
    "mean_abs_premium_60d",
    "core_facts_complete",
)

ENUM_PATCH_FIELDS = (
    "asset_class",
    "allocation_module",
    "allocation_role",
    "region_market",
    "level1_group",
    "level2_group",
    "product_role",
    "candidate_status",
    "exposure_relationship",
    "budget_scope",
    "update_frequency",
)

EXPOSURE_ID_RE = re.compile(r"^[A-Za-z0-9_]+$")
AUTOMATION_LOCK_NAME = "alphahome_etf_candidate_ai_monthly_v1"
MIN_AUTOMATIC_CONFIDENCE = 0.75
# 运维容忍度，不是供应商 SLA；用交易日而非自然日计算，节假日不误报。
FACT_MAX_LAG_TRADE_DAYS = {"price_date": 0, "nav_date": 2, "share_date": 2}
REQUIRED_NUMERIC_FACTS = ("aum_100m", "amount_20d_100m", "age_months", "total_fee_pct")
SCREENING_SCOPES = ("incremental", "full", "lof_initial")
EXPOSURE_IDENTITY_POLICY = "existing_dictionary_or_index_or_lof_product_v2"
EXPOSURE_CLASS_FIELDS = (
    "asset_class",
    "allocation_module",
    "allocation_role",
    "region_market",
    "level1_group",
    "level2_group",
    "budget_scope",
)


def _screening_fingerprint(fact: dict[str, Any], thresholds: dict[str, Any]) -> str:
    """重筛排除项的业务触发器；不因每日日期/规模的小幅变动反复调用模型。"""
    return sha256_json(
        {
            **_subset(
                fact,
                (
                    "fund_code",
                    "fund_name",
                    "tracking_index_code",
                    "tracking_index_name",
                    "market",
                    "etf_type",
                    "list_date",
                    "status",
                    "total_fee_pct",
                ),
            ),
            "product_auxiliary_state": _product_auxiliary_state(fact, thresholds),
        }
    )


class CandidateAutomationError(RuntimeError):
    """自动维护计划或执行不满足安全条件。"""


@dataclass(frozen=True)
class DecisionEnvelope:
    decision: dict[str, Any]
    result: DeepSeekBatchResult
    raw_decision: dict[str, Any] | None = None
    identity_adjustments: dict[str, Any] | None = None


@dataclass
class CandidateAutomationPlan:
    """可审阅、可哈希的只读运行计划，以及执行所需的数据库快照。"""

    plan_hash: str
    plan_payload: dict[str, Any]
    source_batch: dict[str, Any]
    current_records: list[dict[str, Any]]
    facts_by_code: dict[str, dict[str, Any]]
    target_items: list[dict[str, Any]]
    taxonomy: dict[str, Any]

    @property
    def executable(self) -> bool:
        return bool(self.plan_payload["executable"])

    def summary(self) -> dict[str, Any]:
        return {
            "status": "planned",
            "plan_hash": self.plan_hash,
            "executable": self.executable,
            "run_month": self.plan_payload["run_month"],
            "screening_scope": self.plan_payload["screening_scope"],
            "coverage_counts": self.plan_payload["coverage_counts"],
            "facts_as_of": self.plan_payload["facts_as_of"],
            "source_snapshot_id": self.plan_payload["source_snapshot_id"],
            "current_candidate_count": self.plan_payload["current_candidate_count"],
            "llm_target_count": self.plan_payload["llm_target_count"],
            "new_product_count": self.plan_payload["new_product_count"],
            "deferred_new_product_count": self.plan_payload[
                "deferred_new_product_count"
            ],
            "target_reasons": self.plan_payload["target_reasons"],
            "guards": self.plan_payload["guards"],
            "model_requested": self.plan_payload["model_requested"],
            "llm_config": self.plan_payload.get("llm_config"),
            "prompt_version": self.plan_payload["prompt_version"],
            "prompt_sha256": self.plan_payload["prompt_sha256"],
        }


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)[:10]).isoformat()


def _first_of_month(value: date) -> date:
    return value.replace(day=1)


def _json_rows(cursor: Any, sql: str, params: tuple[Any, ...] = ()) -> list[dict]:
    cursor.execute(sql, params)
    return [row["payload"] for row in cursor.fetchall()]


def _subset(source: dict[str, Any], fields: Iterable[str]) -> dict[str, Any]:
    return {field: source.get(field) for field in fields}


def _taxonomy(current_records: list[dict[str, Any]]) -> dict[str, Any]:
    enums: dict[str, list[str]] = {}
    for field in ENUM_PATCH_FIELDS:
        values = {
            str(record[field]).strip()
            for record in current_records
            if record.get(field) is not None and str(record[field]).strip()
        }
        enums[field] = sorted(values)
    for field, values in classification_values().items():
        enums[field] = sorted(set(enums[field]) | set(values))

    exposure_reference: dict[str, dict[str, Any]] = {}
    for record in sorted(
        current_records,
        key=lambda r: (len(missing_classification_fields(r)), r.get("source_rank", 0)),
    ):
        exposure_id = str(record.get("exposure_id") or "").strip()
        if not exposure_id or exposure_id in exposure_reference:
            continue
        exposure_reference[exposure_id] = {
            "exposure_id": exposure_id,
            "exposure_name": record.get("exposure_name"),
            "asset_class": record.get("asset_class"),
            "allocation_module": record.get("allocation_module"),
            "allocation_role": record.get("allocation_role"),
            "region_market": record.get("region_market"),
            "level1_group": record.get("level1_group"),
            "level2_group": record.get("level2_group"),
            "budget_scope": record.get("budget_scope"),
        }
    return {
        "allowed_patch_fields": list(CURATED_PATCH_FIELDS),
        "required_add_fields": sorted(ADD_REQUIRED_FIELDS),
        "allowed_values": enums,
        "status_permission_mapping": STATUS_PERMISSION,
        "minimum_automatic_confidence": MIN_AUTOMATIC_CONFIDENCE,
        "classification_taxonomy_version": TAXONOMY_VERSION,
        "level2_by_level1": LEVEL2_BY_LEVEL1,
        "classification_rules": list(CLASSIFICATION_RULES),
        "require_complete_classification_for_confirmation": True,
        "exposure_reference": sorted(
            exposure_reference.values(), key=lambda row: row["exposure_id"]
        ),
    }


def _target_reason(
    record: dict[str, Any],
    fact: dict[str, Any],
    *,
    reconfirm_all: bool,
    thresholds: dict[str, Any],
) -> str | None:
    if record.get("confirmation_status") == CONFIRMATION_STATUS_HUMAN:
        return None
    if reconfirm_all:
        return "reconfirm_all"
    if record.get("confirmation_status") in (None, CONFIRMATION_STATUS_LEGACY):
        return "legacy_unconfirmed"
    if missing_classification_fields(record):
        return "classification_incomplete"
    if (record.get("fund_name") or "") != (fact.get("fund_name") or ""):
        return "fund_name_changed"
    if (record.get("tracking_index_code") or "") != (
        fact.get("tracking_index_code") or ""
    ):
        return "tracking_index_changed"
    live_auxiliary_state = _product_auxiliary_state(fact, thresholds)
    if record.get("snapshot_product_auxiliary_state") != live_auxiliary_state:
        return "product_auxiliary_state_changed"
    old_fee = record.get("snapshot_total_fee_pct")
    live_fee = fact.get("total_fee_pct")
    if old_fee is None or live_fee is None:
        if old_fee is not live_fee:
            return "total_fee_changed"
    elif abs(float(old_fee) - float(live_fee)) > 1e-12:
        return "total_fee_changed"
    return None


def _fact_quality_issues(
    fact: dict[str, Any] | None,
    *,
    facts_as_of: str,
    minimum_fact_dates: dict[str, str | None],
) -> list[str]:
    """逐产品校验实际业务日期及将写入快照的核心数值。"""

    if fact is None:
        return ["missing_product_facts"]
    issues: list[str] = []
    if _iso_date(fact.get("as_of_date")) != facts_as_of:
        issues.append("as_of_date_mismatch")
    if fact.get("core_facts_complete") is not True:
        issues.append("core_facts_incomplete")
    if fact.get("amount_20d_days") != 20:
        issues.append("amount_20d_history_incomplete")
    is_lof = fact.get("product_type") == "LOF"
    for field in FACT_MAX_LAG_TRADE_DAYS:
        if is_lof and field == "share_date":
            continue  # LOF shares are periodic; they are not a daily AUM input.
        value = _iso_date(fact.get(field))
        minimum = minimum_fact_dates.get(field)
        if value is None:
            issues.append(f"{field}_missing")
        elif minimum is None or value < minimum:
            issues.append(f"{field}_stale")
        elif value > facts_as_of:
            issues.append(f"{field}_after_as_of")
    if is_lof:
        from .exchange_fund_sources import LOF_MAX_AUM_AGE_DAYS

        aum_date = _iso_date(fact.get("aum_date"))
        known_date = _iso_date(fact.get("aum_known_date"))
        if not aum_date or not known_date:
            issues.append("lof_aum_availability_missing")
        elif not aum_date <= known_date <= facts_as_of:
            issues.append("lof_aum_not_known_as_of")
        elif (
            date.fromisoformat(facts_as_of) - date.fromisoformat(aum_date)
        ).days > LOF_MAX_AUM_AGE_DAYS:
            issues.append("lof_aum_report_stale")
    for field in REQUIRED_NUMERIC_FACTS:
        value = fact.get(field)
        try:
            number = float(value)
        except (TypeError, ValueError):
            issues.append(f"{field}_missing_or_invalid")
            continue
        if (
            isinstance(value, bool)
            or not math.isfinite(number)
            or number < 0
            or (field == "aum_100m" and number == 0)
        ):
            issues.append(f"{field}_invalid")
    return issues


def build_candidate_automation_plan(
    connection: Any,
    *,
    model_requested: str,
    llm_base_url: str | None = None,
    run_date: date | None = None,
    reconfirm_all: bool = False,
    max_new_products: int = 50,
    screening_scope: str = "incremental",
) -> CandidateAutomationPlan:
    """只读生成计划；不调用模型、不写数据库。"""

    if screening_scope not in SCREENING_SCOPES:
        raise ValueError(f"screening_scope must be one of {SCREENING_SCOPES}")
    if max_new_products < 0:
        raise ValueError("max_new_products must be non-negative")
    llm_config = resolve_candidate_llm_config(
        model=model_requested, base_url=llm_base_url
    )
    missing_schema = missing_confirmation_schema(connection)
    if missing_schema:
        raise CandidateAutomationError(
            "ETF candidate confirmation migration is required: "
            + ", ".join(missing_schema)
        )

    effective_run_date = run_date or datetime.now().astimezone().date()
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        source_batches = _json_rows(
            cursor,
            """
            SELECT to_jsonb(b) AS payload
            FROM fund_pool_on.etf_candidate_master_latest_batch b
            """,
        )
        if not source_batches:
            raise CandidateAutomationError("no loaded ETF candidate snapshot")
        source_batch = source_batches[0]
        current_records = _json_rows(
            cursor,
            """
            SELECT to_jsonb(c) AS payload
            FROM fund_pool_on.etf_candidate_master_current c
            ORDER BY c.source_rank, c.fund_code
            """,
        )
        cursor.execute(
            """
            SELECT fund_code FROM (
                SELECT DISTINCT ON (s.fund_code)
                       s.fund_code, s.include_in_candidate_pool
                FROM fund_pool_on.etf_candidate_master_snapshot s
                JOIN fund_pool_on.etf_candidate_master_batch b USING (snapshot_id)
                WHERE b.load_status = 'loaded'
                ORDER BY s.fund_code, b.workbook_generated_on DESC, b.loaded_at DESC
            ) latest_membership
            WHERE NOT include_in_candidate_pool
            """,
        )
        rejected_codes = {row["fund_code"] for row in cursor.fetchall()}
        facts = _json_rows(
            cursor,
            """
            SELECT to_jsonb(f) || jsonb_build_object(
                'tracking_index_name', e.benchmark
            ) AS payload
            FROM features.exchange_fund_product_facts_current f
            LEFT JOIN rawdata.fund_etf_basic e ON e.ts_code = f.fund_code
            ORDER BY f.fund_code
            """,
        )
        # fund_basic 补足尚未同步到 ETF 名录的新上市产品，只用于覆盖/缺数台账。
        inventory = _json_rows(
            cursor,
            """
            SELECT to_jsonb(u) - 'priority' AS payload FROM (
                SELECT DISTINCT ON (fund_code) * FROM (
                    SELECT e.ts_code AS fund_code, e.name AS fund_name, e.list_date,
                           e.status, b.list_date AS basic_list_date,
                           b.status AS basic_status, 'ETF'::text AS product_type, 0 AS priority
                    FROM rawdata.fund_etf_basic e
                    LEFT JOIN rawdata.fund_basic b ON b.ts_code = e.ts_code
                    UNION ALL
                    SELECT ts_code, name, list_date, status, list_date, status, 'ETF', 1
                    FROM rawdata.fund_basic
                    WHERE ts_code ~ '[.](SH|SZ)$' AND name ILIKE %s
                      AND name NOT LIKE %s
                    UNION ALL
                    SELECT ts_code,name,list_date,status,list_date,status,'LOF',2
                    FROM rawdata.fund_basic
                    WHERE market='E' AND ts_code ~ '^(16[0-9]{4}[.]SZ|50[12][0-9]{3}[.]SH)$'
                      AND name NOT LIKE '%%分级%%'
                ) combined ORDER BY fund_code, priority
            ) u ORDER BY fund_code
        """,
            ("%ETF%", "%联接%"),
        )
        cursor.execute(
            """
            SELECT DISTINCT ON (d.fund_code) d.fund_code, d.decision_action,
                   d.evidence_payload->>'screening_fingerprint' AS screening_fingerprint
            FROM fund_pool_on.etf_candidate_ai_decision d
            JOIN fund_pool_on.etf_candidate_ai_run r USING (ai_run_id)
            WHERE r.status = 'SUCCEEDED'
            ORDER BY d.fund_code, r.finished_at DESC, r.ai_run_id DESC
        """
        )
        previous_exclusions = {
            row["fund_code"]: row["screening_fingerprint"]
            for row in cursor.fetchall()
            if row["decision_action"] == "EXCLUDE_NEW"
        }
        cursor.execute(
            """
            SELECT DISTINCT cal_date::date AS trade_date
            FROM rawdata.others_calendar
            WHERE exchange = 'SSE'
              AND is_open = 1
              AND cal_date::date < %s
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (effective_run_date, max(FACT_MAX_LAG_TRADE_DAYS.values()) + 1),
        )
        trade_dates = [row["trade_date"] for row in cursor.fetchall()]
        expected_prior_trade_date = trade_dates[0] if trade_dates else None
        cursor.execute(
            """
            SELECT facts_as_of AS last_facts_as_of,
                   plan_payload->'guards'->'deferred_new_product_fact_codes'
                       AS deferred_new_product_fact_codes
            FROM fund_pool_on.etf_candidate_ai_run
            WHERE status = 'SUCCEEDED'
            ORDER BY facts_as_of DESC, finished_at DESC
            LIMIT 1
            """
        )
        last_success_row = cursor.fetchone()
        last_success_facts_as_of = (
            last_success_row["last_facts_as_of"] if last_success_row else None
        )
        deferred_codes = set(
            (last_success_row or {}).get("deferred_new_product_fact_codes") or []
        )

    if not current_records:
        raise CandidateAutomationError("current ETF candidate snapshot is empty")
    if not facts:
        raise CandidateAutomationError("ETF product facts view is empty")

    facts_by_code = {str(fact["fund_code"]).strip().upper(): fact for fact in facts}
    current_codes = {
        str(record["fund_code"]).strip().upper() for record in current_records
    }
    facts_as_of_values = {
        _iso_date(fact.get("as_of_date")) for fact in facts if fact.get("as_of_date")
    }
    if len(facts_as_of_values) != 1:
        raise CandidateAutomationError(
            f"product facts must have one as_of_date, got {facts_as_of_values}"
        )
    facts_as_of = next(iter(facts_as_of_values))
    facts_as_of_date = date.fromisoformat(facts_as_of)

    source_cutoff = (
        _iso_date(last_success_facts_as_of)
        or _iso_date(source_batch.get("product_facts_as_of"))
        or _iso_date(source_batch.get("structure_baseline_as_of"))
    )
    if source_cutoff is None:
        raise CandidateAutomationError("candidate discovery cutoff is unavailable")

    missing_fact_codes = sorted(current_codes - set(facts_by_code))
    minimum_fact_dates = {
        field: _iso_date(trade_dates[lag]) if len(trade_dates) > lag else None
        for field, lag in FACT_MAX_LAG_TRADE_DAYS.items()
    }
    current_fact_issues = {
        code: issues
        for code in sorted(current_codes)
        if (
            issues := _fact_quality_issues(
                facts_by_code.get(code),
                facts_as_of=facts_as_of,
                minimum_fact_dates=minimum_fact_dates,
            )
        )
    }
    inventory_by_code = {row["fund_code"]: row for row in inventory}
    all_codes = (
        set(inventory_by_code) | set(facts_by_code) | current_codes | deferred_codes
    )
    coverage: dict[str, dict[str, Any]] = {}
    discovered_new_codes: set[str] = set()
    for code in sorted(all_codes):
        fact = facts_by_code.get(code)
        metadata = inventory_by_code.get(code) or fact or {}
        # 基金总名录更新较及时；ETF 专用名录仍为 P 时不能漏掉已上市产品。
        listed_on = _iso_date(
            metadata.get("basic_list_date") or metadata.get("list_date")
        )
        listing_status = metadata.get("basic_status") or metadata.get("status")
        if code in current_codes:
            state = "current_candidate"
        elif code in rejected_codes:
            state = "human_rejected"
        elif screening_scope == "lof_initial" and metadata.get("product_type") != "LOF":
            state = "outside_requested_product_type"
        elif not re.fullmatch(r"[0-9]{6}\.(SH|SZ)", code):
            state = "out_of_scope_code"
        elif listing_status in ("D", "P") or (listed_on and listed_on > facts_as_of):
            state = "not_listed_as_of"
        elif (
            screening_scope == "incremental"
            and fact is not None
            and previous_exclusions.get(code) is not None
            and previous_exclusions.get(code)
            == _screening_fingerprint(fact, source_batch["thresholds"])
        ):
            state = "previously_excluded_unchanged"
        else:
            state = "pending_screening"
            discovered_new_codes.add(code)
        coverage[code] = {"state": state, "fund_name": metadata.get("fund_name")}
    deferred_new_issues = {
        code: issues
        for code in sorted(discovered_new_codes)
        if (
            issues := _fact_quality_issues(
                facts_by_code.get(code),
                facts_as_of=facts_as_of,
                minimum_fact_dates=minimum_fact_dates,
            )
        )
    }
    new_facts = [
        facts_by_code[code]
        for code in discovered_new_codes
        if code not in deferred_new_issues
    ]
    new_facts.sort(key=lambda fact: (str(fact.get("list_date")), fact["fund_code"]))
    for code, issues in deferred_new_issues.items():
        coverage[code].update(state="deferred_product_facts", issues=issues)

    target_items: list[dict[str, Any]] = []
    target_reasons: dict[str, int] = {}
    for record in current_records:
        if screening_scope == "lof_initial":
            continue  # Bootstrap LOFs without reclassifying the existing ETF archive.
        code = str(record["fund_code"]).strip().upper()
        fact = facts_by_code.get(code)
        if fact is None or code in current_fact_issues:
            continue
        reason = _target_reason(
            record,
            fact,
            reconfirm_all=reconfirm_all or screening_scope == "full",
            thresholds=source_batch["thresholds"],
        )
        if reason is None:
            continue
        target_reasons[reason] = target_reasons.get(reason, 0) + 1
        target_items.append(
            {
                "kind": "existing",
                "reason": reason,
                "fund_code": code,
                "current_record": _subset(record, PROMPT_CURRENT_FIELDS),
                "live_product_facts": _subset(fact, PROMPT_FACT_FIELDS),
            }
        )

    for fact in new_facts:
        reason = (
            "full_universe"
            if screening_scope == "full"
            else "unseen_or_changed_product"
        )
        target_reasons[reason] = target_reasons.get(reason, 0) + 1
        target_items.append(
            {
                "kind": "new",
                "reason": reason,
                "fund_code": str(fact["fund_code"]).strip().upper(),
                "current_record": None,
                "live_product_facts": _subset(fact, PROMPT_FACT_FIELDS),
            }
        )

    taxonomy = _taxonomy(current_records)
    # 同指数候选是分类参照，不能仅因批次不同就把同一暴露命名为多个新暴露。
    for item in target_items:
        item["same_index_candidates"] = [
            _subset(
                row,
                (
                    "fund_code",
                    "fund_name",
                    "tracking_index_code",
                    "exposure_id",
                    "exposure_name",
                    "product_role",
                    "candidate_status",
                ),
            )
            for row in current_records
            if row.get("tracking_index_code")
            and row["tracking_index_code"]
            == item["live_product_facts"].get("tracking_index_code")
        ]
        if item["kind"] == "new":
            coverage[item["fund_code"]]["state"] = "model_target_new"
        else:
            coverage[item["fund_code"]]["state"] = "model_target_existing"
    coverage_counts: dict[str, int] = {"total": len(coverage)}
    for row in coverage.values():
        coverage_counts[row["state"]] = coverage_counts.get(row["state"], 0) + 1
    expected_date_iso = _iso_date(expected_prior_trade_date)
    guards = {
        "source_snapshot_loaded": True,
        "product_facts_single_as_of": True,
        "product_facts_cover_all_current_candidates": not missing_fact_codes,
        "missing_product_fact_codes": missing_fact_codes,
        "current_product_facts_valid": not current_fact_issues,
        "current_product_fact_issues": current_fact_issues,
        "fact_max_lag_trade_days": FACT_MAX_LAG_TRADE_DAYS.copy(),
        "minimum_fact_dates": minimum_fact_dates,
        "deferred_new_product_fact_codes": sorted(deferred_new_issues),
        "deferred_new_product_fact_issues": deferred_new_issues,
        "expected_prior_trade_date": expected_date_iso,
        "product_facts_fresh": bool(
            expected_prior_trade_date
            and expected_prior_trade_date <= facts_as_of_date <= effective_run_date
        ),
        "new_product_count_within_limit": len(new_facts) <= max_new_products,
        "max_new_products": max_new_products,
        "capital_authority": False,
        "order_authority": False,
    }
    executable = bool(
        guards["product_facts_cover_all_current_candidates"]
        and guards["current_product_facts_valid"]
        and guards["product_facts_fresh"]
        and guards["new_product_count_within_limit"]
    )
    plan_payload = {
        "contract": "etf_candidate_ai_automation_plan_v2",
        "exposure_identity_policy": EXPOSURE_IDENTITY_POLICY,
        "screening_scope": screening_scope,
        "coverage_counts": coverage_counts,
        "coverage": coverage,
        "human_rejected_codes": sorted(rejected_codes),
        "run_month": _first_of_month(effective_run_date).isoformat(),
        "run_date": effective_run_date.isoformat(),
        "facts_as_of": facts_as_of,
        "discovery_cutoff": source_cutoff,
        "source_snapshot_id": source_batch["snapshot_id"],
        "source_batch_fingerprint": sha256_json(source_batch),
        "model_requested": llm_config.model,
        "llm_config": llm_config.audit(),
        "prompt_version": PROMPT_VERSION,
        "prompt_sha256": PROMPT_SHA256,
        "reconfirm_all": reconfirm_all,
        "current_candidate_count": len(current_records),
        "llm_target_count": len(target_items),
        "new_product_count": len(new_facts),
        "discovered_new_product_count": len(discovered_new_codes),
        "deferred_new_product_count": len(deferred_new_issues),
        "target_reasons": target_reasons,
        "target_items": target_items,
        "taxonomy": taxonomy,
        "current_snapshot_fingerprint": sha256_json(
            [
                {
                    key: record.get(key)
                    for key in SNAPSHOT_COLUMNS
                    if key != "snapshot_id"
                }
                for record in current_records
            ]
        ),
        "product_facts_fingerprint": sha256_json(
            [
                _subset(facts_by_code[code], PROMPT_FACT_FIELDS)
                for code in sorted(current_codes & set(facts_by_code))
            ]
            + [
                (
                    {
                        "fund_code": code,
                        "facts": _subset(facts_by_code[code], PROMPT_FACT_FIELDS),
                    }
                    if code in facts_by_code
                    else {"fund_code": code, "facts": None}
                )
                for code in sorted(discovered_new_codes)
            ]
        ),
        "guards": guards,
        "executable": executable,
    }
    return CandidateAutomationPlan(
        plan_hash=sha256_json(plan_payload),
        plan_payload=plan_payload,
        source_batch=source_batch,
        current_records=current_records,
        facts_by_code=facts_by_code,
        target_items=target_items,
        taxonomy=taxonomy,
    )


def _validate_patch(
    decision: dict[str, Any],
    *,
    taxonomy: dict[str, Any],
) -> None:
    code = decision["fund_code"]
    action = decision["action"]
    patch = decision["classification_patch"]
    extra = set(patch) - set(CURATED_PATCH_FIELDS)
    if extra:
        raise CandidateAutomationError(
            f"model patch contains forbidden fields for {code}: {sorted(extra)}"
        )
    if action == "UPDATE" and not patch:
        raise CandidateAutomationError(f"UPDATE patch is empty for {code}")
    if action == "ADD":
        missing = {
            field
            for field in ADD_REQUIRED_FIELDS
            if not str(patch.get(field) or "").strip()
        }
        if missing:
            raise CandidateAutomationError(
                f"ADD patch missing fields for {code}: {sorted(missing)}"
            )
    for field, value in patch.items():
        if value is not None and not isinstance(value, str):
            raise CandidateAutomationError(
                f"model patch field {field} must be string or null for {code}"
            )
    exposure_id = patch.get("exposure_id")
    if exposure_id and not EXPOSURE_ID_RE.fullmatch(exposure_id):
        raise CandidateAutomationError(
            f"invalid exposure_id generated for {code}: {exposure_id}"
        )
    if (
        "candidate_status" in patch
        and patch["candidate_status"] not in STATUS_PERMISSION
    ):
        raise CandidateAutomationError(f"invalid candidate_status generated for {code}")
    allowed_values = taxonomy["allowed_values"]
    for field in ENUM_PATCH_FIELDS:
        value = patch.get(field)
        values = allowed_values.get(field) or []
        if value and values and value not in values:
            raise CandidateAutomationError(
                f"out-of-taxonomy {field} for {code}: {value}"
            )
    if decision["include_in_candidate_pool"] and not decision["evidence"]:
        raise CandidateAutomationError(f"included candidate has no evidence for {code}")


def _decision_confirmation_status(
    decision: dict[str, Any], record: dict[str, Any] | None = None
) -> str:
    if (
        decision["confidence"] < MIN_AUTOMATIC_CONFIDENCE
        or decision["requires_human_review"]
        or (record is not None and missing_classification_fields(record))
    ):
        return CONFIRMATION_STATUS_AI_REVIEW
    return CONFIRMATION_STATUS_AI


def _number(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _product_auxiliary_state(fact: dict[str, Any], thresholds: dict[str, Any]) -> str:
    age = _number(fact.get("age_months"))
    aum = _number(fact.get("aum_100m"))
    amount = _number(fact.get("amount_20d_100m"))
    if age is None or age < float(thresholds["minimum_age_months"]):
        return "观察"
    if (
        aum is not None
        and amount is not None
        and aum >= float(thresholds["strong_aum_100m"])
        and amount >= float(thresholds["strong_amount_20d_100m"])
    ):
        return "强"
    if (
        aum is not None
        and amount is not None
        and aum >= float(thresholds["usable_aum_100m"])
        and amount >= float(thresholds["usable_amount_20d_100m"])
    ):
        return "可用"
    return "观察"


def _premium_label(
    asset_class: str, fact: dict[str, Any], thresholds: dict[str, Any]
) -> str:
    if asset_class != "跨境权益":
        return "不适用"
    premium = _number(fact.get("mean_abs_premium_60d"))
    if premium is None:
        return "缺失"
    if premium <= float(thresholds["low_premium_upper"]):
        return "偏离较低"
    if premium <= float(thresholds["medium_premium_upper"]):
        return "偏离中等"
    return "偏离较高"


def _refresh_record_facts(
    record: dict[str, Any],
    fact: dict[str, Any],
    *,
    facts_as_of: str,
    thresholds: dict[str, Any],
) -> None:
    record["fund_name"] = fact.get("fund_name") or record.get("fund_name")
    record["tracking_index_code"] = fact.get("tracking_index_code")
    record["snapshot_aum_100m"] = fact.get("aum_100m")
    record["snapshot_amount_20d_100m"] = fact.get("amount_20d_100m")
    record["snapshot_age_months"] = fact.get("age_months")
    record["snapshot_total_fee_pct"] = fact.get("total_fee_pct")
    record["snapshot_mean_abs_premium_60d"] = fact.get("mean_abs_premium_60d")
    record["snapshot_product_auxiliary_state"] = _product_auxiliary_state(
        fact, thresholds
    )
    record["snapshot_premium_observation_label"] = _premium_label(
        str(record.get("asset_class") or ""), fact, thresholds
    )
    record["product_facts_as_of"] = facts_as_of
    record["data_source_id"] = f"features.mv_etf_product_facts_current:{facts_as_of}"
    record["research_permission"] = STATUS_PERMISSION[record["candidate_status"]]
    record["duplicate_check"] = "OK"
    if record.get("include_in_candidate_pool") is None:
        record["include_in_candidate_pool"] = True


def _new_record(
    *,
    fact: dict[str, Any],
    decision: dict[str, Any],
    source_row_number: int,
    source_rank: int,
) -> dict[str, Any]:
    patch = decision["classification_patch"]
    record = {column: None for column in SNAPSHOT_COLUMNS if column != "snapshot_id"}
    record.update(patch)
    record.update(
        {
            "source_row_number": source_row_number,
            "source_rank": source_rank,
            "fund_code": str(fact["fund_code"]).strip().upper(),
            "fund_name": fact.get("fund_name"),
            "tracking_index_code": fact.get("tracking_index_code"),
            "manual_review_status": "AI大模型确认",
            "duplicate_check": "OK",
            "include_in_candidate_pool": True,
        }
    )
    return record


def build_ai_snapshot_payload(
    plan: CandidateAutomationPlan,
    *,
    ai_run_id: str,
    envelopes: list[DecisionEnvelope],
    confirmed_at: datetime,
) -> tuple[dict[str, Any], str]:
    """将经本地验证的模型决定合并为完整版本化候选快照。"""

    if not plan.executable:
        raise CandidateAutomationError("cannot build snapshot from a blocked plan")
    envelope_by_code: dict[str, DecisionEnvelope] = {}
    for envelope in envelopes:
        decision = envelope.decision
        _validate_patch(decision, taxonomy=plan.taxonomy)
        code = decision["fund_code"]
        if code in envelope_by_code:
            raise CandidateAutomationError(f"duplicate decision envelope: {code}")
        envelope_by_code[code] = envelope
    expected_codes = {item["fund_code"] for item in plan.target_items}
    if set(envelope_by_code) != expected_codes:
        raise CandidateAutomationError(
            "LLM decision coverage mismatch: expected="
            f"{sorted(expected_codes)}, actual={sorted(envelope_by_code)}"
        )

    thresholds = plan.source_batch["thresholds"]
    facts_as_of = plan.plan_payload["facts_as_of"]
    codes_to_publish = {row["fund_code"] for row in plan.current_records} | {
        code
        for code, envelope in envelope_by_code.items()
        if envelope.decision["include_in_candidate_pool"]
    }
    fact_issues = {
        code: issues
        for code in sorted(codes_to_publish)
        if (
            issues := _fact_quality_issues(
                plan.facts_by_code.get(code),
                facts_as_of=facts_as_of,
                minimum_fact_dates=plan.plan_payload["guards"]["minimum_fact_dates"],
            )
        )
    }
    if fact_issues:
        raise CandidateAutomationError(
            "candidate product facts failed validation: " + canonical_json(fact_issues)
        )
    confirmation_time = confirmed_at.astimezone(timezone.utc).isoformat()
    provider = plan.plan_payload.get("llm_config", {}).get("provider", "deepseek")
    records: list[dict[str, Any]] = []

    for current in plan.current_records:
        code = str(current["fund_code"]).strip().upper()
        fact = plan.facts_by_code[code]
        record = {
            column: current.get(column)
            for column in SNAPSHOT_COLUMNS
            if column != "snapshot_id"
        }
        envelope = envelope_by_code.get(code)
        if envelope is not None:
            decision = envelope.decision
            proposed = {**record, **decision["classification_patch"]}
            confirmation_status = _decision_confirmation_status(decision, proposed)
            if (
                decision["action"] == "UPDATE"
                and confirmation_status == CONFIRMATION_STATUS_AI
            ):
                record.update(decision["classification_patch"])
            # A null-field completion is evidence about these fields only. Keep
            # the original whole-record review status and its supporting audit.
            if plan.plan_payload.get("screening_scope") != "classification_completion":
                record["confirmation_status"] = confirmation_status
                record["confirmation_actor"] = (
                    f"{provider}:{envelope.result.actual_model}"
                )
                record["confirmation_at"] = confirmation_time
                record["ai_run_id"] = ai_run_id
                record["ai_model"] = envelope.result.actual_model
                record["ai_confidence"] = decision["confidence"]
                record["ai_decision_hash"] = sha256_json(decision)
                record["human_review_note"] = None
                record["manual_review_status"] = (
                    "AI待人工复核"
                    if confirmation_status == CONFIRMATION_STATUS_AI_REVIEW
                    else "AI大模型确认"
                )
        elif record.get("confirmation_status") == CONFIRMATION_STATUS_HUMAN:
            record["manual_review_status"] = "人工确认"
        _refresh_record_facts(
            record,
            fact,
            facts_as_of=facts_as_of,
            thresholds=thresholds,
        )
        records.append(record)

    next_source_row = max(int(row["source_row_number"]) for row in records) + 1
    next_rank = max(int(row["source_rank"]) for row in records) + 1
    for item in plan.target_items:
        if item["kind"] != "new":
            continue
        envelope = envelope_by_code[item["fund_code"]]
        decision = envelope.decision
        if decision["action"] == "EXCLUDE_NEW":
            continue
        fact = plan.facts_by_code[item["fund_code"]]
        record = _new_record(
            fact=fact,
            decision=decision,
            source_row_number=next_source_row,
            source_rank=next_rank,
        )
        confirmation_status = _decision_confirmation_status(decision)
        if confirmation_status == CONFIRMATION_STATUS_AI_REVIEW:
            record["candidate_status"] = "观察"
            record["manual_review_status"] = "AI待人工复核"
        record.update(
            {
                "confirmation_status": confirmation_status,
                "confirmation_actor": (
                    f"{provider}:{envelope.result.actual_model}"
                ),
                "confirmation_at": confirmation_time,
                "ai_run_id": ai_run_id,
                "ai_model": envelope.result.actual_model,
                "ai_confidence": decision["confidence"],
                "ai_decision_hash": sha256_json(decision),
                "human_review_note": None,
            }
        )
        _refresh_record_facts(
            record,
            fact,
            facts_as_of=facts_as_of,
            thresholds=thresholds,
        )
        records.append(record)
        next_source_row += 1
        next_rank += 1

    decision_manifest = {
        "ai_run_id": ai_run_id,
        "plan_hash": plan.plan_hash,
        "source_snapshot_id": plan.source_batch["snapshot_id"],
        "facts_as_of": facts_as_of,
        "decisions": [envelope.decision for envelope in envelopes],
        "record_fingerprint": sha256_json(records),
    }
    manifest_hash = sha256_json(decision_manifest)
    snapshot_id = (
        "etf_candidate_master_ai_"
        + plan.plan_payload["run_month"].replace("-", "")[:6]
        + "_"
        + manifest_hash[:16]
    )
    exposures = {record["exposure_id"] for record in records}
    payload = {
        "contract_version": CONTRACT_VERSION,
        "snapshot_id": snapshot_id,
        "source": {
            "source_version": (
                (
                    "AI_FULL_"
                    if plan.plan_payload.get("screening_scope") == "full"
                    else (
                        "AI_REVIEW_"
                        if plan.plan_payload.get("screening_scope") == "review"
                        else (
                            "AI_CLASSIFICATION_COMPLETION_"
                            if plan.plan_payload.get("screening_scope")
                            == "classification_completion"
                            else "AI_MONTHLY_"
                        )
                    )
                )
                + plan.plan_payload["run_month"].replace("-", "")[:6]
            ),
            "source_file_name": f"{snapshot_id}.json",
            "source_file_path": None,
            "source_file_sha256": manifest_hash,
            "workbook_generated_on": plan.plan_payload["run_date"],
            "product_facts_as_of": facts_as_of,
            "structure_baseline_as_of": _iso_date(
                plan.source_batch["structure_baseline_as_of"]
            ),
            "exported_at": confirmation_time,
        },
        "governance": {
            "research_stage": "AI_AUTOMATED_CANDIDATE_CONFIRMATION",
            "authority_scope": "CANDIDATE_POOL_ONLY",
            "capital_authority": False,
            "order_authority": False,
        },
        "thresholds": thresholds,
        "quality": {
            "row_count": len(records),
            "exposure_count": len(exposures),
            "all_checks_ok": True,
            "checks": [
                "UNIQUE_FUND_CODE=OK",
                "UNIQUE_SOURCE_RANK=OK",
                "STATUS_PERMISSION=OK",
                "PRODUCT_FACT_COVERAGE=OK",
                "PRODUCT_FACT_COMPLETENESS=OK",
                "PRODUCT_FACT_FRESHNESS=OK",
                "AI_DECISION_COVERAGE=OK",
                "AI_REVIEW_ROUTING=OK",
                "CONFIRMATION_METADATA=OK",
                "NO_CAPITAL_NO_ORDERS=OK",
            ],
            "ai_run_id": ai_run_id,
            "plan_hash": plan.plan_hash,
            "decision_manifest_hash": manifest_hash,
        },
        "records": records,
    }
    validate_payload(payload, verify_source_file=False)
    return payload, manifest_hash


def _chunks(items: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _sum_usage(results: Iterable[DeepSeekBatchResult], field: str) -> int | None:
    values = [getattr(result, field) for result in results]
    present = [int(value) for value in values if value is not None]
    return sum(present) if present else None


def _resolve_exposure_identities(
    plan: CandidateAutomationPlan, envelopes: list[DecisionEnvelope]
) -> list[DecisionEnvelope]:
    """全批合并时分配稳定身份，保留原模型决定和本地修正，冲突降为待复核。

    既有字典可能有同暴露的不同指数实施变体，模型不能擅自拆分它们。
    新指数使用代码命名空间，不接受各模型批次独立递增的 IND_063 等编号。
    """
    current_by_code = {r["fund_code"]: r for r in plan.current_records}
    references: dict[str, list[dict[str, Any]]] = {}
    for row in plan.current_records:
        if row.get("tracking_index_code"):
            references.setdefault(row["tracking_index_code"], []).append(row)
    groups: dict[str, list[DecisionEnvelope]] = {}
    for envelope in envelopes:
        if envelope.decision["action"] == "ADD":
            code = envelope.decision["fund_code"]
            index_code = plan.facts_by_code[code].get("tracking_index_code")
            groups.setdefault(index_code or f"NO_INDEX_{code}", []).append(envelope)

    canonical: dict[str, dict[str, Any]] = {}
    conflicts: set[str] = set()
    for index_code, group in groups.items():
        if index_code in references:
            # 保留同一指数在原字典中的身份，按原候选排序确定参照。
            canonical[index_code] = references[index_code][0]
            if len({r["exposure_id"] for r in references[index_code]}) > 1:
                conflicts.add(index_code)
            continue
        best = max(
            group,
            key=lambda e: (
                sum(
                    bool(e.decision["classification_patch"].get(f))
                    for f in EXPOSURE_CLASS_FIELDS
                ),
                e.decision["confidence"],
                e.decision["fund_code"],
            ),
        )
        fact = plan.facts_by_code[best.decision["fund_code"]]
        canonical[index_code] = {
            **best.decision["classification_patch"],
            "exposure_id": (
                "LOF_PRODUCT_" + fact["fund_code"].replace(".", "_")
                if fact.get("product_type") == "LOF"
                and not fact.get("tracking_index_code")
                else "ETF_INDEX_" + re.sub(r"[^A-Za-z0-9_]", "_", index_code)
            ),
            "exposure_name": fact.get("tracking_index_name")
            or best.decision["classification_patch"]["exposure_name"],
        }
        signatures = {
            tuple(
                e.decision["classification_patch"].get(f) for f in EXPOSURE_CLASS_FIELDS
            )
            for e in group
        }
        if len(signatures) > 1 or (
            index_code.startswith("NO_INDEX_") and fact.get("product_type") != "LOF"
        ):
            conflicts.add(index_code)

    resolved: list[DecisionEnvelope] = []
    for envelope in envelopes:
        raw = envelope.decision
        decision = deepcopy(raw)
        patch = decision["classification_patch"]
        reasons: list[str] = []
        code = decision["fund_code"]
        if decision["action"] == "UPDATE":
            record = current_by_code[code]
            if any(
                f in patch
                and patch[f] != record.get(f)
                and (f == "exposure_id" or bool(record.get(f)))
                for f in ("exposure_id", *EXPOSURE_CLASS_FIELDS)
            ):
                reasons.append("既有暴露身份或分类调整须人工复核，自动流程保留原字典")
        elif decision["action"] == "ADD":
            fact = plan.facts_by_code[code]
            index_code = fact.get("tracking_index_code") or f"NO_INDEX_{code}"
            reference = canonical[index_code]
            if index_code in references:
                if any(
                    patch.get(f) and patch[f] != reference.get(f)
                    for f in ("asset_class", "allocation_module")
                ):
                    reasons.append(
                        "模型大类与已有同指数字典不一致，使用字典并转人工复核"
                    )
                patch.update(
                    product_role="备选工具",
                    exposure_relationship="同指数备份",
                    parent_fund_code=reference["fund_code"],
                )
            else:
                # 新指数不可自动把自己挂到不相同的既有指数父工具上。
                patch["parent_fund_code"] = None
                patch["exposure_relationship"] = "独立暴露"
            for field in ("exposure_id", "exposure_name", *EXPOSURE_CLASS_FIELDS):
                if reference.get(field) or field not in CLASSIFICATION_FIELDS:
                    patch[field] = reference.get(field)
            if index_code in conflicts:
                reasons.append(
                    "同指数跨批次分类不一致或指数身份不足，合并为一个稳定编号并待人工复核"
                )
        if reasons:
            decision["requires_human_review"] = True
            decision["uncertainty"] = list(
                dict.fromkeys(decision["uncertainty"] + reasons)
            )
        adjustments = {
            field: {"model": raw["classification_patch"].get(field), "resolved": value}
            for field, value in patch.items()
            if value != raw["classification_patch"].get(field)
        }
        if reasons:
            adjustments["review_reasons"] = reasons
        resolved.append(
            DecisionEnvelope(
                decision=decision,
                result=envelope.result,
                raw_decision=deepcopy(raw) if decision != raw else None,
                identity_adjustments=adjustments or None,
            )
        )
    return resolved


def get_existing_month_result(
    connection: Any,
    run_month: str,
    screening_scope: str = "incremental",
    plan_hash: str | None = None,
) -> dict[str, Any] | None:
    """返回自然月内最近一次成功结果；只读且不改变事务状态。"""

    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT ai_run_id, run_month, facts_as_of, output_snapshot_id,
                   decision_count, finished_at, model_requested,
                   plan_payload->'llm_config' AS llm_config
            FROM fund_pool_on.etf_candidate_ai_run
            WHERE run_month = %s AND status = 'SUCCEEDED'
              AND COALESCE(plan_payload->>'screening_scope', 'incremental') = %s
              AND (%s IS NULL OR plan_hash = %s)
            ORDER BY finished_at DESC
            LIMIT 1
            """,
            (run_month, screening_scope, plan_hash, plan_hash),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return {
        "status": "skipped_already_succeeded",
        **dict(row),
    }


def execute_candidate_automation(
    connection: Any,
    plan: CandidateAutomationPlan,
    *,
    client: CandidateLLMClient | None,
    expected_plan_hash: str | None = None,
    batch_size: int = 12,
    checkpoint_dir: str | Path | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    max_workers: int = 1,
) -> dict[str, Any]:
    """执行已生成计划；模型失败或本地验证失败时不写候选快照。"""

    if (
        sha256_json(plan.plan_payload) != plan.plan_hash
        or plan.target_items != plan.plan_payload.get("target_items")
        or plan.taxonomy != plan.plan_payload.get("taxonomy")
        or sha256_json(plan.source_batch) != plan.plan_payload.get("source_batch_fingerprint")
        or sha256_json([
            {key: record.get(key) for key in SNAPSHOT_COLUMNS if key != "snapshot_id"}
            for record in plan.current_records
        ]) != plan.plan_payload.get("current_snapshot_fingerprint")
    ):
        raise CandidateAutomationError("automation plan content differs from its frozen hash")
    if expected_plan_hash is not None and expected_plan_hash != plan.plan_hash:
        raise CandidateAutomationError(
            "expected plan hash does not match current read-only plan"
        )
    if not plan.executable:
        raise CandidateAutomationError(
            "automation plan is not executable: "
            + canonical_json(plan.plan_payload["guards"])
        )
    if batch_size < 1 or batch_size > 25:
        raise ValueError("batch_size must be between 1 and 25")
    if not 1 <= max_workers <= 8:
        raise ValueError("max_workers must be between 1 and 8")
    if plan.target_items and client is None:
        raise CandidateAutomationError("GLMS client is required for LLM targets")
    frozen_llm_config = plan.plan_payload.get("llm_config")
    if (
        client is not None
        and hasattr(client, "llm_config")
        and frozen_llm_config is None
    ):
        raise CandidateAutomationError(
            "model connection is missing from the frozen plan; preview again"
        )
    if client is not None and frozen_llm_config is not None:
        if getattr(client, "llm_config", frozen_llm_config) != frozen_llm_config:
            raise CandidateAutomationError(
                "model client connection does not match the frozen plan"
            )
    if client is not None and getattr(
        client, "prompt_sha256", plan.plan_payload.get("prompt_sha256")
    ) != plan.plan_payload.get("prompt_sha256"):
        raise CandidateAutomationError(
            "model client prompt does not match the frozen plan"
        )

    run_month = plan.plan_payload["run_month"]
    if client is not None and plan.plan_payload.get("generation_settings") is not None:
        if (
            getattr(client, "generation_settings", None)
            != plan.plan_payload["generation_settings"]
        ):
            raise CandidateAutomationError(
                "model generation settings do not match the frozen plan"
            )
    screening_scope = plan.plan_payload.get("screening_scope", "incremental")
    dedup_hash = (
        plan.plan_hash if screening_scope == "classification_completion" else None
    )
    existing = get_existing_month_result(
        connection, run_month, screening_scope, dedup_hash
    )
    if existing:
        return existing

    lock_acquired = False
    ai_run_id = "etf_ai_" + run_month.replace("-", "")[:6] + "_" + uuid4().hex[:12]
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s))",
                (AUTOMATION_LOCK_NAME,),
            )
            lock_acquired = bool(cursor.fetchone()[0])
        if not lock_acquired:
            connection.rollback()
            raise CandidateAutomationError(
                "another ETF candidate AI automation run is active"
            )
        connection.commit()

        existing = get_existing_month_result(
            connection, run_month, screening_scope, dedup_hash
        )
        if existing:
            return existing

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fund_pool_on.etf_candidate_ai_run (
                    ai_run_id, run_month, facts_as_of, source_snapshot_id,
                    plan_hash, plan_payload, model_requested, prompt_version,
                    status, candidate_count, new_product_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                          'RUNNING', %s, %s)
                """,
                (
                    ai_run_id,
                    run_month,
                    plan.plan_payload["facts_as_of"],
                    plan.source_batch["snapshot_id"],
                    plan.plan_hash,
                    Json(plan.plan_payload),
                    plan.plan_payload["model_requested"],
                    plan.plan_payload["prompt_version"],
                    len(plan.current_records),
                    plan.plan_payload["new_product_count"],
                ),
            )
        connection.commit()

        envelopes: list[DecisionEnvelope] = []
        batch_results: list[DeepSeekBatchResult] = []
        cache_dir = Path(checkpoint_dir) if checkpoint_dir is not None else None
        if cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)
        batches = list(_chunks(plan.target_items, batch_size))

        def confirm_one(
            batch: list[dict[str, Any]],
        ) -> tuple[DeepSeekBatchResult, bool]:
            assert client is not None
            cache_key = sha256_json(
                {
                    "model": plan.plan_payload["model_requested"],
                    "llm_config": frozen_llm_config,
                    "prompt": plan.plan_payload["prompt_sha256"],
                    "items": batch,
                    "taxonomy": plan.taxonomy,
                    **(
                        {
                            "generation_settings": plan.plan_payload[
                                "generation_settings"
                            ]
                        }
                        if "generation_settings" in plan.plan_payload
                        else {}
                    ),
                }
            )
            cache_file = cache_dir / (cache_key + ".json") if cache_dir else None
            cached = bool(cache_file and cache_file.exists())
            if cached:
                checkpoint = json.loads(cache_file.read_text(encoding="utf-8"))
                if checkpoint["cache_key"] != cache_key or checkpoint[
                    "result_hash"
                ] != sha256_json(checkpoint["result"]):
                    raise CandidateAutomationError("batch checkpoint hash mismatch")
                result = DeepSeekBatchResult(**checkpoint["result"])
            else:
                result = client.confirm_batch(items=batch, taxonomy=plan.taxonomy)
            validate_decisions(
                {"decisions": result.decisions},
                input_items=batch,
                taxonomy=plan.taxonomy,
            )
            for decision in result.decisions:
                _validate_patch(decision, taxonomy=plan.taxonomy)
            if cache_file and not cached:
                result_data = asdict(result)
                temporary_file = cache_file.with_suffix(".tmp")
                temporary_file.write_text(
                    canonical_json(
                        {
                            "cache_key": cache_key,
                            "result": result_data,
                            "result_hash": sha256_json(result_data),
                        }
                    ),
                    encoding="utf-8",
                )
                temporary_file.replace(cache_file)
            return result, cached

        completed_batches: dict[int, DeepSeekBatchResult] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(confirm_one, batch): number
                for number, batch in enumerate(batches)
            }
            try:
                for future in as_completed(futures):
                    result, cached = future.result()
                    completed_batches[futures[future]] = result
                    if progress_callback:
                        progress_callback(
                            {
                                "batch": len(completed_batches),
                                "batches": len(batches),
                                "completed_decisions": sum(
                                    len(r.decisions) for r in completed_batches.values()
                                ),
                                "total_decisions": len(plan.target_items),
                                "cached": cached,
                            }
                        )
            except Exception:
                for future in futures:
                    future.cancel()
                raise
        # 并发只用于无写入的模型请求；按原计划顺序合并并一次性提交数据库。
        for number in sorted(completed_batches):
            result = completed_batches[number]
            batch_results.append(result)
            envelopes.extend(
                DecisionEnvelope(decision=decision, result=result)
                for decision in result.decisions
            )
        # 模型调用期间不持候选写锁，允许人工复核；发布前在同一受保护事务中
        # 重读完整计划。任何人工状态、批次或产品事实变化都要求重新生成计划。
        lock_candidate_master(connection)
        if screening_scope == "classification_completion":
            from alphahome.curation.etf_candidate_classification_completion import (
                build_candidate_classification_completion_plan,
            )

            current_plan = build_candidate_classification_completion_plan(
                connection,
                model_requested=plan.plan_payload["model_requested"],
                llm_base_url=(frozen_llm_config or {}).get("base_url"),
                run_date=date.fromisoformat(plan.plan_payload["run_date"]),
                web_evidence=plan.plan_payload["web_evidence"],
                expected_target_count=plan.plan_payload["expected_target_count"],
            )
        elif screening_scope == "review":
            from alphahome.curation.etf_candidate_second_review import (
                build_candidate_second_review_plan,
            )

            current_plan = build_candidate_second_review_plan(
                connection,
                model_requested=plan.plan_payload["model_requested"],
                llm_base_url=(frozen_llm_config or {}).get("base_url"),
                run_date=date.fromisoformat(plan.plan_payload["run_date"]),
                expected_target_count=plan.plan_payload["expected_target_count"],
            )
        else:
            current_plan = build_candidate_automation_plan(
                connection,
                model_requested=plan.plan_payload["model_requested"],
                llm_base_url=(frozen_llm_config or {}).get("base_url"),
                run_date=date.fromisoformat(plan.plan_payload["run_date"]),
                reconfirm_all=plan.plan_payload["reconfirm_all"],
                max_new_products=plan.plan_payload["guards"]["max_new_products"],
                screening_scope=screening_scope,
            )
        if current_plan.plan_hash != plan.plan_hash or not current_plan.executable:
            raise CandidateAutomationError(
                "candidate automation plan changed before publication; "
                "regenerate the plan to preserve current human reviews and facts"
            )

        # Publish only the freshly revalidated database snapshot.
        plan = current_plan
        if screening_scope == "classification_completion":
            from alphahome.curation.etf_candidate_classification_completion import (
                resolve_completion_consistency,
            )

            envelopes = resolve_completion_consistency(plan, envelopes)
        elif screening_scope == "review":
            from alphahome.curation.etf_candidate_second_review import (
                resolve_review_consistency,
            )

            envelopes = resolve_review_consistency(plan, envelopes)
        else:
            envelopes = _resolve_exposure_identities(plan, envelopes)

        confirmed_at = datetime.now(timezone.utc)
        payload, manifest_hash = build_ai_snapshot_payload(
            plan,
            ai_run_id=ai_run_id,
            envelopes=envelopes,
            confirmed_at=confirmed_at,
        )

        decision_rows = []
        for envelope in envelopes:
            decision = envelope.decision
            evidence = {
                "decision_summary": decision["decision_summary"],
                "evidence": decision["evidence"],
                "uncertainty": decision["uncertainty"],
                "requires_human_review": decision["requires_human_review"],
                "exposure_identity_policy": plan.plan_payload.get(
                    "exposure_identity_policy", EXPOSURE_IDENTITY_POLICY
                ),
                "model_decision": envelope.raw_decision,
                "identity_adjustments": envelope.identity_adjustments,
                "screening_fingerprint": _screening_fingerprint(
                    plan.facts_by_code[decision["fund_code"]],
                    plan.source_batch["thresholds"],
                ),
            }
            decision_rows.append(
                (
                    ai_run_id,
                    decision["fund_code"],
                    decision["action"],
                    decision["include_in_candidate_pool"],
                    decision["confidence"],
                    Json(decision),
                    Json(evidence),
                    envelope.result.response_id,
                    envelope.result.actual_model,
                    envelope.result.system_fingerprint,
                    envelope.result.input_hash,
                    sha256_json(decision),
                    _decision_confirmation_status(decision),
                )
            )

        if decision_rows:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    INSERT INTO fund_pool_on.etf_candidate_ai_decision (
                        ai_run_id, fund_code, decision_action,
                        include_in_candidate_pool, confidence, decision_payload,
                        evidence_payload, response_id, actual_model,
                        system_fingerprint, input_hash, output_hash,
                        confirmation_status
                    ) VALUES %s
                    """,
                    decision_rows,
                    page_size=100,
                )

        load_result = load_candidate_master_snapshot(
            connection,
            payload,
            verify_source_file=False,
            commit=False,
        )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fund_pool_on.etf_candidate_ai_run
                SET status = 'SUCCEEDED',
                    decision_count = %s,
                    prompt_tokens = %s,
                    completion_tokens = %s,
                    total_tokens = %s,
                    output_snapshot_id = %s,
                    decision_manifest_hash = %s,
                    finished_at = clock_timestamp()
                WHERE ai_run_id = %s
                """,
                (
                    len(envelopes),
                    _sum_usage(batch_results, "prompt_tokens"),
                    _sum_usage(batch_results, "completion_tokens"),
                    _sum_usage(batch_results, "total_tokens"),
                    payload["snapshot_id"],
                    manifest_hash,
                    ai_run_id,
                ),
            )
        connection.commit()
        return {
            "status": "succeeded",
            "ai_run_id": ai_run_id,
            "plan_hash": plan.plan_hash,
            "run_month": run_month,
            "screening_scope": screening_scope,
            "llm_config": frozen_llm_config,
            "coverage_counts": plan.plan_payload.get("coverage_counts", {}),
            "decision_actions": {
                action: sum(e.decision["action"] == action for e in envelopes)
                for action in ("KEEP", "UPDATE", "ADD", "EXCLUDE_NEW")
            },
            "identity_adjusted_count": sum(
                bool(e.identity_adjustments) for e in envelopes
            ),
            "facts_as_of": plan.plan_payload["facts_as_of"],
            "llm_decision_count": len(envelopes),
            "ai_confirmed_count": sum(
                _decision_confirmation_status(envelope.decision)
                == CONFIRMATION_STATUS_AI
                for envelope in envelopes
                if envelope.decision["include_in_candidate_pool"]
            ),
            "ai_review_required_count": sum(
                _decision_confirmation_status(envelope.decision)
                == CONFIRMATION_STATUS_AI_REVIEW
                for envelope in envelopes
                if envelope.decision["include_in_candidate_pool"]
            ),
            "output_snapshot_id": payload["snapshot_id"],
            "decision_manifest_hash": manifest_hash,
            "load_result": load_result,
            "capital_authority": False,
            "order_authority": False,
        }
    except Exception as exc:
        connection.rollback()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fund_pool_on.etf_candidate_ai_run
                    SET status = 'FAILED',
                        error_message = %s,
                        finished_at = clock_timestamp()
                    WHERE ai_run_id = %s AND status = 'RUNNING'
                    """,
                    (str(exc)[:2000], ai_run_id),
                )
            connection.commit()
        except Exception:
            connection.rollback()
        raise
    finally:
        if lock_acquired:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s))",
                        (AUTOMATION_LOCK_NAME,),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
