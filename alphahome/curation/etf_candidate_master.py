"""ETF 候选母表的版本化入库。

手工候选身份与分类进入 ``fund_pool_on``；可重算产品事实来自
``features.mv_etf_product_facts_current``。两者在查询视图中对照，但不改写原始快照。
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from psycopg2.extras import Json, execute_values


CONTRACT_VERSION = "etf_candidate_master_snapshot_v1"
CANDIDATE_WRITE_LOCK_NAME = "alphahome_etf_candidate_master_write_v1"
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
STATUS_PERMISSION = {
    "正式候选": "候选池研究",
    "条件候选": "条件研究",
    "观察": "仅观察",
}

CONFIRMATION_STATUS_LEGACY = "LEGACY_IMPORTED"
CONFIRMATION_STATUS_AI = "AI_CONFIRMED"
CONFIRMATION_STATUS_AI_REVIEW = "AI_REVIEW_REQUIRED"
CONFIRMATION_STATUS_HUMAN = "HUMAN_CONFIRMED"
CONFIRMATION_STATUS_HUMAN_REJECTED = "HUMAN_REJECTED"
CONFIRMATION_STATUSES = {
    CONFIRMATION_STATUS_LEGACY,
    CONFIRMATION_STATUS_AI,
    CONFIRMATION_STATUS_AI_REVIEW,
    CONFIRMATION_STATUS_HUMAN,
    CONFIRMATION_STATUS_HUMAN_REJECTED,
}
MEMBERSHIP_COLUMNS = ["include_in_candidate_pool"]
CONFIRMATION_COLUMNS = [
    "confirmation_status",
    "confirmation_actor",
    "confirmation_at",
    "ai_run_id",
    "ai_model",
    "ai_confidence",
    "ai_decision_hash",
    "human_review_note",
]

SNAPSHOT_COLUMNS = [
    "snapshot_id",
    "source_row_number",
    "source_rank",
    "asset_class",
    "allocation_module",
    "allocation_role",
    "region_market",
    "level1_group",
    "level2_group",
    "exposure_name",
    "exposure_id",
    "fund_code",
    "fund_name",
    "tracking_index_code",
    "tracking_index_name",
    "product_role",
    "candidate_status",
    "exposure_relationship",
    "parent_fund_code",
    "budget_scope",
    "snapshot_aum_100m",
    "snapshot_amount_20d_100m",
    "snapshot_age_months",
    "snapshot_total_fee_pct",
    "snapshot_mean_abs_premium_60d",
    "snapshot_product_auxiliary_state",
    "snapshot_premium_observation_label",
    "product_facts_as_of",
    "source_supplement",
    "manual_review_status",
    "inclusion_reason",
    "risk_boundary",
    "update_frequency",
    "execution_check",
    "research_permission",
    "duplicate_check",
    "data_source_id",
    *CONFIRMATION_COLUMNS,
    *MEMBERSHIP_COLUMNS,
]

REQUIRED_RECORD_FIELDS = set(SNAPSHOT_COLUMNS) - {
    "snapshot_id",
    *CONFIRMATION_COLUMNS,
    *MEMBERSHIP_COLUMNS,
}


class CandidateMasterValidationError(ValueError):
    """候选母表交接合同不满足时抛出。"""


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS fund_pool_on;

CREATE TABLE IF NOT EXISTS fund_pool_on.etf_candidate_master_batch (
    snapshot_id text PRIMARY KEY,
    contract_version text NOT NULL,
    source_version text NOT NULL,
    source_file_name text NOT NULL,
    source_file_path text,
    source_file_sha256 text NOT NULL,
    workbook_generated_on date NOT NULL,
    product_facts_as_of date NOT NULL,
    structure_baseline_as_of date NOT NULL,
    exported_at timestamptz NOT NULL,
    research_stage text NOT NULL,
    authority_scope text NOT NULL,
    capital_authority boolean NOT NULL DEFAULT false,
    order_authority boolean NOT NULL DEFAULT false,
    thresholds jsonb NOT NULL,
    quality_summary jsonb NOT NULL,
    row_count integer NOT NULL,
    exposure_count integer NOT NULL,
    load_status text NOT NULL DEFAULT 'loading',
    loaded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT etf_candidate_master_no_capital CHECK (capital_authority = false),
    CONSTRAINT etf_candidate_master_no_orders CHECK (order_authority = false),
    CONSTRAINT etf_candidate_master_hash_format
        CHECK (source_file_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS fund_pool_on.etf_candidate_master_snapshot (
    snapshot_id text NOT NULL REFERENCES fund_pool_on.etf_candidate_master_batch(snapshot_id),
    source_row_number integer NOT NULL,
    source_rank integer NOT NULL,
    asset_class text NOT NULL,
    allocation_module text NOT NULL,
    allocation_role text,
    region_market text,
    level1_group text,
    level2_group text,
    exposure_name text NOT NULL,
    exposure_id text NOT NULL,
    fund_code text NOT NULL,
    fund_name text NOT NULL,
    tracking_index_code text,
    tracking_index_name text,
    product_role text NOT NULL,
    candidate_status text NOT NULL,
    exposure_relationship text,
    parent_fund_code text,
    budget_scope text,
    snapshot_aum_100m numeric,
    snapshot_amount_20d_100m numeric,
    snapshot_age_months numeric,
    snapshot_total_fee_pct numeric,
    snapshot_mean_abs_premium_60d numeric,
    snapshot_product_auxiliary_state text,
    snapshot_premium_observation_label text,
    product_facts_as_of date NOT NULL,
    source_supplement text,
    manual_review_status text,
    inclusion_reason text,
    risk_boundary text,
    update_frequency text,
    execution_check text,
    research_permission text NOT NULL,
    duplicate_check text NOT NULL,
    data_source_id text,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    confirmation_status text NOT NULL DEFAULT 'LEGACY_IMPORTED',
    confirmation_actor text,
    confirmation_at timestamptz,
    ai_run_id text,
    ai_model text,
    ai_confidence numeric,
    ai_decision_hash text,
    human_review_note text,
    include_in_candidate_pool boolean NOT NULL DEFAULT true,
    PRIMARY KEY (snapshot_id, fund_code),
    UNIQUE (snapshot_id, source_rank),
    CONSTRAINT etf_candidate_master_status_permission CHECK (
        (candidate_status = '正式候选' AND research_permission = '候选池研究') OR
        (candidate_status = '条件候选' AND research_permission = '条件研究') OR
        (candidate_status = '观察' AND research_permission = '仅观察')
    ),
    CONSTRAINT etf_candidate_master_confirmation_status CHECK (
        confirmation_status IN (
            'LEGACY_IMPORTED', 'AI_CONFIRMED', 'AI_REVIEW_REQUIRED',
            'HUMAN_CONFIRMED', 'HUMAN_REJECTED'
        )
    ),
    CONSTRAINT etf_candidate_master_membership_review CHECK (
        (confirmation_status = 'HUMAN_REJECTED' AND NOT include_in_candidate_pool)
        OR
        (confirmation_status <> 'HUMAN_REJECTED' AND include_in_candidate_pool)
    ),
    CONSTRAINT etf_candidate_master_ai_confidence CHECK (
        ai_confidence IS NULL OR (ai_confidence >= 0 AND ai_confidence <= 1)
    ),
    CONSTRAINT etf_candidate_master_duplicate_check CHECK (duplicate_check = 'OK')
);

CREATE INDEX IF NOT EXISTS idx_etf_candidate_master_snapshot_date
    ON fund_pool_on.etf_candidate_master_snapshot (product_facts_as_of, fund_code);
CREATE INDEX IF NOT EXISTS idx_etf_candidate_master_snapshot_exposure
    ON fund_pool_on.etf_candidate_master_snapshot (snapshot_id, exposure_id, source_rank);
CREATE INDEX IF NOT EXISTS idx_etf_candidate_master_snapshot_module
    ON fund_pool_on.etf_candidate_master_snapshot (snapshot_id, allocation_module, source_rank);

CREATE OR REPLACE VIEW fund_pool_on.etf_candidate_master_latest_batch AS
SELECT b.*
FROM fund_pool_on.etf_candidate_master_batch b
WHERE b.load_status = 'loaded'
ORDER BY b.workbook_generated_on DESC, b.loaded_at DESC, b.snapshot_id DESC
LIMIT 1;

CREATE OR REPLACE VIEW fund_pool_on.etf_candidate_master_current AS
SELECT
    s.snapshot_id,
    s.source_row_number,
    s.source_rank,
    s.asset_class,
    s.allocation_module,
    s.allocation_role,
    s.region_market,
    s.level1_group,
    s.level2_group,
    s.exposure_name,
    s.exposure_id,
    s.fund_code,
    s.fund_name,
    s.tracking_index_code,
    s.tracking_index_name,
    s.product_role,
    s.candidate_status,
    s.exposure_relationship,
    s.parent_fund_code,
    s.budget_scope,
    s.snapshot_aum_100m,
    s.snapshot_amount_20d_100m,
    s.snapshot_age_months,
    s.snapshot_total_fee_pct,
    s.snapshot_mean_abs_premium_60d,
    s.snapshot_product_auxiliary_state,
    s.snapshot_premium_observation_label,
    s.product_facts_as_of,
    s.source_supplement,
    s.manual_review_status,
    s.inclusion_reason,
    s.risk_boundary,
    s.update_frequency,
    s.execution_check,
    s.research_permission,
    s.duplicate_check,
    s.data_source_id,
    s.loaded_at,
    b.source_version,
    b.source_file_name,
    b.source_file_sha256,
    b.workbook_generated_on,
    b.structure_baseline_as_of,
    b.research_stage,
    b.authority_scope,
    b.capital_authority,
    b.order_authority,
    s.confirmation_status,
    s.confirmation_actor,
    s.confirmation_at,
    s.ai_run_id,
    s.ai_model,
    s.ai_confidence,
    s.ai_decision_hash,
    s.human_review_note,
    s.include_in_candidate_pool
FROM fund_pool_on.etf_candidate_master_snapshot s
JOIN fund_pool_on.etf_candidate_master_latest_batch b USING (snapshot_id)
WHERE s.include_in_candidate_pool;

CREATE OR REPLACE VIEW fund_pool_on.etf_candidate_exposure_current AS
SELECT
    snapshot_id,
    exposure_id,
    MIN(exposure_name) AS exposure_name,
    MIN(asset_class) AS asset_class,
    MIN(allocation_module) AS allocation_module,
    MIN(allocation_role) AS allocation_role,
    MIN(region_market) AS region_market,
    MIN(level1_group) AS level1_group,
    MIN(level2_group) AS level2_group,
    MIN(budget_scope) AS budget_scope,
    COUNT(*) AS product_count,
    COUNT(*) FILTER (WHERE candidate_status = '正式候选') AS formal_candidate_count,
    COUNT(*) FILTER (WHERE candidate_status = '条件候选') AS conditional_candidate_count,
    COUNT(*) FILTER (WHERE candidate_status = '观察') AS watch_count,
    (ARRAY_AGG(fund_code ORDER BY source_rank)
        FILTER (WHERE product_role = '主工具'))[1] AS primary_fund_code,
    ARRAY_AGG(fund_code ORDER BY source_rank) AS fund_codes,
    BOOL_AND(capital_authority = false AND order_authority = false) AS no_capital_no_orders
FROM fund_pool_on.etf_candidate_master_current
GROUP BY snapshot_id, exposure_id;

COMMENT ON TABLE fund_pool_on.etf_candidate_master_batch IS
    'ETF候选母表版本与质量审计；无资金权限、无下单权限';
COMMENT ON TABLE fund_pool_on.etf_candidate_master_snapshot IS
    'ETF候选身份、人工分类及入库时产品事实快照';
COMMENT ON VIEW fund_pool_on.etf_candidate_master_current IS
    '最新已成功载入且未被人工拒绝的ETF候选母表，不表示交易或资金资格';
"""


ENRICHED_VIEW_SQL = """
CREATE OR REPLACE VIEW fund_pool_on.etf_candidate_master_current_enriched AS
SELECT
    s.snapshot_id,
    s.source_row_number,
    s.source_rank,
    s.asset_class,
    s.allocation_module,
    s.allocation_role,
    s.region_market,
    s.level1_group,
    s.level2_group,
    s.exposure_name,
    s.exposure_id,
    s.fund_code,
    s.fund_name,
    s.tracking_index_code,
    s.tracking_index_name,
    s.product_role,
    s.candidate_status,
    s.exposure_relationship,
    s.parent_fund_code,
    s.budget_scope,
    s.snapshot_aum_100m,
    s.snapshot_amount_20d_100m,
    s.snapshot_age_months,
    s.snapshot_total_fee_pct,
    s.snapshot_mean_abs_premium_60d,
    s.snapshot_product_auxiliary_state,
    s.snapshot_premium_observation_label,
    s.product_facts_as_of,
    s.source_supplement,
    s.manual_review_status,
    s.inclusion_reason,
    s.risk_boundary,
    s.update_frequency,
    s.execution_check,
    s.research_permission,
    s.duplicate_check,
    s.data_source_id,
    s.loaded_at,
    s.source_version,
    s.source_file_name,
    s.source_file_sha256,
    s.workbook_generated_on,
    s.structure_baseline_as_of,
    s.research_stage,
    s.authority_scope,
    s.capital_authority,
    s.order_authority,
    f.as_of_date AS live_facts_as_of,
    f.price_date AS live_price_date,
    f.nav_date AS live_nav_date,
    f.share_date AS live_share_date,
    f.aum_100m AS live_aum_100m,
    f.aum_source AS live_aum_source,
    f.amount_20d_100m AS live_amount_20d_100m,
    f.amount_20d_days AS live_amount_20d_days,
    f.age_months AS live_age_months,
    f.total_fee_pct AS live_total_fee_pct,
    f.mean_abs_premium_60d AS live_mean_abs_premium_60d,
    f.premium_matched_days AS live_premium_matched_days,
    f.core_facts_complete AS live_core_facts_complete,
    CASE
        WHEN f.age_months < (b.thresholds ->> 'minimum_age_months')::numeric THEN '观察'
        WHEN f.aum_100m >= (b.thresholds ->> 'strong_aum_100m')::numeric
         AND f.amount_20d_100m >= (b.thresholds ->> 'strong_amount_20d_100m')::numeric
            THEN '强'
        WHEN f.aum_100m >= (b.thresholds ->> 'usable_aum_100m')::numeric
         AND f.amount_20d_100m >= (b.thresholds ->> 'usable_amount_20d_100m')::numeric
            THEN '可用'
        ELSE '观察'
    END AS live_product_auxiliary_state,
    CASE
        WHEN s.asset_class <> '跨境权益' THEN '不适用'
        WHEN f.mean_abs_premium_60d IS NULL THEN '缺失'
        WHEN f.mean_abs_premium_60d <= (b.thresholds ->> 'low_premium_upper')::numeric
            THEN '偏离较低'
        WHEN f.mean_abs_premium_60d <= (b.thresholds ->> 'medium_premium_upper')::numeric
            THEN '偏离中等'
        ELSE '偏离较高'
    END AS live_premium_observation_label,
    f.as_of_date > s.product_facts_as_of AS live_facts_advanced,
    CASE WHEN s.snapshot_aum_100m <> 0
        THEN f.aum_100m / s.snapshot_aum_100m - 1 END AS aum_relative_change,
    CASE WHEN s.snapshot_amount_20d_100m <> 0
        THEN f.amount_20d_100m / s.snapshot_amount_20d_100m - 1 END
        AS amount_20d_relative_change,
    f.total_fee_pct - s.snapshot_total_fee_pct AS total_fee_pct_change,
    b.thresholds AS maintenance_thresholds,
    s.confirmation_status,
    s.confirmation_actor,
    s.confirmation_at,
    s.ai_run_id,
    s.ai_model,
    s.ai_confidence,
    s.ai_decision_hash,
    s.human_review_note,
    s.include_in_candidate_pool
FROM fund_pool_on.etf_candidate_master_current s
JOIN fund_pool_on.etf_candidate_master_latest_batch b USING (snapshot_id)
LEFT JOIN features.mv_etf_product_facts_current f
  ON f.fund_code = s.fund_code;

COMMENT ON VIEW fund_pool_on.etf_candidate_master_current_enriched IS
    '候选母表快照与AlphaHome最新ETF产品事实对照；快照列不被覆盖';
"""


COVERAGE_VIEW_SQL = """
CREATE OR REPLACE VIEW fund_pool_on.etf_candidate_index_coverage_current AS
WITH candidate_index AS (
    SELECT
        tracking_index_code AS index_code,
        COUNT(*) AS product_count,
        COUNT(DISTINCT exposure_id) AS exposure_count,
        ARRAY_AGG(DISTINCT exposure_id ORDER BY exposure_id) AS exposure_ids
    FROM fund_pool_on.etf_candidate_master_current
    WHERE tracking_index_code IS NOT NULL
    GROUP BY tracking_index_code
),
technical AS (
    SELECT
        index_code,
        MAX(trade_date) AS technical_latest_date
    FROM features.mv_etf_exposure_technical_current_universe_daily
    GROUP BY index_code
),
valuation AS (
    SELECT
        index_code,
        MAX(trade_date) FILTER (WHERE valuation_available) AS valuation_latest_date,
        (ARRAY_AGG(valuation_route ORDER BY trade_date DESC)
            FILTER (WHERE valuation_available))[1] AS valuation_route
    FROM features.mv_index_direct_valuation_daily
    GROUP BY index_code
)
SELECT
    c.index_code,
    c.product_count,
    c.exposure_count,
    c.exposure_ids,
    t.technical_latest_date,
    v.valuation_latest_date,
    v.valuation_route,
    (t.technical_latest_date IS NOT NULL) AS technical_available,
    (v.valuation_latest_date IS NOT NULL) AS direct_valuation_available,
    CASE
        WHEN v.valuation_latest_date IS NOT NULL THEN 'AVAILABLE_DIRECT'
        ELSE 'MISSING_DIRECT_ROUTE'
    END AS direct_valuation_status,
    'current_candidate_universe_not_survivorship_free'::text AS universe_boundary
FROM candidate_index c
LEFT JOIN technical t USING (index_code)
LEFT JOIN valuation v USING (index_code);

COMMENT ON VIEW fund_pool_on.etf_candidate_index_coverage_current IS
    '当前ETF候选跟踪指数的技术与直接估值覆盖；缺失不使用重构值静默填充';
"""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_payload(
    payload: dict[str, Any], *, verify_source_file: bool = True
) -> None:
    """验证标准化交接文件，不修正或推断缺失字段。"""

    if payload.get("contract_version") != CONTRACT_VERSION:
        raise CandidateMasterValidationError("unsupported contract_version")

    snapshot_id = payload.get("snapshot_id")
    source = payload.get("source") or {}
    governance = payload.get("governance") or {}
    quality = payload.get("quality") or {}
    thresholds = payload.get("thresholds") or {}
    records = payload.get("records")

    if not isinstance(snapshot_id, str) or not snapshot_id.startswith(
        "etf_candidate_master_"
    ):
        raise CandidateMasterValidationError("invalid snapshot_id")
    source_hash = source.get("source_file_sha256")
    if not isinstance(source_hash, str) or not HASH_RE.fullmatch(source_hash):
        raise CandidateMasterValidationError("invalid source_file_sha256")
    if governance.get("capital_authority") is not False:
        raise CandidateMasterValidationError("capital_authority must be false")
    if governance.get("order_authority") is not False:
        raise CandidateMasterValidationError("order_authority must be false")
    if governance.get("authority_scope") != "CANDIDATE_POOL_ONLY":
        raise CandidateMasterValidationError(
            "authority_scope must be CANDIDATE_POOL_ONLY"
        )

    required_thresholds = {
        "strong_aum_100m",
        "strong_amount_20d_100m",
        "minimum_age_months",
        "usable_aum_100m",
        "usable_amount_20d_100m",
        "low_premium_upper",
        "medium_premium_upper",
        "premium_is_candidate_gate",
    }
    if required_thresholds - set(thresholds):
        raise CandidateMasterValidationError("missing maintenance thresholds")
    if thresholds.get("premium_is_candidate_gate") is not False:
        raise CandidateMasterValidationError("premium must not be a candidate gate")

    if not isinstance(records, list) or not records:
        raise CandidateMasterValidationError("records must be a non-empty list")
    if quality.get("all_checks_ok") is not True:
        raise CandidateMasterValidationError("workbook checks are not all OK")
    if quality.get("row_count") != len(records):
        raise CandidateMasterValidationError("quality.row_count does not match records")

    fund_codes: set[str] = set()
    exposure_ids: set[str] = set()
    expected_date = source.get("product_facts_as_of")
    for index, record in enumerate(records, start=1):
        missing = REQUIRED_RECORD_FIELDS - set(record)
        if missing:
            raise CandidateMasterValidationError(
                f"record {index} missing fields: {sorted(missing)}"
            )
        fund_code = record.get("fund_code")
        if not fund_code or fund_code in fund_codes:
            raise CandidateMasterValidationError(
                f"duplicate or blank fund_code: {fund_code}"
            )
        fund_codes.add(fund_code)
        exposure_ids.add(record.get("exposure_id"))
        status = record.get("candidate_status")
        if STATUS_PERMISSION.get(status) != record.get("research_permission"):
            raise CandidateMasterValidationError(
                f"status/permission mismatch for {fund_code}"
            )
        if record.get("duplicate_check") != "OK":
            raise CandidateMasterValidationError(
                f"duplicate_check failed for {fund_code}"
            )
        if record.get("product_facts_as_of") != expected_date:
            raise CandidateMasterValidationError(
                f"product_facts_as_of mismatch for {fund_code}"
            )
        confirmation_status = (
            record.get("confirmation_status") or CONFIRMATION_STATUS_LEGACY
        )
        include_in_candidate_pool = record.get("include_in_candidate_pool", True)
        if not isinstance(include_in_candidate_pool, bool):
            raise CandidateMasterValidationError(
                f"include_in_candidate_pool must be boolean for {fund_code}"
            )
        if confirmation_status not in CONFIRMATION_STATUSES:
            raise CandidateMasterValidationError(
                f"invalid confirmation_status for {fund_code}: {confirmation_status}"
            )
        if confirmation_status in {
            CONFIRMATION_STATUS_AI,
            CONFIRMATION_STATUS_AI_REVIEW,
        }:
            required_ai_fields = (
                "confirmation_actor",
                "confirmation_at",
                "ai_run_id",
                "ai_model",
                "ai_decision_hash",
            )
            for field in required_ai_fields:
                if not record.get(field):
                    raise CandidateMasterValidationError(
                        f"AI confirmation requires {field} for {fund_code}"
                    )
            confidence = record.get("ai_confidence")
            try:
                confidence_number = float(confidence)
            except (TypeError, ValueError) as exc:
                raise CandidateMasterValidationError(
                    f"AI confirmation requires numeric ai_confidence for {fund_code}"
                ) from exc
            if not 0.0 <= confidence_number <= 1.0:
                raise CandidateMasterValidationError(
                    f"ai_confidence out of range for {fund_code}"
                )
            if not HASH_RE.fullmatch(str(record.get("ai_decision_hash"))):
                raise CandidateMasterValidationError(
                    f"invalid ai_decision_hash for {fund_code}"
                )
        elif confirmation_status in {
            CONFIRMATION_STATUS_HUMAN,
            CONFIRMATION_STATUS_HUMAN_REJECTED,
        }:
            if not record.get("confirmation_actor"):
                raise CandidateMasterValidationError(
                    f"human review requires confirmation_actor for {fund_code}"
                )
            if not record.get("confirmation_at"):
                raise CandidateMasterValidationError(
                    f"human review requires confirmation_at for {fund_code}"
                )
        if confirmation_status == CONFIRMATION_STATUS_HUMAN_REJECTED:
            if include_in_candidate_pool:
                raise CandidateMasterValidationError(
                    f"HUMAN_REJECTED must leave the candidate pool for {fund_code}"
                )
            if not str(record.get("human_review_note") or "").strip():
                raise CandidateMasterValidationError(
                    f"HUMAN_REJECTED requires human_review_note for {fund_code}"
                )
        elif not include_in_candidate_pool:
            raise CandidateMasterValidationError(
                f"inactive candidate must be HUMAN_REJECTED for {fund_code}"
            )

    if quality.get("exposure_count") != len(exposure_ids):
        raise CandidateMasterValidationError(
            "quality.exposure_count does not match records"
        )

    if verify_source_file:
        source_path_text = source.get("source_file_path")
        if source_path_text:
            source_path = Path(source_path_text)
            if source_path.exists() and _sha256(source_path) != source_hash:
                raise CandidateMasterValidationError("source workbook SHA-256 mismatch")


def read_and_validate_payload(
    input_path: str | Path, *, verify_source_file: bool = True
) -> dict[str, Any]:
    """读取并验证 JSON 交接文件。"""

    path = Path(input_path)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    validate_payload(payload, verify_source_file=verify_source_file)
    return payload


def lock_candidate_master(connection: Any) -> None:
    """串行化候选发布与人工复核，锁随调用方事务提交/回滚释放。"""

    if getattr(connection, "autocommit", False):
        raise CandidateMasterValidationError(
            "candidate writes require autocommit=False"
        )
    with connection.cursor() as cursor:
        cursor.execute("SHOW transaction_isolation")
        if cursor.fetchone()[0] not in {"read committed", "read uncommitted"}:
            raise CandidateMasterValidationError(
                "candidate writes require READ COMMITTED for post-lock revalidation"
            )
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (CANDIDATE_WRITE_LOCK_NAME,),
        )


def ensure_candidate_master_schema(connection: Any) -> None:
    """创建专用历史表和查询视图，不改写旧基金池 latest snapshot。"""

    with connection.cursor() as cursor:
        cursor.execute(SCHEMA_SQL)
        cursor.execute("SELECT to_regclass('features.mv_etf_product_facts_current')")
        if cursor.fetchone()[0] is None:
            raise RuntimeError(
                "features.mv_etf_product_facts_current is required before candidate import"
            )
        cursor.execute(ENRICHED_VIEW_SQL)
        cursor.execute(
            """
            SELECT
                to_regclass(
                    'features.mv_etf_exposure_technical_current_universe_daily'
                ),
                to_regclass('features.mv_index_direct_valuation_daily')
            """
        )
        technical_relation, valuation_relation = cursor.fetchone()
        if technical_relation is not None and valuation_relation is not None:
            cursor.execute(COVERAGE_VIEW_SQL)


def ensure_candidate_data_coverage_view(connection: Any) -> bool:
    """在两个上游 MV 都存在时创建候选指数覆盖率视图。"""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                to_regclass(
                    'features.mv_etf_exposure_technical_current_universe_daily'
                ),
                to_regclass('features.mv_index_direct_valuation_daily')
            """
        )
        technical_relation, valuation_relation = cursor.fetchone()
        if technical_relation is None or valuation_relation is None:
            return False
        cursor.execute(COVERAGE_VIEW_SQL)
    return True


def _batch_values(payload: dict[str, Any]) -> tuple[Any, ...]:
    source = payload["source"]
    governance = payload["governance"]
    quality = payload["quality"]
    quality_summary = {key: value for key, value in quality.items() if key != "checks"}
    quality_summary["checks"] = quality.get("checks", [])
    return (
        payload["snapshot_id"],
        payload["contract_version"],
        source["source_version"],
        source["source_file_name"],
        source.get("source_file_path"),
        source["source_file_sha256"],
        source["workbook_generated_on"],
        source["product_facts_as_of"],
        source["structure_baseline_as_of"],
        source["exported_at"],
        governance["research_stage"],
        governance["authority_scope"],
        governance["capital_authority"],
        governance["order_authority"],
        Json(payload["thresholds"]),
        Json(quality_summary),
        quality["row_count"],
        quality["exposure_count"],
    )


def _normalized_record(record: dict[str, Any]) -> dict[str, Any]:
    """补齐确认元数据；旧版工作簿快照保持向后兼容。"""

    normalized = dict(record)
    normalized.setdefault("confirmation_status", CONFIRMATION_STATUS_LEGACY)
    normalized.setdefault("confirmation_actor", "workbook_import")
    normalized.setdefault("confirmation_at", None)
    normalized.setdefault("ai_run_id", None)
    normalized.setdefault("ai_model", None)
    normalized.setdefault("ai_confidence", None)
    normalized.setdefault("ai_decision_hash", None)
    normalized.setdefault("human_review_note", None)
    normalized.setdefault("include_in_candidate_pool", True)
    return normalized


def load_candidate_master_snapshot(
    connection: Any,
    payload: dict[str, Any],
    *,
    verify_source_file: bool = True,
    commit: bool = True,
) -> dict[str, Any]:
    """在单一事务中幂等载入一个候选母表快照。"""

    validate_payload(payload, verify_source_file=verify_source_file)
    snapshot_id = payload["snapshot_id"]
    source_hash = payload["source"]["source_file_sha256"]
    records = payload["records"]

    try:
        lock_candidate_master(connection)
        ensure_candidate_master_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_file_sha256, load_status, loaded_at
                FROM fund_pool_on.etf_candidate_master_batch
                WHERE snapshot_id = %s
                """,
                (snapshot_id,),
            )
            existing = cursor.fetchone()
            if existing and existing[0] != source_hash:
                raise CandidateMasterValidationError(
                    "snapshot_id already exists with a different source hash"
                )
            if existing and existing[1] == "loaded":
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS row_count,
                        COUNT(DISTINCT exposure_id) AS exposure_count,
                        COUNT(*) FILTER (
                            WHERE candidate_status = '正式候选'
                        ) AS formal_count,
                        COUNT(*) FILTER (
                            WHERE candidate_status = '条件候选'
                        ) AS conditional_count,
                        COUNT(*) FILTER (
                            WHERE candidate_status = '观察'
                        ) AS watch_count
                    FROM fund_pool_on.etf_candidate_master_snapshot
                    WHERE snapshot_id = %s
                    """,
                    (snapshot_id,),
                )
                counts = cursor.fetchone()
                if commit:
                    connection.commit()
                return {
                    "snapshot_id": snapshot_id,
                    "source_file_sha256": source_hash,
                    "row_count": counts[0],
                    "exposure_count": counts[1],
                    "formal_candidate_count": counts[2],
                    "conditional_candidate_count": counts[3],
                    "watch_count": counts[4],
                    "load_status": "loaded",
                    "loaded_at": existing[2].isoformat(),
                    "idempotent_noop": True,
                }

            cursor.execute(
                """
                INSERT INTO fund_pool_on.etf_candidate_master_batch (
                    snapshot_id, contract_version, source_version, source_file_name,
                    source_file_path, source_file_sha256, workbook_generated_on,
                    product_facts_as_of, structure_baseline_as_of, exported_at,
                    research_stage, authority_scope, capital_authority, order_authority,
                    thresholds, quality_summary, row_count, exposure_count, load_status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, 'loading'
                )
                ON CONFLICT (snapshot_id) DO UPDATE SET
                    source_file_path = EXCLUDED.source_file_path,
                    exported_at = EXCLUDED.exported_at,
                    thresholds = EXCLUDED.thresholds,
                    quality_summary = EXCLUDED.quality_summary,
                    row_count = EXCLUDED.row_count,
                    exposure_count = EXCLUDED.exposure_count,
                    load_status = 'loading',
                    loaded_at = clock_timestamp()
                """,
                _batch_values(payload),
            )
            cursor.execute(
                "DELETE FROM fund_pool_on.etf_candidate_master_snapshot "
                "WHERE snapshot_id = %s",
                (snapshot_id,),
            )

            rows = []
            for record in records:
                enriched = {
                    "snapshot_id": snapshot_id,
                    **_normalized_record(record),
                }
                rows.append(tuple(enriched[column] for column in SNAPSHOT_COLUMNS))
            execute_values(
                cursor,
                "INSERT INTO fund_pool_on.etf_candidate_master_snapshot ("
                + ", ".join(SNAPSHOT_COLUMNS)
                + ") VALUES %s",
                rows,
                page_size=500,
            )

            cursor.execute(
                """
                UPDATE fund_pool_on.etf_candidate_master_batch
                SET load_status = 'loaded', loaded_at = clock_timestamp()
                WHERE snapshot_id = %s
                """,
                (snapshot_id,),
            )
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT exposure_id) AS exposure_count,
                    COUNT(*) FILTER (WHERE candidate_status = '正式候选') AS formal_count,
                    COUNT(*) FILTER (WHERE candidate_status = '条件候选') AS conditional_count,
                    COUNT(*) FILTER (WHERE candidate_status = '观察') AS watch_count
                FROM fund_pool_on.etf_candidate_master_snapshot
                WHERE snapshot_id = %s
                """,
                (snapshot_id,),
            )
            counts = cursor.fetchone()
        if commit:
            connection.commit()
    except Exception:
        connection.rollback()
        raise

    return {
        "snapshot_id": snapshot_id,
        "source_file_sha256": source_hash,
        "row_count": counts[0],
        "exposure_count": counts[1],
        "formal_candidate_count": counts[2],
        "conditional_candidate_count": counts[3],
        "watch_count": counts[4],
        "load_status": "loaded",
        "idempotent_noop": False,
    }
