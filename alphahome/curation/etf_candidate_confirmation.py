"""ETF 候选母表的 AI/人工确认元数据与审计。"""

from __future__ import annotations

import hashlib
from typing import Any

from psycopg2.extras import Json

from alphahome.curation.etf_candidate_master import (
    CONFIRMATION_COLUMNS,
    CONFIRMATION_STATUS_HUMAN,
    CONFIRMATION_STATUS_HUMAN_REJECTED,
    COVERAGE_VIEW_SQL,
    ENRICHED_VIEW_SQL,
    MEMBERSHIP_COLUMNS,
    SCHEMA_SQL,
    ensure_candidate_master_schema,
    lock_candidate_master,
)


MIGRATION_ID = "20260916_etf_candidate_pit_v4"

MIGRATION_SQL = """
CREATE TABLE IF NOT EXISTS fund_pool_on.etf_candidate_ai_run (
    ai_run_id text PRIMARY KEY,
    run_month date NOT NULL,
    facts_as_of date NOT NULL,
    source_snapshot_id text NOT NULL,
    plan_hash text NOT NULL,
    plan_payload jsonb NOT NULL,
    model_requested text NOT NULL,
    prompt_version text NOT NULL,
    status text NOT NULL,
    candidate_count integer NOT NULL,
    new_product_count integer NOT NULL,
    decision_count integer NOT NULL DEFAULT 0,
    prompt_tokens bigint,
    completion_tokens bigint,
    total_tokens bigint,
    output_snapshot_id text,
    decision_manifest_hash text,
    error_message text,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    CONSTRAINT etf_candidate_ai_run_plan_hash CHECK (
        plan_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT etf_candidate_ai_run_status CHECK (
        status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'NO_CHANGES')
    )
);

CREATE TABLE IF NOT EXISTS fund_pool_on.etf_candidate_ai_decision (
    ai_run_id text NOT NULL
        REFERENCES fund_pool_on.etf_candidate_ai_run(ai_run_id),
    fund_code text NOT NULL,
    decision_action text NOT NULL,
    include_in_candidate_pool boolean NOT NULL,
    confidence numeric NOT NULL,
    decision_payload jsonb NOT NULL,
    evidence_payload jsonb NOT NULL,
    response_id text,
    actual_model text NOT NULL,
    system_fingerprint text,
    input_hash text NOT NULL,
    output_hash text NOT NULL,
    confirmation_status text NOT NULL DEFAULT 'AI_CONFIRMED',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ai_run_id, fund_code),
    CONSTRAINT etf_candidate_ai_decision_action CHECK (
        decision_action IN ('KEEP', 'UPDATE', 'ADD', 'EXCLUDE_NEW')
    ),
    CONSTRAINT etf_candidate_ai_decision_confidence CHECK (
        confidence >= 0 AND confidence <= 1
    ),
    CONSTRAINT etf_candidate_ai_decision_input_hash CHECK (
        input_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT etf_candidate_ai_decision_output_hash CHECK (
        output_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT etf_candidate_ai_decision_confirmation CHECK (
        confirmation_status IN ('AI_CONFIRMED', 'AI_REVIEW_REQUIRED')
    )
);

ALTER TABLE fund_pool_on.etf_candidate_ai_decision
    DROP CONSTRAINT IF EXISTS etf_candidate_ai_decision_confirmation;
ALTER TABLE fund_pool_on.etf_candidate_ai_decision
    ADD CONSTRAINT etf_candidate_ai_decision_confirmation CHECK (
        confirmation_status IN ('AI_CONFIRMED', 'AI_REVIEW_REQUIRED')
    );

ALTER TABLE fund_pool_on.etf_candidate_master_snapshot
    ADD COLUMN IF NOT EXISTS confirmation_status text
        NOT NULL DEFAULT 'LEGACY_IMPORTED',
    ADD COLUMN IF NOT EXISTS confirmation_actor text,
    ADD COLUMN IF NOT EXISTS confirmation_at timestamptz,
    ADD COLUMN IF NOT EXISTS ai_run_id text,
    ADD COLUMN IF NOT EXISTS ai_model text,
    ADD COLUMN IF NOT EXISTS ai_confidence numeric,
    ADD COLUMN IF NOT EXISTS ai_decision_hash text,
    ADD COLUMN IF NOT EXISTS human_review_note text,
    ADD COLUMN IF NOT EXISTS include_in_candidate_pool boolean
        NOT NULL DEFAULT true;

ALTER TABLE fund_pool_on.etf_candidate_master_snapshot
    DROP CONSTRAINT IF EXISTS etf_candidate_master_confirmation_status;
ALTER TABLE fund_pool_on.etf_candidate_master_snapshot
    ADD CONSTRAINT etf_candidate_master_confirmation_status CHECK (
        confirmation_status IN (
            'LEGACY_IMPORTED', 'AI_CONFIRMED', 'AI_REVIEW_REQUIRED',
            'HUMAN_CONFIRMED', 'HUMAN_REJECTED'
        )
    );

ALTER TABLE fund_pool_on.etf_candidate_master_snapshot
    DROP CONSTRAINT IF EXISTS etf_candidate_master_membership_review;
ALTER TABLE fund_pool_on.etf_candidate_master_snapshot
    ADD CONSTRAINT etf_candidate_master_membership_review CHECK (
        (confirmation_status = 'HUMAN_REJECTED' AND NOT include_in_candidate_pool)
        OR
        (confirmation_status <> 'HUMAN_REJECTED' AND include_in_candidate_pool)
    );

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'etf_candidate_master_ai_confidence'
          AND conrelid =
              'fund_pool_on.etf_candidate_master_snapshot'::regclass
    ) THEN
        ALTER TABLE fund_pool_on.etf_candidate_master_snapshot
            ADD CONSTRAINT etf_candidate_master_ai_confidence CHECK (
                ai_confidence IS NULL OR
                (ai_confidence >= 0 AND ai_confidence <= 1)
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'etf_candidate_master_ai_run_fk'
          AND conrelid =
              'fund_pool_on.etf_candidate_master_snapshot'::regclass
    ) THEN
        ALTER TABLE fund_pool_on.etf_candidate_master_snapshot
            ADD CONSTRAINT etf_candidate_master_ai_run_fk
            FOREIGN KEY (ai_run_id)
            REFERENCES fund_pool_on.etf_candidate_ai_run(ai_run_id);
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS fund_pool_on.etf_candidate_confirmation_audit (
    audit_id bigserial PRIMARY KEY,
    snapshot_id text NOT NULL,
    fund_code text NOT NULL,
    old_confirmation_status text NOT NULL,
    new_confirmation_status text NOT NULL,
    changed_by text NOT NULL,
    review_note text,
    before_record jsonb NOT NULL,
    after_record jsonb NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (snapshot_id, fund_code)
        REFERENCES fund_pool_on.etf_candidate_master_snapshot(
            snapshot_id, fund_code
        ),
    CONSTRAINT etf_candidate_confirmation_audit_status CHECK (
        new_confirmation_status IN ('HUMAN_CONFIRMED', 'HUMAN_REJECTED')
    )
);

ALTER TABLE fund_pool_on.etf_candidate_confirmation_audit
    DROP CONSTRAINT IF EXISTS etf_candidate_confirmation_audit_status;
ALTER TABLE fund_pool_on.etf_candidate_confirmation_audit
    ADD CONSTRAINT etf_candidate_confirmation_audit_status CHECK (
        new_confirmation_status IN ('HUMAN_CONFIRMED', 'HUMAN_REJECTED')
    );

CREATE INDEX IF NOT EXISTS idx_etf_candidate_confirmation_status
    ON fund_pool_on.etf_candidate_master_snapshot (
        snapshot_id, confirmation_status, source_rank
    );
CREATE INDEX IF NOT EXISTS idx_etf_candidate_confirmation_audit_lookup
    ON fund_pool_on.etf_candidate_confirmation_audit (
        snapshot_id, fund_code, changed_at DESC
    );
CREATE UNIQUE INDEX IF NOT EXISTS uq_etf_candidate_ai_run_succeeded_month
    ON fund_pool_on.etf_candidate_ai_run (run_month)
    WHERE status = 'SUCCEEDED';

COMMENT ON TABLE fund_pool_on.etf_candidate_ai_run IS
    'ETF候选池月度AI确认运行；仅候选研究，无资金和下单权限';
COMMENT ON TABLE fund_pool_on.etf_candidate_ai_decision IS
    '逐ETF的LLM结构化判断、证据、置信度及响应哈希';
COMMENT ON TABLE fund_pool_on.etf_candidate_confirmation_audit IS
    '候选记录人工通过或拒绝的不可覆盖审计；拒绝行保留历史但退出当前池';
"""


PIT_SCHEMA_SQL = """
CREATE OR REPLACE VIEW fund_pool_on.etf_candidate_master_pit_history AS
WITH loaded_batches AS (
    SELECT b.*
    FROM fund_pool_on.etf_candidate_master_batch b
    WHERE b.load_status = 'loaded'
),
active_batches AS (
    -- A late import of an older workbook is auditable, but must not replace the
    -- batch that the production latest-batch ordering considered current.
    SELECT b.*
    FROM loaded_batches b
    WHERE NOT EXISTS (
        SELECT 1
        FROM loaded_batches newer
        WHERE newer.loaded_at <= b.loaded_at
          AND (
              newer.workbook_generated_on,
              newer.loaded_at,
              newer.snapshot_id
          ) > (
              b.workbook_generated_on,
              b.loaded_at,
              b.snapshot_id
          )
    )
),
batch_windows AS (
    SELECT
        b.*,
        b.loaded_at AS batch_available_from,
        LEAD(b.loaded_at) OVER (
            ORDER BY b.loaded_at, b.snapshot_id
        ) AS batch_available_to
    FROM active_batches b
),
first_audit AS (
    SELECT DISTINCT ON (a.snapshot_id, a.fund_code)
        a.snapshot_id,
        a.fund_code,
        a.before_record
    FROM fund_pool_on.etf_candidate_confirmation_audit a
    ORDER BY a.snapshot_id, a.fund_code, a.changed_at, a.audit_id
),
state_events AS (
    SELECT
        s.snapshot_id,
        s.fund_code,
        b.batch_available_from AS event_at,
        0::bigint AS event_order,
        'SNAPSHOT_LOADED'::text AS event_type,
        NULL::bigint AS state_audit_id,
        COALESCE(a.before_record, to_jsonb(s)) AS record_payload
    FROM fund_pool_on.etf_candidate_master_snapshot s
    JOIN batch_windows b USING (snapshot_id)
    LEFT JOIN first_audit a USING (snapshot_id, fund_code)

    UNION ALL

    SELECT
        a.snapshot_id,
        a.fund_code,
        a.changed_at AS event_at,
        a.audit_id AS event_order,
        'HUMAN_REVIEW'::text AS event_type,
        a.audit_id AS state_audit_id,
        a.after_record AS record_payload
    FROM fund_pool_on.etf_candidate_confirmation_audit a
    JOIN batch_windows b USING (snapshot_id)
    WHERE a.changed_at >= b.batch_available_from
),
sequenced AS (
    SELECT
        e.*,
        LEAD(e.event_at) OVER (
            PARTITION BY e.snapshot_id, e.fund_code
            ORDER BY e.event_at, e.event_order
        ) AS next_event_at
    FROM state_events e
)
SELECT
    r.*,
    b.product_facts_as_of AS business_as_of_date,
    b.workbook_generated_on,
    b.structure_baseline_as_of,
    b.source_version,
    b.source_file_sha256,
    b.research_stage,
    b.authority_scope,
    b.capital_authority,
    b.order_authority,
    b.batch_available_from AS batch_loaded_at,
    e.event_at AS available_from,
    CASE
        WHEN e.next_event_at IS NULL THEN b.batch_available_to
        WHEN b.batch_available_to IS NULL THEN e.next_event_at
        ELSE LEAST(e.next_event_at, b.batch_available_to)
    END AS available_to,
    e.event_type,
    e.state_audit_id
FROM sequenced e
JOIN batch_windows b USING (snapshot_id)
CROSS JOIN LATERAL jsonb_populate_record(
    NULL::fund_pool_on.etf_candidate_master_snapshot,
    e.record_payload
) AS r
WHERE b.batch_available_to IS NULL
   OR e.event_at < b.batch_available_to;

COMMENT ON VIEW fund_pool_on.etf_candidate_master_pit_history IS
    'ETF候选池PIT状态区间；business_as_of_date是事实截止日，available_from才是可用时间';

CREATE OR REPLACE FUNCTION fund_pool_on.etf_candidate_master_as_of(
    p_available_at timestamptz,
    p_include_rejected boolean DEFAULT false
)
RETURNS SETOF fund_pool_on.etf_candidate_master_pit_history
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $function$
    SELECT h.*
    FROM fund_pool_on.etf_candidate_master_pit_history h
    WHERE h.available_from <= p_available_at
      AND (h.available_to IS NULL OR p_available_at < h.available_to)
      AND (p_include_rejected OR h.include_in_candidate_pool)
    ORDER BY h.source_rank, h.fund_code
$function$;

COMMENT ON FUNCTION fund_pool_on.etf_candidate_master_as_of(
    timestamptz, boolean
) IS
    '按实际可用时间返回当时可见的ETF候选池；默认排除当时已人工拒绝的产品';
"""


def migration_plan_hash() -> str:
    """返回固定迁移内容的 SHA-256，供 plan/apply 防误执行。"""

    full_plan = "\n".join(
        (
            MIGRATION_SQL,
            SCHEMA_SQL,
            ENRICHED_VIEW_SQL,
            COVERAGE_VIEW_SQL,
            PIT_SCHEMA_SQL,
        )
    )
    return hashlib.sha256(full_plan.encode("utf-8")).hexdigest()


def apply_confirmation_migration(connection: Any) -> None:
    """显式执行确认层迁移，并重建依赖 ``s.*`` 的查询视图。"""

    try:
        lock_candidate_master(connection)
        with connection.cursor() as cursor:
            cursor.execute(MIGRATION_SQL)
        ensure_candidate_master_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(PIT_SCHEMA_SQL)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def missing_confirmation_schema(connection: Any) -> list[str]:
    """只读检查 AI 确认层所需表与列。"""

    missing: list[str] = []
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'fund_pool_on'
              AND table_name IN (
                  'etf_candidate_ai_run',
                  'etf_candidate_ai_decision',
                  'etf_candidate_confirmation_audit'
              )
            """
        )
        present_tables = {row[0] for row in cursor.fetchall()}
        for table_name in (
            "etf_candidate_ai_run",
            "etf_candidate_ai_decision",
            "etf_candidate_confirmation_audit",
        ):
            if table_name not in present_tables:
                missing.append(f"fund_pool_on.{table_name}")

        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'fund_pool_on'
              AND table_name = 'etf_candidate_master_snapshot'
            """
        )
        present_columns = {row[0] for row in cursor.fetchall()}
        for column_name in (*CONFIRMATION_COLUMNS, *MEMBERSHIP_COLUMNS):
            if column_name not in present_columns:
                missing.append(
                    "fund_pool_on.etf_candidate_master_snapshot." + column_name
                )

        cursor.execute(
            """
            SELECT c.conname, pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname = 'fund_pool_on'
              AND c.conname IN (
                  'etf_candidate_master_confirmation_status',
                  'etf_candidate_master_membership_review',
                  'etf_candidate_ai_decision_confirmation',
                  'etf_candidate_confirmation_audit_status'
              )
            """
        )
        confirmation_constraints = {row[0]: row[1] for row in cursor.fetchall()}
        expected_constraint_tokens = {
            "etf_candidate_master_confirmation_status": "HUMAN_REJECTED",
            "etf_candidate_master_membership_review": "include_in_candidate_pool",
            "etf_candidate_ai_decision_confirmation": "AI_REVIEW_REQUIRED",
            "etf_candidate_confirmation_audit_status": "HUMAN_REJECTED",
        }
        for constraint_name, token in expected_constraint_tokens.items():
            definition = confirmation_constraints.get(constraint_name, "")
            if token not in definition:
                missing.append("constraint:" + constraint_name)

        cursor.execute(
            """
            SELECT pg_get_viewdef(
                'fund_pool_on.etf_candidate_master_current'::regclass,
                true
            )
            """
        )
        current_view_definition = str(cursor.fetchone()[0])
        if "WHERE s.include_in_candidate_pool" not in current_view_definition:
            missing.append(
                "view:fund_pool_on.etf_candidate_master_current.rejection_filter"
            )

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.views
                WHERE table_schema = 'fund_pool_on'
                  AND table_name = 'etf_candidate_master_pit_history'
            )
            """
        )
        if not bool(cursor.fetchone()[0]):
            missing.append("view:fund_pool_on.etf_candidate_master_pit_history")

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'fund_pool_on'
                  AND p.proname = 'etf_candidate_master_as_of'
                  AND pg_get_function_identity_arguments(p.oid) =
                      'p_available_at timestamp with time zone, '
                      'p_include_rejected boolean'
            )
            """
        )
        if not bool(cursor.fetchone()[0]):
            missing.append("function:fund_pool_on.etf_candidate_master_as_of")
    return missing


def review_candidate_human(
    connection: Any,
    *,
    fund_code: str,
    reviewer: str,
    decision: str,
    review_note: str | None = None,
    snapshot_id: str | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    """人工通过或拒绝一个候选，保留原 AI 证据和前后镜像。"""

    normalized_code = fund_code.strip().upper()
    normalized_reviewer = reviewer.strip()
    normalized_decision = decision.strip().lower()
    if not normalized_code:
        raise ValueError("fund_code must not be blank")
    if not normalized_reviewer:
        raise ValueError("reviewer must not be blank")
    if normalized_decision not in {"approve", "reject"}:
        raise ValueError("decision must be approve or reject")
    if normalized_decision == "reject" and not str(review_note or "").strip():
        raise ValueError("reject decision requires review_note")
    approved = normalized_decision == "approve"
    new_status = (
        CONFIRMATION_STATUS_HUMAN if approved else CONFIRMATION_STATUS_HUMAN_REJECTED
    )
    manual_review_status = "人工确认" if approved else "人工复核不通过"

    try:
        lock_candidate_master(connection)
        missing = missing_confirmation_schema(connection)
        if missing:
            raise RuntimeError(
                "ETF candidate confirmation migration is required: "
                + ", ".join(missing)
            )
        with connection.cursor() as cursor:
            if snapshot_id is None:
                cursor.execute(
                    """
                    SELECT snapshot_id
                    FROM fund_pool_on.etf_candidate_master_latest_batch
                    """
                )
                row = cursor.fetchone()
                if row is None:
                    raise LookupError("no loaded ETF candidate snapshot")
                snapshot_id = str(row[0])

            cursor.execute(
                """
                SELECT to_jsonb(s)
                FROM fund_pool_on.etf_candidate_master_snapshot s
                WHERE s.snapshot_id = %s AND s.fund_code = %s
                FOR UPDATE
                """,
                (snapshot_id, normalized_code),
            )
            row = cursor.fetchone()
            if row is None:
                raise LookupError(
                    f"candidate not found: {snapshot_id}/{normalized_code}"
                )
            before_record = row[0]

            cursor.execute(
                """
                UPDATE fund_pool_on.etf_candidate_master_snapshot AS s
                SET confirmation_status = %s,
                    confirmation_actor = %s,
                    confirmation_at = clock_timestamp(),
                    human_review_note = %s,
                    manual_review_status = %s,
                    include_in_candidate_pool = %s
                WHERE snapshot_id = %s AND fund_code = %s
                RETURNING to_jsonb(s)
                """,
                (
                    new_status,
                    normalized_reviewer,
                    review_note,
                    manual_review_status,
                    approved,
                    snapshot_id,
                    normalized_code,
                ),
            )
            after_record = cursor.fetchone()[0]
            cursor.execute(
                """
                INSERT INTO fund_pool_on.etf_candidate_confirmation_audit (
                    snapshot_id,
                    fund_code,
                    old_confirmation_status,
                    new_confirmation_status,
                    changed_by,
                    review_note,
                    before_record,
                    after_record,
                    changed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING audit_id, changed_at
                """,
                (
                    snapshot_id,
                    normalized_code,
                    before_record["confirmation_status"],
                    new_status,
                    normalized_reviewer,
                    review_note,
                    Json(before_record),
                    Json(after_record),
                    after_record["confirmation_at"],
                ),
            )
            audit_id, changed_at = cursor.fetchone()
        if commit:
            connection.commit()
    except Exception:
        connection.rollback()
        raise

    return {
        "audit_id": audit_id,
        "snapshot_id": snapshot_id,
        "fund_code": normalized_code,
        "decision": normalized_decision,
        "include_in_candidate_pool": approved,
        "confirmation_status": new_status,
        "confirmation_actor": normalized_reviewer,
        "confirmation_at": changed_at.isoformat(),
        "review_note": review_note,
        "ai_run_id": after_record.get("ai_run_id"),
        "ai_decision_hash": after_record.get("ai_decision_hash"),
    }


def confirm_candidate_human(
    connection: Any,
    *,
    fund_code: str,
    reviewer: str,
    review_note: str | None = None,
    snapshot_id: str | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    """向后兼容的人工通过入口。"""

    return review_candidate_human(
        connection,
        fund_code=fund_code,
        reviewer=reviewer,
        decision="approve",
        review_note=review_note,
        snapshot_id=snapshot_id,
        commit=commit,
    )


def reject_candidate_human(
    connection: Any,
    *,
    fund_code: str,
    reviewer: str,
    review_note: str,
    snapshot_id: str | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    """人工拒绝候选；历史行保留，当前候选视图排除。"""

    return review_candidate_human(
        connection,
        fund_code=fund_code,
        reviewer=reviewer,
        decision="reject",
        review_note=review_note,
        snapshot_id=snapshot_id,
        commit=commit,
    )
