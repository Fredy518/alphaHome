"""PIT-owned recovery evidence. Never infer a baseline from GUI task history."""

from datetime import date


class PITBaselineRequired(RuntimeError):
    """A bounded refresh cannot certify an uninitialized history."""


CREATE_RUN_LEDGER_SQL = """
CREATE TABLE IF NOT EXISTS pit.task_run (
    run_id UUID PRIMARY KEY,
    batch_id UUID NOT NULL,
    task_name TEXT NOT NULL,
    plan_hash TEXT NOT NULL,
    mode TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    start_date DATE,
    end_date DATE,
    status TEXT NOT NULL CHECK (status IN ('running','success','error','cancelled')),
    baseline_ready BOOLEAN NOT NULL DEFAULT FALSE,
    result JSONB,
    CHECK (NOT baseline_ready OR status = 'success')
);
CREATE INDEX IF NOT EXISTS pit_task_run_success
    ON pit.task_run (task_name, finished_at DESC)
    WHERE status = 'success' AND baseline_ready;
COMMENT ON TABLE pit.task_run IS
    'Execution/recovery ledger, not proof of historical source availability. Interrupted runs never advance the baseline.';
"""


def read_incremental_baseline(context, task_name):
    try:
        frame = context.query_dataframe(
            """SELECT started_at AT TIME ZONE 'Asia/Shanghai' AS last_success_local,
                      end_date AS coverage_end
               FROM pit.task_run
               WHERE task_name = %s AND status = 'success' AND baseline_ready
               ORDER BY finished_at DESC, run_id DESC LIMIT 1""",
            (task_name,),
        )
    except Exception as exc:
        raise PITBaselineRequired(
            f"pit_recovery_unavailable: {task_name}; install the PIT ledger migration and establish a full baseline"
        ) from exc
    if frame is None or frame.empty:
        raise PITBaselineRequired(
            f"pit_baseline_required: {task_name}; run an explicit full_backfill from the declared history start"
        )
    row = frame.iloc[0]
    if row.get('last_success_local') is None or row.get('coverage_end') is None:
        raise PITBaselineRequired(f"pit_baseline_invalid: {task_name}")
    return row


def baseline_eligible(mode, start, default_start):
    """A manual slice must never initialize or advance a full-history baseline."""
    return mode == 'incremental' or (
        mode == 'full_backfill' and start is not None
        and start <= date.fromisoformat(str(default_start)[:10])
    )
