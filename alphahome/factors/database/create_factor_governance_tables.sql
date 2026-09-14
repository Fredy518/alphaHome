CREATE SCHEMA IF NOT EXISTS factors;

CREATE TABLE IF NOT EXISTS public.task_status (
    id BIGSERIAL PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_task_status_name_time
    ON public.task_status (task_name, update_time DESC);

CREATE TABLE IF NOT EXISTS factors.factor_run (
    run_id UUID PRIMARY KEY,
    task_names TEXT[] NOT NULL,
    run_mode VARCHAR(32) NOT NULL,
    status VARCHAR(64) NOT NULL,
    requested_start_date DATE,
    requested_end_date DATE,
    effective_cutoff_date DATE NOT NULL,
    formula_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
    config_hash VARCHAR(64) NOT NULL,
    source_watermarks JSONB NOT NULL DEFAULT '{}'::jsonb,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_factor_run_started_at
    ON factors.factor_run (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_factor_run_task_names
    ON factors.factor_run USING GIN (task_names);

CREATE TABLE IF NOT EXISTS factors.factor_run_date (
    run_id UUID NOT NULL REFERENCES factors.factor_run(run_id) ON DELETE CASCADE,
    task_name VARCHAR(128) NOT NULL,
    calc_date DATE NOT NULL,
    status VARCHAR(64) NOT NULL,
    input_count BIGINT NOT NULL DEFAULT 0,
    output_count BIGINT NOT NULL DEFAULT 0,
    coverage_rate NUMERIC(12, 8),
    output_checksum VARCHAR(64),
    duration_ms BIGINT,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, task_name, calc_date)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_factor_run_date_current
    ON factors.factor_run_date (task_name, calc_date)
    WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_factor_run_date_lookup
    ON factors.factor_run_date (task_name, calc_date DESC, created_at DESC);

CREATE TABLE IF NOT EXISTS factors.factor_audit_snapshot (
    snapshot_id BIGSERIAL PRIMARY KEY,
    snapshot_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    task_name VARCHAR(128) NOT NULL,
    output_table VARCHAR(256) NOT NULL,
    expected_latest_date DATE,
    actual_latest_date DATE,
    first_calc_date DATE,
    row_count BIGINT NOT NULL DEFAULT 0,
    distinct_date_count BIGINT NOT NULL DEFAULT 0,
    latest_date_row_count BIGINT NOT NULL DEFAULT 0,
    coverage_rate NUMERIC(12, 8),
    missing_date_count BIGINT NOT NULL DEFAULT 0,
    nonstandard_date_count BIGINT NOT NULL DEFAULT 0,
    dependency_status VARCHAR(64),
    formula_version VARCHAR(64) NOT NULL,
    config_hash VARCHAR(64) NOT NULL,
    status VARCHAR(64) NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_factor_audit_task_time
    ON factors.factor_audit_snapshot (task_name, snapshot_time DESC);

CREATE TABLE IF NOT EXISTS factors.factor_repair_manifest (
    repair_id UUID PRIMARY KEY,
    status VARCHAR(64) NOT NULL,
    source_cutoff_at TIMESTAMPTZ NOT NULL,
    effective_cutoff_date DATE NOT NULL,
    apply_requested BOOLEAN NOT NULL DEFAULT FALSE,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS factors.factor_repair_date (
    repair_id UUID NOT NULL REFERENCES factors.factor_repair_manifest(repair_id),
    task_name VARCHAR(128) NOT NULL,
    calc_date DATE NOT NULL,
    action VARCHAR(64) NOT NULL,
    old_row_count BIGINT NOT NULL DEFAULT 0,
    old_checksum VARCHAR(64),
    new_row_count BIGINT,
    new_checksum VARCHAR(64),
    status VARCHAR(64) NOT NULL DEFAULT 'prepared',
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repair_id, task_name, calc_date)
);
