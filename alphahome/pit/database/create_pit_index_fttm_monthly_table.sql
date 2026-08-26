CREATE SCHEMA IF NOT EXISTS pit;

CREATE TABLE IF NOT EXISTS pit.pit_index_fttm_monthly (
    obs_date                    date         NOT NULL,
    universe_type               varchar(16)  NOT NULL,
    universe_code               varchar(20)  NOT NULL,
    universe_name               varchar(64)  NOT NULL,
    weight_basis                varchar(32)  NOT NULL,
    weight_source               varchar(64)  NOT NULL,
    weight_trade_date           date,
    weight_staleness_days       integer,
    structural_member_count     integer      NOT NULL DEFAULT 0,
    active_member_count         integer      NOT NULL DEFAULT 0,
    weight_available_count      integer      NOT NULL DEFAULT 0,
    covered_stock_count         integer      NOT NULL DEFAULT 0,
    org_count                   integer      NOT NULL DEFAULT 0,
    median_org_stock_count      numeric(20,6),
    median_org_weight_coverage  numeric(20,10),
    p25_org_weight_coverage     numeric(20,10),
    matched_org_count           integer      NOT NULL DEFAULT 0,
    up_org_count                integer      NOT NULL DEFAULT 0,
    down_or_flat_org_count      integer      NOT NULL DEFAULT 0,
    structural_weight           numeric(30,8),
    covered_weight              numeric(30,8),
    covered_stock_rate          numeric(20,10),
    covered_weight_rate         numeric(20,10),
    weight_data_coverage_rate   numeric(20,10),
    index_fttm_np               numeric(30,8),
    index_fttm_np_median        numeric(30,8),
    previous_index_fttm_np      numeric(30,8),
    fttm_np_mom_abs             numeric(30,8),
    fttm_np_mom_rate            numeric(20,10),
    diffusion_up                numeric(20,10),
    revision_comparable_stock_count integer      NOT NULL DEFAULT 0,
    revision_comparable_org_count   integer      NOT NULL DEFAULT 0,
    revision_median_org_count       numeric(20,6),
    revision_comparable_weight_rate numeric(20,10),
    revision_rate                   numeric(20,10),
    horizon_roll_rate               numeric(20,10),
    revision_activity_rate          numeric(20,10),
    revision_up_stock_rate          numeric(20,10),
    revision_up_weight_rate         numeric(20,10),
    revision_version                varchar(64)  NOT NULL DEFAULT 'common_stock_org_decomp_v1',
    is_revision_eligible            boolean      NOT NULL DEFAULT false,
    revision_quality_reasons        jsonb        NOT NULL DEFAULT '[]'::jsonb,
    is_eligible                 boolean      NOT NULL DEFAULT false,
    is_diffusion_eligible       boolean      NOT NULL DEFAULT false,
    quality_reasons             jsonb        NOT NULL DEFAULT '[]'::jsonb,
    stock_formula_version       varchar(64)  NOT NULL,
    aggregation_version         varchar(64)  NOT NULL,
    quality_rule_version        varchar(64)  NOT NULL,
    source_max_report_date      date,
    created_at                  timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pit_index_fttm_monthly_pk PRIMARY KEY (
        obs_date,
        universe_type,
        universe_code,
        weight_basis
    ),
    CONSTRAINT pit_index_fttm_monthly_universe_type_ck
        CHECK (universe_type IN ('index', 'all_a')),
    CONSTRAINT pit_index_fttm_monthly_diffusion_ck
        CHECK (diffusion_up IS NULL OR (diffusion_up >= 0 AND diffusion_up <= 1)),
    CONSTRAINT pit_index_fttm_monthly_source_date_ck
        CHECK (source_max_report_date IS NULL OR source_max_report_date <= obs_date),
    CONSTRAINT pit_index_fttm_monthly_weight_date_ck
        CHECK (weight_trade_date IS NULL OR weight_trade_date <= obs_date),
    CONSTRAINT pit_index_fttm_monthly_diffusion_count_ck
        CHECK (matched_org_count = up_org_count + down_or_flat_org_count),
    CONSTRAINT pit_index_fttm_monthly_revision_weight_ck
        CHECK (revision_comparable_weight_rate IS NULL OR (revision_comparable_weight_rate >= 0 AND revision_comparable_weight_rate <= 1)),
    CONSTRAINT pit_index_fttm_monthly_revision_activity_ck
        CHECK (revision_activity_rate IS NULL OR (revision_activity_rate >= 0 AND revision_activity_rate <= 1)),
    CONSTRAINT pit_index_fttm_monthly_revision_up_stock_ck
        CHECK (revision_up_stock_rate IS NULL OR (revision_up_stock_rate >= 0 AND revision_up_stock_rate <= 1)),
    CONSTRAINT pit_index_fttm_monthly_revision_up_weight_ck
        CHECK (revision_up_weight_rate IS NULL OR (revision_up_weight_rate >= 0 AND revision_up_weight_rate <= 1))
);

ALTER TABLE pit.pit_index_fttm_monthly
    ADD COLUMN IF NOT EXISTS revision_comparable_stock_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS revision_comparable_org_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS revision_median_org_count numeric(20,6),
    ADD COLUMN IF NOT EXISTS revision_comparable_weight_rate numeric(20,10),
    ADD COLUMN IF NOT EXISTS revision_rate numeric(20,10),
    ADD COLUMN IF NOT EXISTS horizon_roll_rate numeric(20,10),
    ADD COLUMN IF NOT EXISTS revision_activity_rate numeric(20,10),
    ADD COLUMN IF NOT EXISTS revision_up_stock_rate numeric(20,10),
    ADD COLUMN IF NOT EXISTS revision_up_weight_rate numeric(20,10),
    ADD COLUMN IF NOT EXISTS revision_version varchar(64) NOT NULL DEFAULT 'common_stock_org_decomp_v1',
    ADD COLUMN IF NOT EXISTS is_revision_eligible boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS revision_quality_reasons jsonb NOT NULL DEFAULT '[]'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'pit_index_fttm_monthly_revision_weight_ck'
          AND conrelid = 'pit.pit_index_fttm_monthly'::regclass
    ) THEN
        ALTER TABLE pit.pit_index_fttm_monthly
            ADD CONSTRAINT pit_index_fttm_monthly_revision_weight_ck
            CHECK (revision_comparable_weight_rate IS NULL OR (revision_comparable_weight_rate >= 0 AND revision_comparable_weight_rate <= 1));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'pit_index_fttm_monthly_revision_activity_ck'
          AND conrelid = 'pit.pit_index_fttm_monthly'::regclass
    ) THEN
        ALTER TABLE pit.pit_index_fttm_monthly
            ADD CONSTRAINT pit_index_fttm_monthly_revision_activity_ck
            CHECK (revision_activity_rate IS NULL OR (revision_activity_rate >= 0 AND revision_activity_rate <= 1));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'pit_index_fttm_monthly_revision_up_stock_ck'
          AND conrelid = 'pit.pit_index_fttm_monthly'::regclass
    ) THEN
        ALTER TABLE pit.pit_index_fttm_monthly
            ADD CONSTRAINT pit_index_fttm_monthly_revision_up_stock_ck
            CHECK (revision_up_stock_rate IS NULL OR (revision_up_stock_rate >= 0 AND revision_up_stock_rate <= 1));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'pit_index_fttm_monthly_revision_up_weight_ck'
          AND conrelid = 'pit.pit_index_fttm_monthly'::regclass
    ) THEN
        ALTER TABLE pit.pit_index_fttm_monthly
            ADD CONSTRAINT pit_index_fttm_monthly_revision_up_weight_ck
            CHECK (revision_up_weight_rate IS NULL OR (revision_up_weight_rate >= 0 AND revision_up_weight_rate <= 1));
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_pit_index_fttm_obs_date
    ON pit.pit_index_fttm_monthly (obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_index_fttm_universe_date
    ON pit.pit_index_fttm_monthly (universe_code, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_index_fttm_eligible_date
    ON pit.pit_index_fttm_monthly (is_eligible, obs_date);

COMMENT ON COLUMN pit.pit_index_fttm_monthly.fttm_np_mom_rate IS
    'Legacy adjacent-snapshot change; mixes revisions, horizon roll, and coverage composition.';
COMMENT ON COLUMN pit.pit_index_fttm_monthly.revision_rate IS
    'Matched stock-broker, same-target-year forecast revision component using current-month members and weights.';
COMMENT ON COLUMN pit.pit_index_fttm_monthly.horizon_roll_rate IS
    'Matched-sample FTTM change caused only by FY1/FY2 weight roll.';
