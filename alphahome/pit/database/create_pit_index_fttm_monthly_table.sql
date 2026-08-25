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
        CHECK (matched_org_count = up_org_count + down_or_flat_org_count)
);

CREATE INDEX IF NOT EXISTS idx_pit_index_fttm_obs_date
    ON pit.pit_index_fttm_monthly (obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_index_fttm_universe_date
    ON pit.pit_index_fttm_monthly (universe_code, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_index_fttm_eligible_date
    ON pit.pit_index_fttm_monthly (is_eligible, obs_date);
