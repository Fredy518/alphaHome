CREATE SCHEMA IF NOT EXISTS pit;

CREATE TABLE IF NOT EXISTS pit.pit_industry_fttm_monthly (
    obs_date                    date         NOT NULL,
    classification_source      varchar(16)  NOT NULL,
    industry_level             varchar(8)   NOT NULL,
    industry_code              varchar(32)  NOT NULL,
    industry_name              varchar(128) NOT NULL,
    weight_basis               varchar(32)  NOT NULL,
    weight_trade_date          date,
    weight_staleness_days      integer,
    structural_member_count    integer      NOT NULL,
    active_member_count        integer      NOT NULL,
    weight_available_count     integer      NOT NULL,
    covered_stock_count        integer      NOT NULL,
    org_count                  integer      NOT NULL,
    median_org_stock_count     numeric,
    median_org_mv_coverage     numeric(12,8),
    p25_org_mv_coverage        numeric(12,8),
    matched_org_count          integer      NOT NULL,
    up_org_count               integer      NOT NULL,
    down_or_flat_org_count     integer      NOT NULL,
    structural_mv              numeric,
    covered_mv                 numeric,
    covered_stock_rate         numeric(12,8),
    covered_mv_rate            numeric(12,8),
    weight_data_coverage_rate  numeric(12,8),
    industry_fttm_np           numeric,
    industry_fttm_np_median    numeric,
    previous_industry_fttm_np  numeric,
    fttm_np_mom_abs            numeric,
    fttm_np_mom_rate           numeric,
    diffusion_up               numeric(12,8),
    is_eligible                boolean      NOT NULL,
    is_diffusion_eligible      boolean      NOT NULL,
    quality_reasons            jsonb        NOT NULL DEFAULT '[]'::jsonb,
    stock_formula_version      varchar(32)  NOT NULL,
    -- The immutable V1 value org_mv_weighted_then_equal_mean_v1 is 34 chars.
    aggregation_version        varchar(64)  NOT NULL,
    quality_rule_version       varchar(32)  NOT NULL,
    source_max_report_date     date,
    created_at                 timestamptz  NOT NULL DEFAULT now(),
    updated_at                 timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (
        obs_date,
        classification_source,
        industry_level,
        industry_code,
        weight_basis
    ),
    CHECK (industry_level IN ('L1', 'L2')),
    CHECK (covered_stock_rate BETWEEN 0 AND 1 OR covered_stock_rate IS NULL),
    CHECK (covered_mv_rate BETWEEN 0 AND 1 OR covered_mv_rate IS NULL),
    CHECK (weight_data_coverage_rate BETWEEN 0 AND 1 OR weight_data_coverage_rate IS NULL),
    CHECK (median_org_mv_coverage BETWEEN 0 AND 1 OR median_org_mv_coverage IS NULL),
    CHECK (p25_org_mv_coverage BETWEEN 0 AND 1 OR p25_org_mv_coverage IS NULL),
    CHECK (diffusion_up BETWEEN 0 AND 1 OR diffusion_up IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_pit_industry_fttm_level_date
    ON pit.pit_industry_fttm_monthly (industry_level, obs_date);

COMMENT ON TABLE pit.pit_industry_fttm_monthly IS
    'Monthly after-close PIT snapshots for SW L1/L2 FTTM consensus; total_mv is a research proxy weight.';
