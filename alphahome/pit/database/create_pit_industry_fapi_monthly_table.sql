CREATE SCHEMA IF NOT EXISTS pit;

CREATE TABLE IF NOT EXISTS pit.pit_industry_fapi_monthly (
    obs_date                              date          NOT NULL,
    classification_source                varchar(16)   NOT NULL,
    industry_level                       varchar(8)    NOT NULL,
    industry_code                        varchar(32)   NOT NULL,
    industry_name                        varchar(128)  NOT NULL,
    benchmark_code                       varchar(20)   NOT NULL,
    benchmark_name                       varchar(64)   NOT NULL,
    equity_basis                         varchar(32)   NOT NULL,
    equity_trade_date                    date,
    equity_staleness_days                integer,
    benchmark_weight_trade_date          date,
    benchmark_weight_staleness_days      integer,
    structural_member_count              integer       NOT NULL DEFAULT 0,
    equity_available_member_count        integer       NOT NULL DEFAULT 0,
    matched_stock_count                  integer       NOT NULL DEFAULT 0,
    matched_org_count                    integer       NOT NULL DEFAULT 0,
    ratio_matched_org_count              integer       NOT NULL DEFAULT 0,
    median_common_stock_count            numeric(20,6),
    p25_common_stock_count               numeric(20,6),
    average_report_age_days              numeric(20,6),
    benchmark_structural_member_count    integer       NOT NULL DEFAULT 0,
    benchmark_matched_stock_count        integer       NOT NULL DEFAULT 0,
    benchmark_eligible_org_count         integer       NOT NULL DEFAULT 0,
    benchmark_median_common_stock_count  numeric(20,6),
    spread_up_org_count                  integer       NOT NULL DEFAULT 0,
    spread_down_or_flat_org_count        integer       NOT NULL DEFAULT 0,
    ratio_up_org_count                   integer       NOT NULL DEFAULT 0,
    ratio_down_or_flat_org_count         integer       NOT NULL DEFAULT 0,
    spread_up_org_weight                 numeric(30,12) NOT NULL DEFAULT 0,
    spread_total_org_weight              numeric(30,12) NOT NULL DEFAULT 0,
    ratio_up_org_weight                  numeric(30,12) NOT NULL DEFAULT 0,
    ratio_total_org_weight               numeric(30,12) NOT NULL DEFAULT 0,
    fapi_spread_equal                    numeric(20,10),
    fapi_spread_weighted                 numeric(20,10),
    fapi_ratio_equal                     numeric(20,10),
    fapi_ratio_weighted                  numeric(20,10),
    expected_roe_equal                   numeric(30,12),
    expected_roe_weighted                numeric(30,12),
    previous_expected_roe_equal          numeric(30,12),
    previous_expected_roe_weighted       numeric(30,12),
    benchmark_expected_roe_equal         numeric(30,12),
    benchmark_expected_roe_weighted      numeric(30,12),
    previous_benchmark_expected_roe_equal    numeric(30,12),
    previous_benchmark_expected_roe_weighted numeric(30,12),
    source_max_report_date               date,
    previous_source_max_report_date      date,
    stock_formula_versions               jsonb         NOT NULL DEFAULT '[]'::jsonb,
    is_eligible                          boolean       NOT NULL DEFAULT false,
    is_ratio_eligible                    boolean       NOT NULL DEFAULT false,
    quality_reasons                      jsonb         NOT NULL DEFAULT '[]'::jsonb,
    method_version                       varchar(64)   NOT NULL,
    org_weight_version                   varchar(64)   NOT NULL,
    quality_rule_version                 varchar(64)   NOT NULL,
    created_at                           timestamptz   NOT NULL DEFAULT now(),
    updated_at                           timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pit_industry_fapi_monthly_pk PRIMARY KEY (
        obs_date,
        classification_source,
        industry_level,
        industry_code,
        benchmark_code,
        method_version
    ),
    CONSTRAINT pit_industry_fapi_monthly_level_ck
        CHECK (industry_level IN ('L1', 'L2')),
    CONSTRAINT pit_industry_fapi_monthly_equity_date_ck
        CHECK (equity_trade_date IS NULL OR equity_trade_date <= obs_date),
    CONSTRAINT pit_industry_fapi_monthly_benchmark_date_ck
        CHECK (benchmark_weight_trade_date IS NULL OR benchmark_weight_trade_date <= obs_date),
    CONSTRAINT pit_industry_fapi_monthly_source_date_ck
        CHECK (source_max_report_date IS NULL OR source_max_report_date <= obs_date),
    CONSTRAINT pit_industry_fapi_monthly_previous_source_date_ck
        CHECK (previous_source_max_report_date IS NULL OR previous_source_max_report_date < obs_date),
    CONSTRAINT pit_industry_fapi_monthly_equity_staleness_ck
        CHECK (equity_staleness_days IS NULL OR equity_staleness_days >= 0),
    CONSTRAINT pit_industry_fapi_monthly_benchmark_staleness_ck
        CHECK (benchmark_weight_staleness_days IS NULL OR benchmark_weight_staleness_days >= 0),
    CONSTRAINT pit_industry_fapi_monthly_spread_count_ck
        CHECK (matched_org_count = spread_up_org_count + spread_down_or_flat_org_count),
    CONSTRAINT pit_industry_fapi_monthly_ratio_count_ck
        CHECK (ratio_matched_org_count = ratio_up_org_count + ratio_down_or_flat_org_count),
    CONSTRAINT pit_industry_fapi_monthly_spread_equal_ck
        CHECK (fapi_spread_equal IS NULL OR (fapi_spread_equal >= 0 AND fapi_spread_equal <= 1)),
    CONSTRAINT pit_industry_fapi_monthly_spread_weighted_ck
        CHECK (fapi_spread_weighted IS NULL OR (fapi_spread_weighted >= 0 AND fapi_spread_weighted <= 1)),
    CONSTRAINT pit_industry_fapi_monthly_ratio_equal_ck
        CHECK (fapi_ratio_equal IS NULL OR (fapi_ratio_equal >= 0 AND fapi_ratio_equal <= 1)),
    CONSTRAINT pit_industry_fapi_monthly_ratio_weighted_ck
        CHECK (fapi_ratio_weighted IS NULL OR (fapi_ratio_weighted >= 0 AND fapi_ratio_weighted <= 1)),
    CONSTRAINT pit_industry_fapi_monthly_org_weight_ck
        CHECK (
            spread_up_org_weight >= 0
            AND spread_total_org_weight >= 0
            AND ratio_up_org_weight >= 0
            AND ratio_total_org_weight >= 0
            AND spread_up_org_weight <= spread_total_org_weight
            AND ratio_up_org_weight <= ratio_total_org_weight
        )
);

CREATE INDEX IF NOT EXISTS idx_pit_industry_fapi_level_date
    ON pit.pit_industry_fapi_monthly (industry_level, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_industry_fapi_eligible_date
    ON pit.pit_industry_fapi_monthly (is_eligible, obs_date);

COMMENT ON TABLE pit.pit_industry_fapi_monthly IS
    'Monthly after-close PIT facts for source-adapted SW industry FAPI relative to CSI 800.';

COMMENT ON COLUMN pit.pit_industry_fapi_monthly.fapi_spread_equal IS
    'Share of matched brokers whose industry-minus-CSI800 expected ROE spread rose from t-1 to t.';

COMMENT ON COLUMN pit.pit_industry_fapi_monthly.fapi_spread_weighted IS
    'Available weighted FAPI using log common coverage and a 183-day report-recency half life; not the unavailable source forecast-accuracy weight.';

COMMENT ON COLUMN pit.pit_industry_fapi_monthly.expected_roe_equal IS
    'Equal-broker expected ROE on the current-month common sample using book equity total_mv/pb.';
