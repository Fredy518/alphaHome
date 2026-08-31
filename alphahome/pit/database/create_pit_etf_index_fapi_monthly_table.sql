CREATE SCHEMA IF NOT EXISTS pit;

CREATE TABLE IF NOT EXISTS pit.pit_etf_index_fapi_monthly (
    obs_date                              date          NOT NULL,
    universe_type                        varchar(32)   NOT NULL,
    index_code                           varchar(24)   NOT NULL,
    index_name                           varchar(160)  NOT NULL,
    member_weight_basis                  varchar(40)   NOT NULL,
    member_weight_source                 varchar(80)   NOT NULL,
    member_source_code                   varchar(24)   NOT NULL,
    member_source_effective_date         date          NOT NULL,
    member_source_available_date         date          NOT NULL,
    member_source_staleness_days         integer       NOT NULL,
    member_source_coverage_rate          numeric(20,10) NOT NULL,
    member_source_quality                varchar(16)   NOT NULL,
    member_source_is_fallback            boolean       NOT NULL DEFAULT false,
    member_constituent_scope             varchar(64)   NOT NULL DEFAULT 'full_index',
    member_is_proxy                      boolean       NOT NULL DEFAULT false,
    member_scope_weight_rate             numeric(20,10) NOT NULL DEFAULT 1,
    member_source_is_eligible            boolean       NOT NULL DEFAULT false,
    member_source_quality_reasons        jsonb         NOT NULL DEFAULT '[]'::jsonb,
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
    is_fapi_eligible                     boolean       NOT NULL DEFAULT false,
    is_ratio_fapi_eligible               boolean       NOT NULL DEFAULT false,
    fapi_quality_reasons                 jsonb         NOT NULL DEFAULT '[]'::jsonb,
    is_eligible                          boolean       NOT NULL DEFAULT false,
    quality_reasons                      jsonb         NOT NULL DEFAULT '[]'::jsonb,
    method_version                       varchar(64)   NOT NULL,
    org_weight_version                   varchar(64)   NOT NULL,
    quality_rule_version                 varchar(64)   NOT NULL,
    created_at                           timestamptz   NOT NULL DEFAULT now(),
    updated_at                           timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pit_etf_index_fapi_monthly_pk PRIMARY KEY (
        obs_date,
        index_code,
        benchmark_code,
        method_version
    ),
    CONSTRAINT pit_etf_index_fapi_monthly_type_ck
        CHECK (universe_type = 'etf_tracked_index'),
    CONSTRAINT pit_etf_index_fapi_monthly_member_quality_ck
        CHECK (member_source_quality IN ('high', 'partial', 'low')),
    CONSTRAINT pit_etf_index_fapi_monthly_member_effective_date_ck
        CHECK (member_source_effective_date <= obs_date),
    CONSTRAINT pit_etf_index_fapi_monthly_member_available_date_ck
        CHECK (member_source_available_date <= obs_date),
    CONSTRAINT pit_etf_index_fapi_monthly_member_staleness_ck
        CHECK (member_source_staleness_days >= 0),
    CONSTRAINT pit_etf_index_fapi_monthly_member_coverage_ck
        CHECK (member_source_coverage_rate > 0 AND member_source_coverage_rate <= 1.10),
    CONSTRAINT pit_etf_index_fapi_monthly_member_scope_weight_ck
        CHECK (member_scope_weight_rate > 0 AND member_scope_weight_rate <= 1.000001),
    CONSTRAINT pit_etf_index_fapi_monthly_equity_date_ck
        CHECK (equity_trade_date IS NULL OR equity_trade_date <= obs_date),
    CONSTRAINT pit_etf_index_fapi_monthly_benchmark_date_ck
        CHECK (benchmark_weight_trade_date IS NULL OR benchmark_weight_trade_date <= obs_date),
    CONSTRAINT pit_etf_index_fapi_monthly_source_date_ck
        CHECK (source_max_report_date IS NULL OR source_max_report_date <= obs_date),
    CONSTRAINT pit_etf_index_fapi_monthly_previous_source_date_ck
        CHECK (previous_source_max_report_date IS NULL OR previous_source_max_report_date < obs_date),
    CONSTRAINT pit_etf_index_fapi_monthly_spread_count_ck
        CHECK (matched_org_count = spread_up_org_count + spread_down_or_flat_org_count),
    CONSTRAINT pit_etf_index_fapi_monthly_ratio_count_ck
        CHECK (ratio_matched_org_count = ratio_up_org_count + ratio_down_or_flat_org_count),
    CONSTRAINT pit_etf_index_fapi_monthly_spread_equal_ck
        CHECK (fapi_spread_equal IS NULL OR (fapi_spread_equal >= 0 AND fapi_spread_equal <= 1)),
    CONSTRAINT pit_etf_index_fapi_monthly_spread_weighted_ck
        CHECK (fapi_spread_weighted IS NULL OR (fapi_spread_weighted >= 0 AND fapi_spread_weighted <= 1)),
    CONSTRAINT pit_etf_index_fapi_monthly_ratio_equal_ck
        CHECK (fapi_ratio_equal IS NULL OR (fapi_ratio_equal >= 0 AND fapi_ratio_equal <= 1)),
    CONSTRAINT pit_etf_index_fapi_monthly_ratio_weighted_ck
        CHECK (fapi_ratio_weighted IS NULL OR (fapi_ratio_weighted >= 0 AND fapi_ratio_weighted <= 1)),
    CONSTRAINT pit_etf_index_fapi_monthly_org_weight_ck
        CHECK (
            spread_up_org_weight >= 0
            AND spread_total_org_weight >= 0
            AND ratio_up_org_weight >= 0
            AND ratio_total_org_weight >= 0
            AND spread_up_org_weight <= spread_total_org_weight
            AND ratio_up_org_weight <= ratio_total_org_weight
        )
);

ALTER TABLE pit.pit_etf_index_fapi_monthly
    ADD COLUMN IF NOT EXISTS member_constituent_scope varchar(64) NOT NULL DEFAULT 'full_index';

ALTER TABLE pit.pit_etf_index_fapi_monthly
    ADD COLUMN IF NOT EXISTS member_is_proxy boolean NOT NULL DEFAULT false;

ALTER TABLE pit.pit_etf_index_fapi_monthly
    ADD COLUMN IF NOT EXISTS member_scope_weight_rate numeric(20,10) NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_pit_etf_index_fapi_index_date
    ON pit.pit_etf_index_fapi_monthly (index_code, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_etf_index_fapi_eligible_date
    ON pit.pit_etf_index_fapi_monthly (is_eligible, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_etf_index_fapi_member_source_date
    ON pit.pit_etf_index_fapi_monthly (member_weight_basis, obs_date);

COMMENT ON TABLE pit.pit_etf_index_fapi_monthly IS
    'ETF跟踪指数相对中证800的来源适配FAPI与预期ROE月末PIT快照。';

COMMENT ON COLUMN pit.pit_etf_index_fapi_monthly.member_source_is_fallback IS
    'True表示该月指数成分来自公告后可见的ETF持仓代理，而非官方历史指数权重。';

COMMENT ON COLUMN pit.pit_etf_index_fapi_monthly.member_is_proxy IS
    'True表示基本面只由明确标记的子样本代理，不得解读为完整跨市场指数基本面。';
