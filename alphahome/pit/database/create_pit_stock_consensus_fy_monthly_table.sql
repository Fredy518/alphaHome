CREATE SCHEMA IF NOT EXISTS pit;

CREATE TABLE IF NOT EXISTS pit.pit_stock_consensus_fy_monthly (
    obs_date                         date         NOT NULL,
    ts_code                          varchar(16)  NOT NULL,
    target_year                      integer      NOT NULL,
    forecast_horizon_years           smallint     NOT NULL,
    org_count                        integer      NOT NULL DEFAULT 0,
    np_org_count                     integer      NOT NULL DEFAULT 0,
    eps_org_count                    integer      NOT NULL DEFAULT 0,
    np_consensus_median              numeric,
    np_consensus_mean                numeric,
    eps_consensus_median             numeric,
    eps_consensus_mean               numeric,
    np_dispersion_mad                numeric,
    np_dispersion_rate               numeric,
    latest_report_date               date         NOT NULL,
    oldest_selected_report_date      date         NOT NULL,
    median_forecast_age_days         numeric,
    revision_matched_org_count_1m    integer      NOT NULL DEFAULT 0,
    revision_revised_org_count_1m    integer      NOT NULL DEFAULT 0,
    revision_up_org_count_1m         integer      NOT NULL DEFAULT 0,
    revision_down_org_count_1m       integer      NOT NULL DEFAULT 0,
    np_revision_abs_1m               numeric,
    np_revision_rate_1m              numeric,
    revision_activity_rate_1m        numeric,
    revision_up_org_rate_1m          numeric,
    revision_matched_org_count_3m    integer      NOT NULL DEFAULT 0,
    revision_revised_org_count_3m    integer      NOT NULL DEFAULT 0,
    revision_up_org_count_3m         integer      NOT NULL DEFAULT 0,
    revision_down_org_count_3m       integer      NOT NULL DEFAULT 0,
    np_revision_abs_3m               numeric,
    np_revision_rate_3m              numeric,
    revision_activity_rate_3m        numeric,
    revision_up_org_rate_3m          numeric,
    is_eligible                      boolean      NOT NULL DEFAULT false,
    quality_reasons                  jsonb        NOT NULL DEFAULT '[]'::jsonb,
    availability_basis               varchar(32)  NOT NULL,
    formula_version                  varchar(64)  NOT NULL,
    source_max_report_date           date         NOT NULL,
    created_at                       timestamptz  NOT NULL DEFAULT now(),
    updated_at                       timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (obs_date, ts_code, target_year),
    CHECK (target_year BETWEEN 1990 AND 2100),
    CHECK (forecast_horizon_years BETWEEN -1 AND 2),
    CHECK (latest_report_date <= obs_date),
    CHECK (oldest_selected_report_date <= latest_report_date),
    CHECK (source_max_report_date = latest_report_date),
    CHECK (org_count >= np_org_count AND org_count >= eps_org_count),
    CHECK (revision_revised_org_count_1m = revision_up_org_count_1m + revision_down_org_count_1m),
    CHECK (revision_revised_org_count_3m = revision_up_org_count_3m + revision_down_org_count_3m),
    CHECK (revision_matched_org_count_1m >= revision_revised_org_count_1m),
    CHECK (revision_matched_org_count_3m >= revision_revised_org_count_3m),
    CHECK (revision_activity_rate_1m BETWEEN 0 AND 1 OR revision_activity_rate_1m IS NULL),
    CHECK (revision_activity_rate_3m BETWEEN 0 AND 1 OR revision_activity_rate_3m IS NULL),
    CHECK (revision_up_org_rate_1m BETWEEN 0 AND 1 OR revision_up_org_rate_1m IS NULL),
    CHECK (revision_up_org_rate_3m BETWEEN 0 AND 1 OR revision_up_org_rate_3m IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_pit_stock_consensus_fy_obs_date
    ON pit.pit_stock_consensus_fy_monthly (obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_stock_consensus_fy_stock_date
    ON pit.pit_stock_consensus_fy_monthly (ts_code, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_stock_consensus_fy_target_date
    ON pit.pit_stock_consensus_fy_monthly (target_year, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_stock_consensus_fy_eligible_date
    ON pit.pit_stock_consensus_fy_monthly (is_eligible, obs_date);

COMMENT ON TABLE pit.pit_stock_consensus_fy_monthly IS
    'Month-end stock analyst consensus by fixed fiscal year; one latest forecast per broker.';

COMMENT ON COLUMN pit.pit_stock_consensus_fy_monthly.np_consensus_median IS
    'Median annual parent-net-profit forecast in 10,000 CNY across latest broker estimates.';

COMMENT ON COLUMN pit.pit_stock_consensus_fy_monthly.availability_basis IS
    'Historical availability is reconstructed from report_date, not vendor create_time.';
