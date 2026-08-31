CREATE SCHEMA IF NOT EXISTS pit;

CREATE TABLE IF NOT EXISTS pit.pit_earnings_surprise_annual (
    ts_code                            varchar(16)  NOT NULL,
    end_date                           date         NOT NULL,
    ann_date                           date         NOT NULL,
    target_year                        integer      NOT NULL,
    actual_np_yuan                     numeric,
    actual_np_10k                      numeric,
    actual_basic_eps                   numeric,
    actual_diluted_eps                 numeric,
    actual_source_row_count            integer,
    actual_source_value_conflict       boolean,
    actual_source_update_time          timestamp without time zone,
    actual_source_selection_basis      varchar(64),
    consensus_obs_date                 date,
    consensus_np_10k                   numeric,
    consensus_basic_eps                numeric,
    consensus_org_count                integer,
    consensus_np_org_count             integer,
    consensus_np_dispersion_rate       numeric,
    consensus_age_days                 integer,
    np_surprise_abs_10k                numeric,
    np_surprise_rate                   numeric,
    eps_surprise_abs                   numeric,
    eps_surprise_rate                  numeric,
    is_np_sign_change                  boolean,
    consensus_is_eligible              boolean,
    is_eligible                        boolean      NOT NULL DEFAULT false,
    quality_reasons                    jsonb        NOT NULL DEFAULT '[]'::jsonb,
    consensus_availability_basis       varchar(32),
    formula_version                    varchar(80)  NOT NULL,
    consensus_source_max_report_date   date,
    source_income_updated_at            timestamptz,
    created_at                         timestamptz  NOT NULL DEFAULT now(),
    updated_at                         timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_code, end_date, ann_date),
    CHECK (EXTRACT(MONTH FROM end_date) = 12),
    CHECK (EXTRACT(DAY FROM end_date) = 31),
    CHECK (target_year = EXTRACT(YEAR FROM end_date)::integer),
    CHECK (consensus_obs_date < ann_date OR consensus_obs_date IS NULL),
    CHECK (consensus_age_days >= 1 OR consensus_age_days IS NULL),
    CHECK (consensus_org_count >= consensus_np_org_count OR consensus_org_count IS NULL),
    CHECK (is_eligible = false OR consensus_is_eligible = true)
);

CREATE INDEX IF NOT EXISTS idx_pit_earnings_surprise_ann_date
    ON pit.pit_earnings_surprise_annual (ann_date);

CREATE INDEX IF NOT EXISTS idx_pit_earnings_surprise_stock_period
    ON pit.pit_earnings_surprise_annual (ts_code, end_date);

CREATE INDEX IF NOT EXISTS idx_pit_earnings_surprise_eligible_ann
    ON pit.pit_earnings_surprise_annual (is_eligible, ann_date);

COMMENT ON TABLE pit.pit_earnings_surprise_annual IS
    'First formal annual-report actual versus latest eligible month-end fixed-FY consensus strictly before announcement.';

COMMENT ON COLUMN pit.pit_earnings_surprise_annual.actual_np_yuan IS
    'Annual parent net profit preserved from tushare.fina_income in CNY.';

COMMENT ON COLUMN pit.pit_earnings_surprise_annual.actual_np_10k IS
    'actual_np_yuan divided by 10,000 for comparison with stock_report_rc.np.';
