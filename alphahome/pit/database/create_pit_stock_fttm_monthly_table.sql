CREATE SCHEMA IF NOT EXISTS pit;

CREATE TABLE IF NOT EXISTS pit.pit_stock_fttm_monthly (
    ts_code                  varchar(16)  NOT NULL,
    org_name                 varchar(255) NOT NULL,
    obs_date                 date         NOT NULL,
    selected_report_date     date         NOT NULL,
    selected_author_name     varchar(255) NOT NULL DEFAULT '',
    report_quarter           smallint     NOT NULL,
    fy1_year                 integer      NOT NULL,
    fy2_year                 integer      NOT NULL,
    fy1_np_raw               numeric,
    fy2_np_raw               numeric,
    fy1_np_used              numeric      NOT NULL,
    fy2_np_used              numeric      NOT NULL,
    fy1_value_source         varchar(16),
    fy2_value_source         varchar(16),
    fy1_weight               numeric(8,6) NOT NULL,
    fy2_weight               numeric(8,6) NOT NULL,
    fttm_np                  numeric      NOT NULL,
    selected_total_share     numeric,
    fttm_eps                 numeric,
    estimate_pair_status     varchar(16)  NOT NULL,
    is_single_year_fallback  boolean      NOT NULL,
    source_window_start      date         NOT NULL,
    source_window_end        date         NOT NULL,
    formula_version          varchar(32)  NOT NULL,
    source_max_report_date   date         NOT NULL,
    created_at               timestamptz  NOT NULL DEFAULT now(),
    updated_at               timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_code, org_name, obs_date),
    CHECK (report_quarter BETWEEN 1 AND 4),
    CHECK (fy1_year = extract(year FROM selected_report_date)::integer),
    CHECK (fy2_year = fy1_year + 1),
    CHECK (selected_report_date <= obs_date),
    CHECK (source_max_report_date = selected_report_date),
    CHECK (fy1_weight = (5 - report_quarter)::numeric / 4),
    CHECK (fy2_weight = (report_quarter - 1)::numeric / 4),
    CHECK (estimate_pair_status IN ('both', 'fy1_only', 'fy2_only')),
    CHECK (fy1_weight + fy2_weight = 1)
);

CREATE INDEX IF NOT EXISTS idx_pit_stock_fttm_obs_date
    ON pit.pit_stock_fttm_monthly (obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_stock_fttm_stock_date
    ON pit.pit_stock_fttm_monthly (ts_code, obs_date);

COMMENT ON TABLE pit.pit_stock_fttm_monthly IS
    'Monthly after-close PIT snapshots of broker-event FTTM net-profit estimates.';
