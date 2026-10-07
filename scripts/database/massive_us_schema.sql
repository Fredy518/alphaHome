-- Additive setup for Massive US equity collectors. No historical backfill.
-- Review the configured database and existing relations before applying.
BEGIN;
SET LOCAL lock_timeout = '5s';
CREATE SCHEMA IF NOT EXISTS massive;
CREATE SCHEMA IF NOT EXISTS rawdata;

CREATE TABLE IF NOT EXISTS massive.stock_us_basic (
    ticker VARCHAR(64) NOT NULL,
    snapshot_date DATE NOT NULL,
    name TEXT NOT NULL,
    security_type VARCHAR(32) NOT NULL,
    primary_exchange VARCHAR(16) NOT NULL,
    currency_name VARCHAR(32),
    active BOOLEAN NOT NULL,
    cik VARCHAR(32),
    composite_figi VARCHAR(32),
    share_class_figi VARCHAR(32),
    source_updated_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, snapshot_date)
);
CREATE INDEX IF NOT EXISTS idx_stock_us_basic_date_type
    ON massive.stock_us_basic (snapshot_date, security_type);

CREATE TABLE IF NOT EXISTS massive.stock_us_daily (
    ticker VARCHAR(64) NOT NULL,
    trade_date DATE NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    vwap DOUBLE PRECISION,
    transactions BIGINT,
    bar_timestamp BIGINT NOT NULL,
    adjusted BOOLEAN NOT NULL CHECK (adjusted = false),
    observed_at TIMESTAMPTZ NOT NULL,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_stock_us_daily_date
    ON massive.stock_us_daily (trade_date);

CREATE TABLE IF NOT EXISTS massive.stock_us_split (
    event_id VARCHAR(128) NOT NULL,
    ticker VARCHAR(64) NOT NULL,
    execution_date DATE NOT NULL,
    split_from DOUBLE PRECISION NOT NULL,
    split_to DOUBLE PRECISION NOT NULL,
    adjustment_type VARCHAR(32) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id)
);
CREATE INDEX IF NOT EXISTS idx_stock_us_split_ticker_date
    ON massive.stock_us_split (ticker, execution_date);
CREATE INDEX IF NOT EXISTS idx_stock_us_split_execution_date
    ON massive.stock_us_split (execution_date);

CREATE OR REPLACE VIEW rawdata.stock_us_basic AS SELECT * FROM massive.stock_us_basic;
CREATE OR REPLACE VIEW rawdata.stock_us_daily AS SELECT * FROM massive.stock_us_daily;
CREATE OR REPLACE VIEW rawdata.stock_us_split AS SELECT * FROM massive.stock_us_split;
COMMIT;
