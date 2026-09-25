-- Apply only after checking the NAS target and pausing writers for these tables.
-- The transaction aborts if any existing business key is null or duplicated.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM features.mv_dc_index_features_daily
        WHERE ts_code IS NULL OR trade_date IS NULL
    ) OR EXISTS (
        SELECT 1 FROM features.mv_dc_index_features_daily
        GROUP BY ts_code, trade_date HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'mv_dc_index_features_daily has invalid business keys';
    END IF;

    IF EXISTS (
        SELECT 1 FROM features.mv_industry_toplist_signal_daily
        WHERE industry_level IS NULL OR industry_code IS NULL OR trade_date IS NULL
    ) OR EXISTS (
        SELECT 1 FROM features.mv_industry_toplist_signal_daily
        GROUP BY industry_level, industry_code, trade_date HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'mv_industry_toplist_signal_daily has invalid business keys';
    END IF;

    IF EXISTS (
        SELECT 1 FROM features.mv_market_sentiment_daily WHERE trade_date IS NULL
    ) OR EXISTS (
        SELECT 1 FROM features.mv_market_sentiment_daily
        GROUP BY trade_date HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'mv_market_sentiment_daily has invalid business keys';
    END IF;

    IF EXISTS (
        SELECT 1 FROM features.mv_market_technical_daily WHERE trade_date IS NULL
    ) OR EXISTS (
        SELECT 1 FROM features.mv_market_technical_daily
        GROUP BY trade_date HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'mv_market_technical_daily has invalid business keys';
    END IF;
END $$;

ALTER TABLE features.mv_dc_index_features_daily
    ALTER COLUMN ts_code SET NOT NULL,
    ALTER COLUMN trade_date SET NOT NULL;
CREATE UNIQUE INDEX uq_feature_f9374f46baae
    ON features.mv_dc_index_features_daily (ts_code, trade_date);

ALTER TABLE features.mv_industry_toplist_signal_daily
    ALTER COLUMN industry_level SET NOT NULL,
    ALTER COLUMN industry_code SET NOT NULL,
    ALTER COLUMN trade_date SET NOT NULL;
CREATE UNIQUE INDEX uq_feature_000d1b6d1d9f
    ON features.mv_industry_toplist_signal_daily
    (industry_level, industry_code, trade_date);

ALTER TABLE features.mv_market_sentiment_daily
    ALTER COLUMN trade_date SET NOT NULL;
CREATE UNIQUE INDEX uq_feature_0e004de4de5c
    ON features.mv_market_sentiment_daily (trade_date);

ALTER TABLE features.mv_market_technical_daily
    ALTER COLUMN trade_date SET NOT NULL;
CREATE UNIQUE INDEX uq_feature_bd70bfe25949
    ON features.mv_market_technical_daily (trade_date);

COMMIT;
