-- PREPARED ONLY. Do not run against production without a reviewed migration plan.
-- This adds nullable provenance; it does NOT certify or rewrite existing rows.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE pit.pit_income_quarterly
    ADD COLUMN IF NOT EXISTS source_ann_date date,
    ADD COLUMN IF NOT EXISTS source_f_ann_date date,
    ADD COLUMN IF NOT EXISTS source_update_time timestamp,
    ADD COLUMN IF NOT EXISTS source_version_hash varchar(64),
    ADD COLUMN IF NOT EXISTS availability_basis varchar(64),
    ADD COLUMN IF NOT EXISTS pit_contract_version varchar(64);
ALTER TABLE pit.pit_balance_quarterly
    ADD COLUMN IF NOT EXISTS source_ann_date date,
    ADD COLUMN IF NOT EXISTS source_f_ann_date date,
    ADD COLUMN IF NOT EXISTS source_update_time timestamp,
    ADD COLUMN IF NOT EXISTS source_version_hash varchar(64),
    ADD COLUMN IF NOT EXISTS availability_basis varchar(64),
    ADD COLUMN IF NOT EXISTS pit_contract_version varchar(64);
ALTER TABLE pit.pit_cashflow_quarterly
    ADD COLUMN IF NOT EXISTS source_ann_date date,
    ADD COLUMN IF NOT EXISTS source_f_ann_date date,
    ADD COLUMN IF NOT EXISTS source_update_time timestamp,
    ADD COLUMN IF NOT EXISTS source_version_hash varchar(64),
    ADD COLUMN IF NOT EXISTS availability_basis varchar(64),
    ADD COLUMN IF NOT EXISTS pit_contract_version varchar(64);
ALTER TABLE pit.pit_financial_indicators
    ADD COLUMN IF NOT EXISTS income_ann_date date,
    ADD COLUMN IF NOT EXISTS balance_ann_date date,
    ADD COLUMN IF NOT EXISTS source_available_date date,
    ADD COLUMN IF NOT EXISTS availability_basis varchar(64),
    ADD COLUMN IF NOT EXISTS pit_contract_version varchar(64);
ALTER TABLE factors.p_factor
    ADD COLUMN IF NOT EXISTS source_available_date date,
    ADD COLUMN IF NOT EXISTS availability_basis varchar(64),
    ADD COLUMN IF NOT EXISTS pit_contract_version varchar(64);
ALTER TABLE factors.g_factor
    ADD COLUMN IF NOT EXISTS source_available_date date,
    ADD COLUMN IF NOT EXISTS availability_basis varchar(64),
    ADD COLUMN IF NOT EXISTS pit_contract_version varchar(64);

-- NOT VALID leaves legacy rows unverified, while checking all new/updated rows.
-- Existing four-column statement keys already distinguish public event dates.
-- Constraints are added only if absent, so the reviewed DDL can be retried.
DO $$
DECLARE target text;
BEGIN
    FOREACH target IN ARRAY ARRAY['pit_income_quarterly','pit_balance_quarterly','pit_cashflow_quarterly']
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass('pit.'||target)
                       AND conname=target||'_disclosure_v2') THEN
            EXECUTE format('ALTER TABLE pit.%I ADD CONSTRAINT %I CHECK (
                pit_contract_version IS NULL OR (
                    pit_contract_version = ''public_disclosure_v2''
                    AND availability_basis IS NOT NULL
                    AND availability_basis = ''public_disclosure_reconstructed''
                    AND source_version_hash IS NOT NULL
                    AND COALESCE(source_f_ann_date, source_ann_date) IS NOT NULL
                    AND ann_date >= GREATEST(source_ann_date, source_f_ann_date)
                )) NOT VALID', target, target||'_disclosure_v2');
        END IF;
    END LOOP;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='pit.pit_financial_indicators'::regclass
                   AND conname='financial_indicators_disclosure_v2') THEN
        ALTER TABLE pit.pit_financial_indicators ADD CONSTRAINT financial_indicators_disclosure_v2
            CHECK (pit_contract_version IS NULL OR (
                pit_contract_version='public_disclosure_v2'
                AND availability_basis IS NOT NULL
                AND availability_basis='public_disclosure_reconstructed'
                AND income_ann_date IS NOT NULL AND balance_ann_date IS NOT NULL
                AND source_available_date IS NOT NULL
                AND source_available_date >= GREATEST(income_ann_date,balance_ann_date)
                AND ann_date >= source_available_date
            )) NOT VALID;
    END IF;
    FOREACH target IN ARRAY ARRAY['p_factor','g_factor'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass('factors.'||target)
                       AND conname=target||'_disclosure_v2') THEN
            EXECUTE format('ALTER TABLE factors.%I ADD CONSTRAINT %I CHECK (
                pit_contract_version IS NULL OR (
                    pit_contract_version = ''public_disclosure_v2''
                    AND availability_basis IS NOT NULL
                    AND availability_basis = ''public_disclosure_reconstructed''
                    AND source_available_date IS NOT NULL
                    AND ann_date >= source_available_date AND calc_date >= ann_date
                )) NOT VALID', target, target||'_disclosure_v2');
        END IF;
    END LOOP;
END $$;
COMMIT;
