-- PIT: BEFORE UPDATE triggers to auto-refresh updated_at.

-- 1) Create or replace a local function in pit schema
CREATE SCHEMA IF NOT EXISTS pit;
CREATE OR REPLACE FUNCTION pit.update_updated_at_pit()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2) Recreate triggers for each PIT table when the table exists.
DO $$ BEGIN
    IF to_regclass('pit.pit_income_quarterly') IS NOT NULL THEN
        DROP TRIGGER IF EXISTS trg_pit_income_quarterly_updated_at ON pit.pit_income_quarterly;
        CREATE TRIGGER trg_pit_income_quarterly_updated_at
        BEFORE UPDATE ON pit.pit_income_quarterly
        FOR EACH ROW EXECUTE FUNCTION pit.update_updated_at_pit();
    END IF;
END $$;

DO $$ BEGIN
    IF to_regclass('pit.pit_balance_quarterly') IS NOT NULL THEN
        DROP TRIGGER IF EXISTS trg_pit_balance_quarterly_updated_at ON pit.pit_balance_quarterly;
        CREATE TRIGGER trg_pit_balance_quarterly_updated_at
        BEFORE UPDATE ON pit.pit_balance_quarterly
        FOR EACH ROW EXECUTE FUNCTION pit.update_updated_at_pit();
    END IF;
END $$;

DO $$ BEGIN
    IF to_regclass('pit.pit_cashflow_quarterly') IS NOT NULL THEN
        DROP TRIGGER IF EXISTS trg_pit_cashflow_quarterly_updated_at ON pit.pit_cashflow_quarterly;
        CREATE TRIGGER trg_pit_cashflow_quarterly_updated_at
        BEFORE UPDATE ON pit.pit_cashflow_quarterly
        FOR EACH ROW EXECUTE FUNCTION pit.update_updated_at_pit();
    END IF;
END $$;

DO $$ BEGIN
    IF to_regclass('pit.pit_financial_indicators') IS NOT NULL THEN
        DROP TRIGGER IF EXISTS trg_pit_financial_indicators_updated_at ON pit.pit_financial_indicators;
        CREATE TRIGGER trg_pit_financial_indicators_updated_at
        BEFORE UPDATE ON pit.pit_financial_indicators
        FOR EACH ROW EXECUTE FUNCTION pit.update_updated_at_pit();
    END IF;
END $$;

DO $$ BEGIN
    IF to_regclass('pit.pit_industry_classification') IS NOT NULL THEN
        DROP TRIGGER IF EXISTS trg_pit_industry_classification_updated_at ON pit.pit_industry_classification;
        CREATE TRIGGER trg_pit_industry_classification_updated_at
        BEFORE UPDATE ON pit.pit_industry_classification
        FOR EACH ROW EXECUTE FUNCTION pit.update_updated_at_pit();
    END IF;
END $$;
