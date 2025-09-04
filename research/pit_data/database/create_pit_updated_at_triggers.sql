-- PIT: BEFORE UPDATE triggers to auto-refresh updated_at (self-contained, not reusing pgs_factor code)

-- 1) Create or replace a local function in pgs_factors schema
CREATE SCHEMA IF NOT EXISTS pgs_factors;
CREATE OR REPLACE FUNCTION pgs_factors.update_updated_at_pit()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2) Create triggers for each PIT table if not exists
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_pit_income_quarterly_updated_at'
    ) THEN
        CREATE TRIGGER trg_pit_income_quarterly_updated_at
        BEFORE UPDATE ON pgs_factors.pit_income_quarterly
        FOR EACH ROW EXECUTE FUNCTION pgs_factors.update_updated_at_pit();
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_pit_balance_quarterly_updated_at'
    ) THEN
        CREATE TRIGGER trg_pit_balance_quarterly_updated_at
        BEFORE UPDATE ON pgs_factors.pit_balance_quarterly
        FOR EACH ROW EXECUTE FUNCTION pgs_factors.update_updated_at_pit();
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_pit_financial_indicators_updated_at'
    ) THEN
        CREATE TRIGGER trg_pit_financial_indicators_updated_at
        BEFORE UPDATE ON pgs_factors.pit_financial_indicators
        FOR EACH ROW EXECUTE FUNCTION pgs_factors.update_updated_at_pit();
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_pit_industry_classification_updated_at'
    ) THEN
        CREATE TRIGGER trg_pit_industry_classification_updated_at
        BEFORE UPDATE ON pgs_factors.pit_industry_classification
        FOR EACH ROW EXECUTE FUNCTION pgs_factors.update_updated_at_pit();
    END IF;
END $$;

