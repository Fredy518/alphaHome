-- 幂等创建索引用于收入表查询优化
CREATE SCHEMA IF NOT EXISTS pit;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='pit' AND c.relname='idx_pit_income_quarterly_ts_ds_ann') THEN
    CREATE INDEX idx_pit_income_quarterly_ts_ds_ann
    ON pit.pit_income_quarterly (ts_code, data_source, ann_date);
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='pit' AND c.relname='idx_pit_income_quarterly_ts_end_ann') THEN
    CREATE INDEX idx_pit_income_quarterly_ts_end_ann
    ON pit.pit_income_quarterly (ts_code, end_date, ann_date);
  END IF;
END $$;
