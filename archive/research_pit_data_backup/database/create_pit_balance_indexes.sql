-- 幂等创建索引以优化历史回看查询（PIT 填充使用）
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='pgs_factors' AND c.relname='idx_pit_balance_quarterly_ts_ds_ann') THEN
        CREATE INDEX idx_pit_balance_quarterly_ts_ds_ann
        ON pgs_factors.pit_balance_quarterly (ts_code, data_source, ann_date);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='pgs_factors' AND c.relname='idx_pit_balance_quarterly_ts_end_ann') THEN
        CREATE INDEX idx_pit_balance_quarterly_ts_end_ann
        ON pgs_factors.pit_balance_quarterly (ts_code, end_date, ann_date);
    END IF;
END $$;

