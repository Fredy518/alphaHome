-- 财务指标表（标准命名）
-- 从 create_mvp_financial_indicators_table.sql 重命名来的等价DDL

CREATE TABLE IF NOT EXISTS pgs_factors.pit_financial_indicators (
    ts_code VARCHAR(20) NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    data_source VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    gpa_ttm DECIMAL(15,4),
    roe_excl_ttm DECIMAL(15,4),
    roa_excl_ttm DECIMAL(15,4),

    net_margin_ttm DECIMAL(15,4),
    operating_margin_ttm DECIMAL(15,4),
    roi_ttm DECIMAL(15,4),

    asset_turnover_ttm DECIMAL(15,4),
    equity_multiplier DECIMAL(15,4),

    debt_to_asset_ratio DECIMAL(15,4),
    equity_ratio DECIMAL(15,4),

    revenue_yoy_growth DECIMAL(15,4),           -- 营收同比增长率(%)
    n_income_yoy_growth DECIMAL(15,4),           -- 归属母公司股东的净利润同比增长率(%)
    operate_profit_yoy_growth DECIMAL(15,4),     -- 经营利润同比增长率(%)

    data_quality VARCHAR(20) DEFAULT 'normal',
    calculation_status VARCHAR(20) DEFAULT 'success',
    data_completeness VARCHAR(20) DEFAULT 'income_only', -- 新增：数据完整性
    balance_sheet_lag INTEGER,                         -- 新增：资产负债表数据延迟（天）

    PRIMARY KEY (ts_code, ann_date, end_date, data_source)
);

-- 兼容已存在的旧表：旧主键缺少 data_source，会导致计算器的
-- ON CONFLICT (ts_code, ann_date, end_date, data_source) 无法匹配唯一约束。
DO $$
DECLARE
    pk_name TEXT;
    pk_cols TEXT[];
BEGIN
    SELECT c.conname,
           ARRAY_AGG(a.attname ORDER BY u.ord)
      INTO pk_name, pk_cols
      FROM pg_constraint c
      JOIN pg_class t ON c.conrelid = t.oid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      JOIN UNNEST(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
     WHERE n.nspname = 'pgs_factors'
       AND t.relname = 'pit_financial_indicators'
       AND c.contype = 'p'
     GROUP BY c.conname;

    IF pk_name IS NULL THEN
        ALTER TABLE pgs_factors.pit_financial_indicators
        ADD CONSTRAINT pit_financial_indicators_pkey
        PRIMARY KEY (ts_code, ann_date, end_date, data_source);
    ELSIF NOT (pk_cols @> ARRAY['ts_code','ann_date','end_date','data_source']
               AND ARRAY['ts_code','ann_date','end_date','data_source'] @> pk_cols) THEN
        EXECUTE FORMAT(
            'ALTER TABLE pgs_factors.pit_financial_indicators DROP CONSTRAINT %I',
            pk_name
        );
        ALTER TABLE pgs_factors.pit_financial_indicators
        ADD CONSTRAINT pit_financial_indicators_pkey
        PRIMARY KEY (ts_code, ann_date, end_date, data_source);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_pit_financial_indicators_ts_ann
ON pgs_factors.pit_financial_indicators (ts_code, ann_date);

CREATE INDEX IF NOT EXISTS idx_pit_financial_indicators_ann_date
ON pgs_factors.pit_financial_indicators (ann_date);

CREATE INDEX IF NOT EXISTS idx_pit_financial_indicators_quality
ON pgs_factors.pit_financial_indicators (data_quality, ann_date);

CREATE INDEX IF NOT EXISTS idx_pit_financial_indicators_composite
ON pgs_factors.pit_financial_indicators (ts_code, ann_date, data_quality);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'pit_financial_indicators'
        AND constraint_name = 'chk_data_quality'
    ) THEN
        ALTER TABLE pgs_factors.pit_financial_indicators
        ADD CONSTRAINT chk_data_quality CHECK (data_quality IN ('high', 'normal', 'low', 'outlier_high', 'outlier_low', 'invalid'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'pit_financial_indicators'
        AND constraint_name = 'chk_calc_status'
    ) THEN
        ALTER TABLE pgs_factors.pit_financial_indicators
        ADD CONSTRAINT chk_calc_status CHECK (calculation_status IN ('success', 'failed', 'partial'));
    END IF;
END $$;
