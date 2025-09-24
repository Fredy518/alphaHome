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

    PRIMARY KEY (ts_code, end_date, ann_date)
);

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

