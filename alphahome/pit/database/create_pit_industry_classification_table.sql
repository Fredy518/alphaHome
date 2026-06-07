-- PIT 行业分类表 DDL（标准命名，含 updated_at 默认）
CREATE TABLE IF NOT EXISTS pgs_factors.pit_industry_classification (
    ts_code VARCHAR(20) NOT NULL,
    obs_date DATE NOT NULL,
    data_source VARCHAR(20) NOT NULL,

    industry_level1 VARCHAR(128),
    industry_level2 VARCHAR(128),
    industry_level3 VARCHAR(128),
    industry_code1 VARCHAR(32),
    industry_code2 VARCHAR(32),
    industry_code3 VARCHAR(32),

    requires_special_gpa_handling BOOLEAN DEFAULT FALSE,
    gpa_calculation_method VARCHAR(64),
    special_handling_reason TEXT,

    data_quality VARCHAR(20) DEFAULT 'normal',
    snapshot_version VARCHAR(32),

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ts_code, obs_date, data_source)
);

CREATE INDEX IF NOT EXISTS idx_pit_industry_classification_ts_obs
ON pgs_factors.pit_industry_classification (ts_code, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_industry_classification_quality
ON pgs_factors.pit_industry_classification (data_quality, obs_date);

