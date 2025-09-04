-- PIT行业分类表 (优化版) - 月度快照机制
-- 基于obs_date观察日期的月末快照，支持申万和中信双重分类

DROP TABLE IF EXISTS pgs_factors.pit_industry_classification CASCADE;

CREATE TABLE pgs_factors.pit_industry_classification (
    -- 主键字段
    ts_code VARCHAR(20) NOT NULL,                    -- 股票代码
    obs_date DATE NOT NULL,                          -- 观察日期 (月末快照日期)
    data_source VARCHAR(10) NOT NULL,                -- 数据来源: 'sw'(申万) 或 'ci'(中信)
    
    -- 统一行业分类字段
    industry_level1 VARCHAR(50),                     -- 一级行业名称
    industry_level2 VARCHAR(100),                    -- 二级行业名称  
    industry_level3 VARCHAR(150),                    -- 三级行业名称
    industry_code1 VARCHAR(20),                      -- 一级行业代码
    industry_code2 VARCHAR(20),                      -- 二级行业代码
    industry_code3 VARCHAR(20),                      -- 三级行业代码
    
    -- 特殊处理标识
    requires_special_gpa_handling BOOLEAN DEFAULT FALSE,  -- 是否需要GPA特殊处理
    gpa_calculation_method VARCHAR(50) DEFAULT 'standard', -- GPA计算方法
    special_handling_reason TEXT,                    -- 特殊处理原因说明
    
    -- 数据质量和元信息
    data_quality VARCHAR(20) DEFAULT 'normal',       -- 数据质量: high, normal, low
    snapshot_version VARCHAR(20),                    -- 快照版本号 (如: 2025-01)
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 主键约束 (支持同一股票同一日期的双重分类)
    CONSTRAINT pk_pit_industry_optimized PRIMARY KEY (ts_code, obs_date, data_source),
    
    -- 检查约束
    CONSTRAINT chk_data_source CHECK (data_source IN ('sw', 'ci')),
    CONSTRAINT chk_gpa_calculation_method CHECK (gpa_calculation_method IN ('standard', 'null', 'alternative', 'cost_income_ratio')),
    CONSTRAINT chk_data_quality CHECK (data_quality IN ('high', 'normal', 'low', 'invalid'))
);

-- 创建索引 (优化PIT查询性能)
CREATE INDEX idx_pit_industry_ts_code ON pgs_factors.pit_industry_classification (ts_code);
CREATE INDEX idx_pit_industry_obs_date ON pgs_factors.pit_industry_classification (obs_date);
CREATE INDEX idx_pit_industry_data_source ON pgs_factors.pit_industry_classification (data_source);
CREATE INDEX idx_pit_industry_level1 ON pgs_factors.pit_industry_classification (industry_level1);
CREATE INDEX idx_pit_industry_level2 ON pgs_factors.pit_industry_classification (industry_level2);
CREATE INDEX idx_pit_industry_special_gpa ON pgs_factors.pit_industry_classification (requires_special_gpa_handling);

-- 复合索引 (PIT查询优化)
CREATE INDEX idx_pit_industry_ts_obs_source ON pgs_factors.pit_industry_classification (ts_code, obs_date DESC, data_source);
CREATE INDEX idx_pit_industry_obs_source ON pgs_factors.pit_industry_classification (obs_date, data_source);
CREATE INDEX idx_pit_industry_source_level1 ON pgs_factors.pit_industry_classification (data_source, industry_level1);

-- 添加表注释
COMMENT ON TABLE pgs_factors.pit_industry_classification IS 'PIT行业分类表(优化版) - 基于月末快照的观察日期机制，支持申万和中信双重分类';

-- 添加字段注释
COMMENT ON COLUMN pgs_factors.pit_industry_classification.ts_code IS '股票代码';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.obs_date IS '观察日期 - 月末快照日期，该日期观察到的行业分类状态';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.data_source IS '数据来源 - sw(申万) 或 ci(中信)';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.industry_level1 IS '一级行业名称';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.industry_level2 IS '二级行业名称';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.industry_level3 IS '三级行业名称';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.industry_code1 IS '一级行业代码';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.industry_code2 IS '二级行业代码';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.industry_code3 IS '三级行业代码';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.requires_special_gpa_handling IS '是否需要GPA特殊处理';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.gpa_calculation_method IS 'GPA计算方法';
COMMENT ON COLUMN pgs_factors.pit_industry_classification.snapshot_version IS '快照版本号';

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_pit_industry_optimized_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_pit_industry_optimized_updated_at
    BEFORE UPDATE ON pgs_factors.pit_industry_classification
    FOR EACH ROW
    EXECUTE FUNCTION update_pit_industry_optimized_updated_at();

-- 创建PIT查询函数 (优化版)
CREATE OR REPLACE FUNCTION get_industry_classification_pit_optimized(
    p_ts_code VARCHAR(20),
    p_as_of_date DATE,
    p_data_source VARCHAR(10) DEFAULT 'sw'
) RETURNS TABLE (
    ts_code VARCHAR(20),
    obs_date DATE,
    data_source VARCHAR(10),
    industry_level1 VARCHAR(50),
    industry_level2 VARCHAR(100),
    industry_level3 VARCHAR(150),
    industry_code1 VARCHAR(20),
    industry_code2 VARCHAR(20),
    industry_code3 VARCHAR(20),
    requires_special_gpa_handling BOOLEAN,
    gpa_calculation_method VARCHAR(50),
    special_handling_reason TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pic.ts_code,
        pic.obs_date,
        pic.data_source,
        pic.industry_level1,
        pic.industry_level2,
        pic.industry_level3,
        pic.industry_code1,
        pic.industry_code2,
        pic.industry_code3,
        pic.requires_special_gpa_handling,
        pic.gpa_calculation_method,
        pic.special_handling_reason
    FROM pgs_factors.pit_industry_classification pic
    WHERE pic.ts_code = p_ts_code
    AND pic.obs_date <= p_as_of_date
    AND pic.data_source = p_data_source
    ORDER BY pic.obs_date DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- 创建批量PIT查询函数
CREATE OR REPLACE FUNCTION get_industry_classification_batch_pit_optimized(
    p_ts_codes VARCHAR(20)[],
    p_as_of_date DATE,
    p_data_source VARCHAR(10) DEFAULT 'sw'
) RETURNS TABLE (
    ts_code VARCHAR(20),
    obs_date DATE,
    data_source VARCHAR(10),
    industry_level1 VARCHAR(50),
    industry_level2 VARCHAR(100),
    industry_level3 VARCHAR(150),
    requires_special_gpa_handling BOOLEAN,
    gpa_calculation_method VARCHAR(50),
    special_handling_reason TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH latest_industry AS (
        SELECT 
            pic.ts_code,
            pic.obs_date,
            pic.data_source,
            pic.industry_level1,
            pic.industry_level2,
            pic.industry_level3,
            pic.requires_special_gpa_handling,
            pic.gpa_calculation_method,
            pic.special_handling_reason,
            ROW_NUMBER() OVER (PARTITION BY pic.ts_code ORDER BY pic.obs_date DESC) as rn
        FROM pgs_factors.pit_industry_classification pic
        WHERE pic.ts_code = ANY(p_ts_codes)
        AND pic.obs_date <= p_as_of_date
        AND pic.data_source = p_data_source
    )
    SELECT 
        li.ts_code,
        li.obs_date,
        li.data_source,
        li.industry_level1,
        li.industry_level2,
        li.industry_level3,
        li.requires_special_gpa_handling,
        li.gpa_calculation_method,
        li.special_handling_reason
    FROM latest_industry li
    WHERE li.rn = 1
    ORDER BY li.ts_code;
END;
$$ LANGUAGE plpgsql;

-- 创建月末日期计算函数
CREATE OR REPLACE FUNCTION get_month_end_date(input_date DATE)
RETURNS DATE AS $$
BEGIN
    RETURN (DATE_TRUNC('month', input_date) + INTERVAL '1 month - 1 day')::DATE;
END;
$$ LANGUAGE plpgsql;

-- 创建快照版本生成函数
CREATE OR REPLACE FUNCTION generate_snapshot_version(snapshot_date DATE)
RETURNS VARCHAR(20) AS $$
BEGIN
    RETURN TO_CHAR(snapshot_date, 'YYYY-MM');
END;
$$ LANGUAGE plpgsql;
