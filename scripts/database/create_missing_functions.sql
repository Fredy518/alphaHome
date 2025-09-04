-- 创建缺失的数据库函数
-- 用于P因子计算的交易股票查询函数

-- 创建交易股票查询函数
CREATE OR REPLACE FUNCTION get_trading_stocks_optimized(
    p_calc_date DATE
) RETURNS TABLE (
    ts_code VARCHAR(20),
    name VARCHAR(100),
    list_date DATE,
    delist_date DATE
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        sb.ts_code,
        sb.name,
        sb.list_date,
        sb.delist_date
    FROM tushare.stock_basic sb
    WHERE sb.list_date <= p_calc_date
      AND (sb.delist_date IS NULL OR sb.delist_date > p_calc_date)
      AND sb.ts_code NOT LIKE '%.BJ'  -- 排除北交所股票（如果需要）
    ORDER BY sb.ts_code;
END;
$$ LANGUAGE plpgsql;

-- 创建行业分类批量PIT查询函数（如果还没有的话）
CREATE OR REPLACE FUNCTION get_industry_classification_batch_pit_optimized(
    p_ts_codes TEXT[],
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

-- 创建一个简化版本的行业分类函数（如果原始表不存在）
CREATE OR REPLACE FUNCTION get_industry_classification_batch_pit_optimized_fallback(
    p_ts_codes TEXT[],
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
    -- 返回空结果，P因子计算器会处理空结果的情况
    RETURN QUERY
    SELECT
        unnest(p_ts_codes)::VARCHAR(20) as ts_code,
        p_as_of_date as obs_date,
        p_data_source as data_source,
        '其他'::VARCHAR(50) as industry_level1,
        '其他'::VARCHAR(100) as industry_level2,
        '其他'::VARCHAR(150) as industry_level3,
        false as requires_special_gpa_handling,
        'standard'::VARCHAR(50) as gpa_calculation_method,
        'fallback: no industry data'::TEXT as special_handling_reason;
END;
$$ LANGUAGE plpgsql;
