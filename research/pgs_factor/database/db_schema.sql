-- P/G/S因子数据库表结构
-- 创建schema
CREATE SCHEMA IF NOT EXISTS pgs_factors;

-- ========================================
-- P因子主表
-- ========================================
DROP TABLE IF EXISTS pgs_factors.p_factor CASCADE;
CREATE TABLE pgs_factors.p_factor (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(10) NOT NULL,
    calc_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    data_source VARCHAR(20) NOT NULL, -- 'report'/'express'/'forecast'
    
    -- 新版P因子子因子与排名
    gpa FLOAT,                -- GP/A 原始值（%）
    roa_excl FLOAT,           -- 扣非ROA 原始值（%）
    roe_excl FLOAT,           -- 扣非ROE 原始值（%）
    rank_gpa FLOAT,           -- 0-100 百分位
    rank_roa FLOAT,           -- 0-100 百分位
    rank_roe FLOAT,           -- 0-100 百分位
    p_score FLOAT,            -- (rank_gpa+rank_roa+rank_roe)/3
    
    -- 元数据
    confidence FLOAT,
    data_quality VARCHAR(10), -- 'high'/'medium'/'low'
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束
    UNIQUE(ts_code, calc_date, data_source)
);

-- 创建索引
CREATE INDEX idx_p_factor_stock_date ON pgs_factors.p_factor(ts_code, calc_date);
CREATE INDEX idx_p_factor_ann_date ON pgs_factors.p_factor(ann_date);
CREATE INDEX idx_p_factor_source ON pgs_factors.p_factor(data_source);
CREATE INDEX idx_p_factor_ranks ON pgs_factors.p_factor(calc_date, rank_gpa, rank_roa, rank_roe);
CREATE INDEX idx_p_factor_created ON pgs_factors.p_factor(created_at);

-- ========================================
-- G因子表 (基于README.md规范的完整结构)
-- ========================================
DROP TABLE IF EXISTS pgs_factors.g_factor CASCADE;
CREATE TABLE pgs_factors.g_factor (
    -- 主键字段
    ts_code VARCHAR(20) NOT NULL,                    -- 股票代码
    calc_date DATE NOT NULL,                         -- 计算日期 (PIT截止日期)

    -- PIT时间轴字段
    ann_date DATE NOT NULL,                          -- 真实公告日期 (保持不变)
    data_source VARCHAR(20) NOT NULL,                -- 原始数据源标识 (express/forecast/report)

    -- G因子四个子因子原始值
    g_efficiency_surprise DECIMAL(15,6),             -- 效率惊喜因子
    g_efficiency_momentum DECIMAL(15,6),             -- 效率动量因子
    g_revenue_momentum DECIMAL(15,6),                -- 营收动量因子
    g_profit_momentum DECIMAL(15,6),                 -- 利润动量因子

    -- G因子四个子因子排名 (0-100百分位)
    rank_es DECIMAL(15,6),                           -- 效率惊喜排名 (0-100)
    rank_em DECIMAL(15,6),                           -- 效率动量排名 (0-100)
    rank_rm DECIMAL(15,6),                           -- 营收动量排名 (0-100)
    rank_pm DECIMAL(15,6),                           -- 利润动量排名 (0-100)

    -- G因子综合评分
    g_score DECIMAL(15,6),                           -- G因子综合评分 (0-100)

    -- 时效性权重系统
    data_timeliness_weight DECIMAL(15,6),            -- 数据时效性权重 (express:1.2, forecast:1.1, report:1.0)

    -- 数据质量和状态
    calculation_status VARCHAR(20) DEFAULT 'success', -- 计算状态 (success/failed/partial)

    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 主键约束
    CONSTRAINT pk_g_factor PRIMARY KEY (ts_code, calc_date),

    -- 检查约束
    CONSTRAINT chk_g_factor_calc_status CHECK (calculation_status IN ('success', 'failed', 'partial')),
    CONSTRAINT chk_g_factor_data_source CHECK (data_source IN ('express', 'forecast', 'report')),
    CONSTRAINT chk_g_factor_pit_logic CHECK (ann_date <= calc_date) -- PIT原则: 公告日期不能晚于计算日期
);

-- 创建索引
CREATE INDEX idx_g_factor_ann_date ON pgs_factors.g_factor (ann_date);
CREATE INDEX idx_g_factor_calc_date ON pgs_factors.g_factor (calc_date);
CREATE INDEX idx_g_factor_g_score ON pgs_factors.g_factor (g_score DESC);
CREATE INDEX idx_g_factor_data_source ON pgs_factors.g_factor (data_source);
CREATE INDEX idx_g_factor_timeliness_weight ON pgs_factors.g_factor (data_timeliness_weight);

-- 复合索引
CREATE INDEX idx_g_factor_calc_date_g_score ON pgs_factors.g_factor (calc_date, g_score DESC);
CREATE INDEX idx_g_factor_ts_code_calc_date ON pgs_factors.g_factor (ts_code, calc_date DESC);
CREATE INDEX idx_g_factor_data_source_calc_date ON pgs_factors.g_factor (data_source, calc_date);

-- ========================================
-- S因子表
-- ========================================
DROP TABLE IF EXISTS pgs_factors.s_factor CASCADE;
CREATE TABLE pgs_factors.s_factor (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(10) NOT NULL,
    calc_date DATE NOT NULL,
    
    -- S因子核心指标
    s_score FLOAT,
    debt_ratio FLOAT,
    beta FLOAT,
    roe_volatility FLOAT,
    
    -- 元数据
    data_quality VARCHAR(10),
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束
    UNIQUE(ts_code, calc_date)
);

-- 创建索引
CREATE INDEX idx_s_factor_stock_date ON pgs_factors.s_factor(ts_code, calc_date);
CREATE INDEX idx_s_factor_score ON pgs_factors.s_factor(s_score);

-- ========================================
-- 数据质量监控表
-- ========================================
DROP TABLE IF EXISTS pgs_factors.quality_metrics CASCADE;
CREATE TABLE pgs_factors.quality_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    metric_value FLOAT,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_quality_date ON pgs_factors.quality_metrics(metric_date);
CREATE INDEX idx_quality_type ON pgs_factors.quality_metrics(metric_type);

-- ========================================
-- 处理进度表
-- ========================================
DROP TABLE IF EXISTS pgs_factors.processing_log CASCADE;
CREATE TABLE pgs_factors.processing_log (
    id SERIAL PRIMARY KEY,
    process_type VARCHAR(50) NOT NULL, -- 'p_factor'/'g_factor'/'s_factor'
    last_processed_date TIMESTAMP,
    last_processed_ann_date DATE,
    records_processed INT,
    status VARCHAR(20), -- 'success'/'failed'/'running'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- 综合因子视图
-- ========================================
CREATE OR REPLACE VIEW pgs_factors.factor_summary AS
WITH latest_p AS (
    SELECT DISTINCT ON (ts_code, calc_date) 
        ts_code, calc_date, p_score, data_source, confidence
    FROM pgs_factors.p_factor
    ORDER BY ts_code, calc_date, 
        CASE data_source 
            WHEN 'report' THEN 1 
            WHEN 'express' THEN 2 
            WHEN 'forecast' THEN 3 
        END
)
SELECT 
    COALESCE(p.ts_code, g.ts_code, s.ts_code) as ts_code,
    COALESCE(p.calc_date, g.calc_date, s.calc_date) as calc_date,
    p.p_score,
    p.data_source as p_source,
    p.confidence as p_confidence,
    g.g_score,
    g.data_periods as g_data_periods,
    s.s_score,
    -- 输出原始分，综合得分将由应用层基于Z-Score计算，避免DB与应用侧口径不一致
    NULL::FLOAT as total_score
FROM latest_p p
FULL OUTER JOIN pgs_factors.g_factor g 
    ON p.ts_code = g.ts_code AND p.calc_date = g.calc_date
FULL OUTER JOIN pgs_factors.s_factor s
    ON COALESCE(p.ts_code, g.ts_code) = s.ts_code 
    AND COALESCE(p.calc_date, g.calc_date) = s.calc_date;

-- ========================================
-- G子因子明细表（供高级G因子批处理使用）
-- ========================================
CREATE TABLE IF NOT EXISTS pgs_factors.g_subfactors (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(10) NOT NULL,
    calc_date DATE NOT NULL,
    factor_name VARCHAR(64) NOT NULL,
    factor_value FLOAT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, calc_date, factor_name)
);

-- ========================================
-- PIT单季度利润表快照（测试/持久化用）
-- ========================================
CREATE TABLE IF NOT EXISTS pgs_factors.pit_income_quarterly (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(10) NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    data_source VARCHAR(20), -- report/express/forecast
    year INT,
    quarter INT,
    -- 单季度口径值（已从累计值转换）
    n_income_attr_p FLOAT,
    -- 新增：单季度净利润（总口径，不区分归母/扣非；用于YTD累加与预告拆分）
    n_income FLOAT,
    revenue FLOAT,
    operate_profit FLOAT,
    total_profit FLOAT,
    income_tax FLOAT,
    -- 新增：成本/扣非/财务费用/预告中值（单季化）
    oper_cost FLOAT,
    total_cogs FLOAT,

    fin_exp FLOAT,
    interest_expense FLOAT,
    net_profit_mid FLOAT,
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, end_date, ann_date)
);

CREATE INDEX IF NOT EXISTS idx_pit_income_q_stock ON pgs_factors.pit_income_quarterly(ts_code);
CREATE INDEX IF NOT EXISTS idx_pit_income_q_end ON pgs_factors.pit_income_quarterly(end_date);
CREATE INDEX IF NOT EXISTS idx_pit_income_q_ann ON pgs_factors.pit_income_quarterly(ann_date);

-- 兼容旧表：补充缺失字段
ALTER TABLE pgs_factors.pit_income_quarterly 
    ADD COLUMN IF NOT EXISTS data_source VARCHAR(20);
ALTER TABLE pgs_factors.pit_income_quarterly 
    ADD COLUMN IF NOT EXISTS n_income FLOAT,
    ADD COLUMN IF NOT EXISTS oper_cost FLOAT,
    ADD COLUMN IF NOT EXISTS total_cogs FLOAT,

    ADD COLUMN IF NOT EXISTS fin_exp FLOAT,
    ADD COLUMN IF NOT EXISTS interest_expense FLOAT,
    ADD COLUMN IF NOT EXISTS net_profit_mid FLOAT;

-- ========================================
-- PIT资产负债表快照（测试/持久化用）
-- ========================================
CREATE TABLE IF NOT EXISTS pgs_factors.pit_balance_quarterly (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(10) NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    data_source VARCHAR(20), -- report
    year INT,
    quarter INT,
    -- 时点口径
    tot_assets FLOAT,
    tot_liab FLOAT,
    tot_equity FLOAT,
    -- 新增：流动性相关
    total_cur_assets FLOAT,
    total_cur_liab FLOAT,
    inventories FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, end_date, ann_date)
);

CREATE INDEX IF NOT EXISTS idx_pit_balance_q_stock ON pgs_factors.pit_balance_quarterly(ts_code);
CREATE INDEX IF NOT EXISTS idx_pit_balance_q_end ON pgs_factors.pit_balance_quarterly(end_date);
CREATE INDEX IF NOT EXISTS idx_pit_balance_q_ann ON pgs_factors.pit_balance_quarterly(ann_date);

-- 兼容旧表：补充缺失字段
ALTER TABLE pgs_factors.pit_balance_quarterly 
    ADD COLUMN IF NOT EXISTS total_cur_assets FLOAT,
    ADD COLUMN IF NOT EXISTS total_cur_liab FLOAT,
    ADD COLUMN IF NOT EXISTS inventories FLOAT;

-- ========================================
-- 统一视图：单表查询收入+资产负债（按PIT原则就近对齐）
-- 对于 income 的每一行，关联同一 end_date、且 ann_date 不晚于该行的最近一条 balance
-- 注意：express/forecast 行通常无资产负债，将仅带入最近的 report 口径资产负债
-- ========================================
CREATE OR REPLACE VIEW pgs_factors.v_pit_financial_quarterly AS
SELECT
    i.ts_code,
    i.end_date,
    i.ann_date,
    i.data_source,
    i.year,
    i.quarter,
    -- income 单季字段
    i.n_income_attr_p,
    i.n_income,
    i.revenue,
    i.operate_profit,
    i.total_profit,
    i.income_tax,
    i.oper_cost,
    i.total_cogs,

    i.fin_exp,
    i.interest_expense,
    i.net_profit_mid,
    -- 对齐的资产负债时点字段（可为空）
    b.tot_assets,
    b.tot_liab,
    b.tot_equity,
    b.total_cur_assets,
    b.total_cur_liab,
    b.inventories
FROM pgs_factors.pit_income_quarterly i
LEFT JOIN LATERAL (
    SELECT 
        b1.tot_assets, 
        b1.tot_liab, 
        b1.tot_equity,
        b1.total_cur_assets,
        b1.total_cur_liab,
        b1.inventories
    FROM pgs_factors.pit_balance_quarterly b1
    WHERE b1.ts_code = i.ts_code
      AND b1.end_date = i.end_date
      AND b1.ann_date <= i.ann_date
    ORDER BY b1.ann_date DESC
    LIMIT 1
) b ON TRUE;
-- ========================================
-- 创建更新触发器
-- ========================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_p_factor_updated_at
    BEFORE UPDATE ON pgs_factors.p_factor
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_g_factor_updated_at
    BEFORE UPDATE ON pgs_factors.g_factor
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_processing_log_updated_at
    BEFORE UPDATE ON pgs_factors.processing_log
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 添加注释
COMMENT ON TABLE pgs_factors.p_factor IS 'P因子（盈利能力）数据表';
COMMENT ON TABLE pgs_factors.g_factor IS 'G因子（成长能力）数据表 - 基于P因子历史数据计算，集成时效性权重系统';
COMMENT ON TABLE pgs_factors.s_factor IS 'S因子（安全能力）数据表';
COMMENT ON TABLE pgs_factors.quality_metrics IS '数据质量监控指标表';
COMMENT ON TABLE pgs_factors.processing_log IS '数据处理日志表';
COMMENT ON VIEW pgs_factors.factor_summary IS 'P/G/S因子综合视图';

-- G因子表字段注释
COMMENT ON COLUMN pgs_factors.g_factor.ts_code IS '股票代码';
COMMENT ON COLUMN pgs_factors.g_factor.calc_date IS '计算日期 - PIT截止日期';
COMMENT ON COLUMN pgs_factors.g_factor.ann_date IS '真实公告日期 - 来源P因子数据的公告时间';
COMMENT ON COLUMN pgs_factors.g_factor.data_source IS '原始数据源标识 - express/forecast/report';
COMMENT ON COLUMN pgs_factors.g_factor.g_efficiency_surprise IS '效率惊喜因子 - (ΔP_score_YoY / Std(ΔP_score_YoY)) * Timeliness_Weight';
COMMENT ON COLUMN pgs_factors.g_factor.g_efficiency_momentum IS '效率动量因子 - ΔP_score_YoY * Timeliness_Weight';
COMMENT ON COLUMN pgs_factors.g_factor.g_revenue_momentum IS '营收动量因子 - Revenue_Growth_TTM * Timeliness_Weight';
COMMENT ON COLUMN pgs_factors.g_factor.g_profit_momentum IS '利润动量因子 - Profit_Growth_TTM * Timeliness_Weight';
COMMENT ON COLUMN pgs_factors.g_factor.rank_es IS '效率惊喜排名 - 0-100百分位';
COMMENT ON COLUMN pgs_factors.g_factor.rank_em IS '效率动量排名 - 0-100百分位';
COMMENT ON COLUMN pgs_factors.g_factor.rank_rm IS '营收动量排名 - 0-100百分位';
COMMENT ON COLUMN pgs_factors.g_factor.rank_pm IS '利润动量排名 - 0-100百分位';
COMMENT ON COLUMN pgs_factors.g_factor.g_score IS 'G因子综合评分 - 0.25×Rank_ES + 0.25×Rank_EM + 0.25×Rank_RM + 0.25×Rank_PM';
COMMENT ON COLUMN pgs_factors.g_factor.data_timeliness_weight IS '数据时效性权重 - Express:1.2, Forecast:1.1, Report:1.0';
COMMENT ON COLUMN pgs_factors.g_factor.calculation_status IS '计算状态 - success/failed/partial';
