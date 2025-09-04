-- P因子表 (遵循PIT原则)
-- 基于MVP财务指标预计算表的P因子计算结果

DROP TABLE IF EXISTS pgs_factors.p_factor CASCADE;

CREATE TABLE pgs_factors.p_factor (
    -- 主键字段
    ts_code VARCHAR(20) NOT NULL,                    -- 股票代码
    calc_date DATE NOT NULL,                         -- 计算日期 (PIT截止日期)
    
    -- PIT时间轴字段
    ann_date DATE NOT NULL,                          -- 真实公告日期 (保持不变)
    end_date DATE,                                   -- 财报期结束日期
    
    -- P因子核心指标
    p_score DECIMAL(15,6),                           -- P评分 (主要指标)
    p_rank INTEGER,                                  -- P排名
    
    -- 基础财务指标 (来自MVP预计算表)
    gpa DECIMAL(15,4),                               -- 毛利率TTM %
    roe_excl DECIMAL(15,4),                          -- 净资产收益率TTM %
    roa_excl DECIMAL(15,4),                          -- 总资产收益率TTM %
    net_margin_ttm DECIMAL(15,4),                    -- 净利率TTM %
    operating_margin_ttm DECIMAL(15,4),              -- 营业利润率TTM %
    roi_ttm DECIMAL(15,4),                           -- 投资回报率TTM %
    
    -- 效率指标
    asset_turnover_ttm DECIMAL(15,4),                -- 资产周转率TTM
    equity_multiplier DECIMAL(15,4),                 -- 权益乘数
    
    -- 财务结构指标
    debt_to_asset_ratio DECIMAL(15,4),               -- 资产负债率 %
    equity_ratio DECIMAL(15,4),                      -- 权益比率 %
    
    -- 成长性指标
    revenue_yoy_growth DECIMAL(15,4),                -- 营业收入同比增长率 %
    n_income_yoy_growth DECIMAL(15,4),               -- 净利润同比增长率 %
    operate_profit_yoy_growth DECIMAL(15,4),         -- 营业利润同比增长率 %
    
    -- 分项评分 (可选，用于详细分析)
    profitability_score DECIMAL(15,6),               -- 盈利能力评分
    efficiency_score DECIMAL(15,6),                  -- 运营效率评分
    leverage_score DECIMAL(15,6),                    -- 财务杠杆评分
    growth_score DECIMAL(15,6),                      -- 成长性评分
    
    -- 数据质量和来源
    data_source VARCHAR(50) DEFAULT 'mvp_precomputed', -- 数据来源
    data_quality VARCHAR(20) DEFAULT 'normal',       -- 数据质量: high, normal, outlier_high, outlier_low, low, invalid
    calculation_status VARCHAR(20) DEFAULT 'success', -- 计算状态: success, failed, partial
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 主键约束 (ts_code, calc_date)
    CONSTRAINT pk_p_factor PRIMARY KEY (ts_code, calc_date),
    
    -- 检查约束
    CONSTRAINT chk_p_factor_data_quality CHECK (data_quality IN ('high', 'normal', 'outlier_high', 'outlier_low', 'low', 'invalid')),
    CONSTRAINT chk_p_factor_calc_status CHECK (calculation_status IN ('success', 'failed', 'partial')),
    CONSTRAINT chk_p_factor_pit_logic CHECK (ann_date <= calc_date) -- PIT原则: 公告日期不能晚于计算日期
);

-- 创建索引
CREATE INDEX idx_p_factor_ann_date ON pgs_factors.p_factor (ann_date);
CREATE INDEX idx_p_factor_calc_date ON pgs_factors.p_factor (calc_date);
CREATE INDEX idx_p_factor_p_score ON pgs_factors.p_factor (p_score DESC);
CREATE INDEX idx_p_factor_data_quality ON pgs_factors.p_factor (data_quality);
CREATE INDEX idx_p_factor_end_date ON pgs_factors.p_factor (end_date);

-- 复合索引
CREATE INDEX idx_p_factor_calc_date_p_score ON pgs_factors.p_factor (calc_date, p_score DESC);
CREATE INDEX idx_p_factor_ts_code_calc_date ON pgs_factors.p_factor (ts_code, calc_date DESC);

-- 添加表注释
COMMENT ON TABLE pgs_factors.p_factor IS 'P因子计算结果表 - 基于MVP财务指标预计算表，严格遵循PIT原则';

-- 添加字段注释
COMMENT ON COLUMN pgs_factors.p_factor.ts_code IS '股票代码';
COMMENT ON COLUMN pgs_factors.p_factor.calc_date IS '计算日期 - PIT截止日期，在此时点能看到的所有已公告数据';
COMMENT ON COLUMN pgs_factors.p_factor.ann_date IS '真实公告日期 - 财务数据的实际公告时间，严格保持不变';
COMMENT ON COLUMN pgs_factors.p_factor.end_date IS '财报期结束日期';
COMMENT ON COLUMN pgs_factors.p_factor.p_score IS 'P评分 - 综合盈利能力评分，主要指标';
COMMENT ON COLUMN pgs_factors.p_factor.p_rank IS 'P排名 - 在同一计算日期下的排名';
COMMENT ON COLUMN pgs_factors.p_factor.data_quality IS '数据质量标识 - high/normal/outlier_high/outlier_low/low/invalid';
COMMENT ON COLUMN pgs_factors.p_factor.calculation_status IS '计算状态 - success/failed/partial';

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_p_factor_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_p_factor_updated_at
    BEFORE UPDATE ON pgs_factors.p_factor
    FOR EACH ROW
    EXECUTE FUNCTION update_p_factor_updated_at();

-- 创建分区 (按年份分区，提高查询性能)
-- 注意: 如果数据量很大，可以考虑按calc_date进行分区

-- 授权 (如果用户存在)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON pgs_factors.p_factor TO alphahome_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pgs_factors TO alphahome_user;

-- 显示表结构 (注释掉，因为这是psql命令)
-- \d pgs_factors.p_factor;
