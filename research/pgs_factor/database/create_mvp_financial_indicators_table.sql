-- =====================================================
-- MVP版本财务指标预计算表
-- =====================================================
-- 
-- 目的: 快速验证预计算思路，实现2-4倍性能提升
-- 包含: 13个核心财务指标，基于现有PIT字段
-- 预期: 300-500只/秒 (vs 当前133只/秒)
-- 
-- Author: AI Assistant
-- Date: 2025-08-10
-- =====================================================

-- 创建MVP版本财务指标预计算表
CREATE TABLE IF NOT EXISTS pgs_factors.pit_financial_indicators_mvp (
    -- ===========================================
    -- 基础字段
    -- ===========================================
    ts_code VARCHAR(20) NOT NULL,                    -- 股票代码
    end_date DATE NOT NULL,                          -- 财报期末日期
    ann_date DATE NOT NULL,                          -- 公告日期 (真正的PIT时点)
    data_source VARCHAR(20) NOT NULL,                -- 数据来源 (report/express/forecast)
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 更新时间
    
    -- ===========================================
    -- 核心P因子指标 (TTM) - 必需
    -- ===========================================
    gpa_ttm DECIMAL(15,4),                          -- TTM毛利率 % (扩大精度容纳异常值)
    roe_excl_ttm DECIMAL(15,4),                     -- TTM净资产收益率 % (扩大精度容纳异常值)
    roa_excl_ttm DECIMAL(15,4),                     -- TTM总资产收益率 % (扩大精度容纳异常值)
    
    -- ===========================================
    -- 补充盈利能力指标 (TTM)
    -- ===========================================
    net_margin_ttm DECIMAL(15,4),                   -- TTM净利率 % (扩大精度容纳异常值)
    operating_margin_ttm DECIMAL(15,4),             -- TTM营业利润率 % (扩大精度容纳异常值)
    roi_ttm DECIMAL(15,4),                          -- TTM投资回报率 % (扩大精度容纳异常值)
    
    -- ===========================================
    -- 运营效率指标
    -- ===========================================
    asset_turnover_ttm DECIMAL(15,4),               -- TTM资产周转率 (扩大精度容纳异常值)
    equity_multiplier DECIMAL(15,4),                -- 权益乘数 (扩大精度容纳异常值)
    
    -- ===========================================
    -- 财务结构指标
    -- ===========================================
    debt_to_asset_ratio DECIMAL(15,4),              -- 资产负债率 % (扩大精度容纳异常值)
    equity_ratio DECIMAL(15,4),                     -- 权益比率 % (扩大精度容纳异常值)
    
    -- ===========================================
    -- 成长性指标
    -- ===========================================
    revenue_yoy_growth DECIMAL(15,4),               -- 营业收入同比增长率 % (扩大精度容纳异常值)
    n_income_yoy_growth DECIMAL(15,4),              -- 净利润同比增长率 % (扩大精度容纳异常值)
    operate_profit_yoy_growth DECIMAL(15,4),        -- 营业利润同比增长率 % (扩大精度容纳异常值)
    
    -- ===========================================
    -- 数据质量字段
    -- ===========================================
    data_quality VARCHAR(20) DEFAULT 'normal',      -- 数据质量 (high/normal/low)
    calculation_status VARCHAR(20) DEFAULT 'success', -- 计算状态
    
    -- 主键约束 (PIT原则：股票+财报期+公告日期唯一确定一条记录)
    PRIMARY KEY (ts_code, end_date, ann_date)
);

-- =====================================================
-- 创建索引 (针对查询模式优化)
-- =====================================================

-- 主要查询索引: P因子计算的主要查询模式
CREATE INDEX IF NOT EXISTS idx_mvp_indicators_ts_ann
ON pgs_factors.pit_financial_indicators_mvp (ts_code, ann_date);

-- 时间范围查询索引: 批量计算时的日期过滤
CREATE INDEX IF NOT EXISTS idx_mvp_indicators_ann_date
ON pgs_factors.pit_financial_indicators_mvp (ann_date);

-- 数据质量查询索引: 过滤高质量数据
CREATE INDEX IF NOT EXISTS idx_mvp_indicators_quality
ON pgs_factors.pit_financial_indicators_mvp (data_quality, ann_date);

-- 复合查询索引: 最常用的查询组合
CREATE INDEX IF NOT EXISTS idx_mvp_indicators_composite
ON pgs_factors.pit_financial_indicators_mvp (ts_code, ann_date, data_quality);

-- =====================================================
-- 表和字段注释
-- =====================================================

COMMENT ON TABLE pgs_factors.pit_financial_indicators_mvp IS 
'MVP版本财务指标预计算表 - 快速验证预计算思路，实现2-4倍性能提升';

-- 基础字段注释
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.ts_code IS '股票代码';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.end_date IS '财报期末日期';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.ann_date IS '公告日期(真正的PIT时点)';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.data_source IS '数据来源(report/express/forecast)';

-- 核心P因子指标注释
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.gpa_ttm IS 'TTM毛利率(%) = (revenue-oper_cost)/revenue*100';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.roe_excl_ttm IS 'TTM净资产收益率(%) = n_income_attr_p/tot_equity*100';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.roa_excl_ttm IS 'TTM总资产收益率(%) = n_income_attr_p/tot_assets*100';

-- 补充指标注释
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.net_margin_ttm IS 'TTM净利率(%) = n_income_attr_p/revenue*100';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.operating_margin_ttm IS 'TTM营业利润率(%) = operate_profit/revenue*100';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.roi_ttm IS 'TTM投资回报率(%) = operate_profit/tot_assets*100';

-- 运营效率指标注释
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.asset_turnover_ttm IS 'TTM资产周转率 = revenue_ttm/tot_assets';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.equity_multiplier IS '权益乘数 = tot_assets/tot_equity';

-- 财务结构指标注释
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.debt_to_asset_ratio IS '资产负债率(%) = (tot_assets-tot_equity)/tot_assets*100';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.equity_ratio IS '权益比率(%) = tot_equity/tot_assets*100';

-- 成长性指标注释
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.revenue_yoy_growth IS '营业收入同比增长率(%)';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.n_income_yoy_growth IS '净利润同比增长率(%)';
COMMENT ON COLUMN pgs_factors.pit_financial_indicators_mvp.operate_profit_yoy_growth IS '营业利润同比增长率(%)';

-- =====================================================
-- 数据验证约束 (可选)
-- =====================================================

-- 添加合理性检查约束 (如果不存在)
DO $$
BEGIN
    -- 移除数值范围约束，改为应用层质量控制
    -- 原因：异常数据应该保存到数据库，通过data_quality字段标注

    -- 添加数据质量约束
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'pit_financial_indicators_mvp'
        AND constraint_name = 'chk_data_quality'
    ) THEN
        ALTER TABLE pgs_factors.pit_financial_indicators_mvp
        ADD CONSTRAINT chk_data_quality CHECK (data_quality IN ('high', 'normal', 'low', 'outlier_high', 'outlier_low', 'invalid'));
    END IF;

    -- 添加计算状态约束
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'pit_financial_indicators_mvp'
        AND constraint_name = 'chk_calc_status'
    ) THEN
        ALTER TABLE pgs_factors.pit_financial_indicators_mvp
        ADD CONSTRAINT chk_calc_status CHECK (calculation_status IN ('success', 'failed', 'partial'));
    END IF;
END $$;

-- =====================================================
-- 性能优化设置
-- =====================================================

-- 设置表的统计信息收集
-- ALTER TABLE pgs_factors.pit_financial_indicators_mvp SET (autovacuum_analyze_scale_factor = 0.05);

-- =====================================================
-- 权限设置 (根据实际需要调整)
-- =====================================================

-- 授予读写权限给相关用户
-- GRANT SELECT, INSERT, UPDATE, DELETE ON pgs_factors.pit_financial_indicators_mvp TO pgs_user;
-- GRANT USAGE ON SCHEMA pgs_factors TO pgs_user;

-- =====================================================
-- 使用说明
-- =====================================================

/*
MVP版本使用说明:

1. 表结构特点:
   - 包含13个核心财务指标
   - 基于现有PIT字段，无需额外数据
   - 针对P因子计算优化的索引

2. 主要用途:
   - 替代实时TTM计算，提升P因子计算性能
   - 为后续PGS因子体系奠定基础
   - 验证预计算思路的可行性

3. 查询示例:
   -- 获取指定日期的财务指标
   SELECT * FROM pgs_factors.pit_financial_indicators_mvp 
   WHERE calc_date = '2025-02-28' AND ts_code = '000001.SZ';
   
   -- 批量获取多只股票的指标
   SELECT ts_code, gpa_ttm, roe_excl_ttm, roa_excl_ttm 
   FROM pgs_factors.pit_financial_indicators_mvp 
   WHERE calc_date = '2025-02-28' AND ts_code = ANY(ARRAY['000001.SZ', '000002.SZ']);

4. 性能预期:
   - 当前P因子计算: 133只/秒
   - MVP预计算版本: 300-500只/秒 (2-4倍提升)
   - 大规模计算: 5000只股票历史数据从8.5小时缩短到2-3小时

5. 后续扩展:
   - V2版本: 添加现金流指标
   - V3版本: 支持更多PGS因子
   - 生产版本: 自动更新和监控机制
*/
