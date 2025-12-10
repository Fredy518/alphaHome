-- Clean Layer: 完整 DDL 脚本
-- 创建 clean schema 及所有核心表
--
-- 执行环境约束：
-- - 仅在 dev/staging 环境执行
-- - 生产环境需 DBA 审批
--
-- 包含表：
-- - clean.index_valuation_base: 指数估值基础数据
-- - clean.index_volatility_base: 指数波动率基础数据
-- - clean.industry_base: 行业指数基础数据
-- - clean.money_flow_base: 市场资金流基础数据
-- - clean.market_technical_base: 市场技术指标基础数据
--
-- 版本: 1.0
-- 创建日期: 2025-12-10

-- ============================================================================
-- 1. 创建 clean schema
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS clean;
COMMENT ON SCHEMA clean IS 'Clean Layer: 存放已清洗、标准化的数据，作为研究和特征层的统一输入';

-- ============================================================================
-- 2. 创建 index_valuation_base 表
-- ============================================================================

CREATE TABLE IF NOT EXISTS clean.index_valuation_base (
    trade_date INTEGER NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    pe_ttm FLOAT,
    pb FLOAT,
    _source_table VARCHAR(255) NOT NULL,
    _processed_at TIMESTAMP NOT NULL,
    _data_version VARCHAR(50) NOT NULL,
    _ingest_job_id VARCHAR(100) NOT NULL,
    _validation_flag INTEGER DEFAULT 0,
    CONSTRAINT index_valuation_base_pk PRIMARY KEY (trade_date, ts_code)
);

CREATE INDEX IF NOT EXISTS idx_index_valuation_base_ts_code 
    ON clean.index_valuation_base (ts_code);
CREATE INDEX IF NOT EXISTS idx_index_valuation_base_processed_at 
    ON clean.index_valuation_base (_processed_at);

COMMENT ON TABLE clean.index_valuation_base IS '指数估值基础数据表 - Clean Layer';

-- ============================================================================
-- 3. 创建 index_volatility_base 表
-- ============================================================================

CREATE TABLE IF NOT EXISTS clean.index_volatility_base (
    trade_date INTEGER NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    close FLOAT,
    close_unadj FLOAT,
    _adj_method VARCHAR(20),
    _source_table VARCHAR(255) NOT NULL,
    _processed_at TIMESTAMP NOT NULL,
    _data_version VARCHAR(50) NOT NULL,
    _ingest_job_id VARCHAR(100) NOT NULL,
    _validation_flag INTEGER DEFAULT 0,
    CONSTRAINT index_volatility_base_pk PRIMARY KEY (trade_date, ts_code)
);

CREATE INDEX IF NOT EXISTS idx_index_volatility_base_ts_code 
    ON clean.index_volatility_base (ts_code);
CREATE INDEX IF NOT EXISTS idx_index_volatility_base_processed_at 
    ON clean.index_volatility_base (_processed_at);

COMMENT ON TABLE clean.index_volatility_base IS '指数波动率基础数据表 - Clean Layer';

-- ============================================================================
-- 4. 创建 industry_base 表
-- ============================================================================

CREATE TABLE IF NOT EXISTS clean.industry_base (
    trade_date INTEGER NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    close FLOAT,
    _source_table VARCHAR(255) NOT NULL,
    _processed_at TIMESTAMP NOT NULL,
    _data_version VARCHAR(50) NOT NULL,
    _ingest_job_id VARCHAR(100) NOT NULL,
    _validation_flag INTEGER DEFAULT 0,
    CONSTRAINT industry_base_pk PRIMARY KEY (trade_date, ts_code)
);

CREATE INDEX IF NOT EXISTS idx_industry_base_ts_code 
    ON clean.industry_base (ts_code);
CREATE INDEX IF NOT EXISTS idx_industry_base_processed_at 
    ON clean.industry_base (_processed_at);

COMMENT ON TABLE clean.industry_base IS '行业指数基础数据表 - Clean Layer';

-- ============================================================================
-- 5. 创建 money_flow_base 表
-- ============================================================================

CREATE TABLE IF NOT EXISTS clean.money_flow_base (
    trade_date INTEGER NOT NULL,
    total_net_mf_amount FLOAT,
    total_circ_mv FLOAT,
    _source_table VARCHAR(255) NOT NULL,
    _processed_at TIMESTAMP NOT NULL,
    _data_version VARCHAR(50) NOT NULL,
    _ingest_job_id VARCHAR(100) NOT NULL,
    _validation_flag INTEGER DEFAULT 0,
    CONSTRAINT money_flow_base_pk PRIMARY KEY (trade_date)
);

CREATE INDEX IF NOT EXISTS idx_money_flow_base_processed_at 
    ON clean.money_flow_base (_processed_at);

COMMENT ON TABLE clean.money_flow_base IS '市场资金流基础数据表 - Clean Layer';

-- ============================================================================
-- 6. 创建 market_technical_base 表
-- ============================================================================

CREATE TABLE IF NOT EXISTS clean.market_technical_base (
    trade_date INTEGER NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    close FLOAT,
    vol FLOAT,
    turnover_rate FLOAT,
    _source_table VARCHAR(255) NOT NULL,
    _processed_at TIMESTAMP NOT NULL,
    _data_version VARCHAR(50) NOT NULL,
    _ingest_job_id VARCHAR(100) NOT NULL,
    _validation_flag INTEGER DEFAULT 0,
    CONSTRAINT market_technical_base_pk PRIMARY KEY (trade_date, ts_code)
);

CREATE INDEX IF NOT EXISTS idx_market_technical_base_ts_code 
    ON clean.market_technical_base (ts_code);
CREATE INDEX IF NOT EXISTS idx_market_technical_base_processed_at 
    ON clean.market_technical_base (_processed_at);

COMMENT ON TABLE clean.market_technical_base IS '市场技术指标基础数据表 - Clean Layer';

-- ============================================================================
-- 完成
-- ============================================================================

-- 验证创建结果
SELECT 
    schemaname, 
    tablename 
FROM pg_tables 
WHERE schemaname = 'clean'
ORDER BY tablename;
