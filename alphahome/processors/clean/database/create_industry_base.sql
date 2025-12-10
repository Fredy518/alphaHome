-- Clean Layer: industry_base 表 DDL
-- 行业指数基础数据表
--
-- 主键: (trade_date, ts_code)
-- 分区: 按 trade_date 月分区（可选，大数据量时启用）
--
-- 版本: 1.0
-- 创建日期: 2025-12-10

-- 确保 clean schema 存在
CREATE SCHEMA IF NOT EXISTS clean;

-- 创建 industry_base 表
CREATE TABLE IF NOT EXISTS clean.industry_base (
    -- 主键列
    trade_date INTEGER NOT NULL,           -- 交易日期 (YYYYMMDD)
    ts_code VARCHAR(20) NOT NULL,          -- 行业代码 (如 801010.SI)
    
    -- 业务数据列
    close FLOAT,                           -- 行业指数收盘价 (点)
    
    -- 血缘追踪列
    _source_table VARCHAR(255) NOT NULL,   -- 源表名（多个用逗号分隔）
    _processed_at TIMESTAMP NOT NULL,      -- 处理时间 (UTC)
    _data_version VARCHAR(50) NOT NULL,    -- 数据版本
    _ingest_job_id VARCHAR(100) NOT NULL,  -- 任务执行ID
    _validation_flag INTEGER DEFAULT 0,    -- 校验标记 (0=正常)
    
    -- 主键约束
    CONSTRAINT industry_base_pk PRIMARY KEY (trade_date, ts_code)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_industry_base_ts_code 
    ON clean.industry_base (ts_code);

CREATE INDEX IF NOT EXISTS idx_industry_base_processed_at 
    ON clean.industry_base (_processed_at);

-- 添加列注释
COMMENT ON TABLE clean.industry_base IS '行业指数基础数据表 - Clean Layer';
COMMENT ON COLUMN clean.industry_base.trade_date IS '交易日期 (YYYYMMDD格式)';
COMMENT ON COLUMN clean.industry_base.ts_code IS '行业代码 (如 801010.SI)';
COMMENT ON COLUMN clean.industry_base.close IS '行业指数收盘价 (点)';
COMMENT ON COLUMN clean.industry_base._source_table IS '源表名（多个用逗号分隔）';
COMMENT ON COLUMN clean.industry_base._processed_at IS '处理时间 (UTC)';
COMMENT ON COLUMN clean.industry_base._data_version IS '数据版本';
COMMENT ON COLUMN clean.industry_base._ingest_job_id IS '任务执行ID';
COMMENT ON COLUMN clean.industry_base._validation_flag IS '校验标记 (0=正常)';
