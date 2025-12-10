-- Clean Layer: index_volatility_base 表 DDL
-- 指数波动率基础数据表
--
-- 主键: (trade_date, ts_code)
-- 分区: 按 trade_date 月分区（可选，大数据量时启用）
--
-- 版本: 1.0
-- 创建日期: 2025-12-10

-- 确保 clean schema 存在
CREATE SCHEMA IF NOT EXISTS clean;

-- 创建 index_volatility_base 表
CREATE TABLE IF NOT EXISTS clean.index_volatility_base (
    -- 主键列
    trade_date INTEGER NOT NULL,           -- 交易日期 (YYYYMMDD)
    ts_code VARCHAR(20) NOT NULL,          -- 指数代码 (如 000001.SH)
    
    -- 业务数据列
    close FLOAT,                           -- 收盘价（复权）(元)
    close_unadj FLOAT,                     -- 未复权收盘价 (元)
    _adj_method VARCHAR(20),               -- 复权方式 (前复权/后复权/不复权)
    
    -- 血缘追踪列
    _source_table VARCHAR(255) NOT NULL,   -- 源表名（多个用逗号分隔）
    _processed_at TIMESTAMP NOT NULL,      -- 处理时间 (UTC)
    _data_version VARCHAR(50) NOT NULL,    -- 数据版本
    _ingest_job_id VARCHAR(100) NOT NULL,  -- 任务执行ID
    _validation_flag INTEGER DEFAULT 0,    -- 校验标记 (0=正常)
    
    -- 主键约束
    CONSTRAINT index_volatility_base_pk PRIMARY KEY (trade_date, ts_code)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_index_volatility_base_ts_code 
    ON clean.index_volatility_base (ts_code);

CREATE INDEX IF NOT EXISTS idx_index_volatility_base_processed_at 
    ON clean.index_volatility_base (_processed_at);

-- 添加列注释
COMMENT ON TABLE clean.index_volatility_base IS '指数波动率基础数据表 - Clean Layer';
COMMENT ON COLUMN clean.index_volatility_base.trade_date IS '交易日期 (YYYYMMDD格式)';
COMMENT ON COLUMN clean.index_volatility_base.ts_code IS '指数代码 (如 000001.SH)';
COMMENT ON COLUMN clean.index_volatility_base.close IS '收盘价（复权）(元)';
COMMENT ON COLUMN clean.index_volatility_base.close_unadj IS '未复权收盘价 (元)';
COMMENT ON COLUMN clean.index_volatility_base._adj_method IS '复权方式 (前复权/后复权/不复权)';
COMMENT ON COLUMN clean.index_volatility_base._source_table IS '源表名（多个用逗号分隔）';
COMMENT ON COLUMN clean.index_volatility_base._processed_at IS '处理时间 (UTC)';
COMMENT ON COLUMN clean.index_volatility_base._data_version IS '数据版本';
COMMENT ON COLUMN clean.index_volatility_base._ingest_job_id IS '任务执行ID';
COMMENT ON COLUMN clean.index_volatility_base._validation_flag IS '校验标记 (0=正常)';
