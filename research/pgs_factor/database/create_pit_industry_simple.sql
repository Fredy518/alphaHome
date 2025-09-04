-- PIT行业分类表 (简化版)
DROP TABLE IF EXISTS pgs_factors.pit_industry_classification CASCADE;

CREATE TABLE pgs_factors.pit_industry_classification (
    -- 主键字段
    ts_code VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    
    -- 行业分类信息
    industry_level1 VARCHAR(50),
    industry_level2 VARCHAR(100),
    industry_level3 VARCHAR(150),
    
    -- 标准行业代码
    sw_industry_code VARCHAR(20),
    sw_industry_name VARCHAR(100),
    csrc_industry_code VARCHAR(20),
    csrc_industry_name VARCHAR(100),
    
    -- 特殊处理标识
    requires_special_gpa_handling BOOLEAN DEFAULT FALSE,
    gpa_calculation_method VARCHAR(50) DEFAULT 'standard',
    special_handling_reason TEXT,
    
    -- 数据来源和质量
    data_source VARCHAR(50) DEFAULT 'tushare',
    data_quality VARCHAR(20) DEFAULT 'normal',
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 主键约束
    CONSTRAINT pk_pit_industry_classification PRIMARY KEY (ts_code, effective_date),
    
    -- 检查约束
    CONSTRAINT chk_gpa_calculation_method CHECK (gpa_calculation_method IN ('standard', 'null', 'alternative', 'cost_income_ratio')),
    CONSTRAINT chk_data_quality CHECK (data_quality IN ('high', 'normal', 'low', 'invalid'))
);

-- 创建索引
CREATE INDEX idx_pit_industry_ts_code ON pgs_factors.pit_industry_classification (ts_code);
CREATE INDEX idx_pit_industry_effective_date ON pgs_factors.pit_industry_classification (effective_date);
CREATE INDEX idx_pit_industry_level1 ON pgs_factors.pit_industry_classification (industry_level1);
CREATE INDEX idx_pit_industry_level2 ON pgs_factors.pit_industry_classification (industry_level2);
CREATE INDEX idx_pit_industry_special_gpa ON pgs_factors.pit_industry_classification (requires_special_gpa_handling);
CREATE INDEX idx_pit_industry_ts_effective ON pgs_factors.pit_industry_classification (ts_code, effective_date DESC);
CREATE INDEX idx_pit_industry_date_level1 ON pgs_factors.pit_industry_classification (effective_date, industry_level1);

-- 插入银行业的特殊处理配置示例
INSERT INTO pgs_factors.pit_industry_classification (
    ts_code, effective_date, 
    industry_level1, industry_level2, industry_level3,
    sw_industry_name, 
    requires_special_gpa_handling, gpa_calculation_method, special_handling_reason
) VALUES 
-- 银行业示例数据
('000001.SZ', '2020-01-01', '金融业', '银行业', '商业银行', '银行', TRUE, 'null', '银行业营业成本为0导致GPA=100%，需要特殊处理'),
('600000.SH', '2020-01-01', '金融业', '银行业', '商业银行', '银行', TRUE, 'null', '银行业营业成本为0导致GPA=100%，需要特殊处理'),
('600036.SH', '2020-01-01', '金融业', '银行业', '商业银行', '银行', TRUE, 'null', '银行业营业成本为0导致GPA=100%，需要特殊处理'),

-- 证券业示例数据
('000166.SZ', '2020-01-01', '金融业', '证券业', '证券公司', '证券', TRUE, 'null', '证券业成本结构特殊，GPA指标不适用'),
('600030.SH', '2020-01-01', '金融业', '证券业', '证券公司', '证券', TRUE, 'null', '证券业成本结构特殊，GPA指标不适用'),

-- 保险业示例数据  
('601318.SH', '2020-01-01', '金融业', '保险业', '人寿保险', '保险', TRUE, 'null', '保险业成本结构特殊，GPA指标不适用'),
('601601.SH', '2020-01-01', '金融业', '保险业', '财产保险', '保险', TRUE, 'null', '保险业成本结构特殊，GPA指标不适用'),

-- 制造业示例数据 (标准处理)
('000002.SZ', '2020-01-01', '房地产业', '房地产开发', '住宅开发', '房地产开发', FALSE, 'standard', NULL),
('000858.SZ', '2020-01-01', '食品饮料', '白酒', '白酒制造', '白酒', FALSE, 'standard', NULL)

ON CONFLICT (ts_code, effective_date) DO NOTHING;
