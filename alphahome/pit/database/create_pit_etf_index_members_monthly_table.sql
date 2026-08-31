CREATE SCHEMA IF NOT EXISTS pit;

CREATE TABLE IF NOT EXISTS pit.pit_etf_index_members_monthly (
    obs_date                  date         NOT NULL,
    index_code                varchar(24)  NOT NULL,
    index_name                varchar(160) NOT NULL,
    ts_code                   varchar(20)  NOT NULL,
    weight                    numeric(20,12) NOT NULL,
    raw_weight                numeric(20,8)  NOT NULL,
    weight_basis              varchar(40)  NOT NULL,
    weight_source             varchar(80)  NOT NULL,
    source_code               varchar(24)  NOT NULL,
    source_effective_date     date         NOT NULL,
    source_available_date     date         NOT NULL,
    source_staleness_days     integer      NOT NULL,
    source_member_count       integer      NOT NULL,
    source_weight_sum         numeric(20,8) NOT NULL,
    source_coverage_rate      numeric(20,10) NOT NULL,
    source_quality            varchar(16)  NOT NULL,
    is_fallback               boolean      NOT NULL DEFAULT false,
    constituent_scope         varchar(64)  NOT NULL DEFAULT 'full_index',
    is_proxy                  boolean      NOT NULL DEFAULT false,
    scope_weight_rate         numeric(20,10) NOT NULL DEFAULT 1,
    is_eligible               boolean      NOT NULL DEFAULT false,
    quality_reasons           jsonb        NOT NULL DEFAULT '[]'::jsonb,
    method_version            varchar(80)  NOT NULL,
    created_at                timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pit_etf_index_members_monthly_pk PRIMARY KEY (
        obs_date,
        index_code,
        ts_code,
        method_version
    ),
    CONSTRAINT pit_etf_index_members_monthly_weight_ck
        CHECK (weight > 0 AND weight <= 1),
    CONSTRAINT pit_etf_index_members_monthly_basis_ck
        CHECK (weight_basis IN ('official_index_weight', 'etf_disclosed_holding')),
    CONSTRAINT pit_etf_index_members_monthly_quality_ck
        CHECK (source_quality IN ('high', 'partial', 'low')),
    CONSTRAINT pit_etf_index_members_monthly_effective_date_ck
        CHECK (source_effective_date <= obs_date),
    CONSTRAINT pit_etf_index_members_monthly_available_date_ck
        CHECK (source_available_date <= obs_date),
    CONSTRAINT pit_etf_index_members_monthly_staleness_ck
        CHECK (source_staleness_days >= 0),
    CONSTRAINT pit_etf_index_members_monthly_coverage_ck
        CHECK (source_coverage_rate > 0 AND source_coverage_rate <= 1.10),
    CONSTRAINT pit_etf_index_members_monthly_scope_weight_ck
        CHECK (scope_weight_rate > 0 AND scope_weight_rate <= 1.000001)
);

ALTER TABLE pit.pit_etf_index_members_monthly
    ADD COLUMN IF NOT EXISTS constituent_scope varchar(64) NOT NULL DEFAULT 'full_index';

ALTER TABLE pit.pit_etf_index_members_monthly
    ADD COLUMN IF NOT EXISTS is_proxy boolean NOT NULL DEFAULT false;

ALTER TABLE pit.pit_etf_index_members_monthly
    ADD COLUMN IF NOT EXISTS scope_weight_rate numeric(20,10) NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_pit_etf_index_members_obs_date
    ON pit.pit_etf_index_members_monthly (obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_etf_index_members_index_date
    ON pit.pit_etf_index_members_monthly (index_code, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_etf_index_members_eligible_date
    ON pit.pit_etf_index_members_monthly (is_eligible, obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_etf_index_members_source_date
    ON pit.pit_etf_index_members_monthly (weight_basis, obs_date);

COMMENT ON TABLE pit.pit_etf_index_members_monthly IS
    'ETF跟踪指数月末PIT成分：官方历史指数权重优先，缺失时以公告后可见的ETF定期持仓降级替代。';

COMMENT ON COLUMN pit.pit_etf_index_members_monthly.source_effective_date IS
    '指数权重交易日或ETF持仓报告期末；ETF持仓只能在source_available_date之后使用。';

COMMENT ON COLUMN pit.pit_etf_index_members_monthly.source_coverage_rate IS
    '原始成分权重或已披露ETF股票持仓比例之和除以100；部分披露不会伪装为完整持仓。';

COMMENT ON COLUMN pit.pit_etf_index_members_monthly.constituent_scope IS
    '成分范围：full_index为完整指数；a_share_subset_of_cross_market_index为跨市场指数中的A股子样本代理。';

COMMENT ON COLUMN pit.pit_etf_index_members_monthly.scope_weight_rate IS
    '当is_proxy=true时，为子样本原始权重占完整指数归一化权重的比例；完整指数默认为1。';
