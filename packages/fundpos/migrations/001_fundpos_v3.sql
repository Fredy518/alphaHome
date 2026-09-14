CREATE SCHEMA IF NOT EXISTS fundpos;

CREATE TABLE IF NOT EXISTS fundpos.schema_migration (
    version text PRIMARY KEY,
    sha256 text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fundpos.evidence_snapshot (
    evidence_id text PRIMARY KEY,
    evidence_type text NOT NULL,
    source_name text NOT NULL,
    source_uri text,
    content_sha256 text NOT NULL,
    report_date date,
    announcement_date date,
    first_observed_at timestamptz,
    normalization_version text NOT NULL,
    local_path text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_evidence_content
ON fundpos.evidence_snapshot(evidence_type, content_sha256);

CREATE TABLE IF NOT EXISTS fundpos.product_scope_history (
    scope_version text NOT NULL,
    product_id text NOT NULL,
    representative_share text NOT NULL,
    category text NOT NULL,
    pool_kind text NOT NULL,
    valid_from date NOT NULL,
    valid_to date,
    available_at timestamptz NOT NULL,
    evidence_id text REFERENCES fundpos.evidence_snapshot(evidence_id),
    quality_status text NOT NULL,
    reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (scope_version, product_id, pool_kind, valid_from)
);

CREATE TABLE IF NOT EXISTS fundpos.contract_constraint (
    product_id text NOT NULL,
    constraint_name text NOT NULL,
    effective_date date NOT NULL,
    announcement_date date NOT NULL,
    lower_bound numeric,
    upper_bound numeric,
    denominator text NOT NULL,
    evidence_id text NOT NULL REFERENCES fundpos.evidence_snapshot(evidence_id),
    verified boolean NOT NULL,
    original_text text,
    PRIMARY KEY (product_id, constraint_name, effective_date, evidence_id)
);

CREATE TABLE IF NOT EXISTS fundpos.disclosure_report (
    report_id text PRIMARY KEY,
    product_id text NOT NULL,
    representative_share text NOT NULL,
    report_date date NOT NULL,
    report_type text NOT NULL,
    announcement_date date,
    first_observed_at timestamptz,
    evidence_id text REFERENCES fundpos.evidence_snapshot(evidence_id),
    stock_value numeric,
    ordinary_bond_value numeric,
    convertible_bond_value numeric,
    net_asset_value numeric,
    total_asset_value numeric,
    completeness_status text NOT NULL,
    control_difference numeric,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS fundpos.disclosure_holding (
    report_id text NOT NULL REFERENCES fundpos.disclosure_report(report_id),
    security_code text NOT NULL,
    asset_type text NOT NULL,
    market_value numeric NOT NULL,
    nav_weight numeric,
    announcement_date date,
    announcement_source text NOT NULL,
    industry_code text,
    industry_effective_date date,
    PRIMARY KEY (report_id, security_code, asset_type)
);

CREATE TABLE IF NOT EXISTS fundpos.model_registry (
    model_version text PRIMARY KEY,
    model_family text NOT NULL,
    code_sha256 text NOT NULL,
    protocol_sha256 text NOT NULL,
    parameters jsonb NOT NULL,
    status text NOT NULL,
    final_holdout_opened boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fundpos.estimation_run (
    run_id text PRIMARY KEY,
    logical_run_key text NOT NULL UNIQUE,
    model_version text NOT NULL REFERENCES fundpos.model_registry(model_version),
    model_family text NOT NULL,
    scope_version text NOT NULL,
    valuation_date date NOT NULL,
    information_cutoff date NOT NULL,
    input_sha256 text NOT NULL,
    config_sha256 text NOT NULL,
    code_sha256 text NOT NULL,
    manifest_sha256 text NOT NULL,
    source_data_through date,
    status text NOT NULL,
    formal_publication_eligible boolean NOT NULL DEFAULT false,
    final_holdout_opened boolean NOT NULL DEFAULT false,
    manifest jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fundpos.estimation_attempt (
    attempt_id bigserial PRIMARY KEY,
    logical_run_key text NOT NULL,
    run_id text,
    attempted_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS fundpos.run_evidence (
    run_id text NOT NULL REFERENCES fundpos.estimation_run(run_id),
    evidence_id text NOT NULL REFERENCES fundpos.evidence_snapshot(evidence_id),
    evidence_role text NOT NULL,
    PRIMARY KEY (run_id, evidence_id, evidence_role)
);

CREATE TABLE IF NOT EXISTS fundpos.fund_estimate (
    run_id text NOT NULL REFERENCES fundpos.estimation_run(run_id),
    valuation_date date NOT NULL,
    product_id text NOT NULL,
    representative_share text NOT NULL,
    category text NOT NULL,
    status text NOT NULL,
    reason text,
    aum numeric,
    aum_date date,
    stock_weight numeric,
    convertible_bond_weight numeric,
    ordinary_bond_weight numeric,
    financing_weight numeric,
    stock_quality text,
    convertible_bond_quality text,
    ordinary_bond_quality text,
    proxy_ratio numeric,
    holdings_report_date date,
    constraint_error numeric,
    return_mae numeric,
    r2 numeric,
    condition_number numeric,
    diagnostics jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (run_id, valuation_date, product_id)
) PARTITION BY RANGE (valuation_date);

CREATE TABLE IF NOT EXISTS fundpos.fund_exposure (
    run_id text NOT NULL,
    valuation_date date NOT NULL,
    product_id text NOT NULL,
    asset_code text NOT NULL,
    denominator text NOT NULL,
    exposure numeric,
    value_status text NOT NULL,
    diagnostic_only boolean NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, valuation_date, product_id, asset_code, denominator),
    FOREIGN KEY (run_id, valuation_date, product_id)
        REFERENCES fundpos.fund_estimate(run_id, valuation_date, product_id)
) PARTITION BY RANGE (valuation_date);

DO $$
DECLARE y integer;
BEGIN
    FOR y IN 2022..2027 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS fundpos.fund_estimate_%s PARTITION OF fundpos.fund_estimate FOR VALUES FROM (%L) TO (%L)', y, make_date(y,1,1), make_date(y+1,1,1));
        EXECUTE format('CREATE TABLE IF NOT EXISTS fundpos.fund_exposure_%s PARTITION OF fundpos.fund_exposure FOR VALUES FROM (%L) TO (%L)', y, make_date(y,1,1), make_date(y+1,1,1));
    END LOOP;
END $$;
CREATE TABLE IF NOT EXISTS fundpos.fund_estimate_default PARTITION OF fundpos.fund_estimate DEFAULT;
CREATE TABLE IF NOT EXISTS fundpos.fund_exposure_default PARTITION OF fundpos.fund_exposure DEFAULT;
CREATE INDEX IF NOT EXISTS ix_fund_estimate_product_date ON fundpos.fund_estimate(product_id, valuation_date DESC);
CREATE INDEX IF NOT EXISTS ix_fund_exposure_product_date ON fundpos.fund_exposure(product_id, valuation_date DESC);

CREATE TABLE IF NOT EXISTS fundpos.group_estimate (
    run_id text NOT NULL REFERENCES fundpos.estimation_run(run_id),
    group_key text NOT NULL,
    category text NOT NULL,
    weighting text NOT NULL,
    status text NOT NULL,
    universe_count integer NOT NULL,
    valid_count integer NOT NULL,
    count_coverage numeric NOT NULL,
    aum_coverage numeric,
    aum_known_count integer NOT NULL,
    known_aum numeric,
    PRIMARY KEY (run_id, group_key, weighting)
);

CREATE TABLE IF NOT EXISTS fundpos.group_exposure (
    run_id text NOT NULL,
    group_key text NOT NULL,
    weighting text NOT NULL,
    asset_code text NOT NULL,
    denominator text NOT NULL,
    exposure numeric,
    value_status text NOT NULL,
    PRIMARY KEY (run_id, group_key, weighting, asset_code, denominator),
    FOREIGN KEY (run_id, group_key, weighting)
        REFERENCES fundpos.group_estimate(run_id, group_key, weighting)
);

CREATE TABLE IF NOT EXISTS fundpos.validation_result (
    validation_id text PRIMARY KEY,
    model_version text NOT NULL REFERENCES fundpos.model_registry(model_version),
    phase text NOT NULL,
    scope_version text NOT NULL,
    truth_version text NOT NULL,
    status text NOT NULL,
    metrics jsonb NOT NULL,
    final_holdout_opened boolean NOT NULL DEFAULT false,
    evidence_sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fundpos.publication (
    publication_id bigserial PRIMARY KEY,
    publication_key text NOT NULL,
    run_id text NOT NULL REFERENCES fundpos.estimation_run(run_id),
    validation_id text NOT NULL REFERENCES fundpos.validation_result(validation_id),
    published_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz,
    is_current boolean NOT NULL DEFAULT true,
    UNIQUE (publication_key, run_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_publication_current
ON fundpos.publication(publication_key) WHERE is_current;

CREATE TABLE IF NOT EXISTS fundpos.publication_event (
    event_id bigserial PRIMARY KEY,
    publication_key text NOT NULL,
    run_id text NOT NULL,
    event_type text NOT NULL,
    event_at timestamptz NOT NULL DEFAULT now(),
    detail jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE OR REPLACE VIEW fundpos.latest_available AS
SELECT DISTINCT ON (e.product_id, r.model_family, x.asset_code, x.denominator)
    e.product_id, r.model_family, r.model_version, r.valuation_date,
    r.information_cutoff, r.source_data_through, e.status, e.reason,
    x.asset_code, x.denominator, x.exposure, x.value_status, x.diagnostic_only, r.run_id,
    (r.source_data_through IS NULL OR r.source_data_through < r.valuation_date) AS source_stale_at_valuation,
    (CURRENT_DATE - r.valuation_date) AS valuation_age_days
FROM fundpos.estimation_run r
JOIN fundpos.fund_estimate e ON e.run_id = r.run_id
JOIN fundpos.fund_exposure x
  ON x.run_id = e.run_id
 AND x.valuation_date = e.valuation_date
 AND x.product_id = e.product_id
ORDER BY e.product_id, r.model_family, x.asset_code, x.denominator,
         r.valuation_date DESC, r.information_cutoff DESC, r.ingested_at DESC;

CREATE OR REPLACE VIEW fundpos.published_current AS
SELECT p.publication_key, p.published_at, r.model_family, r.model_version,
       r.valuation_date, r.information_cutoff, r.source_data_through,
       (r.source_data_through IS NULL OR r.source_data_through < r.valuation_date) AS source_stale_at_valuation,
       (CURRENT_DATE - r.valuation_date) AS valuation_age_days,
       e.product_id, e.status, x.asset_code, x.denominator, x.exposure, x.value_status
FROM fundpos.publication p
JOIN fundpos.estimation_run r ON r.run_id = p.run_id
JOIN fundpos.fund_estimate e ON e.run_id = r.run_id
JOIN fundpos.fund_exposure x
  ON x.run_id = e.run_id
 AND x.valuation_date = e.valuation_date
 AND x.product_id = e.product_id
WHERE p.is_current AND p.revoked_at IS NULL;

CREATE OR REPLACE VIEW fundpos.published_group_current AS
SELECT p.publication_key, p.published_at, r.model_family, r.model_version,
       r.valuation_date, r.information_cutoff, r.source_data_through,
       g.group_key, g.category, g.weighting, g.status,
       g.universe_count, g.valid_count, g.count_coverage, g.aum_coverage,
       x.asset_code, x.denominator, x.exposure, x.value_status
FROM fundpos.publication p
JOIN fundpos.estimation_run r ON r.run_id = p.run_id
JOIN fundpos.group_estimate g ON g.run_id = r.run_id
JOIN fundpos.group_exposure x
  ON x.run_id = g.run_id
 AND x.group_key = g.group_key
 AND x.weighting = g.weighting
WHERE p.is_current AND p.revoked_at IS NULL;
