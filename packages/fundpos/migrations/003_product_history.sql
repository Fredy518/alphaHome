CREATE TABLE IF NOT EXISTS fundpos.product_share_history (
    scope_version text NOT NULL,
    product_id text NOT NULL,
    share_code text NOT NULL,
    representative boolean NOT NULL,
    share_class text,
    valid_from date NOT NULL,
    valid_to date,
    available_at timestamptz NOT NULL,
    evidence_id text NOT NULL REFERENCES fundpos.evidence_snapshot(evidence_id),
    quality_status text NOT NULL,
    PRIMARY KEY (scope_version, product_id, share_code, valid_from, evidence_id)
);
CREATE INDEX IF NOT EXISTS ix_product_share_history_share
ON fundpos.product_share_history(share_code, valid_from DESC);

CREATE TABLE IF NOT EXISTS fundpos.product_classification_history (
    scope_version text NOT NULL,
    product_id text NOT NULL,
    share_code text NOT NULL,
    category text NOT NULL,
    valid_from date NOT NULL,
    valid_to date,
    available_at timestamptz NOT NULL,
    evidence_id text NOT NULL REFERENCES fundpos.evidence_snapshot(evidence_id),
    quality_status text NOT NULL,
    PRIMARY KEY (scope_version, share_code, category, valid_from, evidence_id)
);
CREATE INDEX IF NOT EXISTS ix_product_classification_history_product
ON fundpos.product_classification_history(product_id, valid_from DESC);
