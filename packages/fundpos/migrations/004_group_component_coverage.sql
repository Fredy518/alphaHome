ALTER TABLE fundpos.group_exposure
    ADD COLUMN IF NOT EXISTS universe_count integer,
    ADD COLUMN IF NOT EXISTS valid_count integer,
    ADD COLUMN IF NOT EXISTS count_coverage numeric,
    ADD COLUMN IF NOT EXISTS aum_coverage numeric,
    ADD COLUMN IF NOT EXISTS diagnostic_only boolean NOT NULL DEFAULT false;

CREATE OR REPLACE VIEW fundpos.published_group_current AS
SELECT p.publication_key, p.published_at, r.model_family, r.model_version,
       r.valuation_date, r.information_cutoff, r.source_data_through,
       g.group_key, g.category, g.weighting, g.status,
       g.universe_count, g.valid_count, g.count_coverage, g.aum_coverage,
       x.asset_code, x.denominator, x.exposure, x.value_status,
       x.valid_count AS asset_valid_count,
       x.count_coverage AS asset_count_coverage,
       x.aum_coverage AS asset_aum_coverage,
       x.diagnostic_only
FROM fundpos.publication p
JOIN fundpos.estimation_run r ON r.run_id = p.run_id
JOIN fundpos.group_estimate g ON g.run_id = r.run_id
JOIN fundpos.group_exposure x
  ON x.run_id = g.run_id
 AND x.group_key = g.group_key
 AND x.weighting = g.weighting
WHERE p.is_current AND p.revoked_at IS NULL;
