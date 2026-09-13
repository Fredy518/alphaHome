"""行业盈利与预期观察原子（月频，PIT）。"""

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class IndustryEarningsObservationMonthlyMV(BaseFeatureView):
    """合并行业 FAPI 与 FTTM，只生成可复用变化量和质量字段。"""

    name = "industry_earnings_observation_monthly"
    description = "行业FAPI、预期ROE、FTTM及滚动观察原子（月频PIT）"
    source_tables = [
        "pit.pit_industry_fapi_monthly",
        "pit.pit_industry_fttm_monthly",
    ]
    refresh_strategy = "full"
    quality_checks = {
        "grain": "obs_date x classification_source x industry_level x industry_code",
        "minimum_prior_observations_for_z12": 6,
        "posthoc_composite_state": False,
    }

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_industry_earnings_observation_monthly AS
        WITH fapi_base AS (
            SELECT
                f.obs_date,
                f.classification_source,
                f.industry_level,
                f.industry_code,
                f.industry_name,
                f.fapi_spread_weighted,
                f.fapi_ratio_weighted,
                f.expected_roe_weighted,
                f.previous_expected_roe_weighted,
                f.source_max_report_date AS fapi_source_max_report_date,
                f.matched_stock_count AS fapi_matched_stock_count,
                f.matched_org_count AS fapi_matched_org_count,
                f.average_report_age_days,
                f.is_eligible AS is_fapi_eligible,
                f.is_ratio_eligible AS is_fapi_ratio_eligible,
                f.quality_reasons AS fapi_quality_reasons,
                f.method_version AS fapi_method_version,
                f.org_weight_version,
                f.quality_rule_version AS fapi_quality_rule_version
            FROM pit.pit_industry_fapi_monthly f
        ),
        fapi_window AS (
            SELECT
                f.*,
                LAG(f.fapi_spread_weighted) OVER w AS fapi_spread_weighted_prev_1m,
                LAG(f.fapi_ratio_weighted) OVER w AS fapi_ratio_weighted_prev_1m,
                LAG(f.expected_roe_weighted) OVER w AS expected_roe_weighted_prev_1m,
                AVG(f.fapi_spread_weighted) OVER w6 AS fapi_spread_weighted_ma6,
                AVG(f.expected_roe_weighted) OVER w6 AS expected_roe_weighted_ma6,
                COUNT(f.fapi_spread_weighted) OVER w12_prior AS fapi_spread_prior_obs_12,
                AVG(f.fapi_spread_weighted) OVER w12_prior AS fapi_spread_prior_mean_12,
                STDDEV_SAMP(f.fapi_spread_weighted) OVER w12_prior AS fapi_spread_prior_std_12,
                COUNT(f.expected_roe_weighted) OVER w12_prior AS expected_roe_prior_obs_12,
                AVG(f.expected_roe_weighted) OVER w12_prior AS expected_roe_prior_mean_12,
                STDDEV_SAMP(f.expected_roe_weighted) OVER w12_prior AS expected_roe_prior_std_12
            FROM fapi_base f
            WINDOW
                w AS (
                    PARTITION BY classification_source, industry_level, industry_code
                    ORDER BY obs_date
                ),
                w6 AS (
                    PARTITION BY classification_source, industry_level, industry_code
                    ORDER BY obs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ),
                w12_prior AS (
                    PARTITION BY classification_source, industry_level, industry_code
                    ORDER BY obs_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
                )
        ),
        fttm AS (
            SELECT
                t.obs_date,
                t.classification_source,
                t.industry_level,
                t.industry_code,
                t.industry_fttm_np,
                t.previous_industry_fttm_np,
                t.fttm_np_mom_abs,
                t.fttm_np_mom_rate,
                t.diffusion_up,
                t.covered_stock_count AS fttm_covered_stock_count,
                t.covered_stock_rate AS fttm_covered_stock_rate,
                t.covered_mv_rate AS fttm_covered_mv_rate,
                t.source_max_report_date AS fttm_source_max_report_date,
                t.is_eligible AS is_fttm_eligible,
                t.is_diffusion_eligible,
                t.quality_reasons AS fttm_quality_reasons,
                t.stock_formula_version,
                t.aggregation_version,
                t.quality_rule_version AS fttm_quality_rule_version,
                t.revision_rate,
                t.horizon_roll_rate,
                t.revision_activity_rate,
                t.revision_up_stock_rate,
                t.revision_up_weight_rate,
                t.is_revision_eligible,
                t.revision_quality_reasons,
                t.revision_version
            FROM pit.pit_industry_fttm_monthly t
        )
        SELECT
            f.obs_date,
            f.classification_source,
            f.industry_level,
            f.industry_code,
            f.industry_name,
            f.fapi_spread_weighted,
            f.fapi_spread_weighted - f.fapi_spread_weighted_prev_1m
                AS fapi_spread_weighted_change_1m,
            f.fapi_spread_weighted_ma6,
            CASE
                WHEN f.fapi_spread_prior_obs_12 >= 6
                 AND f.fapi_spread_prior_std_12 > 0
                THEN (f.fapi_spread_weighted - f.fapi_spread_prior_mean_12)
                    / f.fapi_spread_prior_std_12
            END AS fapi_spread_weighted_z12_prior,
            f.fapi_ratio_weighted,
            f.fapi_ratio_weighted - f.fapi_ratio_weighted_prev_1m
                AS fapi_ratio_weighted_change_1m,
            f.expected_roe_weighted,
            f.previous_expected_roe_weighted,
            f.expected_roe_weighted - f.expected_roe_weighted_prev_1m
                AS expected_roe_weighted_change_1m,
            f.expected_roe_weighted_ma6,
            CASE
                WHEN f.expected_roe_prior_obs_12 >= 6
                 AND f.expected_roe_prior_std_12 > 0
                THEN (f.expected_roe_weighted - f.expected_roe_prior_mean_12)
                    / f.expected_roe_prior_std_12
            END AS expected_roe_weighted_z12_prior,
            f.fapi_source_max_report_date,
            f.fapi_matched_stock_count,
            f.fapi_matched_org_count,
            f.average_report_age_days,
            f.is_fapi_eligible,
            f.is_fapi_ratio_eligible,
            f.fapi_quality_reasons,
            f.fapi_method_version,
            f.org_weight_version,
            f.fapi_quality_rule_version,
            t.industry_fttm_np,
            t.previous_industry_fttm_np,
            t.fttm_np_mom_abs,
            t.fttm_np_mom_rate,
            t.diffusion_up,
            t.fttm_covered_stock_count,
            t.fttm_covered_stock_rate,
            t.fttm_covered_mv_rate,
            t.fttm_source_max_report_date,
            t.is_fttm_eligible,
            t.is_diffusion_eligible,
            t.fttm_quality_reasons,
            t.stock_formula_version,
            t.aggregation_version,
            t.fttm_quality_rule_version,
            t.revision_rate,
            t.horizon_roll_rate,
            t.revision_activity_rate,
            t.revision_up_stock_rate,
            t.revision_up_weight_rate,
            t.is_revision_eligible,
            t.revision_quality_reasons,
            t.revision_version,
            GREATEST(f.fapi_source_max_report_date, t.fttm_source_max_report_date)
                AS source_max_report_date,
            f.obs_date - GREATEST(
                f.fapi_source_max_report_date, t.fttm_source_max_report_date
            ) AS source_report_age_days,
            'pit.pit_industry_fapi_monthly,pit.pit_industry_fttm_monthly'
                AS _source_table,
            NOW() AS _processed_at,
            f.obs_date AS _data_version
        FROM fapi_window f
        LEFT JOIN fttm t
          ON t.obs_date = f.obs_date
         AND t.classification_source = f.classification_source
         AND t.industry_level = f.industry_level
         AND t.industry_code = f.industry_code
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_industry_earnings_observation_key "
            "ON features.mv_industry_earnings_observation_monthly "
            "(obs_date, classification_source, industry_level, industry_code)",
            "CREATE INDEX IF NOT EXISTS idx_mv_industry_earnings_observation_code_date "
            "ON features.mv_industry_earnings_observation_monthly "
            "(industry_code, obs_date)",
        ]
