"""指数直接估值事实（日频）。

仅整合数据商直接提供的估值字段，不用今日成分回构历史估值，
也不把缺失直接口径填成可比口径。
"""

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class IndexDirectValuationDailyMV(BaseFeatureView):
    """中证/交易所指数与申万行业指数的直接估值面板。"""

    name = "index_direct_valuation_daily"
    description = "指数直接PE/PB估值及来源路由（日频，不含重构值）"
    source_tables = ["rawdata.index_dailybasic", "rawdata.index_swdaily"]
    refresh_strategy = "full"
    quality_checks = {
        "grain": "trade_date x index_code",
        "no_reconstructed_valuation": True,
        "route_priority": ["rawdata.index_dailybasic", "rawdata.index_swdaily"],
    }

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_index_direct_valuation_daily AS
        WITH index_basic AS (
            SELECT DISTINCT ON (v.ts_code, v.trade_date)
                v.trade_date,
                v.ts_code AS index_code,
                v.pe AS pe_static,
                v.pe_ttm,
                COALESCE(v.pe_ttm, v.pe) AS pe_value,
                CASE
                    WHEN v.pe_ttm IS NOT NULL THEN 'pe_ttm'
                    WHEN v.pe IS NOT NULL THEN 'pe_static_fallback'
                    ELSE NULL
                END AS pe_basis,
                v.pb,
                v.total_mv,
                v.float_mv,
                v.turnover_rate,
                v.update_time AS source_update_time,
                'rawdata.index_dailybasic'::text AS valuation_route,
                1 AS route_priority
            FROM rawdata.index_dailybasic v
            WHERE v.ts_code IS NOT NULL AND v.trade_date IS NOT NULL
            ORDER BY v.ts_code, v.trade_date, v.update_time DESC NULLS LAST
        ),
        sw_basic AS (
            SELECT DISTINCT ON (v.ts_code, v.trade_date)
                v.trade_date,
                v.ts_code AS index_code,
                NULL::numeric AS pe_static,
                NULL::numeric AS pe_ttm,
                v.pe AS pe_value,
                CASE WHEN v.pe IS NOT NULL THEN 'provider_pe' END AS pe_basis,
                v.pb,
                v.total_mv,
                v.float_mv,
                NULL::numeric AS turnover_rate,
                v.update_time AS source_update_time,
                'rawdata.index_swdaily'::text AS valuation_route,
                2 AS route_priority
            FROM rawdata.index_swdaily v
            WHERE v.ts_code IS NOT NULL AND v.trade_date IS NOT NULL
            ORDER BY v.ts_code, v.trade_date, v.update_time DESC NULLS LAST
        ),
        routed AS (
            SELECT * FROM index_basic
            UNION ALL
            SELECT * FROM sw_basic
        ),
        dedup AS (
            SELECT DISTINCT ON (trade_date, index_code)
                trade_date,
                index_code,
                pe_static,
                pe_ttm,
                pe_value,
                pe_basis,
                pb,
                total_mv,
                float_mv,
                turnover_rate,
                source_update_time,
                valuation_route
            FROM routed
            ORDER BY trade_date, index_code, route_priority
        )
        SELECT
            trade_date,
            index_code,
            pe_static,
            pe_ttm,
            pe_value,
            pe_basis,
            pb,
            total_mv,
            float_mv,
            turnover_rate,
            valuation_route,
            source_update_time,
            (pe_value > 0 OR pb > 0) AS valuation_available,
            valuation_route AS _source_table,
            NOW() AS _processed_at,
            trade_date AS _data_version
        FROM dedup
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_index_direct_valuation_key "
            "ON features.mv_index_direct_valuation_daily (trade_date, index_code)",
            "CREATE INDEX IF NOT EXISTS idx_mv_index_direct_valuation_code_date "
            "ON features.mv_index_direct_valuation_daily (index_code, trade_date)",
        ]
