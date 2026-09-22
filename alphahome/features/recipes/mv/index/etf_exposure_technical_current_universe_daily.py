"""ETF候选母表当前暴露集的指数技术事实。

输出用于当前观察界面；因指数集来自当前候选母表，不得将本视图
宣称为无幸存者偏差的历史研究宇宙。
"""

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class ETFExposureTechnicalCurrentUniverseDailyMV(BaseFeatureView):
    """当前 ETF 候选暴露指数的趋势与波动原子指标。"""

    name = "etf_exposure_technical_current_universe_daily"
    description = "当前ETF暴露指数的MA、动量和实现波动率原子（仅当前宇宙）"
    source_tables = [
        "fund_pool_on.etf_candidate_master_current",
        "rawdata.index_factor_pro",
        "rawdata.index_swdaily",
    ]
    refresh_strategy = "full"
    quality_checks = {
        "grain": "trade_date x index_code",
        "current_universe_only": True,
        "not_survivorship_free": True,
    }

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_etf_exposure_technical_current_universe_daily AS
        WITH universe AS (
            SELECT DISTINCT tracking_index_code AS index_code
            FROM fund_pool_on.etf_candidate_master_current
            WHERE tracking_index_code IS NOT NULL
        ),
        factor_price AS (
            SELECT DISTINCT ON (p.ts_code, p.trade_date)
                p.trade_date,
                p.ts_code AS index_code,
                p.open,
                p.high,
                p.low,
                p.close,
                p.amount,
                p.update_time AS source_update_time,
                'rawdata.index_factor_pro'::text AS price_route,
                1 AS route_priority
            FROM rawdata.index_factor_pro p
            JOIN universe u ON u.index_code = p.ts_code
            WHERE p.trade_date IS NOT NULL AND p.close > 0
            ORDER BY p.ts_code, p.trade_date, p.update_time DESC NULLS LAST
        ),
        sw_price AS (
            SELECT DISTINCT ON (p.ts_code, p.trade_date)
                p.trade_date,
                p.ts_code AS index_code,
                p.open,
                p.high,
                p.low,
                p.close,
                p.amount,
                p.update_time AS source_update_time,
                'rawdata.index_swdaily'::text AS price_route,
                2 AS route_priority
            FROM rawdata.index_swdaily p
            JOIN universe u ON u.index_code = p.ts_code
            WHERE p.trade_date IS NOT NULL AND p.close > 0
            ORDER BY p.ts_code, p.trade_date, p.update_time DESC NULLS LAST
        ),
        routed AS (
            SELECT * FROM factor_price
            UNION ALL
            SELECT * FROM sw_price
        ),
        prices AS (
            SELECT DISTINCT ON (trade_date, index_code)
                trade_date,
                index_code,
                open,
                high,
                low,
                close,
                amount,
                source_update_time,
                price_route
            FROM routed
            ORDER BY trade_date, index_code, route_priority
        ),
        returns AS (
            SELECT
                p.*,
                COUNT(*) OVER (
                    PARTITION BY index_code ORDER BY trade_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS history_obs_count,
                close / NULLIF(LAG(close) OVER (
                    PARTITION BY index_code ORDER BY trade_date
                ), 0) - 1 AS return_1d,
                close / NULLIF(LAG(close, 20) OVER (
                    PARTITION BY index_code ORDER BY trade_date
                ), 0) - 1 AS return_20d,
                close / NULLIF(LAG(close, 60) OVER (
                    PARTITION BY index_code ORDER BY trade_date
                ), 0) - 1 AS return_60d,
                close / NULLIF(LAG(close, 120) OVER (
                    PARTITION BY index_code ORDER BY trade_date
                ), 0) - 1 AS return_120d,
                close / NULLIF(LAG(close, 252) OVER (
                    PARTITION BY index_code ORDER BY trade_date
                ), 0) - 1 AS return_252d,
                AVG(close) OVER w20 AS ma_20,
                AVG(close) OVER w60 AS ma_60,
                AVG(close) OVER w120 AS ma_120,
                AVG(close) OVER w250 AS ma_250,
                AVG(amount) OVER w20 AS amount_mean_20d_raw
            FROM prices p
            WINDOW
                w20 AS (PARTITION BY index_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                w60 AS (PARTITION BY index_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
                w120 AS (PARTITION BY index_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW),
                w250 AS (PARTITION BY index_code ORDER BY trade_date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW)
        ),
        volatility AS (
            SELECT
                r.*,
                STDDEV_SAMP(return_1d) OVER (
                    PARTITION BY index_code ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) * SQRT(252) AS realized_vol_20d,
                STDDEV_SAMP(return_1d) OVER (
                    PARTITION BY index_code ORDER BY trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) * SQRT(252) AS realized_vol_60d
            FROM returns r
        )
        SELECT
            trade_date,
            index_code,
            open,
            high,
            low,
            close,
            amount AS amount_raw,
            price_route,
            source_update_time,
            history_obs_count,
            return_1d,
            return_20d,
            return_60d,
            return_120d,
            return_252d,
            ma_20,
            ma_60,
            ma_120,
            ma_250,
            amount_mean_20d_raw,
            realized_vol_20d,
            realized_vol_60d,
            realized_vol_20d / NULLIF(realized_vol_60d, 0) AS vol_ratio_20_60,
            CASE WHEN history_obs_count >= 120 THEN close > ma_120 END AS above_ma120,
            CASE WHEN history_obs_count >= 250 THEN close > ma_250 END AS above_ma250,
            price_route || ',fund_pool_on.etf_candidate_master_current' AS _source_table,
            NOW() AS _processed_at,
            trade_date AS _data_version
        FROM volatility
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    async def expected_empty_view_reason(self, connection):
        has_index = await connection.fetchval(
            'SELECT EXISTS(SELECT 1 FROM fund_pool_on.etf_candidate_master_current '
            'WHERE tracking_index_code IS NOT NULL)'
        )
        return None if has_index else 'Current candidate universe has no tracked index.'

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_etf_exposure_technical_key "
            "ON features.mv_etf_exposure_technical_current_universe_daily "
            "(trade_date, index_code)",
            "CREATE INDEX IF NOT EXISTS idx_mv_etf_exposure_technical_code_date "
            "ON features.mv_etf_exposure_technical_current_universe_daily "
            "(index_code, trade_date)",
        ]
