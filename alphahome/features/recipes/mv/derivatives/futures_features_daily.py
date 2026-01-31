"""
股指期货特征（日频）

涵盖：
- IF/IC/IM 加权基差 & 非年化贴水率
- 会员持仓净多空、净变化、多空比
- 前20席位主力持仓变化

数据来源：
- tushare.future_daily
- tushare.future_holding
- tushare.index_daily（现货指数）

时间语义：日终，T 日收盘后可计算 T 日指标
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class FuturesFeaturesDailyMV(BaseFeatureView):
    """股指期货特征（日频）"""

    name = "futures_features_daily"
    description = "股指期货基差、会员持仓、前20席位等特征（日频）"
    source_tables = [
        "tushare.future_daily",
        "tushare.future_holding",
        "tushare.index_daily",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_futures_features_daily AS
        WITH
        -- 1. 品种-指数映射
        mapping AS (
            SELECT 'IF' AS fut_prefix, '000300.SH' AS idx_code
            UNION ALL SELECT 'IC', '000905.SH'
            UNION ALL SELECT 'IM', '000852.SH'
            UNION ALL SELECT 'IH', '000016.SH'
        ),

        -- 2. 期货日行情（以持仓量 oi 对各合约加权聚合）
        future_daily AS (
            SELECT
                trade_date,
                LEFT(ts_code, 2) AS fut_prefix,
                close,
                oi,
                SUM(oi) OVER (PARTITION BY trade_date, LEFT(ts_code, 2)) AS total_oi
            FROM tushare.future_daily
            WHERE (ts_code LIKE 'IF%%' OR ts_code LIKE 'IC%%' OR ts_code LIKE 'IM%%' OR ts_code LIKE 'IH%%')
              AND oi IS NOT NULL AND oi > 0
              AND close IS NOT NULL
        ),

        -- 3. 加权期货价格
        weighted_fut AS (
            SELECT
                trade_date,
                fut_prefix,
                SUM(close * oi / NULLIF(total_oi, 0)) AS weighted_close
            FROM future_daily
            GROUP BY trade_date, fut_prefix
        ),

        -- 4. 现货指数收盘价
        idx_close AS (
            SELECT
                trade_date,
                ts_code AS idx_code,
                close
            FROM tushare.index_daily
            WHERE ts_code IN ('000300.SH', '000905.SH', '000852.SH', '000016.SH')
              AND close IS NOT NULL
        ),

        -- 5. 基差计算
        basis AS (
            SELECT
                w.trade_date,
                w.fut_prefix,
                i.close - w.weighted_close AS basis,
                (i.close - w.weighted_close) / NULLIF(i.close, 0) AS basis_ratio
            FROM weighted_fut w
            JOIN mapping m ON w.fut_prefix = m.fut_prefix
            JOIN idx_close i ON m.idx_code = i.idx_code AND w.trade_date = i.trade_date
        ),

        -- 6. 会员持仓聚合（全量汇总口径）
        member_agg AS (
            SELECT
                trade_date,
                LEFT(symbol, 2) AS fut_prefix,
                SUM(COALESCE(long_hld, 0)) AS total_long,
                SUM(COALESCE(short_hld, 0)) AS total_short,
                SUM(COALESCE(long_chg, 0)) AS total_long_chg,
                SUM(COALESCE(short_chg, 0)) AS total_short_chg
            FROM tushare.future_holding
            WHERE (symbol LIKE 'IF%%' OR symbol LIKE 'IC%%' OR symbol LIKE 'IM%%' OR symbol LIKE 'IH%%')
            GROUP BY trade_date, LEFT(symbol, 2)
        ),
        member AS (
            SELECT
                trade_date,
                fut_prefix,
                total_long - total_short AS net_long,
                total_long_chg - total_short_chg AS net_chg,
                total_long / NULLIF(total_short, 0) AS long_short_ratio
            FROM member_agg
        ),

        -- 7. 前20席位（按多头持仓排序）
        top20 AS (
            SELECT
                trade_date,
                fut_prefix,
                SUM(COALESCE(long_hld, 0)) AS top20_long,
                SUM(COALESCE(short_hld, 0)) AS top20_short,
                SUM(COALESCE(long_chg, 0)) AS top20_long_chg,
                SUM(COALESCE(short_chg, 0)) AS top20_short_chg
            FROM (
                SELECT
                    trade_date,
                    symbol,
                    long_hld,
                    short_hld,
                    long_chg,
                    short_chg,
                    LEFT(symbol, 2) AS fut_prefix,
                    ROW_NUMBER() OVER (
                        PARTITION BY trade_date, LEFT(symbol, 2)
                        ORDER BY COALESCE(long_hld, 0) DESC
                    ) AS rn
                FROM tushare.future_holding
                WHERE (symbol LIKE 'IF%%' OR symbol LIKE 'IC%%' OR symbol LIKE 'IM%%' OR symbol LIKE 'IH%%')
            ) t
            WHERE rn <= 20
            GROUP BY trade_date, fut_prefix
        ),
        top20_calc AS (
            SELECT
                trade_date,
                fut_prefix,
                top20_long - top20_short AS net_long,
                top20_long_chg - top20_short_chg AS net_chg
            FROM top20
        )

        -- 8. 最终输出：透视为宽表
        SELECT
            COALESCE(b.trade_date, m.trade_date, t.trade_date) AS trade_date,

            -- 基差
            MAX(CASE WHEN b.fut_prefix = 'IF' THEN b.basis END) AS IF_Basis,
            MAX(CASE WHEN b.fut_prefix = 'IF' THEN b.basis_ratio END) AS IF_Basis_Ratio,
            MAX(CASE WHEN b.fut_prefix = 'IC' THEN b.basis END) AS IC_Basis,
            MAX(CASE WHEN b.fut_prefix = 'IC' THEN b.basis_ratio END) AS IC_Basis_Ratio,
            MAX(CASE WHEN b.fut_prefix = 'IM' THEN b.basis END) AS IM_Basis,
            MAX(CASE WHEN b.fut_prefix = 'IM' THEN b.basis_ratio END) AS IM_Basis_Ratio,

            -- 会员持仓
            MAX(CASE WHEN m.fut_prefix = 'IF' THEN m.net_long END) AS IF_Member_Net_Long,
            MAX(CASE WHEN m.fut_prefix = 'IF' THEN m.net_chg END) AS IF_Member_Net_Chg,
            MAX(CASE WHEN m.fut_prefix = 'IF' THEN m.long_short_ratio END) AS IF_Member_Long_Short_Ratio,
            MAX(CASE WHEN m.fut_prefix = 'IC' THEN m.net_long END) AS IC_Member_Net_Long,
            MAX(CASE WHEN m.fut_prefix = 'IC' THEN m.net_chg END) AS IC_Member_Net_Chg,
            MAX(CASE WHEN m.fut_prefix = 'IC' THEN m.long_short_ratio END) AS IC_Member_Long_Short_Ratio,
            MAX(CASE WHEN m.fut_prefix = 'IM' THEN m.net_long END) AS IM_Member_Net_Long,
            MAX(CASE WHEN m.fut_prefix = 'IM' THEN m.net_chg END) AS IM_Member_Net_Chg,
            MAX(CASE WHEN m.fut_prefix = 'IM' THEN m.long_short_ratio END) AS IM_Member_Long_Short_Ratio,
            MAX(CASE WHEN m.fut_prefix = 'IH' THEN m.net_long END) AS IH_Member_Net_Long,
            MAX(CASE WHEN m.fut_prefix = 'IH' THEN m.net_chg END) AS IH_Member_Net_Chg,
            MAX(CASE WHEN m.fut_prefix = 'IH' THEN m.long_short_ratio END) AS IH_Member_Long_Short_Ratio,

            -- 前20席位
            MAX(CASE WHEN t.fut_prefix = 'IF' THEN t.net_long END) AS IF_Top20_Net_Long,
            MAX(CASE WHEN t.fut_prefix = 'IF' THEN t.net_chg END) AS IF_Top20_Net_Chg,
            MAX(CASE WHEN t.fut_prefix = 'IC' THEN t.net_long END) AS IC_Top20_Net_Long,
            MAX(CASE WHEN t.fut_prefix = 'IC' THEN t.net_chg END) AS IC_Top20_Net_Chg,
            MAX(CASE WHEN t.fut_prefix = 'IM' THEN t.net_long END) AS IM_Top20_Net_Long,
            MAX(CASE WHEN t.fut_prefix = 'IM' THEN t.net_chg END) AS IM_Top20_Net_Chg,
            MAX(CASE WHEN t.fut_prefix = 'IH' THEN t.net_long END) AS IH_Top20_Net_Long,
            MAX(CASE WHEN t.fut_prefix = 'IH' THEN t.net_chg END) AS IH_Top20_Net_Chg,

            -- 血缘
            'tushare.future_daily,tushare.future_holding,tushare.index_daily' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM basis b
        FULL OUTER JOIN member m ON b.trade_date = m.trade_date AND b.fut_prefix = m.fut_prefix
        FULL OUTER JOIN top20_calc t ON COALESCE(b.trade_date, m.trade_date) = t.trade_date
                                     AND COALESCE(b.fut_prefix, m.fut_prefix) = t.fut_prefix
        GROUP BY COALESCE(b.trade_date, m.trade_date, t.trade_date)
        ORDER BY trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_futures_features_daily_trade_date "
            "ON features.mv_futures_features_daily (trade_date)",
            "COMMENT ON MATERIALIZED VIEW features.mv_futures_features_daily IS "
            "'股指期货基差与持仓特征（日频）';",
        ]


# 兼容旧类名（外部 import 不断）
FuturesFeaturesDaily = FuturesFeaturesDailyMV
