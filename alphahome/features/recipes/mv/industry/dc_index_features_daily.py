"""
打板指数特征（日频）

连板指数/高度板特征，A 股短线情绪核心指标。
"""

from alphahome.features.storage.incremental_view import IncrementalTableView
from alphahome.features.registry import feature_register


@feature_register
class DCIndexFeaturesDailyMV(IncrementalTableView):
    """打板指数特征"""

    name = "dc_index_features_daily"
    description = "打板指数、连板指数、高度板特征"
    source_tables = ["rawdata.stock_dcindex", "rawdata.stock_dcdaily"]
    refresh_strategy = "incremental"  # 默认增量刷新
    incremental_days = 30  # 增量刷新最近 30 天
    date_column = "trade_date"
    _lookback_days = 30  # 窗口函数需要回溯天数

    create_sql = """
        CREATE TABLE features.mv_dc_index_features_daily AS
        WITH dc_indices AS (
            -- 打板指数基础数据
            SELECT
                trade_date,
                ts_code,
                name,
                "leading" AS leading_name,  -- 领涨股名称
                leading_code,
                pct_change,     -- 指数涨跌幅
                leading_pct,    -- 领涨股涨幅
                total_mv,       -- 成分股总市值
                turnover_rate,  -- 换手率
                up_num,         -- 上涨个数
                down_num        -- 下跌个数
            FROM rawdata.stock_dcindex
            WHERE trade_date IS NOT NULL
        ),
        dc_daily AS (
            -- 打板指数行情
            SELECT
                trade_date,
                ts_code,
                close,
                open,
                high,
                low,
                pct_change,
                volume,
                amount,
                turnover_rate
            FROM rawdata.stock_dcdaily
            WHERE trade_date IS NOT NULL
        ),
        combined AS (
            SELECT
                i.trade_date,
                i.ts_code,
                i.name,
                i.pct_change AS index_pct_change,
                i.leading_pct,
                i.up_num,
                i.down_num,
                i.total_mv,
                d.close,
                d.high,
                d.low,
                d.volume,
                d.amount,
                -- 上涨/下跌比
                CASE WHEN i.down_num > 0
                    THEN i.up_num * 1.0 / i.down_num
                    ELSE NULL
                END AS up_down_ratio,
                -- 5 日动量
                d.close / NULLIF(LAG(d.close, 5) OVER (
                    PARTITION BY i.ts_code ORDER BY i.trade_date
                ), 0) - 1 AS momentum_5d,
                -- 20 日新高
                CASE WHEN d.high >= MAX(d.high) OVER (
                    PARTITION BY i.ts_code
                    ORDER BY i.trade_date
                    ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
                ) THEN 1 ELSE 0 END AS is_20d_high
            FROM dc_indices i
            LEFT JOIN dc_daily d ON i.ts_code = d.ts_code AND i.trade_date = d.trade_date
        ),
        market_agg AS (
            -- 市场层面聚合（按日期）
            SELECT
                trade_date,
                -- 高度板指数（连板指数）
                MAX(CASE WHEN ts_code LIKE '%LB%' OR name LIKE '%连板%' THEN index_pct_change END) AS lb_index_pct,
                -- 打板指数
                MAX(CASE WHEN ts_code LIKE '%DB%' OR name LIKE '%打板%' THEN index_pct_change END) AS db_index_pct,
                -- 平均指数涨幅
                AVG(index_pct_change) AS avg_dc_pct,
                -- 指数上涨数量
                COUNT(*) FILTER (WHERE index_pct_change > 0) AS dc_up_count,
                COUNT(*) FILTER (WHERE index_pct_change < 0) AS dc_down_count
            FROM combined
            GROUP BY trade_date
        )
        SELECT
            c.trade_date,
            c.ts_code,
            c.name,
            c.index_pct_change,
            c.leading_pct,
            c.up_num,
            c.down_num,
            c.up_down_ratio,
            c.close,
            c.volume,
            c.amount,
            c.momentum_5d,
            c.is_20d_high,
            -- 市场层面
            m.lb_index_pct,
            m.db_index_pct,
            m.avg_dc_pct,
            m.dc_up_count,
            m.dc_down_count,
            -- 短线情绪信号
            CASE
                WHEN m.lb_index_pct > 2 AND m.db_index_pct > 2 THEN 'HOT'
                WHEN m.lb_index_pct < -2 AND m.db_index_pct < -2 THEN 'COLD'
                ELSE 'NORMAL'
            END AS short_term_sentiment,
            -- 血缘字段
            'rawdata.stock_dcindex,rawdata.stock_dcdaily' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM combined c
        LEFT JOIN market_agg m ON c.trade_date = m.trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_incremental_sql(self, start_date: str, end_date: str) -> str:
        """
        返回增量计算的 SELECT SQL。
        
        窗口函数需要回溯数据，所以读取范围会扩大。
        """
        from datetime import datetime, timedelta
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        lookback_date = (start_dt - timedelta(days=self._lookback_days)).strftime("%Y%m%d")

        return f"""
        WITH dc_indices AS (
            SELECT trade_date, ts_code, name, "leading" AS leading_name, leading_code,
                   pct_change, leading_pct, total_mv, turnover_rate, up_num, down_num
            FROM rawdata.stock_dcindex
            WHERE trade_date >= '{lookback_date}' AND trade_date <= '{end_date}'
        ),
        dc_daily AS (
            SELECT trade_date, ts_code, close, open, high, low, pct_change, volume, amount, turnover_rate
            FROM rawdata.stock_dcdaily
            WHERE trade_date >= '{lookback_date}' AND trade_date <= '{end_date}'
        ),
        combined AS (
            SELECT
                i.trade_date, i.ts_code, i.name, i.pct_change AS index_pct_change, i.leading_pct,
                i.up_num, i.down_num, i.total_mv, d.close, d.high, d.low, d.volume, d.amount,
                CASE WHEN i.down_num > 0 THEN i.up_num * 1.0 / i.down_num ELSE NULL END AS up_down_ratio,
                d.close / NULLIF(LAG(d.close, 5) OVER (PARTITION BY i.ts_code ORDER BY i.trade_date), 0) - 1 AS momentum_5d,
                CASE WHEN d.high >= MAX(d.high) OVER (PARTITION BY i.ts_code ORDER BY i.trade_date ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING)
                     THEN 1 ELSE 0 END AS is_20d_high
            FROM dc_indices i LEFT JOIN dc_daily d ON i.ts_code = d.ts_code AND i.trade_date = d.trade_date
        ),
        market_agg AS (
            SELECT trade_date,
                   MAX(CASE WHEN ts_code LIKE '%LB%' OR name LIKE '%连板%' THEN index_pct_change END) AS lb_index_pct,
                   MAX(CASE WHEN ts_code LIKE '%DB%' OR name LIKE '%打板%' THEN index_pct_change END) AS db_index_pct,
                   AVG(index_pct_change) AS avg_dc_pct,
                   COUNT(*) FILTER (WHERE index_pct_change > 0) AS dc_up_count,
                   COUNT(*) FILTER (WHERE index_pct_change < 0) AS dc_down_count
            FROM combined GROUP BY trade_date
        )
        SELECT c.trade_date, c.ts_code, c.name, c.index_pct_change, c.leading_pct,
               c.up_num, c.down_num, c.up_down_ratio, c.close, c.volume, c.amount,
               c.momentum_5d, c.is_20d_high, m.lb_index_pct, m.db_index_pct, m.avg_dc_pct,
               m.dc_up_count, m.dc_down_count,
               CASE WHEN m.lb_index_pct > 2 AND m.db_index_pct > 2 THEN 'HOT'
                    WHEN m.lb_index_pct < -2 AND m.db_index_pct < -2 THEN 'COLD'
                    ELSE 'NORMAL' END AS short_term_sentiment,
               'rawdata.stock_dcindex,rawdata.stock_dcdaily' AS _source_table,
               NOW() AS _processed_at, CURRENT_DATE AS _data_version
        FROM combined c LEFT JOIN market_agg m ON c.trade_date = m.trade_date
        WHERE c.trade_date >= '{start_date}' AND c.trade_date <= '{end_date}'
        """

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_dc_index_features_daily_trade_date "
            "ON features.mv_dc_index_features_daily (trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_dc_index_features_daily_ts_code "
            "ON features.mv_dc_index_features_daily (ts_code, trade_date)",
        ]
