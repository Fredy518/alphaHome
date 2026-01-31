"""
公募基金重仓股（季度，PIT）

公募持仓是机构行为研究基础，用于抱团股识别、行业配置分析。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class FundHoldingsQuarterlyMV(BaseFeatureView):
    """公募基金重仓股"""

    name = "fund_holdings_quarterly"
    description = "公募基金重仓股持仓市值、比例、基金数量"
    source_tables = ["rawdata.fund_portfolio"]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_fund_holdings_quarterly AS
        WITH holdings AS (
            SELECT
                symbol AS ts_code,          -- 持仓股票代码
                ann_date,
                end_date,
                ts_code AS fund_code,       -- 基金代码
                mkv AS holding_value,       -- 持仓市值
                amount AS holding_shares,   -- 持仓股数
                stk_mkv_ratio               -- 占基金净值比例
            FROM rawdata.fund_portfolio
            WHERE symbol IS NOT NULL
              AND mkv IS NOT NULL
              AND mkv > 0
        ),
        stock_agg AS (
            -- 按股票、报告期聚合
            SELECT
                ts_code,
                end_date,
                MAX(ann_date) AS ann_date,
                COUNT(DISTINCT fund_code) AS fund_count,          -- 持有基金数
                SUM(holding_value) AS total_holding_value,        -- 总持仓市值
                SUM(holding_shares) AS total_holding_shares,      -- 总持仓股数
                AVG(stk_mkv_ratio) AS avg_fund_ratio,             -- 平均占基金比例
                MAX(stk_mkv_ratio) AS max_fund_ratio              -- 最大单基金占比
            FROM holdings
            GROUP BY ts_code, end_date
        ),
        with_chg AS (
            SELECT
                ts_code,
                end_date,
                ann_date,
                fund_count,
                total_holding_value,
                total_holding_shares,
                avg_fund_ratio,
                max_fund_ratio,
                -- 环比变化
                fund_count - LAG(fund_count) OVER (
                    PARTITION BY ts_code ORDER BY end_date
                ) AS fund_count_chg,
                total_holding_value - LAG(total_holding_value) OVER (
                    PARTITION BY ts_code ORDER BY end_date
                ) AS holding_value_chg,
                -- 机构抱团度（持有基金数的历史分位）
                PERCENT_RANK() OVER (
                    PARTITION BY ts_code ORDER BY fund_count
                ) AS fund_count_pctl
            FROM stock_agg
        )
        SELECT
            ts_code,
            end_date,
            ann_date,
            -- PIT 窗口
            ann_date AS query_start_date,
            COALESCE(
                LEAD(ann_date) OVER (PARTITION BY ts_code ORDER BY ann_date) - INTERVAL '1 day',
                '2099-12-31'::DATE
            )::DATE AS query_end_date,
            fund_count,
            fund_count_chg,
            total_holding_value,
            holding_value_chg,
            total_holding_shares,
            avg_fund_ratio,
            max_fund_ratio,
            fund_count_pctl,
            -- 抱团信号
            CASE
                WHEN fund_count_chg > 0 AND fund_count_pctl > 0.8 THEN 'CROWDED_UP'
                WHEN fund_count_chg < 0 AND fund_count_pctl < 0.3 THEN 'UNCROWDED_DOWN'
                WHEN fund_count_pctl > 0.9 THEN 'HIGHLY_CROWDED'
                ELSE 'NORMAL'
            END AS crowd_signal,
            -- 血缘字段
            'rawdata.fund_portfolio' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM with_chg
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_fund_holdings_quarterly_ts_code "
            "ON features.mv_fund_holdings_quarterly (ts_code, query_start_date, query_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_fund_holdings_quarterly_end_date "
            "ON features.mv_fund_holdings_quarterly (end_date)",
        ]
