"""
市场资金流向特征（日频）物化视图定义

设计思路：
- 聚合全市场个股资金流向，生成市场级资金流向特征
- 按 tushare 标准口径分类：小单(<5万)、中单(5-20万)、大单(20-100万)、特大单(≥100万)
- 刻画市场主力资金（大单+特大单）的行为特征

数据来源：
- tushare.stock_moneyflow: 个股资金流向（2010年起）

输出指标：
- 净流入额：全市场净流入（万元）
- 大单净流入：大单净流入额/比例
- 特大单净流入：特大单净流入额/比例
- 主力净流入：(大单+特大单)净流入额/比例
- 资金分布：各类型资金占比
- 资金集中度：主力资金占比

命名规范：
- 文件名: money_flow_daily.py
- 类名: MoneyFlowDailyMV
- recipe.name: money_flow_daily
- 输出表名: features.mv_money_flow_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class MoneyFlowDailyMV(BaseFeatureView):
    """市场资金流向特征物化视图（按资金类型分类聚合）"""

    name = "money_flow_daily"
    description = "全市场资金流向特征：按小单/中单/大单/特大单分类的资金流向聚合（日频）"
    source_tables = [
        "tushare.stock_moneyflow",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_money_flow_daily AS
        WITH 
        -- ========== 个股资金流向基础数据 ==========
        stock_flow AS (
            SELECT 
                trade_date,
                ts_code,
                -- 小单（<5万）
                COALESCE(buy_sm_amount, 0) AS buy_sm_amount,
                COALESCE(sell_sm_amount, 0) AS sell_sm_amount,
                COALESCE(buy_sm_amount, 0) - COALESCE(sell_sm_amount, 0) AS net_sm_amount,
                -- 中单（5-20万）
                COALESCE(buy_md_amount, 0) AS buy_md_amount,
                COALESCE(sell_md_amount, 0) AS sell_md_amount,
                COALESCE(buy_md_amount, 0) - COALESCE(sell_md_amount, 0) AS net_md_amount,
                -- 大单（20-100万）
                COALESCE(buy_lg_amount, 0) AS buy_lg_amount,
                COALESCE(sell_lg_amount, 0) AS sell_lg_amount,
                COALESCE(buy_lg_amount, 0) - COALESCE(sell_lg_amount, 0) AS net_lg_amount,
                -- 特大单（≥100万）
                COALESCE(buy_elg_amount, 0) AS buy_elg_amount,
                COALESCE(sell_elg_amount, 0) AS sell_elg_amount,
                COALESCE(buy_elg_amount, 0) - COALESCE(sell_elg_amount, 0) AS net_elg_amount,
                -- 总净流入
                COALESCE(net_mf_amount, 0) AS net_mf_amount
            FROM tushare.stock_moneyflow
            WHERE trade_date IS NOT NULL
        ),
        
        -- ========== 市场级聚合 ==========
        market_agg AS (
            SELECT 
                trade_date,
                COUNT(*) AS stock_count,
                -- 小单聚合
                SUM(buy_sm_amount) AS mkt_buy_sm_amount,
                SUM(sell_sm_amount) AS mkt_sell_sm_amount,
                SUM(net_sm_amount) AS mkt_net_sm_amount,
                -- 中单聚合
                SUM(buy_md_amount) AS mkt_buy_md_amount,
                SUM(sell_md_amount) AS mkt_sell_md_amount,
                SUM(net_md_amount) AS mkt_net_md_amount,
                -- 大单聚合
                SUM(buy_lg_amount) AS mkt_buy_lg_amount,
                SUM(sell_lg_amount) AS mkt_sell_lg_amount,
                SUM(net_lg_amount) AS mkt_net_lg_amount,
                -- 特大单聚合
                SUM(buy_elg_amount) AS mkt_buy_elg_amount,
                SUM(sell_elg_amount) AS mkt_sell_elg_amount,
                SUM(net_elg_amount) AS mkt_net_elg_amount,
                -- 总净流入
                SUM(net_mf_amount) AS mkt_net_mf_amount,
                -- 主力（大单+特大单）聚合
                SUM(net_lg_amount) + SUM(net_elg_amount) AS mkt_net_main_amount,
                SUM(buy_lg_amount) + SUM(buy_elg_amount) AS mkt_buy_main_amount,
                SUM(sell_lg_amount) + SUM(sell_elg_amount) AS mkt_sell_main_amount,
                -- 散户（小单+中单）聚合
                SUM(net_sm_amount) + SUM(net_md_amount) AS mkt_net_retail_amount,
                -- 净流入家数
                SUM(CASE WHEN net_mf_amount > 0 THEN 1 ELSE 0 END) AS net_inflow_count,
                SUM(CASE WHEN net_mf_amount < 0 THEN 1 ELSE 0 END) AS net_outflow_count,
                SUM(CASE WHEN net_lg_amount + net_elg_amount > 0 THEN 1 ELSE 0 END) AS main_inflow_count,
                SUM(CASE WHEN net_lg_amount + net_elg_amount < 0 THEN 1 ELSE 0 END) AS main_outflow_count
            FROM stock_flow
            GROUP BY trade_date
        ),
        
        -- ========== 计算派生指标 ==========
        final AS (
            SELECT 
                trade_date,
                stock_count,
                
                -- 各类型净流入额（万元）
                mkt_net_sm_amount,
                mkt_net_md_amount,
                mkt_net_lg_amount,
                mkt_net_elg_amount,
                mkt_net_mf_amount,
                mkt_net_main_amount,
                mkt_net_retail_amount,
                
                -- 主力资金占比（主力净流入 / 总成交）
                CASE 
                    WHEN mkt_buy_main_amount + mkt_sell_main_amount > 0 
                    THEN mkt_net_main_amount / (mkt_buy_main_amount + mkt_sell_main_amount) 
                    ELSE NULL 
                END AS main_net_ratio,
                
                -- 各类型买入金额（万元）
                mkt_buy_sm_amount,
                mkt_buy_md_amount,
                mkt_buy_lg_amount,
                mkt_buy_elg_amount,
                mkt_buy_main_amount,
                
                -- 各类型卖出金额（万元）
                mkt_sell_sm_amount,
                mkt_sell_md_amount,
                mkt_sell_lg_amount,
                mkt_sell_elg_amount,
                mkt_sell_main_amount,
                
                -- 主力资金集中度（主力成交 / 总成交）
                CASE 
                    WHEN (mkt_buy_sm_amount + mkt_sell_sm_amount + 
                          mkt_buy_md_amount + mkt_sell_md_amount +
                          mkt_buy_lg_amount + mkt_sell_lg_amount +
                          mkt_buy_elg_amount + mkt_sell_elg_amount) > 0
                    THEN (mkt_buy_main_amount + mkt_sell_main_amount) / 
                         (mkt_buy_sm_amount + mkt_sell_sm_amount + 
                          mkt_buy_md_amount + mkt_sell_md_amount +
                          mkt_buy_lg_amount + mkt_sell_lg_amount +
                          mkt_buy_elg_amount + mkt_sell_elg_amount)
                    ELSE NULL
                END AS main_turnover_ratio,
                
                -- 净流入家数与比例
                net_inflow_count,
                net_outflow_count,
                main_inflow_count,
                main_outflow_count,
                CASE 
                    WHEN stock_count > 0 
                    THEN net_inflow_count::FLOAT / stock_count 
                    ELSE NULL 
                END AS net_inflow_pct,
                CASE 
                    WHEN stock_count > 0 
                    THEN main_inflow_count::FLOAT / stock_count 
                    ELSE NULL 
                END AS main_inflow_pct,
                
                -- 主力与散户博弈（主力净流入 - 散户净流入）
                mkt_net_main_amount - mkt_net_retail_amount AS main_vs_retail_diff,
                
                -- 血缘
                'tushare.stock_moneyflow' AS _source_table,
                NOW() AS _processed_at,
                CURRENT_DATE AS _data_version
            FROM market_agg
        )
        SELECT * FROM final
        ORDER BY trade_date
        WITH DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list:
        """创建索引"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_money_flow_daily_trade_date ON features.mv_money_flow_daily (trade_date)",
        ]


__all__ = ["MoneyFlowDailyMV"]
