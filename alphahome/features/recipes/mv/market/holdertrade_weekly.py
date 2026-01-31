"""
股东增减持特征 - 周频物化视图

数据来源：tushare.stock_holdertrade

设计理念：
- 股东增减持反映内部人对公司前景的判断
- 高管(G)、公司(C)、个人(P)三类股东行为含义不同
- 增持是看多信号，减持是看空信号
- 以周为聚合单位，统计净增减持金额和家数

周定义说明：
- 使用 DATE_TRUNC('week') 按 ISO 周（周一起始）聚合
- week_end 为该周周五日期（自然周口径）
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class HolderTradeWeeklyMV(BaseFeatureView):
    """股东增减持周频物化视图
    
    核心指标：
    - 周净增减持金额（增持-减持）
    - 增减持家数比
    - 按股东类型分层（高管/公司/个人）
    - 大额增减持（>1亿）
    """

    name = "holdertrade_weekly"
    description = "股东增减持周频聚合（增减持金额/家数/股东类型分层）"
    source_tables = ["tushare.stock_holdertrade"]
    refresh_strategy = "full"

    def get_create_sql(self) -> str:
        return """
        CREATE MATERIALIZED VIEW features.mv_holdertrade_weekly AS
        SELECT 
            (DATE_TRUNC('week', ann_date)::date + INTERVAL '4 days')::date as week_end,
            
            -- 总体统计
            COUNT(*) as record_count,
            COUNT(DISTINCT ts_code) as stock_count,
            
            -- 增持统计
            SUM(CASE WHEN in_de = 'IN' THEN 1 ELSE 0 END) as increase_count,
            SUM(CASE WHEN in_de = 'IN' THEN COALESCE(change_vol * avg_price, 0) ELSE 0 END) / 1e8 as increase_amount,
            COUNT(DISTINCT CASE WHEN in_de = 'IN' THEN ts_code END) as increase_stock_count,
            
            -- 减持统计
            SUM(CASE WHEN in_de = 'DE' THEN 1 ELSE 0 END) as decrease_count,
            SUM(CASE WHEN in_de = 'DE' THEN COALESCE(change_vol * avg_price, 0) ELSE 0 END) / 1e8 as decrease_amount,
            COUNT(DISTINCT CASE WHEN in_de = 'DE' THEN ts_code END) as decrease_stock_count,
            
            -- 高管(G)增减持
            SUM(CASE WHEN holder_type = 'G' AND in_de = 'IN' THEN 1 ELSE 0 END) as exec_increase_count,
            SUM(CASE WHEN holder_type = 'G' AND in_de = 'DE' THEN 1 ELSE 0 END) as exec_decrease_count,
            SUM(CASE WHEN holder_type = 'G' AND in_de = 'IN' THEN COALESCE(change_vol * avg_price, 0) ELSE 0 END) / 1e8 as exec_increase_amount,
            SUM(CASE WHEN holder_type = 'G' AND in_de = 'DE' THEN COALESCE(change_vol * avg_price, 0) ELSE 0 END) / 1e8 as exec_decrease_amount,
            
            -- 公司(C)增减持
            SUM(CASE WHEN holder_type = 'C' AND in_de = 'IN' THEN 1 ELSE 0 END) as corp_increase_count,
            SUM(CASE WHEN holder_type = 'C' AND in_de = 'DE' THEN 1 ELSE 0 END) as corp_decrease_count,
            SUM(CASE WHEN holder_type = 'C' AND in_de = 'IN' THEN COALESCE(change_vol * avg_price, 0) ELSE 0 END) / 1e8 as corp_increase_amount,
            SUM(CASE WHEN holder_type = 'C' AND in_de = 'DE' THEN COALESCE(change_vol * avg_price, 0) ELSE 0 END) / 1e8 as corp_decrease_amount,
            
            -- 个人(P)增减持
            SUM(CASE WHEN holder_type = 'P' AND in_de = 'IN' THEN 1 ELSE 0 END) as person_increase_count,
            SUM(CASE WHEN holder_type = 'P' AND in_de = 'DE' THEN 1 ELSE 0 END) as person_decrease_count,
            SUM(CASE WHEN holder_type = 'P' AND in_de = 'IN' THEN COALESCE(change_vol * avg_price, 0) ELSE 0 END) / 1e8 as person_increase_amount,
            SUM(CASE WHEN holder_type = 'P' AND in_de = 'DE' THEN COALESCE(change_vol * avg_price, 0) ELSE 0 END) / 1e8 as person_decrease_amount,
            
            -- 大额增减持（>1亿元）
            SUM(CASE WHEN in_de = 'IN' AND change_vol * avg_price > 1e8 THEN 1 ELSE 0 END) as large_increase_count,
            SUM(CASE WHEN in_de = 'DE' AND change_vol * avg_price > 1e8 THEN 1 ELSE 0 END) as large_decrease_count,
            SUM(CASE WHEN in_de = 'IN' AND change_vol * avg_price > 1e8 THEN change_vol * avg_price ELSE 0 END) / 1e8 as large_increase_amount,
            SUM(CASE WHEN in_de = 'DE' AND change_vol * avg_price > 1e8 THEN change_vol * avg_price ELSE 0 END) / 1e8 as large_decrease_amount,
            
            -- 交易日数
            COUNT(DISTINCT ann_date) as trading_days,

            -- 血缘
            'tushare.stock_holdertrade' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
            
        FROM tushare.stock_holdertrade
        WHERE change_vol IS NOT NULL
        GROUP BY DATE_TRUNC('week', ann_date)
        ORDER BY week_end
        WITH DATA
        """

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_holdertrade_weekly_week_end ON features.mv_holdertrade_weekly (week_end)",
        ]
