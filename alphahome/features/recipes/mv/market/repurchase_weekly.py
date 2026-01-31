"""
股票回购特征 - 周频物化视图

数据来源：tushare.stock_repurchase

设计理念：
- 公司回购是管理层看多信号，表明认为股价被低估
- 以周为聚合单位，统计回购公告数量和金额
- 区分回购进度（新公告/实施中/完成）
- 适合周度调仓的择时策略

周定义说明：
- 使用 DATE_TRUNC('week') 按 ISO 周（周一起始）聚合
- week_end 为该周周五日期（自然周口径）
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class RepurchaseWeeklyMV(BaseFeatureView):
    """股票回购周频物化视图
    
    核心指标：
    - 周回购公告数量（新增/完成）
    - 周回购金额（亿元）
    - 按进度分类统计
    - 大额回购（>1亿元）
    """

    name = "repurchase_weekly"
    description = "股票回购周频聚合（公告数/金额/进度分布）"
    source_tables = ["tushare.stock_repurchase"]
    refresh_strategy = "full"

    def get_create_sql(self) -> str:
        return """
        CREATE MATERIALIZED VIEW features.mv_repurchase_weekly AS
        SELECT 
            (DATE_TRUNC('week', ann_date)::date + INTERVAL '4 days')::date as week_end,
            
            -- 公告数量
            COUNT(*) as announcement_count,
            COUNT(DISTINCT ts_code) as stock_count,
            
            -- 按进度分类
            SUM(CASE WHEN proc IN ('董事会预案', '股东大会通过') THEN 1 ELSE 0 END) as new_plan_count,
            SUM(CASE WHEN proc = '实施中' THEN 1 ELSE 0 END) as in_progress_count,
            SUM(CASE WHEN proc = '完成' THEN 1 ELSE 0 END) as completed_count,
            SUM(CASE WHEN proc = '停止实施' THEN 1 ELSE 0 END) as cancelled_count,
            
            -- 回购金额（亿元）- amount 单位是元
            SUM(COALESCE(amount, 0)) / 1e8 as total_amount,
            SUM(CASE WHEN proc = '完成' THEN COALESCE(amount, 0) ELSE 0 END) / 1e8 as completed_amount,
            AVG(CASE WHEN amount > 0 THEN amount END) / 1e8 as avg_amount,
            
            -- 回购数量（万股）- volume 单位是股
            SUM(COALESCE(volume, 0)) / 1e4 as total_volume,
            
            -- 大额回购（>1亿元）
            SUM(CASE WHEN amount > 1e8 THEN 1 ELSE 0 END) as large_repurchase_count,
            SUM(CASE WHEN amount > 1e8 THEN amount ELSE 0 END) / 1e8 as large_repurchase_amount,
            
            -- 交易日数
            COUNT(DISTINCT ann_date) as trading_days,

            -- 血缘
            'tushare.stock_repurchase' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
            
        FROM tushare.stock_repurchase
        GROUP BY DATE_TRUNC('week', ann_date)
        ORDER BY week_end
        WITH DATA
        """

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_repurchase_weekly_week_end ON features.mv_repurchase_weekly (week_end)",
        ]
