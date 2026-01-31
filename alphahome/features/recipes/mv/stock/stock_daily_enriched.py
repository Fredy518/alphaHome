"""
每日行情增强物化视图定义

从 rawdata.stock_daily 和 rawdata.stock_dailybasic 读取数据，
合并为统一的每日行情+指标视图，输出到 features.mv_stock_daily_enriched。

数据流:
    rawdata.stock_daily + rawdata.stock_dailybasic → features.mv_stock_daily_enriched

命名规范（见 docs/architecture/features_module_design.md Section 3.2.1）:
- 文件名: stock_daily_enriched.py
- 类名: StockDailyEnrichedMV
- recipe.name: stock_daily_enriched
- 输出表名: features.mv_stock_daily_enriched
"""

from typing import Any, Dict, List

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class StockDailyEnrichedMV(BaseFeatureView):
    """
    每日行情增强物化视图。

    合并 stock_daily (OHLCV) 和 stock_dailybasic (估值/换手率) 数据，
    提供统一的每日股票数据视图。

    源表列说明:
    - stock_daily: ts_code, trade_date, open/high/low/close, volume, amount, pct_chg
    - stock_dailybasic: pe_ttm, pb, ps, dv_ttm, total_mv, circ_mv, turnover_rate
    """

    # 命名三件套
    name = "stock_daily_enriched"
    description = "每日行情增强物化视图 (OHLCV + 估值指标)"
    
    # 配置
    refresh_strategy = "full"
    source_tables: List[str] = ["rawdata.stock_daily", "rawdata.stock_dailybasic"]

    quality_checks: Dict[str, Any] = {
        "null_check": {
            "columns": ["ts_code", "trade_date", "close"],
            "threshold": 0.01,
        },
        "row_count_change": {
            "threshold": 0.2,
        },
    }

    def get_create_sql(self) -> str:
        """
        返回创建物化视图的 SQL。

        处理逻辑:
        1. LEFT JOIN 确保所有日行情都保留
        2. 合并 OHLCV 和估值指标
        3. 添加血缘元数据
        """
        # 输出表名遵循规范: mv_{recipe.name}
        sql = """
        CREATE MATERIALIZED VIEW features.mv_stock_daily_enriched AS
        SELECT
            d.ts_code,
            d.trade_date,
            
            -- OHLCV 数据
            d.open,
            d.high,
            d.low,
            d.close,
            d.pre_close,
            d.change,
            d.pct_chg,
            d.volume,
            d.amount,
            
            -- 估值指标
            b.pe AS pe,
            b.pe_ttm,
            b.pb,
            b.ps AS ps,
            b.ps_ttm,
            b.dv_ratio,
            b.dv_ttm,
            
            -- 市值
            b.total_mv,
            b.circ_mv,
            
            -- 流通股本
            b.total_share,
            b.float_share,
            b.free_share,
            
            -- 换手率
            b.turnover_rate,
            b.turnover_rate_f,
            b.volume_ratio,
            
            -- 血缘元数据
            'rawdata.stock_daily,rawdata.stock_dailybasic' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM rawdata.stock_daily d
        LEFT JOIN rawdata.stock_dailybasic b
            ON d.ts_code = b.ts_code
            AND d.trade_date = b.trade_date
        WHERE
            d.ts_code IS NOT NULL
            AND d.trade_date IS NOT NULL
            AND d.close IS NOT NULL
            AND d.close > 0
        WITH NO DATA;
        """
        return sql.strip()

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_daily_enriched_ts_code_trade_date "
            "ON features.mv_stock_daily_enriched (ts_code, trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_daily_enriched_trade_date "
            "ON features.mv_stock_daily_enriched (trade_date)",
        ]
