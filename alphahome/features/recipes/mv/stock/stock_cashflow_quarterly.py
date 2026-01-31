"""
现金流量表 PIT 展开（季度）

与 stock_income_quarterly / stock_balance_quarterly 形成财报三表完整 PIT 体系。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class StockCashflowQuarterlyMV(BaseFeatureView):
    """现金流量表（季度，PIT 时间窗口）"""

    name = "stock_cashflow_quarterly"
    description = "现金流量表 PIT 展开，含经营/投资/筹资活动现金流"
    source_tables = ["rawdata.fina_cashflow"]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_stock_cashflow_quarterly AS
        WITH ranked AS (
            SELECT
                ts_code,
                COALESCE(f_ann_date, ann_date) AS pit_ann_date,
                end_date AS report_period,
                report_type,
                -- 经营活动现金流
                net_profit,
                c_fr_sale_sg,           -- 销售商品收到的现金
                n_cashflow_act,         -- 经营活动现金流量净额
                -- 投资活动现金流
                c_pay_acq_const_fiolta, -- 购建固定资产支付的现金
                c_paid_invest,          -- 投资支付的现金
                n_cashflow_inv_act,     -- 投资活动现金流量净额
                -- 筹资活动现金流
                c_fr_borr,              -- 取得借款收到的现金
                c_recp_borrow,          -- 收到其他筹资活动相关的现金
                n_cash_flows_fnc_act,   -- 筹资活动现金流量净额
                -- 现金净增加额
                n_incr_cash_cash_equ,   -- 现金及现金等价物净增加额
                free_cashflow,          -- 企业自由现金流
                -- PIT 窗口：公告日为查询起始日，下一公告日-1 为查询截止日
                COALESCE(f_ann_date, ann_date) AS query_start_date,
                COALESCE(
                    LEAD(COALESCE(f_ann_date, ann_date)) OVER (
                        PARTITION BY ts_code ORDER BY COALESCE(f_ann_date, ann_date), end_date
                    ) - INTERVAL '1 day',
                    '2099-12-31'::DATE
                )::DATE AS query_end_date,
                ROW_NUMBER() OVER (
                    PARTITION BY ts_code, end_date
                    ORDER BY COALESCE(f_ann_date, ann_date) DESC
                ) AS rn
            FROM rawdata.fina_cashflow
            WHERE report_type = 1  -- 合并报表
        )
        SELECT
            ts_code,
            pit_ann_date AS ann_date,
            report_period,
            query_start_date,
            query_end_date,
            net_profit,
            c_fr_sale_sg,
            n_cashflow_act,
            c_pay_acq_const_fiolta,
            c_paid_invest,
            n_cashflow_inv_act,
            c_fr_borr,
            c_recp_borrow,
            n_cash_flows_fnc_act,
            n_incr_cash_cash_equ,
            free_cashflow,
            -- 血缘字段
            'rawdata.fina_cashflow' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM ranked
        WHERE rn = 1
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_cashflow_quarterly_ts_code "
            "ON features.mv_stock_cashflow_quarterly (ts_code, query_start_date, query_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_cashflow_quarterly_period "
            "ON features.mv_stock_cashflow_quarterly (report_period)",
        ]
