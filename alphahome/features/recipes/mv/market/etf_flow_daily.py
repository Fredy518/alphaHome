"""
ETF 资金流特征（日频）物化视图定义

设计思路：
- 从 ETF 份额变动推算"资金净流"
- 用于观测"机构情绪"
- 动态从 fund_etf_basic 枚举跟踪宽基指数的所有 ETF

数据来源：
- tushare.fund_share: ETF 份额
- tushare.fund_nav: ETF 净值
- tushare.fund_etf_basic: ETF 基本信息（动态获取跟踪宽基指数的 ETF）

输出指标：
- 全市场宽基 ETF 资金净流（亿元）
- 净申赎规模占比
- 5/20 日累计净流入
- 资金动量信号

注意：
- 动态枚举跟踪以下指数的 ETF：
  - 上证50 (000016.SH)
  - 沪深300 (000300.SH/399300.SZ)
  - 中证500 (000905.SH/399905.SZ)
  - 中证1000 (000852.SH/399852.SZ)
  - 创业板指 (399006.SZ/399102.SZ/399673.SZ)
  - 科创50 (000688.SH)

命名规范：
- 文件名: etf_flow_daily.py
- 类名: ETFFlowDailyMV
- recipe.name: etf_flow_daily
- 输出表名: features.mv_etf_flow_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class ETFFlowDailyMV(BaseFeatureView):
    """ETF 资金流物化视图（日频，动态枚举宽基 ETF）"""

    name = "etf_flow_daily"
    description = "ETF 资金净流与动量信号（日频，动态枚举宽基 ETF）"
    source_tables = [
        "tushare.fund_share",
        "tushare.fund_nav",
        "tushare.fund_etf_basic",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_etf_flow_daily AS
        WITH 
        -- 目标指数清单（跟踪这些指数的 ETF 将被纳入统计）
        target_indexes AS (
            SELECT index_code FROM (VALUES
                ('000016.SH'),   -- 上证50
                ('000300.SH'),   -- 沪深300
                ('399300.SZ'),   -- 沪深300（深交所）
                ('000905.SH'),   -- 中证500
                ('399905.SZ'),   -- 中证500（深交所）
                ('000852.SH'),   -- 中证1000
                ('399852.SZ'),   -- 中证1000（深交所）
                ('399006.SZ'),   -- 创业板指
                ('399102.SZ'),   -- 创业板综
                ('399673.SZ'),   -- 创业板50
                ('000688.SH')    -- 科创50
            ) AS t(index_code)
        ),
        
        -- 动态获取跟踪目标指数的 ETF 列表（使用 fund_etf_basic）
        dynamic_etfs AS (
            SELECT 
                e.ts_code,
                e.index_code,
                e.name,
                e.list_date
            FROM tushare.fund_etf_basic e
            WHERE e.index_code IN (SELECT index_code FROM target_indexes)
              AND (e.status IS NULL OR e.status = 'L')  -- 仅在市 ETF
        ),
        
        -- 份额数据
        shares AS (
            SELECT 
                s.ts_code,
                s.trade_date,
                s.fd_share,  -- 份额（万份）
                e.index_code
            FROM tushare.fund_share s
            JOIN dynamic_etfs e ON s.ts_code = e.ts_code
            WHERE s.fd_share IS NOT NULL AND s.fd_share > 0
              AND s.trade_date >= e.list_date  -- 仅使用上市后的数据
        ),
        
        -- 净值数据
        navs AS (
            SELECT 
                ts_code,
                nav_date AS trade_date,
                unit_nav  -- 单位净值
            FROM tushare.fund_nav
            WHERE unit_nav IS NOT NULL AND unit_nav > 0
        ),
        
        -- 合并份额和净值
        merged AS (
            SELECT 
                s.ts_code,
                s.trade_date,
                s.fd_share,
                COALESCE(n.unit_nav, 1.0) AS unit_nav,
                s.fd_share * COALESCE(n.unit_nav, 1.0) / 10000 AS aum,  -- 规模（亿元）
                LAG(s.fd_share) OVER (PARTITION BY s.ts_code ORDER BY s.trade_date) AS prev_share,
                LAG(s.fd_share * COALESCE(n.unit_nav, 1.0) / 10000) OVER (PARTITION BY s.ts_code ORDER BY s.trade_date) AS prev_aum
            FROM shares s
            -- as-of join：用最近一个 <= trade_date 的 NAV（避免 NAV 缺失导致大面积 NULL）
            LEFT JOIN LATERAL (
                SELECT n2.unit_nav
                FROM navs n2
                WHERE n2.ts_code = s.ts_code
                  AND n2.trade_date <= s.trade_date
                ORDER BY n2.trade_date DESC
                LIMIT 1
            ) n ON TRUE
        ),
        
        -- 计算净申赎
        flows AS (
            SELECT 
                ts_code,
                trade_date,
                fd_share,
                unit_nav,
                aum,
                (fd_share - COALESCE(prev_share, fd_share)) * COALESCE(unit_nav, 1) / 10000 AS net_flow  -- 净申赎（亿元）
            FROM merged
        ),
        
        -- 按日期汇总
        daily_agg AS (
            SELECT 
                trade_date,
                SUM(aum) AS total_aum,
                SUM(net_flow) AS total_net_flow,
                COUNT(*) AS etf_count,
                SUM(CASE WHEN net_flow > 0 THEN net_flow ELSE 0 END) AS total_inflow,
                SUM(CASE WHEN net_flow < 0 THEN net_flow ELSE 0 END) AS total_outflow
            FROM flows
            WHERE trade_date IS NOT NULL
            GROUP BY trade_date
        ),
        
        -- 添加累计和动量
        with_momentum AS (
            SELECT 
                trade_date,
                total_aum,
                total_net_flow,
                etf_count,
                total_inflow,
                total_outflow,
                -- 5/20 日累计
                SUM(total_net_flow) OVER (ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS net_flow_5d,
                SUM(total_net_flow) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS net_flow_20d,
                -- 净流入占比
                total_net_flow / NULLIF(total_aum, 0) * 100 AS net_flow_ratio,
                -- 均值（动量基准）
                AVG(total_net_flow) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING) AS net_flow_avg20,
                STDDEV(total_net_flow) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING) AS net_flow_std20
            FROM daily_agg
        )
        
        SELECT 
            trade_date,
            total_aum,
            total_net_flow,
            etf_count,
            total_inflow,
            total_outflow,
            net_flow_5d,
            net_flow_20d,
            net_flow_ratio,
            net_flow_avg20,
            -- 动量信号（z-score > 1.5）
            CASE 
                WHEN net_flow_std20 > 0 AND 
                     (total_net_flow - net_flow_avg20) / net_flow_std20 > 1.5 
                THEN 1 
                ELSE 0 
            END AS flow_momentum_up,
            CASE 
                WHEN net_flow_std20 > 0 AND 
                     (total_net_flow - net_flow_avg20) / net_flow_std20 < -1.5 
                THEN 1 
                ELSE 0 
            END AS flow_momentum_down,
            -- 血缘
            'tushare.fund_share,tushare.fund_nav,tushare.fund_etf_basic' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM with_momentum
        ORDER BY trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_etf_flow_daily_trade_date "
            "ON features.mv_etf_flow_daily (trade_date)",
        ]
