"""
物化视图数据库初始化

负责创建物化视图、索引和相关的元数据表。
"""

from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class MaterializedViewDatabaseInit:
    """物化视图数据库初始化"""
    
    @staticmethod
    def create_pit_financial_indicators_mv_sql() -> str:
        """
        创建 pit_financial_indicators_mv 物化视图的 SQL
        
        Returns:
            str: CREATE MATERIALIZED VIEW SQL 语句
        """
        sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS materialized_views.pit_financial_indicators_mv AS
        WITH normalized AS (
            SELECT
                CASE
                    WHEN ts_code ~ '\\\\.' THEN ts_code
                    WHEN ts_code LIKE '6%' THEN ts_code || '.SH'
                    WHEN ts_code LIKE '0%' THEN ts_code || '.SZ'
                    WHEN ts_code LIKE '3%' THEN ts_code || '.SZ'
                    ELSE ts_code
                END AS ts_code_std,
                CASE
                    WHEN ann_date::text ~ '^\\\\d{8}$' THEN to_date(ann_date::text, 'YYYYMMDD')
                    ELSE ann_date::date
                END AS ann_date_dt,
                CASE
                    WHEN end_date::text ~ '^\\\\d{8}$' THEN to_date(end_date::text, 'YYYYMMDD')
                    ELSE end_date::date
                END AS end_date_dt,
                pe_ttm,
                pb,
                ps,
                dv_ttm,
                total_mv
            FROM rawdata.pit_financial_indicators
            WHERE
                ts_code IS NOT NULL
                AND ann_date IS NOT NULL
                AND end_date IS NOT NULL
                AND pe_ttm IS NOT NULL
                AND pb IS NOT NULL
                AND pe_ttm BETWEEN -1000000 AND 1000000
                AND pb BETWEEN -1000000 AND 1000000
        )
        SELECT
            ts_code_std as ts_code,
            ann_date_dt as query_start_date,
            COALESCE(
                LEAD(ann_date_dt) OVER (PARTITION BY ts_code_std ORDER BY ann_date_dt) - INTERVAL '1 day',
                '2099-12-31'::date
            ) as query_end_date,
            end_date_dt as end_date,
            CAST(pe_ttm AS DECIMAL(10,2)) as pe_ttm,
            CAST(pb AS DECIMAL(10,2)) as pb,
            CAST(ps AS DECIMAL(10,2)) as ps,
            CAST(dv_ttm AS DECIMAL(10,2)) as dv_ttm,
            CAST(total_mv AS DECIMAL(15,2)) as total_mv,
            'rawdata.pit_financial_indicators' as _source_table,
            NOW() as _processed_at,
            CURRENT_DATE as _data_version
        FROM normalized
        WHERE
            ann_date_dt IS NOT NULL
            AND end_date_dt IS NOT NULL
        ORDER BY ts_code_std, ann_date_dt DESC;
        """
        return sql.strip()
    
    @staticmethod
    def create_pit_financial_indicators_mv_indexes_sql() -> str:
        """
        创建 pit_financial_indicators_mv 物化视图的索引
        
        Returns:
            str: CREATE INDEX SQL 语句（多个，用分号分隔）
        """
        sqls = [
            # 主键索引：trade_date + ts_code
            """
            CREATE INDEX IF NOT EXISTS idx_pit_financial_indicators_mv_trade_date_ts_code
            ON materialized_views.pit_financial_indicators_mv (query_start_date, ts_code);
            """,
            
            # 查询优化索引：ts_code + query_start_date
            """
            CREATE INDEX IF NOT EXISTS idx_pit_financial_indicators_mv_ts_code_query_start
            ON materialized_views.pit_financial_indicators_mv (ts_code, query_start_date);
            """,
            
            # 查询优化索引：query_start_date + query_end_date
            """
            CREATE INDEX IF NOT EXISTS idx_pit_financial_indicators_mv_query_range
            ON materialized_views.pit_financial_indicators_mv (query_start_date, query_end_date);
            """,
        ]
        
        return "\n".join(sqls)
    
    @staticmethod
    def create_pit_industry_classification_mv_sql() -> str:
        """
        创建 pit_industry_classification_mv 物化视图的 SQL
        
        Returns:
            str: CREATE MATERIALIZED VIEW SQL 语句
        """
        sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS materialized_views.pit_industry_classification_mv AS
        WITH normalized AS (
            SELECT
                CASE
                    WHEN ts_code ~ '\\\\.' THEN ts_code
                    WHEN ts_code LIKE '6%' THEN ts_code || '.SH'
                    WHEN ts_code LIKE '0%' THEN ts_code || '.SZ'
                    WHEN ts_code LIKE '3%' THEN ts_code || '.SZ'
                    ELSE ts_code
                END AS ts_code_std,
                CASE
                    WHEN obs_date::text ~ '^\\\\d{8}$' THEN to_date(obs_date::text, 'YYYYMMDD')
                    ELSE obs_date::date
                END AS obs_date_dt,
                TRIM(data_source) AS data_source_std,
                TRIM(industry_level1) as industry_level1,
                TRIM(industry_level2) as industry_level2,
                TRIM(industry_level3) as industry_level3,
                TRIM(industry_code1) as industry_code1,
                TRIM(industry_code2) as industry_code2,
                TRIM(industry_code3) as industry_code3,
                requires_special_gpa_handling,
                TRIM(gpa_calculation_method) as gpa_calculation_method,
                TRIM(special_handling_reason) as special_handling_reason,
                TRIM(data_quality) as data_quality
            FROM rawdata.pit_industry_classification
            WHERE
                ts_code IS NOT NULL
                AND obs_date IS NOT NULL
                AND industry_level1 IS NOT NULL
                AND industry_level2 IS NOT NULL
                AND data_source IS NOT NULL
        )
        SELECT
            ts_code_std as ts_code,
            obs_date_dt as query_start_date,
            COALESCE(
                LEAD(obs_date_dt) OVER (PARTITION BY ts_code_std, data_source_std ORDER BY obs_date_dt) - INTERVAL '1 day',
                '2099-12-31'::date
            ) as query_end_date,
            obs_date_dt as obs_date,
            data_source_std as data_source,
            industry_level1,
            industry_level2,
            industry_level3,
            industry_code1,
            industry_code2,
            industry_code3,
            requires_special_gpa_handling,
            gpa_calculation_method,
            special_handling_reason,
            data_quality,
            'rawdata.pit_industry_classification' as _source_table,
            NOW() as _processed_at,
            CURRENT_DATE as _data_version
        FROM normalized
        WHERE
            obs_date_dt IS NOT NULL
            AND data_source_std IS NOT NULL
            AND data_source_std <> ''
            AND industry_level1 IS NOT NULL AND industry_level1 <> ''
            AND industry_level2 IS NOT NULL AND industry_level2 <> ''
        ORDER BY ts_code_std, data_source_std, obs_date_dt DESC;
        """
        return sql.strip()
    
    @staticmethod
    def create_pit_industry_classification_mv_indexes_sql() -> str:
        """
        创建 pit_industry_classification_mv 物化视图的索引
        
        Returns:
            str: CREATE INDEX SQL 语句（多个，用分号分隔）
        """
        sqls = [
            # 主键索引：obs_date + ts_code + data_source
            """
            CREATE INDEX IF NOT EXISTS idx_pit_industry_classification_mv_obs_date_ts_code
            ON materialized_views.pit_industry_classification_mv (query_start_date, ts_code, data_source);
            """,
            
            # 查询优化索引：ts_code + data_source + query_start_date
            """
            CREATE INDEX IF NOT EXISTS idx_pit_industry_classification_mv_ts_code_source
            ON materialized_views.pit_industry_classification_mv (ts_code, data_source, query_start_date);
            """,
            
            # 查询优化索引：query_start_date + query_end_date
            """
            CREATE INDEX IF NOT EXISTS idx_pit_industry_classification_mv_query_range
            ON materialized_views.pit_industry_classification_mv (query_start_date, query_end_date);
            """,
        ]
        
        return "\n".join(sqls)
    
    @staticmethod
    def create_materialized_views_metadata_table_sql() -> str:
        """
        创建物化视图元数据表
        
        Returns:
            str: CREATE TABLE SQL 语句
        """
        sql = """
        CREATE TABLE IF NOT EXISTS materialized_views.materialized_views_metadata (
            view_name VARCHAR(255) PRIMARY KEY,
            view_schema VARCHAR(255),
            source_tables TEXT,
            refresh_strategy VARCHAR(50),
            last_refresh_time TIMESTAMP,
            refresh_status VARCHAR(50),
            row_count INTEGER,
            refresh_duration_seconds FLOAT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """
        return sql.strip()
    
    @staticmethod
    def create_materialized_views_quality_checks_table_sql() -> str:
        """
        创建物化视图数据质量检查结果表
        
        Returns:
            str: CREATE TABLE SQL 语句
        """
        sql = """
        CREATE TABLE IF NOT EXISTS materialized_views.materialized_views_quality_checks (
            id SERIAL PRIMARY KEY,
            view_name VARCHAR(255),
            check_name VARCHAR(255),
            check_status VARCHAR(50),
            check_message TEXT,
            check_details JSONB,
            checked_at TIMESTAMP DEFAULT NOW(),
            FOREIGN KEY (view_name) REFERENCES materialized_views.materialized_views_metadata(view_name)
        );
        """
        return sql.strip()
    
    @staticmethod
    def create_materialized_views_alerts_table_sql() -> str:
        """
        创建物化视图告警表
        
        Returns:
            str: CREATE TABLE SQL 语句
        """
        sql = """
        CREATE TABLE IF NOT EXISTS materialized_views.materialized_views_alerts (
            id SERIAL PRIMARY KEY,
            view_name VARCHAR(255),
            alert_type VARCHAR(100),
            severity VARCHAR(50),
            message TEXT,
            details JSONB,
            acknowledged BOOLEAN DEFAULT FALSE,
            acknowledged_by VARCHAR(255),
            acknowledged_at TIMESTAMP,
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        return sql.strip()
    
    @staticmethod
    def create_materialized_views_alerts_indexes_sql() -> str:
        """
        创建物化视图告警表的索引
        
        Returns:
            str: CREATE INDEX SQL 语句（多个，用分号分隔）
        """
        sqls = [
            # 查询优化索引：view_name + created_at
            """
            CREATE INDEX IF NOT EXISTS idx_materialized_views_alerts_view_name_created
            ON materialized_views.materialized_views_alerts (view_name, created_at DESC);
            """,
            
            # 查询优化索引：severity + created_at
            """
            CREATE INDEX IF NOT EXISTS idx_materialized_views_alerts_severity_created
            ON materialized_views.materialized_views_alerts (severity, created_at DESC);
            """,
            
            # 查询优化索引：acknowledged + created_at
            """
            CREATE INDEX IF NOT EXISTS idx_materialized_views_alerts_acknowledged
            ON materialized_views.materialized_views_alerts (acknowledged, created_at DESC);
            """,
            
            # 查询优化索引：alert_type + created_at
            """
            CREATE INDEX IF NOT EXISTS idx_materialized_views_alerts_alert_type
            ON materialized_views.materialized_views_alerts (alert_type, created_at DESC);
            """,
        ]
        
        return "\n".join(sqls)
    
    @staticmethod
    def create_schema_sql() -> str:
        """
        创建 materialized_views schema
        
        Returns:
            str: CREATE SCHEMA SQL 语句
        """
        sql = """
        CREATE SCHEMA IF NOT EXISTS materialized_views;
        """
        return sql.strip()
    
    @staticmethod
    def create_market_technical_indicators_mv_sql() -> str:
        """
        创建 market_technical_indicators_mv 物化视图的 SQL
        
        Returns:
            str: CREATE MATERIALIZED VIEW SQL 语句
        """
        sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS materialized_views.market_technical_indicators_mv AS
        WITH normalized AS (
            SELECT
                CASE
                    WHEN ts_code ~ '\\\\.' THEN ts_code
                    WHEN ts_code LIKE '6%' THEN ts_code || '.SH'
                    WHEN ts_code LIKE '0%' THEN ts_code || '.SZ'
                    WHEN ts_code LIKE '3%' THEN ts_code || '.SZ'
                    ELSE ts_code
                END AS ts_code_std,
                CASE
                    WHEN trade_date::text ~ '^\\\\d{8}$' THEN to_date(trade_date::text, 'YYYYMMDD')
                    ELSE trade_date::date
                END AS trade_date_dt,
                CAST(close AS DECIMAL(10,2)) as close,
                CAST(vol AS BIGINT) as vol,
                CAST(turnover_rate AS DECIMAL(10,4)) as turnover_rate
            FROM rawdata.market_technical
            WHERE
                ts_code IS NOT NULL
                AND trade_date IS NOT NULL
                AND close IS NOT NULL
                AND vol IS NOT NULL
                AND close > 0
                AND vol >= 0
        )
        SELECT
            ts_code_std as ts_code,
            trade_date_dt as trade_date,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as close_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY close) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as close_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY close) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as close_q75,
            AVG(close) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as close_mean,
            STDDEV(close) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as close_std,
            MIN(close) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as close_min,
            MAX(close) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as close_max,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as vol_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY vol) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as vol_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vol) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as vol_q75,
            AVG(vol) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as vol_mean,
            STDDEV(vol) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as vol_std,
            SUM(vol) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as vol_total,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY turnover_rate) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as turnover_rate_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY turnover_rate) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as turnover_rate_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY turnover_rate) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as turnover_rate_q75,
            AVG(turnover_rate) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as turnover_rate_mean,
            STDDEV(turnover_rate) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as turnover_rate_std,
            CAST(MAX(close) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) / NULLIF(MIN(close) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0) AS DECIMAL(10,4)) as high_price_ratio,
            CAST(MAX(vol) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) / NULLIF(MIN(vol) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0) AS DECIMAL(10,4)) as high_vol_ratio,
            CAST(MAX(turnover_rate) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) / NULLIF(MIN(turnover_rate) OVER (PARTITION BY ts_code_std ORDER BY trade_date_dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0) AS DECIMAL(10,4)) as high_turnover_ratio,
            'rawdata.market_technical' as _source_table,
            NOW() as _processed_at,
            CURRENT_DATE as _data_version
        FROM normalized
        WHERE trade_date_dt IS NOT NULL
        ORDER BY ts_code_std, trade_date_dt DESC;
        """
        return sql.strip()
    
    @staticmethod
    def create_market_technical_indicators_mv_indexes_sql() -> str:
        """
        创建 market_technical_indicators_mv 物化视图的索引
        
        Returns:
            str: CREATE INDEX SQL 语句（多个，用分号分隔）
        """
        sqls = [
            # 主键索引：trade_date + ts_code
            """
            CREATE INDEX IF NOT EXISTS idx_market_technical_indicators_mv_trade_date_ts_code
            ON materialized_views.market_technical_indicators_mv (trade_date, ts_code);
            """,
            
            # 查询优化索引：ts_code + trade_date
            """
            CREATE INDEX IF NOT EXISTS idx_market_technical_indicators_mv_ts_code_trade_date
            ON materialized_views.market_technical_indicators_mv (ts_code, trade_date);
            """,
        ]
        
        return "\n".join(sqls)
    
    @staticmethod
    def create_sector_aggregation_mv_sql() -> str:
        """
        创建 sector_aggregation_mv 物化视图的 SQL
        
        Returns:
            str: CREATE MATERIALIZED VIEW SQL 语句
        """
        sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS materialized_views.sector_aggregation_mv AS
        WITH normalized AS (
            SELECT
                CASE
                    WHEN sd.ts_code ~ '\\\\.' THEN sd.ts_code
                    WHEN sd.ts_code LIKE '6%' THEN sd.ts_code || '.SH'
                    WHEN sd.ts_code LIKE '0%' THEN sd.ts_code || '.SZ'
                    WHEN sd.ts_code LIKE '3%' THEN sd.ts_code || '.SZ'
                    ELSE sd.ts_code
                END AS ts_code_std,
                CASE
                    WHEN sd.trade_date::text ~ '^\\\\d{8}$' THEN to_date(sd.trade_date::text, 'YYYYMMDD')
                    ELSE sd.trade_date::date
                END AS trade_date_dt,
                TRIM(ic.industry_code) as industry_code,
                TRIM(ic.industry_name) as industry_name,
                CAST(sd.close AS DECIMAL(10,2)) as close,
                CAST(sd.vol AS BIGINT) as vol,
                CAST(sd.turnover_rate AS DECIMAL(10,4)) as turnover_rate,
                CAST(sd.amount AS DECIMAL(15,2)) as amount
            FROM rawdata.stock_daily sd
            LEFT JOIN rawdata.industry_classification ic ON sd.ts_code = ic.ts_code
            WHERE
                sd.ts_code IS NOT NULL
                AND sd.trade_date IS NOT NULL
                AND sd.close IS NOT NULL
                AND sd.vol IS NOT NULL
                AND sd.close > 0
                AND sd.vol >= 0
                AND ic.industry_code IS NOT NULL
        )
        SELECT
            trade_date_dt as trade_date,
            industry_code,
            industry_name,
            COUNT(DISTINCT ts_code_std) as stock_count,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close) as close_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY close) as close_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY close) as close_q75,
            AVG(close) as close_mean,
            STDDEV(close) as close_std,
            MIN(close) as close_min,
            MAX(close) as close_max,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol) as vol_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY vol) as vol_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vol) as vol_q75,
            AVG(vol) as vol_mean,
            STDDEV(vol) as vol_std,
            SUM(vol) as vol_total,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY turnover_rate) as turnover_rate_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY turnover_rate) as turnover_rate_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY turnover_rate) as turnover_rate_q75,
            AVG(turnover_rate) as turnover_rate_mean,
            STDDEV(turnover_rate) as turnover_rate_std,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as amount_median,
            AVG(amount) as amount_mean,
            SUM(amount) as amount_total,
            CAST(MAX(close) / NULLIF(MIN(close), 0) AS DECIMAL(10,4)) as high_price_ratio,
            CAST(MAX(vol) / NULLIF(MIN(vol), 0) AS DECIMAL(10,4)) as high_vol_ratio,
            CAST(MAX(turnover_rate) / NULLIF(MIN(turnover_rate), 0) AS DECIMAL(10,4)) as high_turnover_ratio,
            'rawdata.stock_daily, rawdata.industry_classification' as _source_tables,
            NOW() as _processed_at,
            CURRENT_DATE as _data_version
        FROM normalized
        WHERE trade_date_dt IS NOT NULL AND industry_code IS NOT NULL
        GROUP BY trade_date_dt, industry_code, industry_name
        ORDER BY trade_date_dt DESC, industry_code;
        """
        return sql.strip()
    
    @staticmethod
    def create_sector_aggregation_mv_indexes_sql() -> str:
        """
        创建 sector_aggregation_mv 物化视图的索引
        
        Returns:
            str: CREATE INDEX SQL 语句（多个，用分号分隔）
        """
        sqls = [
            # 主键索引：trade_date + industry_code
            """
            CREATE INDEX IF NOT EXISTS idx_sector_aggregation_mv_trade_date_industry
            ON materialized_views.sector_aggregation_mv (trade_date, industry_code);
            """,
            
            # 查询优化索引：industry_code + trade_date
            """
            CREATE INDEX IF NOT EXISTS idx_sector_aggregation_mv_industry_trade_date
            ON materialized_views.sector_aggregation_mv (industry_code, trade_date);
            """,
        ]
        
        return "\n".join(sqls)
    
    @staticmethod
    def get_all_init_sqls() -> list:
        """
        获取所有初始化 SQL 语句
        
        Returns:
            list: SQL 语句列表
        """
        return [
            MaterializedViewDatabaseInit.create_schema_sql(),
            MaterializedViewDatabaseInit.create_materialized_views_metadata_table_sql(),
            MaterializedViewDatabaseInit.create_materialized_views_quality_checks_table_sql(),
            MaterializedViewDatabaseInit.create_materialized_views_alerts_table_sql(),
            MaterializedViewDatabaseInit.create_materialized_views_alerts_indexes_sql(),
            MaterializedViewDatabaseInit.create_pit_financial_indicators_mv_sql(),
            MaterializedViewDatabaseInit.create_pit_financial_indicators_mv_indexes_sql(),
            MaterializedViewDatabaseInit.create_pit_industry_classification_mv_sql(),
            MaterializedViewDatabaseInit.create_pit_industry_classification_mv_indexes_sql(),
            MaterializedViewDatabaseInit.create_market_technical_indicators_mv_sql(),
            MaterializedViewDatabaseInit.create_market_technical_indicators_mv_indexes_sql(),
            MaterializedViewDatabaseInit.create_sector_aggregation_mv_sql(),
            MaterializedViewDatabaseInit.create_sector_aggregation_mv_indexes_sql(),
        ]
