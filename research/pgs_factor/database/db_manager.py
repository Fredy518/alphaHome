"""
P/G/S因子数据库管理器
=====================

管理因子数据的存储、查询和更新操作。
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date
import logging
import json
from contextlib import contextmanager
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class PGSFactorDBManager:
    """P/G/S因子数据库管理器"""
    
    def __init__(self, context):
        """
        初始化数据库管理器
        
        Args:
            context: ResearchContext实例
        """
        self.context = context
        self.schema = 'pgs_factors'
        logger.info("PGSFactorDBManager initialized")
    
    @contextmanager
    def transaction(self):
        """事务管理器（同步模式，复用统一DBManager的连接）"""
        conn = None
        try:
            # 使用统一的DBManager（sync）获取线程本地连接
            dbm = self.context.db_manager
            conn = dbm._get_sync_connection()  # 使用受控的内部方法获取psycopg2连接
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"Transaction failed: {e}")
            raise
    
    def init_schema(self):
        """初始化数据库schema"""
        # 使用相对本文件的路径，避免工作目录影响
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file = os.path.join(current_dir, 'db_schema.sql')
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            with self.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(sql_content)
                logger.info("Database schema initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            raise
    
    # ========================================
    # P因子相关操作
    # ========================================
    
    def save_p_factor(self, factors: pd.DataFrame, ann_date: date, data_source: str):
        """
        保存P因子数据
        
        Args:
            factors: 包含P因子数据的DataFrame
            ann_date: 公告日期
            data_source: 数据来源 ('report'/'express'/'forecast')
        """
        if factors.empty:
            logger.warning("No P factor data to save")
            return
        
        insert_sql = """
            INSERT INTO pgs_factors.p_factor 
            (ts_code, calc_date, ann_date, data_source,
             gpa, roa_excl, roe_excl,
             rank_gpa, rank_roa, rank_roe,
             p_score,
             confidence, data_quality)
            VALUES (%s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s, %s)
            ON CONFLICT (ts_code, calc_date, data_source) 
            DO UPDATE SET
                ann_date = EXCLUDED.ann_date,
                gpa = EXCLUDED.gpa,
                roa_excl = EXCLUDED.roa_excl,
                roe_excl = EXCLUDED.roe_excl,
                rank_gpa = EXCLUDED.rank_gpa,
                rank_roa = EXCLUDED.rank_roa,
                rank_roe = EXCLUDED.rank_roe,
                p_score = EXCLUDED.p_score,
                confidence = EXCLUDED.confidence,
                data_quality = EXCLUDED.data_quality,
                updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                
                for _, row in factors.iterrows():
                    # 确定数据质量
                    data_quality = self._determine_data_quality(row, data_source)
                    # 每行使用自身的公告日期；若不存在则回退到函数参数 ann_date
                    row_ann_date = row.get('ann_date', ann_date)
                    # calc_date 优先取行内；没有则与 ann_date 对齐
                    row_calc_date = row.get('calc_date', row_ann_date)
                    
                    # 统一数值精度：入库前按两位小数四舍五入，避免混合精度
                    def r2(v):
                        try:
                            return round(float(v), 2) if v is not None else None
                        except Exception:
                            return None

                    values = (
                        row['ts_code'],
                        row_calc_date,
                        row_ann_date,
                        data_source,
                        r2(row.get('gpa')),
                        r2(row.get('roa_excl')),
                        r2(row.get('roe_excl')),
                        r2(row.get('rank_gpa')),
                        r2(row.get('rank_roa')),
                        r2(row.get('rank_roe')),
                        r2(row.get('p_score')),
                        r2(row.get('confidence', 0.5)),
                        data_quality
                    )
                    cursor.execute(insert_sql, values)
                
                logger.info(f"Saved {len(factors)} P factor records for {ann_date}")
                
        except Exception as e:
            logger.error(f"Failed to save P factors: {e}")
            raise

    def get_existing_p_factor_ts_codes(self, calc_date: date, data_source: str, stocks: List[str]) -> List[str]:
        """查询已存在的P因子记录的股票集合，用于避免重复计算

        Args:
            calc_date: 计算日期（通常等于公告日期）
            data_source: 数据来源
            stocks: 待检测股票池

        Returns:
            已存在记录的 ts_code 列表
        """
        query = """
            SELECT ts_code
            FROM pgs_factors.p_factor
            WHERE calc_date = %(calc_date)s
              AND data_source = %(data_source)s
              AND ts_code = ANY(%(stocks)s)
        """
        try:
            df = self.context.query_dataframe(query, {
                'calc_date': calc_date,
                'data_source': data_source,
                'stocks': stocks
            })
            return df['ts_code'].tolist() if df is not None and not df.empty else []
        except Exception as e:
            logger.error(f"Failed to get existing P factors: {e}")
            return []
    
    def get_latest_p_factors(self, stocks: List[str], as_of_date: date) -> pd.DataFrame:
        """
        获取最新的P因子数据
        
        Args:
            stocks: 股票列表
            as_of_date: 截止日期
            
        Returns:
            P因子DataFrame
        """
        query = """
            WITH ranked AS (
                SELECT *, 
                    ROW_NUMBER() OVER (
                        PARTITION BY ts_code 
                        ORDER BY calc_date DESC,
                            CASE data_source 
                                WHEN 'report' THEN 1 
                                WHEN 'express' THEN 2 
                                WHEN 'forecast' THEN 3 
                            END
                    ) as rn
                FROM pgs_factors.p_factor
                WHERE ts_code = ANY(%s) 
                    AND calc_date <= %s
            )
            SELECT * FROM ranked WHERE rn = 1
        """
        
        try:
            df = self.context.query_dataframe(
                query, 
                (stocks, as_of_date)
            )
            return df
        except Exception as e:
            logger.error(f"Failed to get P factors: {e}")
            return pd.DataFrame()
    
    def get_historical_p_scores(self, stocks: List[str], 
                               start_date: date, end_date: date) -> Dict[str, pd.DataFrame]:
        """
        获取历史P_score数据（用于G因子计算）
        
        Args:
            stocks: 股票列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            按日期分组的P_score数据字典
        """
        query = """
            WITH latest_per_date AS (
                SELECT DISTINCT ON (ts_code, calc_date)
                    ts_code, calc_date, p_score
                FROM pgs_factors.p_factor
                WHERE ts_code = ANY(%s)
                    AND calc_date BETWEEN %s AND %s
                ORDER BY ts_code, calc_date,
                    CASE data_source 
                        WHEN 'report' THEN 1 
                        WHEN 'express' THEN 2 
                        WHEN 'forecast' THEN 3 
                    END
            )
            SELECT * FROM latest_per_date
            ORDER BY calc_date, ts_code
        """
        
        try:
            df = self.context.query_dataframe(
                query,
                (stocks, start_date, end_date)
            )
            
            # 按日期分组
            result = {}
            for calc_date, group in df.groupby('calc_date'):
                date_str = calc_date.strftime('%Y%m%d')
                result[date_str] = group[['ts_code', 'p_score']].reset_index(drop=True)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get historical P scores: {e}")
            return {}

    def get_latest_report_ann_dates(self, stocks: List[str], as_of_date: date) -> pd.DataFrame:
        """查询各股票在指定日期前最新的正式报告公告日。

        Returns: DataFrame[ts_code, report_ann_date]
        """
        sql = """
            SELECT ts_code, MAX(ann_date) AS report_ann_date
            FROM tushare.fina_balancesheet
            WHERE ts_code = ANY(%(stocks)s)
              AND ann_date <= %(as_of)s
            GROUP BY ts_code
        """
        try:
            df = self.context.query_dataframe(sql, {
                'stocks': stocks,
                'as_of': as_of_date
            })
            return df if df is not None else pd.DataFrame(columns=['ts_code', 'report_ann_date'])
        except Exception as e:
            logger.error(f"Failed to get latest report ann dates: {e}")
            return pd.DataFrame(columns=['ts_code', 'report_ann_date'])

    def get_latest_stored_p_ann_dates(self, stocks: List[str], as_of_date: date, data_source: str = 'report') -> pd.DataFrame:
        """查询各股票在指定日期前，最近一次已存P记录的ann_date（按calc_date倒序）。

        Returns: DataFrame[ts_code, stored_ann_date]
        """
        sql = """
            SELECT DISTINCT ON (ts_code)
                ts_code,
                ann_date AS stored_ann_date,
                calc_date
            FROM pgs_factors.p_factor
            WHERE ts_code = ANY(%(stocks)s)
              AND data_source = %(source)s
              AND calc_date <= %(as_of)s
            ORDER BY ts_code, calc_date DESC
        """
        try:
            df = self.context.query_dataframe(sql, {
                'stocks': stocks,
                'source': data_source,
                'as_of': as_of_date
            })
            return df[['ts_code', 'stored_ann_date']] if df is not None and not df.empty else pd.DataFrame(columns=['ts_code', 'stored_ann_date'])
        except Exception as e:
            logger.error(f"Failed to get latest stored P ann dates: {e}")
            return pd.DataFrame(columns=['ts_code', 'stored_ann_date'])
    
    # ========================================
    # G因子相关操作
    # ========================================
    
    def save_g_factors(self, factors: pd.DataFrame, calc_date: date):
        """
        保存G因子数据
        
        Args:
            factors: G因子DataFrame
            calc_date: 计算日期
        """
        if factors.empty:
            logger.warning("No G factor data to save")
            return
        
        insert_sql = """
            INSERT INTO pgs_factors.g_factor
            (ts_code, calc_date, g_score, factor_a, factor_b,
             rank_a, rank_b, p_score_yoy, p_score_yoy_pct,
             data_periods, data_quality)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ts_code, calc_date)
            DO UPDATE SET
                g_score = EXCLUDED.g_score,
                factor_a = EXCLUDED.factor_a,
                factor_b = EXCLUDED.factor_b,
                rank_a = EXCLUDED.rank_a,
                rank_b = EXCLUDED.rank_b,
                p_score_yoy = EXCLUDED.p_score_yoy,
                p_score_yoy_pct = EXCLUDED.p_score_yoy_pct,
                data_periods = EXCLUDED.data_periods,
                data_quality = EXCLUDED.data_quality
        """
        
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                
                for _, row in factors.iterrows():
                    # 确定数据期数和质量
                    data_periods = row.get('data_periods', 0)
                    data_quality = 'high' if data_periods >= 20 else 'medium' if data_periods >= 12 else 'low'
                    
                    values = (
                        row['ts_code'],
                        calc_date,
                        row.get('g_score'),
                        row.get('factor_a'),
                        row.get('factor_b'),
                        row.get('rank_a'),
                        row.get('rank_b'),
                        row.get('p_score_yoy'),
                        row.get('p_score_yoy_pct'),
                        data_periods,
                        data_quality
                    )
                    cursor.execute(insert_sql, values)
                
                logger.info(f"Saved {len(factors)} G factor records for {calc_date}")
                
        except Exception as e:
            logger.error(f"Failed to save G factors: {e}")
            raise
    
    # ========================================
    # S因子相关操作
    # ========================================
    
    def save_s_factors(self, factors: pd.DataFrame, calc_date: date):
        """
        保存S因子数据
        
        Args:
            factors: S因子DataFrame
            calc_date: 计算日期
        """
        if factors.empty:
            logger.warning("No S factor data to save")
            return
        
        insert_sql = """
            INSERT INTO pgs_factors.s_factor
            (ts_code, calc_date, s_score, debt_ratio, beta, 
             roe_volatility, data_quality)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ts_code, calc_date)
            DO UPDATE SET
                s_score = EXCLUDED.s_score,
                debt_ratio = EXCLUDED.debt_ratio,
                beta = EXCLUDED.beta,
                roe_volatility = EXCLUDED.roe_volatility,
                data_quality = EXCLUDED.data_quality
        """
        
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                
                for _, row in factors.iterrows():
                    # 确定数据质量
                    data_quality = self._determine_s_factor_quality(row)
                    
                    values = (
                        row['ts_code'],
                        calc_date,
                        row.get('s_score'),
                        row.get('debt_ratio'),
                        row.get('beta'),
                        row.get('roe_volatility'),
                        data_quality
                    )
                    cursor.execute(insert_sql, values)
                
                logger.info(f"Saved {len(factors)} S factor records for {calc_date}")
                
        except Exception as e:
            logger.error(f"Failed to save S factors: {e}")
            raise

    # ========================================
    # 高级G因子批处理所需的新增接口
    # ========================================

    def save_g_subfactors(self, subfactors: pd.DataFrame, calc_date: date):
        """保存G因子子因子明细数据（需要表 pgs_factors.g_subfactors）
        
        Args:
            subfactors: 包含子因子明细的DataFrame，需含列：
                        ['ts_code','factor_name','factor_value','update_time']
            calc_date: 计算日期
        """
        if subfactors is None or subfactors.empty:
            logger.warning("No G subfactor data to save")
            return

        insert_sql = """
            INSERT INTO pgs_factors.g_subfactors
            (ts_code, calc_date, factor_name, factor_value, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ts_code, calc_date, factor_name)
            DO UPDATE SET
                factor_value = EXCLUDED.factor_value,
                updated_at = EXCLUDED.updated_at
        """

        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                for _, row in subfactors.iterrows():
                    cursor.execute(
                        insert_sql,
                        (
                            row['ts_code'],
                            calc_date,
                            row['factor_name'],
                            row['factor_value'],
                            row.get('update_time', datetime.now())
                        )
                    )
                logger.info(f"Saved {len(subfactors)} G subfactor records for {calc_date}")
        except Exception as e:
            logger.error(f"Failed to save G subfactors: {e}")
            raise

    def get_active_stocks(self, trade_date: str) -> List[str]:
        """获取指定交易日的活跃股票列表（基于dailybasic有数据的股票）"""
        try:
            # 允许 YYYY-MM-DD / YYYYMMDD，两者都处理为YYYYMMDD
            trade_date_str = str(trade_date).replace('-', '')
            query = """
                SELECT DISTINCT ts_code
                FROM tushare.stock_dailybasic
                WHERE trade_date = %(trade_date)s
                  AND total_mv IS NOT NULL
            """
            df = self.context.query_dataframe(query, {'trade_date': trade_date_str})
            return df['ts_code'].tolist() if not df.empty else []
        except Exception as e:
            logger.error(f"Failed to get active stocks: {e}")
            return []

    def get_financial_data(self, stocks: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """加载用于高级G因子计算的财务数据（营收、净利润及公告期）

        返回列：['ts_code','ann_date','end_date','revenue','n_income_attr_p']
        来自正式利润表与业绩快报的并集。
        """
        try:
            start_str = str(start_date).replace('-', '')
            end_str = str(end_date).replace('-', '')

            # 利润表
            income_sql = """
                SELECT ts_code,
                       ann_date,
                       end_date,
                       revenue,
                       n_income_attr_p,

                FROM tushare.fina_income
                WHERE ann_date BETWEEN %(start)s AND %(end)s
                  AND ts_code = ANY(%(stocks)s)
            """

            # 快报
            express_sql = """
                SELECT ts_code,
                       ann_date,
                       end_date,
                       revenue,
                       COALESCE(n_income_attr_p, n_income) AS n_income_attr_p,

                FROM tushare.fina_express
                WHERE ann_date BETWEEN %(start)s AND %(end)s
                  AND ts_code = ANY(%(stocks)s)
            """

            df_income = self.context.query_dataframe(income_sql, {
                'start': start_str,
                'end': end_str,
                'stocks': stocks,
            })
            df_express = self.context.query_dataframe(express_sql, {
                'start': start_str,
                'end': end_str,
                'stocks': stocks,
            })

            frames = []
            for df in (df_income, df_express):
                if df is not None and not df.empty:
                    # 统一数值类型
                    for col in ['revenue', 'n_income_attr_p']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    frames.append(df)

            if not frames:
                return pd.DataFrame()

            result = pd.concat(frames, ignore_index=True)
            return result
        except Exception as e:
            logger.error(f"Failed to get financial data: {e}")
            return pd.DataFrame()

    def update_processing_progress(self, process_type: str, progress_record: Dict[str, Any]):
        """记录高级批处理进度到 processing_log（复用已有表）"""
        upsert_sql = """
            INSERT INTO pgs_factors.processing_log
            (process_type, last_processed_date, records_processed, status, error_message)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (process_type)
            DO UPDATE SET
                last_processed_date = EXCLUDED.last_processed_date,
                records_processed = EXCLUDED.records_processed,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                updated_at = CURRENT_TIMESTAMP
        """

        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    upsert_sql,
                    (
                        process_type,
                        datetime.now(),
                        int(progress_record.get('total_processed', 0)),
                        'success' if int(progress_record.get('total_failed', 0)) == 0 else 'partial',
                        json.dumps(progress_record, ensure_ascii=False)
                    )
                )
                logger.info(f"Processing progress updated for {process_type}")
        except Exception as e:
            logger.error(f"Failed to update processing progress: {e}")
    
    # ========================================
    # 处理日志相关操作
    # ========================================
    
    def update_processing_log(self, process_type: str, 
                             last_date: datetime,
                             records: int,
                             status: str = 'success',
                             error: str = None):
        """
        更新处理日志
        
        Args:
            process_type: 处理类型
            last_date: 最后处理时间
            records: 处理记录数
            status: 状态
            error: 错误信息
        """
        upsert_sql = """
            INSERT INTO pgs_factors.processing_log
            (process_type, last_processed_date, records_processed, 
             status, error_message)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (process_type)
            DO UPDATE SET
                last_processed_date = EXCLUDED.last_processed_date,
                records_processed = EXCLUDED.records_processed,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(upsert_sql, (
                    process_type, last_date, records, status, error
                ))
                logger.info(f"Processing log updated for {process_type}")
        except Exception as e:
            logger.error(f"Failed to update processing log: {e}")
    
    def get_last_processed_date(self, process_type: str) -> Optional[datetime]:
        """
        获取最后处理时间
        
        Args:
            process_type: 处理类型
            
        Returns:
            最后处理时间
        """
        query = """
            SELECT last_processed_date 
            FROM pgs_factors.processing_log
            WHERE process_type = %s AND status = 'success'
        """
        
        try:
            result = self.context.query_dataframe(query, (process_type,))
            if not result.empty:
                return result.iloc[0]['last_processed_date']
            return None
        except Exception as e:
            logger.error(f"Failed to get last processed date: {e}")
            return None
    
    # ========================================
    # 数据质量监控
    # ========================================
    
    def save_quality_metrics(self, metric_date: date, metrics: Dict[str, Any]):
        """
        保存数据质量指标
        
        Args:
            metric_date: 指标日期
            metrics: 指标字典
        """
        insert_sql = """
            INSERT INTO pgs_factors.quality_metrics
            (metric_date, metric_type, metric_value, details)
            VALUES (%s, %s, %s, %s)
        """
        
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                
                for metric_type, value in metrics.items():
                    if isinstance(value, dict):
                        metric_value = value.get('value', 0)
                        details = json.dumps(value)
                    else:
                        metric_value = value
                        details = None
                    
                    cursor.execute(insert_sql, (
                        metric_date, metric_type, metric_value, details
                    ))
                
                logger.info(f"Saved {len(metrics)} quality metrics for {metric_date}")
                
        except Exception as e:
            logger.error(f"Failed to save quality metrics: {e}")
            raise

    # ========================================
    # PIT测试与持久化
    # ========================================

    def save_pit_income_quarterly(self, df: pd.DataFrame) -> None:
        """保存PIT单季度利润表快照到 pgs_factors.pit_income_quarterly

        期望列至少包含：['ts_code','end_date','ann_date','year','quarter',
                       'n_income_attr_p','revenue','operate_profit','total_profit','income_tax']
        """
        if df is None or df.empty:
            logger.warning("No PIT income records to save")
            return

        insert_sql = """
            INSERT INTO pgs_factors.pit_income_quarterly
            (ts_code, end_date, ann_date, data_source, year, quarter,
             n_income_attr_p, n_income, revenue, operate_profit, total_profit, income_tax,
             oper_cost, total_cogs,
             fin_exp, interest_expense,
             net_profit_mid, conversion_status)
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s)
            ON CONFLICT (ts_code, end_date, ann_date)
            DO UPDATE SET
                n_income_attr_p = EXCLUDED.n_income_attr_p,
                n_income = EXCLUDED.n_income,
                revenue = EXCLUDED.revenue,
                operate_profit = EXCLUDED.operate_profit,
                total_profit = EXCLUDED.total_profit,
                income_tax = EXCLUDED.income_tax,
                oper_cost = EXCLUDED.oper_cost,
                total_cogs = EXCLUDED.total_cogs,

                fin_exp = EXCLUDED.fin_exp,
                interest_expense = EXCLUDED.interest_expense,
                net_profit_mid = EXCLUDED.net_profit_mid,
                conversion_status = EXCLUDED.conversion_status,
                data_source = EXCLUDED.data_source,
                updated_at = CURRENT_TIMESTAMP
        """

        # 统一数值类型
        for col in ['n_income_attr_p', 'n_income', 'revenue', 'operate_profit', 'total_profit', 'income_tax',
                    'oper_cost', 'total_cogs',
                    'fin_exp', 'interest_expense', 'net_profit_mid']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        try:
            with self.transaction() as conn:
                cursor = conn.cursor()

                # 准备批量插入数据
                values = []
                for _, row in df.iterrows():
                    values.append((
                        row['ts_code'],
                        pd.to_datetime(row['end_date']).date(),
                        pd.to_datetime(row['ann_date']).date(),
                        row.get('data_source', 'report'),
                        int(row.get('year', pd.to_datetime(row['end_date']).year)),
                        int(row.get('quarter', pd.to_datetime(row['end_date']).quarter)),
                        round(float(row.get('n_income_attr_p') or 0), 2),
                        round(float(row.get('n_income') or 0), 2),
                        round(float(row.get('revenue') or 0), 2),
                        round(float(row.get('operate_profit') or 0), 2),
                        round(float(row.get('total_profit') or 0), 2),
                        round(float(row.get('income_tax') or 0), 2),
                        round(float(row.get('oper_cost') or 0), 2),
                        round(float(row.get('total_cogs') or 0), 2),
                        round(float(row.get('fin_exp') or 0), 2),
                        round(float(row.get('interest_expense') or 0), 2),
                        round(float(row.get('net_profit_mid') or 0), 2) if row.get('net_profit_mid') is not None else None,
                        row.get('conversion_status', 'SINGLE')
                    ))

                # 使用execute_values进行批量插入
                from psycopg2.extras import execute_values

                # 为execute_values重新构建SQL
                insert_values_sql = """
                INSERT INTO pgs_factors.pit_income_quarterly
                (ts_code, end_date, ann_date, data_source, year, quarter,
                 n_income_attr_p, n_income, revenue, operate_profit, total_profit, income_tax,
                 oper_cost, total_cogs,
                 fin_exp, interest_expense,
                 net_profit_mid, conversion_status)
                VALUES %s
                ON CONFLICT (ts_code, end_date, ann_date)
                DO UPDATE SET
                    n_income_attr_p = EXCLUDED.n_income_attr_p,
                    n_income = EXCLUDED.n_income,
                    revenue = EXCLUDED.revenue,
                    operate_profit = EXCLUDED.operate_profit,
                    total_profit = EXCLUDED.total_profit,
                    income_tax = EXCLUDED.income_tax,
                    oper_cost = EXCLUDED.oper_cost,
                    total_cogs = EXCLUDED.total_cogs,
                    fin_exp = EXCLUDED.fin_exp,
                    interest_expense = EXCLUDED.interest_expense,
                    net_profit_mid = EXCLUDED.net_profit_mid,
                    conversion_status = EXCLUDED.conversion_status,
                    data_source = EXCLUDED.data_source,
                    updated_at = CURRENT_TIMESTAMP
                """

                execute_values(
                    cursor,
                    insert_values_sql,
                    values,
                    template=None,
                    page_size=1000
                )

            logger.info(f"Saved PIT income quarterly records: {len(df)}")
        except Exception as e:
            logger.error(f"Failed to save PIT income quarterly: {e}")
            raise

    def save_pit_balance_quarterly(self, df: pd.DataFrame) -> None:
        """保存PIT资产负债表快照到 pgs_factors.pit_balance_quarterly

        期望列至少包含：['ts_code','end_date','ann_date','year','quarter','tot_assets','tot_liab','tot_equity']
        """
        if df is None or df.empty:
            logger.warning("No PIT balance records to save")
            return

        insert_sql = """
            INSERT INTO pgs_factors.pit_balance_quarterly
            (ts_code, end_date, ann_date, data_source, year, quarter,
             tot_assets, tot_liab, tot_equity,
             total_cur_assets, total_cur_liab, inventories)
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s)
            ON CONFLICT (ts_code, end_date, ann_date)
            DO UPDATE SET
                tot_assets = EXCLUDED.tot_assets,
                tot_liab = EXCLUDED.tot_liab,
                tot_equity = EXCLUDED.tot_equity,
                total_cur_assets = EXCLUDED.total_cur_assets,
                total_cur_liab = EXCLUDED.total_cur_liab,
                inventories = EXCLUDED.inventories,
                data_source = EXCLUDED.data_source,
                updated_at = CURRENT_TIMESTAMP
        """

        # 统一数值类型
        for col in ['tot_assets', 'tot_liab', 'tot_equity', 'total_cur_assets', 'total_cur_liab', 'inventories']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        try:
            with self.transaction() as conn:
                cursor = conn.cursor()

                # 准备批量插入数据
                values = []
                for _, row in df.iterrows():
                    values.append((
                        row['ts_code'],
                        pd.to_datetime(row['end_date']).date(),
                        pd.to_datetime(row['ann_date']).date(),
                        row.get('data_source', 'report'),
                        int(row.get('year', pd.to_datetime(row['end_date']).year)),
                        int(row.get('quarter', pd.to_datetime(row['end_date']).quarter)),
                        row.get('tot_assets'),
                        row.get('tot_liab'),
                        row.get('tot_equity'),
                        row.get('total_cur_assets'),
                        row.get('total_cur_liab'),
                        row.get('inventories')
                    ))

                # 使用execute_values进行批量插入
                from psycopg2.extras import execute_values

                # 为execute_values重新构建SQL
                insert_values_sql = """
                INSERT INTO pgs_factors.pit_balance_quarterly
                (ts_code, end_date, ann_date, data_source, year, quarter,
                 tot_assets, tot_liab, tot_equity, total_cur_assets, total_cur_liab, inventories)
                VALUES %s
                ON CONFLICT (ts_code, end_date, ann_date)
                DO UPDATE SET
                    tot_assets = EXCLUDED.tot_assets,
                    tot_liab = EXCLUDED.tot_liab,
                    tot_equity = EXCLUDED.tot_equity,
                    total_cur_assets = EXCLUDED.total_cur_assets,
                    total_cur_liab = EXCLUDED.total_cur_liab,
                    inventories = EXCLUDED.inventories,
                    data_source = EXCLUDED.data_source,
                    updated_at = CURRENT_TIMESTAMP
                """

                execute_values(
                    cursor,
                    insert_values_sql,
                    values,
                    template=None,
                    page_size=1000
                )

            logger.info(f"Saved PIT balance quarterly records: {len(df)}")
        except Exception as e:
            logger.error(f"Failed to save PIT balance quarterly: {e}")
            raise
    
    def get_factor_summary(self, stocks: List[str] = None, 
                          calc_date: date = None) -> pd.DataFrame:
        """
        获取因子汇总数据
        
        Args:
            stocks: 股票列表（可选）
            calc_date: 计算日期（可选）
            
        Returns:
            因子汇总DataFrame
        """
        conditions = []
        params = []
        
        if stocks:
            conditions.append("ts_code = ANY(%s)")
            params.append(stocks)
        
        if calc_date:
            conditions.append("calc_date = %s")
            params.append(calc_date)
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        query = f"""
            SELECT * FROM pgs_factors.factor_summary
            {where_clause}
            ORDER BY calc_date DESC, total_score DESC
        """
        
        try:
            df = self.context.query_dataframe(query, params)
            if df is None or df.empty:
                return pd.DataFrame()

            # 与应用层保持口径一致：基于Z-Score合成 total_score
            for col in ['p_score', 'g_score', 's_score']:
                if col in df.columns:
                    mean_val = df[col].mean()
                    std_val = df[col].std()
                    if std_val and std_val > 0:
                        df[f'{col}_zscore'] = (df[col] - mean_val) / std_val
                    else:
                        df[f'{col}_zscore'] = 0.0

            # 合成综合分
            df['total_score'] = (
                df.get('p_score_zscore', 0) * 0.4 +
                df.get('g_score_zscore', 0) * 0.3 +
                df.get('s_score_zscore', 0) * 0.3
            )

            # 排名（降序）
            if 'total_score' in df.columns:
                df['total_rank'] = df['total_score'].rank(ascending=False)
            if 'p_score' in df.columns:
                df['p_rank'] = df['p_score'].rank(ascending=False)
            if 'g_score' in df.columns:
                df['g_rank'] = df['g_score'].rank(ascending=False)
            if 's_score' in df.columns:
                df['s_rank'] = df['s_score'].rank(ascending=False)

            return df
        except Exception as e:
            logger.error(f"Failed to get factor summary: {e}")
            return pd.DataFrame()
    
    # ========================================
    # 辅助方法
    # ========================================
    
    def _determine_data_quality(self, row: pd.Series, data_source: str) -> str:
        """确定P因子数据质量"""
        if data_source == 'report':
            return 'high'
        elif data_source == 'express':
            return 'medium'
        else:
            return 'low'
    
    def _determine_s_factor_quality(self, row: pd.Series) -> str:
        """确定S因子数据质量"""
        # 基于数据完整性判断
        non_null_count = sum([
            not pd.isna(row.get('debt_ratio')),
            not pd.isna(row.get('beta')),
            not pd.isna(row.get('roe_volatility'))
        ])
        
        if non_null_count == 3:
            return 'high'
        elif non_null_count >= 2:
            return 'medium'
        else:
            return 'low'
    
    def cleanup_old_data(self, days_to_keep: int = 365):
        """
        清理旧数据
        
        Args:
            days_to_keep: 保留天数
        """
        delete_sql = """
            DELETE FROM pgs_factors.{table}
            WHERE created_at < CURRENT_DATE - INTERVAL '%s days'
        """
        
        tables = ['p_factor', 'g_factor', 's_factor', 'quality_metrics']
        
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                
                for table in tables:
                    cursor.execute(delete_sql.format(table=table), (days_to_keep,))
                    logger.info(f"Cleaned old data from {table}")
                    
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
            raise
