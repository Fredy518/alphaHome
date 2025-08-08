"""
P/G/S因子数据加载器
=====================

负责从AlphaHome数据库中，为P/G/S因子计算准备所有必需的Point-in-Time数据。

核心功能:
- 加载正式财报、业绩快报、业绩预告、行情数据
- 统一处理时间戳，确保数据在"时间点"上的准确性
- 提供面向特定调仓日的数据切片功能
"""
import pandas as pd
import logging
from typing import Dict, List, Optional
from research.tools.context import ResearchContext

logger = logging.getLogger(__name__)

class PGSDataLoader:
    """
    为P/G/S因子计算加载和准备数据
    """
    
    _TABLE_NAMES = {
        # 财务数据直接使用 PIT 统一视图，淘汰tushare原始表路径
        'financial_view_pit': 'pgs_factors.v_pit_financial_quarterly',
        'market_price': 'tushare.stock_daily',
        'market_dailybasic': 'tushare.stock_dailybasic',
        'market_index': None,
        'trading_calendar': None
    }

    def __init__(self, context: ResearchContext):
        """
        初始化数据加载器

        Args:
            context: ResearchContext实例，用于数据库访问
        """
        self.context = context
        self.db = context.db_manager
        # 从全局配置覆盖表名（特别是market_index与trading_calendar）
        try:
            cfg = context.config
            self._TABLE_NAMES['market_index'] = cfg['data_source'].get('market_index', 'tushare.index_factor_pro')
            self._TABLE_NAMES['trading_calendar'] = cfg['data_source'].get('trading_calendar', 'tushare.others_calendar')
        except Exception:
            # 兜底
            if self._TABLE_NAMES['market_index'] is None:
                self._TABLE_NAMES['market_index'] = 'tushare.index_factor_pro'
            if self._TABLE_NAMES['trading_calendar'] is None:
                self._TABLE_NAMES['trading_calendar'] = 'tushare.others_calendar'
        logger.info("PGSDataLoader initialized.")

    def load_all_data(self, end_date: str, stocks: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        加载指定日期之前的所有相关数据

        Args:
            end_date: 数据加载的截止日期（调仓日）
            stocks: 股票列表，如果为None则加载所有股票

        Returns:
            一个字典，包含所有加载的数据表
        """
        logger.info(f"Loading all point-in-time data before {end_date}...")
        
        data = {}
        data['financial_report'] = self._load_financial_reports(end_date, stocks)
        data['financial_income'] = self._load_financial_income(end_date, stocks)
        data['market_price'] = self._load_market_prices(end_date, stocks)
        data['market_dailybasic'] = self._load_market_dailybasic(end_date, stocks)
        data['market_index'] = self._load_index_prices(end_date)
        
        # 在单季/PIT数据基础上，对部分字段进行按股票排序后的前向填充（ffill），
        # 用于处理 express/forecast 仅更新个别科目、其他科目仍为空的情况。
        # 注意：不对 n_income_attr_p 进行填充，避免扭曲分子TTM。
        try:
            data['financial_income'] = self._ffill_by_stock(
                data['financial_income'],
                columns=['revenue', 'operate_profit', 'total_profit', 'income_tax']
            )
        except Exception:
            pass

        try:
            data['financial_report'] = self._ffill_by_stock(
                data['financial_report'],
                columns=['tot_assets', 'tot_liab', 'tot_equity']
            )
        except Exception:
            pass

        logger.info("All data loaded successfully.")
        return data

    def _load_financial_reports(self, end_date: str, stocks: Optional[List[str]]) -> pd.DataFrame:
        """加载资产负债表时点数据（来自 PIT 统一视图）"""
        view = self._TABLE_NAMES['financial_view_pit']
        query = f"""
        SELECT ts_code, ann_date, end_date,
               tot_assets AS tot_assets,
               tot_liab   AS tot_liab,
               tot_equity AS tot_equity
        FROM {view}
        WHERE ann_date <= %(end_date)s
        """
        params = {'end_date': end_date}

        if stocks:
            query += " AND ts_code = ANY(%(stocks)s)"
            params['stocks'] = stocks

        df = self.context.query_dataframe(query, params)

        # 数值列
        numeric_columns = ['tot_assets', 'tot_liab', 'tot_equity']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 标记为单季/时点口径，供 PIT 处理器跳过重复转换
        if not df.empty:
            df['is_single_quarter'] = True

        logger.info(f"Loaded {len(df)} PIT balance snapshots from view.")
        return df
    
    def _load_financial_income(self, end_date: str, stocks: Optional[List[str]]) -> pd.DataFrame:
        """加载利润表单季数据（来自 PIT 统一视图）"""
        view = self._TABLE_NAMES['financial_view_pit']
        query = f"""
        SELECT ts_code, ann_date, end_date,
               n_income_attr_p,
               n_income,
               revenue, operate_profit,
               total_profit, income_tax,
               oper_cost, total_cogs,
               n_income_attr_p_non_recurring,
               fin_exp, interest_expense,
               net_profit_mid,
               -- 将累计净利暴露为 n_income，供预测拆分使用
               n_income_cum AS n_income
        FROM {view}
        WHERE ann_date <= %(end_date)s
        """
        params = {'end_date': end_date}

        if stocks:
            query += " AND ts_code = ANY(%(stocks)s)"
            params['stocks'] = stocks

        df = self.context.query_dataframe(query, params)

        # 数值列
        numeric_columns = [
            'n_income_attr_p', 'n_income', 'revenue', 'operate_profit', 'total_profit', 'income_tax',
            'oper_cost', 'total_cogs', 'n_income_attr_p_non_recurring',
            'fin_exp', 'interest_expense', 'net_profit_mid', 'n_income'
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 标记为单季口径
        if not df.empty:
            df['is_single_quarter'] = True

        logger.info(f"Loaded {len(df)} PIT income quarters from view.")
        return df

    def _ffill_by_stock(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """按股票、时间排序后对指定列执行前向填充（仅在列存在时），保持PIT约束。

        - 排序：ts_code, end_date, ann_date（确保同报告期内按公告时序）
        - 分组：ts_code
        - 填充：仅对提供的列执行 ffill，保留原列不存在的情况
        """
        if df is None or df.empty:
            return df
        work = df.copy()
        sort_cols = [c for c in ['ts_code', 'end_date', 'ann_date'] if c in work.columns]
        if sort_cols:
            work = work.sort_values(sort_cols)
        cols = [c for c in columns if c in work.columns]
        if not cols:
            return work
        work[cols] = work.groupby('ts_code', dropna=False)[cols].ffill()
        return work

    def _load_market_prices(self, end_date: str, stocks: Optional[List[str]]) -> pd.DataFrame:
        """加载股价行情"""
        query = f"""
        SELECT trade_date, ts_code, close
        FROM {self._TABLE_NAMES['market_price']}
        WHERE trade_date <= %(end_date)s
        """
        params = {'end_date': end_date}
        
        if stocks:
            query += " AND ts_code = ANY(%(stocks)s)"
            params['stocks'] = stocks
            
        df = self.context.query_dataframe(query, params)
        
        # Convert numeric columns to proper types
        if 'close' in df.columns:
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
        
        logger.info(f"Loaded {len(df)} market price records.")
        return df
    
    def _load_market_dailybasic(self, end_date: str, stocks: Optional[List[str]]) -> pd.DataFrame:
        """加载每日基础数据"""
        query = f"""
        SELECT trade_date, ts_code,
               total_mv, circ_mv, turnover_rate, pe_ttm, pb, ps_ttm
        FROM {self._TABLE_NAMES['market_dailybasic']}
        WHERE trade_date <= %(end_date)s
        """
        params = {'end_date': end_date}
        
        if stocks:
            query += " AND ts_code = ANY(%(stocks)s)"
            params['stocks'] = stocks
            
        df = self.context.query_dataframe(query, params)
        
        # Convert numeric columns to proper types
        numeric_columns = ['total_mv', 'circ_mv', 'turnover_rate', 'pe_ttm', 'pb', 'ps_ttm']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        logger.info(f"Loaded {len(df)} daily basic records.")
        return df
