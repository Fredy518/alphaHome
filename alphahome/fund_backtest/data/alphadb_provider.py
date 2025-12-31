"""AlphaDB 数据提供者"""

from typing import Optional, List, Dict
import pandas as pd
import logging

from .provider import DataProvider

logger = logging.getLogger(__name__)


# SQL 模板
SQL_GET_FUND_NAV = """
SELECT 
    nav_date as date,
    ts_code as fund_id,
    {nav_type} as nav
FROM tushare.fund_nav
WHERE ts_code = ANY(%(fund_ids)s)
  AND nav_date BETWEEN %(start_date)s AND %(end_date)s
ORDER BY nav_date, ts_code
"""

SQL_GET_REBALANCE = """
SELECT 
    rebalancing_date as rebalance_date,
    fund_code as fund_id,
    strategy_name as fund_name,
    weight as target_weight
FROM excel.fastrategy_portfolio
WHERE strategy_code = %(portfolio_id)s
  AND (%(start_date)s IS NULL OR rebalancing_date >= %(start_date)s)
  AND (%(end_date)s IS NULL OR rebalancing_date <= %(end_date)s)
ORDER BY rebalancing_date, fund_code
"""

SQL_GET_CALENDAR = """
SELECT DISTINCT cal_date as date
FROM tushare.others_calendar
WHERE exchange = %(exchange)s
  AND cal_date BETWEEN %(start_date)s AND %(end_date)s
  AND is_open = 1
ORDER BY cal_date
"""

SQL_GET_PORTFOLIO_CONFIG = """
SELECT 
    strategy_code as portfolio_id,
    strategy_name as portfolio_name,
    channel_code,
    fee_rate as management_fee,
    setup_date,
    redemption_days_strategy as rebalance_delay
FROM excel.fastrategy_basic
WHERE strategy_code = %(portfolio_id)s
"""

# 基准净值 SQL 模板
SQL_GET_INDEX_NAV_FACTOR_PRO = """
SELECT 
    trade_date as date, 
    close as nav
FROM tushare.index_factor_pro
WHERE ts_code = %(benchmark_id)s
  AND trade_date BETWEEN %(start_date)s AND %(end_date)s
ORDER BY trade_date
"""

SQL_GET_INDEX_NAV_DAILYBASIC = """
SELECT 
    trade_date as date, 
    close as nav
FROM tushare.index_dailybasic
WHERE ts_code = %(benchmark_id)s
  AND trade_date BETWEEN %(start_date)s AND %(end_date)s
ORDER BY trade_date
"""

SQL_GET_FUND_NAV_SINGLE = """
SELECT 
    nav_date as date,
    adj_nav as nav
FROM tushare.fund_nav
WHERE ts_code = %(benchmark_id)s
  AND nav_date BETWEEN %(start_date)s AND %(end_date)s
ORDER BY nav_date
"""


class AlphaDBDataProvider(DataProvider):
    """
    AlphaDB 数据提供者
    
    从 PostgreSQL 数据库读取数据。
    """
    
    def __init__(self, connection_string: Optional[str] = None, engine=None):
        """
        Args:
            connection_string: 数据库连接字符串
            engine: SQLAlchemy engine (可选，优先使用)
        """
        self._engine = engine
        self._connection_string = connection_string
        
        if self._engine is None and self._connection_string:
            self._init_engine()
    
    def _init_engine(self):
        """初始化数据库引擎"""
        try:
            from sqlalchemy import create_engine
            from sqlalchemy.pool import QueuePool
            
            self._engine = create_engine(
                self._connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10
            )
            logger.info("AlphaDB 连接初始化成功")
        except Exception as e:
            logger.error(f"AlphaDB 连接初始化失败: {e}")
            raise
    
    def get_fund_nav(
        self,
        fund_ids: List[str],
        start_date: str,
        end_date: str,
        nav_type: str = 'unit_nav'
    ) -> pd.DataFrame:
        if not fund_ids:
            return pd.DataFrame()
        
        sql = SQL_GET_FUND_NAV.format(nav_type=nav_type)
        params = {
            'fund_ids': fund_ids,
            'start_date': start_date,
            'end_date': end_date
        }
        
        df = pd.read_sql(sql, self._engine, params=params)
        
        if df.empty:
            return pd.DataFrame()
        
        # 转换为面板格式
        df['date'] = pd.to_datetime(df['date'])
        pivot = df.pivot(index='date', columns='fund_id', values='nav')
        return pivot
    
    def get_rebalance_records(
        self,
        portfolio_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        params = {
            'portfolio_id': portfolio_id,
            'start_date': start_date,
            'end_date': end_date
        }
        
        df = pd.read_sql(SQL_GET_REBALANCE, self._engine, params=params)
        
        if not df.empty:
            df['rebalance_date'] = pd.to_datetime(df['rebalance_date'])
        
        return df
    
    def get_fund_fee(self, fund_ids: List[str]) -> pd.DataFrame:
        # 注意：当前 alphadb 无完整费率表
        # 这里返回默认费率，实际使用时需要补充费率表
        logger.warning("AlphaDB 无完整费率表，使用默认费率")
        return pd.DataFrame({
            'fund_id': fund_ids,
            'purchase_fee': [0.015] * len(fund_ids),
            'redeem_fee': [0.005] * len(fund_ids),
        })
    
    def get_calendar(
        self,
        start_date: str,
        end_date: str,
        calendar_type: str = 'trade',
        exchange: str = 'SSE'
    ) -> pd.DatetimeIndex:
        """
        获取日历
        
        Args:
            calendar_type: 'trade' 使用 others_calendar 表，'nav' 使用 fund_nav 表
            exchange: 交易所代码（默认SSE）
        """
        if calendar_type == 'trade':
            # 使用交易日历表
            params = {
                'start_date': start_date,
                'end_date': end_date,
                'exchange': exchange
            }
            df = pd.read_sql(SQL_GET_CALENDAR, self._engine, params=params)
        else:
            # 使用净值日期
            sql = """
            SELECT DISTINCT nav_date as date
            FROM tushare.fund_nav
            WHERE nav_date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY nav_date
            """
            params = {
                'start_date': start_date,
                'end_date': end_date
            }
            df = pd.read_sql(sql, self._engine, params=params)
        
        if df.empty:
            return pd.date_range(start_date, end_date, freq='B')
        
        return pd.DatetimeIndex(pd.to_datetime(df['date']))
    
    def get_portfolio_config(self, portfolio_id: str) -> Optional[Dict]:
        params = {'portfolio_id': portfolio_id}
        
        df = pd.read_sql(SQL_GET_PORTFOLIO_CONFIG, self._engine, params=params)
        
        if df.empty:
            return None
        
        row = df.iloc[0]
        return {
            'portfolio_id': row['portfolio_id'],
            'portfolio_name': row['portfolio_name'],
            'initial_cash': 1000.0,  # 默认值，需要从其他地方获取
            'setup_date': str(row['setup_date']),
            'rebalance_delay': int(row.get('rebalance_delay', 2)),
            'purchase_fee_rate': 0.015,  # 默认1.5%申购费
            'redeem_fee_rate': 0.005,    # 默认0.5%赎回费
            'management_fee': float(row.get('management_fee', 0)) / 100,
            'rebalance_effective_delay': 1,  # 默认T+1生效
        }
    
    def get_benchmark_nav(
        self,
        benchmark_id: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.Series]:
        """
        获取基准净值
        
        支持指数和基金两种基准类型:
        - 指数代码 (.SH/.SZ): 优先从 index_factor_pro 获取，回退到 index_dailybasic
        - 基金代码 (.OF): 从 fund_nav 获取复权净值
        
        Args:
            benchmark_id: 基准代码，如 '000300.SH', '000001.OF'
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        
        Returns:
            pd.Series: index=date (DatetimeIndex), values=nav
            None: 如果基准不可用
        """
        params = {
            'benchmark_id': benchmark_id,
            'start_date': start_date,
            'end_date': end_date
        }
        
        # 判断是指数还是基金
        if benchmark_id.endswith(('.SH', '.SZ')):
            # 指数基准：优先从 index_factor_pro 获取
            df = pd.read_sql(SQL_GET_INDEX_NAV_FACTOR_PRO, self._engine, params=params)
            
            if df.empty:
                # 回退到 index_dailybasic
                logger.debug(
                    "index_factor_pro 无数据，尝试 index_dailybasic: %s",
                    benchmark_id
                )
                df = pd.read_sql(SQL_GET_INDEX_NAV_DAILYBASIC, self._engine, params=params)
        
        elif benchmark_id.endswith('.OF'):
            # 基金基准：从 fund_nav 获取复权净值
            df = pd.read_sql(SQL_GET_FUND_NAV_SINGLE, self._engine, params=params)
        
        else:
            logger.warning(
                "未知的基准代码格式: %s，支持 .SH/.SZ (指数) 或 .OF (基金)",
                benchmark_id
            )
            return None
        
        if df.empty:
            logger.warning(
                "基准数据为空: %s (%s ~ %s)",
                benchmark_id, start_date, end_date
            )
            return None
        
        # 转换为 Series
        df['date'] = pd.to_datetime(df['date'])
        result = df.set_index('date')['nav']
        result.name = benchmark_id
        
        logger.debug(
            "获取基准净值成功: %s, %d 条记录 (%s ~ %s)",
            benchmark_id, len(result), result.index.min(), result.index.max()
        )
        
        return result
