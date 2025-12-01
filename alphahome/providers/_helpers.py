"""
数据访问辅助工具模块

提供通用的数据访问辅助功能，包括：
- 交易日历查询
- 行业分类数据查询
- 数据格式转换工具
- 常用数据过滤器
"""

import pandas as pd
from typing import List, Optional, Union
from datetime import datetime, date
from ..common.logging_utils import get_logger


class DataHelpers:
    """数据访问辅助工具类
    
    职责：
    ----
    提供通用的数据访问辅助功能，包括：
    1. 交易日历数据查询
    2. 行业分类数据查询
    3. 数据格式转换和标准化
    4. 常用数据过滤和聚合
    
    设计特点：
    --------
    - 智能表名检测：自动检测可用的辅助数据表
    - 数据缓存：缓存常用的静态数据
    - 格式标准化：统一数据格式和类型
    - 错误处理：优雅处理数据缺失和异常
    """
    
    def __init__(self, db_manager):
        """初始化数据辅助工具类
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = get_logger(self.__class__.__name__)
        
        # 缓存表名检测结果
        self._trade_cal_table = None
        self._industry_table = None
        
        # 缓存常用数据
        self._cached_trade_dates = {}
        self._cached_industry_data = None
    
    def _get_trade_cal_table(self) -> str:
        """获取交易日历表名
        
        Returns:
            可用的交易日历表名
        """
        if self._trade_cal_table is not None:
            return self._trade_cal_table
        
        # 按优先级检查可用的表（基于实际数据库结构）
        candidate_tables = [
            'tushare.others_calendar',          # 其他日历表
            'tushare.trade_cal',                # 标准表名（可能不存在）
            'trade_cal',                        # 默认schema中的标准表名
            'tushare.trading_calendar',         # Tushare schema中的英文表名
            'trading_calendar',                 # 默认schema中的英文表名
            'tushare.calendar',                 # Tushare schema中的简化表名
            'calendar'                          # 默认schema中的简化表名
        ]
        
        for table in candidate_tables:
            try:
                # 检查表是否存在
                if hasattr(self.db, 'mode') and self.db.mode == 'sync':
                    # 同步模式 - 检查表是否存在（支持schema.table格式）
                    if '.' in table:
                        schema_name, table_name = table.split('.', 1)
                        result = self.db.fetch_sync(
                            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s LIMIT 1",
                            (schema_name, table_name)
                        )
                    else:
                        result = self.db.fetch_sync(
                            "SELECT 1 FROM information_schema.tables WHERE table_name = %s LIMIT 1",
                            (table,)
                        )
                    if result:
                        self._trade_cal_table = table
                        self.logger.info(f"使用交易日历表: {table}")
                        return table
                else:
                    # 异步模式 - 简化处理，优先使用带schema的表名
                    self._trade_cal_table = 'tushare.trade_cal'
                    return 'tushare.trade_cal'
            except Exception as e:
                self.logger.debug(f"检查表 {table} 失败: {e}")
                continue
        
        # 如果都不存在，使用实际存在的表名
        self._trade_cal_table = 'tushare.tushare_others_tradecal'
        self.logger.warning(f"未找到交易日历表，使用默认表名: {self._trade_cal_table}")
        return self._trade_cal_table
    
    def _get_industry_table(self) -> str:
        """获取行业分类表名
        
        Returns:
            可用的行业分类表名
        """
        if self._industry_table is not None:
            return self._industry_table
        
        # 按优先级检查可用的表（包含schema前缀）
        candidate_tables = [
            'tushare.stock_industry',   # Tushare schema中的标准表名
            'stock_industry',           # 默认schema中的标准表名
            'tushare.industry_classify', # Tushare schema中的分类表名
            'industry_classify',        # 默认schema中的分类表名
            'tushare.sw_industry',      # Tushare schema中的申万行业表名
            'sw_industry'               # 默认schema中的申万行业表名
        ]
        
        for table in candidate_tables:
            try:
                # 检查表是否存在
                if hasattr(self.db, 'mode') and self.db.mode == 'sync':
                    # 同步模式 - 检查表是否存在（支持schema.table格式）
                    if '.' in table:
                        schema_name, table_name = table.split('.', 1)
                        result = self.db.fetch_sync(
                            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s LIMIT 1",
                            (schema_name, table_name)
                        )
                    else:
                        result = self.db.fetch_sync(
                            "SELECT 1 FROM information_schema.tables WHERE table_name = %s LIMIT 1",
                            (table,)
                        )
                    if result:
                        self._industry_table = table
                        self.logger.info(f"使用行业分类表: {table}")
                        return table
                else:
                    # 异步模式 - 简化处理，优先使用带schema的表名
                    self._industry_table = 'tushare.stock_industry'
                    return 'tushare.stock_industry'
            except Exception as e:
                self.logger.debug(f"检查表 {table} 失败: {e}")
                continue
        
        # 如果都不存在，使用带schema的默认表名
        self._industry_table = 'tushare.stock_industry'
        self.logger.warning(f"未找到行业分类表，使用默认表名: {self._industry_table}")
        return self._industry_table
    
    def get_trade_dates(self, start_date: str, end_date: str, 
                       exchange: str = 'SSE') -> pd.DataFrame:
        """获取交易日历
        
        Args:
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'
            exchange: 交易所代码，'SSE'=上交所，'SZSE'=深交所
            
        Returns:
            包含交易日期的DataFrame
        """
        # 检查缓存
        cache_key = f"{start_date}_{end_date}_{exchange}"
        if cache_key in self._cached_trade_dates:
            self.logger.debug(f"从缓存获取交易日历: {cache_key}")
            return self._cached_trade_dates[cache_key].copy()
        
        table_name = self._get_trade_cal_table()
        
        query = f"""
        SELECT
            cal_date,
            exchange,
            is_open,
            pretrade_date
        FROM {table_name}
        WHERE cal_date >= %s
            AND cal_date <= %s
            AND exchange = %s
        ORDER BY cal_date
        """

        # 准备参数
        query_params = [start_date, end_date, exchange]
        
        try:
            # 执行参数化查询
            if hasattr(self.db, 'mode') and self.db.mode == 'sync':
                # 同步模式
                result = self.db.fetch_sync(query, tuple(query_params))
                df = pd.DataFrame(result)
            else:
                # 异步模式
                if hasattr(self.db, 'query_dataframe'):
                    df = self.db.query_dataframe(query, tuple(query_params))
                else:
                    raise NotImplementedError("异步模式需要在异步环境中调用")
            
            if df.empty:
                self.logger.warning(f"未查询到交易日历数据: {start_date} - {end_date}, {exchange}")
                return df
            
            # 数据类型转换
            df['cal_date'] = pd.to_datetime(df['cal_date'])
            if 'pretrade_date' in df.columns:
                df['pretrade_date'] = pd.to_datetime(df['pretrade_date'])
            
            # 确保is_open是整数类型
            if 'is_open' in df.columns:
                df['is_open'] = pd.to_numeric(df['is_open'], errors='coerce').astype(int)
            
            # 缓存结果（限制缓存大小）
            if len(self._cached_trade_dates) < 10:
                self._cached_trade_dates[cache_key] = df.copy()
            
            self.logger.info(f"成功获取交易日历: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"获取交易日历失败: {e}")
            raise
    
    def get_trading_dates_only(self, start_date: str, end_date: str, 
                              exchange: str = 'SSE') -> List[str]:
        """获取交易日期列表（仅开市日期）
        
        [已禁用] 此方法已废弃，请使用 alphahome.fetchers.tools.calendar.get_trade_days_between 替代。
        
        Args:
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'
            exchange: 交易所代码
            
        Returns:
            交易日期字符串列表
            
        .. deprecated::
            请使用 `alphahome.fetchers.tools.calendar.get_trade_days_between` 替代
        """
        import warnings
        warnings.warn(
            "[已禁用] get_trading_dates_only 已废弃，请使用 "
            "alphahome.fetchers.tools.calendar.get_trade_days_between 替代",
            DeprecationWarning,
            stacklevel=2
        )
        df = self.get_trade_dates(start_date, end_date, exchange)
        
        if df.empty:
            return []
        
        # 筛选开市日期
        trading_dates = df[df['is_open'] == 1]['cal_date'].dt.strftime('%Y-%m-%d').tolist()
        
        self.logger.info(f"获取到 {len(trading_dates)} 个交易日")
        return trading_dates
    
    def get_industry_data(self, symbols: Optional[Union[str, List[str]]] = None,
                         level: str = 'sw_l1') -> pd.DataFrame:
        """获取行业分类数据

        注意：由于数据库中没有专门的行业分类表，此方法从stock_basic表获取行业信息

        Args:
            symbols: 股票代码或代码列表，None表示获取所有股票
            level: 行业分类级别（此参数在当前实现中被忽略）

        Returns:
            包含行业分类数据的DataFrame
        """
        # 检查缓存（仅当获取所有股票时使用缓存）
        if symbols is None and self._cached_industry_data is not None:
            self.logger.debug("从缓存获取行业分类数据")
            return self._cached_industry_data.copy()

        # 使用stock_basic表获取行业信息
        stock_basic_table = 'tushare.stock_basic'

        # 构建参数化查询条件
        where_conditions = ["list_status = %s"]
        query_params = ['L']  # 只获取上市股票

        if symbols is not None:
            if isinstance(symbols, str):
                symbols = [symbols]
            placeholders = ','.join(['%s'] * len(symbols))
            where_conditions.append(f"ts_code IN ({placeholders})")
            query_params.extend(symbols)

        where_clause = " AND ".join(where_conditions)

        # 从stock_basic表获取行业信息，并转换为标准格式
        query = f"""
        SELECT
            ts_code,
            industry as industry_code,
            industry as industry_name,
            'basic' as level,
            'stock_basic' as src
        FROM {stock_basic_table}
        WHERE {where_clause}
        ORDER BY ts_code
        """
        
        try:
            # 执行参数化查询
            if hasattr(self.db, 'mode') and self.db.mode == 'sync':
                # 同步模式
                result = self.db.fetch_sync(query, tuple(query_params) if query_params else None)
                df = pd.DataFrame(result)
            else:
                # 异步模式
                if hasattr(self.db, 'query_dataframe'):
                    df = self.db.query_dataframe(query, tuple(query_params) if query_params else None)
                else:
                    raise NotImplementedError("异步模式需要在异步环境中调用")
            
            # 缓存结果（仅当获取所有股票时）
            if symbols is None and not df.empty:
                self._cached_industry_data = df.copy()
            
            self.logger.info(f"成功获取行业分类数据: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"获取行业分类数据失败: {e}")
            raise
    
    def get_latest_trade_date(self, exchange: str = 'SSE') -> Optional[str]:
        """获取最新交易日期
        
        Args:
            exchange: 交易所代码
            
        Returns:
            最新交易日期字符串，格式 'YYYY-MM-DD'
        """
        table_name = self._get_trade_cal_table()
        
        query = f"""
        SELECT MAX(cal_date) as latest_date
        FROM {table_name}
        WHERE exchange = %s
            AND is_open = 1
            AND cal_date <= CURRENT_DATE
        """

        query_params = [exchange]
        
        try:
            # 执行参数化查询
            if hasattr(self.db, 'mode') and self.db.mode == 'sync':
                # 同步模式
                result = self.db.fetch_sync(query, tuple(query_params))
                if result and result[0]['latest_date']:
                    latest_date = result[0]['latest_date']
                    if isinstance(latest_date, (datetime, date)):
                        return latest_date.strftime('%Y-%m-%d')
                    return str(latest_date)
            else:
                # 异步模式
                if hasattr(self.db, 'query_dataframe'):
                    df = self.db.query_dataframe(query, tuple(query_params))
                    if not df.empty and df.iloc[0]['latest_date'] is not None:
                        latest_date = df.iloc[0]['latest_date']
                        if isinstance(latest_date, (datetime, date)):
                            return latest_date.strftime('%Y-%m-%d')
                        return str(latest_date)
                else:
                    raise NotImplementedError("异步模式需要在异步环境中调用")
            
            return None
            
        except Exception as e:
            self.logger.error(f"获取最新交易日期失败: {e}")
            return None
    
    def is_trading_day(self, check_date: str, exchange: str = 'SSE') -> bool:
        """检查指定日期是否为交易日
        
        Args:
            check_date: 检查日期，格式 'YYYY-MM-DD'
            exchange: 交易所代码
            
        Returns:
            是否为交易日
        """
        table_name = self._get_trade_cal_table()
        
        query = f"""
        SELECT is_open
        FROM {table_name}
        WHERE cal_date = %s
            AND exchange = %s
        """

        query_params = [check_date, exchange]
        
        try:
            # 执行参数化查询
            if hasattr(self.db, 'mode') and self.db.mode == 'sync':
                # 同步模式
                result = self.db.fetch_sync(query, tuple(query_params))
                if result:
                    return bool(result[0]['is_open'])
            else:
                # 异步模式
                if hasattr(self.db, 'query_dataframe'):
                    df = self.db.query_dataframe(query, tuple(query_params))
                    if not df.empty:
                        return bool(df.iloc[0]['is_open'])
                else:
                    raise NotImplementedError("异步模式需要在异步环境中调用")
            
            return False
        except Exception as e:
            # 查询失败时返回 False
            return False


# =============================
# 符号映射辅助函数
# =============================

def map_ts_code_to_hikyuu(ts_code: str) -> str:
    """将 Tushare 风格 ts_code（如 '000001.SZ'）映射为 Hikyuu 风格符号（如 'sz000001'）

    Args:
        ts_code: 形如 '000001.SZ' 的股票代码

    Returns:
        Hikyuu 符号字符串，例如 'sz000001'
    """
    try:
        code, market = ts_code.split('.')
        market_prefix = 'sz' if market.upper() == 'SZ' else 'sh'
        return f"{market_prefix}{code}"
    except Exception:
        # 回退：不抛异常，直接返回原始 ts_code，交由上层处理
        return ts_code
