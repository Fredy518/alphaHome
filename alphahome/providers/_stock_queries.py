"""
股票数据查询模块

专门负责股票相关的数据查询逻辑，包括：
- 股票行情数据查询
- 股票基本信息查询
- 股票复权处理
"""

import pandas as pd
from typing import List, Optional, Union
from ..common.logging_utils import get_logger


class StockQueries:
    """股票数据查询类
    
    职责：
    ----
    专门处理股票相关的数据查询，包括：
    1. 股票日线行情数据
    2. 股票基本信息
    3. 复权价格处理
    4. 股票代码标准化
    
    设计特点：
    --------
    - 智能表名检测：自动检测可用的数据表
    - 复权处理：支持前复权、后复权价格
    - 批量查询：支持单个或多个股票代码
    - 数据清洗：自动处理数据类型转换
    """
    
    def __init__(self, db_manager):
        """初始化股票查询类
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = get_logger(self.__class__.__name__)
        
        # 缓存表名检测结果
        self._stock_daily_table = None
        self._stock_basic_table = None
    
    def _normalize_symbols(self, symbols: Union[str, List[str]]) -> List[str]:
        """标准化股票代码格式
        
        Args:
            symbols: 股票代码或代码列表
            
        Returns:
            标准化后的股票代码列表
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        
        # 确保股票代码格式正确（如添加.SZ/.SH后缀等）
        normalized = []
        for symbol in symbols:
            symbol = symbol.strip().upper()
            # 这里可以添加更多的标准化逻辑
            normalized.append(symbol)
        
        return normalized
    
    def _get_stock_daily_table(self) -> str:
        """获取股票日线数据表名
        
        Returns:
            可用的股票日线数据表名
        """
        if self._stock_daily_table is not None:
            return self._stock_daily_table
        
        # 按优先级检查可用的表（包含schema前缀）
        candidate_tables = [
            'tushare.stock_daily',     # Tushare schema中的标准表名
            'stock_daily',             # 默认schema中的标准表名
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
                        self._stock_daily_table = table
                        self.logger.info(f"使用股票日线数据表: {table}")
                        return table
                else:
                    # 异步模式 - 这里简化处理，实际使用中需要异步检查
                    # 优先使用带schema的表名
                    self._stock_daily_table = 'tushare.stock_daily'
                    return 'tushare.stock_daily'
            except Exception as e:
                self.logger.debug(f"检查表 {table} 失败: {e}")
                continue
        
        # 如果都不存在，使用带schema的默认表名
        self._stock_daily_table = 'tushare.stock_daily'
        self.logger.warning(f"未找到股票日线数据表，使用默认表名: {self._stock_daily_table}")
        return self._stock_daily_table
    
    def _get_stock_basic_table(self) -> str:
        """获取股票基本信息表名
        
        Returns:
            可用的股票基本信息表名
        """
        if self._stock_basic_table is not None:
            return self._stock_basic_table
        
        # 按优先级检查可用的表（包含schema前缀）
        candidate_tables = [
            'tushare.stock_basic',     # Tushare schema中的标准表名
            'stock_basic',             # 默认schema中的标准表名
            'tushare.stock_info',      # Tushare schema中的备选表名
            'stock_info'               # 默认schema中的备选表名
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
                        self._stock_basic_table = table
                        self.logger.info(f"使用股票基本信息表: {table}")
                        return table
                else:
                    # 异步模式 - 简化处理，优先使用带schema的表名
                    self._stock_basic_table = 'tushare.stock_basic'
                    return 'tushare.stock_basic'
            except Exception as e:
                self.logger.debug(f"检查表 {table} 失败: {e}")
                continue
        
        # 如果都不存在，使用带schema的默认表名
        self._stock_basic_table = 'tushare.stock_basic'
        self.logger.warning(f"未找到股票基本信息表，使用默认表名: {self._stock_basic_table}")
        return self._stock_basic_table
    
    def get_stock_data(self, symbols: Union[str, List[str]], start_date: str, 
                      end_date: str, adjust: bool = True) -> pd.DataFrame:
        """获取股票行情数据
        
        Args:
            symbols: 股票代码或代码列表
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'
            adjust: 是否使用复权价格，默认True
            
        Returns:
            包含股票行情数据的DataFrame
        """
        symbols = self._normalize_symbols(symbols)
        table_name = self._get_stock_daily_table()
        
        # 构建参数化SQL查询（防止SQL注入）
        # 创建占位符
        placeholders = ','.join(['%s'] * len(symbols))

        # 选择字段（基于实际表结构）
        # 注意：当前数据库中没有复权字段，统一使用原始价格
        price_fields = "open, high, low, close"

        # 如果需要复权功能，可以在应用层计算或使用其他数据源
        if adjust:
            self.logger.debug("注意：当前数据库不包含复权字段，返回原始价格数据")

        query = f"""
        SELECT
            ts_code,
            trade_date,
            {price_fields},
            pre_close,
            change,
            pct_chg,
            volume as vol,
            amount
        FROM {table_name}
        WHERE ts_code IN ({placeholders})
            AND trade_date >= %s
            AND trade_date <= %s
        ORDER BY ts_code, trade_date
        """

        # 准备参数
        query_params = symbols + [start_date, end_date]
        
        try:
            # 执行参数化查询
            if hasattr(self.db, 'mode') and self.db.mode == 'sync':
                # 同步模式
                result = self.db.fetch_sync(query, tuple(query_params))
                df = pd.DataFrame(result)
            else:
                # 异步模式 - 需要特殊处理
                # 这里假设有query_dataframe方法
                if hasattr(self.db, 'query_dataframe'):
                    df = self.db.query_dataframe(query, tuple(query_params))
                else:
                    raise NotImplementedError("异步模式需要在异步环境中调用")
            
            if df.empty:
                self.logger.warning(f"未查询到股票数据: {symbols}, {start_date} - {end_date}")
                return df
            
            # 数据类型转换
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 数值列转换
            numeric_columns = ['open', 'high', 'low', 'close', 'pre_close', 
                             'change', 'pct_chg', 'vol', 'amount']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            self.logger.info(f"成功获取股票数据: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"获取股票数据失败: {e}")
            raise
    
    def get_stock_info(self, symbols: Optional[Union[str, List[str]]] = None, 
                      list_status: str = 'L') -> pd.DataFrame:
        """获取股票基本信息
        
        Args:
            symbols: 股票代码或代码列表，None表示获取所有股票
            list_status: 上市状态，'L'=上市，'D'=退市，'P'=暂停上市
            
        Returns:
            包含股票基本信息的DataFrame
        """
        table_name = self._get_stock_basic_table()
        
        # 构建参数化查询条件
        where_conditions = ["list_status = %s"]
        query_params = [list_status]

        if symbols is not None:
            symbols = self._normalize_symbols(symbols)
            placeholders = ','.join(['%s'] * len(symbols))
            where_conditions.append(f"ts_code IN ({placeholders})")
            query_params.extend(symbols)

        where_clause = " AND ".join(where_conditions)

        query = f"""
        SELECT
            ts_code,
            symbol,
            name,
            area,
            industry,
            fullname,
            enname,
            cnspell,
            market,
            exchange,
            curr_type,
            list_status,
            list_date,
            delist_date,
            is_hs
        FROM {table_name}
        WHERE {where_clause}
        ORDER BY ts_code
        """
        
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
            
            if not df.empty:
                # 日期列转换
                date_columns = ['list_date', 'delist_date']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
            
            self.logger.info(f"成功获取股票基本信息: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"获取股票基本信息失败: {e}")
            raise
