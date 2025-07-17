"""
指数数据查询模块

专门负责指数相关的数据查询逻辑，包括：
- 指数权重数据查询
- 指数成分股查询
- 指数基本信息查询
"""

import pandas as pd
from typing import List, Optional, Union
from ..common.logging_utils import get_logger


class IndexQueries:
    """指数数据查询类
    
    职责：
    ----
    专门处理指数相关的数据查询，包括：
    1. 指数权重数据
    2. 指数成分股信息
    3. 指数基本信息
    4. 指数历史变更记录
    
    设计特点：
    --------
    - 智能表名检测：自动检测可用的指数数据表
    - 时间过滤：支持按月末、季末等时间点过滤
    - 权重计算：支持权重归一化和调整
    - 成分股追踪：支持成分股变更历史查询
    """
    
    def __init__(self, db_manager):
        """初始化指数查询类
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = get_logger(self.__class__.__name__)
        
        # 缓存表名检测结果
        self._index_weight_table = None
        self._index_basic_table = None
    
    def _get_index_weight_table(self) -> str:
        """获取指数权重数据表名
        
        Returns:
            可用的指数权重数据表名
        """
        if self._index_weight_table is not None:
            return self._index_weight_table
        
        # 按优先级检查可用的表（包含schema前缀）
        candidate_tables = [
            'tushare.index_weight',     # Tushare schema中的标准表名
            'index_weight',             # 默认schema中的标准表名
            'tushare.tushare_index_weight',  # Tushare schema中的完整表名
            'tushare_index_weight',     # 默认schema中的Tushare表名
            'tushare.index_weights',    # Tushare schema中的复数形式
            'index_weights',            # 默认schema中的复数形式
            'tushare.idx_weight',       # Tushare schema中的简化表名
            'idx_weight'                # 默认schema中的简化表名
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
                        self._index_weight_table = table
                        self.logger.info(f"使用指数权重数据表: {table}")
                        return table
                else:
                    # 异步模式 - 简化处理，优先使用带schema的表名
                    self._index_weight_table = 'tushare.index_weight'
                    return 'tushare.index_weight'
            except Exception as e:
                self.logger.debug(f"检查表 {table} 失败: {e}")
                continue
        
        # 如果都不存在，使用带schema的默认表名
        self._index_weight_table = 'tushare.index_weight'
        self.logger.warning(f"未找到指数权重数据表，使用默认表名: {self._index_weight_table}")
        return self._index_weight_table
    
    def _get_index_basic_table(self) -> str:
        """获取指数基本信息表名
        
        Returns:
            可用的指数基本信息表名
        """
        if self._index_basic_table is not None:
            return self._index_basic_table
        
        # 按优先级检查可用的表（包含schema前缀）
        candidate_tables = [
            'tushare.index_basic',     # Tushare schema中的标准表名
            'index_basic',             # 默认schema中的标准表名
            'tushare.tushare_index_basic',  # Tushare schema中的完整表名
            'tushare_index_basic',     # 默认schema中的Tushare表名
            'tushare.index_info',      # Tushare schema中的备选表名
            'index_info'               # 默认schema中的备选表名
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
                        self._index_basic_table = table
                        self.logger.info(f"使用指数基本信息表: {table}")
                        return table
                else:
                    # 异步模式 - 简化处理，优先使用带schema的表名
                    self._index_basic_table = 'tushare.index_basic'
                    return 'tushare.index_basic'
            except Exception as e:
                self.logger.debug(f"检查表 {table} 失败: {e}")
                continue
        
        # 如果都不存在，使用带schema的默认表名
        self._index_basic_table = 'tushare.index_basic'
        self.logger.warning(f"未找到指数基本信息表，使用默认表名: {self._index_basic_table}")
        return self._index_basic_table
    
    def get_index_weights(self, index_code: str, start_date: str, 
                         end_date: str, monthly: bool = False) -> pd.DataFrame:
        """获取指数权重数据
        
        Args:
            index_code: 指数代码，如 '000300.SH'
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'
            monthly: 是否只获取月末数据，默认False（获取所有数据）
            
        Returns:
            包含指数权重数据的DataFrame
        """
        table_name = self._get_index_weight_table()
        
        # 构建参数化基础查询
        base_query = f"""
        SELECT
            index_code,
            con_code,
            trade_date,
            weight
        FROM {table_name}
        WHERE index_code = %s
            AND trade_date >= %s
            AND trade_date <= %s
        """

        # 准备基础参数
        base_params = [index_code, start_date, end_date]
        
        if monthly:
            # 只获取月末数据
            query = f"""
            WITH monthly_data AS (
                {base_query}
            ),
            month_end_dates AS (
                SELECT
                    index_code,
                    con_code,
                    DATE_TRUNC('month', trade_date) + INTERVAL '1 month' - INTERVAL '1 day' as month_end,
                    trade_date,
                    weight,
                    ROW_NUMBER() OVER (
                        PARTITION BY index_code, con_code, DATE_TRUNC('month', trade_date)
                        ORDER BY trade_date DESC
                    ) as rn
                FROM monthly_data
            )
            SELECT
                index_code,
                con_code,
                trade_date,
                weight
            FROM month_end_dates
            WHERE rn = 1
            ORDER BY trade_date, con_code
            """
            query_params = base_params
        else:
            # 获取所有数据
            query = base_query + " ORDER BY trade_date, con_code"
            query_params = base_params
        
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
                self.logger.warning(f"未查询到指数权重数据: {index_code}, {start_date} - {end_date}")
                return df
            
            # 数据类型转换
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
            
            self.logger.info(f"成功获取指数权重数据: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"获取指数权重数据失败: {e}")
            raise
    
    def get_index_constituents(self, index_code: str, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取指数成分股
        
        Args:
            index_code: 指数代码，如 '000300.SH'
            trade_date: 指定日期，None表示获取最新成分股
            
        Returns:
            包含指数成分股的DataFrame
        """
        table_name = self._get_index_weight_table()
        
        if trade_date is None:
            # 获取最新成分股
            query = f"""
            WITH latest_date AS (
                SELECT MAX(trade_date) as max_date
                FROM {table_name}
                WHERE index_code = %s
            )
            SELECT
                index_code,
                con_code,
                trade_date,
                weight
            FROM {table_name}
            WHERE index_code = %s
                AND trade_date = (SELECT max_date FROM latest_date)
            ORDER BY weight DESC
            """
            query_params = [index_code, index_code]
        else:
            # 获取指定日期的成分股
            query = f"""
            SELECT
                index_code,
                con_code,
                trade_date,
                weight
            FROM {table_name}
            WHERE index_code = %s
                AND trade_date = %s
            ORDER BY weight DESC
            """
            query_params = [index_code, trade_date]
        
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
                # 数据类型转换
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
            
            self.logger.info(f"成功获取指数成分股: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"获取指数成分股失败: {e}")
            raise
    
    def get_index_info(self, index_codes: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """获取指数基本信息
        
        Args:
            index_codes: 指数代码或代码列表，None表示获取所有指数
            
        Returns:
            包含指数基本信息的DataFrame
        """
        table_name = self._get_index_basic_table()
        
        # 构建参数化查询条件
        if index_codes is not None:
            if isinstance(index_codes, str):
                index_codes = [index_codes]
            placeholders = ','.join(['%s'] * len(index_codes))
            where_clause = f"WHERE ts_code IN ({placeholders})"
            query_params = index_codes
        else:
            where_clause = ""
            query_params = []

        query = f"""
        SELECT
            ts_code,
            name,
            fullname,
            market,
            publisher,
            index_type,
            category,
            base_date,
            base_point,
            list_date,
            weight_rule,
            desc
        FROM {table_name}
        {where_clause}
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
            
            if not df.empty:
                # 日期列转换
                date_columns = ['base_date', 'list_date']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # 数值列转换
                if 'base_point' in df.columns:
                    df['base_point'] = pd.to_numeric(df['base_point'], errors='coerce')
            
            self.logger.info(f"成功获取指数基本信息: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"获取指数基本信息失败: {e}")
            raise
