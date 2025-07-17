#!/usr/bin/env python3
"""
AlphaHome 数据访问模块设计方案

这个文件展示了建议的数据访问模块的核心设计和实现示例
"""

from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Dict, List, Optional, Union, Any, Tuple
import pandas as pd
import asyncio
from functools import wraps
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# 1. 核心基础类设计
# ============================================================================

class BaseDataAccessor(ABC):
    """数据访问器基类
    
    设计原则：
    1. 与现有 DBManager 集成，不重复造轮子
    2. 提供统一的接口和错误处理
    3. 支持缓存和性能优化
    4. 保持与现有架构的一致性
    """
    
    def __init__(self, db_manager, cache_manager=None):
        """
        Args:
            db_manager: 现有的 DBManager 实例
            cache_manager: 可选的缓存管理器
        """
        self.db = db_manager
        self.cache = cache_manager
        self.logger = logger.getChild(self.__class__.__name__)
    
    @abstractmethod
    def get_table_name(self) -> str:
        """获取主要数据表名"""
        pass
    
    def _build_cache_key(self, method_name: str, **kwargs) -> str:
        """构建缓存键"""
        key_parts = [self.__class__.__name__, method_name]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
        return ":".join(key_parts)
    
    async def _execute_query(
        self, 
        query: str, 
        params: Optional[Tuple[Any, ...]] = None, 
        cache_key: Optional[str] = None, 
        cache_ttl: int = 300
    ) -> pd.DataFrame:
        """执行查询的统一接口
        
        Args:
            query: SQL 查询语句
            params: 查询参数
            cache_key: 缓存键
            cache_ttl: 缓存过期时间（秒）
        """
        # 尝试从缓存获取
        if self.cache and cache_key:
            cached_result = await self.cache.get(cache_key)
            if cached_result is not None:
                self.logger.debug(f"从缓存获取数据: {cache_key}")
                return cached_result
        
        # 执行查询
        try:
            if params:
                result = await self.db.fetch_all(query, *params)
            else:
                result = await self.db.fetch_all(query)
            
            df = pd.DataFrame(result)
            
            # 缓存结果
            if self.cache and cache_key and not df.empty:
                await self.cache.set(cache_key, df, ttl=cache_ttl)
            
            return df
            
        except Exception as e:
            self.logger.error(f"查询执行失败: {e}")
            raise DataAccessError(f"查询执行失败: {e}") from e


# ============================================================================
# 2. 具体访问器实现示例
# ============================================================================

class IndexAccessor(BaseDataAccessor):
    """指数数据访问器
    
    提供指数相关数据的高级访问接口
    """
    
    def get_table_name(self) -> str:
        return "tushare_index_weight"
    
    async def get_index_weights(self, 
                              index_code: str,
                              start_date: Union[str, date],
                              end_date: Union[str, date],
                              con_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """获取指数成分权重数据
        
        Args:
            index_code: 指数代码，如 '000300.SH'
            start_date: 开始日期
            end_date: 结束日期
            con_codes: 可选的成分股代码列表
            
        Returns:
            包含指数权重数据的 DataFrame
            
        Example:
            >>> accessor = IndexAccessor(db_manager)
            >>> weights = await accessor.get_index_weights('000300.SH', '2024-01-01', '2024-12-31')
        """
        # 参数验证
        self._validate_date_range(start_date, end_date)
        self._validate_index_code(index_code)
        
        # 构建查询
        query = """
        SELECT index_code, con_code, trade_date, weight
        FROM tushare_index_weight
        WHERE index_code = $1 
        AND trade_date BETWEEN $2 AND $3
        """
        params = [index_code, str(start_date), str(end_date)]
        
        # 添加成分股过滤
        if con_codes:
            placeholders = ','.join([f'${i+4}' for i in range(len(con_codes))])
            query += f" AND con_code IN ({placeholders})"
            params.extend(con_codes)
        
        query += " ORDER BY trade_date, con_code"
        
        # 构建缓存键
        cache_key = self._build_cache_key(
            "get_index_weights",
            index_code=index_code,
            start_date=str(start_date),
            end_date=str(end_date),
            con_codes=con_codes
        )
        
        # 执行查询
        df = await self._execute_query(query, tuple(params), cache_key)
        
        # 数据后处理
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
            
        self.logger.info(f"获取指数权重数据: {index_code}, {len(df)} 条记录")
        return df
    
    async def get_latest_index_weights(self, index_code: str) -> pd.DataFrame:
        """获取指数最新的成分权重
        
        Args:
            index_code: 指数代码
            
        Returns:
            最新的指数权重数据
        """
        query = """
        SELECT index_code, con_code, trade_date, weight
        FROM tushare_index_weight
        WHERE index_code = $1 
        AND trade_date = (
            SELECT MAX(trade_date) 
            FROM tushare_index_weight 
            WHERE index_code = $1
        )
        ORDER BY weight DESC
        """
        
        cache_key = self._build_cache_key("get_latest_index_weights", index_code=index_code)
        df = await self._execute_query(query, (index_code,), cache_key, cache_ttl=600)
        
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
            
        return df
    
    async def get_index_constituents_history(self, 
                                           index_code: str,
                                           con_code: str,
                                           start_date: Union[str, date],
                                           end_date: Union[str, date]) -> pd.DataFrame:
        """获取特定成分股在指数中的权重历史
        
        Args:
            index_code: 指数代码
            con_code: 成分股代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            成分股权重历史数据
        """
        query = """
        SELECT trade_date, weight
        FROM tushare_index_weight
        WHERE index_code = $1 AND con_code = $2
        AND trade_date BETWEEN $3 AND $4
        ORDER BY trade_date
        """
        
        cache_key = self._build_cache_key(
            "get_index_constituents_history",
            index_code=index_code,
            con_code=con_code,
            start_date=str(start_date),
            end_date=str(end_date)
        )
        
        df = await self._execute_query(
            query, 
            (index_code, con_code, str(start_date), str(end_date)), 
            cache_key
        )
        
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
            
        return df
    
    def _validate_index_code(self, index_code: str):
        """验证指数代码格式"""
        if not index_code or not isinstance(index_code, str):
            raise ValueError("指数代码必须是非空字符串")
        
        # 可以添加更多格式验证
        if not ('.' in index_code and len(index_code) >= 8):
            raise ValueError(f"指数代码格式不正确: {index_code}")
    
    def _validate_date_range(self, start_date, end_date):
        """验证日期范围"""
        if start_date > end_date:
            raise ValueError("开始日期不能晚于结束日期")


class StockAccessor(BaseDataAccessor):
    """股票数据访问器"""
    
    def get_table_name(self) -> str:
        return "tushare_stock_daily"
    
    async def get_stock_daily(self,
                            ts_codes: Union[str, List[str]],
                            start_date: Union[str, date],
                            end_date: Union[str, date],
                            fields: Optional[List[str]] = None) -> pd.DataFrame:
        """获取股票日线数据
        
        Args:
            ts_codes: 股票代码或代码列表
            start_date: 开始日期
            end_date: 结束日期
            fields: 需要的字段列表，默认获取所有字段
            
        Returns:
            股票日线数据 DataFrame
        """
        # 标准化输入
        if isinstance(ts_codes, str):
            ts_codes = [ts_codes]
        
        # 默认字段
        if fields is None:
            fields = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        
        # 构建查询
        field_str = ', '.join(fields)
        placeholders = ','.join([f'${i+3}' for i in range(len(ts_codes))])
        
        query = f"""
        SELECT {field_str}
        FROM tushare_stock_daily
        WHERE trade_date BETWEEN $1 AND $2
        AND ts_code IN ({placeholders})
        ORDER BY ts_code, trade_date
        """
        
        params = [str(start_date), str(end_date)] + ts_codes
        
        cache_key = self._build_cache_key(
            "get_stock_daily",
            ts_codes=sorted(ts_codes),
            start_date=str(start_date),
            end_date=str(end_date),
            fields=sorted(fields)
        )
        
        df = await self._execute_query(query, tuple(params), cache_key)
        
        # 数据处理
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            numeric_fields = ['open', 'high', 'low', 'close', 'volume', 'amount']
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce')
        
        return df
    
    async def get_stock_basic_info(self, ts_codes: Union[str, List[str]] = None) -> pd.DataFrame:
        """获取股票基本信息
        
        Args:
            ts_codes: 股票代码列表，为空则获取所有股票
            
        Returns:
            股票基本信息 DataFrame
        """
        query = """
        SELECT ts_code, symbol, name, area, industry, market, list_date
        FROM tushare_stock_basic
        WHERE status = 'L'
        """
        params = []
        
        if ts_codes:
            if isinstance(ts_codes, str):
                ts_codes = [ts_codes]
            placeholders = ','.join([f'${i+1}' for i in range(len(ts_codes))])
            query += f" AND ts_code IN ({placeholders})"
            params = ts_codes
        
        query += " ORDER BY ts_code"
        
        cache_key = self._build_cache_key("get_stock_basic_info", ts_codes=ts_codes)
        df = await self._execute_query(query, tuple(params) if params else None, cache_key, cache_ttl=3600)
        
        return df


# ============================================================================
# 3. 统一数据访问工具类
# ============================================================================

class AlphaDataTool:
    """AlphaHome 统一数据访问工具
    
    这是用户主要使用的接口，整合了所有数据访问器
    """
    
    def __init__(self, db_manager, cache_manager=None):
        """
        Args:
            db_manager: DBManager 实例
            cache_manager: 可选的缓存管理器
        """
        self.db_manager = db_manager
        self.cache_manager = cache_manager
        
        # 初始化各个访问器
        self.index = IndexAccessor(db_manager, cache_manager)
        self.stock = StockAccessor(db_manager, cache_manager)
        # 可以继续添加其他访问器
        # self.fund = FundAccessor(db_manager, cache_manager)
        # self.macro = MacroAccessor(db_manager, cache_manager)
    
    # 便捷方法，直接暴露常用功能
    async def get_index_weights(self, *args, **kwargs):
        """获取指数权重数据的便捷方法"""
        return await self.index.get_index_weights(*args, **kwargs)
    
    async def get_stock_daily(self, *args, **kwargs):
        """获取股票日线数据的便捷方法"""
        return await self.stock.get_stock_daily(*args, **kwargs)


# ============================================================================
# 4. 异常定义
# ============================================================================

class DataAccessError(Exception):
    """数据访问异常基类"""
    pass

class ValidationError(DataAccessError):
    """数据验证异常"""
    pass

class CacheError(DataAccessError):
    """缓存相关异常"""
    pass


# ============================================================================
# 5. 使用示例
# ============================================================================

async def usage_example():
    """使用示例"""
    from alphahome.common.db_manager import DBManager
    
    # 创建数据库管理器（使用现有的）
    db_manager = DBManager(connection_string="your_connection_string_here")
    
    # 创建数据访问工具
    data_tool = AlphaDataTool(db_manager)
    
    # 获取指数权重数据
    weights = await data_tool.get_index_weights('000300.SH', '2024-01-01', '2024-12-31')
    print(f"获取到 {len(weights)} 条权重数据")
    
    # 获取股票日线数据
    stock_data = await data_tool.get_stock_daily(['000001.SZ', '000002.SZ'], '2024-01-01', '2024-01-31')
    print(f"获取到 {len(stock_data)} 条股票数据")
    
    # 获取最新指数权重
    latest_weights = await data_tool.index.get_latest_index_weights('000300.SH')
    print(f"最新权重数据: {len(latest_weights)} 条")
    
    await db_manager.close()


if __name__ == "__main__":
    # 运行示例
    asyncio.run(usage_example())