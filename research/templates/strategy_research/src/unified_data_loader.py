#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
统一数据加载器 - 简化版集成方案

自动检测并选择最佳数据源：
1. 如果有ResearchContext，使用AlphaHome数据源
2. 否则回退到CSV数据源
"""

import pandas as pd
import backtrader as bt
from pathlib import Path
from typing import Optional, Union, List
import logging

logger = logging.getLogger(__name__)


class UnifiedDataLoader:
    """
    统一数据加载器
    
    智能选择数据源：
    - AlphaHome优先（如果可用）
    - CSV备用（向后兼容）
    """
    
    def __init__(self, research_context=None):
        """
        初始化数据加载器
        
        Args:
            research_context: 可选的ResearchContext实例
        """
        self.context = research_context
        self.logger = logger
        
        # 检测可用的数据源
        self.alphahome_available = self._check_alphahome_availability()
        
        if self.alphahome_available:
            self.logger.info("检测到AlphaHome数据源，将优先使用")
        else:
            self.logger.info("AlphaHome数据源不可用，将使用CSV数据源")
    
    def _check_alphahome_availability(self) -> bool:
        """检查AlphaHome数据源是否可用"""
        try:
            return (
                self.context is not None and 
                hasattr(self.context, 'data_tool') and
                self.context.data_tool is not None
            )
        except Exception:
            return False
    
    def load_stock_data(
        self, 
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        csv_path: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        加载股票数据
        
        Args:
            symbols: 股票代码（AlphaHome模式）
            start_date: 开始日期
            end_date: 结束日期
            csv_path: CSV文件路径（CSV模式）
            **kwargs: 其他参数
            
        Returns:
            标准化的股票数据DataFrame
        """
        if self.alphahome_available and symbols:
            return self._load_from_alphahome(symbols, start_date, end_date, **kwargs)
        elif csv_path:
            return self._load_from_csv(csv_path, **kwargs)
        else:
            raise ValueError(
                "请提供数据源参数：\n"
                "- AlphaHome模式：symbols, start_date, end_date\n"
                "- CSV模式：csv_path"
            )
    
    def _load_from_alphahome(
        self, 
        symbols: Union[str, List[str]], 
        start_date: Optional[str], # 允许为 None
        end_date: Optional[str],   # 允许为 None
        **kwargs
    ) -> pd.DataFrame:
        """从AlphaHome加载数据"""
        try:
            self.logger.info(f"从AlphaHome加载数据: {symbols}")
            
            # 确保symbols是列表
            if isinstance(symbols, str):
                symbols = [symbols]
            
            # 确保data_tool存在，避免Linter警告
            if self.context is None or self.context.data_tool is None:
                raise AttributeError("ResearchContext或其data_tool未正确初始化。")

            # 使用AlphaDataTool获取数据
            # 确保start_date和end_date是字符串类型，或者为None
            data = self.context.data_tool.get_stock_data(
                symbols=symbols,
                start_date=start_date, # 直接传递，因为AlphaDataTool的get_stock_data支持Optional[str]
                end_date=end_date,     # 直接传递
                **kwargs
            )
            
            # 标准化列名
            data = self._standardize_columns(data, source='alphahome')
            
            self.logger.info(f"成功加载 {len(data)} 条记录")
            return data
            
        except Exception as e:
            self.logger.error(f"AlphaHome数据加载失败: {e}")
            raise
    
    def _load_from_csv(self, csv_path: str, **kwargs) -> pd.DataFrame:
        """从CSV文件加载数据"""
        try:
            self.logger.info(f"从CSV加载数据: {csv_path}")
            
            csv_file = Path(csv_path)
            if not csv_file.exists():
                raise FileNotFoundError(f"CSV文件不存在: {csv_path}")
            
            # 读取CSV数据
            data = pd.read_csv(csv_file, **kwargs)
            
            # 标准化列名
            data = self._standardize_columns(data, source='csv')
            
            self.logger.info(f"成功加载 {len(data)} 条记录")
            return data
            
        except Exception as e:
            self.logger.error(f"CSV数据加载失败: {e}")
            raise
    
    def _standardize_columns(self, data: pd.DataFrame, source: str) -> pd.DataFrame:
        """标准化列名，确保backtrader兼容性"""
        # 标准列名映射
        column_mapping = {
            'alphahome': {
                'ts_code': 'symbol',
                'trade_date': 'date',
                'vol': 'volume'
            },
            'csv': {
                # CSV可能有不同的列名，根据实际情况调整
                'code': 'symbol',
                'datetime': 'date',
                'vol': 'volume',
                'Date': 'date',
                'date': 'date',
                'time': 'date',
                'timestamp': 'date'
            }
        }
        
        mapping = column_mapping.get(source, {})
        if mapping:
            data = data.rename(columns=mapping)
        
        # 确保必需的列存在
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            self.logger.warning(f"缺少列: {missing_columns}")
            # 如果缺少date列，尝试使用索引或其他列
            if 'date' in missing_columns:
                if data.index.name == 'date' or 'date' in str(data.index.dtype):
                    data = data.reset_index()
                elif len(data.columns) > 0 and pd.api.types.is_datetime64_any_dtype(data.iloc[:, 0]):
                    data = data.rename(columns={data.columns[0]: 'date'})
        
        # 确保日期列是datetime类型
        if 'date' in data.columns:
            # 处理可能的int类型日期（Unix时间戳）
            if pd.api.types.is_integer_dtype(data['date']):
                data['date'] = pd.to_datetime(data['date'], unit='s')
            else:
                data['date'] = pd.to_datetime(data['date'])
        
        # 确保所有价格列都是数值类型
        price_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in price_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
        
        return data
    
    def to_backtrader_feed(
        self, 
        data: pd.DataFrame, 
        symbol: Optional[str] = None
    ) -> bt.feeds.PandasData:
        """
        转换为backtrader数据源
        
        Args:
            data: 股票数据DataFrame
            symbol: 股票代码（用于过滤）
            
        Returns:
            backtrader PandasData实例
        """
        # 如果指定了symbol，过滤数据
        if symbol and 'symbol' in data.columns:
            data = data[data['symbol'] == symbol].copy()
        
        # 设置日期为索引
        if 'date' in data.columns:
            data = data.set_index('date')
        
        # 创建backtrader数据源，确保所有必需参数都明确指定
        bt_data = bt.feeds.PandasData( # noinspection PyArgumentList # type: ignore
            dataname=data,              # 数据DataFrame # type: ignore
            datetime=None,              # 使用索引作为日期，所以这里设置为None # type: ignore
            open='open', # type: ignore
            high='high', # type: ignore
            low='low', # type: ignore
            close='close', # type: ignore
            volume='volume', # type: ignore
            openinterest=-1             # 如果没有OI数据，通常设置为-1 # type: ignore
        )
        
        return bt_data
    
    def get_available_symbols(self) -> List[str]:
        """获取可用的股票代码列表"""
        if self.alphahome_available:
            try:
                # 确保data_tool存在
                if self.context is None or self.context.data_tool is None:
                    self.logger.warning("ResearchContext或其data_tool未正确初始化，无法获取股票列表。")
                    return []
                # 从AlphaHome获取股票列表
                stock_info = self.context.data_tool.get_stock_info()
                return stock_info['ts_code'].tolist()
            except Exception as e:
                self.logger.warning(f"获取股票列表失败: {e}")
                return []
        else:
            self.logger.info("CSV模式下无法自动获取股票列表")
            return []


# 便捷函数
def create_data_loader(research_context=None) -> UnifiedDataLoader:
    """创建统一数据加载器的便捷函数"""
    return UnifiedDataLoader(research_context)


def load_data_for_backtrader(
    research_context=None,
    symbols: Optional[Union[str, List[str]]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    csv_path: Optional[str] = None,
    **kwargs
) -> Union[bt.feeds.PandasData, List[bt.feeds.PandasData]]:
    """
    为backtrader加载数据的便捷函数
    
    Returns:
        单个股票返回PandasData，多个股票返回PandasData列表
    """
    loader = create_data_loader(research_context)
    
    # 加载数据
    data = loader.load_stock_data(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        csv_path=csv_path,
        **kwargs
    )
    
    # 转换为backtrader格式
    if symbols and isinstance(symbols, list) and len(symbols) > 1:
        # 多个股票
        bt_feeds = []
        for symbol in symbols:
            bt_feed = loader.to_backtrader_feed(data, symbol)
            bt_feeds.append(bt_feed)
        return bt_feeds
    else:
        # 单个股票
        return loader.to_backtrader_feed(data)
