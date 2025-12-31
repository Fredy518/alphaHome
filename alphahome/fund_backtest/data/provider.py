"""数据提供者抽象基类"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict
import pandas as pd


class DataProvider(ABC):
    """
    数据提供者抽象基类
    
    框架上层只调用此接口，不直接写 SQL。
    具体实现可以是 AlphaDB、内存 DataFrame、Excel 等。
    """
    
    @abstractmethod
    def get_fund_nav(
        self,
        fund_ids: List[str],
        start_date: str,
        end_date: str,
        nav_type: str = 'unit_nav'
    ) -> pd.DataFrame:
        """
        获取基金净值面板
        
        Args:
            fund_ids: 基金代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            nav_type: 净值类型 (unit_nav | accum_nav | adj_nav)
        
        Returns:
            DataFrame with index=date, columns=fund_id, values=nav
        """
        pass
    
    @abstractmethod
    def get_rebalance_records(
        self,
        portfolio_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取调仓记录
        
        Args:
            portfolio_id: 组合/策略代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            DataFrame with columns:
            - rebalance_date: 调仓日期
            - fund_id: 基金代码
            - fund_name: 基金名称 (可选)
            - target_weight: 目标权重 (0-1)
        """
        pass
    
    @abstractmethod
    def get_fund_fee(
        self,
        fund_ids: List[str]
    ) -> pd.DataFrame:
        """
        获取基金费率
        
        Args:
            fund_ids: 基金代码列表
        
        Returns:
            DataFrame with columns:
            - fund_id: 基金代码
            - purchase_fee: 申购费率
            - redeem_fee: 赎回费率
        """
        pass
    
    @abstractmethod
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
            start_date: 开始日期
            end_date: 结束日期
            calendar_type: 日历类型 ('trade' | 'nav')
            exchange: 交易所代码（默认SSE上交所）
        
        Returns:
            DatetimeIndex
        """
        pass
    
    def get_portfolio_config(
        self,
        portfolio_id: str
    ) -> Optional[Dict]:
        """
        获取组合配置 (可选实现)
        
        Returns:
            {
                'portfolio_id': str,
                'portfolio_name': str,
                'initial_cash': float,
                'setup_date': str,
                'rebalance_delay': int,
                'purchase_fee_rate': float,
                'redeem_fee_rate': float,
                'management_fee': float,
                'rebalance_effective_delay': int,
            }
        """
        return None
    
    def get_benchmark_nav(
        self,
        benchmark_id: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.Series]:
        """
        获取基准净值 (可选实现)
        
        用于支持相对绩效分析，如信息比率、跟踪误差、Beta 等指标的计算。
        
        Args:
            benchmark_id: 基准代码
                - 指数代码: '000300.SH' (沪深300), '000905.SH' (中证500) 等
                - 基金代码: '000001.OF' 等
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        
        Returns:
            pd.Series: index=date (DatetimeIndex), values=nav (float)
            None: 如果基准不可用或未实现
        
        实现建议:
            1. 指数代码 (.SH/.SZ) 优先从 index_factor_pro 获取
            2. 若 index_factor_pro 无数据，回退到 index_dailybasic
            3. 基金代码 (.OF) 从 fund_nav 获取
        
        示例:
            >>> provider = AlphaDBDataProvider(engine=engine)
            >>> benchmark_nav = provider.get_benchmark_nav(
            ...     '000300.SH', '2020-01-01', '2024-12-31'
            ... )
            >>> if benchmark_nav is not None:
            ...     benchmark_returns = benchmark_nav.pct_change().dropna()
        """
        return None
