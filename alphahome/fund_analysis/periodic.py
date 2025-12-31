"""
周期性分析器

本模块实现周期性绩效分析功能，包括：
- 月度收益矩阵（年 x 月）
- 季度收益
- 年度收益
- 滚动收益、滚动夏普、滚动波动率

约定：
- 滚动计算的前 window-1 个值返回 NaN
- 周期收益使用期末净值 / 期初净值 - 1 计算
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

from ._constants import (
    DEFAULT_PERIODS_PER_YEAR,
    DEFAULT_RISK_FREE_RATE,
    DEFAULT_ROLLING_RETURN_WINDOW,
    DEFAULT_ROLLING_VOLATILITY_WINDOW,
)

logger = logging.getLogger(__name__)


class PeriodicAnalyzer:
    """
    周期性分析器
    
    提供周期性绩效分解和滚动指标计算。
    
    参数:
        periods_per_year: 年化因子，默认 250
    
    示例:
        >>> analyzer = PeriodicAnalyzer()
        >>> nav = pd.Series([1.0, 1.05, 1.10, 1.08], 
        ...                 index=pd.date_range('2024-01-01', periods=4, freq='M'))
        >>> monthly = analyzer.monthly_returns(nav)
    """
    
    def __init__(
        self,
        periods_per_year: int = DEFAULT_PERIODS_PER_YEAR,
        risk_free_rate: float = DEFAULT_RISK_FREE_RATE
    ):
        self.periods_per_year = periods_per_year
        self.risk_free_rate = risk_free_rate
    
    def _is_valid_data(self, data: pd.Series) -> bool:
        """检查数据是否有效"""
        if data is None:
            return False
        if not isinstance(data, pd.Series):
            return False
        data = data.dropna()
        if data.empty:
            return False
        return True
    
    def monthly_returns(self, nav_series: pd.Series) -> pd.DataFrame:
        """
        计算月度收益矩阵
        
        参数:
            nav_series: 净值序列（索引为 DatetimeIndex）
        
        返回:
            pd.DataFrame: 索引为年份，列为月份 (1-12)，值为月收益率
            缺失月份填充 NaN
        """
        if not self._is_valid_data(nav_series):
            return pd.DataFrame()
        
        nav_series = nav_series.dropna()
        
        # 确保索引是 DatetimeIndex
        if not isinstance(nav_series.index, pd.DatetimeIndex):
            try:
                nav_series.index = pd.to_datetime(nav_series.index)
            except Exception as e:
                logger.warning("无法将索引转换为 DatetimeIndex: %s", e)
                return pd.DataFrame()
        
        # 按月重采样，取月末值
        monthly_nav = nav_series.resample('ME').last()
        
        if monthly_nav.empty or len(monthly_nav) < 2:
            return pd.DataFrame()
        
        # 计算月收益率
        monthly_ret = monthly_nav.pct_change()
        
        # 创建年 x 月矩阵
        years = monthly_ret.index.year.unique()
        months = range(1, 13)
        
        result = pd.DataFrame(index=years, columns=months, dtype=float)
        
        for idx, ret in monthly_ret.items():
            if pd.notna(ret):
                result.loc[idx.year, idx.month] = ret
        
        return result
    
    def quarterly_returns(self, nav_series: pd.Series) -> pd.Series:
        """
        计算季度收益
        
        参数:
            nav_series: 净值序列
        
        返回:
            pd.Series: 索引为 'YYYY-QN' 格式，值为季度收益率
        """
        if not self._is_valid_data(nav_series):
            return pd.Series(dtype=float)
        
        nav_series = nav_series.dropna()
        
        # 确保索引是 DatetimeIndex
        if not isinstance(nav_series.index, pd.DatetimeIndex):
            try:
                nav_series.index = pd.to_datetime(nav_series.index)
            except Exception as e:
                logger.warning("无法将索引转换为 DatetimeIndex: %s", e)
                return pd.Series(dtype=float)
        
        # 按季度重采样，取季末值
        quarterly_nav = nav_series.resample('QE').last()
        
        if quarterly_nav.empty or len(quarterly_nav) < 2:
            return pd.Series(dtype=float)
        
        # 计算季度收益率
        quarterly_ret = quarterly_nav.pct_change().dropna()
        
        # 格式化索引为 'YYYY-QN'
        new_index = [f"{idx.year}-Q{idx.quarter}" for idx in quarterly_ret.index]
        quarterly_ret.index = new_index
        
        return quarterly_ret
    
    def yearly_returns(self, nav_series: pd.Series) -> pd.Series:
        """
        计算年度收益
        
        参数:
            nav_series: 净值序列
        
        返回:
            pd.Series: 索引为年份，值为年度收益率
        """
        if not self._is_valid_data(nav_series):
            return pd.Series(dtype=float)
        
        nav_series = nav_series.dropna()
        
        # 确保索引是 DatetimeIndex
        if not isinstance(nav_series.index, pd.DatetimeIndex):
            try:
                nav_series.index = pd.to_datetime(nav_series.index)
            except Exception as e:
                logger.warning("无法将索引转换为 DatetimeIndex: %s", e)
                return pd.Series(dtype=float)
        
        # 按年重采样，取年末值
        yearly_nav = nav_series.resample('YE').last()
        
        if yearly_nav.empty or len(yearly_nav) < 2:
            return pd.Series(dtype=float)
        
        # 计算年度收益率
        yearly_ret = yearly_nav.pct_change().dropna()
        
        # 索引改为年份
        yearly_ret.index = yearly_ret.index.year
        
        return yearly_ret
    
    def rolling_return(
        self,
        returns: pd.Series,
        window: int = DEFAULT_ROLLING_RETURN_WINDOW
    ) -> pd.Series:
        """
        计算滚动收益
        
        参数:
            returns: 收益率序列
            window: 滚动窗口大小（交易日），默认 252
        
        返回:
            pd.Series: 滚动收益，前 window-1 个值为 NaN
        """
        if not self._is_valid_data(returns):
            return pd.Series(dtype=float)
        
        returns = returns.dropna()
        
        if len(returns) < window:
            # 样本不足，全部返回 NaN
            return pd.Series(np.nan, index=returns.index)
        
        # 计算滚动累计收益
        def calc_cum_return(x):
            return np.exp(np.log1p(x).sum()) - 1
        
        rolling_ret = returns.rolling(window=window).apply(calc_cum_return, raw=False)
        
        return rolling_ret
    
    def rolling_sharpe(
        self,
        returns: pd.Series,
        window: int = DEFAULT_ROLLING_RETURN_WINDOW,
        risk_free_rate: Optional[float] = None
    ) -> pd.Series:
        """
        计算滚动夏普比率
        
        参数:
            returns: 收益率序列
            window: 滚动窗口大小（交易日），默认 252
            risk_free_rate: 无风险利率（年化），默认使用实例配置
        
        返回:
            pd.Series: 滚动夏普比率，前 window-1 个值为 NaN
        """
        if not self._is_valid_data(returns):
            return pd.Series(dtype=float)
        
        if risk_free_rate is None:
            risk_free_rate = self.risk_free_rate
        
        returns = returns.dropna()
        
        if len(returns) < window:
            return pd.Series(np.nan, index=returns.index)
        
        # 计算滚动均值和标准差
        rolling_mean = returns.rolling(window=window).mean()
        rolling_std = returns.rolling(window=window).std()
        
        # 年化
        ann_return = rolling_mean * self.periods_per_year
        ann_vol = rolling_std * np.sqrt(self.periods_per_year)
        
        # 计算夏普比率
        rolling_sharpe = (ann_return - risk_free_rate) / ann_vol
        
        # 标准差为零时设为 NaN
        rolling_sharpe = rolling_sharpe.replace([np.inf, -np.inf], np.nan)
        
        return rolling_sharpe
    
    def rolling_volatility(
        self,
        returns: pd.Series,
        window: int = DEFAULT_ROLLING_VOLATILITY_WINDOW
    ) -> pd.Series:
        """
        计算滚动波动率（已年化）
        
        参数:
            returns: 收益率序列
            window: 滚动窗口大小（交易日），默认 60
        
        返回:
            pd.Series: 滚动波动率（年化），前 window-1 个值为 NaN
        """
        if not self._is_valid_data(returns):
            return pd.Series(dtype=float)
        
        returns = returns.dropna()
        
        if len(returns) < window:
            return pd.Series(np.nan, index=returns.index)
        
        # 计算滚动标准差并年化
        rolling_std = returns.rolling(window=window).std()
        rolling_vol = rolling_std * np.sqrt(self.periods_per_year)
        
        return rolling_vol
