"""
基础绩效指标计算器

本模块实现基础绩效指标的计算，包括：
- 收益指标：累计收益率、年化收益率
- 风险指标：年化波动率、VaR、CVaR
- 风险调整收益：夏普比率、索提诺比率、卡玛比率
- 胜率与盈亏：胜率、盈亏比

所有指标遵循以下约定：
- 空数据或样本不足时返回 NaN
- 标准差为零时返回 NaN（而非 0 或 inf）
- 无亏损样本时盈亏比返回 inf
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

from ._constants import (
    DEFAULT_PERIODS_PER_YEAR,
    DEFAULT_RISK_FREE_RATE,
    DEFAULT_VAR_CONFIDENCE,
    MIN_SAMPLE_SIZE,
    ZERO_STD_RETURNS_NAN,
    PROFIT_LOSS_RATIO_NO_LOSS,
)

logger = logging.getLogger(__name__)


class MetricsAnalyzer:
    """
    基础绩效指标计算器
    
    使用组合模式，可独立使用或被 PerformanceAnalyzer 聚合。
    
    参数:
        periods_per_year: 年化因子，默认 250（中国A股交易日）
        risk_free_rate: 无风险利率（年化），默认 0.0
    
    示例:
        >>> analyzer = MetricsAnalyzer()
        >>> returns = pd.Series([0.01, -0.02, 0.03, 0.01])
        >>> analyzer.cumulative_return(returns)
        0.0297...
    """
    
    def __init__(
        self,
        periods_per_year: int = DEFAULT_PERIODS_PER_YEAR,
        risk_free_rate: float = DEFAULT_RISK_FREE_RATE
    ):
        self.periods_per_year = periods_per_year
        self.risk_free_rate = risk_free_rate
    
    def _is_valid_data(self, data: pd.Series, min_size: int = MIN_SAMPLE_SIZE) -> bool:
        """
        检查数据是否有效
        
        参数:
            data: 待检查的数据序列
            min_size: 最小样本数要求
        
        返回:
            bool: 数据是否有效
        """
        if data is None:
            return False
        if not isinstance(data, pd.Series):
            return False
        if data.empty:
            return False
        # 排除 NaN 后检查样本数
        valid_data = data.dropna()
        if len(valid_data) < min_size:
            logger.debug("样本数不足 (n=%d < %d)，返回 NaN", len(valid_data), min_size)
            return False
        return True
    
    def cumulative_return(self, returns: pd.Series) -> float:
        """
        计算累计收益率
        
        公式: (1+r1)*(1+r2)*...*(1+rn) - 1
        
        参数:
            returns: 收益率序列
        
        返回:
            float: 累计收益率，空数据返回 NaN
        """
        if not self._is_valid_data(returns, min_size=1):
            return np.nan
        
        valid_returns = returns.dropna()
        if valid_returns.empty:
            return np.nan
        
        # 使用 log 方法避免数值溢出
        cum_ret = float(np.exp(np.log1p(valid_returns).sum()) - 1)
        return cum_ret
    
    def annualized_return(self, returns: pd.Series) -> float:
        """
        计算年化收益率 (CAGR)
        
        公式: (1 + cum_ret) ^ (periods_per_year / n) - 1
        
        参数:
            returns: 收益率序列
        
        返回:
            float: 年化收益率，空数据返回 NaN
        """
        if not self._is_valid_data(returns, min_size=1):
            return np.nan
        
        valid_returns = returns.dropna()
        cum_ret = self.cumulative_return(valid_returns)
        
        if np.isnan(cum_ret):
            return np.nan
        
        n_periods = len(valid_returns)
        if n_periods == 0:
            return np.nan
        
        # 处理累计收益为 -100% 的情况
        if cum_ret <= -1:
            return -1.0
        
        ann_ret = float((1 + cum_ret) ** (self.periods_per_year / n_periods) - 1)
        return ann_ret
    
    def annualized_volatility(self, returns: pd.Series) -> float:
        """
        计算年化波动率
        
        公式: std(r) * sqrt(periods_per_year)
        
        参数:
            returns: 收益率序列
        
        返回:
            float: 年化波动率，空数据或样本不足返回 NaN
        """
        if not self._is_valid_data(returns):
            return np.nan
        
        valid_returns = returns.dropna()
        std = valid_returns.std()
        
        # 标准差为零时返回 NaN
        if ZERO_STD_RETURNS_NAN and (std == 0 or np.isnan(std)):
            logger.debug("标准差为零或无效，返回 NaN")
            return np.nan
        
        ann_vol = float(std * np.sqrt(self.periods_per_year))
        return ann_vol
    
    def sharpe_ratio(self, returns: pd.Series) -> float:
        """
        计算夏普比率
        
        公式: (ann_ret - rf) / ann_vol
        
        参数:
            returns: 收益率序列
        
        返回:
            float: 夏普比率，波动率为零时返回 NaN
        """
        if not self._is_valid_data(returns):
            return np.nan
        
        ann_ret = self.annualized_return(returns)
        ann_vol = self.annualized_volatility(returns)
        
        if np.isnan(ann_ret) or np.isnan(ann_vol) or ann_vol == 0:
            return np.nan
        
        sharpe = float((ann_ret - self.risk_free_rate) / ann_vol)
        return sharpe
    
    def sortino_ratio(self, returns: pd.Series, mar: Optional[float] = None) -> float:
        """
        计算索提诺比率
        
        公式: (ann_ret - mar) / downside_deviation
        下行偏差 = sqrt(mean(min(r - mar_daily, 0)^2)) * sqrt(periods_per_year)
        
        参数:
            returns: 收益率序列
            mar: 最小可接受收益率（年化），默认为 risk_free_rate
        
        返回:
            float: 索提诺比率，下行偏差为零时返回 NaN
        """
        if not self._is_valid_data(returns):
            return np.nan
        
        if mar is None:
            mar = self.risk_free_rate
        
        valid_returns = returns.dropna()
        ann_ret = self.annualized_return(valid_returns)
        
        if np.isnan(ann_ret):
            return np.nan
        
        # 将年化 MAR 转换为日频
        mar_daily = mar / self.periods_per_year
        
        # 计算下行偏差
        downside_returns = np.minimum(valid_returns - mar_daily, 0)
        downside_var = np.mean(downside_returns ** 2)
        downside_std = np.sqrt(downside_var) * np.sqrt(self.periods_per_year)
        
        if downside_std == 0 or np.isnan(downside_std):
            logger.debug("下行偏差为零，返回 NaN")
            return np.nan
        
        sortino = float((ann_ret - mar) / downside_std)
        return sortino
    
    def calmar_ratio(self, ann_return: float, max_dd: float) -> float:
        """
        计算卡玛比率
        
        公式: ann_return / max_dd
        
        参数:
            ann_return: 年化收益率
            max_dd: 最大回撤（正数）
        
        返回:
            float: 卡玛比率，最大回撤为零时返回 NaN
        """
        if np.isnan(ann_return) or np.isnan(max_dd):
            return np.nan
        
        if max_dd == 0:
            logger.debug("最大回撤为零，返回 NaN")
            return np.nan
        
        calmar = float(ann_return / max_dd)
        return calmar
    
    def win_rate(self, returns: pd.Series) -> float:
        """
        计算胜率
        
        公式: count(r > 0) / count(r)
        
        参数:
            returns: 收益率序列
        
        返回:
            float: 胜率，范围 [0, 1]，空数据返回 NaN
        """
        if not self._is_valid_data(returns, min_size=1):
            return np.nan
        
        valid_returns = returns.dropna()
        if valid_returns.empty:
            return np.nan
        
        win_count = (valid_returns > 0).sum()
        total_count = len(valid_returns)
        
        win_rate_value = float(win_count / total_count)
        return win_rate_value
    
    def profit_loss_ratio(self, returns: pd.Series) -> float:
        """
        计算盈亏比
        
        公式: mean(r | r > 0) / |mean(r | r < 0)|
        
        参数:
            returns: 收益率序列
        
        返回:
            float: 盈亏比，无亏损时返回 inf，无盈利时返回 0
        """
        if not self._is_valid_data(returns, min_size=1):
            return np.nan
        
        valid_returns = returns.dropna()
        
        wins = valid_returns[valid_returns > 0]
        losses = valid_returns[valid_returns < 0]
        
        if wins.empty:
            return 0.0
        
        if losses.empty:
            return PROFIT_LOSS_RATIO_NO_LOSS
        
        avg_win = wins.mean()
        avg_loss = abs(losses.mean())
        
        if avg_loss == 0:
            return PROFIT_LOSS_RATIO_NO_LOSS
        
        ratio = float(avg_win / avg_loss)
        return ratio
    
    def var(self, returns: pd.Series, confidence: float = DEFAULT_VAR_CONFIDENCE) -> float:
        """
        计算风险价值 (VaR) - 历史模拟法
        
        参数:
            returns: 收益率序列
            confidence: 置信水平，默认 0.95
        
        返回:
            float: VaR（负数收益阈值），表示在置信水平下的最差收益
        """
        if not self._is_valid_data(returns):
            return np.nan
        
        valid_returns = returns.dropna()
        
        # 计算分位数（左尾）
        var_value = float(np.percentile(valid_returns, (1 - confidence) * 100))
        return var_value
    
    def cvar(self, returns: pd.Series, confidence: float = DEFAULT_VAR_CONFIDENCE) -> float:
        """
        计算条件风险价值 (CVaR/ES)
        
        公式: mean(r | r <= VaR)
        
        参数:
            returns: 收益率序列
            confidence: 置信水平，默认 0.95
        
        返回:
            float: CVaR（负数），CVaR <= VaR
        """
        if not self._is_valid_data(returns):
            return np.nan
        
        valid_returns = returns.dropna()
        var_value = self.var(valid_returns, confidence)
        
        if np.isnan(var_value):
            return np.nan
        
        # 计算尾部均值
        tail_returns = valid_returns[valid_returns <= var_value]
        
        if tail_returns.empty:
            return var_value
        
        cvar_value = float(tail_returns.mean())
        return cvar_value
    
    def information_ratio(
        self,
        returns: pd.Series,
        benchmark_returns: pd.Series
    ) -> float:
        """
        计算信息比率
        
        公式: mean(r_p - r_b) / std(r_p - r_b) * sqrt(periods_per_year)
        
        参数:
            returns: 组合收益率序列
            benchmark_returns: 基准收益率序列
        
        返回:
            float: 信息比率，无基准或数据不足时返回 NaN
        """
        if benchmark_returns is None or not self._is_valid_data(benchmark_returns):
            logger.debug("基准数据无效，信息比率返回 NaN")
            return np.nan
        
        if not self._is_valid_data(returns):
            return np.nan
        
        # 对齐数据
        common_index = returns.index.intersection(benchmark_returns.index)
        if len(common_index) < MIN_SAMPLE_SIZE:
            logger.debug("对齐后样本数不足，信息比率返回 NaN")
            return np.nan
        
        aligned_returns = returns.loc[common_index].dropna()
        aligned_benchmark = benchmark_returns.loc[common_index].dropna()
        
        # 再次取交集
        common_index = aligned_returns.index.intersection(aligned_benchmark.index)
        if len(common_index) < MIN_SAMPLE_SIZE:
            return np.nan
        
        aligned_returns = aligned_returns.loc[common_index]
        aligned_benchmark = aligned_benchmark.loc[common_index]
        
        # 计算主动收益
        active_returns = aligned_returns - aligned_benchmark
        
        # 计算信息比率
        mean_active = active_returns.mean()
        std_active = active_returns.std()
        
        if std_active == 0 or np.isnan(std_active):
            logger.debug("主动收益标准差为零，信息比率返回 NaN")
            return np.nan
        
        ir = float(mean_active / std_active * np.sqrt(self.periods_per_year))
        return ir
