"""
风险分析器

本模块实现风险分析功能，包括：
- 跟踪误差 (Tracking Error)
- Beta 系数
- 相关性矩阵
- 集中度指标 (HHI, Top N)
- 换手率

所有指标遵循以下约定：
- 需要基准的指标，无基准时返回 NaN
- 空数据或样本不足时返回 NaN
- 相关性矩阵对称且对角线为 1
- HHI 范围 [1/n, 1]
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

from ._constants import (
    DEFAULT_PERIODS_PER_YEAR,
    DEFAULT_TOP_N_CONCENTRATION,
    MIN_SAMPLE_SIZE,
    TRANSACTION_DATE_FIELD,
    TRANSACTION_SIDE_FIELD,
    TRANSACTION_AMOUNT_FIELD,
    TURNOVER_EXCLUDED_SIDES,
)

logger = logging.getLogger(__name__)


class RiskAnalyzer:
    """
    风险分析器
    
    提供风险相关指标的计算，包括相对指标（需要基准）和绝对指标。
    
    参数:
        periods_per_year: 年化因子，默认 250（中国A股交易日）
    
    示例:
        >>> analyzer = RiskAnalyzer()
        >>> returns = pd.Series([0.01, -0.02, 0.03, 0.01])
        >>> benchmark_returns = pd.Series([0.005, -0.01, 0.02, 0.005])
        >>> analyzer.tracking_error(returns, benchmark_returns)
        0.0...
    """
    
    def __init__(self, periods_per_year: int = DEFAULT_PERIODS_PER_YEAR):
        self.periods_per_year = periods_per_year
    
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
        valid_data = data.dropna()
        if len(valid_data) < min_size:
            logger.debug("样本数不足 (n=%d < %d)，返回 NaN", len(valid_data), min_size)
            return False
        return True
    
    def _align_series(
        self,
        returns: pd.Series,
        benchmark_returns: pd.Series
    ) -> tuple:
        """
        对齐组合收益和基准收益序列
        
        参数:
            returns: 组合收益率序列
            benchmark_returns: 基准收益率序列
        
        返回:
            tuple: (对齐后的组合收益, 对齐后的基准收益)
        """
        # 取日期交集
        common_index = returns.index.intersection(benchmark_returns.index)
        if len(common_index) == 0:
            return pd.Series(dtype=float), pd.Series(dtype=float)
        
        aligned_returns = returns.loc[common_index].dropna()
        aligned_benchmark = benchmark_returns.loc[common_index].dropna()
        
        # 再次取交集（排除 NaN 后）
        common_index = aligned_returns.index.intersection(aligned_benchmark.index)
        return aligned_returns.loc[common_index], aligned_benchmark.loc[common_index]
    
    def tracking_error(
        self,
        returns: pd.Series,
        benchmark_returns: pd.Series
    ) -> float:
        """
        计算跟踪误差
        
        公式: std(r_p - r_b) * sqrt(periods_per_year)
        
        参数:
            returns: 组合收益率序列
            benchmark_returns: 基准收益率序列
        
        返回:
            float: 跟踪误差（年化），无基准或数据不足时返回 NaN
        """
        if benchmark_returns is None or not self._is_valid_data(benchmark_returns):
            logger.debug("基准数据无效，跟踪误差返回 NaN")
            return np.nan
        
        if not self._is_valid_data(returns):
            return np.nan
        
        aligned_returns, aligned_benchmark = self._align_series(returns, benchmark_returns)
        
        if len(aligned_returns) < MIN_SAMPLE_SIZE:
            logger.debug("对齐后样本数不足，跟踪误差返回 NaN")
            return np.nan
        
        # 计算主动收益
        active_returns = aligned_returns - aligned_benchmark
        
        # 计算跟踪误差（年化）
        te = float(active_returns.std() * np.sqrt(self.periods_per_year))
        return te
    
    def beta(
        self,
        returns: pd.Series,
        benchmark_returns: pd.Series
    ) -> float:
        """
        计算 Beta 系数
        
        公式: cov(r_p, r_b) / var(r_b)
        
        参数:
            returns: 组合收益率序列
            benchmark_returns: 基准收益率序列
        
        返回:
            float: Beta 系数，无基准或数据不足时返回 NaN
        """
        if benchmark_returns is None or not self._is_valid_data(benchmark_returns):
            logger.debug("基准数据无效，Beta 返回 NaN")
            return np.nan
        
        if not self._is_valid_data(returns):
            return np.nan
        
        aligned_returns, aligned_benchmark = self._align_series(returns, benchmark_returns)
        
        if len(aligned_returns) < MIN_SAMPLE_SIZE:
            logger.debug("对齐后样本数不足，Beta 返回 NaN")
            return np.nan
        
        # 计算协方差和方差
        cov = np.cov(aligned_returns, aligned_benchmark)[0, 1]
        var_benchmark = aligned_benchmark.var()
        
        if var_benchmark == 0 or np.isnan(var_benchmark):
            logger.debug("基准方差为零，Beta 返回 NaN")
            return np.nan
        
        beta_value = float(cov / var_benchmark)
        return beta_value
    
    def correlation_matrix(self, holdings_returns: pd.DataFrame) -> pd.DataFrame:
        """
        计算持仓收益的相关性矩阵
        
        参数:
            holdings_returns: 持仓收益 DataFrame
                - 索引: 日期
                - 列: fund_id
                - 值: 收益率
        
        返回:
            pd.DataFrame: 相关性矩阵（对称，对角线为 1）
                空数据时返回空 DataFrame
        """
        if holdings_returns is None:
            return pd.DataFrame()
        
        if not isinstance(holdings_returns, pd.DataFrame):
            logger.debug("输入不是 DataFrame，返回空矩阵")
            return pd.DataFrame()
        
        if holdings_returns.empty:
            return pd.DataFrame()
        
        # 删除全为 NaN 的列
        valid_holdings = holdings_returns.dropna(axis=1, how='all')
        
        if valid_holdings.empty or valid_holdings.shape[1] < 1:
            return pd.DataFrame()
        
        # 计算相关性矩阵
        corr_matrix = valid_holdings.corr()
        
        return corr_matrix
    
    def hhi(self, weights_t: pd.Series) -> float:
        """
        计算赫芬达尔-赫希曼指数 (HHI)
        
        公式: Σ(w_i)^2
        
        参数:
            weights_t: 单期权重（截面数据）
                - 索引: fund_id
                - 值: 权重 (0-1)
                - 前提: 权重应已归一化（和为 1）
        
        返回:
            float: HHI 值，范围 [1/n, 1]
                空数据时返回 NaN
        
        说明:
            - HHI = 1 表示完全集中（单一资产）
            - HHI = 1/n 表示完全分散（等权重）
            - 若权重和偏离 1 超过 1%，会发出警告并自动归一化
        """
        if weights_t is None:
            return np.nan
        
        if not isinstance(weights_t, pd.Series):
            logger.debug("输入不是 Series，HHI 返回 NaN")
            return np.nan
        
        # 排除 NaN 和零权重
        valid_weights = weights_t.dropna()
        valid_weights = valid_weights[valid_weights > 0]
        
        if valid_weights.empty:
            return np.nan
        
        # 检查权重是否归一化
        weight_sum = valid_weights.sum()
        if abs(weight_sum - 1.0) > 0.01:  # 容忍 1% 的误差
            logger.warning(
                "权重和 (%.4f) 偏离 1，已自动归一化。"
                "建议在调用前确保权重已归一化。",
                weight_sum
            )
            valid_weights = valid_weights / weight_sum
        
        # 计算 HHI
        hhi_value = float((valid_weights ** 2).sum())
        
        return hhi_value
    
    def top_n_concentration(
        self,
        weights_t: pd.Series,
        n: int = DEFAULT_TOP_N_CONCENTRATION
    ) -> float:
        """
        计算前 N 大持仓集中度
        
        参数:
            weights_t: 单期权重（截面数据）
            n: 前 N 大持仓，默认 5
        
        返回:
            float: 前 N 大持仓的权重之和
                空数据时返回 NaN
        """
        if weights_t is None:
            return np.nan
        
        if not isinstance(weights_t, pd.Series):
            logger.debug("输入不是 Series，Top N 集中度返回 NaN")
            return np.nan
        
        # 排除 NaN 和零权重
        valid_weights = weights_t.dropna()
        valid_weights = valid_weights[valid_weights > 0]
        
        if valid_weights.empty:
            return np.nan
        
        # 排序并取前 N 大
        sorted_weights = valid_weights.sort_values(ascending=False)
        top_n_weights = sorted_weights.head(n)
        
        concentration = float(top_n_weights.sum())
        return concentration
    
    def turnover_rate(
        self,
        transactions: pd.DataFrame,
        avg_aum: float
    ) -> float:
        """
        计算换手率
        
        公式: (Σ|buy| + Σ|sell|) / (2 * avg_aum)
        
        参数:
            transactions: 交易记录 DataFrame
                必需列:
                - settle_date: 结算日期
                - side: 交易方向 (1=买入, -1=卖出, 0=管理费)
                - amount: 交易金额（不含费用）
            avg_aum: 平均资产规模
        
        返回:
            float: 换手率（年化）
                无效数据时返回 NaN
        
        说明:
            - 使用 settle_date 作为日期字段
            - 排除 side=0 的管理费记录
            - 当前实现计算整个区间的换手率
            - 未来 Phase3 可扩展为按期汇总
        """
        if transactions is None:
            return np.nan
        
        if not isinstance(transactions, pd.DataFrame):
            logger.debug("输入不是 DataFrame，换手率返回 NaN")
            return np.nan
        
        if transactions.empty:
            return np.nan
        
        if avg_aum is None or avg_aum <= 0 or np.isnan(avg_aum):
            logger.debug("平均 AUM 无效，换手率返回 NaN")
            return np.nan
        
        # 检查必需列
        required_cols = [TRANSACTION_SIDE_FIELD, TRANSACTION_AMOUNT_FIELD]
        missing_cols = [col for col in required_cols if col not in transactions.columns]
        if missing_cols:
            logger.warning("交易记录缺少必需列: %s，换手率返回 NaN", missing_cols)
            return np.nan
        
        # 检查 settle_date 列（口径要求）
        if TRANSACTION_DATE_FIELD not in transactions.columns:
            logger.warning(
                "交易记录缺少 '%s' 列。当前实现计算整个区间的换手率，"
                "未来版本可能需要此列进行按期汇总。",
                TRANSACTION_DATE_FIELD
            )
        
        # 排除管理费等非交易记录
        valid_trades = transactions[
            ~transactions[TRANSACTION_SIDE_FIELD].isin(TURNOVER_EXCLUDED_SIDES)
        ].copy()
        
        if valid_trades.empty:
            return 0.0
        
        # 计算买入和卖出金额
        total_amount = valid_trades[TRANSACTION_AMOUNT_FIELD].abs().sum()
        
        # 计算换手率
        turnover = float(total_amount / (2 * avg_aum))
        
        return turnover
