"""
归因分析器

本模块实现归因分析功能，包括：
- 贡献分析 (Contribution Analysis)
- Brinson 归因 (可选，需要基准权重)

本期范围说明：
- 仅支持基金层贡献分析和相对基准的基金层归因
- 不做穿透到股票/行业层面的归因
- Brinson 归因的"分组"指基金类型（如股票型/债券型/混合型），而非底层行业

所有指标遵循以下约定：
- 贡献之和等于组合收益
- Brinson 三效应之和等于超额收益
- 无基准时仅返回贡献分析结果
"""

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class AttributionAnalyzer:
    """
    归因分析器
    
    提供贡献分析和 Brinson 归因功能。
    当缺少基准数据时，仅返回贡献分析结果。
    
    示例:
        >>> analyzer = AttributionAnalyzer()
        >>> weights = pd.Series({'fund_a': 0.4, 'fund_b': 0.3, 'fund_c': 0.3})
        >>> returns = pd.Series({'fund_a': 0.05, 'fund_b': 0.02, 'fund_c': -0.01})
        >>> contribution = analyzer.contribution(weights, returns)
        >>> print(contribution.sum())  # 等于组合收益
    """
    
    def contribution(
        self,
        weights_t: pd.Series,
        returns: pd.Series
    ) -> pd.Series:
        """
        贡献分析
        
        公式: contribution_i = w_i * r_i
        
        参数:
            weights_t: 单期权重（截面数据）
                - 索引: fund_id
                - 值: 权重 (0-1)
            returns: 各资产收益率
                - 索引: fund_id
                - 值: 收益率
        
        返回:
            pd.Series: 各资产贡献
                - 索引: fund_id
                - 值: 贡献值
                - 性质: sum(contribution) == sum(weights_t * returns)
        
        说明:
            - 使用期初权重计算
            - 索引不匹配的资产将被忽略
        """
        if weights_t is None or returns is None:
            return pd.Series(dtype=float)
        
        if not isinstance(weights_t, pd.Series) or not isinstance(returns, pd.Series):
            logger.debug("输入不是 Series，返回空 Series")
            return pd.Series(dtype=float)
        
        if weights_t.empty or returns.empty:
            return pd.Series(dtype=float)
        
        # 取索引交集
        common_index = weights_t.index.intersection(returns.index)
        
        if len(common_index) == 0:
            logger.debug("权重和收益无共同资产，返回空 Series")
            return pd.Series(dtype=float)
        
        aligned_weights = weights_t.loc[common_index]
        aligned_returns = returns.loc[common_index]
        
        # 计算贡献
        contribution = aligned_weights * aligned_returns
        
        return contribution
    
    def brinson_attribution(
        self,
        portfolio_weights: pd.Series,
        benchmark_weights: pd.Series,
        portfolio_returns: pd.Series,
        benchmark_returns: pd.Series
    ) -> Dict[str, float]:
        """
        Brinson 归因分析
        
        公式:
        - 配置效应 (Allocation): Σ(w_p,i - w_b,i) * (r_b,i - r_b)
        - 选择效应 (Selection): Σw_b,i * (r_p,i - r_b,i)
        - 交互效应 (Interaction): Σ(w_p,i - w_b,i) * (r_p,i - r_b,i)
        
        参数:
            portfolio_weights: 组合权重
                - 索引: fund_id
                - 值: 权重 (0-1)
            benchmark_weights: 基准权重
                - 索引: fund_id
                - 值: 权重 (0-1)
            portfolio_returns: 组合各资产收益率
                - 索引: fund_id
                - 值: 收益率
            benchmark_returns: 基准各资产收益率
                - 索引: fund_id
                - 值: 收益率
        
        返回:
            dict: Brinson 归因结果
                - allocation: 配置效应
                - selection: 选择效应
                - interaction: 交互效应
                - total_active: 总主动收益 = allocation + selection + interaction
                - portfolio_return: 组合收益
                - benchmark_return: 基准收益
        
        性质:
            allocation + selection + interaction == portfolio_return - benchmark_return
        
        说明:
            - 本期仅支持基金层归因，不穿透到底层股票/行业
            - 无基准权重时返回全 NaN
        """
        result = {
            'allocation': np.nan,
            'selection': np.nan,
            'interaction': np.nan,
            'total_active': np.nan,
            'portfolio_return': np.nan,
            'benchmark_return': np.nan,
        }
        
        # 验证输入
        if any(x is None for x in [portfolio_weights, benchmark_weights, 
                                    portfolio_returns, benchmark_returns]):
            logger.debug("输入数据不完整，Brinson 归因返回 NaN")
            return result
        
        if not all(isinstance(x, pd.Series) for x in [portfolio_weights, benchmark_weights,
                                                       portfolio_returns, benchmark_returns]):
            logger.debug("输入不是 Series，Brinson 归因返回 NaN")
            return result
        
        if any(x.empty for x in [portfolio_weights, benchmark_weights,
                                  portfolio_returns, benchmark_returns]):
            logger.debug("输入数据为空，Brinson 归因返回 NaN")
            return result
        
        # 取所有输入的索引交集
        common_index = (
            portfolio_weights.index
            .intersection(benchmark_weights.index)
            .intersection(portfolio_returns.index)
            .intersection(benchmark_returns.index)
        )
        
        if len(common_index) == 0:
            logger.debug("无共同资产，Brinson 归因返回 NaN")
            return result
        
        # 对齐数据
        w_p = portfolio_weights.loc[common_index]
        w_b = benchmark_weights.loc[common_index]
        r_p = portfolio_returns.loc[common_index]
        r_b = benchmark_returns.loc[common_index]
        
        # 计算组合和基准的总收益
        portfolio_return = float((w_p * r_p).sum())
        benchmark_return = float((w_b * r_b).sum())
        
        # 计算 Brinson 三效应
        # 配置效应: Σ(w_p,i - w_b,i) * (r_b,i - r_b)
        allocation = float(((w_p - w_b) * (r_b - benchmark_return)).sum())
        
        # 选择效应: Σw_b,i * (r_p,i - r_b,i)
        selection = float((w_b * (r_p - r_b)).sum())
        
        # 交互效应: Σ(w_p,i - w_b,i) * (r_p,i - r_b,i)
        interaction = float(((w_p - w_b) * (r_p - r_b)).sum())
        
        # 总主动收益
        total_active = allocation + selection + interaction
        
        result = {
            'allocation': allocation,
            'selection': selection,
            'interaction': interaction,
            'total_active': total_active,
            'portfolio_return': portfolio_return,
            'benchmark_return': benchmark_return,
        }
        
        return result
