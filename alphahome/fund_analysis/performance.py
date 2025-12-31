"""
绩效分析器 - 统一入口

本模块提供 PerformanceAnalyzer 类，作为基金分析模块的统一入口。
使用组合模式聚合各专门分析器：
- MetricsAnalyzer: 基础绩效指标
- DrawdownAnalyzer: 回撤分析
- PeriodicAnalyzer: 周期性分析
- RiskAnalyzer: 风险分析
- AttributionAnalyzer: 归因分析
- ReportBuilder: 报告生成
- Visualization: 可视化
"""

import logging
from typing import Dict, List, Optional, Any, Tuple, Union

import numpy as np
import pandas as pd

from ._constants import (
    DEFAULT_PERIODS_PER_YEAR,
    DEFAULT_RISK_FREE_RATE,
    DEFAULT_TOP_N_DRAWDOWNS,
    DEFAULT_ROLLING_RETURN_WINDOW,
    DEFAULT_ROLLING_VOLATILITY_WINDOW,
    DEFAULT_TOP_N_CONCENTRATION,
    FFILL_LIMIT,
)
from ._schema import METRICS_SCHEMA_KEYS
from .metrics import MetricsAnalyzer
from .drawdown import DrawdownAnalyzer, DrawdownPeriod
from .periodic import PeriodicAnalyzer
from .risk import RiskAnalyzer
from .attribution import AttributionAnalyzer
from .report import ReportBuilder

logger = logging.getLogger(__name__)

# 延迟导入 Visualization 以避免 matplotlib 依赖问题
_Visualization = None

def _get_visualization():
    """延迟导入 Visualization 类"""
    global _Visualization
    if _Visualization is None:
        from .visualization import Visualization
        _Visualization = Visualization
    return _Visualization


class PerformanceAnalyzer:
    """
    绩效分析器 - 统一入口
    
    使用组合模式聚合各专门分析器，提供统一的分析接口。
    
    参数:
        periods_per_year: 年化因子，默认 250（中国A股交易日）
        risk_free_rate: 无风险利率（年化），默认 0.0
    
    示例:
        >>> from alphahome.fund_analysis import PerformanceAnalyzer
        >>> 
        >>> analyzer = PerformanceAnalyzer()
        >>> nav = pd.Series([1.0, 1.01, 0.99, 1.02, 1.03],
        ...                 index=pd.date_range('2024-01-01', periods=5))
        >>> returns = nav.pct_change().dropna()
        >>> 
        >>> # 计算绩效指标
        >>> metrics = analyzer.calculate_metrics(returns, nav)
        >>> print(f"年化收益: {metrics['annualized_return']:.2%}")
        >>> print(f"最大回撤: {metrics['max_drawdown']:.2%}")
    """
    
    def __init__(
        self,
        periods_per_year: int = DEFAULT_PERIODS_PER_YEAR,
        risk_free_rate: float = DEFAULT_RISK_FREE_RATE
    ):
        self.periods_per_year = periods_per_year
        self.risk_free_rate = risk_free_rate
        
        # 组合各分析器
        self._metrics = MetricsAnalyzer(periods_per_year, risk_free_rate)
        self._drawdown = DrawdownAnalyzer()
        self._periodic = PeriodicAnalyzer(periods_per_year, risk_free_rate)
        self._risk = RiskAnalyzer(periods_per_year)
        self._attribution = AttributionAnalyzer()
        self._report = ReportBuilder()
        self._viz = None  # 延迟初始化，避免 matplotlib 依赖
    
    def _align_benchmark(
        self,
        returns: pd.Series,
        benchmark: pd.Series,
        ffill_limit: int = FFILL_LIMIT
    ) -> Tuple[pd.Series, pd.Series, bool]:
        """
        对齐组合收益率和基准净值
        
        对齐规则:
        1. 对齐到组合与基准的日期交集
        2. 对基准净值做前值填充 (ffill)，最大容忍缺口为 ffill_limit
        3. 超过 ffill_limit 的缺口保留 NaN
        4. 若基准起始晚于组合，前段数据标记为 NaN
        
        Args:
            returns: 组合收益率序列
            benchmark: 基准净值序列
            ffill_limit: 前值填充最大容忍缺口（交易日数）
        
        Returns:
            Tuple[aligned_returns, aligned_benchmark_returns, has_warning]:
            - aligned_returns: 对齐后的组合收益率
            - aligned_benchmark_returns: 对齐后的基准收益率
            - has_warning: 是否有数据质量警告
        """
        has_warning = False
        
        if benchmark is None or benchmark.empty:
            return returns, pd.Series(dtype=float), False
        
        # 确保索引是 DatetimeIndex
        if not isinstance(returns.index, pd.DatetimeIndex):
            returns = returns.copy()
            returns.index = pd.to_datetime(returns.index)
        if not isinstance(benchmark.index, pd.DatetimeIndex):
            benchmark = benchmark.copy()
            benchmark.index = pd.to_datetime(benchmark.index)
        
        # 获取组合的日期范围
        portfolio_dates = returns.index
        
        # 将基准净值 reindex 到组合日期，并做前值填充
        benchmark_aligned = benchmark.reindex(portfolio_dates)
        
        # 计算填充前的缺失数量
        missing_before_ffill = benchmark_aligned.isna().sum()
        
        # 前值填充，限制最大填充天数
        benchmark_aligned = benchmark_aligned.ffill(limit=ffill_limit)
        
        # 计算填充后仍然缺失的数量
        missing_after_ffill = benchmark_aligned.isna().sum()
        
        # 检查是否有超过 ffill_limit 的缺口
        if missing_after_ffill > 0:
            # 检查是否是因为基准起始晚于组合
            benchmark_start = benchmark.index.min()
            portfolio_start = portfolio_dates.min()
            
            if benchmark_start > portfolio_start:
                # 基准起始晚于组合，前段数据无法计算
                early_missing = (portfolio_dates < benchmark_start).sum()
                gap_missing = missing_after_ffill - early_missing
                
                if gap_missing > 0:
                    logger.warning(
                        "基准数据缺失 %d 天 (超过 ffill_limit=%d)，"
                        "部分日期无法计算相对指标。"
                        "另有 %d 天因基准起始晚于组合而无数据。",
                        gap_missing, ffill_limit, early_missing
                    )
                    has_warning = True
                else:
                    logger.debug(
                        "基准起始日期 (%s) 晚于组合起始日期 (%s)，"
                        "前 %d 天无法计算相对指标。",
                        benchmark_start.date(), portfolio_start.date(), early_missing
                    )
            else:
                logger.warning(
                    "基准数据缺失 %d 天 (超过 ffill_limit=%d)，"
                    "部分日期无法计算相对指标。",
                    missing_after_ffill, ffill_limit
                )
                has_warning = True
        
        # 计算基准收益率
        # 重要：对 ffill 后的净值计算收益率，这样 ffill 补齐的日期收益率为 0
        # 而不是对原始净值计算收益率后再 reindex（会导致补齐日收益率为 NaN）
        benchmark_returns_aligned = benchmark_aligned.pct_change()
        
        # 对于超过 ffill_limit 的缺口（benchmark_aligned 仍为 NaN），收益率也是 NaN
        # 这是正确的行为，因为我们无法确定这些日期的基准表现
        
        # 对齐到共同的非 NaN 日期
        valid_mask = returns.notna() & benchmark_returns_aligned.notna()
        aligned_returns = returns[valid_mask]
        aligned_benchmark_returns = benchmark_returns_aligned[valid_mask]
        
        return aligned_returns, aligned_benchmark_returns, has_warning
    
    def calculate_metrics(
        self,
        returns: pd.Series,
        nav_series: pd.Series,
        benchmark: Optional[pd.Series] = None
    ) -> Dict[str, Any]:
        """
        计算所有绩效指标
        
        参数:
            returns: 收益率序列
            nav_series: 净值序列
            benchmark: 基准净值序列（可选）
        
        返回:
            dict: 固定 schema 的指标字典，缺失值为 np.nan
        
        输出 schema:
            - cumulative_return: 累计收益率
            - annualized_return: 年化收益率
            - annualized_volatility: 年化波动率
            - max_drawdown: 最大回撤（正数）
            - max_drawdown_start: 最大回撤开始日期
            - max_drawdown_end: 最大回撤结束日期
            - sharpe_ratio: 夏普比率
            - sortino_ratio: 索提诺比率
            - calmar_ratio: 卡玛比率
            - win_rate: 胜率
            - profit_loss_ratio: 盈亏比
            - var_95: 95% VaR
            - cvar_95: 95% CVaR
            - total_days: 总交易日数
            - information_ratio: 信息比率（需要基准）
            - tracking_error: 跟踪误差（需要基准）
            - beta: Beta（需要基准）
            - excess_return: 超额收益（需要基准）
        
        基准对齐规则:
            - 仅对 benchmark_nav 做 ffill，limit=FFILL_LIMIT (默认 5 天)
            - 超过 ffill_limit 的缺口保留 NaN，相对指标返回 NaN 并记录 warning
            - 若基准起始晚于组合，前段数据标记为 NaN，不参与相对指标计算
        """
        # 初始化结果字典，所有值默认为 NaN
        result = {key: np.nan for key in METRICS_SCHEMA_KEYS}
        
        # 检查输入数据
        if returns is None or nav_series is None:
            logger.debug("输入数据为空，返回全 NaN 结果")
            result['total_days'] = 0
            return result
        
        returns = returns.dropna() if isinstance(returns, pd.Series) else pd.Series(dtype=float)
        nav_series = nav_series.dropna() if isinstance(nav_series, pd.Series) else pd.Series(dtype=float)
        
        if returns.empty or nav_series.empty:
            logger.debug("输入数据为空，返回全 NaN 结果")
            result['total_days'] = 0
            return result
        
        # 检查 returns 和 nav_series 的索引一致性
        # 收益率应该是净值的 pct_change()，索引应该是净值索引的子集
        if not returns.index.isin(nav_series.index).all():
            # 找出不在 nav_series 中的日期
            missing_dates = returns.index.difference(nav_series.index)
            if len(missing_dates) > 0:
                logger.warning(
                    "returns 索引中有 %d 个日期不在 nav_series 索引中。"
                    "这可能导致收益指标和回撤指标的计算窗口不一致。"
                    "建议使用 nav_series.pct_change().dropna() 生成 returns。",
                    len(missing_dates)
                )
        
        # 基础指标
        result['cumulative_return'] = self._metrics.cumulative_return(returns)
        result['annualized_return'] = self._metrics.annualized_return(returns)
        result['annualized_volatility'] = self._metrics.annualized_volatility(returns)
        result['sharpe_ratio'] = self._metrics.sharpe_ratio(returns)
        result['sortino_ratio'] = self._metrics.sortino_ratio(returns)
        result['win_rate'] = self._metrics.win_rate(returns)
        result['profit_loss_ratio'] = self._metrics.profit_loss_ratio(returns)
        result['var_95'] = self._metrics.var(returns, confidence=0.95)
        result['cvar_95'] = self._metrics.cvar(returns, confidence=0.95)
        result['total_days'] = len(returns)
        
        # 回撤指标
        max_dd, dd_start, dd_end = self._drawdown.max_drawdown(nav_series)
        result['max_drawdown'] = max_dd
        result['max_drawdown_start'] = str(dd_start) if dd_start else None
        result['max_drawdown_end'] = str(dd_end) if dd_end else None
        
        # 卡玛比率（需要年化收益和最大回撤）
        result['calmar_ratio'] = self._metrics.calmar_ratio(
            result['annualized_return'],
            result['max_drawdown']
        )
        
        # 基准相关指标
        if benchmark is not None and not benchmark.empty:
            # 对齐基准数据（使用 ffill_limit 规则）
            aligned_returns, aligned_benchmark_returns, _ = self._align_benchmark(
                returns, benchmark, ffill_limit=FFILL_LIMIT
            )
            
            if not aligned_benchmark_returns.empty and len(aligned_benchmark_returns) >= 2:
                # 信息比率
                result['information_ratio'] = self._metrics.information_ratio(
                    aligned_returns, aligned_benchmark_returns
                )
                
                # 跟踪误差
                result['tracking_error'] = self._risk.tracking_error(
                    aligned_returns, aligned_benchmark_returns
                )
                
                # Beta
                result['beta'] = self._risk.beta(
                    aligned_returns, aligned_benchmark_returns
                )
                
                # 超额收益（使用对齐后的数据计算）
                portfolio_ann_ret = self._metrics.annualized_return(aligned_returns)
                benchmark_ann_ret = self._metrics.annualized_return(aligned_benchmark_returns)
                if not np.isnan(portfolio_ann_ret) and not np.isnan(benchmark_ann_ret):
                    result['excess_return'] = portfolio_ann_ret - benchmark_ann_ret
                else:
                    result['excess_return'] = np.nan
            else:
                logger.debug(
                    "对齐后的基准数据不足 (n=%d)，无法计算相对指标",
                    len(aligned_benchmark_returns)
                )
                result['information_ratio'] = np.nan
                result['tracking_error'] = np.nan
                result['beta'] = np.nan
                result['excess_return'] = np.nan
        else:
            result['information_ratio'] = np.nan
            result['tracking_error'] = np.nan
            result['beta'] = np.nan
            result['excess_return'] = np.nan
        
        return result
    
    def analyze_drawdowns(
        self,
        nav_series: pd.Series,
        top_n: int = DEFAULT_TOP_N_DRAWDOWNS
    ) -> Dict[str, Any]:
        """
        回撤分析
        
        参数:
            nav_series: 净值序列
            top_n: 返回的最大回撤周期数，默认 5
        
        返回:
            dict: 回撤分析结果
                - max_drawdown: 最大回撤（正数）
                - max_drawdown_start: 最大回撤开始日期
                - max_drawdown_end: 最大回撤结束日期
                - max_drawdown_recovery: 最大回撤恢复日期
                - avg_drawdown_duration: 平均回撤持续时间
                - max_drawdown_duration: 最大回撤持续时间
                - top_n_drawdowns: 前 N 大回撤周期列表
                - underwater_curve: 水下曲线
        """
        result = {
            'max_drawdown': np.nan,
            'max_drawdown_start': None,
            'max_drawdown_end': None,
            'max_drawdown_recovery': None,
            'avg_drawdown_duration': np.nan,
            'max_drawdown_duration': 0,
            'top_n_drawdowns': [],
            'underwater_curve': pd.Series(dtype=float),
        }
        
        if nav_series is None or nav_series.empty:
            return result
        
        nav_series = nav_series.dropna()
        if nav_series.empty:
            return result
        
        # 最大回撤
        max_dd, dd_start, dd_end = self._drawdown.max_drawdown(nav_series)
        result['max_drawdown'] = max_dd
        result['max_drawdown_start'] = str(dd_start) if dd_start else None
        result['max_drawdown_end'] = str(dd_end) if dd_end else None
        
        # 前 N 大回撤
        top_drawdowns = self._drawdown.top_n_drawdowns(nav_series, top_n)
        result['top_n_drawdowns'] = [dd.to_dict() for dd in top_drawdowns]
        
        # 最大回撤恢复日期
        if top_drawdowns:
            # 找到最大回撤对应的周期
            max_dd_period = max(top_drawdowns, key=lambda x: x.drawdown)
            if max_dd_period.recovery_date:
                result['max_drawdown_recovery'] = str(max_dd_period.recovery_date)
        
        # 回撤持续时间
        result['avg_drawdown_duration'] = self._drawdown.avg_drawdown_duration(nav_series)
        result['max_drawdown_duration'] = self._drawdown.max_drawdown_duration(nav_series)
        
        # 水下曲线
        result['underwater_curve'] = self._drawdown.underwater_curve(nav_series)
        
        return result
    
    def calculate_periodic_returns(self, nav_series: pd.Series) -> Dict[str, Any]:
        """
        计算周期性收益
        
        参数:
            nav_series: 净值序列
        
        返回:
            dict: 周期性收益结果
                - monthly_returns: 月度收益矩阵（年 x 月）
                - quarterly_returns: 季度收益序列
                - yearly_returns: 年度收益序列
        """
        result = {
            'monthly_returns': pd.DataFrame(),
            'quarterly_returns': pd.Series(dtype=float),
            'yearly_returns': pd.Series(dtype=float),
        }
        
        if nav_series is None or nav_series.empty:
            return result
        
        nav_series = nav_series.dropna()
        if nav_series.empty:
            return result
        
        result['monthly_returns'] = self._periodic.monthly_returns(nav_series)
        result['quarterly_returns'] = self._periodic.quarterly_returns(nav_series)
        result['yearly_returns'] = self._periodic.yearly_returns(nav_series)
        
        return result
    
    def calculate_rolling_metrics(
        self,
        returns: pd.Series,
        return_window: int = DEFAULT_ROLLING_RETURN_WINDOW,
        volatility_window: int = DEFAULT_ROLLING_VOLATILITY_WINDOW
    ) -> Dict[str, pd.Series]:
        """
        计算滚动指标
        
        参数:
            returns: 收益率序列
            return_window: 滚动收益/夏普窗口，默认 252
            volatility_window: 滚动波动率窗口，默认 60
        
        返回:
            dict: 滚动指标结果
                - rolling_return: 滚动收益
                - rolling_sharpe: 滚动夏普比率
                - rolling_volatility: 滚动波动率
        """
        result = {
            'rolling_return': pd.Series(dtype=float),
            'rolling_sharpe': pd.Series(dtype=float),
            'rolling_volatility': pd.Series(dtype=float),
        }
        
        if returns is None or returns.empty:
            return result
        
        returns = returns.dropna()
        if returns.empty:
            return result
        
        result['rolling_return'] = self._periodic.rolling_return(returns, return_window)
        result['rolling_sharpe'] = self._periodic.rolling_sharpe(returns, return_window)
        result['rolling_volatility'] = self._periodic.rolling_volatility(returns, volatility_window)
        
        return result
    
    def calculate_risk_metrics(
        self,
        returns: pd.Series,
        benchmark: Optional[pd.Series] = None,
        weights_t: Optional[pd.Series] = None,
        holdings_returns: Optional[pd.DataFrame] = None,
        transactions: Optional[pd.DataFrame] = None,
        avg_aum: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        计算风险指标
        
        参数:
            returns: 组合收益率序列
            benchmark: 基准净值序列（可选）
            weights_t: 单期权重（可选，用于集中度计算）
            holdings_returns: 持仓收益 DataFrame（可选，用于相关性矩阵）
            transactions: 交易记录 DataFrame（可选，用于换手率）
            avg_aum: 平均资产规模（可选，用于换手率）
        
        返回:
            dict: 风险指标结果
                - tracking_error: 跟踪误差（需要基准）
                - beta: Beta（需要基准）
                - correlation_matrix: 相关性矩阵（需要持仓收益）
                - hhi: 赫芬达尔指数（需要权重）
                - top_n_concentration: 前 N 大持仓集中度（需要权重）
                - turnover_rate: 换手率（需要交易记录和 AUM）
        
        基准对齐规则:
            - 仅对 benchmark_nav 做 ffill，limit=FFILL_LIMIT (默认 5 天)
            - 超过 ffill_limit 的缺口保留 NaN，相对指标返回 NaN 并记录 warning
        """
        result = {
            'tracking_error': np.nan,
            'beta': np.nan,
            'correlation_matrix': pd.DataFrame(),
            'hhi': np.nan,
            'top_n_concentration': np.nan,
            'turnover_rate': np.nan,
        }
        
        if returns is None or returns.empty:
            return result
        
        returns = returns.dropna()
        if returns.empty:
            return result
        
        # 基准相关指标（使用对齐逻辑）
        if benchmark is not None and not benchmark.empty:
            aligned_returns, aligned_benchmark_returns, _ = self._align_benchmark(
                returns, benchmark, ffill_limit=FFILL_LIMIT
            )
            
            if not aligned_benchmark_returns.empty and len(aligned_benchmark_returns) >= 2:
                result['tracking_error'] = self._risk.tracking_error(
                    aligned_returns, aligned_benchmark_returns
                )
                result['beta'] = self._risk.beta(
                    aligned_returns, aligned_benchmark_returns
                )
            else:
                logger.debug(
                    "对齐后的基准数据不足 (n=%d)，无法计算跟踪误差和 Beta",
                    len(aligned_benchmark_returns)
                )
        
        # 相关性矩阵
        if holdings_returns is not None and not holdings_returns.empty:
            result['correlation_matrix'] = self._risk.correlation_matrix(holdings_returns)
        
        # 集中度指标
        if weights_t is not None and not weights_t.empty:
            result['hhi'] = self._risk.hhi(weights_t)
            result['top_n_concentration'] = self._risk.top_n_concentration(
                weights_t, DEFAULT_TOP_N_CONCENTRATION
            )
        
        # 换手率
        if transactions is not None and avg_aum is not None:
            result['turnover_rate'] = self._risk.turnover_rate(transactions, avg_aum)
        
        return result
    
    def calculate_attribution(
        self,
        weights_t: pd.Series,
        returns: pd.Series,
        benchmark_weights: Optional[pd.Series] = None,
        benchmark_returns: Optional[pd.Series] = None
    ) -> Dict[str, Any]:
        """
        计算归因分析
        
        参数:
            weights_t: 组合权重（截面数据）
            returns: 各资产收益率（截面数据）
            benchmark_weights: 基准权重（可选，Brinson 归因需要）
            benchmark_returns: 基准各资产收益率（可选，Brinson 归因需要）
        
        返回:
            dict: 归因分析结果
                - contribution: 各资产贡献（pd.Series）
                - allocation: 配置效应（需要基准）
                - selection: 选择效应（需要基准）
                - interaction: 交互效应（需要基准）
                - total_active: 总主动收益（需要基准）
                - portfolio_return: 组合收益
                - benchmark_return: 基准收益（需要基准）
        """
        result = {
            'contribution': pd.Series(dtype=float),
            'allocation': np.nan,
            'selection': np.nan,
            'interaction': np.nan,
            'total_active': np.nan,
            'portfolio_return': np.nan,
            'benchmark_return': np.nan,
        }
        
        if weights_t is None or returns is None:
            return result
        
        if weights_t.empty or returns.empty:
            return result
        
        # 贡献分析（不需要基准）
        result['contribution'] = self._attribution.contribution(weights_t, returns)
        
        # 计算组合收益
        if not result['contribution'].empty:
            result['portfolio_return'] = float(result['contribution'].sum())
        
        # Brinson 归因（需要基准）
        if (benchmark_weights is not None and benchmark_returns is not None and
            not benchmark_weights.empty and not benchmark_returns.empty):
            brinson = self._attribution.brinson_attribution(
                weights_t, benchmark_weights, returns, benchmark_returns
            )
            result['allocation'] = brinson['allocation']
            result['selection'] = brinson['selection']
            result['interaction'] = brinson['interaction']
            result['total_active'] = brinson['total_active']
            result['benchmark_return'] = brinson['benchmark_return']
        
        return result
    
    def to_dict(self, nav_series: pd.Series, returns: Optional[pd.Series] = None) -> Dict[str, Any]:
        """
        输出所有指标为 JSON 可序列化的字典
        
        这是报告生成的单一数据源。
        
        参数:
            nav_series: 净值序列
            returns: 收益率序列（可选，若不提供则从净值计算）
        
        返回:
            dict: 可 JSON 序列化的完整分析结果
        """
        if nav_series is None or nav_series.empty:
            return {
                'metrics': {},
                'drawdowns': {},
                'periodic': {},
            }
        
        nav_series = nav_series.dropna()
        
        # 如果未提供收益率，从净值计算
        if returns is None:
            returns = nav_series.pct_change().dropna()
        else:
            returns = returns.dropna()
        
        # 计算各类指标
        metrics = self.calculate_metrics(returns, nav_series)
        drawdowns = self.analyze_drawdowns(nav_series)
        periodic = self.calculate_periodic_returns(nav_series)
        
        # 转换为可序列化格式
        result = {
            'metrics': self._serialize_metrics(metrics),
            'drawdowns': self._serialize_drawdowns(drawdowns),
            'periodic': self._serialize_periodic(periodic),
        }
        
        return result
    
    def _serialize_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """将指标字典转换为可序列化格式"""
        result = {}
        for key, value in metrics.items():
            if isinstance(value, (np.floating, np.integer)):
                value = float(value) if not np.isnan(value) else None
            elif isinstance(value, float) and np.isnan(value):
                value = None
            result[key] = value
        return result
    
    def _serialize_drawdowns(self, drawdowns: Dict[str, Any]) -> Dict[str, Any]:
        """将回撤分析结果转换为可序列化格式"""
        result = {}
        for key, value in drawdowns.items():
            if key == 'underwater_curve':
                # 转换 Series 为列表
                if isinstance(value, pd.Series) and not value.empty:
                    result[key] = [
                        {'date': str(idx.date()) if hasattr(idx, 'date') else str(idx), 
                         'value': float(v) if not np.isnan(v) else None}
                        for idx, v in value.items()
                    ]
                else:
                    result[key] = []
            elif isinstance(value, (np.floating, np.integer)):
                result[key] = float(value) if not np.isnan(value) else None
            elif isinstance(value, float) and np.isnan(value):
                result[key] = None
            else:
                result[key] = value
        return result
    
    def _serialize_periodic(self, periodic: Dict[str, Any]) -> Dict[str, Any]:
        """将周期性分析结果转换为可序列化格式"""
        result = {}
        for key, value in periodic.items():
            if isinstance(value, pd.DataFrame):
                if not value.empty:
                    # DataFrame 转换为嵌套字典
                    result[key] = value.to_dict()
                else:
                    result[key] = {}
            elif isinstance(value, pd.Series):
                if not value.empty:
                    # Series 转换为字典
                    result[key] = {
                        str(k): (float(v) if not np.isnan(v) else None)
                        for k, v in value.items()
                    }
                else:
                    result[key] = {}
            else:
                result[key] = value
        return result
    
    # =========================================================================
    # 报告生成方法
    # =========================================================================
    
    def generate_report(
        self,
        nav_series: pd.Series,
        returns: Optional[pd.Series] = None,
        benchmark: Optional[pd.Series] = None,
        format: str = 'dict',
        include_charts: bool = False,
        **kwargs
    ) -> Union[Dict[str, Any], str, bytes]:
        """
        生成分析报告
        
        参数:
            nav_series: 净值序列
            returns: 收益率序列（可选，若不提供则从净值计算）
            benchmark: 基准净值序列（可选）
            format: 输出格式，支持 'dict', 'excel', 'html'
            include_charts: 是否包含图表（仅 html 格式有效）
            **kwargs: 其他参数
                - path: Excel 输出路径（format='excel' 时必需）
                - title: HTML 报告标题（format='html' 时可选）
                - trades: 交易记录 DataFrame（可选）
                - holdings: 持仓历史 DataFrame（可选）
        
        返回:
            - format='dict': 返回可 JSON 序列化的字典
            - format='excel': 返回 None（文件保存到 path）
            - format='html': 返回 HTML 字符串
        
        示例:
            >>> # 生成字典格式报告
            >>> report = analyzer.generate_report(nav, format='dict')
            >>> 
            >>> # 生成 Excel 报告
            >>> analyzer.generate_report(nav, format='excel', path='report.xlsx')
            >>> 
            >>> # 生成 HTML 报告（含图表）
            >>> html = analyzer.generate_report(nav, format='html', include_charts=True)
        """
        if nav_series is None or nav_series.empty:
            if format == 'dict':
                return {'metrics': {}, 'drawdowns': {}, 'periodic': {}}
            elif format == 'html':
                return '<html><body><p>无数据</p></body></html>'
            else:
                return None
        
        nav_series = nav_series.dropna()
        
        # 计算收益率
        if returns is None:
            returns = nav_series.pct_change().dropna()
        else:
            returns = returns.dropna()
        
        # 计算各类指标
        metrics = self.calculate_metrics(returns, nav_series, benchmark)
        drawdowns = self.analyze_drawdowns(nav_series)
        periodic = self.calculate_periodic_returns(nav_series)
        
        # 准备报告数据
        report_data = {
            'metrics': metrics,
            'monthly_returns': periodic.get('monthly_returns', pd.DataFrame()),
            'drawdowns': drawdowns,
        }
        
        # 添加可选数据
        if 'trades' in kwargs:
            report_data['trades'] = kwargs['trades']
        if 'holdings' in kwargs:
            report_data['holdings'] = kwargs['holdings']
        
        # 根据格式生成报告
        if format == 'dict':
            return self._report.to_dict(report_data)
        
        elif format == 'excel':
            path = kwargs.get('path', 'report.xlsx')
            self._report.to_excel(report_data, path)
            return None
        
        elif format == 'html':
            title = kwargs.get('title', '基金绩效分析报告')
            charts = None
            
            if include_charts:
                charts = self._generate_charts(nav_series, benchmark, periodic)
            
            return self._report.to_html(report_data, charts=charts, title=title)
        
        else:
            raise ValueError(f"不支持的报告格式: {format}，支持 'dict', 'excel', 'html'")
    
    def _generate_charts(
        self,
        nav_series: pd.Series,
        benchmark: Optional[pd.Series] = None,
        periodic: Optional[Dict[str, Any]] = None
    ) -> Dict[str, bytes]:
        """生成图表字节流"""
        charts = {}
        
        try:
            # 净值曲线
            fig, ax = self.plot_nav(nav_series, benchmark)
            charts['净值曲线'] = self._get_viz().to_bytes(fig)
            self._get_viz().close(fig)
            
            # 回撤曲线
            fig, ax = self.plot_drawdown(nav_series)
            charts['回撤曲线'] = self._get_viz().to_bytes(fig)
            self._get_viz().close(fig)
            
            # 月度收益热力图
            if periodic and 'monthly_returns' in periodic:
                monthly = periodic['monthly_returns']
                if isinstance(monthly, pd.DataFrame) and not monthly.empty:
                    fig, ax = self.plot_monthly_heatmap(monthly)
                    charts['月度收益热力图'] = self._get_viz().to_bytes(fig)
                    self._get_viz().close(fig)
        
        except ImportError as e:
            logger.warning(f"生成图表失败（matplotlib 不可用）: {e}")
        except Exception as e:
            logger.warning(f"生成图表时发生错误: {e}")
        
        return charts
    
    def _get_viz(self):
        """获取 Visualization 实例（延迟初始化）"""
        if self._viz is None:
            Visualization = _get_visualization()
            self._viz = Visualization()
        return self._viz
    
    # =========================================================================
    # 可视化委托方法
    # =========================================================================
    
    def plot_nav(
        self,
        nav_series: pd.Series,
        benchmark: Optional[pd.Series] = None,
        ax: Optional[Any] = None,
        save_path: Optional[str] = None,
        **kwargs
    ) -> Tuple[Any, Any]:
        """
        绑制净值曲线图
        
        参数:
            nav_series: 组合净值序列
            benchmark: 基准净值序列（可选）
            ax: matplotlib Axes 对象（可选）
            save_path: 保存路径（可选）
            **kwargs: 其他参数传递给 Visualization.plot_nav
        
        返回:
            Tuple[Figure, Axes]: matplotlib 图表对象
        
        抛出:
            ImportError: 当 matplotlib 不可用时
        """
        return self._get_viz().plot_nav(nav_series, benchmark, ax, save_path, **kwargs)
    
    def plot_drawdown(
        self,
        nav_series: pd.Series,
        ax: Optional[Any] = None,
        save_path: Optional[str] = None,
        **kwargs
    ) -> Tuple[Any, Any]:
        """
        绘制回撤图（水下曲线）
        
        参数:
            nav_series: 净值序列
            ax: matplotlib Axes 对象（可选）
            save_path: 保存路径（可选）
            **kwargs: 其他参数传递给 Visualization.plot_drawdown
        
        返回:
            Tuple[Figure, Axes]: matplotlib 图表对象
        
        抛出:
            ImportError: 当 matplotlib 不可用时
        """
        return self._get_viz().plot_drawdown(nav_series, ax, save_path, **kwargs)
    
    def plot_monthly_heatmap(
        self,
        monthly_returns: pd.DataFrame,
        ax: Optional[Any] = None,
        save_path: Optional[str] = None,
        **kwargs
    ) -> Tuple[Any, Any]:
        """
        绘制月度收益热力图
        
        参数:
            monthly_returns: 月度收益矩阵，索引为年份，列为月份 (1-12)
            ax: matplotlib Axes 对象（可选）
            save_path: 保存路径（可选）
            **kwargs: 其他参数传递给 Visualization.plot_monthly_heatmap
        
        返回:
            Tuple[Figure, Axes]: matplotlib 图表对象
        
        抛出:
            ImportError: 当 matplotlib 不可用时
        """
        return self._get_viz().plot_monthly_heatmap(monthly_returns, ax, save_path, **kwargs)
    
    def plot_weights(
        self,
        weights_ts: pd.DataFrame,
        ax: Optional[Any] = None,
        save_path: Optional[str] = None,
        **kwargs
    ) -> Tuple[Any, Any]:
        """
        绘制持仓权重变化图
        
        参数:
            weights_ts: 权重时序 DataFrame，索引为日期，列为 fund_id
            ax: matplotlib Axes 对象（可选）
            save_path: 保存路径（可选）
            **kwargs: 其他参数传递给 Visualization.plot_weights
        
        返回:
            Tuple[Figure, Axes]: matplotlib 图表对象
        
        抛出:
            ImportError: 当 matplotlib 不可用时
        """
        return self._get_viz().plot_weights(weights_ts, ax, save_path, **kwargs)
