"""
MetricsAnalyzer 单元测试

测试基础绩效指标计算的正确性。
"""

import numpy as np
import pandas as pd
import pytest

from fund_analysis import MetricsAnalyzer


class TestMetricsAnalyzer:
    """MetricsAnalyzer 测试类"""
    
    @pytest.fixture
    def analyzer(self):
        """创建默认分析器实例"""
        return MetricsAnalyzer()
    
    @pytest.fixture
    def sample_returns(self):
        """创建示例收益率序列"""
        return pd.Series(
            [0.01, -0.02, 0.03, 0.01, -0.01],
            index=pd.date_range('2024-01-01', periods=5)
        )
    
    def test_cumulative_return_basic(self, analyzer, sample_returns):
        """测试累计收益率计算"""
        cum_ret = analyzer.cumulative_return(sample_returns)
        # (1.01) * (0.98) * (1.03) * (1.01) * (0.99) - 1
        expected = (1.01 * 0.98 * 1.03 * 1.01 * 0.99) - 1
        assert abs(cum_ret - expected) < 1e-10
    
    def test_cumulative_return_empty(self, analyzer):
        """测试空数据返回 NaN"""
        empty_returns = pd.Series(dtype=float)
        assert np.isnan(analyzer.cumulative_return(empty_returns))
    
    def test_annualized_return_basic(self, analyzer, sample_returns):
        """测试年化收益率计算"""
        ann_ret = analyzer.annualized_return(sample_returns)
        # 应该是正数（因为累计收益为正）
        assert ann_ret > 0
        assert not np.isnan(ann_ret)
    
    def test_annualized_volatility_basic(self, analyzer, sample_returns):
        """测试年化波动率计算"""
        ann_vol = analyzer.annualized_volatility(sample_returns)
        assert ann_vol > 0
        assert not np.isnan(ann_vol)
    
    def test_annualized_volatility_zero_std(self, analyzer):
        """测试标准差为零时返回 NaN"""
        constant_returns = pd.Series([0.01, 0.01, 0.01, 0.01])
        assert np.isnan(analyzer.annualized_volatility(constant_returns))
    
    def test_sharpe_ratio_basic(self, analyzer, sample_returns):
        """测试夏普比率计算"""
        sharpe = analyzer.sharpe_ratio(sample_returns)
        assert not np.isnan(sharpe)
    
    def test_sortino_ratio_basic(self, analyzer, sample_returns):
        """测试索提诺比率计算"""
        sortino = analyzer.sortino_ratio(sample_returns)
        assert not np.isnan(sortino)
    
    def test_calmar_ratio_basic(self, analyzer):
        """测试卡玛比率计算"""
        calmar = analyzer.calmar_ratio(ann_return=0.1, max_dd=0.05)
        assert calmar == 2.0
    
    def test_calmar_ratio_zero_dd(self, analyzer):
        """测试最大回撤为零时返回 NaN"""
        assert np.isnan(analyzer.calmar_ratio(ann_return=0.1, max_dd=0.0))
    
    def test_win_rate_basic(self, analyzer, sample_returns):
        """测试胜率计算"""
        win_rate = analyzer.win_rate(sample_returns)
        # 5个收益中有3个正收益
        assert win_rate == 0.6
    
    def test_win_rate_range(self, analyzer, sample_returns):
        """测试胜率范围在 [0, 1]"""
        win_rate = analyzer.win_rate(sample_returns)
        assert 0 <= win_rate <= 1
    
    def test_profit_loss_ratio_basic(self, analyzer, sample_returns):
        """测试盈亏比计算"""
        pl_ratio = analyzer.profit_loss_ratio(sample_returns)
        assert pl_ratio > 0
        assert not np.isnan(pl_ratio)
    
    def test_profit_loss_ratio_no_loss(self, analyzer):
        """测试无亏损时返回 inf"""
        all_wins = pd.Series([0.01, 0.02, 0.03])
        assert analyzer.profit_loss_ratio(all_wins) == float('inf')
    
    def test_var_basic(self, analyzer, sample_returns):
        """测试 VaR 计算"""
        var = analyzer.var(sample_returns, confidence=0.95)
        # VaR 应该是负数（表示损失阈值）
        assert var < 0 or np.isclose(var, sample_returns.min())
    
    def test_cvar_basic(self, analyzer, sample_returns):
        """测试 CVaR 计算"""
        var = analyzer.var(sample_returns, confidence=0.95)
        cvar = analyzer.cvar(sample_returns, confidence=0.95)
        # CVaR <= VaR
        assert cvar <= var or np.isclose(cvar, var)
    
    def test_edge_case_empty_data(self, analyzer):
        """测试边缘情况：空数据"""
        empty = pd.Series(dtype=float)
        assert np.isnan(analyzer.cumulative_return(empty))
        assert np.isnan(analyzer.annualized_return(empty))
        assert np.isnan(analyzer.annualized_volatility(empty))
        assert np.isnan(analyzer.sharpe_ratio(empty))
        assert np.isnan(analyzer.win_rate(empty))
    
    def test_edge_case_single_value(self, analyzer):
        """测试边缘情况：单个值"""
        single = pd.Series([0.01])
        # 累计收益应该可以计算
        assert not np.isnan(analyzer.cumulative_return(single))
        # 波动率需要至少2个样本
        assert np.isnan(analyzer.annualized_volatility(single))
