"""
PeriodicAnalyzer 单元测试

测试周期性分析功能的正确性。
"""

import numpy as np
import pandas as pd
import pytest

from alphahome.fund_analysis import PeriodicAnalyzer


class TestPeriodicAnalyzer:
    """PeriodicAnalyzer 测试类"""
    
    @pytest.fixture
    def analyzer(self):
        """创建分析器实例"""
        return PeriodicAnalyzer()
    
    @pytest.fixture
    def sample_nav(self):
        """创建示例净值序列（跨越多个月）"""
        dates = pd.date_range('2023-01-01', '2024-06-30', freq='B')
        np.random.seed(42)
        returns = np.random.normal(0.0005, 0.01, len(dates))
        nav = (1 + pd.Series(returns)).cumprod()
        nav.index = dates
        return nav
    
    @pytest.fixture
    def sample_returns(self):
        """创建示例收益率序列"""
        dates = pd.date_range('2023-01-01', '2024-06-30', freq='B')
        np.random.seed(42)
        returns = pd.Series(np.random.normal(0.0005, 0.01, len(dates)), index=dates)
        return returns
    
    def test_monthly_returns_basic(self, analyzer, sample_nav):
        """测试月度收益矩阵"""
        monthly = analyzer.monthly_returns(sample_nav)
        # 应该返回 DataFrame
        assert isinstance(monthly, pd.DataFrame)
        # 列应该是 1-12
        if not monthly.empty:
            assert all(col in range(1, 13) for col in monthly.columns)
    
    def test_monthly_returns_empty(self, analyzer):
        """测试空数据返回空 DataFrame"""
        empty_nav = pd.Series(dtype=float)
        monthly = analyzer.monthly_returns(empty_nav)
        assert monthly.empty
    
    def test_quarterly_returns_basic(self, analyzer, sample_nav):
        """测试季度收益"""
        quarterly = analyzer.quarterly_returns(sample_nav)
        # 应该返回 Series
        assert isinstance(quarterly, pd.Series)
        # 索引格式应该是 'YYYY-QN'
        if not quarterly.empty:
            assert all('-Q' in str(idx) for idx in quarterly.index)
    
    def test_yearly_returns_basic(self, analyzer, sample_nav):
        """测试年度收益"""
        yearly = analyzer.yearly_returns(sample_nav)
        # 应该返回 Series
        assert isinstance(yearly, pd.Series)
        # 索引应该是年份
        if not yearly.empty:
            assert all(isinstance(idx, (int, np.integer)) for idx in yearly.index)
    
    def test_rolling_return_basic(self, analyzer, sample_returns):
        """测试滚动收益"""
        window = 60
        rolling_ret = analyzer.rolling_return(sample_returns, window=window)
        # 应该返回 Series
        assert isinstance(rolling_ret, pd.Series)
        # 前 window-1 个值应该是 NaN
        assert rolling_ret.iloc[:window-1].isna().all()
    
    def test_rolling_sharpe_basic(self, analyzer, sample_returns):
        """测试滚动夏普比率"""
        window = 60
        rolling_sharpe = analyzer.rolling_sharpe(sample_returns, window=window)
        # 应该返回 Series
        assert isinstance(rolling_sharpe, pd.Series)
        # 前 window-1 个值应该是 NaN
        assert rolling_sharpe.iloc[:window-1].isna().all()
    
    def test_rolling_volatility_basic(self, analyzer, sample_returns):
        """测试滚动波动率"""
        window = 30
        rolling_vol = analyzer.rolling_volatility(sample_returns, window=window)
        # 应该返回 Series
        assert isinstance(rolling_vol, pd.Series)
        # 前 window-1 个值应该是 NaN
        assert rolling_vol.iloc[:window-1].isna().all()
        # 波动率应该是非负的
        valid_vol = rolling_vol.dropna()
        if not valid_vol.empty:
            assert (valid_vol >= 0).all()
    
    def test_rolling_window_nan_count(self, analyzer, sample_returns):
        """测试滚动计算前 w-1 个值为 NaN"""
        window = 50
        rolling_ret = analyzer.rolling_return(sample_returns, window=window)
        # 精确检查前 window-1 个值
        nan_count = rolling_ret.iloc[:window-1].isna().sum()
        assert nan_count == window - 1
