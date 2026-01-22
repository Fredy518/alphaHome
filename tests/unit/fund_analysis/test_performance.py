"""
PerformanceAnalyzer 单元测试

测试统一入口的功能正确性。
"""

import json
import numpy as np
import pandas as pd
import pytest

from fund_analysis import PerformanceAnalyzer
from fund_analysis._schema import METRICS_SCHEMA_KEYS


class TestPerformanceAnalyzer:
    """PerformanceAnalyzer 测试类"""
    
    @pytest.fixture
    def analyzer(self):
        """创建分析器实例"""
        return PerformanceAnalyzer()
    
    @pytest.fixture
    def sample_nav(self):
        """创建示例净值序列"""
        return pd.Series(
            [1.0, 1.01, 0.99, 1.02, 1.03],
            index=pd.date_range('2024-01-01', periods=5)
        )
    
    @pytest.fixture
    def sample_returns(self, sample_nav):
        """创建示例收益率序列"""
        return sample_nav.pct_change().dropna()
    
    def test_calculate_metrics_returns_dict(self, analyzer, sample_returns, sample_nav):
        """测试 calculate_metrics 返回字典"""
        metrics = analyzer.calculate_metrics(sample_returns, sample_nav)
        assert isinstance(metrics, dict)
    
    def test_calculate_metrics_schema_keys(self, analyzer, sample_returns, sample_nav):
        """测试 calculate_metrics 返回固定 schema 的 keys"""
        metrics = analyzer.calculate_metrics(sample_returns, sample_nav)
        # 检查所有 schema keys 都存在
        for key in METRICS_SCHEMA_KEYS:
            assert key in metrics, f"Missing key: {key}"
    
    def test_calculate_metrics_empty_data(self, analyzer):
        """测试空数据返回全 NaN"""
        empty_returns = pd.Series(dtype=float)
        empty_nav = pd.Series(dtype=float)
        metrics = analyzer.calculate_metrics(empty_returns, empty_nav)
        assert metrics['total_days'] == 0
    
    def test_analyze_drawdowns_returns_dict(self, analyzer, sample_nav):
        """测试 analyze_drawdowns 返回字典"""
        drawdowns = analyzer.analyze_drawdowns(sample_nav)
        assert isinstance(drawdowns, dict)
        assert 'max_drawdown' in drawdowns
        assert 'top_n_drawdowns' in drawdowns
        assert 'underwater_curve' in drawdowns
    
    def test_calculate_periodic_returns_returns_dict(self, analyzer, sample_nav):
        """测试 calculate_periodic_returns 返回字典"""
        periodic = analyzer.calculate_periodic_returns(sample_nav)
        assert isinstance(periodic, dict)
        assert 'monthly_returns' in periodic
        assert 'quarterly_returns' in periodic
        assert 'yearly_returns' in periodic
    
    def test_calculate_rolling_metrics_returns_dict(self, analyzer, sample_returns):
        """测试 calculate_rolling_metrics 返回字典"""
        rolling = analyzer.calculate_rolling_metrics(sample_returns)
        assert isinstance(rolling, dict)
        assert 'rolling_return' in rolling
        assert 'rolling_sharpe' in rolling
        assert 'rolling_volatility' in rolling
    
    def test_to_dict_json_serializable(self, analyzer, sample_nav):
        """测试 to_dict 输出可 JSON 序列化"""
        result = analyzer.to_dict(sample_nav)
        # 尝试 JSON 序列化
        try:
            json_str = json.dumps(result)
            assert isinstance(json_str, str)
        except (TypeError, ValueError) as e:
            pytest.fail(f"to_dict 输出不可 JSON 序列化: {e}")
    
    def test_to_dict_structure(self, analyzer, sample_nav):
        """测试 to_dict 输出结构"""
        result = analyzer.to_dict(sample_nav)
        assert 'metrics' in result
        assert 'drawdowns' in result
        assert 'periodic' in result
    
    def test_integration_with_longer_data(self, analyzer):
        """测试较长数据的集成"""
        # 创建一年的日频数据
        dates = pd.date_range('2023-01-01', '2023-12-31', freq='B')
        np.random.seed(42)
        returns = np.random.normal(0.0005, 0.01, len(dates))
        nav = (1 + pd.Series(returns)).cumprod()
        nav.index = dates
        returns_series = nav.pct_change().dropna()
        
        # 计算指标
        metrics = analyzer.calculate_metrics(returns_series, nav)
        
        # 验证基本指标
        assert not np.isnan(metrics['cumulative_return'])
        assert not np.isnan(metrics['annualized_return'])
        assert not np.isnan(metrics['annualized_volatility'])
        assert not np.isnan(metrics['sharpe_ratio'])
        assert not np.isnan(metrics['max_drawdown'])
        assert metrics['total_days'] > 0
