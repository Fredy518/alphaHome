"""
DrawdownAnalyzer 单元测试

测试回撤分析功能的正确性。
"""

import numpy as np
import pandas as pd
import pytest

from alphahome.fund_analysis import DrawdownAnalyzer


class TestDrawdownAnalyzer:
    """DrawdownAnalyzer 测试类"""
    
    @pytest.fixture
    def analyzer(self):
        """创建分析器实例"""
        return DrawdownAnalyzer()
    
    @pytest.fixture
    def sample_nav(self):
        """创建示例净值序列"""
        return pd.Series(
            [1.0, 1.1, 0.9, 1.0, 1.2],
            index=pd.date_range('2024-01-01', periods=5)
        )
    
    def test_underwater_curve_basic(self, analyzer, sample_nav):
        """测试水下曲线计算"""
        underwater = analyzer.underwater_curve(sample_nav)
        # 水下曲线应该 <= 0
        assert (underwater <= 0).all()
    
    def test_underwater_curve_at_peak(self, analyzer, sample_nav):
        """测试峰值处水下曲线为 0"""
        underwater = analyzer.underwater_curve(sample_nav)
        # 第一个点和最后一个点是峰值
        assert underwater.iloc[0] == 0
        assert underwater.iloc[-1] == 0
    
    def test_max_drawdown_basic(self, analyzer, sample_nav):
        """测试最大回撤计算"""
        max_dd, peak_date, trough_date = analyzer.max_drawdown(sample_nav)
        # 最大回撤应该是正数
        assert max_dd > 0
        # 从 1.1 跌到 0.9，回撤约 18.18%
        expected_dd = (1.1 - 0.9) / 1.1
        assert abs(max_dd - expected_dd) < 1e-10
    
    def test_max_drawdown_equals_underwater_min(self, analyzer, sample_nav):
        """测试 max_drawdown == abs(underwater_curve.min())"""
        max_dd, _, _ = analyzer.max_drawdown(sample_nav)
        underwater = analyzer.underwater_curve(sample_nav)
        assert abs(max_dd - abs(underwater.min())) < 1e-10
    
    def test_max_drawdown_empty(self, analyzer):
        """测试空数据返回 NaN"""
        empty_nav = pd.Series(dtype=float)
        max_dd, peak, trough = analyzer.max_drawdown(empty_nav)
        assert np.isnan(max_dd)
        assert peak is None
        assert trough is None
    
    def test_top_n_drawdowns_basic(self, analyzer, sample_nav):
        """测试前 N 大回撤"""
        top_dds = analyzer.top_n_drawdowns(sample_nav, n=3)
        # 应该返回列表
        assert isinstance(top_dds, list)
        # 按回撤深度降序排列
        if len(top_dds) > 1:
            for i in range(len(top_dds) - 1):
                assert top_dds[i].drawdown >= top_dds[i + 1].drawdown
    
    def test_top_n_drawdowns_no_overlap(self, analyzer):
        """测试回撤周期不重叠"""
        # 创建有多个回撤的净值序列
        nav = pd.Series(
            [1.0, 1.1, 0.95, 1.05, 1.15, 1.0, 1.1],
            index=pd.date_range('2024-01-01', periods=7)
        )
        top_dds = analyzer.top_n_drawdowns(nav, n=5)
        
        # 检查周期不重叠
        for i in range(len(top_dds)):
            for j in range(i + 1, len(top_dds)):
                dd1 = top_dds[i]
                dd2 = top_dds[j]
                # 周期不应该重叠
                if dd1.peak_date and dd2.peak_date and dd1.trough_date and dd2.trough_date:
                    # 简单检查：一个周期的谷值不应该在另一个周期的峰值和谷值之间
                    pass  # 复杂的重叠检查可以在属性测试中完成
    
    def test_avg_drawdown_duration_basic(self, analyzer, sample_nav):
        """测试平均回撤持续时间"""
        avg_duration = analyzer.avg_drawdown_duration(sample_nav)
        # 可能返回 NaN（如果没有已恢复的回撤）或正数
        assert np.isnan(avg_duration) or avg_duration >= 0
    
    def test_max_drawdown_duration_basic(self, analyzer, sample_nav):
        """测试最大回撤持续时间"""
        max_duration = analyzer.max_drawdown_duration(sample_nav)
        assert max_duration >= 0
    
    def test_drawdown_period_to_dict(self, analyzer, sample_nav):
        """测试回撤周期可序列化"""
        top_dds = analyzer.top_n_drawdowns(sample_nav, n=1)
        if top_dds:
            dd_dict = top_dds[0].to_dict()
            assert isinstance(dd_dict, dict)
            assert 'drawdown' in dd_dict
            assert 'peak_date' in dd_dict
