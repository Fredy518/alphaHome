"""
fund_analysis 独立模块集成测试

测试 fund_analysis 模块仅使用 nav_series/returns 的场景，
验证模块可以独立于回测引擎使用。

Requirements: 9.1
"""

import json
import numpy as np
import pandas as pd
import pytest

from alphahome.fund_analysis import (
    PerformanceAnalyzer,
    MetricsAnalyzer,
    DrawdownAnalyzer,
    PeriodicAnalyzer,
    RiskAnalyzer,
    AttributionAnalyzer,
    ReportBuilder,
    DEFAULT_PERIODS_PER_YEAR,
    DEFAULT_RISK_FREE_RATE,
)


class TestFundAnalysisStandalone:
    """测试 fund_analysis 模块独立使用场景"""

    @pytest.fixture
    def simple_nav(self):
        """简单净值序列（5天）"""
        return pd.Series(
            [1.0, 1.01, 0.99, 1.02, 1.03],
            index=pd.date_range('2024-01-01', periods=5)
        )

    @pytest.fixture
    def yearly_nav(self):
        """一年期净值序列（约250个交易日）"""
        dates = pd.date_range('2023-01-01', '2023-12-31', freq='B')
        np.random.seed(42)
        daily_returns = np.random.normal(0.0005, 0.01, len(dates))
        nav = (1 + pd.Series(daily_returns)).cumprod()
        nav.index = dates
        return nav

    @pytest.fixture
    def benchmark_nav(self, yearly_nav):
        """基准净值序列"""
        np.random.seed(123)
        daily_returns = np.random.normal(0.0003, 0.008, len(yearly_nav))
        benchmark = (1 + pd.Series(daily_returns)).cumprod()
        benchmark.index = yearly_nav.index
        return benchmark

    # =========================================================================
    # 场景1: 分析单只基金（仅使用 nav_series）
    # =========================================================================

    def test_analyze_single_fund_with_nav_only(self, simple_nav):
        """测试仅使用净值序列分析单只基金"""
        analyzer = PerformanceAnalyzer()
        
        # 从净值计算收益率
        returns = simple_nav.pct_change().dropna()
        
        # 计算绩效指标
        metrics = analyzer.calculate_metrics(returns, simple_nav)
        
        # 验证基本指标存在且有效
        assert isinstance(metrics, dict)
        assert 'cumulative_return' in metrics
        assert 'annualized_return' in metrics
        assert 'max_drawdown' in metrics
        assert metrics['total_days'] == len(returns)

    def test_analyze_single_fund_complete_workflow(self, yearly_nav):
        """测试单只基金完整分析流程"""
        analyzer = PerformanceAnalyzer()
        returns = yearly_nav.pct_change().dropna()
        
        # 1. 计算绩效指标
        metrics = analyzer.calculate_metrics(returns, yearly_nav)
        assert not np.isnan(metrics['cumulative_return'])
        assert not np.isnan(metrics['annualized_return'])
        assert not np.isnan(metrics['sharpe_ratio'])
        
        # 2. 回撤分析
        drawdowns = analyzer.analyze_drawdowns(yearly_nav)
        assert 'max_drawdown' in drawdowns
        assert 'top_n_drawdowns' in drawdowns
        assert isinstance(drawdowns['underwater_curve'], pd.Series)
        
        # 3. 周期性分析
        periodic = analyzer.calculate_periodic_returns(yearly_nav)
        assert isinstance(periodic['monthly_returns'], pd.DataFrame)
        assert isinstance(periodic['quarterly_returns'], pd.Series)
        assert isinstance(periodic['yearly_returns'], pd.Series)
        
        # 4. 滚动指标
        rolling = analyzer.calculate_rolling_metrics(returns, return_window=60, volatility_window=20)
        assert isinstance(rolling['rolling_return'], pd.Series)
        assert isinstance(rolling['rolling_sharpe'], pd.Series)
        assert isinstance(rolling['rolling_volatility'], pd.Series)

    # =========================================================================
    # 场景2: 分析基金与基准对比
    # =========================================================================

    def test_analyze_fund_with_benchmark(self, yearly_nav, benchmark_nav):
        """测试基金与基准对比分析"""
        analyzer = PerformanceAnalyzer()
        returns = yearly_nav.pct_change().dropna()
        
        # 计算包含基准的指标
        metrics = analyzer.calculate_metrics(returns, yearly_nav, benchmark=benchmark_nav)
        
        # 验证基准相关指标
        assert 'information_ratio' in metrics
        assert 'tracking_error' in metrics
        assert 'beta' in metrics
        assert 'excess_return' in metrics
        
        # 有基准时这些指标应该有值
        assert not np.isnan(metrics['information_ratio'])
        assert not np.isnan(metrics['tracking_error'])
        assert not np.isnan(metrics['beta'])

    def test_analyze_fund_without_benchmark(self, yearly_nav):
        """测试无基准时相对指标为 NaN"""
        analyzer = PerformanceAnalyzer()
        returns = yearly_nav.pct_change().dropna()
        
        metrics = analyzer.calculate_metrics(returns, yearly_nav, benchmark=None)
        
        # 无基准时相对指标应为 NaN
        assert np.isnan(metrics['information_ratio'])
        assert np.isnan(metrics['tracking_error'])
        assert np.isnan(metrics['beta'])
        assert np.isnan(metrics['excess_return'])

    # =========================================================================
    # 场景3: 使用各独立分析器
    # =========================================================================

    def test_metrics_analyzer_standalone(self, yearly_nav):
        """测试 MetricsAnalyzer 独立使用"""
        analyzer = MetricsAnalyzer()
        returns = yearly_nav.pct_change().dropna()
        
        # 测试各指标计算
        cum_ret = analyzer.cumulative_return(returns)
        ann_ret = analyzer.annualized_return(returns)
        ann_vol = analyzer.annualized_volatility(returns)
        sharpe = analyzer.sharpe_ratio(returns)
        sortino = analyzer.sortino_ratio(returns)
        win_rate = analyzer.win_rate(returns)
        
        assert not np.isnan(cum_ret)
        assert not np.isnan(ann_ret)
        assert not np.isnan(ann_vol)
        assert not np.isnan(sharpe)
        assert not np.isnan(sortino)
        assert 0 <= win_rate <= 1

    def test_drawdown_analyzer_standalone(self, yearly_nav):
        """测试 DrawdownAnalyzer 独立使用"""
        analyzer = DrawdownAnalyzer()
        
        # 最大回撤
        max_dd, dd_start, dd_end = analyzer.max_drawdown(yearly_nav)
        assert max_dd >= 0  # 回撤为正数
        assert dd_start is not None
        assert dd_end is not None
        
        # 水下曲线
        underwater = analyzer.underwater_curve(yearly_nav)
        assert isinstance(underwater, pd.Series)
        assert underwater.max() <= 0  # 水下曲线非正
        
        # 前N大回撤
        top_drawdowns = analyzer.top_n_drawdowns(yearly_nav, n=3)
        assert isinstance(top_drawdowns, list)
        assert len(top_drawdowns) <= 3

    def test_periodic_analyzer_standalone(self, yearly_nav):
        """测试 PeriodicAnalyzer 独立使用"""
        analyzer = PeriodicAnalyzer()
        
        # 月度收益
        monthly = analyzer.monthly_returns(yearly_nav)
        assert isinstance(monthly, pd.DataFrame)
        
        # 季度收益
        quarterly = analyzer.quarterly_returns(yearly_nav)
        assert isinstance(quarterly, pd.Series)
        
        # 年度收益
        yearly = analyzer.yearly_returns(yearly_nav)
        assert isinstance(yearly, pd.Series)

    def test_risk_analyzer_standalone(self, yearly_nav, benchmark_nav):
        """测试 RiskAnalyzer 独立使用"""
        analyzer = RiskAnalyzer()
        returns = yearly_nav.pct_change().dropna()
        benchmark_returns = benchmark_nav.pct_change().dropna()
        
        # 跟踪误差
        te = analyzer.tracking_error(returns, benchmark_returns)
        assert not np.isnan(te)
        assert te >= 0
        
        # Beta
        beta = analyzer.beta(returns, benchmark_returns)
        assert not np.isnan(beta)
        
        # 集中度指标（需要权重）
        weights = pd.Series({'fund_a': 0.4, 'fund_b': 0.3, 'fund_c': 0.3})
        hhi = analyzer.hhi(weights)
        assert 1/3 - 0.01 <= hhi <= 1.01  # HHI 范围 [1/n, 1]

    def test_attribution_analyzer_standalone(self):
        """测试 AttributionAnalyzer 独立使用"""
        analyzer = AttributionAnalyzer()
        
        # 贡献分析
        weights = pd.Series({'fund_a': 0.4, 'fund_b': 0.3, 'fund_c': 0.3})
        returns = pd.Series({'fund_a': 0.05, 'fund_b': 0.02, 'fund_c': -0.01})
        
        contribution = analyzer.contribution(weights, returns)
        assert isinstance(contribution, pd.Series)
        
        # 贡献之和应等于组合收益
        portfolio_return = (weights * returns).sum()
        assert abs(contribution.sum() - portfolio_return) < 1e-10

    # =========================================================================
    # 场景4: 报告生成
    # =========================================================================

    def test_report_to_dict_json_serializable(self, yearly_nav):
        """测试报告输出可 JSON 序列化"""
        analyzer = PerformanceAnalyzer()
        
        result = analyzer.to_dict(yearly_nav)
        
        # 验证可 JSON 序列化
        try:
            json_str = json.dumps(result)
            assert isinstance(json_str, str)
        except (TypeError, ValueError) as e:
            pytest.fail(f"to_dict 输出不可 JSON 序列化: {e}")

    def test_generate_report_dict_format(self, yearly_nav):
        """测试生成字典格式报告"""
        analyzer = PerformanceAnalyzer()
        
        report = analyzer.generate_report(yearly_nav, format='dict')
        
        assert isinstance(report, dict)
        assert 'metrics' in report

    def test_report_builder_standalone(self, yearly_nav):
        """测试 ReportBuilder 独立使用"""
        analyzer = PerformanceAnalyzer()
        builder = ReportBuilder()
        
        returns = yearly_nav.pct_change().dropna()
        metrics = analyzer.calculate_metrics(returns, yearly_nav)
        
        # 汇总 DataFrame
        summary_df = builder.summary_dataframe(metrics)
        assert isinstance(summary_df, pd.DataFrame)
        
        # to_dict
        report_data = {'metrics': metrics}
        result = builder.to_dict(report_data)
        assert isinstance(result, dict)

    # =========================================================================
    # 场景5: 边缘情况处理
    # =========================================================================

    def test_empty_data_handling(self):
        """测试空数据处理"""
        analyzer = PerformanceAnalyzer()
        
        empty_nav = pd.Series(dtype=float)
        empty_returns = pd.Series(dtype=float)
        
        # 空数据应返回全 NaN 结果，不抛异常
        metrics = analyzer.calculate_metrics(empty_returns, empty_nav)
        assert metrics['total_days'] == 0
        
        drawdowns = analyzer.analyze_drawdowns(empty_nav)
        assert np.isnan(drawdowns['max_drawdown'])
        
        periodic = analyzer.calculate_periodic_returns(empty_nav)
        assert periodic['monthly_returns'].empty

    def test_single_data_point_handling(self):
        """测试单数据点处理"""
        analyzer = PerformanceAnalyzer()
        
        single_nav = pd.Series([1.0], index=pd.date_range('2024-01-01', periods=1))
        single_returns = single_nav.pct_change().dropna()
        
        # 单数据点应返回 NaN，不抛异常
        metrics = analyzer.calculate_metrics(single_returns, single_nav)
        assert isinstance(metrics, dict)

    def test_constant_nav_handling(self):
        """测试恒定净值处理（零波动率）"""
        analyzer = PerformanceAnalyzer()
        
        constant_nav = pd.Series(
            [1.0, 1.0, 1.0, 1.0, 1.0],
            index=pd.date_range('2024-01-01', periods=5)
        )
        returns = constant_nav.pct_change().dropna()
        
        metrics = analyzer.calculate_metrics(returns, constant_nav)
        
        # 零波动率时，波动率应为 0 或 NaN
        assert metrics['annualized_volatility'] == 0 or np.isnan(metrics['annualized_volatility'])
        # 夏普比率应为 NaN（除以零）
        assert np.isnan(metrics['sharpe_ratio'])

    # =========================================================================
    # 场景6: 配置参数
    # =========================================================================

    def test_custom_periods_per_year(self, yearly_nav):
        """测试自定义年化因子"""
        analyzer_250 = PerformanceAnalyzer(periods_per_year=250)
        analyzer_252 = PerformanceAnalyzer(periods_per_year=252)
        
        returns = yearly_nav.pct_change().dropna()
        
        metrics_250 = analyzer_250.calculate_metrics(returns, yearly_nav)
        metrics_252 = analyzer_252.calculate_metrics(returns, yearly_nav)
        
        # 不同年化因子应产生不同的年化指标
        # 但差异应该很小
        assert metrics_250['annualized_return'] != metrics_252['annualized_return']

    def test_custom_risk_free_rate(self, yearly_nav):
        """测试自定义无风险利率"""
        analyzer_0 = PerformanceAnalyzer(risk_free_rate=0.0)
        analyzer_3 = PerformanceAnalyzer(risk_free_rate=0.03)
        
        returns = yearly_nav.pct_change().dropna()
        
        metrics_0 = analyzer_0.calculate_metrics(returns, yearly_nav)
        metrics_3 = analyzer_3.calculate_metrics(returns, yearly_nav)
        
        # 不同无风险利率应产生不同的夏普比率
        assert metrics_0['sharpe_ratio'] != metrics_3['sharpe_ratio']


class TestFundAnalysisModuleImports:
    """测试 fund_analysis 模块导入"""

    def test_import_performance_analyzer(self):
        """测试导入 PerformanceAnalyzer"""
        from alphahome.fund_analysis import PerformanceAnalyzer
        assert PerformanceAnalyzer is not None

    def test_import_all_analyzers(self):
        """测试导入所有分析器"""
        from alphahome.fund_analysis import (
            PerformanceAnalyzer,
            MetricsAnalyzer,
            DrawdownAnalyzer,
            PeriodicAnalyzer,
            RiskAnalyzer,
            AttributionAnalyzer,
            ReportBuilder,
        )
        
        assert PerformanceAnalyzer is not None
        assert MetricsAnalyzer is not None
        assert DrawdownAnalyzer is not None
        assert PeriodicAnalyzer is not None
        assert RiskAnalyzer is not None
        assert AttributionAnalyzer is not None
        assert ReportBuilder is not None

    def test_import_constants(self):
        """测试导入常量"""
        from alphahome.fund_analysis import (
            DEFAULT_PERIODS_PER_YEAR,
            DEFAULT_RISK_FREE_RATE,
            FFILL_LIMIT,
        )
        
        assert DEFAULT_PERIODS_PER_YEAR == 250
        assert DEFAULT_RISK_FREE_RATE == 0.0
        assert FFILL_LIMIT == 5

    def test_import_schemas(self):
        """测试导入 schema 定义"""
        from alphahome.fund_analysis import (
            METRICS_SCHEMA,
            DRAWDOWN_SCHEMA,
            PERIODIC_SCHEMA,
        )
        
        assert isinstance(METRICS_SCHEMA, dict)
        assert isinstance(DRAWDOWN_SCHEMA, dict)
        assert isinstance(PERIODIC_SCHEMA, dict)
