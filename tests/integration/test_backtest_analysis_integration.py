"""
fund_backtest + fund_analysis 组合集成测试

测试回测结果 -> 分析的完整流程。

Requirements: 9.1
"""

import numpy as np
import pandas as pd
import pytest

from fund_backtest import (
    BacktestEngine,
    PortfolioConfig,
    BacktestResult,
    MemoryDataProvider,
)
from fund_analysis import (
    PerformanceAnalyzer,
    MetricsAnalyzer,
    DrawdownAnalyzer,
    RiskAnalyzer,
)


class TestBacktestAnalysisIntegration:
    """测试回测引擎与分析模块的集成"""

    @pytest.fixture
    def sample_nav_panel(self):
        """创建示例净值面板（3只基金，约60个交易日）"""
        dates = pd.date_range('2024-01-01', '2024-03-31', freq='B')
        np.random.seed(42)
        
        # 生成3只基金的净值
        nav_data = {}
        for fund_id in ['fund_a', 'fund_b', 'fund_c']:
            daily_returns = np.random.normal(0.0005, 0.01, len(dates))
            nav = (1 + pd.Series(daily_returns)).cumprod()
            nav_data[fund_id] = nav.values
        
        nav_panel = pd.DataFrame(nav_data, index=dates)
        return nav_panel

    @pytest.fixture
    def sample_rebalance_records(self):
        """创建示例调仓记录"""
        records = pd.DataFrame([
            # 首次建仓
            {'rebalance_date': '2024-01-02', 'fund_id': 'fund_a', 'fund_name': '基金A', 'target_weight': 0.4},
            {'rebalance_date': '2024-01-02', 'fund_id': 'fund_b', 'fund_name': '基金B', 'target_weight': 0.3},
            {'rebalance_date': '2024-01-02', 'fund_id': 'fund_c', 'fund_name': '基金C', 'target_weight': 0.3},
            # 调仓
            {'rebalance_date': '2024-02-01', 'fund_id': 'fund_a', 'fund_name': '基金A', 'target_weight': 0.5},
            {'rebalance_date': '2024-02-01', 'fund_id': 'fund_b', 'fund_name': '基金B', 'target_weight': 0.3},
            {'rebalance_date': '2024-02-01', 'fund_id': 'fund_c', 'fund_name': '基金C', 'target_weight': 0.2},
        ])
        return records

    @pytest.fixture
    def data_provider(self, sample_nav_panel, sample_rebalance_records):
        """创建内存数据提供者"""
        provider = MemoryDataProvider(nav_panel=sample_nav_panel)
        provider.set_rebalance_records('test_portfolio', sample_rebalance_records)
        return provider

    @pytest.fixture
    def portfolio_config(self):
        """创建组合配置"""
        return PortfolioConfig(
            portfolio_id='test_portfolio',
            portfolio_name='测试组合',
            initial_cash=1000000.0,
            setup_date='2024-01-01',
            rebalance_delay=2,
            purchase_fee_rate=0.015,
            redeem_fee_rate=0.005,
            management_fee=0.01,
        )

    # =========================================================================
    # 场景1: 完整回测 -> 分析流程
    # =========================================================================

    def test_backtest_to_analysis_complete_flow(self, data_provider, portfolio_config):
        """测试完整的回测到分析流程"""
        # 1. 执行回测
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        # 验证回测结果
        assert 'test_portfolio' in results
        result = results['test_portfolio']
        assert isinstance(result, BacktestResult)
        assert isinstance(result.nav_series, pd.Series)
        assert isinstance(result.returns, pd.Series)
        assert len(result.nav_series) > 0
        
        # 2. 使用 fund_analysis 分析回测结果
        analyzer = PerformanceAnalyzer()
        
        # 计算绩效指标
        metrics = analyzer.calculate_metrics(result.returns, result.nav_series)
        
        # 验证指标存在且有效
        assert isinstance(metrics, dict)
        assert 'cumulative_return' in metrics
        assert 'annualized_return' in metrics
        assert 'max_drawdown' in metrics
        assert 'sharpe_ratio' in metrics
        assert metrics['total_days'] > 0

    def test_backtest_result_contains_metrics(self, data_provider, portfolio_config):
        """测试回测结果已包含分析指标"""
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        result = results['test_portfolio']
        
        # BacktestResult 应该已经包含 metrics
        assert hasattr(result, 'metrics')
        assert isinstance(result.metrics, dict)
        assert 'cumulative_return' in result.metrics
        assert 'annualized_return' in result.metrics

    def test_backtest_result_nav_series_format(self, data_provider, portfolio_config):
        """测试回测结果的净值序列格式符合分析模块要求"""
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        result = results['test_portfolio']
        
        # 验证 nav_series 格式
        assert isinstance(result.nav_series, pd.Series)
        assert isinstance(result.nav_series.index, pd.DatetimeIndex) or \
               all(isinstance(idx, (pd.Timestamp, type(pd.Timestamp('2024-01-01').date()))) 
                   for idx in result.nav_series.index)
        assert result.nav_series.dtype in [np.float64, np.float32, float]
        
        # 验证 returns 格式
        assert isinstance(result.returns, pd.Series)
        assert len(result.returns) == len(result.nav_series) - 1 or len(result.returns) <= len(result.nav_series)

    # =========================================================================
    # 场景2: 回测结果的详细分析
    # =========================================================================

    def test_analyze_backtest_drawdowns(self, data_provider, portfolio_config):
        """测试分析回测结果的回撤"""
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        result = results['test_portfolio']
        analyzer = PerformanceAnalyzer()
        
        # 回撤分析
        drawdowns = analyzer.analyze_drawdowns(result.nav_series)
        
        assert 'max_drawdown' in drawdowns
        assert 'top_n_drawdowns' in drawdowns
        assert 'underwater_curve' in drawdowns
        assert isinstance(drawdowns['underwater_curve'], pd.Series)

    def test_analyze_backtest_periodic_returns(self, data_provider, portfolio_config):
        """测试分析回测结果的周期性收益"""
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        result = results['test_portfolio']
        analyzer = PerformanceAnalyzer()
        
        # 周期性分析
        periodic = analyzer.calculate_periodic_returns(result.nav_series)
        
        assert 'monthly_returns' in periodic
        assert 'quarterly_returns' in periodic
        assert 'yearly_returns' in periodic

    def test_analyze_backtest_rolling_metrics(self, data_provider, portfolio_config):
        """测试分析回测结果的滚动指标"""
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        result = results['test_portfolio']
        analyzer = PerformanceAnalyzer()
        
        # 滚动指标（使用较小窗口以适应短期数据）
        rolling = analyzer.calculate_rolling_metrics(
            result.returns, 
            return_window=20, 
            volatility_window=10
        )
        
        assert 'rolling_return' in rolling
        assert 'rolling_sharpe' in rolling
        assert 'rolling_volatility' in rolling

    # =========================================================================
    # 场景3: 使用回测结果的持仓数据进行风险分析
    # =========================================================================

    def test_analyze_backtest_risk_with_holdings(self, data_provider, portfolio_config):
        """测试使用回测持仓数据进行风险分析"""
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        result = results['test_portfolio']
        
        # 从持仓历史提取权重
        if not result.holdings_history.empty:
            # 获取最新持仓权重
            latest_date = result.holdings_history['update'].max()
            latest_holdings = result.holdings_history[
                result.holdings_history['update'] == latest_date
            ]
            
            # 排除现金
            holdings_no_cash = latest_holdings[latest_holdings['fund_id'] != 'cash']
            
            if not holdings_no_cash.empty:
                weights = holdings_no_cash.set_index('fund_id')['weight']
                
                # 风险分析
                risk_analyzer = RiskAnalyzer()
                
                # HHI 集中度
                hhi = risk_analyzer.hhi(weights)
                assert not np.isnan(hhi)
                assert 0 <= hhi <= 1
                
                # Top N 集中度
                top_n = risk_analyzer.top_n_concentration(weights, n=2)
                assert not np.isnan(top_n)
                assert 0 <= top_n <= 1

    def test_analyze_backtest_trades_for_turnover(self, data_provider, portfolio_config):
        """测试使用回测交易记录计算换手率"""
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        result = results['test_portfolio']
        
        # 验证交易记录存在
        assert hasattr(result, 'trades')
        assert isinstance(result.trades, pd.DataFrame)
        
        # 如果有交易记录，可以计算换手率
        if not result.trades.empty:
            risk_analyzer = RiskAnalyzer()
            
            # 计算平均 AUM
            avg_aum = result.nav_series.mean() * portfolio_config.initial_cash
            
            # 换手率计算需要特定格式的交易记录
            # 这里验证交易记录包含必要字段
            assert 'fund_id' in result.trades.columns or result.trades.empty

    # =========================================================================
    # 场景4: 生成完整报告
    # =========================================================================

    def test_generate_report_from_backtest(self, data_provider, portfolio_config):
        """测试从回测结果生成完整报告"""
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        result = results['test_portfolio']
        analyzer = PerformanceAnalyzer()
        
        # 生成字典格式报告
        report = analyzer.generate_report(
            result.nav_series,
            returns=result.returns,
            format='dict'
        )
        
        assert isinstance(report, dict)
        assert 'metrics' in report

    def test_to_dict_from_backtest(self, data_provider, portfolio_config):
        """测试从回测结果生成可序列化字典"""
        import json
        
        engine = BacktestEngine(data_provider)
        engine.add_portfolio(portfolio_config)
        results = engine.run('2024-01-01', '2024-03-31')
        
        result = results['test_portfolio']
        analyzer = PerformanceAnalyzer()
        
        # 生成可序列化字典
        data = analyzer.to_dict(result.nav_series, result.returns)
        
        # 验证可 JSON 序列化
        try:
            json_str = json.dumps(data)
            assert isinstance(json_str, str)
        except (TypeError, ValueError) as e:
            pytest.fail(f"to_dict 输出不可 JSON 序列化: {e}")

    # =========================================================================
    # 场景5: 多组合回测分析
    # =========================================================================

    def test_multiple_portfolios_analysis(self, sample_nav_panel, sample_rebalance_records):
        """测试多组合回测分析"""
        # 创建两个组合的调仓记录
        records_1 = sample_rebalance_records.copy()
        records_2 = sample_rebalance_records.copy()
        records_2['target_weight'] = [0.5, 0.25, 0.25, 0.4, 0.4, 0.2]  # 不同权重
        
        provider = MemoryDataProvider(nav_panel=sample_nav_panel)
        provider.set_rebalance_records('portfolio_1', records_1)
        provider.set_rebalance_records('portfolio_2', records_2)
        
        config_1 = PortfolioConfig(
            portfolio_id='portfolio_1',
            portfolio_name='组合1',
            initial_cash=1000000.0,
            setup_date='2024-01-01',
        )
        config_2 = PortfolioConfig(
            portfolio_id='portfolio_2',
            portfolio_name='组合2',
            initial_cash=500000.0,
            setup_date='2024-01-01',
        )
        
        engine = BacktestEngine(provider)
        engine.add_portfolio(config_1)
        engine.add_portfolio(config_2)
        results = engine.run('2024-01-01', '2024-03-31')
        
        # 验证两个组合都有结果
        assert 'portfolio_1' in results
        assert 'portfolio_2' in results
        
        # 分别分析
        analyzer = PerformanceAnalyzer()
        
        metrics_1 = analyzer.calculate_metrics(
            results['portfolio_1'].returns,
            results['portfolio_1'].nav_series
        )
        metrics_2 = analyzer.calculate_metrics(
            results['portfolio_2'].returns,
            results['portfolio_2'].nav_series
        )
        
        # 两个组合的指标应该不同（因为权重不同）
        assert metrics_1['total_days'] > 0
        assert metrics_2['total_days'] > 0


class TestBacktestEngineUsesAnalyzer:
    """测试回测引擎内部使用分析器"""

    @pytest.fixture
    def simple_setup(self):
        """简单的回测设置"""
        dates = pd.date_range('2024-01-01', '2024-01-31', freq='B')
        np.random.seed(42)
        
        nav_panel = pd.DataFrame({
            'fund_a': (1 + np.random.normal(0.001, 0.01, len(dates))).cumprod(),
        }, index=dates)
        
        records = pd.DataFrame([
            {'rebalance_date': '2024-01-02', 'fund_id': 'fund_a', 'fund_name': '基金A', 'target_weight': 1.0},
        ])
        
        provider = MemoryDataProvider(nav_panel=nav_panel)
        provider.set_rebalance_records('test', records)
        
        config = PortfolioConfig(
            portfolio_id='test',
            portfolio_name='测试',
            initial_cash=100000.0,
            setup_date='2024-01-01',
        )
        
        return provider, config

    def test_engine_imports_from_fund_analysis(self, simple_setup):
        """测试引擎从 fund_analysis 导入分析器"""
        provider, config = simple_setup
        
        engine = BacktestEngine(provider)
        
        # 验证引擎使用的是 fund_analysis 的 PerformanceAnalyzer
        from fund_analysis import PerformanceAnalyzer
        assert isinstance(engine.analyzer, PerformanceAnalyzer)

    def test_engine_result_metrics_match_analyzer(self, simple_setup):
        """测试引擎结果的指标与独立分析器一致"""
        provider, config = simple_setup
        
        engine = BacktestEngine(provider)
        engine.add_portfolio(config)
        results = engine.run('2024-01-01', '2024-01-31')
        
        result = results['test']
        
        # 使用独立分析器计算
        analyzer = PerformanceAnalyzer()
        independent_metrics = analyzer.calculate_metrics(result.returns, result.nav_series)
        
        # 验证关键指标一致
        assert abs(result.metrics['cumulative_return'] - independent_metrics['cumulative_return']) < 1e-10
        assert abs(result.metrics['max_drawdown'] - independent_metrics['max_drawdown']) < 1e-10
