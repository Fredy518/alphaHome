#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BT Extensions 完整功能演示

展示重新设计的 btextensions 模块的所有功能：
1. 批量数据加载和缓存
2. 并行回测执行  
3. 智能缓存管理
4. 性能监控
5. 增强分析器
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backtrader as bt
from datetime import date, datetime
from alphahome.bt_extensions import (
    BatchDataLoader, 
    ParallelBacktestRunner,
    CacheManager,
    PerformanceMonitor,
    EnhancedAnalyzer
)
from alphahome.common.db_manager import create_sync_manager
from alphahome.common.config_manager import ConfigManager


class DualMovingAverageStrategy(bt.Strategy):
    """双移动平均线策略"""
    
    params = (
        ('fast_period', 10),
        ('slow_period', 30),
    )
    
    def __init__(self):
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.params.fast_period)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.params.slow_period)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)
    
    def next(self):
        if not self.position:
            if self.crossover > 0:  # 金叉
                self.buy()
        else:
            if self.crossover < 0:  # 死叉
                self.sell()


def demo_batch_loading():
    """演示批量数据加载功能"""
    print("\n" + "="*60)
    print("1. 批量数据加载演示")
    print("="*60)
    
    # 初始化组件
    config_manager = ConfigManager()
    db_url = config_manager.get_database_url()
    db_manager = create_sync_manager(db_url)
    cache_manager = CacheManager(max_memory_mb=256, enable_disk_cache=True)
    batch_loader = BatchDataLoader(db_manager, cache_manager)
    
    # 测试股票列表
    test_stocks = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
    start_date = date(2023, 1, 1)
    end_date = date(2023, 12, 31)
    
    print(f"加载股票: {test_stocks}")
    print(f"时间范围: {start_date} 到 {end_date}")
    
    # 第一次加载（数据库查询）
    print("\n第一次加载（从数据库）:")
    start_time = datetime.now()
    stock_data = batch_loader.load_stocks_data(
        stock_codes=test_stocks,
        start_date=start_date,
        end_date=end_date,
        use_cache=True
    )
    load_time1 = (datetime.now() - start_time).total_seconds()
    
    print(f"加载完成: {len(stock_data)} 只股票")
    for code, df in stock_data.items():
        print(f"  {code}: {len(df)} 条记录")
    print(f"耗时: {load_time1:.2f}秒")
    
    # 第二次加载（缓存命中）
    print("\n第二次加载（从缓存）:")
    start_time = datetime.now()
    stock_data_cached = batch_loader.load_stocks_data(
        stock_codes=test_stocks,
        start_date=start_date,
        end_date=end_date,
        use_cache=True
    )
    load_time2 = (datetime.now() - start_time).total_seconds()
    
    print(f"加载完成: {len(stock_data_cached)} 只股票")
    print(f"耗时: {load_time2:.2f}秒")
    print(f"性能提升: {load_time1/load_time2:.1f}x")
    
    # 缓存统计
    cache_stats = batch_loader.get_cache_stats()
    print(f"\n缓存统计:")
    print(f"  命中率: {cache_stats.get('overall_hit_rate', 0):.1f}%")
    print(f"  内存使用: {cache_stats.get('memory_size_mb', 0):.1f}MB")
    print(f"  缓存项目: {cache_stats.get('memory_items', 0)} 个")
    
    return stock_data


def demo_parallel_execution():
    """演示并行回测执行"""
    print("\n" + "="*60)
    print("2. 并行回测执行演示")
    print("="*60)
    
    # 初始化并行执行器
    config_manager = ConfigManager()
    db_url = config_manager.get_database_url()
    
    parallel_runner = ParallelBacktestRunner(
        max_workers=2,  # 使用2个进程
        batch_size=2,   # 每批2只股票
        db_config={'url': db_url}
    )
    
    # 测试股票
    test_stocks = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
    
    print(f"并行回测股票: {test_stocks}")
    print(f"策略: 双移动平均线")
    print(f"并行度: 2个进程")
    
    # 执行并行回测
    results = parallel_runner.run_parallel_backtests(
        stock_codes=test_stocks,
        strategy_class=DualMovingAverageStrategy,
        strategy_params={'fast_period': 10, 'slow_period': 30},
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        initial_cash=100000.0,
        commission=0.001
    )
    
    # 显示结果摘要
    summary = results['summary']
    performance = results['performance']
    
    print(f"\n回测结果摘要:")
    print(f"  成功股票: {summary['successful_stocks']}/{summary['total_stocks']}")
    print(f"  成功率: {summary['success_rate']:.1f}%")
    print(f"  平均收益率: {summary['avg_return']:.2f}%")
    print(f"  平均胜率: {summary['avg_win_rate']:.1f}%")
    print(f"  平均最大回撤: {summary['avg_max_drawdown']:.2f}%")
    print(f"  总执行时间: {performance['duration']:.2f}秒")
    
    # 显示个股结果
    print(f"\n个股详细结果:")
    for stock_code, result in results['results'].items():
        print(f"  {stock_code}: 收益率 {result['total_return']:.2f}%")
    
    return results


def demo_strategy_comparison():
    """演示策略比较功能"""
    print("\n" + "="*60)
    print("3. 策略比较演示")
    print("="*60)
    
    # 配置多个策略
    strategy_configs = [
        {
            'name': '快速策略',
            'class': DualMovingAverageStrategy,
            'params': {'fast_period': 5, 'slow_period': 15}
        },
        {
            'name': '中速策略',
            'class': DualMovingAverageStrategy,
            'params': {'fast_period': 10, 'slow_period': 30}
        },
        {
            'name': '慢速策略',
            'class': DualMovingAverageStrategy,
            'params': {'fast_period': 20, 'slow_period': 60}
        }
    ]
    
    # 初始化执行器
    config_manager = ConfigManager()
    db_url = config_manager.get_database_url()
    
    parallel_runner = ParallelBacktestRunner(
        max_workers=2,
        batch_size=2,
        db_config={'url': db_url}
    )
    
    test_stocks = ['000001.SZ', '000002.SZ']
    
    print(f"比较策略数: {len(strategy_configs)}")
    print(f"测试股票: {test_stocks}")
    
    # 执行策略比较
    comparison_results = parallel_runner.run_strategy_comparison(
        stock_codes=test_stocks,
        strategy_configs=strategy_configs,
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        initial_cash=100000.0,
        commission=0.001
    )
    
    # 显示比较结果
    analysis = comparison_results['comparison_analysis']
    
    print(f"\n策略比较结果:")
    print(f"最佳策略: {analysis['best_strategy']}")
    print(f"最佳评分: {analysis['best_score']:.2f}")
    
    print(f"\n策略排名:")
    for rank, (strategy_name, metrics) in enumerate(analysis['ranking'], 1):
        print(f"  {rank}. {strategy_name}: 平均收益 {metrics['avg_return']:.2f}%")
    
    return comparison_results


def demo_enhanced_analysis():
    """演示增强分析功能"""
    print("\n" + "="*60)
    print("4. 增强分析演示")
    print("="*60)
    
    # 创建单一回测来演示增强分析
    cerebro = bt.Cerebro()
    
    # 添加策略和增强分析器
    cerebro.addstrategy(DualMovingAverageStrategy, fast_period=10, slow_period=30)
    cerebro.addanalyzer(EnhancedAnalyzer, _name='enhanced')
    
    # 添加数据源
    config_manager = ConfigManager()
    db_url = config_manager.get_database_url()
    db_manager = create_sync_manager(db_url)
    from alphahome.bt_extensions.data.feeds import PostgreSQLDataFeed
    
    data_feed = PostgreSQLDataFeed(
        db_manager=db_manager,
        ts_code='000001.SZ',
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31)
    )
    cerebro.adddata(data_feed)
    
    # 设置参数
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)
    
    print("执行增强分析回测...")
    
    # 运行回测
    results = cerebro.run()
    strat = results[0]
    
    # 获取增强分析结果
    enhanced_analysis = strat.analyzers.enhanced.get_analysis()
    
    # 显示详细分析结果
    print(f"\n基础统计:")
    basic = enhanced_analysis.get('basic', {})
    print(f"  总收益率: {basic.get('total_return', 0):.2f}%")
    print(f"  交易天数: {basic.get('trading_days', 0)}")
    print(f"  最大资金: {basic.get('peak_value', 0):.2f}")
    
    print(f"\n风险指标:")
    risk = enhanced_analysis.get('risk', {})
    print(f"  最大回撤: {risk.get('max_drawdown', 0):.2f}%")
    print(f"  夏普比率: {risk.get('sharpe_ratio', 0):.3f}")
    print(f"  索提诺比率: {risk.get('sortino_ratio', 0):.3f}")
    
    print(f"\n交易统计:")
    trades = enhanced_analysis.get('trades', {})
    print(f"  总交易次数: {trades.get('total_trades', 0)}")
    print(f"  胜率: {trades.get('win_rate', 0):.1f}%")
    print(f"  盈利因子: {trades.get('profit_factor', 0):.2f}")
    
    print(f"\n绩效评估:")
    performance = enhanced_analysis.get('performance', {})
    print(f"  综合评分: {performance.get('overall_score', 0):.1f}/100")
    print(f"  策略等级: {performance.get('grade', 'N/A')}")
    print(f"  Kelly比例: {performance.get('kelly_fraction', 0):.3f}")
    
    return enhanced_analysis


def demo_performance_monitoring():
    """演示性能监控功能"""
    print("\n" + "="*60)
    print("5. 性能监控演示")
    print("="*60)
    
    # 创建性能监控器
    monitor = PerformanceMonitor(monitor_interval=0.5)
    
    print("开始性能监控...")
    monitor.start_monitoring()
    
    # 模拟一些工作负载（简单的回测）
    cerebro = bt.Cerebro()
    cerebro.addstrategy(DualMovingAverageStrategy)
    
    db_manager = SyncDBManager()
    from alphahome.bt_extensions.data.feeds import PostgreSQLDataFeed
    
    data_feed = PostgreSQLDataFeed(
        db_manager=db_manager,
        ts_code='000001.SZ',
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31)
    )
    cerebro.adddata(data_feed)
    cerebro.broker.setcash(100000.0)
    
    print("执行回测任务...")
    cerebro.run()
    
    # 停止监控并获取统计
    print("停止性能监控...")
    stats = monitor.stop_monitoring()
    
    # 显示监控结果
    monitor.print_stats(stats)
    
    # 获取优化建议
    recommendations = monitor.get_performance_recommendations(stats)
    print(f"\n性能优化建议:")
    for i, rec in enumerate(recommendations, 1):
        print(f"  {i}. {rec}")
    
    return stats


def main():
    """主演示函数"""
    print("BT Extensions 完整功能演示")
    print("="*60)
    print("重新定位为 Backtrader 插件，提供数据库连接和性能增强功能")
    
    try:
        # 1. 批量数据加载演示
        stock_data = demo_batch_loading()
        
        # 2. 并行回测执行演示
        parallel_results = demo_parallel_execution()
        
        # 3. 策略比较演示
        comparison_results = demo_strategy_comparison()
        
        # 4. 增强分析演示
        enhanced_analysis = demo_enhanced_analysis()
        
        # 5. 性能监控演示
        performance_stats = demo_performance_monitoring()
        
        print("\n" + "="*60)
        print("所有演示完成!")
        print("="*60)
        
        print(f"\n总结:")
        print(f"✅ 批量数据加载: 成功加载 {len(stock_data)} 只股票数据")
        print(f"✅ 并行回测执行: 成功完成 {parallel_results['summary']['total_stocks']} 只股票回测")
        print(f"✅ 策略比较: 成功比较 3 个不同策略")
        print(f"✅ 增强分析: 生成详细的风险和收益分析")
        print(f"✅ 性能监控: 实时监控系统资源使用")
        
        print(f"\n🎯 BT Extensions 作为插件成功增强了 Backtrader 的功能！")
        
    except Exception as e:
        print(f"演示过程中出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 