#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BT Extensions 全市场回测效率测试

测试新设计的 bt_extensions 模块在大规模回测中的性能表现：
1. 批量数据加载效率
2. 并行回测性能
3. 缓存系统效果
4. 内存使用监控
5. 与之前版本的性能对比
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import psutil
import backtrader as bt
from datetime import date, datetime
from typing import List, Dict, Any

from alphahome.bt_extensions import (
    BatchDataLoader,
    ParallelBacktestRunner,
    CacheManager,
    PerformanceMonitor,
    PostgreSQLDataFeed
)
from alphahome.common.db_manager import create_sync_manager
from alphahome.common.config_manager import ConfigManager


class SimpleMAStrategy(bt.Strategy):
    """简单移动平均策略，用于性能测试"""
    
    params = (
        ('ma_period', 20),
    )
    
    def __init__(self):
        self.ma = bt.indicators.SMA(self.data.close, period=self.params.ma_period)
    
    def next(self):
        if not self.position:
            if self.data.close[0] > self.ma[0]:
                self.buy()
        else:
            if self.data.close[0] < self.ma[0]:
                self.sell()


class PerformanceTester:
    """性能测试器"""
    
    def __init__(self):
        self.config_manager = ConfigManager()
        db_url = self.config_manager.get_database_url()
        self.db_manager = create_sync_manager(db_url)
        self.results = {}
    
    def get_stock_list(self, limit: int = None) -> List[str]:
        """获取股票列表"""
        sql = """
        SELECT DISTINCT ts_code 
        FROM tushare_stock_daily 
        WHERE trade_date >= '2023-01-01' 
        AND trade_date <= '2023-12-31'
        ORDER BY ts_code
        """
        
        if limit:
            sql += f" LIMIT {limit}"
        
        records = self.db_manager.fetch(sql)
        return [record['ts_code'] for record in records]
    
    def test_batch_loading_performance(self, stock_counts: List[int]):
        """测试批量数据加载性能"""
        print("\n" + "="*80)
        print("1. 批量数据加载性能测试")
        print("="*80)
        
        # 不同缓存配置的测试
        cache_configs = [
            {"name": "无缓存", "cache": None},
            {"name": "内存缓存(256MB)", "cache": CacheManager(max_memory_mb=256, enable_disk_cache=False)},
            {"name": "混合缓存(256MB+磁盘)", "cache": CacheManager(max_memory_mb=256, enable_disk_cache=True)}
        ]
        
        results = {}
        
        for stock_count in stock_counts:
            print(f"\n测试 {stock_count} 只股票的加载性能:")
            stock_list = self.get_stock_list(stock_count)
            
            results[stock_count] = {}
            
            for config in cache_configs:
                print(f"  {config['name']}:")
                
                batch_loader = BatchDataLoader(self.db_manager, config['cache'])
                
                # 第一次加载（冷启动）
                start_time = time.time()
                start_memory = psutil.Process().memory_info().rss / 1024 / 1024
                
                stock_data = batch_loader.load_stocks_data(
                    stock_codes=stock_list,
                    start_date=date(2023, 1, 1),
                    end_date=date(2023, 12, 31),
                    use_cache=config['cache'] is not None
                )
                
                cold_time = time.time() - start_time
                cold_memory = psutil.Process().memory_info().rss / 1024 / 1024
                
                # 第二次加载（热启动，测试缓存效果）
                if config['cache'] is not None:
                    start_time = time.time()
                    stock_data_cached = batch_loader.load_stocks_data(
                        stock_codes=stock_list,
                        start_date=date(2023, 1, 1),
                        end_date=date(2023, 12, 31),
                        use_cache=True
                    )
                    hot_time = time.time() - start_time
                    hot_memory = psutil.Process().memory_info().rss / 1024 / 1024
                    
                    # 缓存统计
                    cache_stats = batch_loader.get_cache_stats()
                    hit_rate = cache_stats.get('overall_hit_rate', 0)
                    cache_memory = cache_stats.get('memory_size_mb', 0)
                else:
                    hot_time = None
                    hot_memory = cold_memory
                    hit_rate = 0
                    cache_memory = 0
                
                results[stock_count][config['name']] = {
                    'loaded_stocks': len(stock_data),
                    'cold_time': cold_time,
                    'hot_time': hot_time,
                    'memory_growth': cold_memory - start_memory,
                    'final_memory': hot_memory,
                    'hit_rate': hit_rate,
                    'cache_memory': cache_memory,
                    'records_per_second': sum(len(df) for df in stock_data.values()) / cold_time
                }
                
                print(f"    冷启动: {cold_time:.2f}秒, 内存增长: {cold_memory - start_memory:.1f}MB")
                if hot_time is not None:
                    print(f"    热启动: {hot_time:.2f}秒, 缓存命中率: {hit_rate:.1f}%")
                    if hot_time > 0:
                        print(f"    性能提升: {cold_time/hot_time:.1f}x")
                    else:
                        print(f"    性能提升: >1000x (瞬时缓存命中)")
                print(f"    数据处理速度: {results[stock_count][config['name']]['records_per_second']:.0f} 记录/秒")
                
                # 清理缓存
                if config['cache']:
                    config['cache'].clear()
        
        self.results['batch_loading'] = results
        return results
    
    def test_parallel_execution_performance(self, stock_count: int, worker_configs: List[Dict]):
        """测试并行执行性能"""
        print(f"\n" + "="*80)
        print(f"2. 并行执行性能测试 ({stock_count} 只股票)")
        print("="*80)
        
        stock_list = self.get_stock_list(stock_count)
        results = {}
        
        for config in worker_configs:
            workers = config['workers']
            batch_size = config['batch_size']
            name = f"{workers}进程-批次{batch_size}"
            
            print(f"\n测试配置: {name}")
            
            # 创建并行执行器
            db_url = self.config_manager.get_database_url()
            parallel_runner = ParallelBacktestRunner(
                max_workers=workers,
                batch_size=batch_size,
                db_config=db_url
            )
            
            # 启动性能监控
            monitor = PerformanceMonitor(monitor_interval=1.0)
            monitor.start_monitoring()
            
            start_time = time.time()
            start_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            # 执行并行回测
            backtest_results = parallel_runner.run_parallel_backtests(
                stock_codes=stock_list,
                strategy_class=SimpleMAStrategy,
                strategy_params={'ma_period': 20},
                start_date=date(2023, 1, 1),
                end_date=date(2023, 12, 31),
                initial_cash=100000.0,
                commission=0.001
            )
            
            total_time = time.time() - start_time
            final_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            # 停止监控
            perf_stats = monitor.stop_monitoring()
            
            # 分析结果
            summary = backtest_results['summary']
            successful_stocks = summary['successful_stocks']
            
            results[name] = {
                'total_time': total_time,
                'successful_stocks': successful_stocks,
                'success_rate': summary['success_rate'],
                'stocks_per_second': successful_stocks / total_time,
                'memory_growth': final_memory - start_memory,
                'avg_cpu': perf_stats.get('cpu', {}).get('avg_percent', 0),
                'peak_memory': perf_stats.get('memory', {}).get('peak_rss_mb', 0),
                'avg_return': summary.get('avg_return', 0),
                'performance_stats': perf_stats
            }
            
            print(f"  执行时间: {total_time:.1f}秒")
            print(f"  成功股票: {successful_stocks}/{stock_count} ({summary['success_rate']:.1f}%)")
            print(f"  处理速度: {successful_stocks / total_time:.1f} 股票/秒")
            print(f"  内存增长: {final_memory - start_memory:.1f}MB")
            print(f"  平均CPU: {perf_stats.get('cpu', {}).get('avg_percent', 0):.1f}%")
            print(f"  峰值内存: {perf_stats.get('memory', {}).get('peak_rss_mb', 0):.1f}MB")
        
        self.results['parallel_execution'] = results
        return results
    
    def test_scaling_analysis(self, stock_counts: List[int]):
        """测试扩展性分析"""
        print(f"\n" + "="*80)
        print("3. 扩展性分析测试")
        print("="*80)
        
        results = {}
        
        # 使用最优配置进行扩展性测试
        best_config = {
            'workers': min(4, psutil.cpu_count()),
            'batch_size': 50
        }
        
        for stock_count in stock_counts:
            print(f"\n测试 {stock_count} 只股票:")
            
            stock_list = self.get_stock_list(stock_count)
            actual_count = len(stock_list)
            
            if actual_count < stock_count:
                print(f"  警告: 实际只有 {actual_count} 只股票数据")
            
            # 执行回测
            db_url = self.config_manager.get_database_url()
            parallel_runner = ParallelBacktestRunner(
                max_workers=best_config['workers'],
                batch_size=best_config['batch_size'],
                db_config=db_url
            )
            
            start_time = time.time()
            start_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            backtest_results = parallel_runner.run_parallel_backtests(
                stock_codes=stock_list,
                strategy_class=SimpleMAStrategy,
                strategy_params={'ma_period': 20},
                start_date=date(2023, 1, 1),
                end_date=date(2023, 12, 31),
                initial_cash=100000.0,
                commission=0.001
            )
            
            total_time = time.time() - start_time
            final_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            summary = backtest_results['summary']
            
            results[stock_count] = {
                'actual_stocks': actual_count,
                'successful_stocks': summary['successful_stocks'],
                'total_time': total_time,
                'stocks_per_second': summary['successful_stocks'] / total_time,
                'memory_growth': final_memory - start_memory,
                'memory_per_stock': (final_memory - start_memory) / actual_count if actual_count > 0 else 0,
                'success_rate': summary['success_rate']
            }
            
            print(f"  实际处理: {actual_count} 只股票")
            print(f"  成功完成: {summary['successful_stocks']} 只")
            print(f"  执行时间: {total_time:.1f}秒")
            print(f"  处理速度: {summary['successful_stocks'] / total_time:.1f} 股票/秒")
            print(f"  内存效率: {(final_memory - start_memory) / actual_count:.2f}MB/股票")
        
        self.results['scaling'] = results
        return results
    
    def generate_performance_report(self):
        """生成性能报告"""
        print("\n" + "="*80)
        print("📊 BT Extensions 性能测试报告")
        print("="*80)
        
        # 批量加载性能报告
        if 'batch_loading' in self.results:
            print("\n🚀 批量数据加载性能:")
            batch_results = self.results['batch_loading']
            
            for stock_count, configs in batch_results.items():
                print(f"\n  {stock_count} 只股票:")
                for config_name, stats in configs.items():
                    cold_time = stats['cold_time']
                    hot_time = stats.get('hot_time')
                    hit_rate = stats['hit_rate']
                    
                    print(f"    {config_name}: {cold_time:.2f}秒", end="")
                    if hot_time is not None:
                        print(f" → {hot_time:.2f}秒 (缓存命中率{hit_rate:.1f}%)")
                    else:
                        print()
        
        # 并行执行性能报告
        if 'parallel_execution' in self.results:
            print("\n⚡ 并行执行性能:")
            parallel_results = self.results['parallel_execution']
            
            best_config = min(parallel_results.items(), key=lambda x: x[1]['total_time'])
            print(f"  最优配置: {best_config[0]}")
            print(f"  最佳性能: {best_config[1]['stocks_per_second']:.1f} 股票/秒")
            
            for config_name, stats in parallel_results.items():
                print(f"    {config_name}: {stats['stocks_per_second']:.1f} 股票/秒, "
                      f"成功率 {stats['success_rate']:.1f}%")
        
        # 扩展性分析报告
        if 'scaling' in self.results:
            print("\n📈 扩展性分析:")
            scaling_results = self.results['scaling']
            
            stock_counts = sorted(scaling_results.keys())
            for stock_count in stock_counts:
                stats = scaling_results[stock_count]
                print(f"  {stock_count:4d} 股票: {stats['stocks_per_second']:5.1f} 股票/秒, "
                      f"{stats['memory_per_stock']:4.2f}MB/股票")
            
            # 线性度分析
            if len(stock_counts) >= 2:
                first_perf = scaling_results[stock_counts[0]]['stocks_per_second']
                last_perf = scaling_results[stock_counts[-1]]['stocks_per_second']
                efficiency = (last_perf / first_perf) * 100
                print(f"\n  扩展效率: {efficiency:.1f}% (理想值: 100%)")
        
        # 全市场回测预估
        if 'scaling' in self.results:
            print("\n🎯 全市场回测预估:")
            
            # 基于最大测试规模预估
            max_tested = max(self.results['scaling'].keys())
            max_stats = self.results['scaling'][max_tested]
            avg_speed = max_stats['stocks_per_second']
            
            full_market_estimates = [
                (1000, "小盘股选股"),
                (2000, "中等规模回测"),
                (4000, "全市场回测"),
                (5000, "全市场+指数")
            ]
            
            for stock_count, description in full_market_estimates:
                estimated_time = stock_count / avg_speed
                estimated_memory = max_stats['memory_per_stock'] * stock_count
                
                hours = int(estimated_time // 3600)
                minutes = int((estimated_time % 3600) // 60)
                seconds = int(estimated_time % 60)
                
                time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                
                print(f"  {stock_count:4d} 股票 ({description}): "
                      f"预估 {time_str}, 内存需求 {estimated_memory:.0f}MB")
        
        print("\n" + "="*80)


def main():
    """主测试函数"""
    print("BT Extensions 全市场回测效率测试")
    print("="*80)
    
    tester = PerformanceTester()
    
    try:
        # 1. 批量加载性能测试（不同股票数量）
        print("开始批量数据加载性能测试...")
        batch_results = tester.test_batch_loading_performance([10, 50, 100, 200])
        
        # 2. 并行执行性能测试
        print("\n开始并行执行性能测试...")
        parallel_configs = [
            {'workers': 1, 'batch_size': 25},   # 单进程基准
            {'workers': 2, 'batch_size': 25},   # 双进程
            {'workers': 4, 'batch_size': 25},   # 四进程
            {'workers': 2, 'batch_size': 50},   # 大批次
            {'workers': 4, 'batch_size': 100}   # 大批次+多进程
        ]
        parallel_results = tester.test_parallel_execution_performance(100, parallel_configs)
        
        # 3. 扩展性分析测试
        print("\n开始扩展性分析测试...")
        scaling_results = tester.test_scaling_analysis([50, 100, 200, 500])
        
        # 4. 生成综合报告
        tester.generate_performance_report()
        
        print(f"\n✅ 所有测试完成！")
        print(f"📋 详细结果已保存在测试器对象中")
        
    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 