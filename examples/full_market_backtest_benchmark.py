#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
全市场回测效率基准测试

测试指标：
1. 数据加载速度
2. 内存使用情况
3. 回测执行时间
4. 不同股票数量的性能对比
"""

import sys
import os
import time
import psutil
from datetime import date, datetime
import backtrader as bt
import pandas as pd

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from alphahome.backtesting.strategies.examples.dual_moving_average import DualMovingAverageStrategy
from alphahome.common.sync_db_manager import SyncDBManager
from alphahome.common.config_manager import get_database_url, get_backtesting_config
from examples.final_sync_backtest_demo import SyncPostgreSQLDataFeed


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.start_memory = None
    
    def start(self):
        """开始监控"""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        return self
    
    def get_stats(self):
        """获取当前统计"""
        current_time = time.time()
        current_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        
        return {
            'elapsed_time': current_time - self.start_time,
            'current_memory_mb': current_memory,
            'memory_increase_mb': current_memory - self.start_memory,
            'cpu_percent': self.process.cpu_percent()
        }


def get_available_stocks(sync_db_manager, table='tushare_stock_daily', limit=None):
    """获取可用的股票列表"""
    print("📋 获取可用股票列表...")
    
    sql = f"""
    SELECT ts_code, COUNT(*) as record_count
    FROM {table} 
    WHERE trade_date >= '2023-01-01' AND trade_date <= '2023-12-31'
    GROUP BY ts_code 
    HAVING COUNT(*) >= 200
    ORDER BY ts_code
    """
    
    if limit:
        sql += f" LIMIT {limit}"
    
    records = sync_db_manager.fetch(sql)
    stocks = [(r['ts_code'], r['record_count']) for r in records]
    
    print(f"✅ 找到 {len(stocks)} 只符合条件的股票")
    return stocks


def benchmark_data_loading(sync_db_manager, stocks, table='tushare_stock_daily'):
    """基准测试：数据加载"""
    print(f"\n📊 数据加载基准测试 ({len(stocks)} 只股票)")
    print("-" * 50)
    
    monitor = PerformanceMonitor().start()
    
    # 批量数据加载测试
    start_date = date(2023, 1, 1)
    end_date = date(2023, 12, 31)
    
    total_records = 0
    successful_loads = 0
    
    for i, (ts_code, expected_count) in enumerate(stocks, 1):
        try:
            sql = f"""
            SELECT COUNT(*) as count
            FROM {table} 
            WHERE ts_code = $1 AND trade_date BETWEEN $2 AND $3
            """
            result = sync_db_manager.fetch_one(sql, ts_code, start_date, end_date)
            count = result['count'] if result else 0
            
            if count > 0:
                total_records += count
                successful_loads += 1
            
            # 每100只股票报告一次进度
            if i % 100 == 0:
                stats = monitor.get_stats()
                print(f"   进度: {i}/{len(stocks)} ({i/len(stocks)*100:.1f}%) "
                      f"时间: {stats['elapsed_time']:.1f}s "
                      f"内存: {stats['current_memory_mb']:.1f}MB")
                
        except Exception as e:
            print(f"   ❌ {ts_code}: {e}")
    
    final_stats = monitor.get_stats()
    
    print(f"\n📈 数据加载结果:")
    print(f"   成功加载: {successful_loads}/{len(stocks)} 只股票")
    print(f"   总数据量: {total_records:,} 条记录")
    print(f"   总耗时: {final_stats['elapsed_time']:.2f} 秒")
    print(f"   平均速度: {len(stocks)/final_stats['elapsed_time']:.1f} 股票/秒")
    print(f"   数据吞吐: {total_records/final_stats['elapsed_time']:,.0f} 记录/秒")
    print(f"   内存使用: {final_stats['current_memory_mb']:.1f} MB")
    print(f"   内存增长: {final_stats['memory_increase_mb']:.1f} MB")
    
    return {
        'successful_loads': successful_loads,
        'total_records': total_records,
        'elapsed_time': final_stats['elapsed_time'],
        'memory_mb': final_stats['current_memory_mb']
    }


def benchmark_backtest_execution(sync_db_manager, stocks, max_stocks=None):
    """基准测试：回测执行"""
    if max_stocks:
        stocks = stocks[:max_stocks]
    
    print(f"\n🚀 回测执行基准测试 ({len(stocks)} 只股票)")
    print("-" * 50)
    
    monitor = PerformanceMonitor().start()
    
    try:
        # 创建Cerebro引擎
        cerebro = bt.Cerebro()
        cerebro.broker.setcash(1000000)  # 100万初始资金
        cerebro.broker.setcommission(commission=0.001)
        
        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        
        # 添加数据源
        print("📈 添加数据源...")
        added_feeds = 0
        
        for ts_code, _ in stocks:
            try:
                data_feed = SyncPostgreSQLDataFeed(
                    ts_code=ts_code,
                    sync_db_manager=sync_db_manager,
                    start_date=date(2023, 1, 1),
                    end_date=date(2023, 12, 31),
                    table_name='tushare_stock_daily'
                )
                cerebro.adddata(data_feed, name=ts_code)
                added_feeds += 1
                
                # 每50只股票报告一次进度
                if added_feeds % 50 == 0:
                    stats = monitor.get_stats()
                    print(f"   已添加: {added_feeds} 只股票 "
                          f"时间: {stats['elapsed_time']:.1f}s "
                          f"内存: {stats['current_memory_mb']:.1f}MB")
                    
            except Exception as e:
                print(f"   ⚠️  {ts_code}: 跳过 - {e}")
        
        print(f"✅ 成功添加 {added_feeds} 只股票数据源")
        
        # 添加策略
        cerebro.addstrategy(DualMovingAverageStrategy, fast_period=5, slow_period=20, printlog=False)
        
        # 运行回测
        print("\n🎯 执行回测...")
        backtest_start = time.time()
        results = cerebro.run()
        backtest_end = time.time()
        
        backtest_time = backtest_end - backtest_start
        final_stats = monitor.get_stats()
        
        # 分析结果
        strategy = results[0] if results else None
        final_value = cerebro.broker.getvalue()
        
        print(f"\n📊 回测执行结果:")
        print(f"   股票数量: {added_feeds}")
        print(f"   回测耗时: {backtest_time:.2f} 秒")
        print(f"   总耗时: {final_stats['elapsed_time']:.2f} 秒")
        print(f"   平均每股: {backtest_time/added_feeds*1000:.1f} 毫秒") if added_feeds > 0 else None
        print(f"   最终价值: {final_value:,.2f}")
        print(f"   内存峰值: {final_stats['current_memory_mb']:.1f} MB")
        
        # 交易统计
        if strategy:
            try:
                trades = strategy.analyzers.trades.get_analysis()
                total_trades = trades.get('total', {}).get('total', 0)
                print(f"   总交易数: {total_trades}")
            except:
                print("   交易统计: 获取失败")
        
        return {
            'stocks_count': added_feeds,
            'backtest_time': backtest_time,
            'total_time': final_stats['elapsed_time'],
            'final_value': final_value,
            'memory_mb': final_stats['current_memory_mb']
        }
        
    except Exception as e:
        print(f"❌ 回测执行失败: {e}")
        return None


def run_scale_tests(sync_db_manager):
    """运行不同规模的测试"""
    print("\n🔬 规模化测试")
    print("=" * 60)
    
    # 获取股票列表
    all_stocks = get_available_stocks(sync_db_manager, limit=1000)  # 限制1000只以防内存溢出
    
    # 不同规模的测试
    test_scales = [10, 50, 100, 200, 500]
    results = []
    
    for scale in test_scales:
        if scale > len(all_stocks):
            print(f"⚠️  跳过测试规模 {scale} (可用股票不足)")
            continue
            
        print(f"\n📏 测试规模: {scale} 只股票")
        print("-" * 30)
        
        test_stocks = all_stocks[:scale]
        
        # 数据加载测试
        load_result = benchmark_data_loading(sync_db_manager, test_stocks)
        
        # 回测执行测试（较小规模）
        if scale <= 100:
            backtest_result = benchmark_backtest_execution(sync_db_manager, test_stocks)
            
            results.append({
                'scale': scale,
                'load_time': load_result['elapsed_time'],
                'load_memory': load_result['memory_mb'],
                'backtest_time': backtest_result['backtest_time'] if backtest_result else None,
                'backtest_memory': backtest_result['memory_mb'] if backtest_result else None
            })
        else:
            results.append({
                'scale': scale,
                'load_time': load_result['elapsed_time'],
                'load_memory': load_result['memory_mb'],
                'backtest_time': None,
                'backtest_memory': None
            })
    
    # 汇总报告
    print(f"\n📈 规模化测试汇总")
    print("=" * 60)
    print(f"{'规模':<8} {'加载时间':<10} {'加载内存':<10} {'回测时间':<10} {'回测内存':<10}")
    print("-" * 60)
    
    for result in results:
        scale = result['scale']
        load_time = f"{result['load_time']:.1f}s"
        load_memory = f"{result['load_memory']:.0f}MB"
        backtest_time = f"{result['backtest_time']:.1f}s" if result['backtest_time'] else "N/A"
        backtest_memory = f"{result['backtest_memory']:.0f}MB" if result['backtest_memory'] else "N/A"
        
        print(f"{scale:<8} {load_time:<10} {load_memory:<10} {backtest_time:<10} {backtest_memory:<10}")


def main():
    """主函数"""
    print("🔬 AlphaHome 全市场回测效率基准测试")
    print("=" * 60)
    print("🎯 测试目标: 评估全市场回测的性能表现")
    print()
    
    try:
        # 初始化
        connection_string = get_database_url()
        if not connection_string:
            print("❌ 未找到数据库配置")
            return
        
        sync_db_manager = SyncDBManager(connection_string)
        
        if not sync_db_manager.test_connection():
            print("❌ 数据库连接失败")
            return
        
        print("✅ 数据库连接成功")
        
        # 运行测试
        run_scale_tests(sync_db_manager)
        
        print(f"\n🎉 基准测试完成！")
        print(f"\n💡 性能优化建议:")
        print(f"   🔹 小规模回测 (≤50股): 性能优异")
        print(f"   🔹 中规模回测 (50-200股): 可接受")
        print(f"   🔹 大规模回测 (>200股): 建议分批处理")
        print(f"   🔹 全市场回测: 建议使用并行处理和数据预加载")
        
    except KeyboardInterrupt:
        print("\n⏹️  用户中断测试")
    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 