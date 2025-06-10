#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
优化的全市场回测方案

优化策略：
1. 批量数据加载 - 减少数据库连接次数
2. 并行批次处理 - 充分利用多核CPU
3. 智能内存管理 - 避免内存溢出
4. 进度监控 - 实时查看处理状态
"""

import sys
import os
import time
import asyncio
from datetime import date, datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing as mp
import pandas as pd

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from alphahome.common.sync_db_manager import SyncDBManager
from alphahome.common.config_manager import get_database_url


class BatchDataLoader:
    """批量数据加载器"""
    
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.sync_db = SyncDBManager(connection_string)
    
    def get_all_stocks(self, table='tushare_stock_daily'):
        """获取所有可用股票列表"""
        sql = f"""
        SELECT ts_code, COUNT(*) as record_count
        FROM {table} 
        WHERE trade_date >= '2023-01-01' AND trade_date <= '2023-12-31'
        GROUP BY ts_code 
        HAVING COUNT(*) >= 200
        ORDER BY ts_code
        """
        
        records = self.sync_db.fetch(sql)
        return [(r['ts_code'], r['record_count']) for r in records]
    
    def load_batch_data(self, stock_codes, start_date, end_date, table='tushare_stock_daily'):
        """批量加载多只股票数据"""
        print(f"🔄 批量加载 {len(stock_codes)} 只股票数据...")
        
        # 构建批量查询SQL
        placeholders = ','.join([f'${i+1}' for i in range(len(stock_codes))])
        sql = f"""
        SELECT ts_code, trade_date, open, high, low, close, volume
        FROM {table} 
        WHERE ts_code IN ({placeholders})
          AND trade_date BETWEEN ${len(stock_codes)+1} AND ${len(stock_codes)+2}
        ORDER BY ts_code, trade_date
        """
        
        params = list(stock_codes) + [start_date, end_date]
        records = self.sync_db.fetch(sql, *params)
        
        # 按股票代码分组
        data_by_stock = {}
        for record in records:
            ts_code = record['ts_code']
            if ts_code not in data_by_stock:
                data_by_stock[ts_code] = []
            
            data_by_stock[ts_code].append({
                'datetime': pd.to_datetime(record['trade_date']),
                'open': float(record['open']),
                'high': float(record['high']),
                'low': float(record['low']),
                'close': float(record['close']),
                'volume': float(record['volume'])
            })
        
        # 转换为DataFrame
        result = {}
        for ts_code, data_list in data_by_stock.items():
            df = pd.DataFrame(data_list)
            df = df.sort_values('datetime').reset_index(drop=True)
            result[ts_code] = df
        
        print(f"✅ 批量加载完成: {len(result)} 只股票")
        return result


def run_batch_backtest(batch_stocks, batch_id, connection_string, start_date, end_date):
    """运行单个批次的回测（用于多进程）"""
    try:
        print(f"📊 批次 {batch_id}: 开始处理 {len(batch_stocks)} 只股票")
        
        # 批量加载数据
        loader = BatchDataLoader(connection_string)
        stock_codes = [code for code, _ in batch_stocks]
        batch_data = loader.load_batch_data(stock_codes, start_date, end_date)
        
        # 简化的回测逻辑（避免backtrader在多进程中的问题）
        results = {}
        
        for ts_code in stock_codes:
            if ts_code not in batch_data:
                continue
                
            df = batch_data[ts_code]
            if len(df) < 30:  # 数据不足
                continue
            
            # 简单的移动平均策略回测
            df['ma5'] = df['close'].rolling(5).mean()
            df['ma20'] = df['close'].rolling(20).mean()
            df['signal'] = 0
            df.loc[df['ma5'] > df['ma20'], 'signal'] = 1
            df.loc[df['ma5'] < df['ma20'], 'signal'] = -1
            
            # 计算收益
            df['position'] = df['signal'].shift(1).fillna(0)
            df['returns'] = df['close'].pct_change()
            df['strategy_returns'] = df['position'] * df['returns']
            
            total_return = (1 + df['strategy_returns']).prod() - 1
            max_drawdown = (df['close'] / df['close'].cummax() - 1).min()
            
            results[ts_code] = {
                'total_return': total_return,
                'max_drawdown': max_drawdown,
                'data_points': len(df),
                'trades': (df['signal'].diff() != 0).sum()
            }
        
        print(f"✅ 批次 {batch_id}: 完成 {len(results)} 只股票回测")
        return batch_id, results
        
    except Exception as e:
        print(f"❌ 批次 {batch_id}: 处理失败 - {e}")
        return batch_id, {}


class OptimizedFullMarketBacktest:
    """优化的全市场回测引擎"""
    
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.loader = BatchDataLoader(connection_string)
    
    def run_parallel_backtest(self, batch_size=200, max_workers=None):
        """运行并行全市场回测"""
        print("🚀 优化全市场回测引擎启动")
        print("=" * 60)
        
        start_time = time.time()
        
        # 1. 获取所有股票
        print("📋 获取股票列表...")
        all_stocks = self.loader.get_all_stocks()
        total_stocks = len(all_stocks)
        print(f"✅ 找到 {total_stocks} 只股票")
        
        # 2. 分批处理
        batches = []
        for i in range(0, total_stocks, batch_size):
            batch = all_stocks[i:i + batch_size]
            batches.append(batch)
        
        print(f"📦 分为 {len(batches)} 个批次，每批约 {batch_size} 只股票")
        
        # 3. 确定进程数
        if max_workers is None:
            max_workers = min(mp.cpu_count(), len(batches))
        
        print(f"🔧 使用 {max_workers} 个进程并行处理")
        
        # 4. 并行执行回测
        start_date = date(2023, 1, 1)
        end_date = date(2023, 12, 31)
        
        all_results = {}
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有批次任务
            futures = []
            for i, batch in enumerate(batches, 1):
                future = executor.submit(
                    run_batch_backtest, 
                    batch, i, 
                    self.connection_string, 
                    start_date, end_date
                )
                futures.append(future)
            
            # 收集结果
            completed = 0
            for future in futures:
                try:
                    batch_id, batch_results = future.result(timeout=300)  # 5分钟超时
                    all_results.update(batch_results)
                    completed += 1
                    
                    progress = completed / len(batches) * 100
                    elapsed = time.time() - start_time
                    
                    print(f"📊 进度: {completed}/{len(batches)} "
                          f"({progress:.1f}%) "
                          f"已完成: {len(all_results)} 只股票 "
                          f"耗时: {elapsed:.1f}秒")
                    
                except Exception as e:
                    print(f"⚠️  批次处理失败: {e}")
        
        total_time = time.time() - start_time
        
        # 5. 汇总结果
        self._summarize_results(all_results, total_time, total_stocks)
        
        return all_results
    
    def _summarize_results(self, results, total_time, total_stocks):
        """汇总回测结果"""
        print(f"\n📈 全市场回测结果汇总")
        print("=" * 60)
        
        successful_stocks = len(results)
        success_rate = successful_stocks / total_stocks * 100
        
        print(f"📊 执行统计:")
        print(f"   总股票数: {total_stocks}")
        print(f"   成功回测: {successful_stocks} ({success_rate:.1f}%)")
        print(f"   总耗时: {total_time:.1f} 秒 ({total_time/60:.1f} 分钟)")
        print(f"   平均速度: {total_stocks/total_time:.1f} 股票/秒")
        
        if results:
            returns = [r['total_return'] for r in results.values()]
            drawdowns = [r['max_drawdown'] for r in results.values()]
            
            avg_return = sum(returns) / len(returns)
            avg_drawdown = sum(drawdowns) / len(drawdowns)
            
            positive_returns = sum(1 for r in returns if r > 0)
            win_rate = positive_returns / len(returns) * 100
            
            print(f"\n📈 收益统计:")
            print(f"   平均收益率: {avg_return:.2%}")
            print(f"   平均最大回撤: {avg_drawdown:.2%}")
            print(f"   盈利股票占比: {win_rate:.1f}%")
            print(f"   最佳收益: {max(returns):.2%}")
            print(f"   最差收益: {min(returns):.2%}")
        
        print(f"\n⚡ 性能对比:")
        print(f"   当前方案: {total_time/60:.1f} 分钟")
        print(f"   传统单线程: {total_stocks/7.7/60:.1f} 分钟")
        print(f"   性能提升: {(total_stocks/7.7)/total_time:.1f}x")


def main():
    """主函数"""
    print("⚡ AlphaHome 优化全市场回测演示")
    print("=" * 60)
    
    try:
        # 获取配置
        connection_string = get_database_url()
        if not connection_string:
            print("❌ 未找到数据库配置")
            return
        
        # 创建优化引擎
        engine = OptimizedFullMarketBacktest(connection_string)
        
        # 运行并行回测
        results = engine.run_parallel_backtest(
            batch_size=100,    # 每批100只股票
            max_workers=4      # 使用4个进程
        )
        
        print(f"\n🎉 全市场回测完成！")
        print(f"💾 回测结果包含 {len(results)} 只股票的详细数据")
        
    except KeyboardInterrupt:
        print("\n⏹️  用户中断回测")
    except Exception as e:
        print(f"\n❌ 回测过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 