#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库批量保存性能优化测试脚本

该脚本用于测试和验证第一阶段数据库性能优化的效果，包括：
1. 连接池配置优化测试
2. 性能监控功能测试
3. 批量写入性能基准测试
4. 配置文件支持测试

使用方法：
    python test_db_performance_optimization.py

注意：
    - 需要有效的数据库连接配置
    - 会创建测试表，测试完成后自动清理
    - 建议在测试环境中运行
"""

import asyncio
import time
import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Dict, Any

# 导入优化后的数据库管理器
from alphahome.common.db_manager import create_async_manager
from alphahome.common.config_manager import load_config


class DatabasePerformanceTest:
    """数据库性能测试类"""
    
    def __init__(self):
        self.db_manager = None
        self.test_table_name = f"test_performance_{int(time.time())}"
        self.test_results = {}
    
    async def setup(self):
        """初始化测试环境"""
        print("🔧 初始化测试环境...")
        
        # 加载配置
        config = load_config()
        db_url = config.get('database', {}).get('url')
        
        if not db_url:
            raise ValueError("未找到数据库连接配置，请检查 config.json 文件")
        
        # 创建数据库管理器
        self.db_manager = create_async_manager(db_url)
        await self.db_manager.connect()
        
        # 创建测试表
        await self._create_test_table()
        
        print(f"✅ 测试环境初始化完成，测试表: {self.test_table_name}")
    
    async def _create_test_table(self):
        """创建测试表"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.test_table_name} (
            id SERIAL PRIMARY KEY,
            ts_code VARCHAR(15) NOT NULL,
            trade_date DATE NOT NULL,
            open_price FLOAT,
            close_price FLOAT,
            volume BIGINT,
            amount FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, trade_date)
        );
        """
        await self.db_manager.execute(create_table_sql)
    
    def generate_test_data(self, num_rows: int) -> pd.DataFrame:
        """生成测试数据"""
        np.random.seed(42)  # 确保可重复性
        
        # 生成股票代码
        stock_codes = [f"{str(i).zfill(6)}.SZ" for i in range(1, 101)]
        
        # 生成日期范围
        start_date = date(2024, 1, 1)
        dates = pd.date_range(start_date, periods=num_rows//len(stock_codes) + 1, freq='D')
        
        data = []
        for i in range(num_rows):
            data.append({
                'ts_code': np.random.choice(stock_codes),
                'trade_date': np.random.choice(dates).date(),
                'open_price': np.random.uniform(10, 100),
                'close_price': np.random.uniform(10, 100),
                'volume': np.random.randint(1000, 1000000),
                'amount': np.random.uniform(10000, 10000000)
            })
        
        return pd.DataFrame(data)
    
    async def test_connection_pool_config(self):
        """测试连接池配置"""
        print("\n📊 测试连接池配置...")
        
        # 检查连接池是否正确配置
        pool = self.db_manager.pool
        if pool:
            print(f"✅ 连接池已创建")
            print(f"   - 最小连接数: {pool._minsize}")
            print(f"   - 最大连接数: {pool._maxsize}")
            
            # 测试并发连接
            start_time = time.time()
            tasks = []
            for i in range(10):
                task = asyncio.create_task(
                    self.db_manager.fetch_val("SELECT 1")
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)
            end_time = time.time()
            
            print(f"✅ 并发查询测试完成")
            print(f"   - 并发任务数: {len(tasks)}")
            print(f"   - 总耗时: {end_time - start_time:.3f}s")
            print(f"   - 平均耗时: {(end_time - start_time) / len(tasks):.3f}s")
            
            self.test_results['connection_pool'] = {
                'min_size': pool._minsize,
                'max_size': pool._maxsize,
                'concurrent_queries': len(tasks),
                'total_time': end_time - start_time,
                'avg_time_per_query': (end_time - start_time) / len(tasks)
            }
        else:
            print("❌ 连接池未创建")
    
    async def test_performance_monitoring(self):
        """测试性能监控功能"""
        print("\n📈 测试性能监控功能...")
        
        # 重置性能统计
        self.db_manager.reset_performance_statistics()
        
        # 执行几次不同大小的批量操作
        batch_sizes = [1000, 5000, 10000]
        
        for batch_size in batch_sizes:
            print(f"   测试批次大小: {batch_size} 行")
            
            # 生成测试数据
            test_data = self.generate_test_data(batch_size)
            
            # 执行批量插入
            start_time = time.time()
            result = await self.db_manager.copy_from_dataframe(
                df=test_data,
                target=self.test_table_name,
                conflict_columns=['ts_code', 'trade_date'],
                update_columns=['open_price', 'close_price', 'volume', 'amount']
            )
            end_time = time.time()
            
            print(f"     - 处理行数: {result}")
            print(f"     - 耗时: {end_time - start_time:.3f}s")
            print(f"     - 吞吐量: {result / (end_time - start_time):.0f} 行/秒")
        
        # 获取性能统计
        stats = self.db_manager.get_performance_statistics()
        print(f"\n📊 性能统计摘要:")
        print(f"   - 总操作数: {stats['total_operations']}")
        print(f"   - 总处理行数: {stats['total_rows_processed']}")
        print(f"   - 平均吞吐量: {stats['average_throughput']:.0f} 行/秒")
        print(f"   - 最近平均吞吐量: {stats['recent_average_throughput']:.0f} 行/秒")
        print(f"   - 建议批次大小: {stats['optimal_batch_size']} 行")
        
        self.test_results['performance_monitoring'] = stats
    
    async def test_batch_size_optimization(self):
        """测试不同批次大小的性能"""
        print("\n🎯 测试批次大小优化...")
        
        batch_sizes = [500, 1000, 2500, 5000, 7500, 10000, 15000]
        results = {}
        
        for batch_size in batch_sizes:
            print(f"   测试批次大小: {batch_size} 行")
            
            # 生成测试数据
            test_data = self.generate_test_data(batch_size)
            
            # 执行批量操作并测量性能
            start_time = time.time()
            try:
                result = await self.db_manager.copy_from_dataframe(
                    df=test_data,
                    target=self.test_table_name,
                    conflict_columns=['ts_code', 'trade_date']
                )
                end_time = time.time()
                
                processing_time = end_time - start_time
                throughput = result / processing_time if processing_time > 0 else 0
                
                results[batch_size] = {
                    'rows_processed': result,
                    'processing_time': processing_time,
                    'throughput': throughput
                }
                
                print(f"     - 成功: {throughput:.0f} 行/秒")
                
            except Exception as e:
                print(f"     - 失败: {str(e)}")
                results[batch_size] = {
                    'error': str(e)
                }
        
        # 找到最优批次大小
        valid_results = {k: v for k, v in results.items() if 'throughput' in v}
        if valid_results:
            optimal_batch_size = max(valid_results.keys(), 
                                   key=lambda k: valid_results[k]['throughput'])
            optimal_throughput = valid_results[optimal_batch_size]['throughput']
            
            print(f"\n🏆 最优批次大小: {optimal_batch_size} 行")
            print(f"   最高吞吐量: {optimal_throughput:.0f} 行/秒")
            
            self.test_results['batch_optimization'] = {
                'optimal_batch_size': optimal_batch_size,
                'optimal_throughput': optimal_throughput,
                'all_results': results
            }
    
    async def cleanup(self):
        """清理测试环境"""
        print(f"\n🧹 清理测试环境...")
        
        try:
            # 删除测试表
            await self.db_manager.execute(f"DROP TABLE IF EXISTS {self.test_table_name}")
            print(f"✅ 测试表 {self.test_table_name} 已删除")
            
            # 关闭数据库连接
            await self.db_manager.close()
            print("✅ 数据库连接已关闭")
            
        except Exception as e:
            print(f"⚠️ 清理过程中出现错误: {e}")
    
    def print_summary(self):
        """打印测试总结"""
        print("\n" + "="*60)
        print("📋 数据库性能优化测试总结")
        print("="*60)
        
        if 'connection_pool' in self.test_results:
            pool_stats = self.test_results['connection_pool']
            print(f"\n🔗 连接池配置:")
            print(f"   最大连接数: {pool_stats['max_size']} (优化前: 10)")
            print(f"   并发查询性能: {pool_stats['avg_time_per_query']:.3f}s/查询")
        
        if 'performance_monitoring' in self.test_results:
            perf_stats = self.test_results['performance_monitoring']
            print(f"\n📊 性能监控:")
            print(f"   总操作数: {perf_stats['total_operations']}")
            print(f"   平均吞吐量: {perf_stats['average_throughput']:.0f} 行/秒")
            print(f"   建议批次大小: {perf_stats['optimal_batch_size']} 行")
        
        if 'batch_optimization' in self.test_results:
            batch_stats = self.test_results['batch_optimization']
            print(f"\n🎯 批次优化:")
            print(f"   最优批次大小: {batch_stats['optimal_batch_size']} 行")
            print(f"   最高吞吐量: {batch_stats['optimal_throughput']:.0f} 行/秒")
        
        print(f"\n✅ 测试完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


async def main():
    """主测试函数"""
    print("🚀 开始数据库批量保存性能优化测试")
    print("="*60)
    
    test = DatabasePerformanceTest()
    
    try:
        # 初始化测试环境
        await test.setup()
        
        # 执行各项测试
        await test.test_connection_pool_config()
        await test.test_performance_monitoring()
        await test.test_batch_size_optimization()
        
        # 打印测试总结
        test.print_summary()
        
    except Exception as e:
        print(f"❌ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 清理测试环境
        await test.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
