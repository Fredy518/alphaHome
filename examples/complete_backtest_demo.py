#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
完整的回测示例 - 展示统一配置管理器的使用

这个示例展示了：
1. 如何使用统一的配置管理器
2. 完整的错误处理
3. 轻量级的backtrader集成
4. 性能分析和结果展示
"""

import sys
import os
import asyncio
from datetime import date, datetime
import backtrader as bt

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from alphahome.backtesting import PostgreSQLDataFeed
from alphahome.backtesting.strategies.examples.dual_moving_average import DualMovingAverageStrategy
from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import (
    get_database_url, 
    get_backtesting_config,
    ConfigManager
)


class BacktestDemo:
    """回测演示类"""
    
    def __init__(self):
        self.config_manager = ConfigManager()
        self.db_manager = None
        self.cerebro = None
        
    async def setup_database(self):
        """设置数据库连接"""
        print("🔧 设置数据库连接...")
        
        # 使用统一配置管理器获取数据库URL
        connection_string = get_database_url()
        
        if not connection_string:
            print("⚠️  未找到数据库配置，使用默认配置")
            # 在实际项目中，这里应该提示用户创建配置文件
            connection_string = "postgresql://postgres:password@localhost:5432/tusharedb"
            print(f"   默认连接: {connection_string}")
        else:
            # 隐藏密码显示
            safe_url = connection_string.split('@')[1] if '@' in connection_string else connection_string
            print(f"   配置连接: ***@{safe_url}")
        
        self.db_manager = DBManager(connection_string)
        
        try:
            await self.db_manager.connect()
            print("✅ 数据库连接成功")
            return True
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            return False
    
    def setup_backtest_config(self):
        """设置回测配置"""
        print("\n📊 设置回测配置...")
        
        # 使用统一配置管理器获取回测配置
        bt_config = get_backtesting_config()
        
        # 显示配置信息
        print("   回测配置:")
        cash = bt_config.get('default_cash', 100000)
        commission = bt_config.get('default_commission', 0.001)
        start_date = bt_config.get('default_start_date', '2023-01-01')
        end_date = bt_config.get('default_end_date', '2023-12-31')
        table = bt_config.get('default_table', 'tushare_stock_daily')
        
        print(f"   - 初始资金: {cash:,}")
        print(f"   - 手续费率: {commission:.3%}")
        print(f"   - 回测周期: {start_date} 至 {end_date}")
        print(f"   - 数据表: {table}")
        
        return {
            'cash': cash,
            'commission': commission,
            'start_date': datetime.strptime(start_date, '%Y-%m-%d').date(),
            'end_date': datetime.strptime(end_date, '%Y-%m-%d').date(),
            'table': table
        }
    
    def create_cerebro(self, config):
        """创建backtrader引擎"""
        print("\n🧠 创建Cerebro引擎...")
        
        # 直接使用backtrader的Cerebro - 我们的价值在于数据源
        self.cerebro = bt.Cerebro()
        
        # 设置资金和手续费
        self.cerebro.broker.setcash(config['cash'])
        self.cerebro.broker.setcommission(commission=config['commission'])
        
        # 添加分析器
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        
        print("✅ Cerebro引擎配置完成")
    
    def add_data_feeds(self, config, stocks):
        """添加数据源"""
        print(f"\n📈 添加数据源...")
        
        for i, ts_code in enumerate(stocks):
            try:
                # 我们唯一的价值：PostgreSQL数据源
                data_feed = PostgreSQLDataFeed(
                    ts_code=ts_code,
                    db_manager=self.db_manager,
                    start_date=config['start_date'],
                    end_date=config['end_date'],
                    table_name=config['table'],
                    name=f'stock_{i}_{ts_code[:6]}'
                )
                self.cerebro.adddata(data_feed)
                print(f"   ✅ {ts_code}")
                
            except Exception as e:
                print(f"   ❌ {ts_code}: {e}")
                return False
        
        return True
    
    def add_strategy(self):
        """添加策略"""
        print("\n📈 添加交易策略...")
        
        # 直接使用backtrader添加策略
        self.cerebro.addstrategy(
            DualMovingAverageStrategy,
            fast_period=5,
            slow_period=20,
            printlog=False  # 关闭详细日志
        )
        
        print("   ✅ 双均线策略已添加")
    
    def run_backtest(self):
        """运行回测"""
        print("\n🚀 开始回测...")
        print("   这可能需要一些时间，请稍候...")
        
        initial_value = self.cerebro.broker.getvalue()
        print(f"   初始资金: {initial_value:,.2f}")
        
        start_time = datetime.now()
        
        try:
            # 直接使用backtrader运行
            results = self.cerebro.run()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"✅ 回测完成，耗时: {duration.total_seconds():.1f}秒")
            return results[0]  # 返回策略实例
            
        except Exception as e:
            print(f"❌ 回测失败: {e}")
            return None
    
    def analyze_results(self, strategy):
        """分析回测结果"""
        print("\n📊 回测结果分析")
        print("=" * 50)
        
        final_value = self.cerebro.broker.getvalue()
        initial_value = get_backtesting_config().get('default_cash', 100000)
        total_return = (final_value - initial_value) / initial_value
        
        print(f"初始资金: {initial_value:,.2f}")
        print(f"最终价值: {final_value:,.2f}")
        print(f"总收益率: {total_return:.2%}")
        print(f"绝对收益: {final_value - initial_value:,.2f}")
        
        # 分析器结果
        try:
            sharpe = strategy.analyzers.sharpe.get_analysis()
            drawdown = strategy.analyzers.drawdown.get_analysis()
            returns = strategy.analyzers.returns.get_analysis()
            trades = strategy.analyzers.trades.get_analysis()
            
            print(f"\n📈 风险指标:")
            print(f"夏普比率: {sharpe.get('sharperatio', 0):.3f}")
            print(f"最大回撤: {drawdown.get('max', {}).get('drawdown', 0):.2%}")
            
            print(f"\n🎯 交易统计:")
            total_trades = trades.get('total', {}).get('total', 0)
            won_trades = trades.get('won', {}).get('total', 0)
            lost_trades = trades.get('lost', {}).get('total', 0)
            
            print(f"总交易次数: {total_trades}")
            print(f"盈利交易: {won_trades}")
            print(f"亏损交易: {lost_trades}")
            
            if total_trades > 0:
                win_rate = won_trades / total_trades
                print(f"胜率: {win_rate:.1%}")
                
        except Exception as e:
            print(f"⚠️  部分分析结果获取失败: {e}")
    
    async def cleanup(self):
        """清理资源"""
        if self.db_manager:
            await self.db_manager.close()
            print("🔒 数据库连接已关闭")


async def main():
    """主函数"""
    print("🎯 AlphaHome 回测系统演示")
    print("=" * 50)
    print("本示例展示统一配置管理器的使用")
    print()
    
    demo = BacktestDemo()
    
    try:
        # 1. 设置数据库
        if not await demo.setup_database():
            print("无法连接数据库，演示结束")
            return
        
        # 2. 配置回测参数
        config = demo.setup_backtest_config()
        
        # 3. 创建回测引擎
        demo.create_cerebro(config)
        
        # 4. 添加数据源
        stocks = ['000001.SZ', '000002.SZ']  # 平安银行、万科A
        if not demo.add_data_feeds(config, stocks):
            print("数据源添加失败，演示结束")
            return
            
        # 5. 添加策略
        demo.add_strategy()
        
        # 6. 运行回测
        strategy = demo.run_backtest()
        if not strategy:
            print("回测失败，演示结束")
            return
            
        # 7. 分析结果
        demo.analyze_results(strategy)
        
        print("\n🎉 演示完成！")
        print("\n💡 提示:")
        print("   - 配置文件位置由ConfigManager自动管理")
        print("   - 支持环境变量回退")
        print("   - 各模块配置统一管理")
        print("   - 直接使用backtrader，无多余wrapper")
        
    except KeyboardInterrupt:
        print("\n⏹️  用户中断演示")
    except Exception as e:
        print(f"\n❌ 演示过程中发生错误: {e}")
    finally:
        await demo.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 