#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修正版回测演示 - 使用同步DBManager

特点：
1. 使用DBManager的同步方法
2. 完全避免异步冲突问题
3. 简洁的实现
4. 统一配置管理
"""

import sys
import os
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
    get_backtesting_config
)


def main():
    """主函数 - 完全同步的回测演示"""
    print("🎯 AlphaHome 修正版回测演示")
    print("=" * 50)
    print("使用同步DBManager，解决异步冲突问题")
    print()
    
    try:
        # 1. 获取配置
        print("📋 加载配置...")
        connection_string = get_database_url()
        if not connection_string:
            print("⚠️  未找到数据库配置")
            return
        
        bt_config = get_backtesting_config()
        cash = bt_config.get('default_cash', 100000)
        commission = bt_config.get('default_commission', 0.001)
        start_date = datetime.strptime(bt_config.get('default_start_date', '2023-01-01'), '%Y-%m-%d').date()
        end_date = datetime.strptime(bt_config.get('default_end_date', '2023-12-31'), '%Y-%m-%d').date()
        table = bt_config.get('default_table', 'tushare_stock_daily')
        
        print(f"   初始资金: {cash:,}")
        print(f"   手续费率: {commission:.3%}")
        print(f"   回测周期: {start_date} 至 {end_date}")
        
        # 2. 创建数据库管理器
        print("\n🔧 创建数据库连接...")
        db_manager = DBManager(connection_string)
        db_manager.connect_sync()  # 使用同步连接
        print("✅ 数据库连接成功")
        
        # 3. 创建Cerebro引擎
        print("\n🧠 创建回测引擎...")
        cerebro = bt.Cerebro()
        cerebro.broker.setcash(cash)
        cerebro.broker.setcommission(commission=commission)
        
        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        
        # 4. 添加数据源
        print("\n📈 添加数据源...")
        ts_code = '000001.SZ'  # 平安银行
        
        # 检查数据是否存在
        count_sql = f"""
        SELECT COUNT(*) as count 
        FROM {table} 
        WHERE ts_code = $1 AND trade_date BETWEEN $2 AND $3
        """
        result = db_manager.fetch_one_sync(count_sql, ts_code, start_date, end_date)
        count = result['count'] if result else 0
        
        if count == 0:
            print(f"❌ {ts_code}: 未找到数据")
            return
        
        print(f"✅ {ts_code}: 找到 {count} 条数据记录")
        
        # 创建数据源 - 现在使用同步方法
        data_feed = PostgreSQLDataFeed(
            ts_code=ts_code,
            db_manager=db_manager,
            start_date=start_date,
            end_date=end_date,
            table_name=table,
            name=f'stock_{ts_code[:6]}'
        )
        
        cerebro.adddata(data_feed)
        
        # 5. 添加策略
        print("\n📈 添加交易策略...")
        cerebro.addstrategy(
            DualMovingAverageStrategy,
            fast_period=5,
            slow_period=20,
            printlog=False
        )
        
        # 6. 运行回测
        print("\n🚀 开始回测...")
        initial_value = cerebro.broker.getvalue()
        print(f"   初始资金: {initial_value:,.2f}")
        
        start_time = datetime.now()
        results = cerebro.run()
        end_time = datetime.now()
        
        duration = end_time - start_time
        print(f"✅ 回测完成，耗时: {duration.total_seconds():.1f}秒")
        
        # 7. 分析结果
        print("\n📊 回测结果分析")
        print("=" * 50)
        
        strategy = results[0]
        final_value = cerebro.broker.getvalue()
        total_return = (final_value - initial_value) / initial_value
        
        print(f"初始资金: {initial_value:,.2f}")
        print(f"最终价值: {final_value:,.2f}")
        print(f"总收益率: {total_return:.2%}")
        print(f"绝对收益: {final_value - initial_value:,.2f}")
        
        # 分析器结果
        try:
            sharpe = strategy.analyzers.sharpe.get_analysis()
            drawdown = strategy.analyzers.drawdown.get_analysis()
            trades = strategy.analyzers.trades.get_analysis()
            
            print(f"\n📈 风险指标:")
            sharpe_ratio = sharpe.get('sharperatio', 0)
            if sharpe_ratio:
                print(f"夏普比率: {sharpe_ratio:.3f}")
            else:
                print("夏普比率: 无法计算")
                
            max_drawdown = drawdown.get('max', {}).get('drawdown', 0)
            if max_drawdown:
                print(f"最大回撤: {max_drawdown:.2%}")
            else:
                print("最大回撤: 0.00%")
            
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
            else:
                print("胜率: 无交易")
                
        except Exception as e:
            print(f"⚠️  分析结果获取部分失败: {e}")
        
        # 8. 清理资源
        db_manager.close_sync()
        print("\n🔒 数据库连接已关闭")
        
        print("\n🎉 演示完成！")
        print("\n💡 解决方案特点:")
        print("   ✅ 同步DBManager方法")
        print("   ✅ 避免异步冲突")
        print("   ✅ 统一配置管理")
        print("   ✅ 轻量级架构")
        print("   ✅ 完全兼容backtrader")
        
    except KeyboardInterrupt:
        print("\n⏹️  用户中断演示")
    except Exception as e:
        print(f"\n❌ 演示过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 