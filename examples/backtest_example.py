#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AlphaHome 回测框架使用示例

本示例演示如何使用 alphahome 回测模块进行策略回测。
包括双均线策略和买入持有策略的完整示例。
"""

import sys
import os
import asyncio
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alphahome.common.db_manager import DBManager
from alphahome.backtest import AlphaHomeBacktestRunner
from alphahome.backtest.strategies.dual_moving_average import create_dual_moving_average_strategy
from alphahome.backtest.strategies.buy_and_hold import create_buy_and_hold_strategy
from alphahome.backtest.config import BacktestConfig, ConfigTemplates


async def main():
    """主函数"""
    print("=== AlphaHome 回测框架示例 ===")
    
    # 1. 数据库连接设置
    # 请根据实际情况修改数据库连接字符串
    db_connection_string = "postgresql://username:password@localhost:5432/alphahome"
    
    # 创建数据库管理器
    db_manager = DBManager(db_connection_string)
    
    try:
        # 连接数据库
        await db_manager.connect()
        print("✓ 数据库连接成功")
        
        # 2. 创建回测运行器
        runner = AlphaHomeBacktestRunner(db_manager)
        print("✓ 回测运行器创建成功")
        
        # 3. 示例1：使用配置模板进行双均线策略回测
        print("\n--- 示例1：双均线策略回测 ---")
        await run_dual_moving_average_example(runner)
        
        # 4. 示例2：买入持有策略回测
        print("\n--- 示例2：买入持有策略回测 ---")
        await run_buy_and_hold_example(runner)
        
        # 5. 示例3：自定义参数的策略回测
        print("\n--- 示例3：自定义参数回测 ---")
        await run_custom_strategy_example(runner)
        
        # 6. 示例4：获取价格数据（不进行回测）
        print("\n--- 示例4：直接获取价格数据 ---")
        await get_price_data_example(runner)
        
    except Exception as e:
        print(f"❌ 示例执行失败: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 关闭数据库连接
        await db_manager.close()
        print("✓ 数据库连接已关闭")


async def run_dual_moving_average_example(runner: AlphaHomeBacktestRunner):
    """运行双均线策略示例"""
    try:
        # 使用配置模板
        config = ConfigTemplates.dual_moving_average()
        
        # 可以修改配置参数
        config.start_date = "2022-01-01"
        config.end_date = "2023-12-31"
        config.initial_capital = 100000.0
        
        print(f"回测时间范围: {config.start_date} 到 {config.end_date}")
        print(f"初始资金: {config.initial_capital:,.0f} 元")
        
        # 创建策略函数
        initialize, handle_data = create_dual_moving_average_strategy(
            stocks=["000001.SZ"],  # 平安银行
            short_window=5,
            long_window=20
        )
        
        # 运行回测
        result = runner.run_backtest(
            strategy_func=handle_data,
            start_date=config.start_date,
            end_date=config.end_date,
            initial_capital=config.initial_capital,
            initialize=initialize
        )
        
        # 显示结果摘要
        print("回测完成！")
        print(f"回测期间收益率: {result['returns'].iloc[-1]:.2%}")
        print(f"最大回撤: {result['max_drawdown'].min():.2%}")
        print(f"夏普比率: {result['sharpe'].iloc[-1]:.3f}")
        
    except Exception as e:
        print(f"双均线策略回测失败: {str(e)}")


async def run_buy_and_hold_example(runner: AlphaHomeBacktestRunner):
    """运行买入持有策略示例"""
    try:
        # 创建策略函数
        initialize, handle_data = create_buy_and_hold_strategy(
            stocks=["000001.SZ", "000002.SZ"],  # 平安银行、万科A
            weights={"000001.SZ": 0.6, "000002.SZ": 0.4}  # 6:4 权重分配
        )
        
        # 运行回测
        result = runner.run_backtest(
            strategy_func=handle_data,
            start_date="2022-01-01",
            end_date="2023-12-31",
            initial_capital=100000.0,
            initialize=initialize
        )
        
        print("买入持有策略回测完成！")
        print(f"回测期间收益率: {result['returns'].iloc[-1]:.2%}")
        
    except Exception as e:
        print(f"买入持有策略回测失败: {str(e)}")


async def run_custom_strategy_example(runner: AlphaHomeBacktestRunner):
    """运行自定义参数策略示例"""
    try:
        # 自定义策略参数
        def custom_strategy(context, data):
            """自定义简单策略：定期调仓"""
            if not hasattr(context, 'rebalance_count'):
                context.rebalance_count = 0
            
            # 每20个交易日调仓一次
            context.rebalance_count += 1
            if context.rebalance_count % 20 == 0:
                # 获取当前价格
                stock = context.stocks[0]
                current_price = data.current(stock, 'close')
                
                # 简单的价格判断逻辑
                if current_price < 15:  # 低于15元时买入
                    from zipline.api import order_target_percent
                    order_target_percent(stock, 1.0)
                    print(f"买入 {stock.symbol}，价格: {current_price:.2f}")
                elif current_price > 20:  # 高于20元时卖出
                    from zipline.api import order_target_percent
                    order_target_percent(stock, 0.0)
                    print(f"卖出 {stock.symbol}，价格: {current_price:.2f}")
        
        def custom_initialize(context):
            """自定义初始化"""
            context.stocks = [runner.asset_finder.lookup_symbol('000001.SZ')]
            context.rebalance_count = 0
        
        # 运行回测
        result = runner.run_backtest(
            strategy_func=custom_strategy,
            start_date="2023-01-01",
            end_date="2023-06-30",
            initial_capital=50000.0,
            initialize=custom_initialize
        )
        
        print("自定义策略回测完成！")
        
    except Exception as e:
        print(f"自定义策略回测失败: {str(e)}")


async def get_price_data_example(runner: AlphaHomeBacktestRunner):
    """获取价格数据示例（不进行回测）"""
    try:
        # 获取多只股票的收盘价数据
        price_data = runner.get_price_data(
            symbols=["000001.SZ", "000002.SZ"],
            start_date="2023-01-01",
            end_date="2023-03-31",
            fields=["close"]
        )
        
        print("价格数据获取成功！")
        print(f"数据形状: {price_data.shape}")
        print("前5行数据:")
        print(price_data.head())
        
        # 获取可用股票代码
        available_symbols = runner.get_available_symbols(limit=10)
        print(f"\n前10个可用股票代码: {available_symbols}")
        
        # 获取特定股票信息
        stock_info = runner.get_asset_info("000001.SZ")
        if stock_info:
            print(f"\n000001.SZ 股票信息:")
            for key, value in stock_info.items():
                print(f"  {key}: {value}")
        
    except Exception as e:
        print(f"获取价格数据失败: {str(e)}")


def create_config_example():
    """创建配置文件示例"""
    print("\n=== 配置管理示例 ===")
    
    # 1. 使用预定义模板
    config = ConfigTemplates.dual_moving_average()
    print("使用双均线策略模板配置")
    
    # 2. 修改配置参数
    config.initial_capital = 200000.0
    config.strategy_params['short_window'] = 10
    config.strategy_params['long_window'] = 30
    
    # 3. 保存配置文件
    config_file = "my_backtest_config.json"
    config.to_json_file(config_file)
    print(f"配置已保存到: {config_file}")
    
    # 4. 从文件加载配置
    loaded_config = BacktestConfig.from_json_file(config_file)
    print("配置加载成功")
    
    # 5. 验证配置
    if loaded_config.validate():
        print("✓ 配置验证通过")
    else:
        print("❌ 配置验证失败")
    
    # 清理临时文件
    if os.path.exists(config_file):
        os.remove(config_file)


if __name__ == "__main__":
    # 运行配置示例（不需要数据库连接）
    create_config_example()
    
    # 运行回测示例（需要数据库连接）
    print("\n如果您已配置数据库连接，请取消下面代码的注释来运行回测示例：")
    print("# asyncio.run(main())")
    
    # 取消注释下面这行来运行完整示例
    # asyncio.run(main()) 