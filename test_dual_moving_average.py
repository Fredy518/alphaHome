#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双均线策略测试脚本
"""

import sys
import os
from datetime import datetime, date

# 确保能找到 alphahome 模块
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def test_dual_moving_average():
    """测试双均线策略"""
    try:
        # 导入必要的模块
        from alphahome.backtest.config.backtest_config import BacktestConfig
        from alphahome.backtest.strategies.dual_moving_average import create_dual_moving_average_strategy
        from alphahome.backtest.runners.alphahome_runner import AlphaHomeBacktestRunner
        
        print("开始测试双均线策略...")
        
        # 创建配置
        config = BacktestConfig(
            start_date=date(2023, 1, 1),
            end_date=date(2023, 12, 31),
            initial_capital=100000.0,
            symbols=['000001.SZ', '000002.SZ'],  # 平安银行、万科A
            benchmark_symbol='000001.SH'  # 上证指数
        )
        
        print(f"配置创建成功: {config}")
        
        # 创建策略
        strategy = create_dual_moving_average_strategy(
            short_window=10,
            long_window=30
        )
        
        print("双均线策略创建成功")
        
        # 创建回测运行器
        runner = AlphaHomeBacktestRunner()
        
        print("回测运行器创建成功")
        print("策略测试完成，所有组件都能正常导入和创建")
        
        return True
        
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_dual_moving_average()
    if success:
        print("\n✅ 双均线策略测试通过！")
    else:
        print("\n❌ 双均线策略测试失败！")
        sys.exit(1) 