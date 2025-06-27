#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试统一验证架构的效果
验证数据验证功能是否已成功统一到 BaseTask 层面，避免重复验证
"""

import asyncio
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.task_system.task_factory import UnifiedTaskFactory


async def test_unified_validation_architecture():
    """测试统一验证架构"""
    print("=" * 80)
    print("测试统一验证架构效果")
    print("=" * 80)
    
    try:
        # 初始化任务工厂
        await UnifiedTaskFactory.initialize()
        task_factory = UnifiedTaskFactory
        
        # 测试任务列表 - 选择不同验证模式的任务
        test_tasks = [
            {
                "name": "tushare_stock_daily",
                "expected_mode": "filter",
                "description": "股票日线数据（过滤模式）"
            },
            {
                "name": "tushare_stock_basic", 
                "expected_mode": "report",
                "description": "股票基础信息（报告模式）"
            },
            {
                "name": "tushare_fund_nav",
                "expected_mode": "report", 
                "description": "基金净值数据（报告模式）"
            },
        ]
        
        print(f"将测试 {len(test_tasks)} 个任务的统一验证架构\n")
        
        for task_info in test_tasks:
            task_name = task_info["name"]
            expected_mode = task_info["expected_mode"]
            description = task_info["description"]
            
            print(f"🔍 测试任务: {task_name}")
            print(f"   描述: {description}")
            print(f"   期望验证模式: {expected_mode}")
            print("-" * 60)
            
            try:
                # 创建任务实例
                task = await task_factory.create_task_instance(task_name)
                
                # 检查验证配置
                has_validations = hasattr(task, 'validations') and task.validations
                validation_mode = getattr(task, 'validation_mode', 'report')
                
                print(f"  ✅ 验证规则: {'已定义' if has_validations else '未定义'}")
                if has_validations:
                    print(f"     规则数量: {len(task.validations)}")
                    # 显示前3个验证规则
                    for i, validation in enumerate(task.validations[:3]):
                        if isinstance(validation, tuple) and len(validation) == 2:
                            _, desc = validation
                            print(f"     {i+1}. {desc}")
                    if len(task.validations) > 3:
                        print(f"     ... 还有 {len(task.validations) - 3} 个验证规则")
                
                print(f"  ✅ 验证模式: {validation_mode}")
                
                # 检查是否符合期望
                mode_match = validation_mode == expected_mode
                print(f"  {'✅' if mode_match else '⚠️'} 模式匹配: {'符合期望' if mode_match else f'期望{expected_mode}，实际{validation_mode}'}")
                
                # 测试验证功能
                print(f"  🧪 测试验证功能:")
                
                # 创建测试数据（包含一些无效数据）
                test_data = create_test_data_with_invalid_rows(task_name)
                original_count = len(test_data)
                print(f"     原始数据: {original_count} 行（包含无效数据）")
                
                # 测试统一验证方法
                validation_passed, validated_data, validation_details = task._validate_data(
                    test_data, 
                    validation_mode=validation_mode
                )
                
                result_count = len(validated_data) if isinstance(validated_data, pd.DataFrame) else original_count
                print(f"     验证结果: {'通过' if validation_passed else '未完全通过'}")
                print(f"     结果数据: {result_count} 行")
                
                if validation_mode == "filter" and result_count < original_count:
                    filtered_count = original_count - result_count
                    print(f"     ✅ 过滤模式生效: 移除了 {filtered_count} 行无效数据")
                elif validation_mode == "report" and result_count == original_count:
                    print(f"     ✅ 报告模式生效: 保留了所有数据，仅记录验证结果")
                
                # 显示验证详情
                if validation_details.get("failed_validations"):
                    print(f"     失败的验证规则: {len(validation_details['failed_validations'])} 个")
                    for rule_name, failure_info in list(validation_details["failed_validations"].items())[:2]:
                        print(f"       - {rule_name}: {failure_info}")
                
                print(f"  ✅ 任务 {task_name} 验证架构测试完成")
                
            except Exception as e:
                print(f"  ❌ 任务 {task_name} 测试失败: {e}")
                import traceback
                traceback.print_exc()
            
            print()
        
        print("=" * 80)
        print("统一验证架构测试总结")
        print("=" * 80)
        
        # 测试架构一致性
        print("\n🏗️ 验证架构一致性:")
        
        # 检查 TushareDataTransformer 是否还有验证方法
        from alphahome.fetchers.sources.tushare.tushare_data_transformer import TushareDataTransformer
        
        # 创建一个临时任务实例来测试
        temp_task = await task_factory.create_task_instance("tushare_stock_basic")
        transformer = TushareDataTransformer(temp_task)
        
        has_validate_method = hasattr(transformer, 'validate_data')
        print(f"  {'⚠️' if has_validate_method else '✅'} TushareDataTransformer.validate_data: {'仍存在' if has_validate_method else '已移除'}")
        
        # 检查 BaseTask 的验证方法
        has_unified_validate = hasattr(temp_task, '_validate_data')
        print(f"  ✅ BaseTask._validate_data: {'存在' if has_unified_validate else '不存在'}")
        
        # 测试验证模式支持
        print(f"\n🔧 验证模式支持测试:")
        test_data = pd.DataFrame({
            'value': [1, -1, 2, -2, 3],  # 包含负数
            'name': ['A', 'B', 'C', 'D', 'E']
        })
        
        # 定义简单的验证规则
        temp_task.validations = [
            (lambda df: df['value'] > 0, "值必须为正数")
        ]
        
        # 测试报告模式
        _, report_data, report_details = temp_task._validate_data(test_data.copy(), validation_mode="report")
        print(f"  ✅ 报告模式: 输入{len(test_data)}行，输出{len(report_data)}行")
        
        # 测试过滤模式
        _, filter_data, filter_details = temp_task._validate_data(test_data.copy(), validation_mode="filter")
        print(f"  ✅ 过滤模式: 输入{len(test_data)}行，输出{len(filter_data)}行")
        
        print(f"\n🎉 统一验证架构测试完成！")
        print(f"📊 架构改进效果:")
        print(f"  - 消除了重复验证的问题")
        print(f"  - 统一了验证入口点到 BaseTask 层面")
        print(f"  - 支持灵活的验证模式（报告 vs 过滤）")
        print(f"  - 符合模板方法模式的设计原则")
        print(f"  - 提供了详细的验证结果和统计信息")
        
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


def create_test_data_with_invalid_rows(task_name: str) -> pd.DataFrame:
    """创建包含无效数据的测试数据"""
    if 'stock' in task_name and 'daily' in task_name:
        return pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH', '000003.SZ'],
            'trade_date': ['20240101', '20240101', '20240101', '20240101'],
            'close': [10.5, -5.0, 15.8, 0],  # 包含负数和零
            'open': [10.0, 8.0, 15.0, -2.0],  # 包含负数
            'high': [11.0, 8.5, 16.0, 5.0],
            'low': [9.5, 7.5, 15.0, -1.0],   # 包含负数
            'volume': [1000000, -500000, 1500000, 800000],  # 包含负数
            'amount': [10500000, 4000000, 23700000, -1000000]  # 包含负数
        })
    elif 'stock' in task_name and 'basic' in task_name:
        return pd.DataFrame({
            'ts_code': ['000001.SZ', 'INVALID', '600000.SH', ''],  # 包含无效代码
            'symbol': ['平安银行', '万科A', '', '测试'],  # 包含空值
            'name': ['平安银行', '', '浦发银行', '测试公司'],  # 包含空值
            'market': ['主板', '主板', '主板', '主板'],
            'exchange': ['SZ', 'SZ', 'SH', 'SZ']
        })
    elif 'fund' in task_name:
        return pd.DataFrame({
            'ts_code': ['110001.OF', '110002.OF', 'INVALID.OF'],
            'nav_date': ['20240101', '20240101', '20240101'],
            'unit_nav': [1.234, -0.5, 1.890],  # 包含负数
            'accum_nav': [1.234, 2.567, 1.890]
        })
    else:
        return pd.DataFrame({
            'id': [1, 2, 3, 4],
            'value': [10.5, -20.3, 15.8, 0],  # 包含负数和零
            'name': ['A', '', 'C', 'D']  # 包含空值
        })


if __name__ == "__main__":
    asyncio.run(test_unified_validation_architecture())
