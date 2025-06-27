#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试数据验证流程改进效果的脚本
验证以下改进是否生效：
1. 删除死代码 validate_data 方法
2. 完善 validations 列表
3. 增强验证可见性
4. 避免重复验证
"""

import asyncio
import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import ConfigManager


async def test_validation_improvements():
    """测试验证流程改进效果"""
    print("=" * 60)
    print("测试数据验证流程改进效果")
    print("=" * 60)
    
    try:
        # 初始化任务工厂
        await UnifiedTaskFactory.initialize()
        task_factory = UnifiedTaskFactory
        
        # 测试任务列表 - 选择一些已经改进的任务
        test_tasks = [
            "tushare_stock_daily",      # 已有完善的 validations
            "tushare_stock_basic",      # 已有完善的 validations
            "tushare_hk_daily",         # 新增的 validations
            "tushare_index_basic",      # 新增的 validations
            "tushare_macro_cpi",        # 新增的 validations
        ]
        
        print(f"将测试以下 {len(test_tasks)} 个任务的验证改进效果：")
        for task_name in test_tasks:
            print(f"  - {task_name}")
        print()
        
        for task_name in test_tasks:
            print(f"测试任务: {task_name}")
            print("-" * 40)
            
            try:
                # 创建任务实例
                task = await task_factory.create_task_instance(task_name)
                
                # 检查任务是否有 validations 列表
                if hasattr(task, 'validations') and task.validations:
                    print(f"✅ 任务 {task_name} 已定义 {len(task.validations)} 个验证规则")
                    
                    # 显示验证规则的简要信息
                    for i, validation in enumerate(task.validations, 1):
                        try:
                            import inspect
                            source = inspect.getsource(validation).strip()
                            # 提取 lambda 函数的主要内容
                            if 'lambda df:' in source:
                                rule_desc = source.split('lambda df:')[1].strip().rstrip(',')
                                if len(rule_desc) > 50:
                                    rule_desc = rule_desc[:47] + "..."
                                print(f"    {i}. {rule_desc}")
                        except:
                            print(f"    {i}. 验证规则_{i}")
                else:
                    print(f"⚠️  任务 {task_name} 未定义验证规则")
                
                # 检查任务是否还有 validate_data 方法（应该已被删除）
                if hasattr(task, 'validate_data'):
                    print(f"❌ 任务 {task_name} 仍然包含 validate_data 方法（应该已被删除）")
                else:
                    print(f"✅ 任务 {task_name} 已删除 validate_data 方法")
                
                print()
                
            except Exception as e:
                print(f"❌ 创建任务 {task_name} 时出错: {e}")
                print()
                continue
        
        print("=" * 60)
        print("验证改进测试完成")
        print("=" * 60)
        
        # 测试验证日志增强效果
        print("\n测试验证日志增强效果...")
        print("-" * 40)
        
        try:
            # 选择一个有验证规则的任务进行实际验证测试
            task = await task_factory.create_task_instance("tushare_stock_basic")
            
            # 创建一些测试数据
            import pandas as pd
            test_data = pd.DataFrame({
                'ts_code': ['000001.SZ', '000002.SZ', '', None],  # 包含空值测试验证
                'symbol': ['平安银行', '万科A', '测试', ''],
                'name': ['平安银行', '万科A', '', None],
                'market': ['主板', '主板', '主板', '主板'],
                'exchange': ['SZ', 'SZ', 'SZ', 'SZ'],
            })
            
            print(f"使用测试数据验证任务 {task.name}:")
            print(f"测试数据行数: {len(test_data)}")
            print("测试数据包含空值和无效数据，用于测试验证规则...")
            
            # 执行验证
            validation_result = task._validate_data(test_data)
            
            if validation_result:
                print("✅ 验证通过")
            else:
                print("⚠️  验证未完全通过（这是预期的，因为测试数据包含无效值）")
            
        except Exception as e:
            print(f"❌ 验证测试时出错: {e}")
        
        print("\n测试完成！")
        
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_validation_improvements())
