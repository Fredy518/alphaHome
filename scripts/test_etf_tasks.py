#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试新创建的ETF数据采集任务
验证 tushare_fund_etf_basic 和 tushare_fund_etf_index 任务是否能够正确工作
"""

import asyncio
import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.task_system.task_factory import UnifiedTaskFactory


async def test_etf_tasks():
    """测试ETF任务创建和配置"""
    print("=" * 80)
    print("测试新创建的ETF数据采集任务")
    print("=" * 80)
    
    try:
        # 初始化任务工厂
        await UnifiedTaskFactory.initialize()
        task_factory = UnifiedTaskFactory
        
        # 测试任务列表
        etf_tasks = [
            "tushare_fund_etf_basic",
            "tushare_fund_etf_index",
        ]
        
        print(f"将测试 {len(etf_tasks)} 个ETF任务\n")
        
        for task_name in etf_tasks:
            print(f"📊 测试任务: {task_name}")
            print("-" * 60)
            
            try:
                # 创建任务实例
                task = await task_factory.create_task_instance(task_name)
                
                # 检查基本属性
                print(f"  ✅ 任务名称: {task.name}")
                print(f"  ✅ 任务描述: {task.description}")
                print(f"  ✅ 表名: {task.table_name}")
                print(f"  ✅ 主键: {task.primary_keys}")
                print(f"  ✅ API名称: {task.api_name}")
                print(f"  ✅ 数据源: {task.data_source}")
                
                # 检查字段配置
                if hasattr(task, 'fields') and task.fields:
                    print(f"  ✅ API字段数量: {len(task.fields)}")
                    print(f"     字段列表: {', '.join(task.fields[:5])}{'...' if len(task.fields) > 5 else ''}")
                
                # 检查表结构定义
                if hasattr(task, 'schema_def') and task.schema_def:
                    print(f"  ✅ 表结构字段数量: {len(task.schema_def)}")
                
                # 检查验证规则
                if hasattr(task, 'validations') and task.validations:
                    validation_count = len(task.validations)
                    print(f"  ✅ 验证规则数量: {validation_count}")
                    
                    # 检查验证规则格式
                    new_format_count = 0
                    for validation in task.validations:
                        if isinstance(validation, tuple) and len(validation) == 2:
                            func, desc = validation
                            if callable(func) and isinstance(desc, str):
                                new_format_count += 1
                    
                    if new_format_count == validation_count:
                        print(f"  ✅ 验证规则格式: 全部为新格式（带描述）")
                        # 显示前3个验证规则的描述
                        print(f"     验证规则示例:")
                        for i, validation in enumerate(task.validations[:3]):
                            if isinstance(validation, tuple):
                                _, desc = validation
                                print(f"       {i+1}. {desc}")
                        if validation_count > 3:
                            print(f"       ... 还有 {validation_count - 3} 个验证规则")
                    else:
                        print(f"  ⚠️  验证规则格式: 部分为旧格式")
                else:
                    print(f"  ⚠️  未定义验证规则")
                
                # 检查索引配置
                if hasattr(task, 'indexes') and task.indexes:
                    print(f"  ✅ 索引数量: {len(task.indexes)}")
                
                # 检查数据类型转换
                if hasattr(task, 'transformations') and task.transformations:
                    print(f"  ✅ 数据转换规则数量: {len(task.transformations)}")
                
                # 测试批处理列表生成
                try:
                    batch_list = await task.get_batch_list(
                        start_date="20240101",
                        end_date="20240105"
                    )
                    print(f"  ✅ 批处理列表生成成功: {len(batch_list)} 个批次")
                    if batch_list:
                        print(f"     首个批次参数: {batch_list[0]}")
                except Exception as e:
                    print(f"  ⚠️  批处理列表生成失败: {e}")
                
                print(f"  ✅ 任务 {task_name} 创建和配置检查完成")
                
            except Exception as e:
                print(f"  ❌ 任务 {task_name} 创建失败: {e}")
                import traceback
                traceback.print_exc()
            
            print()
        
        print("=" * 80)
        print("ETF任务测试完成")
        print("=" * 80)
        
        # 测试任务注册情况
        print("\n📋 检查任务注册情况:")
        registry = task_factory._task_registry
        
        for task_name in etf_tasks:
            if task_name in registry:
                task_class = registry[task_name]
                print(f"  ✅ {task_name} - 已注册 ({task_class.__name__})")
            else:
                print(f"  ❌ {task_name} - 未注册")
        
        print(f"\n📊 任务工厂统计:")
        print(f"  总注册任务数: {len(registry)}")
        fund_tasks = [name for name in registry.keys() if 'fund' in name]
        print(f"  基金相关任务数: {len(fund_tasks)}")
        etf_registered = [name for name in registry.keys() if 'etf' in name]
        print(f"  ETF相关任务数: {len(etf_registered)}")
        
        if len(etf_registered) >= 2:
            print(f"\n🎉 ETF任务创建成功！已注册的ETF任务: {', '.join(etf_registered)}")
        else:
            print(f"\n⚠️  ETF任务注册不完整，请检查导入配置")
        
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_etf_tasks())
