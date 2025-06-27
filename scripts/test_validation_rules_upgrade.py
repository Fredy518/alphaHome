#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试验证规则升级效果的脚本
验证所有数据采集任务是否已更新为新的验证规则格式：(验证函数, "描述文本")
"""

import asyncio
import sys
import os
from pathlib import Path
from typing import List, Tuple, Union, Callable

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.task_system.task_factory import UnifiedTaskFactory


async def test_validation_rules_upgrade():
    """测试验证规则升级效果"""
    print("=" * 80)
    print("测试验证规则升级效果")
    print("=" * 80)
    
    try:
        # 初始化任务工厂
        await UnifiedTaskFactory.initialize()
        task_factory = UnifiedTaskFactory
        
        # 测试任务列表 - 按类别分组
        test_tasks = {
            "股票数据": [
                "tushare_stock_daily",
                "tushare_stock_basic", 
                "tushare_stock_adjfactor",
                "tushare_stock_dailybasic",
            ],
            "港股数据": [
                "tushare_hk_daily",
            ],
            "指数数据": [
                "tushare_index_basic",
                "tushare_index_cidaily",
                "tushare_index_swdaily",
                "tushare_index_cimember",
                "tushare_index_swmember",
            ],
            "宏观数据": [
                "tushare_macro_cpi",
                "tushare_macro_shibor",
                "tushare_macro_hibor",
            ],
            "期权数据": [
                "tushare_option_basic",
            ],
            "基金数据": [
                "tushare_fund_basic",
                "tushare_fund_nav",
                "tushare_fund_daily",
                "tushare_fund_adjfactor",
                "tushare_fund_share",
            ],
            "财务数据": [
                "tushare_fina_indicator",
                "tushare_fina_income",
                "tushare_fina_express",
                "tushare_fina_balancesheet",
            ]
        }
        
        total_tasks = sum(len(tasks) for tasks in test_tasks.values())
        print(f"将测试 {total_tasks} 个任务的验证规则升级效果\n")
        
        success_count = 0
        failed_tasks = []
        
        for category, task_names in test_tasks.items():
            print(f"📂 {category}")
            print("-" * 60)
            
            for task_name in task_names:
                try:
                    # 创建任务实例
                    task = await task_factory.create_task_instance(task_name)
                    
                    # 检查任务是否有 validations 列表
                    if hasattr(task, 'validations') and task.validations:
                        validation_count = len(task.validations)
                        
                        # 检查验证规则格式
                        new_format_count = 0
                        old_format_count = 0
                        
                        for i, validation in enumerate(task.validations):
                            if isinstance(validation, tuple) and len(validation) == 2:
                                # 新格式：(函数, 描述)
                                func, desc = validation
                                if callable(func) and isinstance(desc, str):
                                    new_format_count += 1
                                else:
                                    print(f"    ⚠️  验证规则 {i+1} 格式异常")
                            elif callable(validation):
                                # 旧格式：lambda 函数
                                old_format_count += 1
                            else:
                                print(f"    ❌ 验证规则 {i+1} 格式错误")
                        
                        if new_format_count == validation_count:
                            print(f"  ✅ {task_name:<30} - {validation_count} 个验证规则（全部为新格式）")
                            success_count += 1
                        elif old_format_count > 0:
                            print(f"  ⚠️  {task_name:<30} - {validation_count} 个验证规则（{old_format_count} 个旧格式，{new_format_count} 个新格式）")
                            failed_tasks.append((task_name, "部分旧格式"))
                        else:
                            print(f"  ❌ {task_name:<30} - {validation_count} 个验证规则（格式异常）")
                            failed_tasks.append((task_name, "格式异常"))
                            
                        # 显示前3个验证规则的描述
                        if new_format_count > 0:
                            print(f"    📝 验证规则示例:")
                            for i, validation in enumerate(task.validations[:3]):
                                if isinstance(validation, tuple) and len(validation) == 2:
                                    _, desc = validation
                                    print(f"       {i+1}. {desc}")
                            if validation_count > 3:
                                print(f"       ... 还有 {validation_count - 3} 个验证规则")
                    else:
                        print(f"  ❌ {task_name:<30} - 未定义验证规则")
                        failed_tasks.append((task_name, "无验证规则"))
                    
                    print()
                    
                except Exception as e:
                    print(f"  ❌ {task_name:<30} - 创建任务失败: {e}")
                    failed_tasks.append((task_name, f"创建失败: {e}"))
                    print()
                    continue
            
            print()
        
        # 输出总结
        print("=" * 80)
        print("验证规则升级测试总结")
        print("=" * 80)
        print(f"✅ 成功升级: {success_count}/{total_tasks} 个任务")
        print(f"❌ 需要处理: {len(failed_tasks)} 个任务")
        
        if failed_tasks:
            print("\n需要处理的任务:")
            for task_name, reason in failed_tasks:
                print(f"  - {task_name}: {reason}")
        
        print(f"\n升级完成率: {success_count/total_tasks*100:.1f}%")
        
        if success_count == total_tasks:
            print("\n🎉 所有任务的验证规则已成功升级为新格式！")
        else:
            print(f"\n⚠️  还有 {total_tasks - success_count} 个任务需要升级")
        
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_validation_rules_upgrade())
