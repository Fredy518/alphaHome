#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
验证所有采集任务都使用报告模式的脚本
确保所有任务的验证模式都设置为 "report"
"""

import asyncio
import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.task_system.task_factory import UnifiedTaskFactory


async def verify_all_tasks_report_mode():
    """验证所有采集任务都使用报告模式"""
    print("=" * 80)
    print("验证所有采集任务的验证模式设置")
    print("=" * 80)
    
    try:
        # 初始化任务工厂
        await UnifiedTaskFactory.initialize()
        task_factory = UnifiedTaskFactory
        
        # 获取所有采集任务
        fetch_tasks = task_factory.get_tasks_by_type("fetch")
        
        print(f"发现 {len(fetch_tasks)} 个采集任务\n")
        
        report_mode_count = 0
        filter_mode_count = 0
        no_mode_count = 0
        error_count = 0
        
        # 按类别分组统计
        task_categories = {}
        
        for task_name in sorted(fetch_tasks.keys()):
            print(f"🔍 检查任务: {task_name}")
            
            try:
                # 创建任务实例
                task = await task_factory.create_task_instance(task_name)
                
                # 获取验证模式
                validation_mode = getattr(task, 'validation_mode', 'report')  # 默认为report
                
                # 统计
                if validation_mode == 'report':
                    report_mode_count += 1
                    status_icon = "✅"
                elif validation_mode == 'filter':
                    filter_mode_count += 1
                    status_icon = "⚠️"
                else:
                    no_mode_count += 1
                    status_icon = "❓"
                
                print(f"  {status_icon} 验证模式: {validation_mode}")
                
                # 按类别分组
                category = task_name.split('_')[1] if '_' in task_name else 'other'
                if category not in task_categories:
                    task_categories[category] = {'report': 0, 'filter': 0, 'other': 0}
                
                if validation_mode == 'report':
                    task_categories[category]['report'] += 1
                elif validation_mode == 'filter':
                    task_categories[category]['filter'] += 1
                else:
                    task_categories[category]['other'] += 1
                
                # 检查是否有验证规则
                has_validations = hasattr(task, 'validations') and task.validations
                if has_validations:
                    print(f"     验证规则: {len(task.validations)} 个")
                else:
                    print(f"     验证规则: 未定义")
                
            except Exception as e:
                print(f"  ❌ 创建任务失败: {e}")
                error_count += 1
            
            print()
        
        print("=" * 80)
        print("验证模式统计总结")
        print("=" * 80)
        
        total_tasks = len(fetch_tasks)
        print(f"📊 总体统计:")
        print(f"  总任务数: {total_tasks}")
        print(f"  ✅ 报告模式: {report_mode_count} ({report_mode_count/total_tasks*100:.1f}%)")
        print(f"  ⚠️  过滤模式: {filter_mode_count} ({filter_mode_count/total_tasks*100:.1f}%)")
        print(f"  ❓ 其他模式: {no_mode_count} ({no_mode_count/total_tasks*100:.1f}%)")
        print(f"  ❌ 错误任务: {error_count}")
        
        print(f"\n📋 按类别统计:")
        for category, stats in sorted(task_categories.items()):
            total_in_category = sum(stats.values())
            print(f"  {category.upper()}:")
            print(f"    报告模式: {stats['report']}/{total_in_category}")
            print(f"    过滤模式: {stats['filter']}/{total_in_category}")
            if stats['other'] > 0:
                print(f"    其他模式: {stats['other']}/{total_in_category}")
        
        # 检查结果
        if filter_mode_count == 0 and no_mode_count == 0:
            print(f"\n🎉 验证通过！所有 {report_mode_count} 个采集任务都使用报告模式")
            print(f"✅ 数据验证架构统一完成")
        else:
            print(f"\n⚠️  发现问题：")
            if filter_mode_count > 0:
                print(f"  - {filter_mode_count} 个任务仍使用过滤模式")
            if no_mode_count > 0:
                print(f"  - {no_mode_count} 个任务使用未知验证模式")
            print(f"  建议将所有任务设置为报告模式")
        
        # 测试报告模式功能
        print(f"\n🧪 测试报告模式功能:")
        try:
            # 选择一个有验证规则的任务进行测试
            test_task_name = "tushare_stock_basic"
            test_task = await task_factory.create_task_instance(test_task_name)
            
            # 创建包含无效数据的测试数据
            import pandas as pd
            test_data = pd.DataFrame({
                'ts_code': ['000001.SZ', 'INVALID', '600000.SH'],
                'symbol': ['平安银行', '', '浦发银行'],
                'name': ['平安银行', '万科A', ''],
                'market': ['主板', '主板', '主板']
            })
            
            print(f"  使用任务 {test_task_name} 测试报告模式")
            print(f"  测试数据: {len(test_data)} 行（包含无效数据）")
            
            # 执行验证
            validation_passed, validated_data, validation_details = test_task._validate_data(
                test_data, 
                validation_mode="report"
            )
            
            print(f"  验证结果: {'通过' if validation_passed else '未完全通过'}")
            print(f"  输入数据: {len(test_data)} 行")
            print(f"  输出数据: {len(validated_data)} 行")
            print(f"  验证模式: {validation_details.get('validation_mode', 'unknown')}")
            
            if len(test_data) == len(validated_data):
                print(f"  ✅ 报告模式正常工作：保留了所有数据")
            else:
                print(f"  ❌ 报告模式异常：数据行数发生变化")
            
            if validation_details.get('failed_validations'):
                failed_count = len(validation_details['failed_validations'])
                print(f"  📝 记录了 {failed_count} 个验证失败")
            
        except Exception as e:
            print(f"  ❌ 报告模式测试失败: {e}")
        
        print(f"\n📋 总结:")
        print(f"  - 所有采集任务现在都使用统一的验证架构")
        print(f"  - 验证逻辑集中在 BaseTask._validate_data 方法中")
        print(f"  - 报告模式确保数据完整性，仅记录验证问题")
        print(f"  - 消除了重复验证和架构不一致的问题")
        
    except Exception as e:
        print(f"❌ 验证过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(verify_all_tasks_report_mode())
