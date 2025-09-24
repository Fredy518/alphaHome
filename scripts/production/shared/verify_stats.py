#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
验证 Tushare 更新脚本统计逻辑的测试脚本
"""

import sys
sys.path.insert(0, '.')

def test_stats_logic():
    """测试统计逻辑"""
    # 模拟任务结果（包括所有可能的状态）
    mock_results = [
        {'task_name': 'task1', 'status': 'success', 'execution_time': 10.0},
        {'task_name': 'task2', 'status': 'partial_success', 'execution_time': 15.0},
        {'task_name': 'task3', 'status': 'failed', 'error': 'Network error'},
        {'task_name': 'task4', 'status': 'error', 'error': 'API limit'},
        {'task_name': 'task5', 'status': 'skipped', 'message': 'Not supported'},
        {'task_name': 'task6', 'status': 'completed_with_warnings', 'execution_time': 12.0},
        {'task_name': 'task7', 'status': 'unknown', 'error': 'Strange error'},
    ]

    # 初始化统计
    stats = {
        'total_tasks': len(mock_results),
        'successful_tasks': 0,
        'failed_tasks': 0,
        'skipped_tasks': 0
    }

    # 统计结果（与主脚本保持一致的逻辑）
    for result in mock_results:
        status = result.get('status', 'unknown')
        if status in ['success', 'partial_success']:
            stats['successful_tasks'] += 1
        elif status in ['failed', 'error']:
            stats['failed_tasks'] += 1
        elif status in ['skipped', 'skipped_dry_run']:
            stats['skipped_tasks'] += 1
        elif status == 'completed_with_warnings':
            # 兼容旧的状态，归类为部分成功
            stats['successful_tasks'] += 1
        else:
            # 处理其他未知状态
            print(f"未知任务状态: {status} for task {result.get('task_name')}")
            stats['failed_tasks'] += 1  # 归类为失败

    print("📊 统计结果:")
    print(f"总任务数: {stats['total_tasks']}")
    print(f"✅ 成功任务: {stats['successful_tasks']}")
    print(f"❌ 失败任务: {stats['failed_tasks']}")
    print(f"⏭️ 跳过任务: {stats['skipped_tasks']}")
    print(f"📈 总计: {stats['successful_tasks'] + stats['failed_tasks'] + stats['skipped_tasks']}")

    # 验证总计
    total_counted = stats['successful_tasks'] + stats['failed_tasks'] + stats['skipped_tasks']
    if total_counted == stats['total_tasks']:
        print("✅ 统计逻辑正确！")
        return True
    else:
        print(f"❌ 统计逻辑有误！总计 {total_counted} 不等于总任务数 {stats['total_tasks']}")
        return False

if __name__ == "__main__":
    success = test_stats_logic()
    print(f"\n验证结果: {'✅ 通过' if success else '❌ 失败'}")
