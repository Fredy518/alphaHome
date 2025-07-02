#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试运行脚本

提供便捷的测试运行接口，支持不同类型的测试。
"""

import sys
import os
import asyncio
import argparse
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    import pytest
except ImportError:
    print("错误: 需要安装pytest")
    print("请运行: pip install pytest pytest-asyncio")
    sys.exit(1)


def run_unit_tests():
    """运行单元测试"""
    print("=== 运行单元测试 ===")
    return pytest.main([
        "-v",
        "-m", "unit",
        "--tb=short",
        str(Path(__file__).parent)
    ])


def run_integration_tests():
    """运行集成测试"""
    print("=== 运行集成测试 ===")
    return pytest.main([
        "-v", 
        "-m", "integration",
        "--tb=short",
        str(Path(__file__).parent)
    ])


def run_all_tests():
    """运行所有测试"""
    print("=== 运行所有测试 ===")
    return pytest.main([
        "-v",
        "--tb=short",
        "--cov=alphahome.processors",
        "--cov-report=html",
        "--cov-report=term-missing",
        str(Path(__file__).parent)
    ])


def run_specific_test(test_file):
    """运行特定测试文件"""
    print(f"=== 运行测试文件: {test_file} ===")
    test_path = Path(__file__).parent / test_file
    if not test_path.exists():
        print(f"错误: 测试文件不存在: {test_path}")
        return 1
    
    return pytest.main([
        "-v",
        "--tb=short", 
        str(test_path)
    ])


def run_quick_test():
    """运行快速测试（排除慢速测试）"""
    print("=== 运行快速测试 ===")
    return pytest.main([
        "-v",
        "-m", "not slow",
        "--tb=short",
        str(Path(__file__).parent)
    ])


async def run_example_usage():
    """运行使用示例测试"""
    print("=== 运行使用示例 ===")
    
    try:
        # 导入示例模块
        from alphahome.processors.examples.usage_example import example_usage
        
        # 运行示例
        await example_usage()
        print("✓ 使用示例运行成功")
        return 0
        
    except Exception as e:
        print(f"✗ 使用示例运行失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Processors模块测试运行器")
    parser.add_argument(
        "test_type", 
        choices=["unit", "integration", "all", "quick", "example"],
        nargs="?",
        default="quick",
        help="测试类型 (默认: quick)"
    )
    parser.add_argument(
        "--file", "-f",
        help="运行特定测试文件"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="详细输出"
    )
    
    args = parser.parse_args()
    
    # 设置环境变量
    os.environ["PYTHONPATH"] = str(project_root)
    
    if args.file:
        return run_specific_test(args.file)
    
    if args.test_type == "unit":
        return run_unit_tests()
    elif args.test_type == "integration":
        return run_integration_tests()
    elif args.test_type == "all":
        return run_all_tests()
    elif args.test_type == "quick":
        return run_quick_test()
    elif args.test_type == "example":
        return asyncio.run(run_example_usage())
    else:
        print(f"未知的测试类型: {args.test_type}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
