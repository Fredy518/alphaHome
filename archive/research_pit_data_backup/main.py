#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT数据统一入口
==============

这是pit_data模块的统一入口程序，负责协调所有PIT数据表的历史回填、增量更新等功能。

功能特点:
1. 统一管理所有PIT数据表（行业分类、资产负债表、利润表、财务指标）
2. 按正确依赖顺序执行数据更新
3. 支持历史全量回填和增量更新
4. 提供数据验证和状态检查
5. 命令行接口友好，支持灵活的参数配置

更新顺序:
1. 行业数据 (pit_industry_classification) - 基础分类数据
2. 资产负债表 (pit_balance_quarterly) - 财务基础数据
3. 利润表 (pit_income_quarterly) - 利润相关数据
4. 财务指标 (pit_financial_indicators) - 基于前三者的计算指标

Author: AI Assistant
Date: 2025-01-01
"""

import sys
import os
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
import time

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.pit_data.pit_industry_classification_manager import PITIndustryClassificationManager
from research.pit_data.pit_balance_quarterly_manager import PITBalanceQuarterlyManager
from research.pit_data.pit_income_quarterly_manager import PITIncomeQuarterlyManager
from research.pit_data.pit_financial_indicators_manager import PITFinancialIndicatorsManager
from research.pit_data.base.pit_config import PITConfig


class PITDataCoordinator:
    """PIT数据协调器 - 统一管理所有PIT数据表"""

    # 更新顺序定义
    UPDATE_SEQUENCE = [
        {
            'name': 'pit_industry_classification',
            'description': '行业分类数据',
            'manager_class': PITIndustryClassificationManager,
            'depends_on': []
        },
        {
            'name': 'pit_balance_quarterly',
            'description': '资产负债表',
            'manager_class': PITBalanceQuarterlyManager,
            'depends_on': []
        },
        {
            'name': 'pit_income_quarterly',
            'description': '利润表',
            'manager_class': PITIncomeQuarterlyManager,
            'depends_on': []
        },
        {
            'name': 'pit_financial_indicators',
            'description': '财务指标',
            'manager_class': PITFinancialIndicatorsManager,
            'depends_on': ['pit_balance_quarterly', 'pit_income_quarterly']
        }
    ]

    def __init__(self):
        self.managers = {}
        self.logger = None

    def _get_manager(self, table_name: str):
        """获取指定表的manager实例"""
        if table_name not in self.managers:
            for seq_item in self.UPDATE_SEQUENCE:
                if seq_item['name'] == table_name:
                    # 创建manager实例但不进入上下文（稍后在使用时进入）
                    manager = seq_item['manager_class']()
                    self.managers[table_name] = manager
                    break

        return self.managers.get(table_name)

    def _validate_dependencies(self, target_table: str, processed_tables: set) -> bool:
        """验证依赖关系"""
        for seq_item in self.UPDATE_SEQUENCE:
            if seq_item['name'] == target_table:
                for dependency in seq_item['depends_on']:
                    if dependency not in processed_tables:
                        print(f"❌ 依赖检查失败: {target_table} 依赖于 {dependency}，但 {dependency} 尚未处理")
                        return False
                break
        return True

    def full_backfill_all(self,
                         start_date: str = None,
                         end_date: str = None,
                         batch_size: int = None,
                         skip_validation: bool = False) -> Dict[str, Any]:
        """
        历史全量回填所有表

        Args:
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 批次大小
            skip_validation: 是否跳过依赖验证

        Returns:
            执行结果统计
        """
        print("🏭 开始PIT数据历史全量回填")
        print("=" * 80)

        # 设置默认参数
        if start_date is None or end_date is None:
            start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)

        print(f"📅 回填日期范围: {start_date} ~ {end_date}")
        print(f"📦 默认批次大小: {batch_size or '各表默认'}")
        print()

        start_time = time.time()
        results = {}
        processed_tables = set()

        for seq_item in self.UPDATE_SEQUENCE:
            table_name = seq_item['name']
            description = seq_item['description']

            print(f"📋 处理: {description} ({table_name})")
            print("-" * 60)

            # 依赖验证
            if not skip_validation and not self._validate_dependencies(table_name, processed_tables):
                results[table_name] = {'error': '依赖验证失败', 'status': 'skipped'}
                continue

            try:
                manager = self._get_manager(table_name)

                # 根据表类型设置合适的batch_size
                table_batch_size = batch_size or PITConfig.get_batch_size(table_name)

                # 执行全量回填
                with manager as mgr:
                    result = mgr.full_backfill(
                        start_date=start_date,
                        end_date=end_date,
                        batch_size=table_batch_size
                    )

                results[table_name] = result
                processed_tables.add(table_name)

                print("✅ 完成:")
                for key, value in result.items():
                    if key != 'error':
                        print(f"  {key}: {value}")

            except Exception as e:
                print(f"❌ 失败: {e}")
                results[table_name] = {'error': str(e), 'status': 'failed'}

            print()

        # 汇总结果
        total_time = time.time() - start_time
        summary = self._generate_summary(results, total_time, "历史全量回填")

        print("=" * 80)
        print("📊 执行汇总:")
        for key, value in summary.items():
            print(f"  {key}: {value}")

        return {'results': results, 'summary': summary}

    def incremental_update_all(self,
                              days: int = None,
                              batch_size: int = None,
                              skip_validation: bool = False) -> Dict[str, Any]:
        """
        增量更新所有表

        Args:
            days: 检查最近几天的数据变更
            batch_size: 批次大小
            skip_validation: 是否跳过依赖验证

        Returns:
            执行结果统计
        """
        print("🔄 开始PIT数据增量更新")
        print("=" * 80)

        # 设置默认参数
        if days is None:
            days = PITConfig.DEFAULT_DATE_RANGES['incremental_days']

        print(f"📅 检查最近 {days} 天的变更")
        print(f"📦 默认批次大小: {batch_size or '各表默认'}")
        print()

        start_time = time.time()
        results = {}
        processed_tables = set()

        for seq_item in self.UPDATE_SEQUENCE:
            table_name = seq_item['name']
            description = seq_item['description']

            print(f"📋 处理: {description} ({table_name})")
            print("-" * 60)

            # 依赖验证
            if not skip_validation and not self._validate_dependencies(table_name, processed_tables):
                results[table_name] = {'error': '依赖验证失败', 'status': 'skipped'}
                continue

            try:
                manager = self._get_manager(table_name)

                # 根据表类型设置合适的参数
                with manager as mgr:
                    if table_name == 'pit_industry_classification':
                        # 行业分类使用月份参数
                        result = mgr.incremental_update(months=days//30 or 1, batch_size=batch_size)
                    elif table_name == 'pit_financial_indicators':
                        # 财务指标使用天数参数
                        result = mgr.incremental_update(days=days, batch_size=batch_size)
                    else:
                        # 其他表使用通用的增量更新
                        result = mgr.incremental_update(days=days, batch_size=batch_size)

                results[table_name] = result
                processed_tables.add(table_name)

                print("✅ 完成:")
                for key, value in result.items():
                    if key != 'error':
                        print(f"  {key}: {value}")

            except Exception as e:
                print(f"❌ 失败: {e}")
                results[table_name] = {'error': str(e), 'status': 'failed'}

            print()

        # 汇总结果
        total_time = time.time() - start_time
        summary = self._generate_summary(results, total_time, "增量更新")

        print("=" * 80)
        print("📊 执行汇总:")
        for key, value in summary.items():
            print(f"  {key}: {value}")

        return {'results': results, 'summary': summary}

    def full_backfill_single(self,
                           table_name: str,
                           start_date: str = None,
                           end_date: str = None,
                           batch_size: int = None) -> Dict[str, Any]:
        """
        历史全量回填单个表

        Args:
            table_name: 表名
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 批次大小

        Returns:
            执行结果
        """
        print(f"🏭 开始 {table_name} 历史全量回填")
        print("=" * 60)

        try:
            manager = self._get_manager(table_name)
            if not manager:
                raise ValueError(f"未知的表名: {table_name}")

            # 设置默认参数
            if start_date is None or end_date is None:
                start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)

            table_batch_size = batch_size or PITConfig.get_batch_size(table_name)

            print(f"📅 回填日期范围: {start_date} ~ {end_date}")
            print(f"📦 批次大小: {table_batch_size}")
            print()

            with manager as mgr:
                if 'financial_indicators' in table_name and hasattr(mgr, 'full_backfill'):
                    result = mgr.full_backfill(
                        start_date=start_date,
                        end_date=end_date,
                        batch_size=table_batch_size
                    )
                else:
                    result = mgr.full_backfill(
                        start_date=start_date,
                        end_date=end_date,
                        batch_size=table_batch_size
                    )

            print("✅ 执行完成:")
            for key, value in result.items():
                if key != 'error':
                    print(f"  {key}: {value}")

            return result

        except Exception as e:
            print(f"❌ 执行失败: {e}")
            return {'error': str(e), 'status': 'failed'}

    def incremental_update_single(self,
                                table_name: str,
                                days: int = None,
                                batch_size: int = None) -> Dict[str, Any]:
        """
        增量更新单个表

        Args:
            table_name: 表名
            days: 检查最近几天的数据变更
            batch_size: 批次大小

        Returns:
            执行结果
        """
        print(f"🔄 开始 {table_name} 增量更新")
        print("=" * 60)

        try:
            manager = self._get_manager(table_name)
            if not manager:
                raise ValueError(f"未知的表名: {table_name}")

            # 设置默认参数
            if days is None:
                days = PITConfig.DEFAULT_DATE_RANGES['incremental_days']

            print(f"📅 检查最近 {days} 天的变更")
            print(f"📦 批次大小: {batch_size or '默认'}")
            print()

            # 根据表类型调用相应的增量更新方法
            with manager as mgr:
                if table_name == 'pit_industry_classification':
                    result = mgr.incremental_update(months=days//30 or 1, batch_size=batch_size)
                elif table_name == 'pit_financial_indicators':
                    result = mgr.incremental_update(days=days, batch_size=batch_size)
                else:
                    result = mgr.incremental_update(days=days, batch_size=batch_size)

            print("✅ 执行完成:")
            for key, value in result.items():
                if key != 'error':
                    print(f"  {key}: {value}")

            return result

        except Exception as e:
            print(f"❌ 执行失败: {e}")
            return {'error': str(e), 'status': 'failed'}

    def check_status_all(self) -> Dict[str, Any]:
        """
        检查所有表的状态

        Returns:
            状态信息
        """
        print("📊 检查所有PIT表状态")
        print("=" * 80)

        status_results = {}

        for seq_item in self.UPDATE_SEQUENCE:
            table_name = seq_item['name']
            description = seq_item['description']

            print(f"📋 检查: {description} ({table_name})")

            try:
                manager = self._get_manager(table_name)
                with manager as mgr:
                    status = mgr.get_table_status()

                status_results[table_name] = status

                print("✅ 状态信息:")
                for key, value in status.items():
                    print(f"  {key}: {value}")

            except Exception as e:
                print(f"❌ 检查失败: {e}")
                status_results[table_name] = {'error': str(e)}

            print()

        return status_results

    def validate_data_all(self) -> Dict[str, Any]:
        """
        验证所有表的数据完整性

        Returns:
            验证结果
        """
        print("🔍 验证所有PIT表数据完整性")
        print("=" * 80)

        validation_results = {}

        for seq_item in self.UPDATE_SEQUENCE:
            table_name = seq_item['name']
            description = seq_item['description']

            print(f"📋 验证: {description} ({table_name})")

            try:
                manager = self._get_manager(table_name)
                with manager as mgr:
                    validation = mgr.validate_data_integrity()

                validation_results[table_name] = validation

                print("✅ 验证结果:")
                print(f"  总体状态: {validation['overall_status']}")
                print(f"  发现问题: {validation['issues_found']} 个")
                for check in validation['checks']:
                    status_icon = "✅" if check['status'] == 'passed' else "❌"
                    print(f"  {status_icon} {check['check_name']}: {check['message']}")

            except Exception as e:
                print(f"❌ 验证失败: {e}")
                validation_results[table_name] = {'error': str(e), 'overall_status': 'error'}

            print()

        return validation_results

    def _generate_summary(self, results: Dict, total_time: float, operation: str) -> Dict[str, Any]:
        """生成执行汇总"""
        summary = {
            'operation': operation,
            'total_time_seconds': round(total_time, 2),
            'total_tables': len(results),
            'successful_tables': 0,
            'failed_tables': 0,
            'skipped_tables': 0,
            'total_records': 0
        }

        for table_name, result in results.items():
            if result.get('status') == 'skipped':
                summary['skipped_tables'] += 1
            elif 'error' in result:
                summary['failed_tables'] += 1
            else:
                summary['successful_tables'] += 1
                # 累加记录数
                for key, value in result.items():
                    if 'records' in key.lower() and isinstance(value, (int, float)):
                        summary['total_records'] += value

        return summary

    def get_available_tables(self) -> List[str]:
        """获取可用的表名列表"""
        return [item['name'] for item in self.UPDATE_SEQUENCE]

    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """获取指定表的信息"""
        for item in self.UPDATE_SEQUENCE:
            if item['name'] == table_name:
                return item
        return None


def main():
    """主函数 - 命令行接口"""

    parser = argparse.ArgumentParser(
        description='PIT数据统一管理器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:

# 历史全量回填所有表
python main.py --mode full-backfill-all --start-date 2020-01-01 --end-date 2024-12-31

# 增量更新所有表
python main.py --mode incremental-all --days 30

# 历史全量回填单个表
python main.py --mode full-backfill --table pit_balance_quarterly --start-date 2023-01-01

# 增量更新单个表
python main.py --mode incremental --table pit_financial_indicators --days 7

# 检查所有表状态
python main.py --mode status

# 验证所有表数据完整性
python main.py --mode validate

# 显示可用表列表
python main.py --mode list-tables
        """
    )

    # 主要模式选择
    parser.add_argument('--mode', required=True, choices=[
        'full-backfill-all',      # 历史全量回填所有表
        'incremental-all',        # 增量更新所有表
        'full-backfill',          # 历史全量回填单个表
        'incremental',           # 增量更新单个表
        'status',                # 检查表状态
        'validate',              # 验证数据完整性
        'list-tables'            # 列出可用表
    ], help='执行模式')

    # 表名参数
    parser.add_argument('--table', choices=[
        'pit_industry_classification',
        'pit_balance_quarterly',
        'pit_income_quarterly',
        'pit_financial_indicators'
    ], help='目标表名（用于单表操作）')

    # 日期参数
    parser.add_argument('--start-date', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, help='增量更新检查天数')

    # 执行参数
    parser.add_argument('--batch-size', type=int, help='批次大小')
    parser.add_argument('--skip-validation', action='store_true', help='跳过依赖验证')
    # 财务指标强制正序处理，移除 fill-order 选项

    args = parser.parse_args()

    print("🏭 PIT数据统一管理器")
    print("=" * 80)
    print(f"执行模式: {args.mode}")
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        coordinator = PITDataCoordinator()

        # 执行相应操作
        if args.mode == 'full-backfill-all':
            result = coordinator.full_backfill_all(
                start_date=args.start_date,
                end_date=args.end_date,
                batch_size=args.batch_size,
                skip_validation=args.skip_validation
            )

        elif args.mode == 'incremental-all':
            result = coordinator.incremental_update_all(
                days=args.days,
                batch_size=args.batch_size,
                skip_validation=args.skip_validation
            )

        elif args.mode == 'full-backfill':
            if not args.table:
                print("❌ 单表历史回填模式需要指定 --table 参数")
                return 1
            result = coordinator.full_backfill_single(
                table_name=args.table,
                start_date=args.start_date,
                end_date=args.end_date,
                batch_size=args.batch_size
            )

        elif args.mode == 'incremental':
            if not args.table:
                print("❌ 单表增量更新模式需要指定 --table 参数")
                return 1
            result = coordinator.incremental_update_single(
                table_name=args.table,
                days=args.days,
                batch_size=args.batch_size
            )

        elif args.mode == 'status':
            result = coordinator.check_status_all()

        elif args.mode == 'validate':
            result = coordinator.validate_data_all()

        elif args.mode == 'list-tables':
            tables = coordinator.get_available_tables()
            print("📋 可用的PIT数据表:")
            print()
            for table_name in tables:
                info = coordinator.get_table_info(table_name)
                if info:
                    print("30")
            result = {'tables': tables}

        print("\n✅ 操作完成")
        return 0

    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
