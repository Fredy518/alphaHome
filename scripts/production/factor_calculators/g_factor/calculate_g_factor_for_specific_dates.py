#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
G因子指定日期计算脚本
支持为指定日期列表计算G因子，常用于补算非交易日的因子数据

使用方法：
python scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py --dates 2025-11-28
"""

import sys
import os
import argparse
import time
import logging
from datetime import datetime
from typing import List
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# 添加research路径
research_path = project_root / "research"
sys.path.insert(0, str(research_path))

# 导入g因子计算器和上下文
from pgs_factor.processors.production_g_factor_calculator import ProductionGFactorCalculator
from research.tools.context import ResearchContext


class SpecificDateGFactorCalculator:
    """指定日期G因子计算器"""

    def __init__(self):
        self.context = ResearchContext()
        self.calculator = ProductionGFactorCalculator(self.context)
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """设置日志"""
        import logging
        logger = logging.getLogger('SpecificDateGFactorCalculator')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(handler)
        return logger

    def validate_dates(self, dates: List[str]) -> List[str]:
        """验证日期格式并返回有效日期"""
        valid_dates = []
        for date_str in dates:
            try:
                # 验证日期格式
                datetime.strptime(date_str, '%Y-%m-%d')
                valid_dates.append(date_str)
            except ValueError:
                self.logger.warning(f"无效日期格式: {date_str}，跳过")
        return valid_dates

    def calculate_g_factors_for_dates(self, dates: List[str]) -> dict:
        """为指定日期计算G因子

        Args:
            dates: 日期列表，格式为 'YYYY-MM-DD'

        Returns:
            计算结果统计
        """
        self.logger.info(f"开始为指定日期计算G因子: {dates}")

        # 验证日期
        valid_dates = self.validate_dates(dates)
        if not valid_dates:
            self.logger.error("没有有效的日期需要计算")
            return {'success_count': 0, 'failed_count': 0, 'total_dates': 0}

        self.logger.info(f"有效日期: {valid_dates}")

        total_start_time = time.time()
        total_success = 0
        total_failed = 0
        results_by_date = {}

        for i, calc_date in enumerate(valid_dates, 1):
            date_start_time = time.time()
            self.logger.info(f"\n进度: [{i}/{len(valid_dates)}] 计算日期: {calc_date}")

            try:
                # 获取该日期有P因子数据的股票列表
                stock_codes = self._get_stocks_with_p_factor_data(calc_date)

                if not stock_codes:
                    self.logger.warning(f"{calc_date}: 未找到有P因子数据的股票，跳过")
                    results_by_date[calc_date] = {
                        'success_count': 0,
                        'failed_count': 0,
                        'stock_count': 0,
                        'error': 'no_p_factor_data'
                    }
                    continue

                self.logger.info(f"{calc_date}: 找到 {len(stock_codes)} 只股票有P因子数据")

                # 计算G因子
                result = self.calculator.calculate_g_factors_pit(calc_date, stock_codes)

                success_count = result.get('success_count', 0)
                failed_count = result.get('failed_count', 0)

                total_success += success_count
                total_failed += failed_count

                date_time = time.time() - date_start_time
                self.logger.info(f"{calc_date}: 成功 {success_count}, 失败 {failed_count}, 耗时 {date_time:.2f}秒")

                results_by_date[calc_date] = {
                    'success_count': success_count,
                    'failed_count': failed_count,
                    'stock_count': len(stock_codes),
                    'calculation_time': date_time
                }

            except Exception as e:
                self.logger.error(f"{calc_date} 计算失败: {e}")
                total_failed += len(stock_codes) if 'stock_codes' in locals() else 0
                results_by_date[calc_date] = {
                    'success_count': 0,
                    'failed_count': len(stock_codes) if 'stock_codes' in locals() else 0,
                    'stock_count': len(stock_codes) if 'stock_codes' in locals() else 0,
                    'error': str(e)
                }

        total_time = time.time() - total_start_time

        self.logger.info("\n" + "=" * 60)
        self.logger.info("指定日期G因子计算完成")
        self.logger.info("=" * 60)
        self.logger.info(f"总耗时: {total_time:.2f} 秒")
        self.logger.info(f"总成功: {total_success}")
        self.logger.info(f"总失败: {total_failed}")

        if total_time > 0:
            throughput = total_success / total_time
            self.logger.info(f"总吞吐量: {throughput:.1f} 只/秒")

        # 显示详细结果
        self.logger.info("\n详细结果:")
        for date, result in results_by_date.items():
            if 'error' in result:
                self.logger.info(f"  {date}: 错误 - {result['error']}")
            else:
                self.logger.info(f"  {date}: 成功 {result['success_count']}/{result['stock_count']}, "
                               f"失败 {result['failed_count']}, 耗时 {result['calculation_time']:.2f}秒")

        return {
            'total_dates': len(valid_dates),
            'successful_dates': len([r for r in results_by_date.values() if 'error' not in r]),
            'failed_dates': len([r for r in results_by_date.values() if 'error' in r]),
            'total_success': total_success,
            'total_failed': total_failed,
            'total_time': total_time,
            'throughput': total_success / total_time if total_time > 0 else 0,
            'results_by_date': results_by_date
        }

    def _get_stocks_with_p_factor_data(self, calc_date: str) -> List[str]:
        """获取指定日期有P因子数据的股票列表

        Args:
            calc_date: 计算日期

        Returns:
            股票代码列表
        """
        try:
            # 查询指定日期有P因子数据的股票
            from research.tools.context import ResearchContext
            context = ResearchContext()

            query = """
            SELECT DISTINCT ts_code
            FROM pgs_factors.p_factor
            WHERE calc_date = %s
            AND p_score IS NOT NULL
            """

            result = context.query_dataframe(query, (calc_date,))

            if result is not None and not result.empty:
                stock_codes = result['ts_code'].tolist()
                return stock_codes
            else:
                return []

        except Exception as e:
            self.logger.error(f"获取 {calc_date} P因子股票列表失败: {e}")
            return []


def main():
    parser = argparse.ArgumentParser(description='G因子指定日期计算器')
    parser.add_argument('--dates', nargs='+', required=True,
                       help='需要计算的日期列表，格式: YYYY-MM-DD YYYY-MM-DD ...')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='日志级别')

    args = parser.parse_args()

    # 设置日志级别
    logging.basicConfig(level=getattr(logging, args.log_level))

    calculator = SpecificDateGFactorCalculator()
    result = calculator.calculate_g_factors_for_dates(args.dates)

    # 返回适当的退出码
    if result['total_success'] > 0:
        print(f"\n计算完成: 成功 {result['total_success']} 只股票，失败 {result['total_failed']} 只股票")
        return 0
    else:
        print("\n❌ 计算失败: 没有成功计算任何股票")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
