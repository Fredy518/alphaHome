#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量补齐最近缺失的P/G因子数据脚本
用于补齐最近几个月非交易日的因子数据

使用方法：
python scripts/production/factor_calculators/batch_calculate_recent_missing_factors.py --months 3
"""

import sys
import os
import argparse
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# 添加research路径
research_path = project_root / "research"
sys.path.insert(0, str(research_path))

# 导入计算器
from pgs_factor.processors.production_p_factor_calculator import ProductionPFactorCalculator
from pgs_factor.processors.production_g_factor_calculator import ProductionGFactorCalculator


class RecentMissingFactorCalculator:
    """最近缺失因子计算器"""

    def __init__(self):
        from research.tools.context import ResearchContext
        self.context = ResearchContext()
        self.p_calculator = ProductionPFactorCalculator(self.context)
        self.g_calculator = ProductionGFactorCalculator(self.context)
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """设置日志"""
        logger = logging.getLogger('RecentMissingFactorCalculator')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(handler)
        return logger

    def get_recent_missing_dates(self, months_back: int = 3) -> Dict[str, List[str]]:
        """获取最近几个月缺失因子数据的日期"""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=months_back * 30)  # 近似计算

        try:
            # 生成日期范围
            query = """
            WITH date_range AS (
                SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS calc_date
            ),
            p_factor_dates AS (
                SELECT DISTINCT calc_date FROM pgs_factors.p_factor
            ),
            g_factor_dates AS (
                SELECT DISTINCT calc_date FROM pgs_factors.g_factor
            )
            SELECT
                dr.calc_date,
                CASE WHEN pfd.calc_date IS NOT NULL THEN '有P因子' ELSE '无P因子' END as p_factor_status,
                CASE WHEN gfd.calc_date IS NOT NULL THEN '有G因子' ELSE '无G因子' END as g_factor_status,
                CASE WHEN pfd.calc_date IS NULL OR gfd.calc_date IS NULL THEN '数据缺失' ELSE '数据完整' END as data_status
            FROM date_range dr
            LEFT JOIN p_factor_dates pfd ON dr.calc_date = pfd.calc_date
            LEFT JOIN g_factor_dates gfd ON dr.calc_date = gfd.calc_date
            WHERE dr.calc_date IS NOT NULL
            ORDER BY dr.calc_date
            """

            result = self.context.query_dataframe(query, (start_date, end_date))

            if result is None or result.empty:
                return {'missing_dates': [], 'total_missing': 0}

            # 筛选出数据缺失的日期
            missing_data = result[result['data_status'] == '数据缺失']

            # 确保calc_date是datetime类型
            if 'calc_date' in missing_data.columns:
                calc_dates = pd.to_datetime(missing_data['calc_date'], errors='coerce')
                missing_dates = calc_dates.dropna().dt.strftime('%Y-%m-%d').tolist()
            else:
                missing_dates = []

            # 按月份分组
            missing_by_month = {}
            for date_str in missing_dates:
                month_key = date_str[:7]  # YYYY-MM
                if month_key not in missing_by_month:
                    missing_by_month[month_key] = []
                missing_by_month[month_key].append(date_str)

            self.logger.info(f"最近{months_back}个月发现 {len(missing_dates)} 个缺失因子数据的日期")
            for month, dates in missing_by_month.items():
                self.logger.info(f"  {month}: {len(dates)} 个日期")

            return {
                'missing_dates': missing_dates,
                'total_missing': len(missing_dates),
                'missing_by_month': missing_by_month
            }

        except Exception as e:
            self.logger.error(f"获取缺失日期失败: {e}")
            return {'missing_dates': [], 'total_missing': 0}

    def calculate_recent_missing_factors(self, months_back: int = 3) -> Dict[str, Any]:
        """计算最近几个月缺失的因子数据"""
        self.logger.info(f"开始计算最近{months_back}个月的缺失因子数据")

        # 获取缺失日期
        missing_info = self.get_recent_missing_dates(months_back)

        if missing_info['total_missing'] == 0:
            self.logger.info("没有发现缺失的因子数据")
            return {'total_dates': 0, 'successful_dates': 0, 'failed_dates': 0}

        self.logger.info(f"发现 {missing_info['total_missing']} 个缺失日期，开始计算...")

        total_start_time = time.time()
        results = {
            'total_dates': len(missing_info['missing_dates']),
            'successful_dates': 0,
            'failed_dates': 0,
            'total_p_success': 0,
            'total_p_failed': 0,
            'total_g_success': 0,
            'total_g_failed': 0,
            'details': {}
        }

        # 按月份分组处理
        for month_key, dates in missing_info['missing_by_month'].items():
            month_start_time = time.time()
            self.logger.info(f"\n处理月份: {month_key} ({len(dates)} 个日期)")

            month_results = self._calculate_month_factors(dates)
            results['details'][month_key] = month_results

            # 累计统计
            results['successful_dates'] += month_results['successful_dates']
            results['failed_dates'] += month_results['failed_dates']
            results['total_p_success'] += month_results['p_success']
            results['total_p_failed'] += month_results['p_failed']
            results['total_g_success'] += month_results['g_success']
            results['total_g_failed'] += month_results['g_failed']

            month_time = time.time() - month_start_time
            self.logger.info(f"月份 {month_key} 处理完成: 耗时 {month_time:.2f}秒")

        total_time = time.time() - total_start_time
        results['total_time'] = total_time

        self.logger.info("\n" + "=" * 80)
        self.logger.info("最近缺失因子计算完成")
        self.logger.info("=" * 80)
        self.logger.info(f"总耗时: {total_time:.2f} 秒")
        self.logger.info(f"处理日期: {results['successful_dates'] + results['failed_dates']}/{results['total_dates']}")
        self.logger.info(f"P因子: 成功 {results['total_p_success']}, 失败 {results['total_p_failed']}")
        self.logger.info(f"G因子: 成功 {results['total_g_success']}, 失败 {results['total_g_failed']}")

        return results

    def _calculate_month_factors(self, dates: List[str]) -> Dict[str, Any]:
        """计算一个月份的因子数据"""
        month_results = {
            'successful_dates': 0,
            'failed_dates': 0,
            'p_success': 0,
            'p_failed': 0,
            'g_success': 0,
            'g_failed': 0,
            'date_details': {}
        }

        for calc_date in dates:
            date_start_time = time.time()

            try:
                # 计算P因子
                p_result = self._calculate_p_factor_for_date(calc_date)
                month_results['p_success'] += p_result['success_count']
                month_results['p_failed'] += p_result['failed_count']

                # 计算G因子（需要P因子数据存在）
                g_result = self._calculate_g_factor_for_date(calc_date)
                month_results['g_success'] += g_result['success_count']
                month_results['g_failed'] += g_result['failed_count']

                # 记录日期详情
                month_results['date_details'][calc_date] = {
                    'p_result': p_result,
                    'g_result': g_result,
                    'calculation_time': time.time() - date_start_time
                }

                if p_result['success_count'] > 0 and g_result['success_count'] > 0:
                    month_results['successful_dates'] += 1
                    self.logger.info(f"{calc_date}: 成功计算因子数据")
                else:
                    month_results['failed_dates'] += 1
                    self.logger.warning(f"{calc_date}: 因子计算失败或部分失败")

            except Exception as e:
                self.logger.error(f"{calc_date} 计算失败: {e}")
                month_results['failed_dates'] += 1
                month_results['date_details'][calc_date] = {
                    'error': str(e),
                    'calculation_time': time.time() - date_start_time
                }

        return month_results

    def _calculate_p_factor_for_date(self, calc_date: str) -> Dict[str, Any]:
        """为指定日期计算P因子"""
        try:
            # 获取该日期的交易股票
            stock_codes = self.p_calculator._get_trading_stock_codes(calc_date)

            if not stock_codes:
                return {'success_count': 0, 'failed_count': 0, 'error': 'no_trading_stocks'}

            # 计算P因子
            result = self.p_calculator.calculate_p_factors_pit(calc_date, stock_codes)
            return result

        except Exception as e:
            self.logger.error(f"P因子计算失败 {calc_date}: {e}")
            return {'success_count': 0, 'failed_count': len(stock_codes) if 'stock_codes' in locals() else 0, 'error': str(e)}

    def _calculate_g_factor_for_date(self, calc_date: str) -> Dict[str, Any]:
        """为指定日期计算G因子"""
        try:
            # 获取该日期有P因子数据的股票
            stock_codes = self._get_stocks_with_p_factor_data(calc_date)

            if not stock_codes:
                return {'success_count': 0, 'failed_count': 0, 'error': 'no_p_factor_data'}

            # 计算G因子
            result = self.g_calculator.calculate_g_factors_pit(calc_date, stock_codes)
            return result

        except Exception as e:
            self.logger.error(f"G因子计算失败 {calc_date}: {e}")
            return {'success_count': 0, 'failed_count': len(stock_codes) if 'stock_codes' in locals() else 0, 'error': str(e)}

    def _get_stocks_with_p_factor_data(self, calc_date: str) -> List[str]:
        """获取指定日期有P因子数据的股票列表"""
        try:
            query = """
            SELECT DISTINCT ts_code
            FROM pgs_factors.p_factor
            WHERE calc_date = %s
            AND p_score IS NOT NULL
            """

            result = self.context.query_dataframe(query, (calc_date,))

            if result is not None and not result.empty:
                return result['ts_code'].tolist()
            else:
                return []

        except Exception as e:
            self.logger.error(f"获取 {calc_date} P因子股票列表失败: {e}")
            return []


def main():
    parser = argparse.ArgumentParser(description='批量补齐最近缺失的P/G因子数据')
    parser.add_argument('--months', type=int, default=3, help='补齐最近几个月的数据 (默认: 3)')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='日志级别')

    args = parser.parse_args()

    # 设置日志级别
    logging.basicConfig(level=getattr(logging, args.log_level))

    calculator = RecentMissingFactorCalculator()

    # 显示将要处理的月份范围
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=args.months * 30)
    print(f"将处理 {start_date} 到 {end_date} 的缺失因子数据")

    # 获取缺失日期
    missing_info = calculator.get_recent_missing_dates(args.months)

    if missing_info['total_missing'] == 0:
        print("没有发现缺失的因子数据")
        return 0

    print("\n缺失数据分析:")
    print(f"   总缺失日期: {missing_info['total_missing']}")
    for month, dates in missing_info['missing_by_month'].items():
        print(f"   {month}: {len(dates)} 个日期")

    # 确认是否继续
    confirm = input(f"\n是否开始计算 {missing_info['total_missing']} 个日期的因子数据? (y/N): ")
    if confirm.lower() != 'y':
        print("已取消")
        return 0

    # 执行批量计算
    results = calculator.calculate_recent_missing_factors(args.months)

    # 输出结果摘要
    print("\n计算结果摘要:")
    print(f"   成功处理月份: {len(results['details'])}")
    print(f"   成功计算日期: {results['successful_dates']}")
    print(f"   失败日期: {results['failed_dates']}")
    print(f"   P因子成功: {results['total_p_success']}")
    print(f"   P因子失败: {results['total_p_failed']}")
    print(f"   G因子成功: {results['total_g_success']}")
    print(f"   G因子失败: {results['total_g_failed']}")
    print(f"   总耗时: {results['total_time']:.2f} 秒")

    if results['total_p_success'] > 0 and results['total_g_success'] > 0:
        print("\n批量计算成功完成!")
        return 0
    else:
        print("\n批量计算完成但存在问题")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
