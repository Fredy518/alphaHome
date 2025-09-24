#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT财务指标管理器（原 PITFinancialIndicatorsMVPManager 重命名）
==============

负责pit_financial_indicators表的历史全量回填和增量更新

功能特点:
1. 从pit_income_quarterly等表计算财务指标
2. 支持历史全量回填和增量更新
3. 自动处理数据转换和清洗
4. 提供数据验证和状态检查

Author: AI Assistant
Date: 2025-08-11
"""

import sys
import os
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from .base.pit_table_manager import PITTableManager
    from .base.pit_config import PITConfig
    from .calculators.financial_indicators_calculator import FinancialIndicatorsCalculator
except ImportError:
    from base.pit_table_manager import PITTableManager
    from base.pit_config import PITConfig
    from calculators.financial_indicators_calculator import FinancialIndicatorsCalculator


class PITFinancialIndicatorsManager(PITTableManager):
    """财务指标 Manager：负责建表、依赖校验与调度计算"""

    def __init__(self):
        super().__init__('pit_financial_indicators')
        self.source_tables = self.table_config['source_tables']
        self.key_fields = self.table_config['key_fields']
        self.data_fields = self.table_config['data_fields']
        self.depends_on = self.table_config.get('depends_on', [])
        self.calculator = None

    def ensure_table_exists(self) -> None:
        """确保财务指标表存在（DDL 职责归位到 Manager）"""
        try:
            # 优先 pit_data/database
            sql_path = os.path.join(os.path.dirname(__file__), 'database', 'create_pit_financial_indicators_table.sql')
            sql_path = os.path.normpath(sql_path)
            if not os.path.exists(sql_path):
                alt_path = os.path.join(
                    os.path.dirname(__file__), 'database', 'create_mvp_financial_indicators_table.sql'
                )
                alt_path = os.path.normpath(alt_path)
                if os.path.exists(alt_path):
                    sql_path = alt_path
                else:
                    self.logger.warning(f"未找到建表SQL: {sql_path}")
                    return
            with open(sql_path, 'r', encoding='utf-8') as f:
                create_sql = f.read()
            self.context.db_manager.execute_sync(create_sql)
            self.logger.info("财务指标表创建/验证完成")
        except Exception as e:
            self.logger.error(f"创建财务指标表失败: {e}")

    def _initialize_calculator(self):
        if self.calculator is None:
            self.calculator = FinancialIndicatorsCalculator(self.context)

    def incremental_update(self, days: int = 7, batch_size: int | None = None) -> Dict[str, Any]:
        """
        增量更新财务指标 - 正确处理每个公告日期

        增量更新策略：
        1. 获取最近days天内有新披露的利润表记录
        2. 按公告日期分组，为每个公告日期计算财务指标
        3. 确保每个历史时间点的财务指标都被正确计算

        修复的问题：
        - 之前只在一个固定as_of_date上计算所有股票
        - 现在为每个公告日期分别计算，正确反映历史状态
        """
        from datetime import date, timedelta
        if batch_size is None:
            batch_size = self.batch_size
        # 容错：当调用方传入 days=None 时，回退至配置默认天数（或7天）
        if days is None:
            try:
                days = int(PITConfig.DEFAULT_DATE_RANGES.get('incremental_days', 7))
            except Exception:
                days = 7

        # 确保表结构完整
        self._ensure_table_exists()
        end_date = date.today().isoformat()
        start_date = (date.today() - timedelta(days=days)).isoformat()
        self.ensure_table_exists()  # 再次确认表存在
        self._initialize_calculator()

        try:
            # 1. 获取最近days天内所有利润表记录，按公告日期分组
            q = """
            SELECT DISTINCT ts_code, end_date, ann_date, data_source
            FROM pgs_factors.pit_income_quarterly
            WHERE ann_date BETWEEN %s AND %s
            ORDER BY ann_date ASC, ts_code, end_date ASC, data_source
            """
            df = self.context.query_dataframe(q, (start_date, end_date))

            if df is None or df.empty:
                self.logger.info("近期无新披露的利润表数据")
                return {'updated_records': 0, 'calculated_dates': 0, 'processed_stocks': 0, 'message': '无需要更新的数据'}

            # 获取基本统计信息
            unique_stocks = df['ts_code'].nunique()
            unique_ann_dates = df['ann_date'].nunique()
            total_records = len(df)

            self.logger.info(f"增量更新: 找到 {unique_stocks} 只股票，{unique_ann_dates} 个公告日期，共 {total_records} 条记录")

            # 2. 为每个利润表记录单独计算财务指标
            total_processed = 0
            processed_stocks = set()

            # 增量更新：按公告日期分组，严格正序（PIT）处理
            self.logger.info("增量更新：按公告日期分组，按正序处理（PIT）")

            # 按公告日期分组，确保每个公告日期的所有股票都被处理
            unique_ann_dates_list = sorted(df['ann_date'].unique(), reverse=False)

            for ann_date in unique_ann_dates_list:
                try:
                    # 获取该公告日期的所有记录
                    date_records = df[df['ann_date'] == ann_date]

                    # 按报告期分组处理（处理同一天发布多份财报的情况）
                    report_periods = date_records.groupby('end_date')
                    self.logger.debug(f"增量更新公告日期 {ann_date}: 发现 {len(report_periods)} 个报告期")

                    for end_date, period_records in report_periods:
                        try:
                            # 获取该报告期的股票列表
                            period_stocks = period_records['ts_code'].unique().tolist()

                            # 分析数据源分布
                            data_source_counts = period_records['data_source'].value_counts()
                            self.logger.debug(f"  报告期 {end_date}: {len(period_stocks)} 只股票")
                            self.logger.debug(f"    数据源分布: {dict(data_source_counts)}")

                            # 为该报告期的所有股票计算财务指标（以公告日为PIT观察时点，确保多条同end_date不同ann_date保留）
                            as_of = pd.to_datetime(ann_date).date().isoformat() if hasattr(pd.to_datetime(ann_date), 'date') else str(ann_date)
                            res = self.calculator.calculate_indicators_for_date(
                                as_of_date=as_of,
                                stock_codes=period_stocks,
                                batch_size=batch_size,
                                target_data_sources=None  # 让计算器内部处理数据源优先级
                            )

                            success_count = int(res.get('success_count', 0))
                            total_processed += success_count

                            if success_count > 0:
                                processed_stocks.update(period_stocks)

                            self.logger.debug(f"    报告期 {end_date} 增量更新完成: {success_count} 条财务指标记录")

                        except Exception as e:
                            self.logger.warning(f"公告日期 {ann_date} 报告期 {end_date} 增量更新失败: {e}")
                            continue

                    # 定期报告进度
                    if len(unique_ann_dates_list) > 10 and unique_ann_dates_list.index(ann_date) % 5 == 0:
                        progress = unique_ann_dates_list.index(ann_date) + 1
                        self.logger.info(f"增量更新进度: 已处理 {progress}/{len(unique_ann_dates_list)} 个公告日期，{len(processed_stocks)} 只股票，生成 {total_processed} 条财务指标记录")

                except Exception as e:
                    self.logger.warning(f"公告日期 {ann_date} 增量更新失败: {e}")
                    continue

            self.logger.info(f"增量更新完成: 共处理 {len(processed_stocks)}/{unique_stocks} 只股票，生成 {total_processed} 条财务指标记录")

            return {
                'updated_records': total_processed,
                'processed_stocks': len(processed_stocks),
                'total_stocks': unique_stocks,
                'message': f'成功增量更新 {total_processed} 条财务指标记录，共处理 {len(processed_stocks)} 只股票'
            }

        except Exception as e:
            self.logger.error(f"增量更新失败: {e}")
        return {
                'updated_records': 0,
                'calculated_dates': 0,
                'processed_stocks': 0,
                'error': str(e),
                'message': '增量更新失败'
        }

    def full_backfill(self, start_date: str | None = None, end_date: str | None = None, batch_size: int | None = None) -> Dict[str, Any]:
        """
        历史全量回填财务指标 - 重新计算所有历史时间点的财务指标

        Args:
            start_date: 开始日期 (ann_date)
            end_date: 结束日期 (ann_date)
            batch_size: 批次大小
            fill_order: 填充顺序 ('asc' for 正序, 'desc' for 倒序)

        Returns:
            执行结果统计
        """
        self.logger.info("开始PIT财务指标历史全量回填")

        # 设置默认参数
        if start_date is None or end_date is None:
            start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)

        if batch_size is None:
            batch_size = self.batch_size

        self.logger.info(f"回填日期范围: {start_date} ~ {end_date}")
        self.logger.info(f"每批股票数: {batch_size}")

        try:
            # 0. 确保目标表存在
            self._ensure_table_exists()
            self._initialize_calculator()

            # 1. 获取所有股票的所有历史利润表记录
            q = """
            SELECT ts_code, end_date, ann_date, data_source
            FROM pgs_factors.pit_income_quarterly
            WHERE ann_date BETWEEN %s AND %s
            ORDER BY ann_date ASC, ts_code, end_date ASC, data_source
            """
            df = self.context.query_dataframe(q, (start_date, end_date))

            if df is None or df.empty:
                self.logger.warning("未找到需要回填的历史利润表数据")
                return {'backfilled_records': 0, 'message': '无回填数据'}

            # 获取基本统计信息
            unique_stocks = df['ts_code'].nunique()
            unique_ann_dates = df['ann_date'].nunique()
            total_records = len(df)

            self.logger.info(f"从利润表获取到 {unique_stocks} 只股票，{unique_ann_dates} 个公告日期，共 {total_records} 条记录")

            # 2. 为每个利润表记录单独计算财务指标
            total_processed = 0
            processed_stocks = set()
            failed_stocks = set()

            # 全量回填：按公告日期分组，严格正序处理（PIT）
            self.logger.info("全量回填：按公告日期正序处理（PIT）")
            unique_ann_dates_list = sorted(df['ann_date'].unique(), reverse=False)

            for ann_date in unique_ann_dates_list:
                try:
                    # 获取该公告日期的所有记录
                    date_records = df[df['ann_date'] == ann_date]

                    # 分析该公告日期的数据结构
                    self.logger.debug(f"分析公告日期 {ann_date} 的数据结构...")

                    # 按报告期分组处理（处理同一天发布多份财报的情况）
                    report_periods = date_records.groupby('end_date')
                    self.logger.info(f"公告日期 {ann_date}: 发现 {len(report_periods)} 个报告期")

                    for end_date, period_records in report_periods:
                        try:
                            # 获取该报告期的股票列表
                            period_stocks = period_records['ts_code'].unique().tolist()

                            # 分析数据源分布
                            data_source_counts = period_records['data_source'].value_counts()
                            self.logger.debug(f"  报告期 {end_date}: {len(period_stocks)} 只股票")
                            self.logger.debug(f"    数据源分布: {dict(data_source_counts)}")

                            # 为该报告期的所有股票计算财务指标
                            # 统一修复：使用公告日作为PIT观察时点，确保 forecast/express 不被时间窗口排除
                            as_of = pd.to_datetime(ann_date).date().isoformat() if hasattr(pd.to_datetime(ann_date), 'date') else str(ann_date)
                            res = self.calculator.calculate_indicators_for_date(
                                as_of_date=as_of,
                                stock_codes=period_stocks,
                                batch_size=batch_size,
                                target_data_sources=None  # 让计算器内部处理数据源优先级
                            )

                            success_count = int(res.get('success_count', 0))
                            total_processed += success_count

                            if success_count > 0:
                                processed_stocks.update(period_stocks)

                            self.logger.debug(f"    报告期 {end_date} 处理完成: {success_count} 条财务指标记录")

                        except Exception as e:
                            self.logger.warning(f"公告日期 {ann_date} 报告期 {end_date} 计算失败: {e}")
                            continue

                    # 定期报告进度
                    if len(unique_ann_dates_list) > 100 and unique_ann_dates_list.index(ann_date) % 10 == 0:
                        progress = unique_ann_dates_list.index(ann_date) + 1
                        self.logger.info(f"进度: 已处理 {progress}/{len(unique_ann_dates_list)} 个公告日期，{len(processed_stocks)} 只股票，生成 {total_processed} 条财务指标记录")

                except Exception as e:
                    self.logger.warning(f"公告日期 {ann_date} 计算失败: {e}")
                    continue

            # 检查是否有股票完全没有被处理
            all_stocks_in_df = set(df['ts_code'].unique())
            unprocessed_stocks = all_stocks_in_df - processed_stocks

            if unprocessed_stocks:
                self.logger.warning(f"发现 {len(unprocessed_stocks)} 只股票没有被成功处理: {list(unprocessed_stocks)[:10]}...")

            self.logger.info(f"历史全量回填完成: 共处理 {len(processed_stocks)}/{unique_stocks} 只股票，生成 {total_processed} 条财务指标记录")

            return {
                'backfilled_records': total_processed,
                'processed_stocks': len(processed_stocks),
                'total_stocks': unique_stocks,
                'message': f'成功全量回填 {total_processed} 条财务指标记录，共处理 {len(processed_stocks)} 只股票'
            }

        except Exception as e:
            self.logger.error(f"历史全量回填失败: {e}")
            return {
                'backfilled_records': 0,
                'error': str(e),
                'message': '历史全量回填失败'
            }

    def single_backfill(self,
                         ts_code: str,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         batch_size: Optional[int] = None,
                         do_validate: bool = True) -> Dict[str, Any]:
        """单个股票历史回填（可选验证）。

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 批次大小
            do_validate: 是否执行验证
            fill_order: 填充顺序 ('asc' for 正序, 'desc' for 倒序)

        - 对指定 ts_code 重新计算所有历史时间点的财务指标
        - 每个利润表记录都会对应生成财务指标记录
        - 不影响既有全量/增量逻辑
        """
        if not ts_code:
            return {'backfilled_records': 0, 'error': '缺少 ts_code', 'message': '必须提供 --ts-code 才能执行单股回填'}

        self.logger.info(f"开始个股财务指标历史回填: ts_code={ts_code}")

        # 默认参数
        if start_date is None or end_date is None:
            start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)
        if batch_size is None:
            batch_size = self.batch_size

        self.logger.info(f"回填日期范围: {start_date} ~ {end_date}")

        try:
            # 0. 确保目标表存在
            self._ensure_table_exists()
            self._initialize_calculator()

            # 1. 获取该股票的所有历史利润表记录
            q = """
            SELECT DISTINCT ts_code, end_date, ann_date, data_source
            FROM pgs_factors.pit_income_quarterly
            WHERE ts_code = %s AND ann_date BETWEEN %s AND %s
            ORDER BY ann_date ASC, end_date ASC, data_source
            """
            df = self.context.query_dataframe(q, (ts_code, start_date, end_date))

            if df is None or df.empty:
                self.logger.warning("该股票在指定日期范围内无利润表数据")
                return {'backfilled_records': 0, 'message': '无利润表数据可供计算', 'ts_code': ts_code}

            self.logger.info(f"该股票在指定日期范围内有 {len(df)} 个唯一的(end_date, ann_date)组合")

            # 2. 为每个历史时间点计算财务指标
            total_processed = 0
            processed_dates = []

            # 按公告日期分组，每次计算一批
            unique_ann_dates = df['ann_date'].unique()
            self.logger.info(f"将为 {len(unique_ann_dates)} 个公告日期计算财务指标")

            # 单股回填：为该股票的所有公告日期计算财务指标
            self.logger.info(f"单股回填：为股票 {ts_code} 的 {len(unique_ann_dates)} 个公告日期计算财务指标")

            # 逐个处理每个公告日期，确保每个日期都能获得正确的财务指标
            for ann_date in sorted(unique_ann_dates, reverse=False):
                try:
                    # 获取该公告日期的所有记录
                    date_records = df[df['ann_date'] == ann_date]

                    # 按报告期分组处理（处理同一天发布多份财报的情况）
                    report_periods = date_records.groupby('end_date')
                    self.logger.debug(f"处理股票 {ts_code} 公告日期 {ann_date}: 发现 {len(report_periods)} 个报告期")

                    for end_date, period_records in report_periods:
                        try:
                            # 确保该股票在该报告期有记录
                            stock_period_records = period_records[period_records['ts_code'] == ts_code]
                            if stock_period_records.empty:
                                continue

                            # 分析数据源分布
                            data_source_counts = stock_period_records['data_source'].value_counts()
                            self.logger.debug(f"  报告期 {end_date}: {len(stock_period_records)} 条记录")
                            self.logger.debug(f"    数据源分布: {dict(data_source_counts)}")

                            # 为该股票在该公告日期计算财务指标（以公告日为PIT观察时点）
                            as_of = pd.to_datetime(ann_date).date().isoformat() if hasattr(pd.to_datetime(ann_date), 'date') else str(ann_date)
                            res = self.calculator.calculate_indicators_for_date(
                                as_of_date=as_of,
                                stock_codes=[ts_code],
                                batch_size=batch_size,
                                target_data_sources=None  # 让计算器内部处理数据源优先级
                            )

                            success_count = int(res.get('success_count', 0))
                            total_processed += success_count

                            if success_count > 0:
                                processed_dates.append(f"{ann_date}_{end_date}")
                                self.logger.debug(f"股票 {ts_code} 公告日期 {ann_date} 报告期 {end_date} 计算成功: {success_count} 条记录")

                        except Exception as e:
                            self.logger.warning(f"股票 {ts_code} 公告日期 {ann_date} 报告期 {end_date} 计算失败: {e}")
                            continue

                except Exception as e:
                    self.logger.warning(f"股票 {ts_code} 公告日期 {ann_date} 计算失败: {e}")
                    continue

            self.logger.info(f"历史回填完成: 共处理 {len(processed_dates)} 个公告日期，生成 {total_processed} 条财务指标记录")

            out = {
                'ts_code': ts_code,
                'backfilled_records': total_processed,
                'processed_dates': len(processed_dates),
                'message': f"单股财务指标历史回填完成，共处理 {len(processed_dates)} 个公告日期，生成 {total_processed} 条记录"
            }

            # 3. 可选：针对该股做轻量验证
            if do_validate:
                try:
                    out['validation'] = self._validate_single_stock(ts_code, start_date, end_date)
                except Exception as ve:
                    self.logger.warning(f"单股回填验证失败（忽略不中断）: {ve}")
            return out

        except Exception as e:
            self.logger.error(f"个股财务指标历史回填失败: {e}")
            return {
                'ts_code': ts_code,
                'backfilled_records': 0,
                'error': str(e),
                'message': '个股财务指标历史回填失败'
            }

    def _get_table_columns(self, schema: str, table: str) -> set:
        """获取表的列名集合"""
        sql = f"SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s"
        try:
            df = self.context.query_dataframe(sql, (schema, table))
            return set(df['column_name'].tolist()) if df is not None else set()
        except Exception:
            return set()

    def _validate_single_stock(self, ts_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """对指定股票在给定日期范围内进行轻量验证，辅助核对处理逻辑正确性。
        验证内容：
        - 统计记录行数
        - 核心字段是否全部为空的记录数量（应尽量为0）
        - ts_code/end_date 关键字段完整性
        """
        pit_cols = self._get_table_columns(PITConfig.PIT_SCHEMA, self.table_name)
        core_fields = [c for c in self.data_fields if c in pit_cols]
        select_cols = ['ts_code', 'end_date'] + ([
            'as_of_date'] if 'as_of_date' in pit_cols else []) + core_fields
        sql = (
            f"SELECT {', '.join(select_cols)} FROM {PITConfig.PIT_SCHEMA}.{self.table_name} "
            f"WHERE ts_code=%s"
        )
        df = self.context.query_dataframe(sql, (ts_code,))
        if df is None or df.empty:
            return {'ts_code': ts_code, 'range': [start_date, end_date], 'rows': 0}

        work = df.copy()

        # 统计
        # 核心字段全空统计
        if core_fields:
            all_null = work[core_fields].isna().all(axis=1)
            null_count = int(all_null.sum())
        else:
            null_count = 0

        issues = 0
        if null_count > 0:
            issues += 1

        key_null = int(work[['ts_code','end_date']].isna().any(axis=1).sum())
        if key_null > 0:
            issues += 1

        result = {
            'ts_code': ts_code,
            'range': [start_date, end_date],
            'rows': int(len(work)),
            'all_core_null_rows': null_count,
            'key_field_null_rows': key_null,
            'status': 'passed' if issues == 0 else 'warning'
        }
        return result


def main():
    """主函数 - 命令行接口"""

    parser = argparse.ArgumentParser(
        description='PIT财务指标管理器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:

# 全量回填
python pit_financial_indicators_manager.py --mode full-backfill --start-date 2020-01-01 --end-date 2024-12-31

# 增量更新
python pit_financial_indicators_manager.py --mode incremental --days 7

# 单股回填
python pit_financial_indicators_manager.py --mode single-backfill --ts-code 600000.SH

# 显示表状态
python pit_financial_indicators_manager.py --status

# 验证数据完整性
python pit_financial_indicators_manager.py --validate
        """
    )
    parser.add_argument('--mode', choices=['full-backfill', 'incremental', 'single-backfill'],
                       help='执行模式')
    parser.add_argument('--start-date', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, help='增量更新天数')
    parser.add_argument('--batch-size', type=int, help='每批股票数（按 ts_code 分批）')
    # 强制正序，不再提供 fill-order 选项
    parser.add_argument('--status', action='store_true', help='显示表状态')
    parser.add_argument('--validate', action='store_true', help='验证数据完整性')
    parser.add_argument('--ts-code', help='指定单股 ts_code（如 600000.SH），用于 single-backfill 模式')

    args = parser.parse_args()

    # 初始化统一日志（方案C）：控制台 +（可选）文件
    try:
        from alphahome.common.logging_utils import setup_logging
        # 文件名按表名区分，避免混淆
        log_fn = f"pit_financial_indicators_{datetime.now().strftime('%Y%m%d')}.log"
        setup_logging(log_level="INFO", log_to_file=True, log_dir="logs", log_filename=log_fn)
    except Exception:
        # 忽略日志初始化异常，继续执行
        pass

    print("📊 PIT财务指标管理器")
    print("=" * 60)

    try:
        with PITFinancialIndicatorsManager() as manager:

            # 显示表状态
            if args.status:
                print("📈 表状态:")
                status = manager.get_table_status()
                for key, value in status.items():
                    print(f"  {key}: {value}")
                return 0

            # 仅当未指定 mode 时，单独执行全表验证
            if args.validate and not args.mode:
                print("🔍 数据完整性验证:")
                validation = manager.validate_data_integrity()
                print(f"  总体状态: {validation['overall_status']}")
                print(f"  发现问题: {validation['issues_found']} 个")
                for check in validation['checks']:
                    status_icon = "✅" if check['status'] == 'passed' else "❌"
                    print(f"  {status_icon} {check['check_name']}: {check['message']}")
                return 0

            # 执行主要功能
            if args.mode and args.mode == 'full-backfill':
                result = manager.full_backfill(
                    start_date=args.start_date,
                    end_date=args.end_date,
                    batch_size=args.batch_size
                )
            elif args.mode and args.mode == 'incremental':
                result = manager.incremental_update(
                    days=args.days,
                    batch_size=args.batch_size
                )
            elif args.mode and args.mode == 'single-backfill':
                result = manager.single_backfill(
                    ts_code=args.ts_code,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    batch_size=args.batch_size,
                    do_validate=args.validate
                )

            if args.mode:
                print(f"\n✅ 执行结果:")
                for key, value in result.items():
                    print(f"  {key}: {value}")

                # 若指定了 validate 且非单股模式，则在执行后进行全表数据验证
                if args.validate and args.mode != 'single-backfill':
                    print("\n🔍 执行后数据完整性验证:")
                    validation = manager.validate_data_integrity()
                    print(f"  总体状态: {validation['overall_status']}")
                    print(f"  发现问题: {validation['issues_found']} 个")
                    for check in validation['checks']:
                        status_icon = "✅" if check['status'] == 'passed' else "❌"
                        print(f"  {status_icon} {check['check_name']}: {check['message']}")

                return 0 if 'error' not in result else 1

            return 0

    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

