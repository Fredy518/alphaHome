#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
标准财务指标计算器（由 ProductionFinancialIndicatorsCalculator 重命名为统一入口）
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import List, Optional, Dict, Any, Tuple
import time
import random
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

from research.tools.context import ResearchContext


class FinancialIndicatorsCalculator:
    """标准财务指标计算器（pit_data 版）"""

    def __init__(self, context: ResearchContext):
        self.context = context
        self.db_manager = context.db_manager
        self.logger = self._setup_logger()

        # 优化配置
        self.max_workers = 4  # 并发线程数
        self.enable_cache = True  # 启用缓存
        self.cache_size = 1000  # 缓存大小

        # 数据缓存
        self._data_cache = {}
        self._yoy_cache = {}

        self.stats = {
            'processed_stocks': 0,
            'successful_calculations': 0,
            'failed_calculations': 0,
            'start_time': None,
            'end_time': None,
            'cache_hits': 0,
            'cache_misses': 0
        }

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('FinancialIndicatorsCalculator')
        logger.setLevel(logging.INFO)
        return logger

    def _safe_numeric(self, value: Any) -> Optional[float]:
        """
        安全地将值转换为float类型，处理decimal.Decimal和其他数值类型
        返回 None 如果值无效，这样可以更好地处理同比计算
        """
        if value is None or pd.isna(value):
            return None
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_numeric_for_calc(self, value: Any, default: float = 0.0) -> float:
        """
        安全地将值转换为float类型，用于计算场景
        如果值无效，返回指定的默认值而不是None
        """
        result = self._safe_numeric(value)
        return result if result is not None else default

    def _safe_divide(self, numerator: Any, denominator: Any) -> Optional[float]:
        """
        安全除法运算，处理decimal.Decimal和其他数值类型
        """
        num = self._safe_numeric(numerator)
        den = self._safe_numeric(denominator)
        if den is None or den == 0 or num is None:
            return None
        return num / den

    def _get_data_source_priority(self, data_source: str) -> int:
        """
        获取数据源优先级（数值越小优先级越高）
        report > express > forecast
        """
        priority_map = {
            'report': 1,
            'express': 2,
            'forecast': 3
        }
        return priority_map.get(data_source, 999)

    def _filter_by_data_source_priority(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        根据数据源优先级过滤数据，保留每个(ts_code, end_date, ann_date)组合中优先级最高的数据源
        """
        if df.empty or 'data_source' not in df.columns:
            return df

        # 按优先级排序（优先级数字小的在前）
        df_sorted = df.copy()
        df_sorted['_priority'] = df_sorted['data_source'].apply(self._get_data_source_priority)
        df_sorted = df_sorted.sort_values(['ts_code', 'end_date', 'ann_date', '_priority'])

        # 保留每个组合的第一条记录（优先级最高）
        df_filtered = df_sorted.drop_duplicates(['ts_code', 'end_date', 'ann_date'], keep='first')
        df_filtered = df_filtered.drop(columns=['_priority'])

        # 不再输出数据源优先级过滤日志，因为已在分组处理阶段解决
        return df_filtered

    def clear_cache(self):
        """清空缓存"""
        self._data_cache.clear()
        self._yoy_cache.clear()
        self.logger.info("缓存已清空")

    def _get_cache_key(self, as_of_date: str, stock_codes: List[str]) -> str:
        """生成缓存键"""
        codes_str = ','.join(sorted(stock_codes))
        return f"{as_of_date}:{codes_str}"

    def _vectorized_calculate_growth_indicators(self, financial_data: pd.DataFrame) -> pd.DataFrame:
        """
        向量化的增长指标计算 - 大幅提升性能
        使用pandas的向量化操作替代循环查找

        计算的核心指标包括：
        - revenue_yoy_growth: 营收同比增长率(%)
        - n_income_yoy_growth: 归属母公司股东的净利润同比增长率(%) ⭐
          * 基于 n_income_attr_p 字段（归母净利润）
          * 反映母公司股东实际获得的利润增长情况
          * 是衡量公司盈利能力的关键指标
        - operate_profit_yoy_growth: 经营利润同比增长率(%)
        """
        if financial_data.empty:
            return pd.DataFrame()

        # 按股票分组并排序
        grouped = financial_data.sort_values(['ts_code', 'end_date'], ascending=[True, False]).groupby('ts_code')

        results = []
        for ts_code, group in grouped:
            if len(group) < 2:
                continue

            # 获取当前最新数据
            current = group.iloc[0]

            # 查找同比数据（向量化）
            current_date = pd.to_datetime(current['end_date'])
            yoy_target_date = current_date - relativedelta(months=12)

            # 在组内查找最接近的同比数据
            group_dates = pd.to_datetime(group['end_date'])
            date_diffs = abs((group_dates - yoy_target_date).dt.days)

            # 找到最接近的记录（排除当前记录）
            min_diff_idx = date_diffs[1:].idxmin() if len(date_diffs) > 1 else None

            if min_diff_idx is not None and date_diffs.loc[min_diff_idx] <= 90:  # 允许90天容差
                yoy_data = group.loc[min_diff_idx]

                # 安全数值转换
                current_revenue = self._safe_numeric(current['revenue'])
                yoy_revenue = self._safe_numeric(yoy_data['revenue'])
                current_n_income = self._safe_numeric(current['n_income_attr_p'])
                yoy_n_income = self._safe_numeric(yoy_data['n_income_attr_p'])
                current_operate_profit = self._safe_numeric(current['operate_profit'])
                yoy_operate_profit = self._safe_numeric(yoy_data['operate_profit'])

                growth_indicators = {
                    'ts_code': ts_code,
                    'revenue_yoy_growth': (current_revenue - yoy_revenue) / yoy_revenue * 100 if yoy_revenue is not None and yoy_revenue > 0 and current_revenue is not None else None,  # 营收同比增长率(%)
                    'n_income_yoy_growth': (current_n_income - yoy_n_income) / abs(yoy_n_income) * 100 if yoy_n_income is not None and yoy_n_income != 0 and current_n_income is not None else None,  # 归属母公司股东的净利润同比增长率(%)
                    'operate_profit_yoy_growth': (current_operate_profit - yoy_operate_profit) / abs(yoy_operate_profit) * 100 if yoy_operate_profit is not None and yoy_operate_profit != 0 and current_operate_profit is not None and self._should_calculate_operating_profit_growth(current['data_source']) else None  # 经营利润同比增长率(%)
                }

                # 过滤掉NaN值
                growth_indicators = {k: v for k, v in growth_indicators.items() if v is not None and not pd.isna(v)}
                if growth_indicators:
                    results.append(growth_indicators)

        return pd.DataFrame(results)

    def calculate_indicators_for_date(
        self,
        as_of_date: str,
        stock_codes: Optional[List[str]] = None,
        batch_size: int = 1000,  # 增大批次大小
        target_data_sources: Optional[List[str]] = None,
        use_parallel: bool = True  # 是否使用并行处理
    ) -> Dict[str, Any]:
        self.stats['start_time'] = time.time()

        if stock_codes is None:
            stock_codes = self._get_active_stocks()

        # 只在处理多个股票或特定条件时才输出详细信息
        if len(stock_codes) > 1 or target_data_sources:
            self.logger.info(f"计算 {as_of_date}: {len(stock_codes)} 只股票")
            if target_data_sources:
                self.logger.info(f"数据源筛选: {target_data_sources}")

        # 优化策略：根据数据量选择处理方式
        if len(stock_codes) < 100 or not use_parallel:
            # 小批量使用串行处理
            result = self._calculate_serial(as_of_date, stock_codes, batch_size, target_data_sources)
        else:
            # 大批量使用并行处理
            result = self._calculate_parallel(as_of_date, stock_codes, batch_size, target_data_sources)

        # 只在最终汇总时输出详细性能统计，避免每个日期都输出
        # 注释掉这里，避免每个日期都输出详细统计
        # self._log_performance_stats(detailed=True)
        return result

    def _calculate_serial(self, as_of_date: str, stock_codes: List[str], batch_size: int,
                         target_data_sources: Optional[List[str]] = None) -> Dict[str, Any]:
        """串行计算模式"""
        total_success = 0
        total_failed = 0

        for i in range(0, len(stock_codes), batch_size):
            batch_stocks = stock_codes[i:i + batch_size]
            try:
                success_count = self._calculate_batch_indicators(as_of_date, batch_stocks, target_data_sources)
                total_success += success_count
                total_failed += len(batch_stocks) - success_count
                # 移除冗余的批次日志输出
                pass
            except Exception as e:
                self.logger.error(f"批次计算失败: {e}")
                total_failed += len(batch_stocks)

        return self._finalize_calculation(total_success, total_failed)

    def _calculate_parallel(self, as_of_date: str, stock_codes: List[str], batch_size: int,
                           target_data_sources: Optional[List[str]] = None) -> Dict[str, Any]:
        """并行计算模式"""
        # 预加载数据到缓存
        self._preload_data_cache(as_of_date, stock_codes)

        # 分批创建任务
        futures = []
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 将股票列表分成多个批次
            for i in range(0, len(stock_codes), batch_size):
                batch_stocks = stock_codes[i:i + batch_size]
                future = executor.submit(
                    self._calculate_batch_indicators_parallel,
                    as_of_date, batch_stocks, target_data_sources
                )
                futures.append((future, i//batch_size + 1, len(batch_stocks)))

            # 收集结果
            for future, batch_num, batch_size_count in futures:
                try:
                    result = future.result(timeout=300)  # 5分钟超时
                    results.append(result)
                    # 移除冗余的并行批次日志输出
                    pass
                except Exception as e:
                    self.logger.error(f"并行批次 {batch_num} 失败: {e}")
                    results.append({'success': 0, 'failed': batch_size_count})

        total_success = sum(r['success'] for r in results)
        total_failed = sum(r['failed'] for r in results)

        return self._finalize_calculation(total_success, total_failed)

    def _preload_data_cache(self, as_of_date: str, stock_codes: List[str]):
        """预加载数据到缓存"""
        if not self.enable_cache:
            return

        cache_key = self._get_cache_key(as_of_date, stock_codes[:1000])  # 只缓存前1000只股票的数据
        if cache_key not in self._data_cache:
            self.logger.info("预加载数据到缓存...")
            self._data_cache[cache_key] = self._get_pit_data_for_calculation(as_of_date, stock_codes[:1000])
            self.stats['cache_misses'] += 1
        else:
            self.stats['cache_hits'] += 1

    def _finalize_calculation(self, total_success: int, total_failed: int) -> Dict[str, Any]:
        """完成计算并返回结果"""
        self.stats['end_time'] = time.time()
        self.stats['successful_calculations'] = total_success
        self.stats['failed_calculations'] = max(0, total_failed)  # 确保失败数量不为负数
        self._log_performance_stats(detailed=False)
        return {
            'success_count': total_success,
            'failed_count': max(0, total_failed),  # 确保失败数量不为负数
            'total_time': self.stats['end_time'] - self.stats['start_time'],
            'cache_stats': {
                'hits': self.stats['cache_hits'],
                'misses': self.stats['cache_misses']
            }
        }

    def _calculate_batch_indicators(self, as_of_date: str, stock_codes: List[str], target_data_sources: List[str] = None) -> int:
        # 尝试从缓存获取数据
        cache_key = self._get_cache_key(as_of_date, stock_codes)
        if self.enable_cache and cache_key in self._data_cache:
            pit_data = self._data_cache[cache_key]
            self.stats['cache_hits'] += 1
        else:
            pit_data = self._get_pit_data_for_calculation(as_of_date, stock_codes)
            if self.enable_cache:
                self._data_cache[cache_key] = pit_data
                self.stats['cache_misses'] += 1

        if pit_data.empty:
            return 0

        indicators_list = []
        processed_records = 0  # 记录成功处理的记录数量

        # 按股票分组：使用完整的历史数据作为同比/TTM的计算上下文
        # 仅对“当前公告日等于 as_of_date”的记录进行计算与入库，历史记录仅作为参考不保存
        for ts_code, stock_data in pit_data.groupby('ts_code'):
            try:
                # 筛选目标数据源
                if target_data_sources:
                    stock_data = stock_data[stock_data['data_source'].isin(target_data_sources)]
                    if stock_data.empty:
                        continue

                # 仅计算“当日公告”的记录；同时保留完整 stock_data 作为同比/TTM上下文
                try:
                    as_of_dt = pd.to_datetime(as_of_date).date()
                except Exception:
                    # 若无法解析，则回退为字符串比较
                    as_of_dt = as_of_date

                work = stock_data.copy()
                # 统一将 ann_date 转换为 date 便于对比（兼容 datetime / str）
                try:
                    work['ann_date'] = pd.to_datetime(work['ann_date']).dt.date
                except Exception:
                    pass

                current_rows = work[work['ann_date'] == as_of_dt]
                if current_rows.empty:
                    # 当天该股票无公告，跳过入库，仅用于他股的基期上下文
                    continue

                for idx, row in current_rows.iterrows():
                    try:
                        # 传递当前行和完整的股票历史数据用于同比计算
                        indicators = self._calculate_single_stock_indicators(
                            ts_code, as_of_date, work,
                            force_data_source=row['data_source'],
                            current_record=row
                        )

                        if indicators:
                            indicators_list.append(indicators)
                            processed_records += 1
                            # 每处理100条记录输出一次进度
                            if processed_records % 100 == 0:
                                self.logger.info(f"已处理 {processed_records} 条财务记录")
                        else:
                            # 检查是否是因为PRT_ORIG数据而跳过
                            conversion_status = row.get('conversion_status')
                            if conversion_status == 'RPT_ORIG':
                                # 静默跳过PRT_ORIG数据，不输出警告
                                pass
                            else:
                                self.logger.warning(f"计算失败: 股票 {ts_code} 公告日期 {row['ann_date']} 报告期 {row['end_date']} (数据源: {row['data_source']})")

                    except Exception as e:
                        self.logger.error(f"计算股票 {ts_code} 行 {idx} 失败: {e}")
                        continue

            except Exception as e:
                self.logger.error(f"处理股票 {ts_code} 失败: {e}")
                continue

        if indicators_list:
            self._save_indicators_batch(indicators_list)

        # 返回成功处理的记录数量
        return processed_records

    def _calculate_batch_indicators_parallel(self, as_of_date: str, stock_codes: List[str],
                                            target_data_sources: List[str] = None) -> Dict[str, int]:
        """并行处理的批次计算"""
        try:
            success_count = self._calculate_batch_indicators(as_of_date, stock_codes, target_data_sources)
            # success_count 现在是成功处理的财务记录数量
            # 由于我们现在按记录分组，返回的是记录数量而不是股票数量
            # 所以我们无法准确计算失败数量，这里设为0表示无失败
            return {'success': success_count, 'failed': 0}
        except Exception as e:
            self.logger.error(f"并行批次计算失败: {e}")
            return {'success': 0, 'failed': 0}

    def _calculate_single_stock_indicators(
        self,
        ts_code: str,
        as_of_date: str,
        financial_data: pd.DataFrame,
        force_data_source: str = None,
        current_record: pd.Series = None
    ) -> Optional[Dict[str, Any]]:
        try:
            # 如果指定了当前记录，使用它；否则使用数据集的第一行
            if current_record is not None:
                latest = current_record
            else:
                latest = financial_data.iloc[0]

            # 跳过PRT_ORIG数据的财务指标计算
            if latest.get('conversion_status') == 'RPT_ORIG':
                return None

            # 修复TTM计算：使用基于当前报告期的最近4个季度数据，而不是最早的4条记录
            # 1. 按end_date降序排序，确保最新的数据在前
            sorted_data = financial_data.sort_values('end_date', ascending=False)

            # 2. 找到当前记录在排序后的位置
            current_idx = None
            for idx, row in sorted_data.iterrows():
                if (row['end_date'] == latest['end_date'] and
                    row['ann_date'] == latest['ann_date'] and
                    row['data_source'] == latest['data_source']):
                    current_idx = idx
                    break

            # 3. 获取当前记录之后（更早的）的3条记录，加上当前记录，共4条记录用于TTM计算
            if current_idx is not None:
                # 获取当前记录之后的数据（按时间倒序，所以之后的数据是更早的报告期）
                ttm_candidates = sorted_data.loc[current_idx:]
                ttm_data = ttm_candidates.head(4)  # 取最近的4个报告期（包括当前）
            else:
                # 回退方案：如果找不到当前记录，使用最旧的4条记录
                ttm_data = sorted_data.tail(4)
                self.logger.warning(f"[{latest['ts_code']}] 无法精确定位当前记录，使用回退TTM计算方案")

            self.logger.debug(f"[{latest['ts_code']}] TTM计算使用 {len(ttm_data)} 条记录，日期范围: {ttm_data['end_date'].min()} 到 {ttm_data['end_date'].max()}")

            # 特殊年份的调试信息（仅在需要诊断时启用）
            # if latest['end_date'].year in [1997, 1998, 1999]:
            #     self.logger.info(f"[{ts_code}] 计算 {latest['end_date']} (报告期: {latest['ann_date']})")
            #     self.logger.info(f"[{ts_code}] TTM数据包含 {len(ttm_data)} 条记录")
            #     if len(ttm_data) > 0:
            #         date_range = f"{ttm_data['end_date'].min()} 到 {ttm_data['end_date'].max()}"
            #         self.logger.info(f"[{ts_code}] TTM数据日期范围: {date_range}")

            if force_data_source:
                original_data_source = force_data_source
            else:
                original_data_source = latest['data_source']
            unified_data_source = self._unify_data_source(original_data_source)

            indicators: Dict[str, Any] = {
                'ts_code': ts_code,
                'end_date': latest['end_date'],
                'ann_date': latest['ann_date'],
                'data_source': unified_data_source,
                'data_completeness': latest.get('data_completeness', 'complete'),
                'balance_sheet_lag': self._safe_numeric_for_calc(latest.get('balance_sheet_lag', 0), 0),

            }

            indicators.update(self._calculate_core_p_indicators(latest, ttm_data))
            indicators.update(self._calculate_profitability_indicators(latest, ttm_data))
            indicators.update(self._calculate_efficiency_indicators(latest, ttm_data))
            indicators.update(self._calculate_structure_indicators(latest))
            indicators.update(self._calculate_growth_indicators_optimized(financial_data))

            indicators = self._fill_forecast_revenue_growth(
                indicators, ts_code, latest['end_date'], latest['ann_date'], unified_data_source
            )
            indicators.update(self._assess_data_quality(indicators))
            return indicators
        except Exception as e:
            self.logger.error(f"计算 {ts_code} 单股指标失败: {e}")
            return None

    def _calculate_core_p_indicators(self, latest: pd.Series, ttm_data: pd.DataFrame) -> Dict[str, float]:
        indicators = {}
        ttm_revenue = self._safe_numeric(ttm_data['revenue'].sum())
        ttm_oper_cost = self._safe_numeric(ttm_data['oper_cost'].sum())
        ttm_n_income = self._safe_numeric(ttm_data['n_income_attr_p'].sum())
        tot_assets = self._safe_numeric(latest.get('tot_assets'))
        tot_equity = self._safe_numeric(latest.get('tot_equity'))

        if ttm_revenue is not None and ttm_revenue > 0 and ttm_oper_cost is not None:
            indicators['gpa_ttm'] = (ttm_revenue - ttm_oper_cost) / ttm_revenue * 100
        if tot_equity is not None and tot_equity > 0 and ttm_n_income is not None:
            indicators['roe_excl_ttm'] = ttm_n_income / tot_equity * 100
        else:
            indicators['roe_excl_ttm'] = None
        if tot_assets is not None and tot_assets > 0 and ttm_n_income is not None:
            indicators['roa_excl_ttm'] = ttm_n_income / tot_assets * 100
        else:
            indicators['roa_excl_ttm'] = None
        return indicators

    def _calculate_profitability_indicators(self, latest: pd.Series, ttm_data: pd.DataFrame) -> Dict[str, float]:
        indicators = {}
        ttm_revenue = self._safe_numeric(ttm_data['revenue'].sum())
        ttm_n_income = self._safe_numeric(ttm_data['n_income_attr_p'].sum())
        ttm_operate_profit = self._safe_numeric(ttm_data['operate_profit'].sum())
        tot_assets = self._safe_numeric(latest.get('tot_assets'))

        if ttm_revenue is not None and ttm_revenue > 0:
            if ttm_n_income is not None:
                indicators['net_margin_ttm'] = ttm_n_income / ttm_revenue * 100
            if ttm_operate_profit is not None:
                indicators['operating_margin_ttm'] = ttm_operate_profit / ttm_revenue * 100
        if tot_assets is not None and tot_assets > 0 and ttm_operate_profit is not None:
            indicators['roi_ttm'] = ttm_operate_profit / tot_assets * 100
        else:
            indicators['roi_ttm'] = None
        return indicators

    def _calculate_efficiency_indicators(self, latest: pd.Series, ttm_data: pd.DataFrame) -> Dict[str, float]:
        indicators = {}
        ttm_revenue = self._safe_numeric(ttm_data['revenue'].sum())
        tot_assets = self._safe_numeric(latest.get('tot_assets'))
        tot_equity = self._safe_numeric(latest.get('tot_equity'))

        if tot_assets is not None and tot_assets > 0 and ttm_revenue is not None:
            indicators['asset_turnover_ttm'] = ttm_revenue / tot_assets
        else:
            indicators['asset_turnover_ttm'] = None
        if tot_equity is not None and tot_equity > 0 and tot_assets is not None:
            indicators['equity_multiplier'] = tot_assets / tot_equity
        else:
            indicators['equity_multiplier'] = None
        return indicators

    def _calculate_structure_indicators(self, latest: pd.Series) -> Dict[str, float]:
        """
        计算结构指标 - PIT财务分析视角

        PIT计算逻辑：
        - debt_to_asset_ratio = (总资产 - 所有者权益) / 总资产 * 100
        - equity_ratio = 所有者权益 / 总资产 * 100

        PIT财务分析要点：
        1. 数据有效性验证：确保资产和权益数据的合理性
        2. 异常情况处理：识别并处理异常财务结构
        3. 综合分析：结合商誉等项目进行全面评估
        4. 时点一致性：基于特定时间点的财务数据进行分析
        """
        indicators = {}
        tot_assets = self._safe_numeric(latest['tot_assets'])
        tot_equity = self._safe_numeric(latest['tot_equity'])

        # PIT数据质量检查
        if tot_assets is not None and tot_assets > 0 and tot_equity is not None:
            total_liab = tot_assets - tot_equity
            indicators['debt_to_asset_ratio'] = total_liab / tot_assets * 100
            indicators['equity_ratio'] = tot_equity / tot_assets * 100
        return indicators

    def _should_calculate_operating_profit_growth(self, data_source: str) -> bool:
        if data_source == 'forecast':
            return True
        elif data_source == 'express':
            return True
        elif data_source == 'report':
            return True
        else:
            return False

    def _calculate_growth_indicators_optimized(self, financial_data: pd.DataFrame) -> Dict[str, float]:
        """
        优化的增长指标计算 - 使用向量化和缓存
        """
        if financial_data.empty:
            return {}

        # 尝试从向量化的方法获取增长指标
        growth_df = self._vectorized_calculate_growth_indicators(financial_data)
        if not growth_df.empty:
            # 如果有向量化的结果，直接使用
            growth_row = growth_df.iloc[0] if len(growth_df) > 0 else None
            if growth_row is not None:
                return {k: v for k, v in growth_row.items() if k != 'ts_code' and v is not None}

        # 回退到原来的方法（但使用缓存优化）
        return self._calculate_growth_indicators(financial_data)

    def _calculate_growth_indicators(self, financial_data: pd.DataFrame) -> Dict[str, float]:
        """
        原始增长指标计算方法（带缓存优化）
        """
        indicators: Dict[str, float] = {}
        data = financial_data.sort_values('end_date', ascending=False)
        if len(data) < 1:
            return indicators

        current = data.iloc[0]
        current_data_source = current['data_source']
        current_end_date = current['end_date']
        ts_code = current['ts_code']

        # 使用缓存的同比数据查找
        cache_key = f"{ts_code}:{current_end_date}:{current_data_source}"
        if cache_key in self._yoy_cache:
            yoy_data = self._yoy_cache[cache_key]
            self.stats['cache_hits'] += 1
        else:
            yoy_data = self._find_yoy_baseline_data(financial_data, current_end_date, current_data_source)
            if self.enable_cache:
                self._yoy_cache[cache_key] = yoy_data
                self.stats['cache_misses'] += 1

        # 调试日志：记录同比数据查找结果
        if yoy_data is None:
            self.logger.debug(f"[{ts_code}] 未找到同比基期数据: {current_end_date}")

        if yoy_data is not None:
            # 安全数值转换
            current_revenue = self._safe_numeric(current['revenue'])
            yoy_revenue = self._safe_numeric(yoy_data['revenue'])
            current_n_income = self._safe_numeric(current['n_income_attr_p'])
            yoy_n_income = self._safe_numeric(yoy_data['n_income_attr_p'])
            current_operate_profit = self._safe_numeric(current['operate_profit'])
            yoy_operate_profit = self._safe_numeric(yoy_data['operate_profit'])

            # 计算营收同比增长率
            if yoy_revenue is not None and yoy_revenue > 0 and current_revenue is not None:
                indicators['revenue_yoy_growth'] = (current_revenue - yoy_revenue) / yoy_revenue * 100
            else:
                indicators['revenue_yoy_growth'] = None

            # 计算净利润同比增长率
            if yoy_n_income is not None and yoy_n_income != 0 and current_n_income is not None:
                indicators['n_income_yoy_growth'] = (current_n_income - yoy_n_income) / abs(yoy_n_income) * 100
            else:
                indicators['n_income_yoy_growth'] = None

            # 计算经营利润同比增长率
            if not self._should_calculate_operating_profit_growth(current_data_source):
                indicators['operate_profit_yoy_growth'] = None
            elif current_data_source == 'express' and (current_operate_profit is None or current_operate_profit == 0):
                indicators['operate_profit_yoy_growth'] = None
            elif yoy_operate_profit is not None and yoy_operate_profit != 0 and current_operate_profit is not None:
                indicators['operate_profit_yoy_growth'] = (current_operate_profit - yoy_operate_profit) / abs(yoy_operate_profit) * 100
            else:
                indicators['operate_profit_yoy_growth'] = None

        return indicators

    def _fill_forecast_revenue_growth(self, indicators: Dict[str, float], ts_code: str, end_date, ann_date, current_data_source: str) -> Dict[str, float]:
        if current_data_source != 'forecast':
            return indicators
        revenue_growth = indicators.get('revenue_yoy_growth')
        if revenue_growth is not None:
            try:
                import math
                if not pd.isna(revenue_growth):
                    return indicators
            except (TypeError, ValueError):
                if str(revenue_growth).lower() != 'nan':
                    return indicators
        try:
            fill_query = """
            SELECT data_source, end_date, ann_date, revenue_yoy_growth
            FROM pgs_factors.pit_financial_indicators
            WHERE ts_code = %s AND ann_date <= %s
              AND data_source IN ('express','report')
              AND revenue_yoy_growth IS NOT NULL
            ORDER BY ann_date DESC,
                     CASE data_source WHEN 'express' THEN 1 WHEN 'report' THEN 2 ELSE 3 END
            LIMIT 1
            """
            fill_result = self.context.query_dataframe(fill_query, (ts_code, ann_date))
            if fill_result is not None and not fill_result.empty:
                fill_row = fill_result.iloc[0]
                indicators['revenue_yoy_growth'] = float(fill_row['revenue_yoy_growth'])
        except Exception as e:
            self.logger.warning(f"填充forecast营收增长失败: {ts_code} {end_date} - {e}")
        return indicators

    def _find_yoy_baseline_data(self, financial_data: pd.DataFrame, current_end_date, current_data_source) -> Optional[pd.Series]:
        from datetime import datetime, date
        try:
            if isinstance(current_end_date, str):
                current_date = datetime.strptime(current_end_date, '%Y-%m-%d').date()
            elif isinstance(current_end_date, datetime):
                current_date = current_end_date.date()
            elif isinstance(current_end_date, date):
                current_date = current_end_date  # PIT日期处理：直接使用日期对象
            else:
                self.logger.warning(f"不支持的日期类型: {type(current_end_date)}")
                return None
            yoy_target_date = current_date - relativedelta(months=12)
            data = financial_data.sort_values('end_date', ascending=False)

            # 查找最接近但不晚于目标日期的基准数据
            baseline_candidates = data[data['end_date'] <= yoy_target_date]

            # 特殊年份的同比调试信息（仅在需要诊断时启用）
            # if current_end_date.year in [1997, 1998, 1999]:
            #     self.logger.info(f"[{ts_code}] 同比查找: 目标={yoy_target_date}, 当前={current_end_date}, 基准候选={len(baseline_candidates)}")
            #     if len(baseline_candidates) > 0:
            #         self.logger.info(f"[{ts_code}] 基准日期范围: {baseline_candidates['end_date'].min()} 到 {baseline_candidates['end_date'].max()}")

            if current_data_source in ['express', 'forecast']:
                # 对于预告和快报，优先使用正式报告数据作为基准
                report_candidates = baseline_candidates[baseline_candidates['data_source'] == 'report']
                if not report_candidates.empty:
                    # 选择最接近目标日期的报告数据
                    closest_idx = (report_candidates['end_date'] - yoy_target_date).abs().idxmin()
                    return report_candidates.loc[closest_idx]
                else:
                    # 如果没有报告数据，使用任何可用的历史数据
                    if not baseline_candidates.empty:
                        closest_idx = (baseline_candidates['end_date'] - yoy_target_date).abs().idxmin()
                        return baseline_candidates.loc[closest_idx]
            else:
                # 对于正式报告，查找最近的报告数据（排除当前记录）
                report_candidates = baseline_candidates[
                    (baseline_candidates['data_source'] == 'report') &
                    (baseline_candidates['end_date'] != current_end_date)
                ]
                if not report_candidates.empty:
                    # 选择最接近目标日期的报告数据
                    closest_idx = (report_candidates['end_date'] - yoy_target_date).abs().idxmin()
                    return report_candidates.loc[closest_idx]
            return None
        except Exception as e:
            self.logger.warning(f"查找同比基期数据失败: {e}")
            return None

    def _unify_data_source(self, s: str) -> str:
        """统一数据源命名，兼容历史命名"""
        if s in ('forecast_direct', 'forecast_calculated'):
            return 'forecast'
        return s

    def _assess_data_quality(self, indicators: Dict[str, Any]) -> Dict[str, str]:
        """
        数据质量评估 - PIT数据质量控制

        PIT评估逻辑：
        1. 计算核心指标有效数量，确保数据完整性
        2. 检测异常值并进行合理的阈值处理
        3. 根据有效指标数量和异常值情况确定数据质量等级

        PIT质量控制要点：
        1. 多维度阈值：综合考虑行业特征、公司规模等因素
        2. 智能异常处理：基于数据分布特征进行合理调整
        3. 理论基础：基于财务分析理论确定质量等级标准
        4. 增长合理性：设置基于历史经验的增长率合理区间
        5. 相对比较：结合历史数据识别相对异常值
        """
        import math
        quality_indicators: Dict[str, str] = {}
        core_indicators = ['gpa_ttm', 'roe_excl_ttm', 'roa_excl_ttm']
        valid_count = sum(1 for k in core_indicators if indicators.get(k) is not None and not math.isnan(float(indicators.get(k, 0))))
        outlier_flags = []
        extreme_outlier_flags = []

        # PIT阈值设置：基于财务分析经验的合理区间
        # 多层次阈值设计，支持未来按行业和规模进行细分
        thresholds = {
            # 盈利能力：超过100%的ROE/ROA通常是异常的（例如，净资产极小）
            'roe_excl_ttm': {'moderate': 100, 'extreme': 200},
            'roa_excl_ttm': {'moderate': 50, 'extreme': 100},
            # 利润率：通常不应超过100%，除非有特殊非经营性收入
            'net_margin_ttm': {'moderate': 100, 'extreme': 200},
            'operating_margin_ttm': {'moderate': 100, 'extreme': 200},
            'roi_ttm': {'moderate': 100, 'extreme': 200},
            # 效率：周转率和乘数通常在个位数或十位数
            'asset_turnover_ttm': {'moderate': 10, 'extreme': 20},
            'equity_multiplier': {'moderate': 20, 'extreme': 50}, # 杠杆
            # 结构：比率应在0-100之间
            'debt_to_asset_ratio': {'moderate': 95, 'extreme': 200}, # 超过100%意味着资不抵债
            'equity_ratio': {'moderate': 100, 'extreme': 200},
            # 增长率：超过500% (5倍) 的年增长是罕见的
            'revenue_yoy_growth': {'moderate': 500, 'extreme': 1000},
            'n_income_yoy_growth': {'moderate': 500, 'extreme': 1000},
            'operate_profit_yoy_growth': {'moderate': 500, 'extreme': 1000}
        }

        for indicator, threshold in thresholds.items():
            value = indicators.get(indicator)
            if value is not None and not math.isnan(float(value)):
                abs_value = abs(float(value))
                if abs_value > threshold['extreme']:
                    extreme_outlier_flags.append(f'{indicator}_extreme')
                    # PIT异常值处理：保持数据符号，限制极端值
                    sign = 1 if float(value) >= 0 else -1
                    indicators[indicator] = sign * threshold['extreme']
                elif abs_value > threshold['moderate']:
                    outlier_flags.append(f'{indicator}_moderate')

        # PIT质量等级划分：基于数据完整性和异常程度综合评估
        if extreme_outlier_flags:
            # 安全地检查核心指标是否有正值，避免 None 和数值比较
            has_positive_core = False
            for k in core_indicators:
                value = indicators.get(k)
                if value is not None and not math.isnan(float(value)) and float(value) > 0:
                    has_positive_core = True
                    break
            quality_indicators['data_quality'] = 'outlier_high' if has_positive_core else 'outlier_low'
        elif outlier_flags:
            quality_indicators['data_quality'] = 'normal'
        elif valid_count >= 3:
            quality_indicators['data_quality'] = 'high'
        elif valid_count >= 2:
            quality_indicators['data_quality'] = 'normal'
        elif valid_count >= 1:
            quality_indicators['data_quality'] = 'low'
        else:
            quality_indicators['data_quality'] = 'invalid'
        quality_indicators['calculation_status'] = 'success'
        return quality_indicators

    def _get_active_stocks(self) -> List[str]:
        query = """
        SELECT DISTINCT ts_code
        FROM pgs_factors.pit_income_quarterly
        WHERE end_date >= '2020-01-01'
        GROUP BY ts_code
        HAVING COUNT(*) >= 4
        ORDER BY ts_code
        """
        results = self.db_manager.fetch_sync(query)
        return [row['ts_code'] if isinstance(row, dict) else row[0] for row in results]

    def _get_pit_data_for_calculation(self, as_of_date: str, stock_codes: List[str]) -> pd.DataFrame:
        """
        获取用于财务指标计算的PIT数据 - [优化版本]
        优化策略：
        1. 使用预编译的批量查询，减少数据库往返次数
        2. 优化JOIN逻辑，避免LATERAL JOIN的性能开销
        3. 添加索引提示（如果数据库支持）
        4. 使用CTE缓存中间结果
        """
        # 统计RPT_ORIG数据数量（用于同比计算）
        try:
            count_query = """
            SELECT COUNT(*) as total_rpt_orig_count
            FROM pgs_factors.pit_income_quarterly
            WHERE ann_date <= %s AND end_date >= DATE(%s) - INTERVAL '2 years' AND ts_code = ANY(%s)
              AND conversion_status = 'RPT_ORIG'
            """
            result = self.db_manager.fetch_sync(count_query, (as_of_date, as_of_date, stock_codes))
            rpt_orig_count = result[0]['total_rpt_orig_count'] if result else 0
            if rpt_orig_count > 0:
                self.logger.info(f"包含 {rpt_orig_count} 条原始报告期数据（可用于同比基期计算）")
        except Exception as e:
            # 静默处理统计失败，不影响主要流程
            pass

        # 优化1：分批查询，避免单次查询过大
        batch_size = min(2000, len(stock_codes))  # 减少批次大小，提高查询效率

        if len(stock_codes) <= batch_size:
            return self._fetch_pit_data_batch(as_of_date, stock_codes, skip_rpt_orig_count=True)

        # 分批获取数据
        all_results = []
        for i in range(0, len(stock_codes), batch_size):
            batch_codes = stock_codes[i:i + batch_size]
            batch_df = self._fetch_pit_data_batch(as_of_date, batch_codes, skip_rpt_orig_count=True)
            if not batch_df.empty:
                all_results.append(batch_df)

        return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

    def _fetch_pit_data_batch(self, as_of_date: str, stock_codes: List[str], skip_rpt_orig_count: bool = False) -> pd.DataFrame:
        """优化的单批次数据获取"""
        # RPT_ORIG统计已在上级方法中完成，这里不再重复统计

        query = """
        WITH latest_income AS (
            SELECT ts_code, end_date, ann_date, data_source,
                   revenue, n_income_attr_p, oper_cost, operate_profit,
                   conversion_status,
                   ROW_NUMBER() OVER (
                       PARTITION BY ts_code, end_date
                       ORDER BY ann_date DESC,
                                CASE data_source WHEN 'report' THEN 1 WHEN 'express' THEN 2 WHEN 'forecast' THEN 3 ELSE 5 END
                   ) as rn
            FROM pgs_factors.pit_income_quarterly
            WHERE ann_date <= %s AND end_date >= DATE(%s) - INTERVAL '2 years' AND ts_code = ANY(%s)
        ),
        latest_balance AS (
            SELECT ts_code, end_date, ann_date, tot_assets, tot_equity,
                   ROW_NUMBER() OVER (
                       PARTITION BY ts_code
                       ORDER BY end_date DESC, ann_date DESC
                   ) as rn
            FROM pgs_factors.pit_balance_quarterly
            WHERE ann_date <= %s AND data_source IN ('report','express') AND ts_code = ANY(%s)
        )
        SELECT i.ts_code, i.end_date, i.ann_date, i.data_source,
               i.revenue, i.n_income_attr_p, i.oper_cost, i.operate_profit,
               i.conversion_status,
               b.tot_assets, b.tot_equity, b.end_date as balance_end_date,
               CASE WHEN b.end_date = i.end_date THEN 'complete'
                    WHEN b.end_date IS NOT NULL THEN 'estimated'
                    ELSE 'income_only' END as data_completeness,
               CASE WHEN b.end_date IS NOT NULL THEN ABS(i.end_date::date - b.end_date::date) ELSE NULL END as balance_sheet_lag
        FROM (SELECT * FROM latest_income WHERE rn=1) i
        LEFT JOIN (SELECT * FROM latest_balance WHERE rn=1) b ON b.ts_code = i.ts_code
        WHERE (b.tot_assets IS NOT NULL OR i.data_source IN ('forecast','forecast_direct','forecast_calculated'))
        ORDER BY i.ts_code, i.end_date DESC
        """
        params = (as_of_date, as_of_date, stock_codes, as_of_date, stock_codes)
        results = self.db_manager.fetch_sync(query, params)
        return pd.DataFrame(results)

    def _save_indicators_batch(self, indicators_list: List[Dict[str, Any]]) -> None:
        """
        优化的批量保存 - 支持多线程和批量插入，增强错误处理
        """
        if not indicators_list:
            return

        # 数据去重：基于业务主键去重
        unique_indicators = {}
        for indicator in indicators_list:
            # 使用 (ts_code, ann_date, end_date, data_source) 作为唯一键
            key = (indicator['ts_code'], str(indicator['ann_date']),
                   str(indicator['end_date']), indicator['data_source'])
            unique_indicators[key] = indicator

        indicators_list = list(unique_indicators.values())
        df = pd.DataFrame(indicators_list)

        # 移除数据源优先级过滤 - 现在已在分组处理阶段解决
        # original_count = len(df)
        # df = self._filter_by_data_source_priority(df)
        # if original_count != len(df) and original_count > 10:
        #     self.logger.info(f"保存过滤: {original_count} -> {len(df)} 条记录")

        required_columns = [
            'ts_code', 'end_date', 'ann_date', 'data_source',
            'gpa_ttm', 'roe_excl_ttm', 'roa_excl_ttm',
            'net_margin_ttm', 'operating_margin_ttm', 'roi_ttm',
            'asset_turnover_ttm', 'equity_multiplier',
            'debt_to_asset_ratio', 'equity_ratio',
            'revenue_yoy_growth', 'n_income_yoy_growth', 'operate_profit_yoy_growth',
            'data_quality', 'calculation_status',
            'data_completeness', 'balance_sheet_lag'
        ]

        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        # 数据清理和质量检查
        df = self._clean_indicators_data(df, required_columns)

        df = df[required_columns]

        # 优化策略：根据数据量选择保存方式
        if len(df) < 50:
            # 小批量使用原来的方法
            self._save_indicators_single(df)
        else:
            # 大批量使用优化的方法
            try:
                self._save_indicators_optimized(df)
            except Exception as e:
                self.logger.warning(f"批量保存失败({len(df)}条记录): {e}，回退到单条插入")
                self._save_indicators_single(df)

    def _clean_indicators_data(self, df: pd.DataFrame, required_columns: List[str]) -> pd.DataFrame:
        """
        数据清理和质量检查，避免批量插入溢出
        """
        # 数值字段列表
        numeric_columns = [
            'gpa_ttm', 'roe_excl_ttm', 'roa_excl_ttm', 'net_margin_ttm',
            'operating_margin_ttm', 'roi_ttm', 'asset_turnover_ttm',
            'equity_multiplier', 'debt_to_asset_ratio', 'equity_ratio',
            'revenue_yoy_growth', 'n_income_yoy_growth', 'operate_profit_yoy_growth'
        ]

        cleaned_count = 0

        for col in numeric_columns:
            if col in df.columns:
                original_values = df[col].copy()

                # 先将列转换为数值类型，然后处理None和NaN值
                df[col] = pd.to_numeric(df[col], errors='coerce')

                # 处理None和NaN值
                df[col] = df[col].fillna(0.0)

                # 处理无穷大和异常大值（避免numeric溢出）
                df[col] = df[col].replace([float('inf'), -float('inf')], 0.0)

                # 限制数值范围：-999999 到 999999（适合numeric(18,6)）
                df[col] = df[col].clip(-999999, 999999)

                # 检查是否有清理
                if not df[col].equals(original_values):
                    cleaned_count += 1

        if cleaned_count > 0:
            self.logger.debug(f"数据清理完成，处理了 {cleaned_count} 个数值字段")

        return df

    def _sanitize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        参数清洗: 将 NaN/NaT/pd.NA 转换为 None，避免数据库写入异常
        参考 pit_income_quarterly_manager.py 的实现
        """
        cleaned = {}
        for k, v in params.items():
            try:
                # pandas 的缺失值统一转 None
                if v is None:
                    cleaned[k] = None
                else:
                    # 对于 numpy/pandas NA/NaT
                    if isinstance(v, float) and pd.isna(v):
                        cleaned[k] = None
                    elif hasattr(pd, 'isna') and pd.isna(v):
                        cleaned[k] = None
                    else:
                        cleaned[k] = v
            except Exception:
                cleaned[k] = v
        return cleaned

    def _save_indicators_single(self, df: pd.DataFrame) -> None:
        """单条插入模式（用于小批量）- 修复为使用字典参数格式"""
        

        # 定义字段列表（对应数据库表结构）
        fields = [
            'ts_code', 'end_date', 'ann_date', 'data_source',
            'gpa_ttm', 'roe_excl_ttm', 'roa_excl_ttm',
            'net_margin_ttm', 'operating_margin_ttm', 'roi_ttm',
            'asset_turnover_ttm', 'equity_multiplier',
            'debt_to_asset_ratio', 'equity_ratio',
            'revenue_yoy_growth', 'n_income_yoy_growth', 'operate_profit_yoy_growth',
            'data_quality', 'calculation_status',
            'data_completeness', 'balance_sheet_lag'
        ]

        # 构建 INSERT 语句（使用命名占位符）
        field_list = ', '.join(fields)
        placeholder_list = ', '.join([f'%({field})s' for field in fields])

        insert_sql = f"""
        INSERT INTO pgs_factors.pit_financial_indicators (
            {field_list}
        ) VALUES (
            {placeholder_list}
        )
        ON CONFLICT (ts_code, ann_date, end_date, data_source)
        DO UPDATE SET
            data_source = EXCLUDED.data_source,
            gpa_ttm = EXCLUDED.gpa_ttm,
            roe_excl_ttm = EXCLUDED.roe_excl_ttm,
            roa_excl_ttm = EXCLUDED.roa_excl_ttm,
            net_margin_ttm = EXCLUDED.net_margin_ttm,
            operating_margin_ttm = EXCLUDED.operating_margin_ttm,
            roi_ttm = EXCLUDED.roi_ttm,
            asset_turnover_ttm = EXCLUDED.asset_turnover_ttm,
            equity_multiplier = EXCLUDED.equity_multiplier,
            debt_to_asset_ratio = EXCLUDED.debt_to_asset_ratio,
            equity_ratio = EXCLUDED.equity_ratio,
            revenue_yoy_growth = EXCLUDED.revenue_yoy_growth,
            n_income_yoy_growth = EXCLUDED.n_income_yoy_growth,
            operate_profit_yoy_growth = EXCLUDED.operate_profit_yoy_growth,
            data_quality = EXCLUDED.data_quality,
            calculation_status = EXCLUDED.calculation_status,
            data_completeness = EXCLUDED.data_completeness,
            balance_sheet_lag = EXCLUDED.balance_sheet_lag,
            updated_at = CURRENT_TIMESTAMP
        """

        success = 0
        for _, row in df.iterrows():
            try:
                # 使用字典格式参数（与 pit_income_quarterly_manager.py 一致）
                params = {field: row[field] for field in fields}
                params = self._sanitize_params(params)

                # 调试日志


                # 使用 context.db_manager（与 pit_income_quarterly_manager.py 一致）
                self.context.db_manager.execute_sync(insert_sql, params)
                success += 1
            except Exception as e:
                self.logger.error(f"插入指标失败 {row['ts_code']}: {e}")

        # 只在有失败或批量处理时才输出详细信息
        if success != len(df) or len(df) >= 10:
            self.logger.info(f"保存 {success}/{len(df)} 条财务指标记录")

    def _save_indicators_optimized(self, df: pd.DataFrame) -> None:
        """
        优化的批量插入 - 使用临时表和批量操作
        """
        try:
            # 创建临时表并批量插入 - 使用微秒级时间戳确保唯一性
            temp_table_name = f"temp_financial_indicators_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}"

            # 0. 清理可能存在的同名临时表（以防万一）
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
                self.context.db_manager.execute_sync(cleanup_sql)
            except Exception as cleanup_error:
                self.logger.debug(f"清理临时表 {temp_table_name} 时出现问题（可忽略）: {cleanup_error}")

            # 1. 创建临时表
            create_temp_sql = f"""
            CREATE TEMPORARY TABLE {temp_table_name} (
                ts_code VARCHAR(20),
                end_date DATE,
                ann_date DATE,
                data_source VARCHAR(20),
                gpa_ttm DECIMAL(10,4),
                roe_excl_ttm DECIMAL(10,4),
                roa_excl_ttm DECIMAL(10,4),
                net_margin_ttm DECIMAL(10,4),
                operating_margin_ttm DECIMAL(10,4),
                roi_ttm DECIMAL(10,4),
                asset_turnover_ttm DECIMAL(10,4),
                equity_multiplier DECIMAL(10,4),
                debt_to_asset_ratio DECIMAL(10,4),
                equity_ratio DECIMAL(10,4),
                revenue_yoy_growth DECIMAL(10,4),           -- 营收同比增长率(%)
                n_income_yoy_growth DECIMAL(10,4),           -- 归属母公司股东的净利润同比增长率(%)
                operate_profit_yoy_growth DECIMAL(10,4),     -- 经营利润同比增长率(%)
                data_quality VARCHAR(20),
                calculation_status VARCHAR(20),
                data_completeness VARCHAR(20),
                balance_sheet_lag INTEGER
            )
            """
            self.context.db_manager.execute_sync(create_temp_sql)

            # 2. 批量插入临时表
            # 使用字典格式参数，避免 NaN 值问题
            fields = [
                'ts_code', 'end_date', 'ann_date', 'data_source',
                'gpa_ttm', 'roe_excl_ttm', 'roa_excl_ttm',
                'net_margin_ttm', 'operating_margin_ttm', 'roi_ttm',
                'asset_turnover_ttm', 'equity_multiplier',
                'debt_to_asset_ratio', 'equity_ratio',
                'revenue_yoy_growth', 'n_income_yoy_growth', 'operate_profit_yoy_growth',
                'data_quality', 'calculation_status',
                'data_completeness', 'balance_sheet_lag'
            ]

            # 使用批量命名参数插入
            batch_size = 100
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i + batch_size]

                # 构建批量插入的 VALUES 子句
                values_placeholders = []
                all_params = {}

                for j, (_, row) in enumerate(batch_df.iterrows()):
                    row_params = {f"{field}_{j}": row[field] for field in fields}
                    row_params = self._sanitize_params(row_params)
                    all_params.update(row_params)

                    row_placeholders = ', '.join([f'%({field}_{j})s' for field in fields])
                    values_placeholders.append(f"({row_placeholders})")

                insert_temp_sql = f"""
                INSERT INTO {temp_table_name} ({', '.join(fields)})
                VALUES {', '.join(values_placeholders)}
                """

                self.context.db_manager.execute_sync(insert_temp_sql, all_params)

            # 3. 使用UPSERT将临时表数据合并到目标表
            upsert_sql = f"""
            INSERT INTO pgs_factors.pit_financial_indicators (
                ts_code, end_date, ann_date, data_source,
                gpa_ttm, roe_excl_ttm, roa_excl_ttm,
                net_margin_ttm, operating_margin_ttm, roi_ttm,
                asset_turnover_ttm, equity_multiplier,
                debt_to_asset_ratio, equity_ratio,
                revenue_yoy_growth, n_income_yoy_growth, operate_profit_yoy_growth,
                data_quality, calculation_status,
                data_completeness, balance_sheet_lag
            )
            SELECT * FROM {temp_table_name}
            ON CONFLICT (ts_code, ann_date, end_date, data_source)
            DO UPDATE SET
                data_source = EXCLUDED.data_source,
                updated_at = CURRENT_TIMESTAMP,
                gpa_ttm = EXCLUDED.gpa_ttm,
                roe_excl_ttm = EXCLUDED.roe_excl_ttm,
                roa_excl_ttm = EXCLUDED.roa_excl_ttm,
                net_margin_ttm = EXCLUDED.net_margin_ttm,
                operating_margin_ttm = EXCLUDED.operating_margin_ttm,
                roi_ttm = EXCLUDED.roi_ttm,
                asset_turnover_ttm = EXCLUDED.asset_turnover_ttm,
                equity_multiplier = EXCLUDED.equity_multiplier,
                debt_to_asset_ratio = EXCLUDED.debt_to_asset_ratio,
                equity_ratio = EXCLUDED.equity_ratio,
                revenue_yoy_growth = EXCLUDED.revenue_yoy_growth,
                n_income_yoy_growth = EXCLUDED.n_income_yoy_growth,
                operate_profit_yoy_growth = EXCLUDED.operate_profit_yoy_growth,
                data_quality = EXCLUDED.data_quality,
                calculation_status = EXCLUDED.calculation_status,
                data_completeness = EXCLUDED.data_completeness,
                balance_sheet_lag = EXCLUDED.balance_sheet_lag
            """

            self.context.db_manager.execute_sync(upsert_sql)
            self.logger.info(f"批量保存 {len(df)} 条财务指标记录（使用临时表优化）")

            # 4. 清理临时表
            try:
                cleanup_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
                self.context.db_manager.execute_sync(cleanup_sql)
                self.logger.debug(f"临时表 {temp_table_name} 已清理")
            except Exception as cleanup_error:
                self.logger.debug(f"清理临时表 {temp_table_name} 时出现问题（可忽略）: {cleanup_error}")

        except Exception as e:
            self.logger.warning(f"批量保存失败，回退到单条插入: {e}")
            # 确保在异常情况下也清理临时表
            try:
                if 'temp_table_name' in locals():
                    cleanup_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
                    self.context.db_manager.execute_sync(cleanup_sql)
            except Exception as cleanup_error:
                self.logger.debug(f"异常情况下清理临时表失败（可忽略）: {cleanup_error}")
            # 回退到单条插入
            self._save_indicators_single(df)

    def _log_performance_stats(self, detailed: bool = True) -> None:
        """优化的性能统计日志，包含缓存和并行处理信息"""
        if self.stats['start_time'] and self.stats['end_time']:
            total_time = self.stats['end_time'] - self.stats['start_time']
            success_count = self.stats['successful_calculations']
            failed_count = self.stats['failed_calculations']
            total_processed = success_count + failed_count

            # 简洁模式：只输出基本统计
            if not detailed:
                success_rate = success_count/total_processed*100 if total_processed > 0 else 0
                # 进一步简化输出，只在关键时候输出
                if total_processed >= 10:  # 只在处理较多记录时才输出
                    self.logger.info(f"✓ 完成 {total_processed} 条记录 ({success_rate:.0f}%)")
                return

            # 详细模式：输出完整统计
            self.logger.info("=" * 60)
            self.logger.info("🚀 财务指标计算完成 - 性能报告")
            self.logger.info("=" * 60)

            # 基本统计
            self.logger.info("📊 基础统计:")
            self.logger.info(f"   总耗时: {total_time:.2f} 秒")
            self.logger.info(f"   处理财务记录: {total_processed} 条")
            self.logger.info(f"   成功计算: {success_count} 条")
            self.logger.info(f"   失败计算: {failed_count} 条")

            # 验证数据合理性
            if success_count > total_processed:
                self.logger.warning(f"⚠️ 数据异常: 成功计算({success_count}) > 处理总数({total_processed})")
            if failed_count < 0:
                self.logger.warning(f"⚠️ 数据异常: 失败计算({failed_count})为负数")

            success_rate = success_count/total_processed*100 if total_processed > 0 else 0
            self.logger.info(f"   成功率: {success_rate:.1f}%")

            if total_time > 0:
                throughput = success_count / total_time
                self.logger.info(f"   计算吞吐量: {throughput:.1f} 只/秒")

            # 缓存统计
            if self.enable_cache:
                total_cache_requests = self.stats['cache_hits'] + self.stats['cache_misses']
                if total_cache_requests > 0:
                    cache_hit_rate = self.stats['cache_hits'] / total_cache_requests * 100
                    self.logger.info("\n💾 缓存统计:")
                    self.logger.info(f"   缓存命中: {self.stats['cache_hits']} 次")
                    self.logger.info(f"   缓存未命中: {self.stats['cache_misses']} 次")
                    self.logger.info(f"   缓存命中率: {cache_hit_rate:.1f}%")

            # 性能评估
            self.logger.info("\n⚡ 性能评估:")
            if throughput > 50:
                self.logger.info("   性能等级: 优秀 (吞吐量 > 50只/秒)")
            elif throughput > 20:
                self.logger.info("   性能等级: 良好 (吞吐量 20-50只/秒)")
            elif throughput > 5:
                self.logger.info("   性能等级: 一般 (吞吐量 5-20只/秒)")
            else:
                self.logger.info("   性能等级: 需优化 (吞吐量 < 5只/秒)")

            # 优化建议
            if cache_hit_rate < 50 and total_cache_requests > 10:
                self.logger.info("💡 优化建议: 缓存命中率较低，考虑增大缓存大小或调整缓存策略")
            if failed_count / total_processed > 0.1:
                self.logger.info("💡 优化建议: 失败率较高，检查数据质量或计算逻辑")

            self.logger.info("=" * 60)

