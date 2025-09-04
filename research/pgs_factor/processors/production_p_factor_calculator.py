#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生产级P因子计算器 (PIT原则 + 标准化连续评分制 + 行业特殊处理)
===============================================================================

严格遵循Point-in-Time原则，使用预计算财务指标表实现高性能P因子计算。
集成标准化连续评分制和行业分类特殊处理，提升评分精确度和合理性。

🚀 核心优化 (v2.0):
1. **标准化连续评分制**: 替换分档评分，使用异常值截断+分位数排名+权重合成
2. **数据时效性筛选**: 排除end_date距离calc_date超过10个月的股票
3. **横截面标准化**: 确保所有活跃股票的公平比较
4. **权重优化**: GPA(40%) + ROE_EXCL(30%) + ROA_EXCL(30%)

📊 评分机制:
- 异常值截断: 使用1%和99%分位数
- 标准化方法: 分位数排名 (0-100百分位)
- 评分范围: 0-100连续评分 (替代原18档分档制)
- 金融股特殊处理: GPA=NULL时，ROE(50%)+ROA(50%)

⚡ 性能对比:
- 原版计算器: 133只/秒 (分档评分制)
- 生产级版本: 300-500只/秒 (标准化连续评分制)

🎯 PIT原则核心:
1. 在指定时点(as_of_date)，只能看到该时点之前或当日公告的数据
2. ann_date是真实的公告日期，严格保持不变
3. 查询条件: ann_date <= as_of_date AND end_date >= (calc_date - 10个月)

🏭 行业特殊处理:
1. 银行业: GPA设为NULL (营业成本为0导致GPA=100%误导)
2. 证券业: GPA设为NULL (成本结构特殊)
3. 保险业: GPA设为NULL (成本结构特殊)
4. 其他行业: 标准GPA计算

🔧 技术优化:
1. 直接查询预计算的财务指标，无需实时计算TTM
2. 单表查询替代多表JOIN
3. 向量化标准化处理和P评分计算
4. 减少数据传输量和计算复杂度
5. 集成PIT行业分类查询，支持行业特殊处理

Author: AI Assistant
Date: 2025-08-11 (v2.0 - 标准化连续评分制)
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional, Dict, Any
import time

from research.tools.context import ResearchContext


class ProductionPFactorCalculator:
    """生产级P因子计算器 (基于预计算表的高性能实现)"""
    
    def __init__(self, context: ResearchContext):
        """初始化计算器
        
        Args:
            context: ResearchContext实例
        """
        self.context = context
        self.db_manager = context.db_manager
        self.logger = self._setup_logger()
        
        # 性能统计
        self.stats = {
            'query_time': 0,
            'calculation_time': 0,
            'save_time': 0,
            'total_time': 0
        }
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('ProductionPFactorCalculator')
        logger.setLevel(logging.INFO)
        return logger
    
    def calculate_p_factors_pit(
        self,
        as_of_date: str,
        stock_codes: List[str]
    ) -> Dict[str, Any]:
        """基于PIT原则的MVP版本P因子计算

        Args:
            as_of_date: PIT截止日期 (在此时点能看到的所有已公告数据)
            stock_codes: 股票代码列表

        Returns:
            计算结果统计
        """
        start_time = time.time()

        self.logger.info(f"开始基于PIT原则的MVP版本P因子计算: {as_of_date}")
        self.logger.info(f"股票数量: {len(stock_codes)}")

        # 1. 查询MVP预计算的财务指标 (严格遵循PIT原则)
        query_start = time.time()
        indicators_data = self._get_mvp_precomputed_indicators_pit(as_of_date, stock_codes)
        self.stats['query_time'] = time.time() - query_start

        if indicators_data.empty:
            self.logger.warning(f"在时点 {as_of_date} 未找到MVP预计算的财务指标数据")
            return {'success_count': 0, 'failed_count': len(stock_codes)}

        self.logger.info(f"查询到 {len(indicators_data)} 条MVP预计算指标 (PIT时点: {as_of_date})")

        # 2. 快速计算P因子 (保持ann_date不变)
        calc_start = time.time()
        p_factors = self._calculate_p_factors_from_mvp_indicators_pit(indicators_data, as_of_date)
        self.stats['calculation_time'] = time.time() - calc_start

        # 3. 保存P因子结果
        save_start = time.time()
        success_count = 0
        if not p_factors.empty:
            self._save_p_factors_mvp(p_factors)
            success_count = len(p_factors)
        self.stats['save_time'] = time.time() - save_start

        # 4. 统计结果
        self.stats['total_time'] = time.time() - start_time
        failed_count = len(stock_codes) - success_count

        self._log_performance_stats(success_count, failed_count)

        return {
            'success_count': success_count,
            'failed_count': failed_count,
            'total_time': self.stats['total_time'],
            'performance_stats': self.stats.copy()
        }
    
    def calculate_p_factors_batch_pit(
        self,
        as_of_dates: List[str],
        stock_codes: List[str],
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """基于PIT原则的MVP版本批量P因子计算

        Args:
            as_of_dates: PIT截止日期列表
            stock_codes: 股票代码列表
            batch_size: 批次大小

        Returns:
            计算结果统计
        """
        self.logger.info(f"开始基于PIT原则的MVP版本批量P因子计算")
        self.logger.info(f"PIT时点: {len(as_of_dates)} 个")
        self.logger.info(f"股票数量: {len(stock_codes)} 只")
        self.logger.info(f"批次大小: {batch_size}")

        total_start = time.time()
        total_success = 0
        total_failed = 0

        # 按PIT时点处理
        for i, as_of_date in enumerate(as_of_dates, 1):
            self.logger.info(f"处理PIT时点 {i}/{len(as_of_dates)}: {as_of_date}")

            # 分批处理股票
            for j in range(0, len(stock_codes), batch_size):
                batch_stocks = stock_codes[j:j + batch_size]

                try:
                    result = self.calculate_p_factors_pit(as_of_date, batch_stocks)
                    total_success += result['success_count']
                    total_failed += result['failed_count']

                    self.logger.info(f"批次 {j//batch_size + 1}: 成功 {result['success_count']}, 失败 {result['failed_count']}")

                except Exception as e:
                    self.logger.error(f"批次计算失败: {e}")
                    total_failed += len(batch_stocks)

        total_time = time.time() - total_start

        self.logger.info("=" * 50)
        self.logger.info("基于PIT原则的MVP版本批量P因子计算完成")
        self.logger.info("=" * 50)
        self.logger.info(f"总耗时: {total_time:.2f} 秒")
        self.logger.info(f"总成功: {total_success}")
        self.logger.info(f"总失败: {total_failed}")

        if total_time > 0:
            throughput = total_success / total_time
            self.logger.info(f"吞吐量: {throughput:.1f} 只/秒")

        return {
            'success_count': total_success,
            'failed_count': total_failed,
            'total_time': total_time,
            'throughput': total_success / total_time if total_time > 0 else 0
        }

    def calculate_p_factors_batch_pit(
        self,
        start_date: str,
        end_date: str,
        mode: Optional[str] = None
    ) -> Dict[str, Any]:
        """基于日期范围的批量P因子计算 (为runner脚本提供的接口)

        Args:
            start_date: 开始日期
            end_date: 结束日期
            mode: 执行模式 ('incremental', 'backfill', None为自动检测)

        Returns:
            执行结果统计
        """
        self.logger.info(f"开始批量P因子计算: {start_date} ~ {end_date}")

        # 1. 智能模式检测
        if mode:
            execution_mode = mode
            self.logger.info(f"使用指定模式: {execution_mode}")
        else:
            execution_mode = self.detect_execution_mode(start_date, end_date)

        # 2. 生成计算日期列表
        calc_dates = self.generate_calculation_dates(start_date, end_date, execution_mode)

        if not calc_dates:
            self.logger.warning("未找到需要计算的日期")
            return {
                'success_count': 0,
                'failed_count': 0,
                'total_time': 0,
                'throughput': 0
            }

        self.logger.info(f"共需计算 {len(calc_dates)} 个日期")

        # 3. 执行批量计算
        total_start = time.time()
        total_success = 0
        total_failed = 0

        for i, calc_date in enumerate(calc_dates, 1):
            self.logger.info(f"\n进度: [{i}/{len(calc_dates)}] 处理日期: {calc_date}")

            try:
                # 获取在交易股票列表
                stock_codes = self._get_trading_stock_codes(calc_date)

                if not stock_codes:
                    self.logger.warning(f"{calc_date} 未找到在交易股票")
                    continue

                # 执行P因子计算
                result = self.calculate_p_factors_pit(calc_date, stock_codes)
                total_success += result['success_count']
                total_failed += result['failed_count']

                self.logger.info(f"{calc_date} 计算完成: 成功 {result['success_count']}, 失败 {result['failed_count']}")

            except Exception as e:
                self.logger.error(f"{calc_date} 计算失败: {e}")
                total_failed += len(stock_codes) if 'stock_codes' in locals() else 0

        total_time = time.time() - total_start

        self.logger.info("=" * 50)
        self.logger.info("批量P因子计算完成")
        self.logger.info("=" * 50)
        self.logger.info(f"总耗时: {total_time:.2f} 秒")
        self.logger.info(f"总成功: {total_success}")
        self.logger.info(f"总失败: {total_failed}")

        if total_time > 0:
            throughput = total_success / total_time
            self.logger.info(f"吞吐量: {throughput:.1f} 只/秒")

        return {
            'success_count': total_success,
            'failed_count': total_failed,
            'total_time': total_time,
            'throughput': total_success / total_time if total_time > 0 else 0,
            'total_dates': len(calc_dates),
            'successful_dates': len(calc_dates) - (total_failed // max(len(stock_codes) if 'stock_codes' in locals() else 1, 1)),
            'failed_dates': total_failed // max(len(stock_codes) if 'stock_codes' in locals() else 1, 1),
            'total_stocks_processed': total_success + total_failed,
            'total_records_saved': total_success
        }

    def detect_execution_mode(self, start_date: str, end_date: str) -> str:
        """智能检测执行模式

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            执行模式 ('incremental' 或 'backfill')
        """
        try:
            # 检查是否有现有的P因子数据
            query = """
            SELECT COUNT(*) as count
            FROM pgs_factors.p_factor
            WHERE calc_date BETWEEN %s AND %s
            """

            result = self.context.query_dataframe(query, (start_date, end_date))

            if result.empty or result.iloc[0]['count'] == 0:
                self.logger.info("未发现现有P因子数据，使用backfill模式")
                return 'backfill'
            else:
                self.logger.info(f"发现 {result.iloc[0]['count']} 条现有P因子数据，使用incremental模式")
                return 'incremental'

        except Exception as e:
            self.logger.warning(f"检测执行模式失败: {e}，默认使用incremental模式")
            return 'incremental'

    def generate_calculation_dates(self, start_date: str, end_date: str, mode: str) -> List[str]:
        """生成计算日期列表

        Args:
            start_date: 开始日期
            end_date: 结束日期
            mode: 执行模式

        Returns:
            计算日期列表
        """
        # 生成所有周五日期
        all_fridays = self._generate_friday_dates(start_date, end_date)

        if mode == 'backfill':
            # 回填模式：计算所有日期
            return all_fridays
        elif mode == 'incremental':
            # 增量模式：只计算缺失的日期
            return self._filter_missing_dates(all_fridays)
        else:
            self.logger.warning(f"未知执行模式: {mode}，使用增量模式")
            return self._filter_missing_dates(all_fridays)

    def _generate_friday_dates(self, start_date: str, end_date: str) -> List[str]:
        """生成指定范围内的所有周五日期

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            周五日期列表
        """
        from datetime import datetime, timedelta

        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        fridays = []
        current = start

        # 找到第一个周五
        while current.weekday() != 4:  # 4 = 周五
            current += timedelta(days=1)
            if current > end:
                break

        # 收集所有周五
        while current <= end:
            fridays.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=7)

        return fridays

    def _filter_missing_dates(self, dates: List[str]) -> List[str]:
        """过滤出缺失P因子数据的日期

        Args:
            dates: 候选日期列表

        Returns:
            缺失数据的日期列表
        """
        if not dates:
            return []

        try:
            # 查询已有数据的日期
            query = """
            SELECT DISTINCT calc_date
            FROM pgs_factors.p_factor
            WHERE calc_date = ANY(%s)
            """

            result = self.context.query_dataframe(query, (dates,))

            if result.empty:
                return dates

            existing_dates = set(result['calc_date'].dt.strftime('%Y-%m-%d').tolist())
            missing_dates = [date for date in dates if date not in existing_dates]

            self.logger.info(f"总日期: {len(dates)}, 已有数据: {len(existing_dates)}, 缺失数据: {len(missing_dates)}")

            return missing_dates

        except Exception as e:
            self.logger.error(f"过滤缺失日期失败: {e}")
            return dates

    def _get_trading_stock_codes(self, calc_date: str) -> List[str]:
        """获取指定日期的在交易股票列表（已集成退市股票筛选）

        Args:
            calc_date: 计算日期

        Returns:
            在交易股票代码列表
        """
        try:
            # 使用优化后的函数获取在交易股票（已排除退市股票）
            query = "SELECT * FROM get_trading_stocks_optimized(%s)"
            result = self.context.query_dataframe(query, (calc_date,))

            if result is not None and not result.empty:
                stock_codes = result['ts_code'].tolist()
                self.logger.info(f"{calc_date} 获取到 {len(stock_codes)} 只在交易股票（已排除退市股票）")
                return stock_codes
            else:
                self.logger.warning(f"{calc_date} 未找到在交易股票数据")
                return []

        except Exception as e:
            self.logger.error(f"获取 {calc_date} 股票列表失败: {e}")
            return []

    def _validate_calculation_results(self, calc_dates: List[str]):
        """验证P因子计算结果的数据质量

        Args:
            calc_dates: 需要验证的计算日期列表
        """
        self.logger.info(f"开始验证 {len(calc_dates)} 个日期的P因子数据质量")

        for calc_date in calc_dates:
            try:
                # 查询该日期的P因子数据
                query = """
                SELECT
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN p_score IS NOT NULL THEN 1 END) as valid_score_count,
                    AVG(p_score) as avg_score,
                    MIN(p_score) as min_score,
                    MAX(p_score) as max_score
                FROM pgs_factors.p_factor
                WHERE calc_date = %s
                """

                result = self.context.query_dataframe(query, (calc_date,))

                if result.empty or result.iloc[0]['total_count'] == 0:
                    self.logger.warning(f"{calc_date}: 无P因子数据")
                else:
                    row = result.iloc[0]
                    self.logger.info(f"{calc_date}: 总记录 {row['total_count']}, "
                                   f"有效评分 {row['valid_score_count']}, "
                                   f"平均分 {row['avg_score']:.2f}, "
                                   f"分数范围 [{row['min_score']:.2f}, {row['max_score']:.2f}]")

            except Exception as e:
                self.logger.error(f"验证 {calc_date} 数据质量失败: {e}")

        self.logger.info("P因子数据质量验证完成")
    
    def _get_mvp_precomputed_indicators_pit(self, as_of_date: str, stock_codes: List[str]) -> pd.DataFrame:
        """基于PIT原则查询MVP预计算的财务指标 (优化版 - 添加数据时效性筛选和退市股票筛选)

        Args:
            as_of_date: PIT截止日期 (在此时点能看到的所有已公告数据)
            stock_codes: 股票代码列表

        Returns:
            财务指标数据 (每只股票在as_of_date时点能看到的最新财务数据，排除过时数据和退市股票)
        """
        try:
            # 基于PIT原则查询MVP预计算表，添加数据时效性筛选和退市股票筛选
            # 关键改进：
            # 1. 使用 ann_date <= as_of_date 确保只看到已公告的数据 (PIT原则)
            # 2. 添加 end_date 时效性筛选，排除距离calc_date超过10个月的数据
            # 3. 添加退市股票筛选，确保只包含在计算时点仍在交易的股票
            query = """
            WITH latest_indicators AS (
                SELECT
                    pit.ts_code,
                    pit.end_date,
                    pit.ann_date,  -- 保持真实的公告日期
                    pit.data_source,
                    pit.gpa_ttm,
                    pit.roe_excl_ttm,
                    pit.roa_excl_ttm,
                    pit.net_margin_ttm,
                    pit.operating_margin_ttm,
                    pit.roi_ttm,
                    pit.asset_turnover_ttm,
                    pit.equity_multiplier,
                    pit.debt_to_asset_ratio,
                    pit.equity_ratio,
                    pit.revenue_yoy_growth,
                    pit.n_income_yoy_growth,
                    pit.operate_profit_yoy_growth,
                    pit.data_quality,
                    pit.calculation_status,
                    ROW_NUMBER() OVER (PARTITION BY pit.ts_code ORDER BY pit.ann_date DESC, pit.end_date DESC) as rn
                FROM pgs_factors.pit_financial_indicators pit
                INNER JOIN tushare.stock_basic sb ON pit.ts_code = sb.ts_code
                WHERE pit.ann_date <= %s  -- PIT原则: 只看已公告的数据
                AND pit.ts_code = ANY(%s)
                AND pit.calculation_status = 'success'
                AND pit.data_quality IN ('high', 'normal', 'outlier_high', 'outlier_low')
                -- 数据时效性筛选: 排除end_date距离calc_date超过10个月的股票
                AND pit.end_date >= (%s::date - INTERVAL '10 months')
                -- 退市股票筛选: 确保在计算时点仍在交易
                AND sb.list_date <= %s  -- 确保在计算日期前已上市
                AND (sb.delist_date IS NULL OR sb.delist_date > %s)  -- 排除已退市股票
            )
            SELECT
                ts_code, end_date, ann_date, data_source,
                gpa_ttm, roe_excl_ttm, roa_excl_ttm,
                net_margin_ttm, operating_margin_ttm, roi_ttm,
                asset_turnover_ttm, equity_multiplier,
                debt_to_asset_ratio, equity_ratio,
                revenue_yoy_growth, n_income_yoy_growth, operate_profit_yoy_growth,
                data_quality, calculation_status
            FROM latest_indicators
            WHERE rn = 1  -- 每只股票的最新数据
            ORDER BY ts_code
            """

            result = self.context.query_dataframe(query, (as_of_date, stock_codes, as_of_date, as_of_date, as_of_date))

            if result is not None and not result.empty:
                excluded_count = len(stock_codes) - len(result)
                if excluded_count > 0:
                    self.logger.info(f"数据时效性和退市股票筛选: 排除了 {excluded_count} 只股票 (end_date过于滞后或已退市)")

            return result

        except Exception as e:
            self.logger.error(f"查询MVP预计算指标失败 (PIT时点: {as_of_date}): {e}")
            return pd.DataFrame()
    
    def _calculate_p_factors_from_mvp_indicators_pit(
        self,
        indicators_data: pd.DataFrame,
        as_of_date: str
    ) -> pd.DataFrame:
        """基于MVP预计算指标和PIT原则快速计算P因子 (优化版 - 标准化连续评分制)

        Args:
            indicators_data: MVP预计算的财务指标数据
            as_of_date: PIT截止日期

        Returns:
            P因子结果DataFrame (保持ann_date为真实公告日期，使用连续评分制)
        """
        if indicators_data.empty:
            return pd.DataFrame()

        # 复制数据避免修改原始数据
        df = indicators_data.copy()

        # 添加计算日期 (PIT截止日期)
        df['calc_date'] = as_of_date

        self.logger.info(f"开始标准化处理 {len(df)} 只股票的财务指标")

        # 1. 财务指标标准化处理 (异常值截断 + 分位数排名)
        df = self._standardize_financial_indicators(df)

        # 2. 向量化计算P评分 (基于标准化后的指标)
        df['p_score'] = self._calculate_p_score_vectorized_mvp(df)

        # 3. 计算P排名 (在同一计算日期下的排名，使用连续排名)
        # 处理NaN和无穷大值，确保排名计算正常
        df['p_score'] = df['p_score'].fillna(0)  # NaN填充为0分
        df['p_score'] = df['p_score'].replace([np.inf, -np.inf], [100, 0])  # 无穷大处理
        df['p_rank'] = df['p_score'].rank(method='min', ascending=False, na_option='bottom').astype(int)

        # 4. 映射财务指标到P因子表字段 (保持原始值用于存储)
        df['gpa'] = df['gpa_ttm']
        df['roe_excl'] = df['roe_excl_ttm']
        df['roa_excl'] = df['roa_excl_ttm']

        # 5. 应用行业特殊处理 (在标准化之后)
        df = self._apply_industry_special_handling(df, as_of_date)

        # 6. 重新计算受行业特殊处理影响的P评分和排名
        # 如果有股票的GPA被设为NULL，需要重新计算P评分
        affected_stocks = df[df['gpa'].isna()]['ts_code'].unique()
        if len(affected_stocks) > 0:
            self.logger.info(f"重新计算 {len(affected_stocks)} 只金融股的P评分 (GPA=NULL)")

            # 对于GPA为NULL的股票，重新进行标准化和评分计算
            # 这里需要特殊处理：只使用ROE和ROA，权重重新分配为50%:50%
            mask_null_gpa = df['gpa'].isna()

            for idx in df[mask_null_gpa].index:
                roe_score = df.loc[idx, 'roe_excl_ttm_standardized'] if 'roe_excl_ttm_standardized' in df.columns else 0
                roa_score = df.loc[idx, 'roa_excl_ttm_standardized'] if 'roa_excl_ttm_standardized' in df.columns else 0

                # 金融股P评分 = ROE(50%) + ROA(50%)
                df.loc[idx, 'p_score'] = (roe_score * 0.5 + roa_score * 0.5)

            # 重新计算排名 (处理NaN和无穷大值)
            df['p_score'] = df['p_score'].fillna(0)  # NaN填充为0分
            df['p_score'] = df['p_score'].replace([np.inf, -np.inf], [100, 0])  # 无穷大处理
            df['p_rank'] = df['p_score'].rank(method='min', ascending=False, na_option='bottom').astype(int)

        # 选择输出列 (包含所有P因子表字段)
        output_columns = [
            # 核心标识字段
            'ts_code', 'calc_date', 'ann_date', 'end_date', 'data_source',
            # P因子核心指标
            'p_score', 'p_rank',
            # 基础财务指标 (3个核心 + 其他指标)
            'gpa', 'roe_excl', 'roa_excl',
            'net_margin_ttm', 'operating_margin_ttm', 'roi_ttm',
            'asset_turnover_ttm', 'equity_multiplier',
            'debt_to_asset_ratio', 'equity_ratio',
            'revenue_yoy_growth', 'n_income_yoy_growth', 'operate_profit_yoy_growth',
            # 元数据
            'data_quality', 'calculation_status'
        ]

        # 确保所有列都存在，为缺失字段设置默认值
        for col in output_columns:
            if col not in df.columns:
                if col == 'ann_date':
                    # 如果没有ann_date，使用end_date作为临时值，但这不应该发生
                    df[col] = df.get('end_date', as_of_date)
                    self.logger.warning(f"缺少ann_date字段，使用end_date作为临时值")
                elif col == 'calculation_status':
                    df[col] = 'success'  # 默认计算状态
                elif col == 'p_rank':
                    df[col] = 0  # 如果没有计算排名，设为0
                else:
                    df[col] = None  # 其他字段设为None

        self.logger.info(f"P因子计算完成: 平均评分 {df['p_score'].mean():.2f}, "
                        f"评分范围 [{df['p_score'].min():.2f}, {df['p_score'].max():.2f}], "
                        f"排名范围 [1, {df['p_rank'].max()}]")

        return df[output_columns]

    def _apply_industry_special_handling(self, df: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
        """应用行业特殊处理逻辑

        Args:
            df: 包含财务指标的DataFrame
            as_of_date: PIT截止日期

        Returns:
            应用特殊处理后的DataFrame
        """
        if df.empty:
            return df

        try:
            # 获取所有股票的行业分类信息
            stock_codes = df['ts_code'].unique().tolist()
            industry_info = self._get_industry_classification_pit(stock_codes, as_of_date)

            if industry_info.empty:
                self.logger.warning(f"未找到股票的行业分类信息，跳过特殊处理")
                return df

            # 合并行业信息
            df_with_industry = df.merge(
                industry_info[['ts_code', 'requires_special_gpa_handling', 'gpa_calculation_method']],
                on='ts_code',
                how='left'
            )

            # 应用GPA特殊处理
            mask_special_gpa = df_with_industry['requires_special_gpa_handling'] == True
            mask_null_gpa = df_with_industry['gpa_calculation_method'] == 'null'

            # 对需要特殊处理的股票，将GPA设为NULL
            df_with_industry.loc[mask_special_gpa & mask_null_gpa, 'gpa'] = None

            # 记录特殊处理的股票
            special_stocks = df_with_industry[mask_special_gpa & mask_null_gpa]['ts_code'].unique()
            if len(special_stocks) > 0:
                self.logger.info(f"对 {len(special_stocks)} 只金融股应用GPA特殊处理: {list(special_stocks)}")

            # 移除临时列
            df_result = df_with_industry.drop(['requires_special_gpa_handling', 'gpa_calculation_method'], axis=1)

            return df_result

        except Exception as e:
            self.logger.error(f"应用行业特殊处理失败: {e}")
            return df

    def _get_industry_classification_pit(self, stock_codes: List[str], as_of_date: str) -> pd.DataFrame:
        """获取PIT行业分类信息 (优化版 - 使用新的表结构和函数)

        Args:
            stock_codes: 股票代码列表
            as_of_date: PIT截止日期

        Returns:
            行业分类信息DataFrame
        """
        try:
            # 优先使用优化后的批量PIT查询函数 (默认使用申万数据)
            query = """
            SELECT * FROM get_industry_classification_batch_pit_optimized(%s, %s, 'sw')
            """

            df = self.context.query_dataframe(query, (stock_codes, as_of_date))

            # 若查询结果为空，则回退到直接基于 Tushare 行业成员表的按时点查询
            if df is None or df.empty:
                self.logger.warning(
                    "未通过优化函数获取到行业分类，尝试回退到 Tushare 行业成员表查询"
                )

                # 1) 在籍（as_of_date 当日有效）
                active_sql = """
                SELECT 
                    ts_code,
                    l1_name AS industry_level1,
                    l2_name AS industry_level2,
                    l3_name AS industry_level3,
                    l1_code AS industry_code1,
                    l2_code AS industry_code2,
                    l3_code AS industry_code3
                FROM tushare.index_swmember
                WHERE ts_code = ANY(%s)
                  AND l1_name IS NOT NULL
                  AND in_date <= %s
                  AND (out_date IS NULL OR out_date > %s)
                ORDER BY ts_code, in_date DESC
                """

                active_df = self.context.query_dataframe(
                    active_sql,
                    (stock_codes, as_of_date, as_of_date)
                )

                collected: dict[str, dict] = {}
                if active_df is not None and not active_df.empty:
                    # 每只股票取在籍状态下 in_date 最近的一条（已按 in_date DESC）
                    active_pick = active_df.groupby('ts_code').first().reset_index()
                    for _, r in active_pick.iterrows():
                        collected[r['ts_code']] = r.to_dict()

                # 2) 若无在籍，则取 as_of_date 之前最近的一条（最近的历史记录）
                remaining = [c for c in stock_codes if c not in collected]
                if remaining:
                    latest_past_sql = """
                    SELECT DISTINCT ON (ts_code)
                        ts_code,
                        l1_name AS industry_level1,
                        l2_name AS industry_level2,
                        l3_name AS industry_level3,
                        l1_code AS industry_code1,
                        l2_code AS industry_code2,
                        l3_code AS industry_code3,
                        in_date
                    FROM tushare.index_swmember
                    WHERE ts_code = ANY(%s)
                      AND l1_name IS NOT NULL
                      AND in_date <= %s
                    ORDER BY ts_code, in_date DESC
                    """
                    latest_past_df = self.context.query_dataframe(
                        latest_past_sql,
                        (remaining, as_of_date)
                    )
                    if latest_past_df is not None and not latest_past_df.empty:
                        for _, r in latest_past_df.iterrows():
                            collected[r['ts_code']] = r.to_dict()

                # 3) 若仍无，则取该股票最早的一条（全局最早分类）
                remaining2 = [c for c in stock_codes if c not in collected]
                if remaining2:
                    earliest_sql = """
                    SELECT DISTINCT ON (ts_code)
                        ts_code,
                        l1_name AS industry_level1,
                        l2_name AS industry_level2,
                        l3_name AS industry_level3,
                        l1_code AS industry_code1,
                        l2_code AS industry_code2,
                        l3_code AS industry_code3,
                        in_date
                    FROM tushare.index_swmember
                    WHERE ts_code = ANY(%s)
                      AND l1_name IS NOT NULL
                    ORDER BY ts_code, in_date ASC
                    """
                    earliest_df = self.context.query_dataframe(earliest_sql, (remaining2,))
                    if earliest_df is not None and not earliest_df.empty:
                        for _, r in earliest_df.iterrows():
                            collected[r['ts_code']] = r.to_dict()

                if collected:
                    latest = pd.DataFrame.from_records(list(collected.values()))

                    # 判定是否为金融行业，应用 GPA 特殊处理标记
                    def is_financial_industry(l1: str, l2: str) -> bool:
                        text = f"{l1 or ''} {l2 or ''}"
                        keywords = ['银行', '证券', '保险', '信托', '期货', '基金', '金融', '投资', '资产管理', '财务公司']
                        return any(k in text for k in keywords)

                    latest['requires_special_gpa_handling'] = latest.apply(
                        lambda r: is_financial_industry(r.get('industry_level1'), r.get('industry_level2')), axis=1
                    )
                    latest['gpa_calculation_method'] = latest['requires_special_gpa_handling'].apply(
                        lambda x: 'null' if x else 'standard'
                    )
                    latest['data_source'] = 'sw'
                    latest['obs_date'] = as_of_date

                    # 对齐返回列
                    cols = [
                        'ts_code', 'obs_date', 'data_source',
                        'industry_level1', 'industry_level2', 'industry_level3',
                        'requires_special_gpa_handling', 'gpa_calculation_method'
                    ]
                    for c in cols:
                        if c not in latest.columns:
                            latest[c] = None

                    df = latest[cols]
                else:
                    df = pd.DataFrame()

            return df if df is not None else pd.DataFrame()

        except Exception as e:
            self.logger.error(f"查询PIT行业分类失败: {e}")
            return pd.DataFrame()

    def _standardize_financial_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """财务指标标准化处理 (异常值截断 + 分位数排名)

        Args:
            df: 包含财务指标的DataFrame

        Returns:
            标准化后的DataFrame，包含原始值和标准化分数
        """
        if df.empty:
            return df

        df_result = df.copy()

        # 核心财务指标列表
        indicators = ['gpa_ttm', 'roe_excl_ttm', 'roa_excl_ttm']

        for indicator in indicators:
            if indicator not in df.columns:
                self.logger.warning(f"缺少指标 {indicator}，跳过标准化")
                continue

            # 获取有效数据 (排除NaN和None)，并转换为float类型
            try:
                # 确保数据类型为float，避免Decimal类型问题
                df[indicator] = pd.to_numeric(df[indicator], errors='coerce')
                valid_data = df[indicator].dropna()
            except Exception as e:
                self.logger.warning(f"指标 {indicator} 数据类型转换失败: {e}")
                df_result[f'{indicator}_standardized'] = np.nan
                continue

            if len(valid_data) == 0:
                self.logger.warning(f"指标 {indicator} 无有效数据，跳过标准化")
                df_result[f'{indicator}_standardized'] = np.nan
                continue

            # 1. 异常值截断 (使用1%和99%分位数)
            try:
                p1 = float(valid_data.quantile(0.01))
                p99 = float(valid_data.quantile(0.99))
            except Exception as e:
                self.logger.warning(f"指标 {indicator} 分位数计算失败: {e}")
                df_result[f'{indicator}_standardized'] = np.nan
                continue

            # 对所有数据进行截断处理
            clipped_values = df[indicator].clip(lower=p1, upper=p99)

            # 2. 分位数排名标准化 (0-100百分位)
            # 使用rank方法计算排名，然后转换为百分位
            ranks = clipped_values.rank(method='average', na_option='keep')
            max_rank = ranks.max()

            if max_rank > 1:
                # 转换为0-100百分位
                percentile_scores = (ranks - 1) / (max_rank - 1) * 100
            else:
                # 如果所有值相同，设为50分
                percentile_scores = pd.Series(50.0, index=df.index)

            # 保存标准化结果
            df_result[f'{indicator}_standardized'] = percentile_scores

            # 记录标准化统计信息
            self.logger.debug(f"{indicator} 标准化完成: "
                            f"截断范围 [{p1:.2f}, {p99:.2f}], "
                            f"百分位范围 [{percentile_scores.min():.1f}, {percentile_scores.max():.1f}]")

        return df_result

    def _calculate_p_score_vectorized_mvp(self, df: pd.DataFrame) -> pd.Series:
        """基于MVP指标向量化计算P评分 (优化版 - 标准化连续评分制)

        Args:
            df: 包含MVP财务指标的DataFrame (已经过标准化处理)

        Returns:
            P评分Series (0-100连续评分)
        """
        if df.empty:
            return pd.Series(dtype=float)

        # 权重配置
        weights = {
            'gpa_ttm_standardized': 0.40,      # GPA权重: 40%
            'roe_excl_ttm_standardized': 0.30, # ROE权重: 30%
            'roa_excl_ttm_standardized': 0.30  # ROA权重: 30%
        }

        # 初始化P评分
        p_scores = pd.Series(0.0, index=df.index)
        total_weight = 0.0

        # 按权重合成P因子评分
        for indicator, weight in weights.items():
            if indicator in df.columns:
                # 使用标准化后的百分位分数 (0-100)
                indicator_scores = df[indicator].fillna(0)  # NaN填充为0分
                p_scores += indicator_scores * weight
                total_weight += weight
            else:
                self.logger.warning(f"缺少标准化指标 {indicator}，权重将重新分配")

        # 如果有缺失指标，重新归一化权重
        if total_weight > 0 and total_weight != 1.0:
            p_scores = p_scores / total_weight

        # 确保评分在0-100范围内
        p_scores = p_scores.clip(lower=0, upper=100)

        return p_scores
    
    def _save_p_factors_mvp(self, p_factors: pd.DataFrame) -> None:
        """快速保存P因子结果（先删除旧数据，再插入新数据）

        Args:
            p_factors: P因子结果DataFrame
        """
        if p_factors.empty:
            return

        # 获取计算日期
        calc_date = p_factors['calc_date'].iloc[0]

        # 先删除该计算日期的所有旧数据（确保排除的股票被完全移除）
        delete_sql = """
        DELETE FROM pgs_factors.p_factor
        WHERE calc_date = %s
        """

        self.context.db_manager.execute_sync(delete_sql, (calc_date,))
        self.logger.info(f"已删除计算日期 {calc_date} 的所有旧P因子数据")

        # 构建批量插入SQL（不再需要ON CONFLICT，因为已删除旧数据）
        insert_sql = """
        INSERT INTO pgs_factors.p_factor
        (ts_code, calc_date, gpa, roe_excl, roa_excl, p_score, data_quality, ann_date, data_source)
        VALUES %s
        """
        
        # 准备数据 (遵循PIT原则，保持真实的ann_date)
        values = []
        for _, row in p_factors.iterrows():
            values.append((
                row['ts_code'],
                row['calc_date'],
                row['gpa'],
                row['roe_excl'],
                row['roa_excl'],
                row['p_score'],
                row['data_quality'],
                row.get('ann_date', row.get('end_date')),  # 优先使用真实的ann_date
                row['data_source']
            ))
        
        # 批量插入 - 使用完整字段的逐条插入方式（已删除旧数据，无需ON CONFLICT）
        for _, row in p_factors.iterrows():
            insert_query = """
            INSERT INTO pgs_factors.p_factor (
                ts_code, calc_date, ann_date, end_date, data_source,
                p_score, p_rank,
                gpa, roe_excl, roa_excl,
                net_margin_ttm, operating_margin_ttm, roi_ttm,
                asset_turnover_ttm, equity_multiplier,
                debt_to_asset_ratio, equity_ratio,
                revenue_yoy_growth, n_income_yoy_growth, operate_profit_yoy_growth,
                data_quality, calculation_status
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s
            )
            """

            # 准备参数值
            params = (
                row['ts_code'], row['calc_date'], row['ann_date'], row['end_date'], row['data_source'],
                row['p_score'], row['p_rank'],
                row['gpa'], row['roe_excl'], row['roa_excl'],
                row['net_margin_ttm'], row['operating_margin_ttm'], row['roi_ttm'],
                row['asset_turnover_ttm'], row['equity_multiplier'],
                row['debt_to_asset_ratio'], row['equity_ratio'],
                row['revenue_yoy_growth'], row['n_income_yoy_growth'], row['operate_profit_yoy_growth'],
                row['data_quality'], row['calculation_status']
            )

            # 执行单条插入
            self.context.db_manager.execute_sync(insert_query, params)
    
    def _log_performance_stats(self, success_count: int, failed_count: int) -> None:
        """记录性能统计"""
        stats = self.stats
        total_time = stats['total_time']
        
        self.logger.info("=" * 40)
        self.logger.info("MVP版本P因子计算完成")
        self.logger.info("=" * 40)
        self.logger.info(f"成功: {success_count}, 失败: {failed_count}")
        self.logger.info(f"总耗时: {total_time:.3f} 秒")
        
        if total_time > 0:
            throughput = success_count / total_time
            self.logger.info(f"吞吐量: {throughput:.1f} 只/秒")
        
        self.logger.info(f"时间分布:")
        if total_time > 0:
            self.logger.info(f"  查询时间: {stats['query_time']:.3f}s ({stats['query_time']/total_time*100:.1f}%)")
            self.logger.info(f"  计算时间: {stats['calculation_time']:.3f}s ({stats['calculation_time']/total_time*100:.1f}%)")
            self.logger.info(f"  保存时间: {stats['save_time']:.3f}s ({stats['save_time']/total_time*100:.1f}%)")
    
    def check_mvp_data_availability(self, calc_date: str, stock_codes: List[str]) -> Dict[str, Any]:
        """检查MVP预计算数据的可用性
        
        Args:
            calc_date: 计算日期
            stock_codes: 股票代码列表
            
        Returns:
            数据可用性统计
        """
        query = """
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN data_quality = 'high' THEN 1 END) as high_quality_count,
            COUNT(CASE WHEN data_quality = 'normal' THEN 1 END) as normal_quality_count,
            COUNT(CASE WHEN data_quality = 'low' THEN 1 END) as low_quality_count,
            COUNT(CASE WHEN calculation_status = 'success' THEN 1 END) as success_count
        FROM pgs_factors.pit_financial_indicators
        WHERE ann_date <= %s  -- 修正为PIT原则
        AND ts_code = ANY(%s)
        """
        
        result_df = self.context.query_dataframe(query, (calc_date, stock_codes))
        result = result_df.iloc[0].tolist() if result_df is not None and not result_df.empty else None
        
        if result:
            total_count = result[0]
            coverage_rate = (total_count / len(stock_codes) * 100) if len(stock_codes) > 0 else 0
            
            return {
                'total_stocks': len(stock_codes),
                'available_count': total_count,
                'coverage_rate': coverage_rate,
                'high_quality_count': result[1],
                'normal_quality_count': result[2],
                'low_quality_count': result[3],
                'success_count': result[4]
            }
        
        return {
            'total_stocks': len(stock_codes),
            'available_count': 0,
            'coverage_rate': 0,
            'high_quality_count': 0,
            'normal_quality_count': 0,
            'low_quality_count': 0,
            'success_count': 0
        }

    def check_mvp_data_availability_pit(self, as_of_date: str, stock_codes: List[str]) -> Dict[str, Any]:
        """检查MVP预计算数据的可用性 (基于PIT原则)

        Args:
            as_of_date: PIT截止日期
            stock_codes: 股票代码列表

        Returns:
            数据可用性统计
        """
        query = """
        SELECT
            COUNT(*) as total_count,
            COUNT(CASE WHEN data_quality = 'high' THEN 1 END) as high_quality_count,
            COUNT(CASE WHEN data_quality = 'normal' THEN 1 END) as normal_quality_count,
            COUNT(CASE WHEN data_quality = 'outlier_high' THEN 1 END) as outlier_high_count,
            COUNT(CASE WHEN data_quality = 'outlier_low' THEN 1 END) as outlier_low_count,
            COUNT(CASE WHEN calculation_status = 'success' THEN 1 END) as success_count,
            COUNT(DISTINCT ts_code) as unique_stocks
        FROM pgs_factors.pit_financial_indicators
        WHERE ann_date <= %s  -- PIT原则: 只看已公告的数据
        AND ts_code = ANY(%s)
        AND calculation_status = 'success'
        """

        try:
            result_df = self.context.query_dataframe(query, (as_of_date, stock_codes))
            result = result_df.iloc[0].tolist() if result_df is not None and not result_df.empty else None

            if result and len(result) >= 7:
                total_records = result[0] if result[0] is not None else 0
                high_quality = result[1] if result[1] is not None else 0
                normal_quality = result[2] if result[2] is not None else 0
                outlier_high = result[3] if result[3] is not None else 0
                outlier_low = result[4] if result[4] is not None else 0
                success_count = result[5] if result[5] is not None else 0
                unique_stocks = result[6] if result[6] is not None else 0

                coverage_rate = (unique_stocks / len(stock_codes) * 100) if len(stock_codes) > 0 else 0

                return {
                    'pit_date': as_of_date,
                    'total_stocks': len(stock_codes),
                    'available_stocks': unique_stocks,
                    'coverage_rate': coverage_rate,
                    'total_records': total_records,
                    'high_quality_count': high_quality,
                    'normal_quality_count': normal_quality,
                    'outlier_high_count': outlier_high,
                    'outlier_low_count': outlier_low,
                    'success_count': success_count
                }
        except Exception as e:
            self.logger.error(f"查询MVP数据可用性失败: {e}")

        return {
            'pit_date': as_of_date,
            'total_stocks': len(stock_codes),
            'available_stocks': 0,
            'coverage_rate': 0,
            'total_records': 0,
            'high_quality_count': 0,
            'normal_quality_count': 0,
            'outlier_high_count': 0,
            'outlier_low_count': 0,
            'success_count': 0
        }
