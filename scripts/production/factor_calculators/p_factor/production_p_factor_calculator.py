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

Author: AlphaHome Team (迁移自research/pgs_factor)
Date: 2025-09-17 (迁移到生产环境)
"""

import sys
import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional, Dict, Any
import time
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import ConfigManager


class ProductionPFactorCalculator:
    """生产级P因子计算器 (基于预计算表的高性能实现)"""
    
    def __init__(self):
        """初始化计算器"""
        self.config_manager = ConfigManager()
        # 获取数据库连接字符串
        connection_string = self.config_manager.get_database_url()
        if not connection_string:
            raise ValueError("数据库连接字符串未配置，请设置config.json或环境变量DATABASE_URL")
        self.db_manager = DBManager(connection_string, mode='sync')  # 使用同步模式
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
        """基于PIT原则的P因子计算

        Args:
            as_of_date: PIT截止日期 (在此时点能看到的所有已公告数据)
            stock_codes: 股票代码列表

        Returns:
            计算结果统计
        """
        start_time = time.time()

        self.logger.info(f"开始基于PIT原则的P因子计算: {as_of_date}")
        self.logger.info(f"股票数量: {len(stock_codes)}")

        # 1. 查询预计算的财务指标 (严格遵循PIT原则)
        query_start = time.time()
        indicators_data = self._get_precomputed_indicators_pit(as_of_date, stock_codes)
        self.stats['query_time'] = time.time() - query_start

        if indicators_data.empty:
            self.logger.warning(f"在时点 {as_of_date} 未找到预计算的财务指标数据")
            return {'success_count': 0, 'failed_count': len(stock_codes)}

        self.logger.info(f"查询到 {len(indicators_data)} 条预计算指标 (PIT时点: {as_of_date})")

        # 2. 快速计算P因子 (保持ann_date不变)
        calc_start = time.time()
        p_factors = self._calculate_p_factors_from_indicators_pit(indicators_data, as_of_date)
        self.stats['calculation_time'] = time.time() - calc_start

        # 3. 保存P因子结果
        save_start = time.time()
        success_count = 0
        if not p_factors.empty:
            self._save_p_factors(p_factors)
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
        start_date: str,
        end_date: str,
        mode: Optional[str] = None
    ) -> Dict[str, Any]:
        """基于日期范围的批量P因子计算

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
                'throughput': 0,
                'total_dates': 0,
                'successful_dates': 0,
                'failed_dates': 0,
                'total_stocks_processed': 0,
                'total_records_saved': 0
            }

        self.logger.info(f"共需计算 {len(calc_dates)} 个日期")

        # 3. 执行批量计算
        total_start = time.time()
        total_success = 0
        total_failed = 0
        successful_dates = 0

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

                if result['success_count'] > 0:
                    successful_dates += 1

                self.logger.info(f"{calc_date} 计算完成: 成功 {result['success_count']}, 失败 {result['failed_count']}")

            except Exception as e:
                self.logger.error(f"{calc_date} 计算失败: {e}")
                if 'stock_codes' in locals():
                    total_failed += len(stock_codes)

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
            'successful_dates': successful_dates,
            'failed_dates': len(calc_dates) - successful_dates,
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

            result = self.db_manager.fetch_one_sync(query, (start_date, end_date))

            if result[0] == 0:
                self.logger.info("未发现现有P因子数据，使用backfill模式")
                return 'backfill'
            else:
                self.logger.info(f"发现 {result[0]} 条现有P因子数据，使用incremental模式")
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
            if dates:
                placeholders = ','.join(['%s'] * len(dates))
                query = f"""
                SELECT DISTINCT calc_date
                FROM pgs_factors.p_factor
                WHERE calc_date IN ({placeholders})
                """

                results = self.db_manager.fetch_sync(query, dates)
                existing_dates = {row[0].strftime('%Y-%m-%d') for row in results}

                missing_dates = [date for date in dates if date not in existing_dates]
            else:
                missing_dates = []

            self.logger.info(f"总日期: {len(dates)}, 已有数据: {len(existing_dates)}, 缺失数据: {len(missing_dates)}")

            return missing_dates

        except Exception as e:
            self.logger.error(f"过滤缺失日期失败: {e}")
            return dates

    def _get_trading_stock_codes(self, calc_date: str) -> List[str]:
        """获取指定日期的在交易股票列表

        Args:
            calc_date: 计算日期

        Returns:
            在交易股票代码列表
        """
        try:
            # 直接使用psycopg2进行同步查询，避免DBManager的复杂性
            import psycopg2

            connection_string = self.config_manager.get_database_url()
            if not connection_string:
                self.logger.error("无法获取数据库连接字符串")
                return []

            conn = psycopg2.connect(connection_string)
            cursor = conn.cursor()

            query = """
            SELECT ts_code
            FROM tushare.stock_basic
            WHERE list_date <= %s
            AND (delist_date IS NULL OR delist_date > %s)
            ORDER BY ts_code
            """

            cursor.execute(query, (calc_date, calc_date))
            results = cursor.fetchall()
            stock_codes = [row[0] for row in results]

            cursor.close()
            conn.close()

            self.logger.info(f"{calc_date} 获取到 {len(stock_codes)} 只在交易股票")
            return stock_codes

        except Exception as e:
            self.logger.error(f"获取 {calc_date} 股票列表失败: {e}")
            return []

    def _get_precomputed_indicators_pit(self, as_of_date: str, stock_codes: List[str]) -> pd.DataFrame:
        """基于PIT原则查询预计算的财务指标

        Args:
            as_of_date: PIT截止日期
            stock_codes: 股票代码列表

        Returns:
            财务指标数据
        """
        try:
            query = """
            WITH latest_indicators AS (
                SELECT
                    pit.ts_code,
                    pit.end_date,
                    pit.ann_date,
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
                    ROW_NUMBER() OVER (
                        PARTITION BY pit.ts_code
                        ORDER BY pit.ann_date DESC,
                                 pit.end_date DESC,
                                 CASE pit.data_source
                                     WHEN 'report' THEN 1
                                     WHEN 'express' THEN 2
                                     WHEN 'forecast' THEN 3
                                     ELSE 9
                                 END
                    ) as rn
                FROM pgs_factors.pit_financial_indicators pit
                INNER JOIN tushare.stock_basic sb ON pit.ts_code = sb.ts_code
                WHERE pit.ann_date <= %s  -- PIT原则
                AND pit.ts_code = ANY(%s)
                AND pit.calculation_status = 'success'
                AND pit.data_quality IN ('high', 'normal', 'outlier_high', 'outlier_low')
                AND pit.end_date >= (%s::date - INTERVAL '10 months')
                AND sb.list_date <= %s
                AND (sb.delist_date IS NULL OR sb.delist_date > %s)
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
            WHERE rn = 1
            ORDER BY ts_code
            """

            # 使用同步查询方法
            results = self.db_manager.fetch_sync(query, (
                as_of_date, stock_codes, as_of_date, as_of_date, as_of_date
            ))
            
            if results:
                # 转换为DataFrame
                columns = [
                    'ts_code', 'end_date', 'ann_date', 'data_source',
                    'gpa_ttm', 'roe_excl_ttm', 'roa_excl_ttm',
                    'net_margin_ttm', 'operating_margin_ttm', 'roi_ttm',
                    'asset_turnover_ttm', 'equity_multiplier',
                    'debt_to_asset_ratio', 'equity_ratio',
                    'revenue_yoy_growth', 'n_income_yoy_growth', 'operate_profit_yoy_growth',
                    'data_quality', 'calculation_status'
                ]
                df = pd.DataFrame(results, columns=columns)
            else:
                df = pd.DataFrame()

            if not df.empty:
                excluded_count = len(stock_codes) - len(df)
                if excluded_count > 0:
                    self.logger.info(f"数据筛选: 排除了 {excluded_count} 只股票")

            return df

        except Exception as e:
            self.logger.error(f"查询预计算指标失败: {e}")
            return pd.DataFrame()
    
    def _calculate_p_factors_from_indicators_pit(
        self,
        indicators_data: pd.DataFrame,
        as_of_date: str
    ) -> pd.DataFrame:
        """基于预计算指标和PIT原则快速计算P因子

        Args:
            indicators_data: 预计算的财务指标数据
            as_of_date: PIT截止日期

        Returns:
            P因子结果DataFrame
        """
        if indicators_data.empty:
            return pd.DataFrame()

        # 复制数据避免修改原始数据
        df = indicators_data.copy()

        # 添加计算日期
        df['calc_date'] = as_of_date

        self.logger.info(f"开始标准化处理 {len(df)} 只股票的财务指标")

        # 1. 财务指标标准化处理
        df = self._standardize_financial_indicators(df)

        # 2. 向量化计算P评分
        df['p_score'] = self._calculate_p_score_vectorized(df)

        # 3. 计算P排名
        df['p_score'] = df['p_score'].fillna(0)
        df['p_score'] = df['p_score'].replace([np.inf, -np.inf], [100, 0])
        df['p_rank'] = df['p_score'].rank(method='min', ascending=False, na_option='bottom').astype(int)

        # 4. 映射财务指标到P因子表字段
        df['gpa'] = df['gpa_ttm']
        df['roe_excl'] = df['roe_excl_ttm']
        df['roa_excl'] = df['roa_excl_ttm']

        # 5. 应用行业特殊处理
        df = self._apply_industry_special_handling(df, as_of_date)

        # 6. 重新计算受行业特殊处理影响的P评分和排名
        affected_stocks = df[df['gpa'].isna()]['ts_code'].unique()
        if len(affected_stocks) > 0:
            self.logger.info(f"重新计算 {len(affected_stocks)} 只金融股的P评分 (GPA=NULL)")
            
            # 对于GPA为NULL的股票，重新进行评分计算
            mask_null_gpa = df['gpa'].isna()
            
            for idx in df[mask_null_gpa].index:
                roe_score = df.loc[idx, 'roe_excl_ttm_standardized'] if 'roe_excl_ttm_standardized' in df.columns else 0
                roa_score = df.loc[idx, 'roa_excl_ttm_standardized'] if 'roa_excl_ttm_standardized' in df.columns else 0
                
                # 金融股P评分 = ROE(50%) + ROA(50%)
                df.loc[idx, 'p_score'] = (roe_score * 0.5 + roa_score * 0.5)
            
            # 重新计算排名
            df['p_score'] = df['p_score'].fillna(0)
            df['p_score'] = df['p_score'].replace([np.inf, -np.inf], [100, 0])
            df['p_rank'] = df['p_score'].rank(method='min', ascending=False, na_option='bottom').astype(int)

        # 选择输出列
        output_columns = [
            'ts_code', 'calc_date', 'ann_date', 'end_date', 'data_source',
            'p_score', 'p_rank',
            'gpa', 'roe_excl', 'roa_excl',
            'net_margin_ttm', 'operating_margin_ttm', 'roi_ttm',
            'asset_turnover_ttm', 'equity_multiplier',
            'debt_to_asset_ratio', 'equity_ratio',
            'revenue_yoy_growth', 'n_income_yoy_growth', 'operate_profit_yoy_growth',
            'data_quality', 'calculation_status'
        ]

        # 确保所有列都存在
        for col in output_columns:
            if col not in df.columns:
                df[col] = None

        self.logger.info(f"P因子计算完成: 平均评分 {df['p_score'].mean():.2f}")

        return df[output_columns]

    def _apply_industry_special_handling(self, df: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
        """应用行业特殊处理逻辑"""
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
        """获取PIT行业分类信息 (与研究目录版本一致)"""
        try:
            # 优先使用数据库中的行业分类数据
            try:
                # 尝试使用优化后的批量PIT查询函数 (申万数据)
                query = """
                SELECT * FROM get_industry_classification_batch_pit_optimized(%s, %s, 'sw')
                """
                
                results = self.db_manager.fetch_sync(query, (stock_codes, as_of_date))
                
                if results:
                    columns = ['ts_code', 'industry_level1', 'industry_level2', 'industry_level3', 
                              'data_source', 'obs_date']
                    df = pd.DataFrame(results, columns=columns)
                    
                    # 应用与研究目录版本相同的金融行业识别逻辑
                    def is_financial_industry(l1: str, l2: str) -> bool:
                        text = f"{l1 or ''} {l2 or ''}"
                        keywords = ['银行', '证券', '保险', '信托', '期货', '基金', '金融', '投资', '资产管理', '财务公司']
                        return any(k in text for k in keywords)
                    
                    df['requires_special_gpa_handling'] = df.apply(
                        lambda r: is_financial_industry(r.get('industry_level1'), r.get('industry_level2')), axis=1
                    )
                    df['gpa_calculation_method'] = df['requires_special_gpa_handling'].apply(
                        lambda x: 'null' if x else 'standard'
                    )
                    
                    self.logger.info(f"从数据库获取行业分类: {len(df)} 只股票")
                    return df
                    
            except Exception as db_error:
                self.logger.warning(f"数据库行业分类查询失败: {db_error}")
            
            # 回退到基于Tushare行业成员表的查询
            try:
                query_fallback = """
                SELECT DISTINCT
                    t.ts_code,
                    COALESCE(t.industry_name, '其他') AS industry_level1,
                    COALESCE(t.industry_name, '其他') AS industry_level2,
                    COALESCE(t.industry_name, '其他') AS industry_level3,
                    'tushare' AS data_source,
                    %s AS obs_date
                FROM tushare.index_member t
                WHERE t.ts_code = ANY(%s)
                AND t.out_date IS NULL
                """
                
                results = self.db_manager.fetch_sync(query_fallback, (as_of_date, stock_codes))
                
                if results:
                    columns = ['ts_code', 'industry_level1', 'industry_level2', 'industry_level3', 
                              'data_source', 'obs_date']
                    df = pd.DataFrame(results, columns=columns)
                    
                    # 应用金融行业识别逻辑
                    def is_financial_industry(l1: str, l2: str) -> bool:
                        text = f"{l1 or ''} {l2 or ''}"
                        keywords = ['银行', '证券', '保险', '信托', '期货', '基金', '金融', '投资', '资产管理', '财务公司']
                        return any(k in text for k in keywords)
                    
                    df['requires_special_gpa_handling'] = df.apply(
                        lambda r: is_financial_industry(r.get('industry_level1'), r.get('industry_level2')), axis=1
                    )
                    df['gpa_calculation_method'] = df['requires_special_gpa_handling'].apply(
                        lambda x: 'null' if x else 'standard'
                    )
                    
                    self.logger.info(f"从Tushare获取行业分类: {len(df)} 只股票")
                    return df
                    
            except Exception as fallback_error:
                self.logger.warning(f"Tushare行业分类查询失败: {fallback_error}")
            
            # 最终回退：基于股票代码和名称的启发式识别
            financial_stocks = []
            for stock in stock_codes:
                # 基于股票代码启发式识别金融股
                is_financial = self._is_financial_stock_by_code(stock)
                
                financial_stocks.append({
                    'ts_code': stock,
                    'obs_date': as_of_date,
                    'data_source': 'heuristic',
                    'industry_level1': '金融业' if is_financial else '其他',
                    'industry_level2': '金融业' if is_financial else '其他', 
                    'industry_level3': '金融业' if is_financial else '其他',
                    'requires_special_gpa_handling': is_financial,
                    'gpa_calculation_method': 'null' if is_financial else 'standard'
                })
            
            if financial_stocks:
                df = pd.DataFrame(financial_stocks)
                self.logger.warning(f"使用启发式方法识别行业分类: {len(df)} 只股票")
                return df
            else:
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取行业分类失败: {e}")
            return pd.DataFrame()

    def _is_financial_stock_by_code(self, stock_code: str) -> bool:
        """基于股票代码启发式识别金融股"""
        # 常见金融股代码（银行、保险、券商）
        financial_codes = {
            # 四大行
            '601398.SH', '601939.SH', '601988.SH', '601328.SH',
            # 股份制银行
            '000001.SZ', '600000.SH', '600036.SH', '601166.SH', '600015.SH', '600016.SH',
            # 城商行
            '600054.SH', '600919.SH', '601169.SH', '002142.SZ',
            # 保险
            '601318.SH', '601601.SH', '601336.SH',
            # 券商  
            '600030.SH', '000166.SZ', '600999.SH', '600837.SH', '601377.SH',
            '600053.SH', '601009.SH', '000783.SZ', '600958.SH'
        }
        return stock_code in financial_codes

    def _standardize_financial_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """财务指标标准化处理"""
        if df.empty:
            return df

        df_result = df.copy()
        indicators = ['gpa_ttm', 'roe_excl_ttm', 'roa_excl_ttm']

        for indicator in indicators:
            if indicator not in df.columns:
                continue

            try:
                df[indicator] = pd.to_numeric(df[indicator], errors='coerce')
                valid_data = df[indicator].dropna()
            except Exception:
                df_result[f'{indicator}_standardized'] = np.nan
                continue

            if len(valid_data) == 0:
                df_result[f'{indicator}_standardized'] = np.nan
                continue

            # 异常值截断
            p1 = float(valid_data.quantile(0.01))
            p99 = float(valid_data.quantile(0.99))
            clipped_values = df[indicator].clip(lower=p1, upper=p99)

            # 分位数排名标准化
            ranks = clipped_values.rank(method='average', na_option='keep')
            max_rank = ranks.max()

            if max_rank > 1:
                percentile_scores = (ranks - 1) / (max_rank - 1) * 100
            else:
                percentile_scores = pd.Series(50.0, index=df.index)

            df_result[f'{indicator}_standardized'] = percentile_scores

        return df_result

    def _calculate_p_score_vectorized(self, df: pd.DataFrame) -> pd.Series:
        """向量化计算P评分"""
        if df.empty:
            return pd.Series(dtype=float)

        weights = {
            'gpa_ttm_standardized': 0.40,
            'roe_excl_ttm_standardized': 0.30,
            'roa_excl_ttm_standardized': 0.30
        }

        p_scores = pd.Series(0.0, index=df.index)
        total_weight = 0.0

        for indicator, weight in weights.items():
            if indicator in df.columns:
                indicator_scores = df[indicator].fillna(0)
                p_scores += indicator_scores * weight
                total_weight += weight

        if total_weight > 0 and total_weight != 1.0:
            p_scores = p_scores / total_weight

        p_scores = p_scores.clip(lower=0, upper=100)
        return p_scores
    
    def _save_p_factors(self, p_factors: pd.DataFrame) -> None:
        """快速保存P因子结果"""
        if p_factors.empty:
            return

        calc_date = p_factors['calc_date'].iloc[0]

        # 先删除旧数据
        delete_sql = """
        DELETE FROM pgs_factors.p_factor
        WHERE calc_date = %s
        """

        # 先删除旧数据
        self.db_manager.execute_sync(delete_sql, (calc_date,))
        
        # 批量插入新数据
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

        for _, row in p_factors.iterrows():
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
            self.db_manager.execute_sync(insert_query, params)

        self.logger.info(f"已保存 {len(p_factors)} 条P因子数据到数据库")
    
    def _log_performance_stats(self, success_count: int, failed_count: int) -> None:
        """记录性能统计"""
        stats = self.stats
        total_time = stats['total_time']
        
        self.logger.info("=" * 40)
        self.logger.info("P因子计算完成")
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
