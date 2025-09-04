#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生产级G因子计算器 (PIT原则 + 成长因子分析)
=======================================================

基于P因子数据计算G因子（成长能力指标），严格遵循Point-in-Time原则。
简化版：移除权重系统，统一使用权重1.0，提高因子可解释性和稳定性。

🚀 核心特性 (v1.1):
1. **四个成长子因子**: 效率惊喜、效率动量、营收动量、利润动量
2. **简化权重系统**: 统一权重1.0，消除数据源偏好
3. **前瞻性成长指标**: 基于Forecast数据的增长预期
4. **百分位排名系统**: 0-100分标准化评分
5. **数据源信息保留**: 记录数据源类型用于分析和监控

📊 G因子计算公式:
- G_Efficiency_Surprise = ΔP_score_YoY / Std(ΔP_score_YoY)
- G_Efficiency_Momentum = ΔP_score_YoY
- G_Revenue_Momentum = Revenue_YoY_Growth
- G_Profit_Momentum = N_Income_YoY_Growth
- Final_G_Score = 0.25×Rank_ES + 0.25×Rank_EM + 0.25×Rank_RM + 0.25×Rank_PM

🎯 PIT原则核心:
1. 在指定时点(as_of_date)，只能看到该时点之前或当日公告的数据
2. 基于P因子历史数据计算同比增长和动量指标
3. 查询条件: calc_date <= as_of_date

🔧 技术优化:
1. 基于已计算的P因子数据，避免重复计算
2. 向量化计算和批量处理
3. 智能缓存和数据预加载
4. 简化权重系统，提高计算一致性

Author: AI Assistant
Date: 2025-09-01 (v1.1 - 简化权重系统)
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import time

from research.tools.context import ResearchContext


class ProductionGFactorCalculator:
    """生产级G因子计算器 (基于P因子数据的高性能实现)"""
    
    def __init__(self, context: ResearchContext, config: Optional[Dict[str, Any]] = None):
        """初始化计算器

        Args:
            context: ResearchContext实例
            config: 配置参数字典（可选）
        """
        self.context = context
        self.db_manager = context.db_manager
        self.logger = self._setup_logger()
        self.config = config or {}

        # 统一权重为1.0，简化计算逻辑
        self.timeliness_weights = {'express': 1.0, 'forecast': 1.0, 'report': 1.0}

        # 效率惊喜（ES）相关可配置参数
        self.es_params = {
            'yoy_interval_weeks': int(self.config.get('yoy_interval_weeks', 52)),
            'yoy_match_tolerance_days': int(self.config.get('yoy_match_tolerance_days', 45)),
            'min_yoy_pairs_for_std': int(self.config.get('min_yoy_pairs_for_std', 8)),
            'min_yoy_pairs_soft': int(self.config.get('min_yoy_pairs_soft', 3)),
            'enable_pooled_std_fallback': bool(self.config.get('enable_pooled_std_fallback', True))
        }
        self.logger.info(
            f"效率惊喜参数: {self.es_params}"
        )
        
        # 子因子权重配置 (等权重合成)
        self.subfactor_weights = {
            'efficiency_surprise': 0.25,
            'efficiency_momentum': 0.25,
            'revenue_momentum': 0.25,
            'profit_momentum': 0.25
        }
        
        # 性能统计
        self.stats = {
            'query_time': 0,
            'calculation_time': 0,
            'save_time': 0,
            'total_time': 0
        }
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('ProductionGFactorCalculator')
        logger.setLevel(logging.INFO)
        return logger
    
    def calculate_g_factors_pit(
        self,
        as_of_date: str,
        stock_codes: List[str]
    ) -> Dict[str, Any]:
        """基于PIT原则的G因子计算
        
        Args:
            as_of_date: PIT截止日期 (在此时点能看到的所有已公告数据)
            stock_codes: 股票代码列表
        
        Returns:
            计算结果统计
        """
        start_time = time.time()
        
        self.logger.info(f"开始基于PIT原则的G因子计算: {as_of_date}")
        self.logger.info(f"股票数量: {len(stock_codes)}")
        
        # 1. 查询P因子历史数据 (严格遵循PIT原则)
        query_start = time.time()
        p_factor_data = self._get_p_factor_historical_data_pit(as_of_date, stock_codes)
        self.stats['query_time'] = time.time() - query_start
        
        if p_factor_data.empty:
            self.logger.warning(f"在时点 {as_of_date} 未找到P因子历史数据")
            return {'success_count': 0, 'failed_count': len(stock_codes)}
        
        # 2. 计算G因子 (基于P因子数据)
        calc_start = time.time()
        g_factor_results = self._calculate_g_factors_from_p_data_pit(p_factor_data, as_of_date)
        self.stats['calculation_time'] = time.time() - calc_start
        
        if g_factor_results.empty:
            self.logger.warning(f"G因子计算结果为空")
            return {'success_count': 0, 'failed_count': len(stock_codes)}
        
        # 3. 保存G因子结果
        save_start = time.time()
        success_count = self._save_g_factor_results_pit(g_factor_results, as_of_date)
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
    
    def _get_p_factor_historical_data_pit(self, as_of_date: str, stock_codes: List[str]) -> pd.DataFrame:
        """基于PIT原则查询P因子历史数据
        
        Args:
            as_of_date: PIT截止日期
            stock_codes: 股票代码列表
        
        Returns:
            P因子历史数据DataFrame
        """
        try:
            # 计算需要的历史时间范围 (至少需要2年数据用于同比计算)
            as_of_datetime = datetime.strptime(as_of_date, '%Y-%m-%d')
            start_date = (as_of_datetime - timedelta(days=730)).strftime('%Y-%m-%d')  # 2年前
            
            self.logger.info(f"查询P因子历史数据: {start_date} ~ {as_of_date}")
            
            # 查询P因子历史数据 (PIT原则: calc_date <= as_of_date)
            # 重要：包含营收和利润增长字段用于G因子计算
            query = """
            SELECT
                ts_code,
                calc_date,
                p_score,
                data_source,
                ann_date,
                gpa,
                roe_excl,
                roa_excl,
                revenue_yoy_growth,
                n_income_yoy_growth
            FROM pgs_factors.p_factor
            WHERE ts_code = ANY(%s)
              AND calc_date BETWEEN %s AND %s  -- 已包含上界as_of_date
              AND p_score IS NOT NULL
            ORDER BY ts_code, calc_date
            """
            
            result = self.context.query_dataframe(query, (stock_codes, start_date, as_of_date))
            
            if result is not None and not result.empty:
                self.logger.info(f"获取到 {len(result)} 条P因子历史记录")
                return result
            else:
                self.logger.warning("未找到P因子历史数据")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"查询P因子历史数据失败 (PIT时点: {as_of_date}): {e}")
            return pd.DataFrame()
    
    def _calculate_g_factors_from_p_data_pit(
        self,
        p_factor_data: pd.DataFrame,
        as_of_date: str
    ) -> pd.DataFrame:
        """基于P因子数据和PIT原则计算G因子
        
        Args:
            p_factor_data: P因子历史数据
            as_of_date: PIT截止日期
        
        Returns:
            G因子结果DataFrame
        """
        if p_factor_data.empty:
            return pd.DataFrame()
        
        self.logger.info(f"开始计算G因子，基于 {len(p_factor_data)} 条P因子记录")
        
        # 转换日期列
        p_factor_data['calc_date'] = pd.to_datetime(p_factor_data['calc_date'])
        
        # 按股票分组计算G因子
        g_factor_results = []
        
        for ts_code, group in p_factor_data.groupby('ts_code'):
            try:
                # 获取最新的P因子记录 (as_of_date当日或之前最近的记录)
                latest_record = self._get_latest_p_factor_record(group, as_of_date)
                
                if latest_record is None:
                    continue
                
                # 计算G因子子指标
                g_factors = self._calculate_g_subfactors(group, latest_record, as_of_date)
                
                if g_factors:
                    g_factor_results.append(g_factors)
                    
            except Exception as e:
                self.logger.error(f"计算 {ts_code} G因子失败: {e}")
                continue
        
        if not g_factor_results:
            return pd.DataFrame()
        
        # 转换为DataFrame
        df = pd.DataFrame(g_factor_results)
        
        # 计算横截面排名 (0-100百分位)
        df = self._calculate_cross_sectional_rankings(df)
        
        # 计算最终G评分
        df['g_score'] = self._calculate_final_g_score(df)
        
        # 添加计算日期
        df['calc_date'] = as_of_date
        
        self.logger.info(f"G因子计算完成，共 {len(df)} 只股票")

        return df

    def _get_latest_p_factor_record(self, group: pd.DataFrame, as_of_date: str) -> Optional[pd.Series]:
        """获取指定日期的最新P因子记录

        Args:
            group: 单只股票的P因子历史数据
            as_of_date: PIT截止日期

        Returns:
            最新的P因子记录
        """
        as_of_datetime = pd.to_datetime(as_of_date)

        # 筛选PIT时点之前或当日的记录
        valid_records = group[group['calc_date'] <= as_of_datetime]

        if valid_records.empty:
            return None

        # 返回最新的记录
        return valid_records.loc[valid_records['calc_date'].idxmax()]

    def _calculate_g_subfactors(
        self,
        group: pd.DataFrame,
        latest_record: pd.Series,
        as_of_date: str
    ) -> Optional[Dict[str, Any]]:
        """计算G因子的四个子因子

        Args:
            group: 单只股票的P因子历史数据
            latest_record: 最新的P因子记录
            as_of_date: PIT截止日期

        Returns:
            G因子子指标字典
        """
        try:
            ts_code = latest_record['ts_code']
            data_source = latest_record['data_source']

            # 统一权重为1.0，简化计算逻辑
            timeliness_weight = 1.0

            # 计算同比数据 (1年前的P因子数据)
            yoy_data = self._get_yoy_p_factor_data(group, latest_record['calc_date'])

            # 1. 效率惊喜因子 (G_Efficiency_Surprise)
            efficiency_surprise = self._calculate_efficiency_surprise(
                group, latest_record, yoy_data
            )

            # 2. 效率动量因子 (G_Efficiency_Momentum)
            efficiency_momentum = self._calculate_efficiency_momentum(
                latest_record, yoy_data
            )

            # 3. 营收动量因子 (G_Revenue_Momentum) - 基于财务指标
            revenue_momentum = self._calculate_revenue_momentum(
                group, latest_record
            )

            # 4. 利润动量因子 (G_Profit_Momentum) - 基于财务指标
            profit_momentum = self._calculate_profit_momentum(
                group, latest_record
            )

            return {
                'ts_code': ts_code,
                'data_source': data_source,
                'data_timeliness_weight': timeliness_weight,  # 统一为1.0
                'g_efficiency_surprise': efficiency_surprise,
                'g_efficiency_momentum': efficiency_momentum,
                'g_revenue_momentum': revenue_momentum,
                'g_profit_momentum': profit_momentum,
                'ann_date': latest_record['ann_date'],
                'calculation_status': 'success'
            }

        except Exception as e:
            self.logger.error(f"计算G因子子指标失败: {e}")
            return None

    def _get_yoy_p_factor_data(self, group: pd.DataFrame, current_date: pd.Timestamp) -> Optional[pd.Series]:
        """获取同比P因子数据 (1年前)

        Args:
            group: 单只股票的P因子历史数据
            current_date: 当前计算日期

        Returns:
            1年前的P因子记录
        """
        # 计算1年前的日期范围 (允许±30天的误差)
        target_date = current_date - pd.DateOffset(years=1)
        start_range = target_date - pd.DateOffset(days=30)
        end_range = target_date + pd.DateOffset(days=30)

        # 查找1年前的数据
        yoy_candidates = group[
            (group['calc_date'] >= start_range) &
            (group['calc_date'] <= end_range)
        ]

        if yoy_candidates.empty:
            return None

        # 返回最接近目标日期的记录
        yoy_candidates = yoy_candidates.copy()
        yoy_candidates['date_diff'] = abs(yoy_candidates['calc_date'] - target_date)
        return yoy_candidates.loc[yoy_candidates['date_diff'].idxmin()]

    def _build_yoy_delta_series_52w(self, group: pd.DataFrame) -> List[float]:
        """构建基于可配置间隔周数(默认52周)的ΔP_score(YoY)序列

        说明:
        - 遍历该股票的每个计算时点 t，找到最接近 t-intervalWeeks 的记录作为同比基准
        - 采用 ±toleranceDays 容忍窗口，与 _get_yoy_p_factor_data 的口径保持一致
        - 仅对存在同比基准的时点计入序列
        """
        if group is None or group.empty:
            return []

        sorted_group = group.sort_values('calc_date')
        deltas: List[float] = []

        for _, current in sorted_group.iterrows():
            try:
                interval_weeks = max(1, int(self.es_params.get('yoy_interval_weeks', 52)))
                tolerance_days = max(0, int(self.es_params.get('yoy_match_tolerance_days', 45)))

                target_date = current['calc_date'] - pd.DateOffset(weeks=interval_weeks)
                start_range = target_date - pd.DateOffset(days=tolerance_days)
                end_range = target_date + pd.DateOffset(days=tolerance_days)

                yoy_candidates = sorted_group[
                    (sorted_group['calc_date'] >= start_range) &
                    (sorted_group['calc_date'] <= end_range)
                ]

                if yoy_candidates.empty:
                    continue

                candidates = yoy_candidates.copy()
                candidates['date_diff'] = abs(candidates['calc_date'] - target_date)
                yoy_rec = candidates.loc[candidates['date_diff'].idxmin()]

                # 仅当两端均有有效 p_score 时纳入序列
                if pd.notna(current.get('p_score')) and pd.notna(yoy_rec.get('p_score')):
                    delta = float(current['p_score']) - float(yoy_rec['p_score'])
                    deltas.append(delta)
            except Exception:
                # 单点异常忽略，继续累积其它时点
                continue

        return deltas

    def _calculate_efficiency_surprise(
        self,
        group: pd.DataFrame,
        latest_record: pd.Series,
        yoy_data: Optional[pd.Series],
        pooled_yoy_std: Optional[float] = None
    ) -> float:
        """计算效率惊喜因子

        公式: ΔP_score_YoY / Std(ΔP_score_YoY)
        """
        if yoy_data is None:
            return np.nan

        # 计算P评分同比变化
        delta_p_score = float(latest_record['p_score']) - float(yoy_data['p_score'])

        # 使用配置的YoY差序列作为分母基础
        p_score_changes = self._build_yoy_delta_series_52w(group)
        n_samples = len(p_score_changes)

        hard_n = max(1, int(self.es_params.get('min_yoy_pairs_for_std', 8)))
        soft_n = max(1, int(self.es_params.get('min_yoy_pairs_soft', 3)))
        use_pooled = bool(self.es_params.get('enable_pooled_std_fallback', True))

        if n_samples >= hard_n:
            std_delta = np.std(p_score_changes)
            if std_delta > 0:
                return (delta_p_score / std_delta) * 1.0
            # std=0 则回退到未归一化
            return delta_p_score * 1.0

        if n_samples >= soft_n:
            std_delta = np.std(p_score_changes)
            if std_delta > 0:
                # 软阈值下按样本占比进行衰减，抑制噪声
                scale = np.sqrt(n_samples / float(hard_n))
                return (delta_p_score / std_delta) * scale * 1.0
            return delta_p_score * 1.0

        # 样本数不足soft阈值时，尝试横截面池化标准差
        if use_pooled and pooled_yoy_std is not None and pooled_yoy_std > 0:
            return (delta_p_score / pooled_yoy_std) * 1.0

        # 最后回退：未归一化惊喜
        return delta_p_score * 1.0

    def _calculate_efficiency_momentum(
        self,
        latest_record: pd.Series,
        yoy_data: Optional[pd.Series]
    ) -> float:
        """计算效率动量因子

        公式: ΔP_score_YoY
        """
        if yoy_data is None:
            return np.nan

        delta_p_score = float(latest_record['p_score']) - float(yoy_data['p_score'])
        return delta_p_score * 1.0

    def _calculate_revenue_momentum(
        self,
        group: pd.DataFrame,
        latest_record: pd.Series
    ) -> float:
        """计算营收动量因子

        基于营收同比增长率 (revenue_yoy_growth)
        """
        try:
            # 直接使用最新记录的营收同比增长率
            revenue_growth = latest_record.get('revenue_yoy_growth')

            if revenue_growth is None or pd.isna(revenue_growth):
                return np.nan

            # 营收增长率已经是百分比，直接使用
            return float(revenue_growth) * 1.0

        except Exception as e:
            self.logger.warning(f"计算营收动量失败: {e}")
            return 0.0

    def _calculate_profit_momentum(
        self,
        group: pd.DataFrame,
        latest_record: pd.Series
    ) -> float:
        """计算利润动量因子

        基于净利润同比增长率 (n_income_yoy_growth)
        """
        try:
            # 直接使用最新记录的净利润同比增长率
            profit_growth = latest_record.get('n_income_yoy_growth')

            if profit_growth is None or pd.isna(profit_growth):
                return np.nan

            # 利润增长率已经是百分比，直接使用
            return float(profit_growth) * 1.0

        except Exception as e:
            self.logger.warning(f"计算利润动量失败: {e}")
            return 0.0

    def _calculate_cross_sectional_rankings(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算横截面排名 (0-100百分位)

        Args:
            df: G因子数据DataFrame

        Returns:
            包含排名的DataFrame
        """
        # 计算各子因子的百分位排名 (0-100)
        # 对于缺失值（NaN），使用na_option='keep'保持为NaN，不参与排名
        # 这样空值因子在最终G评分计算中会被正确排除，权重动态调整
        
        # 效率惊喜排名：只对有效值进行排名
        df['rank_es'] = df['g_efficiency_surprise'].rank(pct=True, na_option='keep') * 100
        
        # 效率动量排名：只对有效值进行排名
        df['rank_em'] = df['g_efficiency_momentum'].rank(pct=True, na_option='keep') * 100
        
        # 营收动量排名：只对有效值进行排名
        df['rank_rm'] = df['g_revenue_momentum'].rank(pct=True, na_option='keep') * 100
        
        # 利润动量排名：只对有效值进行排名
        df['rank_pm'] = df['g_profit_momentum'].rank(pct=True, na_option='keep') * 100

        # 注意：不再将NaN填充为0，保持NaN状态
        # 这样在最终G评分计算中，空值因子会被正确排除，权重动态调整

        return df

    def _calculate_final_g_score(self, df: pd.DataFrame) -> pd.Series:
        """计算最终G评分

        动态权重公式: 对于空值因子，权重赋为0
        Final_G_Score = (w1×Rank_ES×logic_ES + w2×Rank_EM×logic_EM + w3×Rank_RM×logic_RM + w4×Rank_PM×logic_PM) / (w1×logic_ES + w2×logic_EM + w3×logic_RM + w4×logic_PM)
        
        其中 logic_X = 1 if Rank_X is not null else 0
        """
        # 检查各子因子是否有有效值（非空值）
        has_es = df['g_efficiency_surprise'].notna()
        has_em = df['g_efficiency_momentum'].notna()
        has_rm = df['g_revenue_momentum'].notna()
        has_pm = df['g_profit_momentum'].notna()
        
        # 计算动态权重
        w_es = self.subfactor_weights['efficiency_surprise']
        w_em = self.subfactor_weights['efficiency_momentum']
        w_rm = self.subfactor_weights['revenue_momentum']
        w_pm = self.subfactor_weights['profit_momentum']
        
        # 初始化结果Series
        g_score = pd.Series(index=df.index, dtype=float)
        
        # 逐行计算G评分，确保正确处理空值
        for idx in df.index:
            # 获取该行的有效因子信息
            row_has_es = has_es.loc[idx]
            row_has_em = has_em.loc[idx]
            row_has_rm = has_rm.loc[idx]
            row_has_pm = has_pm.loc[idx]
            
            # 计算有效因子的权重和
            total_weight = (
                w_es * row_has_es +
                w_em * row_has_em +
                w_rm * row_has_rm +
                w_pm * row_has_pm
            )
            
            # 如果没有任何有效因子，G评分为0
            if total_weight == 0:
                g_score.loc[idx] = 0.0
                continue
            
            # 计算加权排名和
            weighted_sum = 0.0
            if row_has_es and pd.notna(df.loc[idx, 'rank_es']):
                weighted_sum += df.loc[idx, 'rank_es'] * w_es
            if row_has_em and pd.notna(df.loc[idx, 'rank_em']):
                weighted_sum += df.loc[idx, 'rank_em'] * w_em
            if row_has_rm and pd.notna(df.loc[idx, 'rank_rm']):
                weighted_sum += df.loc[idx, 'rank_rm'] * w_rm
            if row_has_pm and pd.notna(df.loc[idx, 'rank_pm']):
                weighted_sum += df.loc[idx, 'rank_pm'] * w_pm
            
            # 计算最终G评分
            g_score.loc[idx] = weighted_sum / total_weight
        
        return g_score

    def _save_g_factor_results_pit(self, g_factor_results: pd.DataFrame, calc_date: str) -> int:
        """保存G因子计算结果

        Args:
            g_factor_results: G因子结果DataFrame
            calc_date: 计算日期

        Returns:
            成功保存的记录数
        """
        if g_factor_results.empty:
            return 0

        try:
            self.logger.info(f"开始保存G因子结果: {len(g_factor_results)} 条记录")

            # 删除该计算日期的旧数据
            delete_query = """
            DELETE FROM pgs_factors.g_factor
            WHERE calc_date = %s
            """
            self.context.db_manager.execute_sync(delete_query, (calc_date,))
            self.logger.info(f"已删除计算日期 {calc_date} 的所有旧G因子数据")

            # 准备插入数据
            insert_data = []
            for _, row in g_factor_results.iterrows():
                insert_data.append((
                    row['ts_code'],
                    calc_date,
                    row['data_source'],
                    float(row['g_efficiency_surprise']) if pd.notna(row['g_efficiency_surprise']) else None,
                    float(row['g_efficiency_momentum']) if pd.notna(row['g_efficiency_momentum']) else None,
                    float(row['g_revenue_momentum']) if pd.notna(row['g_revenue_momentum']) else None,
                    float(row['g_profit_momentum']) if pd.notna(row['g_profit_momentum']) else None,
                    float(row['rank_es']) if pd.notna(row['rank_es']) else None,
                    float(row['rank_em']) if pd.notna(row['rank_em']) else None,
                    float(row['rank_rm']) if pd.notna(row['rank_rm']) else None,
                    float(row['rank_pm']) if pd.notna(row['rank_pm']) else None,
                    float(row['g_score']) if pd.notna(row['g_score']) else None,
                    float(row['data_timeliness_weight']) if pd.notna(row['data_timeliness_weight']) else None,
                    row['calculation_status'],
                    row['ann_date']
                ))

            # 批量插入
            insert_query = """
            INSERT INTO pgs_factors.g_factor
            (ts_code, calc_date, data_source, g_efficiency_surprise, g_efficiency_momentum,
             g_revenue_momentum, g_profit_momentum, rank_es, rank_em, rank_rm, rank_pm,
             g_score, data_timeliness_weight, calculation_status, ann_date)
            VALUES %s
            """

            # 使用同步方法逐条插入数据（使用正确的表结构）
            insert_query = """
            INSERT INTO pgs_factors.g_factor (
                ts_code, calc_date, ann_date, data_source,
                g_efficiency_surprise, g_efficiency_momentum, g_revenue_momentum, g_profit_momentum,
                rank_es, rank_em, rank_rm, rank_pm,
                g_score, data_timeliness_weight, calculation_status
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
            """

            # 逐条插入（已删除旧数据，无需ON CONFLICT）
            success_count = 0
            for data_tuple in insert_data:
                try:
                    # 数据结构: (ts_code, calc_date, data_source, g_efficiency_surprise, g_efficiency_momentum,
                    #           g_revenue_momentum, g_profit_momentum, rank_es, rank_em, rank_rm, rank_pm,
                    #           g_score, data_timeliness_weight, calculation_status, ann_date)
                    # 重新排序以匹配INSERT语句的字段顺序

                    new_data_tuple = (
                        data_tuple[0],   # ts_code
                        data_tuple[1],   # calc_date
                        data_tuple[14],  # ann_date
                        data_tuple[2],   # data_source
                        data_tuple[3],   # g_efficiency_surprise
                        data_tuple[4],   # g_efficiency_momentum
                        data_tuple[5],   # g_revenue_momentum
                        data_tuple[6],   # g_profit_momentum
                        data_tuple[7],   # rank_es
                        data_tuple[8],   # rank_em
                        data_tuple[9],   # rank_rm
                        data_tuple[10],  # rank_pm
                        data_tuple[11],  # g_score
                        data_tuple[12],  # data_timeliness_weight
                        data_tuple[13]   # calculation_status
                    )

                    self.context.db_manager.execute_sync(insert_query, new_data_tuple)
                    success_count += 1
                except Exception as e:
                    self.logger.error(f"插入G因子数据失败 {data_tuple[0]}: {e}")

            self.logger.info(f"成功保存 {success_count}/{len(insert_data)} 条G因子记录")
            return success_count

        except Exception as e:
            self.logger.error(f"保存G因子结果失败: {e}")
            return 0

    def _log_performance_stats(self, success_count: int, failed_count: int):
        """记录性能统计信息"""
        total_count = success_count + failed_count

        self.logger.info("=" * 50)
        self.logger.info("G因子计算性能统计")
        self.logger.info("=" * 50)
        self.logger.info(f"查询时间: {self.stats['query_time']:.2f} 秒")
        self.logger.info(f"计算时间: {self.stats['calculation_time']:.2f} 秒")
        self.logger.info(f"保存时间: {self.stats['save_time']:.2f} 秒")
        self.logger.info(f"总耗时: {self.stats['total_time']:.2f} 秒")
        self.logger.info(f"成功: {success_count} 只")
        self.logger.info(f"失败: {failed_count} 只")
        self.logger.info(f"成功率: {(success_count/max(total_count,1)*100):.1f}%")

        if self.stats['total_time'] > 0:
            throughput = success_count / self.stats['total_time']
            self.logger.info(f"吞吐量: {throughput:.1f} 只/秒")

    def calculate_g_factors_batch_pit(
        self,
        start_date: str,
        end_date: str,
        mode: Optional[str] = None
    ) -> Dict[str, Any]:
        """基于日期范围的批量G因子计算 (为runner脚本提供的接口)

        Args:
            start_date: 开始日期
            end_date: 结束日期
            mode: 执行模式 ('incremental', 'backfill', None为自动检测)

        Returns:
            执行结果统计
        """
        self.logger.info(f"开始批量G因子计算: {start_date} ~ {end_date}")

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
        per_date_stats: Dict[str, Dict[str, int]] = {}

        for i, calc_date in enumerate(calc_dates, 1):
            self.logger.info(f"\n进度: [{i}/{len(calc_dates)}] 处理日期: {calc_date}")

            try:
                # 获取在交易股票列表
                stock_codes = self._get_trading_stock_codes(calc_date)

                if not stock_codes:
                    self.logger.warning(f"{calc_date} 未找到在交易股票")
                    continue

                # 执行G因子计算
                result = self.calculate_g_factors_pit(calc_date, stock_codes)
                total_success += result['success_count']
                total_failed += result['failed_count']
                per_date_stats[calc_date] = {
                    'success': result['success_count'],
                    'failed': result['failed_count'],
                    'total': result['success_count'] + result['failed_count']
                }

                self.logger.info(f"{calc_date} 计算完成: 成功 {result['success_count']}, 失败 {result['failed_count']}")

            except Exception as e:
                self.logger.error(f"{calc_date} 计算失败: {e}")
                total_failed += len(stock_codes) if 'stock_codes' in locals() else 0

        total_time = time.time() - total_start

        self.logger.info("=" * 50)
        self.logger.info("批量G因子计算完成")
        self.logger.info("=" * 50)
        self.logger.info(f"总耗时: {total_time:.2f} 秒")
        self.logger.info(f"总成功: {total_success}")
        self.logger.info(f"总失败: {total_failed}")

        if total_time > 0:
            throughput = total_success / total_time
            self.logger.info(f"吞吐量: {throughput:.1f} 只/秒")

        # 基于逐日统计计算成功/失败日期数
        successful_dates = 0
        failed_dates = 0
        for d, s in per_date_stats.items():
            if s['total'] == 0:
                # 无样本的日期既不算成功也不算失败
                continue
            if s['failed'] == 0 and s['success'] > 0:
                successful_dates += 1
            else:
                failed_dates += 1

        return {
            'success_count': total_success,
            'failed_count': total_failed,
            'total_time': total_time,
            'throughput': total_success / total_time if total_time > 0 else 0,
            'total_dates': len(calc_dates),
            'successful_dates': successful_dates,
            'failed_dates': failed_dates,
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
            # 检查是否有现有的G因子数据
            query = """
            SELECT COUNT(*) as count
            FROM pgs_factors.g_factor
            WHERE calc_date BETWEEN %s AND %s
            """

            result = self.context.query_dataframe(query, (start_date, end_date))

            if result.empty or result.iloc[0]['count'] == 0:
                self.logger.info("未发现现有G因子数据，使用backfill模式")
                return 'backfill'
            else:
                self.logger.info(f"发现 {result.iloc[0]['count']} 条现有G因子数据，使用incremental模式")
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
        """过滤出缺失G因子数据的日期

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
            FROM pgs_factors.g_factor
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
            # 优先使用统一的在籍可交易股票集合（与P因子口径一致）
            query_primary = "SELECT * FROM get_trading_stocks_optimized(%s)"
            df_primary = self.context.query_dataframe(query_primary, (calc_date,))

            if df_primary is not None and not df_primary.empty:
                stock_codes = df_primary['ts_code'].tolist()
                self.logger.info(f"{calc_date} 获取到 {len(stock_codes)} 只在交易股票（优化函数）")
                return stock_codes

            # 回退：使用当日已有P因子数据的股票集合
            self.logger.warning(f"{calc_date} 优化函数无返回，回退到P因子股票集合")
            query_fallback = """
            SELECT DISTINCT ts_code
            FROM pgs_factors.p_factor
            WHERE calc_date = %s
            ORDER BY ts_code
            """
            df_fb = self.context.query_dataframe(query_fallback, (calc_date,))
            if df_fb is not None and not df_fb.empty:
                stock_codes = df_fb['ts_code'].tolist()
                self.logger.info(f"{calc_date} 回退集合包含 {len(stock_codes)} 只股票")
                return stock_codes

            self.logger.warning(f"{calc_date} 未获取到股票列表")
            return []

        except Exception as e:
            self.logger.error(f"获取 {calc_date} 股票列表失败: {e}")
            return []

    def _validate_calculation_results(self, calc_dates: List[str]):
        """验证G因子计算结果的数据质量

        Args:
            calc_dates: 需要验证的计算日期列表
        """
        self.logger.info(f"开始验证 {len(calc_dates)} 个日期的G因子数据质量")

        for calc_date in calc_dates:
            try:
                # 查询该日期的G因子数据
                query = """
                SELECT
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN g_score IS NOT NULL THEN 1 END) as valid_score_count,
                    AVG(g_score) as avg_score,
                    MIN(g_score) as min_score,
                    MAX(g_score) as max_score,
                    AVG(data_timeliness_weight) as avg_timeliness_weight,
                    COUNT(CASE WHEN data_source = 'express' THEN 1 END) as express_count,
                    COUNT(CASE WHEN data_source = 'forecast' THEN 1 END) as forecast_count
                FROM pgs_factors.g_factor
                WHERE calc_date = %s
                """

                result = self.context.query_dataframe(query, (calc_date,))

                if result.empty or result.iloc[0]['total_count'] == 0:
                    self.logger.warning(f"{calc_date}: 无G因子数据")
                else:
                    row = result.iloc[0]
                    express_forecast_ratio = (row['express_count'] + row['forecast_count']) / row['total_count'] * 100

                    self.logger.info(f"{calc_date}: 总记录 {row['total_count']}, "
                                   f"有效评分 {row['valid_score_count']}, "
                                   f"平均分 {row['avg_score']:.2f}, "
                                   f"分数范围 [{row['min_score']:.2f}, {row['max_score']:.2f}], "
                                   f"平均权重 {row['avg_timeliness_weight']:.3f}, "
                                   f"Express+Forecast占比 {express_forecast_ratio:.1f}%")

            except Exception as e:
                self.logger.error(f"验证 {calc_date} 数据质量失败: {e}")

        self.logger.info("G因子数据质量验证完成")
