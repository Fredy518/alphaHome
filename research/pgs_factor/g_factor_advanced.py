"""
高级G因子计算模块
==================

基于多维度成长子因子的综合评估体系：
1. G_Efficiency_Surprise (效率惊喜因子)
2. G_Efficiency_Momentum (效率动量因子)  
3. G_Revenue_Momentum (营收动量因子)
4. G_Profit_Momentum (利润动量因子)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from scipy import stats
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class AdvancedGFactorCalculator:
    """
    高级G因子计算器
    
    计算4个成长子因子并通过排名合成最终G_score
    """
    
    def __init__(self, min_quarters: int = 12, target_quarters: int = 20):
        """
        初始化高级G因子计算器
        
        Args:
            min_quarters: 最少需要的P_score季度数（默认12个）
            target_quarters: 目标P_score季度数（默认20个）
        """
        self.min_quarters = min_quarters
        self.target_quarters = target_quarters
        
        # 子因子权重配置
        self.weights = {
            'efficiency_surprise': 0.25,
            'efficiency_momentum': 0.25,
            'revenue_momentum': 0.25,
            'profit_momentum': 0.25
        }
        
        logger.info(f"AdvancedGFactorCalculator initialized with weights: {self.weights}")
    
    def calculate_g_factors(self,
                           stocks: List[str],
                           calc_date: str,
                           financial_data: pd.DataFrame,
                           p_scores_current: pd.DataFrame,
                           p_scores_historical: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        计算所有股票的G因子
        
        Args:
            stocks: 股票列表
            calc_date: 计算日期 (YYYYMMDD)
            financial_data: 财务数据DataFrame (包含营收、利润等)
            p_scores_current: 当前P_score数据
            p_scores_historical: 历史P_score数据
            
        Returns:
            包含G因子及所有子因子的DataFrame
        """
        logger.info(f"Calculating advanced G factors for {len(stocks)} stocks on {calc_date}")
        
        results = []
        
        # 步骤1：计算所有子因子
        subfactors = self._calculate_all_subfactors(
            stocks, calc_date, financial_data, 
            p_scores_current, p_scores_historical
        )
        
        if subfactors.empty:
            logger.warning("No subfactors calculated")
            return pd.DataFrame()
        
        # 步骤2：将子因子转换为排名
        ranked_subfactors = self._convert_to_ranks(subfactors)
        
        # 步骤3：合成最终G_score
        final_scores = self._calculate_final_scores(ranked_subfactors)
        
        # 组装结果
        result_df = pd.concat([subfactors, ranked_subfactors, final_scores], axis=1)
        
        logger.info(f"G factors calculated for {len(result_df)} stocks")
        return result_df
    
    def _calculate_all_subfactors(self,
                                  stocks: List[str],
                                  calc_date: str,
                                  financial_data: pd.DataFrame,
                                  p_scores_current: pd.DataFrame,
                                  p_scores_historical: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        计算所有成长子因子
        
        Returns:
            包含4个子因子的DataFrame
        """
        subfactors_list = []
        
        for stock in stocks:
            try:
                subfactors = {}
                subfactors['ts_code'] = stock
                subfactors['calc_date'] = calc_date
                
                # 1. 效率惊喜因子 (G_Efficiency_Surprise)
                es_value = self._calculate_efficiency_surprise(
                    stock, p_scores_current, p_scores_historical
                )
                subfactors['g_efficiency_surprise'] = es_value
                
                # 2. 效率动量因子 (G_Efficiency_Momentum)
                em_value = self._calculate_efficiency_momentum(
                    stock, p_scores_current, p_scores_historical
                )
                subfactors['g_efficiency_momentum'] = em_value
                
                # 3. 营收动量因子 (G_Revenue_Momentum)
                rm_value = self._calculate_revenue_momentum(
                    stock, calc_date, financial_data
                )
                subfactors['g_revenue_momentum'] = rm_value
                
                # 4. 利润动量因子 (G_Profit_Momentum)
                pm_value = self._calculate_profit_momentum(
                    stock, calc_date, financial_data
                )
                subfactors['g_profit_momentum'] = pm_value
                
                # 记录数据质量
                data_periods = self._count_stock_historical_data(
                    stock, p_scores_historical
                )
                subfactors['data_periods'] = data_periods
                
                subfactors_list.append(subfactors)
                
            except Exception as e:
                logger.warning(f"Failed to calculate subfactors for {stock}: {e}")
                continue
        
        if subfactors_list:
            return pd.DataFrame(subfactors_list)
        return pd.DataFrame()
    
    def _calculate_efficiency_surprise(self,
                                      stock: str,
                                      p_scores_current: pd.DataFrame,
                                      p_scores_historical: Dict[str, pd.DataFrame]) -> float:
        """
        计算效率惊喜因子：ΔP_score_YoY / Std(ΔP_score_YoY)
        
        衡量盈利能力改善的相对强度
        """
        try:
            # 获取当前P_score
            current_row = p_scores_current[p_scores_current['ts_code'] == stock]
            if current_row.empty:
                return np.nan
            current_p_score = current_row.iloc[0]['p_score']
            
            # 获取去年同期P_score
            current_date = max(p_scores_historical.keys())
            last_year_date = self._get_last_year_date(current_date)
            
            if last_year_date not in p_scores_historical:
                return np.nan
            
            last_year_df = p_scores_historical[last_year_date]
            last_year_row = last_year_df[last_year_df['ts_code'] == stock]
            if last_year_row.empty:
                return np.nan
            last_year_p_score = last_year_row.iloc[0]['p_score']
            
            # 计算YoY变化
            p_score_yoy = current_p_score - last_year_p_score
            
            # 计算历史变化的标准差
            historical_changes = self._get_historical_p_score_changes(
                stock, p_scores_historical
            )
            
            if len(historical_changes) < 8:  # 需要足够的历史数据
                return np.nan
            
            std_changes = np.std(historical_changes)
            if std_changes == 0:
                return np.nan
            
            # 计算惊喜因子
            surprise = p_score_yoy / std_changes
            
            return surprise
            
        except Exception as e:
            logger.debug(f"Error calculating efficiency surprise for {stock}: {e}")
            return np.nan
    
    def _calculate_efficiency_momentum(self,
                                      stock: str,
                                      p_scores_current: pd.DataFrame,
                                      p_scores_historical: Dict[str, pd.DataFrame]) -> float:
        """
        计算效率动量因子：ΔP_score_YoY
        
        衡量盈利能力改善的绝对力度
        """
        try:
            # 获取当前P_score
            current_row = p_scores_current[p_scores_current['ts_code'] == stock]
            if current_row.empty:
                return np.nan
            current_p_score = current_row.iloc[0]['p_score']
            
            # 获取去年同期P_score
            current_date = max(p_scores_historical.keys())
            last_year_date = self._get_last_year_date(current_date)
            
            if last_year_date not in p_scores_historical:
                return np.nan
            
            last_year_df = p_scores_historical[last_year_date]
            last_year_row = last_year_df[last_year_df['ts_code'] == stock]
            if last_year_row.empty:
                return np.nan
            last_year_p_score = last_year_row.iloc[0]['p_score']
            
            # 计算YoY变化
            momentum = current_p_score - last_year_p_score
            
            return momentum
            
        except Exception as e:
            logger.debug(f"Error calculating efficiency momentum for {stock}: {e}")
            return np.nan
    
    def _calculate_revenue_momentum(self,
                                   stock: str,
                                   calc_date: str,
                                   financial_data: pd.DataFrame) -> float:
        """
        计算营收动量因子：营业收入(TTM)同比增长率
        
        衡量市场扩张的速度
        """
        try:
            # 筛选该股票的数据
            stock_data = financial_data[financial_data['ts_code'] == stock]
            if stock_data.empty:
                return np.nan
            
            # 获取最新的营业收入TTM
            current_revenue = self._get_ttm_revenue(stock_data, calc_date)
            if pd.isna(current_revenue):
                return np.nan
            
            # 获取去年同期的营业收入TTM
            last_year_date = self._get_last_year_date(calc_date)
            last_year_revenue = self._get_ttm_revenue(stock_data, last_year_date)
            if pd.isna(last_year_revenue) or last_year_revenue == 0:
                return np.nan
            
            # 计算同比增长率
            revenue_growth = (current_revenue - last_year_revenue) / abs(last_year_revenue) * 100
            
            return revenue_growth
            
        except Exception as e:
            logger.debug(f"Error calculating revenue momentum for {stock}: {e}")
            return np.nan
    
    def _calculate_profit_momentum(self,
                                  stock: str,
                                  calc_date: str,
                                  financial_data: pd.DataFrame) -> float:
        """
        计算利润动量因子：扣非净利润(TTM)同比增长率
        
        衡量最终盈利成果的增长
        """
        try:
            # 筛选该股票的数据
            stock_data = financial_data[financial_data['ts_code'] == stock]
            if stock_data.empty:
                return np.nan
            
            # 获取最新的扣非净利润TTM
            current_profit = self._get_ttm_profit(stock_data, calc_date)
            if pd.isna(current_profit):
                return np.nan
            
            # 获取去年同期的扣非净利润TTM
            last_year_date = self._get_last_year_date(calc_date)
            last_year_profit = self._get_ttm_profit(stock_data, last_year_date)
            if pd.isna(last_year_profit):
                return np.nan
            
            # 处理负利润的情况
            if last_year_profit == 0:
                if current_profit > 0:
                    return 100.0  # 扭亏为盈，给予高分
                else:
                    return np.nan
            
            # 计算同比增长率
            profit_growth = (current_profit - last_year_profit) / abs(last_year_profit) * 100
            
            return profit_growth
            
        except Exception as e:
            logger.debug(f"Error calculating profit momentum for {stock}: {e}")
            return np.nan
    
    def _get_ttm_revenue(self, stock_data: pd.DataFrame, date: str) -> float:
        """
        获取指定日期的营业收入TTM
        
        简化实现：获取最近4个季度的营业收入总和
        """
        try:
            # 转换日期格式
            target_date = pd.to_datetime(date)
            
            # 筛选在目标日期之前公告的数据
            available_data = stock_data[
                pd.to_datetime(stock_data['ann_date']) <= target_date
            ].sort_values('end_date', ascending=False)
            
            if available_data.empty:
                return np.nan
            
            # 获取最近4个季度的数据
            recent_quarters = available_data.head(4)
            
            if 'revenue' in recent_quarters.columns:
                # 如果有revenue字段，计算TTM
                ttm_revenue = recent_quarters['revenue'].sum()
                return ttm_revenue
            elif 'total_revenue' in recent_quarters.columns:
                ttm_revenue = recent_quarters['total_revenue'].sum()
                return ttm_revenue
            else:
                return np.nan
                
        except Exception as e:
            logger.debug(f"Error getting TTM revenue: {e}")
            return np.nan
    
    def _get_ttm_profit(self, stock_data: pd.DataFrame, date: str) -> float:
        """
        获取指定日期的扣非净利润TTM
        
        简化实现：获取最近4个季度的扣非净利润总和
        """
        try:
            # 转换日期格式
            target_date = pd.to_datetime(date)
            
            # 筛选在目标日期之前公告的数据
            available_data = stock_data[
                pd.to_datetime(stock_data['ann_date']) <= target_date
            ].sort_values('end_date', ascending=False)
            
            if available_data.empty:
                return np.nan
            
            # 获取最近4个季度的数据
            recent_quarters = available_data.head(4)
            
            # 优先使用扣非净利润，其次使用归母净利润
            if 'n_income_attr_p_non_recurring' in recent_quarters.columns:
                ttm_profit = recent_quarters['n_income_attr_p_non_recurring'].sum()
            elif 'n_income_attr_p' in recent_quarters.columns:
                ttm_profit = recent_quarters['n_income_attr_p'].sum()
            else:
                return np.nan
            
            return ttm_profit
                
        except Exception as e:
            logger.debug(f"Error getting TTM profit: {e}")
            return np.nan
    
    def _convert_to_ranks(self, subfactors: pd.DataFrame) -> pd.DataFrame:
        """
        将子因子转换为百分位排名 (0-100)
        
        Args:
            subfactors: 包含原始子因子值的DataFrame
            
        Returns:
            包含排名的DataFrame
        """
        logger.info("Converting subfactors to percentile ranks")
        
        rank_columns = {}
        
        # 对每个子因子进行排名转换
        for factor in ['g_efficiency_surprise', 'g_efficiency_momentum', 
                      'g_revenue_momentum', 'g_profit_momentum']:
            if factor in subfactors.columns:
                # 计算排名（处理NaN值）
                ranks = self._calculate_percentile_rank(subfactors[factor])
                rank_name = f"rank_{factor.split('_', 1)[1]}"  # 简化列名
                rank_columns[rank_name] = ranks
        
        rank_df = pd.DataFrame(rank_columns, index=subfactors.index)
        return rank_df
    
    def _calculate_percentile_rank(self, values: pd.Series) -> pd.Series:
        """
        将原始值转换为百分位排名（0-100）
        
        Args:
            values: 原始因子值
            
        Returns:
            百分位排名（0-100）
        """
        # 处理NaN值
        valid_mask = ~values.isna()
        ranks = pd.Series(index=values.index, dtype=float)
        
        if valid_mask.sum() > 1:
            # 使用scipy.stats.rankdata计算排名
            valid_values = values[valid_mask]
            rank_values = stats.rankdata(valid_values, method='average')
            
            # 转换为百分位（0-100）
            percentile_ranks = (rank_values - 1) / (len(rank_values) - 1) * 100
            ranks[valid_mask] = percentile_ranks
        elif valid_mask.sum() == 1:
            # 只有一个有效值，设为50
            ranks[valid_mask] = 50.0
        
        # NaN值保持为NaN
        ranks[~valid_mask] = np.nan
        
        return ranks
    
    def _calculate_final_scores(self, ranked_subfactors: pd.DataFrame) -> pd.DataFrame:
        """
        根据排名计算最终G_score
        
        Final_G_Score = 0.25*Rank_ES + 0.25*Rank_EM + 0.25*Rank_RM + 0.25*Rank_PM
        
        Args:
            ranked_subfactors: 包含子因子排名的DataFrame
            
        Returns:
            包含最终G_score的DataFrame
        """
        logger.info("Calculating final G scores")
        
        # 提取排名列
        rank_cols = [col for col in ranked_subfactors.columns if col.startswith('rank_')]
        
        if not rank_cols:
            logger.warning("No rank columns found")
            return pd.DataFrame({'g_score': [np.nan] * len(ranked_subfactors)})
        
        # 计算加权平均（等权重0.25）
        g_scores = pd.Series(index=ranked_subfactors.index, dtype=float)
        
        for i in ranked_subfactors.index:
            row_ranks = []
            for col in rank_cols:
                if not pd.isna(ranked_subfactors.loc[i, col]):
                    row_ranks.append(ranked_subfactors.loc[i, col])
            
            if row_ranks:
                # 计算有效排名的平均值
                g_scores[i] = np.mean(row_ranks)
            else:
                g_scores[i] = np.nan
        
        # 数据质量评估
        data_quality = self._assess_data_quality(ranked_subfactors)
        
        result_df = pd.DataFrame({
            'g_score': g_scores,
            'data_quality': data_quality,
            'valid_factors': ranked_subfactors[rank_cols].notna().sum(axis=1)
        })
        
        return result_df
    
    def _assess_data_quality(self, ranked_subfactors: pd.DataFrame) -> pd.Series:
        """
        评估数据质量
        
        Args:
            ranked_subfactors: 排名数据
            
        Returns:
            数据质量标签 ('high'/'medium'/'low')
        """
        rank_cols = [col for col in ranked_subfactors.columns if col.startswith('rank_')]
        valid_counts = ranked_subfactors[rank_cols].notna().sum(axis=1)
        
        quality = pd.Series(index=ranked_subfactors.index, dtype=str)
        quality[valid_counts == 4] = 'high'
        quality[valid_counts == 3] = 'medium'
        quality[valid_counts <= 2] = 'low'
        
        return quality
    
    # ========================================
    # 辅助方法
    # ========================================
    
    def _get_last_year_date(self, date_str: str) -> str:
        """
        获取去年同期日期
        
        Args:
            date_str: 当前日期字符串（格式：'YYYYMMDD'）
            
        Returns:
            去年同期日期字符串
        """
        if len(date_str) == 8:  # YYYYMMDD格式
            year = int(date_str[:4])
            month_day = date_str[4:]
            return f"{year - 1}{month_day}"
        else:  # YYYY-MM-DD格式
            date_obj = pd.to_datetime(date_str)
            last_year = date_obj - pd.DateOffset(years=1)
            return last_year.strftime('%Y%m%d')
    
    def _count_stock_historical_data(self, ts_code: str, 
                                    historical_p_scores: Dict[str, pd.DataFrame]) -> int:
        """
        统计某个股票的历史P_score数据点数量
        
        Args:
            ts_code: 股票代码
            historical_p_scores: 历史P_score数据
            
        Returns:
            历史数据点数量
        """
        count = 0
        for date, df in historical_p_scores.items():
            if ts_code in df['ts_code'].values:
                count += 1
        return count
    
    def _get_historical_p_score_changes(self, stock: str, 
                                       p_scores_historical: Dict[str, pd.DataFrame]) -> List[float]:
        """
        获取股票的历史P_score YoY变化序列
        
        Args:
            stock: 股票代码
            p_scores_historical: 历史P_score数据
            
        Returns:
            历史变化值列表
        """
        changes = []
        sorted_dates = sorted(p_scores_historical.keys())
        
        for i in range(1, len(sorted_dates)):
            current_date = sorted_dates[i]
            last_year_date = self._get_last_year_date(current_date)
            
            if last_year_date in p_scores_historical:
                current_df = p_scores_historical[current_date]
                last_year_df = p_scores_historical[last_year_date]
                
                current_row = current_df[current_df['ts_code'] == stock]
                last_year_row = last_year_df[last_year_df['ts_code'] == stock]
                
                if not current_row.empty and not last_year_row.empty:
                    change = current_row.iloc[0]['p_score'] - last_year_row.iloc[0]['p_score']
                    changes.append(change)
        
        return changes
    
    def update_weights(self, new_weights: Dict[str, float]):
        """
        更新子因子权重
        
        Args:
            new_weights: 新的权重字典
        """
        if sum(new_weights.values()) != 1.0:
            raise ValueError("Weights must sum to 1.0")
        
        self.weights = new_weights
        logger.info(f"Updated weights: {self.weights}")


def test_advanced_g_factor():
    """测试高级G因子计算"""
    import pandas as pd
    import numpy as np
    
    # 创建测试数据
    np.random.seed(42)
    stocks = [f"{i:06d}.SZ" for i in range(1, 21)]
    
    # 当前P_score
    current_p_scores = pd.DataFrame({
        'ts_code': stocks,
        'p_score': np.random.randn(20) * 10 + 50
    })
    
    # 历史P_score
    historical_p_scores = {}
    dates = ['20250801', '20250501', '20250201', '20241101',
             '20240801', '20240501', '20240201', '20231101']
    
    for i, date in enumerate(dates):
        historical_p_scores[date] = pd.DataFrame({
            'ts_code': stocks,
            'p_score': np.random.randn(20) * 10 + 45 - i
        })
    
    # 模拟财务数据
    financial_data = pd.DataFrame({
        'ts_code': np.repeat(stocks, 8),
        'ann_date': np.tile(dates, 20),
        'end_date': np.tile(dates, 20),
        'revenue': np.random.randn(160) * 1000000 + 10000000,
        'n_income_attr_p': np.random.randn(160) * 100000 + 1000000
    })
    
    # 创建计算器
    calculator = AdvancedGFactorCalculator()
    
    # 计算G因子
    result = calculator.calculate_g_factors(
        stocks, 
        '20250801',
        financial_data,
        current_p_scores,
        historical_p_scores
    )
    
    # 显示结果
    print("\n高级G因子计算结果（前10只股票）:")
    print(result.head(10)[['ts_code', 'g_score', 'data_quality', 'valid_factors']].to_string())
    
    # 统计分析
    print("\nG因子统计:")
    print(f"平均值: {result['g_score'].mean():.2f}")
    print(f"标准差: {result['g_score'].std():.2f}")
    print(f"最小值: {result['g_score'].min():.2f}")
    print(f"最大值: {result['g_score'].max():.2f}")
    
    print("\n数据质量分布:")
    print(result['data_quality'].value_counts())
    
    return result


if __name__ == "__main__":
    # 运行测试
    test_result = test_advanced_g_factor()
