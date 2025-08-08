"""
G/S因子批量计算处理器
=====================

定期（周频）批量计算G因子和S因子。
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, date, timedelta
import logging

from ..factor_calculator import PGSFactorCalculator
from ..g_factor_new import GFactorCalculator
from ..database.db_manager import PGSFactorDBManager
from ..data_loader import PGSDataLoader

logger = logging.getLogger(__name__)


class GSFactorBatchProcessor:
    """G/S因子批量处理器"""
    
    def __init__(self, context):
        """
        初始化批量处理器
        
        Args:
            context: ResearchContext实例
        """
        self.context = context
        self.factor_calculator = PGSFactorCalculator(context)
        self.g_calculator = GFactorCalculator(min_quarters=12, target_quarters=20)
        self.db_manager = PGSFactorDBManager(context)
        
        logger.info("GSFactorBatchProcessor initialized")
    
    def calculate_weekly_factors(self, calc_date: date = None):
        """
        计算周频因子（每周五运行）
        
        Args:
            calc_date: 计算日期，默认为今天
        """
        if calc_date is None:
            calc_date = date.today()
        
        # 确保是周五
        if calc_date.weekday() != 4:
            # 找到最近的周五
            days_until_friday = (4 - calc_date.weekday()) % 7
            if days_until_friday == 0:
                days_until_friday = 7  # 如果不是周五，找下一个周五
            calc_date = calc_date + timedelta(days=days_until_friday)
            logger.info(f"Adjusted calculation date to Friday: {calc_date}")
        
        logger.info(f"Starting weekly factor calculation for {calc_date}")
        
        try:
            # 获取活跃股票列表
            stocks = self._get_active_stocks(calc_date)
            logger.info(f"Processing {len(stocks)} stocks")

            # 先计算并入库P因子（批处理，使用各自report的最新公告日）
            p_factors = self._calculate_p_factors(stocks, calc_date)
            if not p_factors.empty:
                # 计算横截面排名与合成分
                for col, rank_col in [('gpa','rank_gpa'), ('roa_excl','rank_roa'), ('roe_excl','rank_roe')]:
                    if col in p_factors.columns:
                        vals = p_factors[col]
                        ranks = vals.rank(pct=True) * 100.0
                        ranks[vals.isna()] = None
                        p_factors[rank_col] = ranks
                rank_cols = [c for c in ['rank_gpa','rank_roa','rank_roe'] if c in p_factors.columns]
                if rank_cols:
                    p_factors['p_score'] = p_factors[rank_cols].mean(axis=1)

                p_factors['calc_date'] = calc_date
                # 行内包含ann_date
                self.db_manager.save_p_factor(p_factors, ann_date=calc_date, data_source='report')
                logger.info(f"Saved {len(p_factors)} P factor records")

            # 计算G因子
            g_factors = self._calculate_g_factors(stocks, calc_date)
            if not g_factors.empty:
                self.db_manager.save_g_factors(g_factors, calc_date)
                logger.info(f"Saved {len(g_factors)} G factor records")

            # 计算S因子
            s_factors = self._calculate_s_factors(stocks, calc_date)
            if not s_factors.empty:
                self.db_manager.save_s_factors(s_factors, calc_date)
                logger.info(f"Saved {len(s_factors)} S factor records")

            # 计算数据质量指标
            self._calculate_quality_metrics(calc_date, g_factors, s_factors)

            # 更新处理日志
            total_records = (0 if 'p_factors' not in locals() or p_factors.empty else len(p_factors)) + len(g_factors) + len(s_factors)
            self.db_manager.update_processing_log(
                'gs_factor_batch',
                datetime.combine(calc_date, datetime.min.time()),
                total_records,
                'success'
            )
            
            logger.info(f"Weekly factor calculation completed for {calc_date}")
            
        except Exception as e:
            logger.error(f"Error in weekly factor calculation: {e}")
            self.db_manager.update_processing_log(
                'gs_factor_batch',
                datetime.now(),
                0,
                'failed',
                str(e)
            )
            raise
    
    def _get_active_stocks(self, calc_date: date) -> List[str]:
        """
        获取活跃股票列表
        
        Args:
            calc_date: 计算日期
            
        Returns:
            股票代码列表
        """
        # 获取有P因子数据的股票
        query = """
            SELECT DISTINCT ts_code
            FROM pgs_factors.p_factor
            WHERE calc_date <= %s
                AND calc_date >= %s - INTERVAL '30 days'
        """
        
        try:
            df = self.context.query_dataframe(query, (calc_date, calc_date))
            stocks = df['ts_code'].tolist()
            
            # 额外筛选条件（如上市时间、ST等）
            stocks = self._filter_stocks(stocks, calc_date)
            
            return stocks
            
        except Exception as e:
            logger.error(f"Failed to get active stocks: {e}")
            return []
    
    def _filter_stocks(self, stocks: List[str], calc_date: date) -> List[str]:
        """
        筛选股票（排除ST、新股等）
        
        Args:
            stocks: 原始股票列表
            calc_date: 计算日期
            
        Returns:
            筛选后的股票列表
        """
        # 这里可以添加更多筛选逻辑
        # 例如：排除ST股票、上市不满60天的新股等
        
        # 简化处理，暂时返回全部
        return stocks
    
    def _calculate_g_factors(self, stocks: List[str], calc_date: date) -> pd.DataFrame:
        """
        批量计算G因子
        
        Args:
            stocks: 股票列表
            calc_date: 计算日期
            
        Returns:
            G因子DataFrame
        """
        logger.info(f"Calculating G factors for {len(stocks)} stocks")
        
        try:
            # 获取当前P_score
            current_p_scores = self.db_manager.get_latest_p_factors(stocks, calc_date)
            
            if current_p_scores.empty:
                logger.warning("No current P scores available")
                return pd.DataFrame()
            
            # 准备当前P_score数据
            current_p_scores_df = current_p_scores[['ts_code', 'p_score']].copy()
            
            # 获取历史P_score（5年数据）
            start_date = calc_date - timedelta(days=365 * 5)
            historical_p_scores = self.db_manager.get_historical_p_scores(
                stocks, start_date, calc_date
            )
            
            if not historical_p_scores:
                logger.warning("No historical P scores available")
                return pd.DataFrame()
            
            # 计算G因子
            g_factors = self.g_calculator.calculate_g_factor(
                current_p_scores_df,
                historical_p_scores
            )
            
            # 添加数据期数信息
            for i, row in g_factors.iterrows():
                stock = row['ts_code']
                data_periods = self.g_calculator._count_stock_historical_data(
                    stock, historical_p_scores
                )
                g_factors.at[i, 'data_periods'] = data_periods
            
            return g_factors
            
        except Exception as e:
            logger.error(f"Failed to calculate G factors: {e}")
            return pd.DataFrame()

    def _calculate_p_factors(self, stocks: List[str], calc_date: date) -> pd.DataFrame:
        """
        批量计算并返回P子因子（仅report口径）：gpa/roa_excl/roe_excl，含各自最新report公告日。
        """
        logger.info(f"Calculating P factors for {len(stocks)} stocks")
        results = []
        trade_date_str = calc_date.strftime('%Y%m%d')

        # 增量策略：若最新report公告日未超过最近一次已存P记录的ann_date，则跳过该股票
        try:
            latest_report = self.db_manager.get_latest_report_ann_dates(stocks, calc_date)
            latest_stored = self.db_manager.get_latest_stored_p_ann_dates(stocks, calc_date, 'report')
            merged = pd.merge(latest_report, latest_stored, on='ts_code', how='left')
        except Exception as e:
            logger.warning(f"Failed to load ann_date baselines, fallback to full calculation: {e}")
            merged = pd.DataFrame({'ts_code': stocks, 'report_ann_date': None, 'stored_ann_date': None})

        for stock in stocks:
            try:
                row = merged[merged['ts_code'] == stock]
                report_ann = row['report_ann_date'].iloc[0] if not row.empty else None
                stored_ann = row['stored_ann_date'].iloc[0] if not row.empty else None

                if pd.notna(report_ann) and pd.notna(stored_ann) and pd.to_datetime(report_ann) <= pd.to_datetime(stored_ann):
                    # 无新报告，跳过
                    continue

                pf = self.factor_calculator._calculate_p_factor(stock, trade_date_str)
                if pf:
                    out = {
                        'ts_code': stock,
                        'gpa': pf.get('gpa'),
                        'roa_excl': pf.get('roa_excl'),
                        'roe_excl': pf.get('roe_excl'),
                        'ann_date': pf.get('ann_date', None)
                    }
                    if out['ann_date'] is None and pd.notna(report_ann):
                        out['ann_date'] = pd.to_datetime(report_ann).date()
                    results.append(out)
            except Exception as e:
                logger.debug(f"Skip P for {stock}: {e}")
                continue
        df = pd.DataFrame(results) if results else pd.DataFrame()
        return df
    
    def _calculate_s_factors(self, stocks: List[str], calc_date: date) -> pd.DataFrame:
        """
        批量计算S因子
        
        Args:
            stocks: 股票列表
            calc_date: 计算日期
            
        Returns:
            S因子DataFrame
        """
        logger.info(f"Calculating S factors for {len(stocks)} stocks")
        
        try:
            # 使用factor_calculator计算S因子
            # 这里需要先加载数据
            all_data = self.factor_calculator.data_loader.load_all_data(
                calc_date.strftime('%Y%m%d'), stocks
            )
            self.factor_calculator.pit_db.update_data(all_data)
            
            # 计算S因子
            s_factors = []
            for stock in stocks:
                try:
                    s_factor = self.factor_calculator._calculate_s_factor(
                        stock, calc_date.strftime('%Y%m%d')
                    )
                    s_factor['ts_code'] = stock
                    s_factors.append(s_factor)
                except Exception as e:
                    logger.warning(f"Failed to calculate S factor for {stock}: {e}")
                    continue
            
            if s_factors:
                return pd.DataFrame(s_factors)
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Failed to calculate S factors: {e}")
            return pd.DataFrame()
    
    def _calculate_quality_metrics(self, calc_date: date, 
                                  g_factors: pd.DataFrame,
                                  s_factors: pd.DataFrame):
        """
        计算数据质量指标
        
        Args:
            calc_date: 计算日期
            g_factors: G因子数据
            s_factors: S因子数据
        """
        metrics = {}
        
        # G因子质量指标
        if not g_factors.empty:
            metrics['g_factor_coverage'] = len(g_factors[g_factors['g_score'].notna()]) / len(g_factors)
            metrics['g_factor_high_quality'] = len(g_factors[g_factors.get('data_periods', 0) >= 20]) / len(g_factors)
            metrics['g_factor_mean'] = g_factors['g_score'].mean()
            metrics['g_factor_std'] = g_factors['g_score'].std()
        
        # S因子质量指标
        if not s_factors.empty:
            metrics['s_factor_coverage'] = len(s_factors[s_factors['s_score'].notna()]) / len(s_factors)
            metrics['s_factor_mean'] = s_factors['s_score'].mean()
            metrics['s_factor_std'] = s_factors['s_score'].std()
        
        # 保存质量指标
        if metrics:
            self.db_manager.save_quality_metrics(calc_date, metrics)
            logger.info(f"Saved {len(metrics)} quality metrics")
    
    def backfill_historical_factors(self, start_date: date, end_date: date):
        """
        回填历史因子数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
        """
        logger.info(f"Backfilling G/S factors from {start_date} to {end_date}")
        
        # 生成所有周五日期
        current_date = start_date
        while current_date <= end_date:
            # 找到下一个周五
            days_until_friday = (4 - current_date.weekday()) % 7
            if days_until_friday == 0 and current_date.weekday() != 4:
                days_until_friday = 7
            
            friday = current_date + timedelta(days=days_until_friday)
            
            if friday <= end_date:
                logger.info(f"Processing {friday}")
                try:
                    self.calculate_weekly_factors(friday)
                except Exception as e:
                    logger.error(f"Failed to process {friday}: {e}")
                    # 继续处理下一个日期
            
            # 移动到下一周
            current_date = friday + timedelta(days=1)
        
        logger.info("Backfill completed")
    
    def run_scheduled(self):
        """
        按计划运行（用于调度器）
        """
        # 获取今天日期
        today = date.today()
        
        # 如果是周五，运行计算
        if today.weekday() == 4:
            logger.info("Today is Friday, running weekly calculation")
            self.calculate_weekly_factors(today)
        else:
            logger.info(f"Today is {today.strftime('%A')}, skipping calculation")
    
    def get_factor_statistics(self, calc_date: date = None) -> Dict:
        """
        获取因子统计信息
        
        Args:
            calc_date: 计算日期
            
        Returns:
            统计信息字典
        """
        if calc_date is None:
            calc_date = date.today()
        
        stats = {}
        
        # 获取因子汇总
        summary = self.db_manager.get_factor_summary(calc_date=calc_date)
        
        if not summary.empty:
            stats['total_stocks'] = len(summary)
            stats['p_factor_coverage'] = len(summary[summary['p_score'].notna()]) / len(summary)
            stats['g_factor_coverage'] = len(summary[summary['g_score'].notna()]) / len(summary)
            stats['s_factor_coverage'] = len(summary[summary['s_score'].notna()]) / len(summary)
            
            # Top 10 stocks
            top_stocks = summary.nlargest(10, 'total_score')[['ts_code', 'total_score']]
            stats['top_10_stocks'] = top_stocks.to_dict('records')
        
        return stats


def main():
    """主函数"""
    from research.tools.context import ResearchContext
    import argparse
    
    parser = argparse.ArgumentParser(description='G/S Factor Batch Processor')
    parser.add_argument('--date', type=str, help='Calculation date (YYYY-MM-DD)')
    parser.add_argument('--backfill', action='store_true', help='Backfill historical data')
    parser.add_argument('--start', type=str, help='Backfill start date')
    parser.add_argument('--end', type=str, help='Backfill end date')
    
    args = parser.parse_args()
    
    # 初始化
    context = ResearchContext()
    processor = GSFactorBatchProcessor(context)
    
    if args.backfill:
        # 回填历史数据
        start_date = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date.today() - timedelta(days=90)
        end_date = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
        processor.backfill_historical_factors(start_date, end_date)
    else:
        # 计算指定日期的因子
        calc_date = datetime.strptime(args.date, '%Y-%m-%d').date() if args.date else None
        processor.calculate_weekly_factors(calc_date)
        
        # 显示统计信息
        stats = processor.get_factor_statistics(calc_date)
        print("\nFactor Statistics:")
        print(f"Total stocks: {stats.get('total_stocks', 0)}")
        print(f"P factor coverage: {stats.get('p_factor_coverage', 0):.2%}")
        print(f"G factor coverage: {stats.get('g_factor_coverage', 0):.2%}")
        print(f"S factor coverage: {stats.get('s_factor_coverage', 0):.2%}")
        
        if 'top_10_stocks' in stats:
            print("\nTop 10 stocks:")
            for stock in stats['top_10_stocks']:
                print(f"  {stock['ts_code']}: {stock['total_score']:.2f}")


if __name__ == "__main__":
    main()
