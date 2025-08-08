"""
P/G/S因子计算系统主程序
=====================

简化的主运行脚本，提供清晰的接口和使用方式
"""

import sys
import os
from pathlib import Path
import yaml
import argparse
from datetime import datetime, timedelta
import pandas as pd

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from research.tools.context import ResearchContext
from research.pgs_factor import PGSFactorCalculator
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PGSFactorSystem:
    """P/G/S因子计算系统主类"""
    
    def __init__(self, config_path: str = None):
        """
        初始化系统
        
        Args:
            config_path: 配置文件路径，默认使用config.yaml
        """
        # 加载配置
        if config_path is None:
            config_path = Path(__file__).parent / 'config.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # 初始化研究上下文
        self.context = ResearchContext()
        
        # 初始化因子计算器
        self.calculator = PGSFactorCalculator(self.context)
        
        logger.info("P/G/S Factor System initialized")
    
    def calculate_single_date(self, date: str, stocks: list = None) -> pd.DataFrame:
        """
        计算单个日期的P/G/S因子
        
        Args:
            date: 计算日期 (YYYY-MM-DD)
            stocks: 股票列表，None表示计算所有股票
            
        Returns:
            包含P/G/S因子的DataFrame
        """
        logger.info(f"Calculating factors for {date}")
        
        if stocks is None:
            # 获取当日市值前100的股票
            stocks = self._get_top_stocks(date, n=100)
        
        # 计算因子
        factors_df = self.calculator.calculate_factors(date, stocks=stocks)
        
        logger.info(f"Calculated factors for {len(factors_df)} stocks")
        return factors_df
    
    def calculate_batch(self, start_date: str, end_date: str, 
                       frequency: str = 'W') -> pd.DataFrame:
        """
        批量计算P/G/S因子
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            frequency: 计算频率 ('D'=日, 'W'=周, 'M'=月)
            
        Returns:
            所有日期的因子合并DataFrame
        """
        logger.info(f"Batch calculation from {start_date} to {end_date}")
        
        # 基于交易日历生成日期序列，确保与市场交易日对齐
        from research.pgs_factor.data_loader import PGSDataLoader
        loader = PGSDataLoader(self.context)
        cal = loader.get_trading_dates(start_date.replace('-', ''), end_date.replace('-', ''))
        if frequency == 'W':
            cal = cal[cal['is_open'] == 1]
            # 仅取周五
            cal['weekday'] = cal['cal_date'].dt.weekday
            cal = cal[cal['weekday'] == 4]
        elif frequency == 'M':
            cal = cal[cal['is_open'] == 1]
            # 每月最后一个交易日
            cal['ym'] = cal['cal_date'].dt.to_period('M')
            cal = cal.sort_values('cal_date').groupby('ym').tail(1)
        else:
            cal = cal[cal['is_open'] == 1]
        dates = cal['cal_date']
        
        all_factors = []
        for date in dates:
            # cal_date 为 Timestamp
            date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
            try:
                factors = self.calculate_single_date(date_str)
                if not factors.empty:
                    factors['calc_date'] = date_str
                    all_factors.append(factors)
            except Exception as e:
                logger.error(f"Error calculating {date_str}: {e}")
                continue
        
        if all_factors:
            result = pd.concat(all_factors, ignore_index=True)
            logger.info(f"Batch calculation completed: {len(result)} records")
            return result
        else:
            logger.warning("No factors calculated")
            return pd.DataFrame()
    
    def _get_top_stocks(self, date: str, n: int = 100) -> list:
        """获取指定日期市值最大的N只股票"""
        query = f"""
        SELECT ts_code, total_mv
        FROM {self.config['data_source']['daily_basic']}
        WHERE trade_date = %(trade_date)s
        AND total_mv IS NOT NULL
        ORDER BY total_mv DESC
        LIMIT %(n)s
        """
        
        df = self.context.query_dataframe(query, {
            'trade_date': date.replace('-', ''),
            'n': n
        })
        
        return df['ts_code'].tolist() if not df.empty else []
    
    def save_results(self, factors_df: pd.DataFrame, output_name: str = None):
        """
        保存计算结果
        
        Args:
            factors_df: 因子数据
            output_name: 输出文件名（不含扩展名）
        """
        if output_name is None:
            output_name = f"pgs_factors_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        output_dir = Path(self.config['output']['directory'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 根据配置保存不同格式
        for fmt in self.config['output']['formats']:
            if fmt == 'csv':
                output_path = output_dir / f"{output_name}.csv"
                factors_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                logger.info(f"Saved CSV: {output_path}")
            elif fmt == 'pickle':
                output_path = output_dir / f"{output_name}.pkl"
                factors_df.to_pickle(output_path)
                logger.info(f"Saved Pickle: {output_path}")
            elif fmt == 'parquet':
                output_path = output_dir / f"{output_name}.parquet"
                factors_df.to_parquet(output_path)
                logger.info(f"Saved Parquet: {output_path}")
    
    def generate_report(self, factors_df: pd.DataFrame):
        """生成分析报告"""
        if not self.config['output']['generate_report']:
            return
        
        # TODO: 实现报告生成逻辑
        logger.info("Report generation not yet implemented")
    
    def close(self):
        """关闭系统"""
        self.context.close()
        logger.info("P/G/S Factor System closed")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='P/G/S Factor Calculation System')
    parser.add_argument('--mode', choices=['single', 'batch'], default='single',
                       help='Calculation mode')
    parser.add_argument('--date', type=str, 
                       help='Calculation date for single mode (YYYY-MM-DD)')
    parser.add_argument('--start', type=str,
                       help='Start date for batch mode')
    parser.add_argument('--end', type=str,
                       help='End date for batch mode')
    parser.add_argument('--frequency', choices=['D', 'W', 'M'], default='W',
                       help='Frequency for batch mode')
    parser.add_argument('--stocks', nargs='+',
                       help='Stock codes to calculate')
    parser.add_argument('--config', type=str,
                       help='Config file path')
    parser.add_argument('--output', type=str,
                       help='Output file name')
    
    args = parser.parse_args()
    
    # 创建系统实例
    system = PGSFactorSystem(args.config)
    
    try:
        if args.mode == 'single':
            # 单日计算
            if not args.date:
                args.date = datetime.now().strftime('%Y-%m-%d')
            
            factors = system.calculate_single_date(args.date, args.stocks)
            
            if not factors.empty:
                system.save_results(factors, args.output)
                print(f"\n✅ Calculated {len(factors)} stocks for {args.date}")
                print(f"📊 Factor Summary:")
                print(factors[['ts_code', 'p_score', 'g_score', 's_score', 'total_score']].describe())
        
        elif args.mode == 'batch':
            # 批量计算
            if not args.start or not args.end:
                # 默认计算最近3个月
                args.end = datetime.now().strftime('%Y-%m-%d')
                args.start = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            
            factors = system.calculate_batch(args.start, args.end, args.frequency)
            
            if not factors.empty:
                system.save_results(factors, args.output)
                print(f"\n✅ Calculated {len(factors)} records from {args.start} to {args.end}")
                print(f"📊 Factor Summary by Date:")
                summary = factors.groupby('calc_date')[['p_score', 'g_score', 's_score']].agg(['mean', 'std'])
                print(summary)
        
    finally:
        system.close()


if __name__ == "__main__":
    main()
