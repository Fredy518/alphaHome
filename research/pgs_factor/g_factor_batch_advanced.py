"""
G因子批量处理器（高级版）
========================

集成高级G因子计算的批量处理系统，支持：
- 基于财务公告日期的触发计算
- 多维度成长子因子分析
- 数据库批量写入和更新
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple, Any
import logging
from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

from g_factor_advanced import AdvancedGFactorCalculator
from database.db_manager import PGSFactorDBManager

logger = logging.getLogger(__name__)


class GFactorBatchProcessorAdvanced:
    """
    高级G因子批量处理器
    
    集成了新的G因子计算方案，支持批量计算和数据库管理
    """
    
    def __init__(self, 
                 db_manager: PGSFactorDBManager,
                 batch_size: int = 100,
                 max_workers: int = 4):
        """
        初始化批量处理器
        
        Args:
            db_handler: 数据库处理器
            batch_size: 批次大小
            max_workers: 最大并发工作线程数
        """
        self.db_manager = db_manager
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # 初始化高级G因子计算器
        self.g_calculator = AdvancedGFactorCalculator()
        
        # 处理统计
        self.stats = {
            'total_stocks': 0,
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': None,
            'end_time': None
        }
        
        logger.info(f"GFactorBatchProcessorAdvanced initialized with batch_size={batch_size}, max_workers={max_workers}")
    
    def process_announcement_triggered(self,
                                     announcement_date: str,
                                     affected_stocks: List[str]) -> Dict[str, Any]:
        """
        基于财务公告触发的G因子计算
        
        Args:
            announcement_date: 公告日期 (YYYYMMDD)
            affected_stocks: 受影响的股票列表
            
        Returns:
            处理结果统计
        """
        logger.info(f"Processing G factors triggered by announcement on {announcement_date}")
        logger.info(f"Affected stocks: {len(affected_stocks)}")
        
        self.stats['start_time'] = datetime.now()
        self.stats['total_stocks'] = len(affected_stocks)
        
        try:
            # 1. 获取全市场股票列表（用于排名计算）
            all_stocks = self._get_all_active_stocks(announcement_date)
            logger.info(f"Total market stocks for ranking: {len(all_stocks)}")
            
            # 2. 加载必要的数据
            data_package = self._prepare_data_package(all_stocks, announcement_date)
            
            if not data_package:
                logger.error("Failed to prepare data package")
                return self._generate_result_summary()
            
            # 3. 批量计算G因子
            g_factor_results = self._batch_calculate_g_factors(
                all_stocks, 
                announcement_date,
                data_package
            )
            
            # 4. 筛选受影响股票的结果
            affected_results = g_factor_results[
                g_factor_results['ts_code'].isin(affected_stocks)
            ]
            
            # 5. 写入数据库
            self._batch_write_to_database(affected_results, announcement_date)
            
            # 6. 更新处理进度
            self._update_processing_progress(announcement_date, affected_stocks)
            
        except Exception as e:
            logger.error(f"Error in announcement-triggered processing: {e}")
            self.stats['failed'] = self.stats['total_stocks']
        
        self.stats['end_time'] = datetime.now()
        return self._generate_result_summary()
    
    def process_full_market(self, calc_date: str) -> Dict[str, Any]:
        """
        处理全市场G因子计算
        
        Args:
            calc_date: 计算日期 (YYYYMMDD)
            
        Returns:
            处理结果统计
        """
        logger.info(f"Processing full market G factors for {calc_date}")
        
        self.stats['start_time'] = datetime.now()
        
        try:
            # 1. 获取全市场股票列表
            all_stocks = self._get_all_active_stocks(calc_date)
            self.stats['total_stocks'] = len(all_stocks)
            logger.info(f"Total stocks to process: {len(all_stocks)}")
            
            # 2. 准备数据
            data_package = self._prepare_data_package(all_stocks, calc_date)
            
            if not data_package:
                logger.error("Failed to prepare data package")
                return self._generate_result_summary()
            
            # 3. 批量计算
            g_factor_results = self._batch_calculate_g_factors(
                all_stocks,
                calc_date,
                data_package
            )
            
        # 4. 批量写入数据库（若接口未实现则跳过并告警）
        try:
            self._batch_write_to_database(g_factor_results, calc_date)
        except AttributeError:
            logger.warning("save_g_subfactors/update_processing_progress 等接口未实现，跳过高级批处理写入阶段")
            
        except Exception as e:
            logger.error(f"Error in full market processing: {e}")
            self.stats['failed'] = self.stats['total_stocks']
        
        self.stats['end_time'] = datetime.now()
        return self._generate_result_summary()
    
    def _prepare_data_package(self, 
                             stocks: List[str], 
                             calc_date: str) -> Optional[Dict[str, Any]]:
        """
        准备计算所需的数据包
        
        Args:
            stocks: 股票列表
            calc_date: 计算日期
            
        Returns:
            包含所有必要数据的字典
        """
        logger.info("Preparing data package for G factor calculation")
        
        try:
            data_package = {}
            
            # 1. 加载当前P_score数据
            logger.info("Loading current P scores...")
            current_p_scores = self.db_manager.get_latest_p_factors(stocks, calc_date)
            data_package['p_scores_current'] = current_p_scores
            
            # 2. 加载历史P_score数据（最近20个季度）
            logger.info("Loading historical P scores...")
            historical_p_scores = self._load_historical_p_scores(stocks, calc_date)
            data_package['p_scores_historical'] = historical_p_scores
            
            # 3. 加载财务数据（营收、利润等）
            logger.info("Loading financial data...")
            financial_data = self._load_financial_data(stocks, calc_date)
            data_package['financial_data'] = financial_data
            
            # 数据验证
            if current_p_scores.empty:
                logger.warning("No current P scores available")
            
            if not historical_p_scores:
                logger.warning("No historical P scores available")
            
            if financial_data.empty:
                logger.warning("No financial data available")
            
            return data_package
            
        except Exception as e:
            logger.error(f"Error preparing data package: {e}")
            return None
    
    def _batch_calculate_g_factors(self,
                                  stocks: List[str],
                                  calc_date: str,
                                  data_package: Dict[str, Any]) -> pd.DataFrame:
        """
        批量计算G因子
        
        Args:
            stocks: 股票列表
            calc_date: 计算日期
            data_package: 数据包
            
        Returns:
            G因子计算结果DataFrame
        """
        logger.info(f"Batch calculating G factors for {len(stocks)} stocks")
        
        # 调用高级G因子计算器
        g_factor_results = self.g_calculator.calculate_g_factors(
            stocks,
            calc_date,
            data_package['financial_data'],
            data_package['p_scores_current'],
            data_package['p_scores_historical']
        )
        
        # 添加元数据
        g_factor_results['update_time'] = datetime.now()
        g_factor_results['calc_version'] = 'v2.0'  # 新版本标记
        
        # 统计结果
        valid_results = g_factor_results[g_factor_results['g_score'].notna()]
        self.stats['processed'] = len(valid_results)
        self.stats['skipped'] = len(stocks) - len(valid_results)
        
        logger.info(f"G factors calculated: {len(valid_results)} valid, {self.stats['skipped']} skipped")
        
        return g_factor_results
    
    def _batch_write_to_database(self, 
                                results: pd.DataFrame,
                                calc_date: str):
        """
        批量写入数据库
        
        Args:
            results: G因子计算结果
            calc_date: 计算日期
        """
        if results.empty:
            logger.warning("No results to write to database")
            return
        
        logger.info(f"Writing {len(results)} G factor records to database")
        
        # 分批写入
        for i in range(0, len(results), self.batch_size):
            batch = results.iloc[i:i+self.batch_size]
            
            try:
                # 准备写入数据
                records = self._prepare_database_records(batch, calc_date)
                
                # 写入主表
                self.db_manager.save_g_factors(pd.DataFrame(records), calc_date)
                
                # 写入子因子明细表（如果需要）
                subfactor_records = self._prepare_subfactor_records(batch, calc_date)
                if subfactor_records and hasattr(self.db_manager, 'save_g_subfactors'):
                    self.db_manager.save_g_subfactors(pd.DataFrame(subfactor_records), calc_date)
                
                logger.info(f"Batch {i//self.batch_size + 1} written successfully")
                
            except Exception as e:
                logger.error(f"Error writing batch {i//self.batch_size + 1}: {e}")
                self.stats['failed'] += len(batch)
    
    def _prepare_database_records(self, 
                                 results: pd.DataFrame,
                                 calc_date: str) -> List[Dict]:
        """
        准备数据库记录格式
        
        Args:
            results: 计算结果DataFrame
            calc_date: 计算日期
            
        Returns:
            数据库记录列表
        """
        records = []
        
        for _, row in results.iterrows():
            record = {
                'ts_code': row['ts_code'],
                'calc_date': calc_date,
                'g_score': row['g_score'] if not pd.isna(row['g_score']) else None,
                'data_quality': row.get('data_quality', 'unknown'),
                'valid_factors': int(row.get('valid_factors', 0)),
                'update_time': row.get('update_time', datetime.now()),
                'calc_version': row.get('calc_version', 'v2.0')
            }
            records.append(record)
        
        return records
    
    def _prepare_subfactor_records(self,
                                  results: pd.DataFrame,
                                  calc_date: str) -> List[Dict]:
        """
        准备子因子明细记录
        
        Args:
            results: 计算结果DataFrame  
            calc_date: 计算日期
            
        Returns:
            子因子记录列表
        """
        records = []
        
        subfactor_columns = [
            'g_efficiency_surprise', 'g_efficiency_momentum',
            'g_revenue_momentum', 'g_profit_momentum',
            'rank_efficiency_surprise', 'rank_efficiency_momentum',
            'rank_revenue_momentum', 'rank_profit_momentum'
        ]
        
        for _, row in results.iterrows():
            for col in subfactor_columns:
                if col in row and not pd.isna(row[col]):
                    record = {
                        'ts_code': row['ts_code'],
                        'calc_date': calc_date,
                        'factor_name': col,
                        'factor_value': float(row[col]),
                        'update_time': row.get('update_time', datetime.now())
                    }
                    records.append(record)
        
        return records
    
    def _update_processing_progress(self,
                                   announcement_date: str,
                                   affected_stocks: List[str]):
        """
        更新处理进度记录
        
        Args:
            announcement_date: 公告日期
            affected_stocks: 受影响的股票列表
        """
        progress_record = {
            'process_date': datetime.now().strftime('%Y%m%d'),
            'announcement_date': announcement_date,
            'affected_stocks': ','.join(affected_stocks),
            'total_processed': self.stats['processed'],
            'total_failed': self.stats['failed'],
            'process_time': (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        }
        
        try:
            if hasattr(self.db_manager, 'update_processing_progress'):
                self.db_manager.update_processing_progress('g_factor', progress_record)
                logger.info("Processing progress updated")
            else:
                logger.warning("DB manager lacks update_processing_progress; skipping progress update")
        except Exception as e:
            logger.error(f"Failed to update processing progress: {e}")
    
    def _load_historical_p_scores(self,
                                 stocks: List[str],
                                 calc_date: str,
                                 periods: int = 20) -> Dict[str, pd.DataFrame]:
        """
        加载历史P_score数据
        
        Args:
            stocks: 股票列表
            calc_date: 计算日期
            periods: 历史期数
            
        Returns:
            历史P_score数据字典
        """
        historical_data = {}
        
        # 生成历史日期列表（每季度）
        historical_dates = self._generate_historical_dates(calc_date, periods)
        
        for date in historical_dates:
            try:
                p_scores = self.db_manager.get_latest_p_factors(stocks, date)
                if not p_scores.empty:
                    historical_data[date] = p_scores
            except Exception as e:
                logger.debug(f"Failed to load P scores for {date}: {e}")
        
        logger.info(f"Loaded historical P scores for {len(historical_data)} periods")
        return historical_data
    
    def _load_financial_data(self,
                           stocks: List[str],
                           calc_date: str) -> pd.DataFrame:
        """
        加载财务数据
        
        Args:
            stocks: 股票列表
            calc_date: 计算日期
            
        Returns:
            财务数据DataFrame
        """
        try:
            # 获取最近2年的财务数据
            start_date = (pd.to_datetime(calc_date) - pd.DateOffset(years=2)).strftime('%Y%m%d')
            
            financial_data = self.db_manager.get_financial_data(
                stocks, 
                start_date,
                calc_date
            )
            
            logger.info(f"Loaded {len(financial_data)} financial records")
            return financial_data
            
        except Exception as e:
            logger.error(f"Error loading financial data: {e}")
            return pd.DataFrame()
    
    def _get_all_active_stocks(self, date: str) -> List[str]:
        """
        获取指定日期的所有活跃股票
        
        Args:
            date: 日期
            
        Returns:
            股票代码列表
        """
        try:
            stocks = self.db_manager.get_active_stocks(date)
            return stocks
        except Exception as e:
            logger.error(f"Error getting active stocks: {e}")
            return []
    
    def _generate_historical_dates(self, 
                                  base_date: str,
                                  periods: int) -> List[str]:
        """
        生成历史日期序列
        
        Args:
            base_date: 基准日期
            periods: 期数
            
        Returns:
            日期列表
        """
        dates = []
        current = pd.to_datetime(base_date)
        
        for i in range(periods):
            # 每季度递减
            quarter_date = current - pd.DateOffset(months=3*i)
            dates.append(quarter_date.strftime('%Y%m%d'))
        
        return dates
    
    def _generate_result_summary(self) -> Dict[str, Any]:
        """
        生成处理结果摘要
        
        Returns:
            结果统计字典
        """
        summary = {
            'total_stocks': self.stats['total_stocks'],
            'processed': self.stats['processed'],
            'failed': self.stats['failed'],
            'skipped': self.stats['skipped'],
            'success_rate': (self.stats['processed'] / self.stats['total_stocks'] * 100) 
                          if self.stats['total_stocks'] > 0 else 0,
            'start_time': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
            'end_time': self.stats['end_time'].isoformat() if self.stats['end_time'] else None,
            'duration_seconds': (self.stats['end_time'] - self.stats['start_time']).total_seconds()
                              if self.stats['end_time'] and self.stats['start_time'] else None
        }
        
        logger.info(f"Processing summary: {json.dumps(summary, indent=2)}")
        return summary
    
    def update_g_factor_weights(self, new_weights: Dict[str, float]):
        """
        更新G因子子因子权重
        
        Args:
            new_weights: 新的权重配置
        """
        self.g_calculator.update_weights(new_weights)
        logger.info(f"G factor weights updated: {new_weights}")


def test_batch_processor():
    """测试批量处理器"""
    from unittest.mock import Mock
    
    # 创建模拟数据库处理器
    mock_db = Mock(spec=PGSFactorDBManager)
    
    # 模拟数据返回
    mock_db.get_active_stocks.return_value = [f"{i:06d}.SZ" for i in range(1, 101)]
    mock_db.get_latest_p_factors.return_value = pd.DataFrame({
        'ts_code': [f"{i:06d}.SZ" for i in range(1, 101)],
        'p_score': np.random.randn(100) * 10 + 50
    })
    mock_db.get_financial_data.return_value = pd.DataFrame({
        'ts_code': np.repeat([f"{i:06d}.SZ" for i in range(1, 101)], 4),
        'ann_date': np.tile(['20250801', '20250501', '20250201', '20241101'], 100),
        'end_date': np.tile(['20250630', '20250331', '20241231', '20240930'], 100),
        'revenue': np.random.randn(400) * 1000000 + 10000000,
        'n_income_attr_p': np.random.randn(400) * 100000 + 1000000
    })
    
    # 创建处理器
    processor = GFactorBatchProcessorAdvanced(mock_db)
    
    # 测试公告触发处理
    print("\n测试公告触发的G因子计算:")
    print("="*50)
    affected_stocks = [f"{i:06d}.SZ" for i in range(1, 11)]
    result = processor.process_announcement_triggered('20250801', affected_stocks)
    
    print("\n处理结果:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    
    # 测试全市场处理
    print("\n测试全市场G因子计算:")
    print("="*50)
    result = processor.process_full_market('20250801')
    
    print("\n处理结果:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    
    return processor


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 运行测试
    test_batch_processor()
