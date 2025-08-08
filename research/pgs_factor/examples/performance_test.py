#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT数据管理器性能测试
"""

import time
import logging
from research.tools.context import ResearchContext
from research.pgs_factor.examples.pit_data_manager import PITDataManager


def performance_test():
    """性能测试"""
    
    logger = logging.getLogger('PerformanceTest')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    # 测试股票列表（选择一些有代表性的股票）
    test_stocks = [
        '601020.SH',  # 华钰矿业（我们之前测试过的）
        '000001.SZ',  # 平安银行
        '000002.SZ',  # 万科A
        '600000.SH',  # 浦发银行
        '600036.SH',  # 招商银行
    ]
    
    with ResearchContext() as ctx:
        # 清空表
        logger.info("清空测试表...")
        try:
            ctx.db_manager.execute_sync('TRUNCATE TABLE pgs_factors.pit_income_quarterly;')
            ctx.db_manager.execute_sync('TRUNCATE TABLE pgs_factors.pit_balance_quarterly;')
        except:
            logger.info("表不存在，将自动创建")
        
        # 测试不同批处理大小的性能
        batch_sizes = [100, 500, 1000]
        
        for batch_size in batch_sizes:
            logger.info(f"\n=== 测试批处理大小: {batch_size} ===")
            
            # 清空表
            try:
                ctx.db_manager.execute_sync('TRUNCATE TABLE pgs_factors.pit_income_quarterly;')
                ctx.db_manager.execute_sync('TRUNCATE TABLE pgs_factors.pit_balance_quarterly;')
            except:
                pass
            
            # 创建管理器
            manager = PITDataManager(ctx, batch_size)
            
            # 开始计时
            start_time = time.time()
            
            # 执行测试
            try:
                total_processed = 0
                
                # 处理report数据
                report_start = time.time()
                report_rows = manager.process_report_data(test_stocks)
                report_time = time.time() - report_start
                total_processed += report_rows
                logger.info(f"Report数据处理: {report_rows}行, 耗时: {report_time:.2f}秒")
                
                # 处理balance数据
                balance_start = time.time()
                balance_rows = manager.process_balance_data(test_stocks)
                balance_time = time.time() - balance_start
                total_processed += balance_rows
                logger.info(f"Balance数据处理: {balance_rows}行, 耗时: {balance_time:.2f}秒")
                
                # 处理express数据
                express_start = time.time()
                express_rows = manager.process_express_data(test_stocks)
                express_time = time.time() - express_start
                total_processed += express_rows
                logger.info(f"Express数据处理: {express_rows}行, 耗时: {express_time:.2f}秒")
                
                # 处理forecast数据
                forecast_start = time.time()
                forecast_rows = manager.process_forecast_data(test_stocks)
                forecast_time = time.time() - forecast_start
                total_processed += forecast_rows
                logger.info(f"Forecast数据处理: {forecast_rows}行, 耗时: {forecast_time:.2f}秒")
                
                # 总计时间
                total_time = time.time() - start_time
                
                logger.info(f"批处理大小 {batch_size} 总结:")
                logger.info(f"  总处理行数: {total_processed}")
                logger.info(f"  总耗时: {total_time:.2f}秒")
                logger.info(f"  平均处理速度: {total_processed/total_time:.1f}行/秒")
                logger.info(f"  每行平均耗时: {total_time/total_processed*1000:.2f}毫秒")
                
            except Exception as e:
                logger.error(f"批处理大小 {batch_size} 测试失败: {e}")
        
        # 验证数据质量
        logger.info("\n=== 验证数据质量 ===")
        verify_query = """
        SELECT conversion_status, COUNT(*) as count
        FROM pgs_factors.pit_income_quarterly
        GROUP BY conversion_status
        ORDER BY conversion_status
        """
        
        result = ctx.query_dataframe(verify_query)
        if result is not None and not result.empty:
            logger.info("Conversion_status分布:")
            for _, row in result.iterrows():
                logger.info(f"  {row['conversion_status']}: {row['count']}条")


if __name__ == '__main__':
    performance_test()
