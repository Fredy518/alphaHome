#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试conversion_status字段是否正确设置
"""

import logging
from research.tools.context import ResearchContext


def test_conversion_status():
    """测试conversion_status字段"""
    
    logger = logging.getLogger('ConversionStatusTest')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    with ResearchContext() as ctx:
        # 查询601020.SH的conversion_status分布
        query = """
        SELECT ts_code, year, quarter, conversion_status, data_source, COUNT(*) as count
        FROM pgs_factors.pit_income_quarterly 
        WHERE ts_code = '601020.SH'
        GROUP BY ts_code, year, quarter, conversion_status, data_source
        ORDER BY year, quarter
        """
        
        result = ctx.query_dataframe(query)
        
        if result is not None and not result.empty:
            logger.info("601020.SH的conversion_status分布:")
            for _, row in result.iterrows():
                logger.info(f"  {row['year']}年Q{row['quarter']} {row['data_source']}: {row['conversion_status']} ({row['count']}条)")
        else:
            logger.info("未找到601020.SH的数据")
        
        # 查询所有conversion_status的分布
        summary_query = """
        SELECT conversion_status, COUNT(*) as count
        FROM pgs_factors.pit_income_quarterly
        GROUP BY conversion_status
        ORDER BY conversion_status
        """
        
        summary_result = ctx.query_dataframe(summary_query)
        
        if summary_result is not None and not summary_result.empty:
            logger.info("全部数据的conversion_status分布:")
            for _, row in summary_result.iterrows():
                logger.info(f"  {row['conversion_status']}: {row['count']}条")


if __name__ == '__main__':
    test_conversion_status()
