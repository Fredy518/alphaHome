#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
更新现有数据的conversion_status字段
根据现有数据的季报模式推断并更新conversion_status
"""

import logging
from research.tools.context import ResearchContext


def update_conversion_status():
    """更新现有数据的conversion_status字段"""
    
    logger = logging.getLogger('UpdateConversionStatus')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    with ResearchContext() as ctx:
        try:
            logger.info("开始更新conversion_status字段...")
            
            # 1. 获取所有股票-年份的季报模式
            query = """
            SELECT ts_code, year, 
                   ARRAY_AGG(quarter ORDER BY quarter) as quarters,
                   COUNT(*) as quarter_count
            FROM pgs_factors.pit_income_quarterly
            WHERE data_source = 'report'
            GROUP BY ts_code, year
            ORDER BY ts_code, year
            """
            
            patterns = ctx.query_dataframe(query)
            
            if patterns is None or patterns.empty:
                logger.warning("未找到report数据")
                return
            
            logger.info(f"找到 {len(patterns)} 个股票-年份组合")
            
            # 2. 根据季报模式更新conversion_status
            update_count = 0
            
            for _, row in patterns.iterrows():
                ts_code = row['ts_code']
                year = row['year']
                quarters = row['quarters']
                
                # 根据季报模式确定conversion_status
                if quarters == [1]:  # 仅一季报
                    status_updates = [(1, 'SINGLE')]
                elif quarters == [4]:  # 仅年报
                    status_updates = [(4, 'ANNUAL')]
                elif len(quarters) == 1 and quarters[0] in [2, 3]:  # 单个季报（Q2或Q3）
                    status_updates = [(quarters[0], 'CUMULATIVE')]
                elif quarters == [1, 2]:  # Q1+中报
                    status_updates = [(1, 'SINGLE'), (2, 'SINGLE')]
                elif quarters == [1, 4]:  # Q1+年报
                    status_updates = [(1, 'SINGLE'), (4, 'CALCULATED')]
                elif quarters == [2, 4]:  # 中报+年报
                    status_updates = [(2, 'CUMULATIVE'), (4, 'CALCULATED')]
                elif quarters == [3, 4]:  # Q3+年报
                    status_updates = [(3, 'CUMULATIVE'), (4, 'SINGLE')]
                elif len(quarters) >= 3:  # 完整季报或接近完整
                    status_updates = [(q, 'SINGLE') for q in quarters]
                else:  # 其他不规则情况
                    status_updates = [(q, 'CUMULATIVE') for q in quarters]
                    logger.warning(f"{ts_code} {year}年季报模式异常: {quarters}")
                
                # 执行更新
                for quarter, status in status_updates:
                    update_sql = """
                    UPDATE pgs_factors.pit_income_quarterly 
                    SET conversion_status = %s
                    WHERE ts_code = %s AND year = %s AND quarter = %s AND data_source = 'report'
                    """
                    
                    ctx.db_manager.execute_sync(update_sql, (status, ts_code, year, quarter))
                    update_count += 1
                
                if update_count % 100 == 0:
                    logger.info(f"已更新 {update_count} 条记录...")
            
            # 3. 更新forecast数据
            forecast_update_sql = """
            UPDATE pgs_factors.pit_income_quarterly 
            SET conversion_status = 'FORECAST'
            WHERE data_source LIKE 'forecast%'
            """
            ctx.db_manager.execute_sync(forecast_update_sql)
            
            # 4. 更新express数据（保持SINGLE，因为express数据经过了单季化处理）
            express_update_sql = """
            UPDATE pgs_factors.pit_income_quarterly 
            SET conversion_status = 'SINGLE'
            WHERE data_source = 'express'
            """
            ctx.db_manager.execute_sync(express_update_sql)
            
            logger.info(f"conversion_status更新完成，共更新 {update_count} 条report记录")
            
            # 5. 验证结果
            verify_query = """
            SELECT conversion_status, COUNT(*) as count
            FROM pgs_factors.pit_income_quarterly
            GROUP BY conversion_status
            ORDER BY conversion_status
            """
            
            verify_result = ctx.query_dataframe(verify_query)
            
            if verify_result is not None and not verify_result.empty:
                logger.info("更新后的conversion_status分布:")
                for _, row in verify_result.iterrows():
                    logger.info(f"  {row['conversion_status']}: {row['count']}条")
            
        except Exception as e:
            logger.error(f"更新失败: {e}")
            raise


if __name__ == '__main__':
    update_conversion_status()
