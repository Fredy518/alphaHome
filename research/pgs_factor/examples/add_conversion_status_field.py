#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT数据表字段迁移脚本
====================

为现有的PIT数据表添加conversion_status字段，并根据现有数据推断转换状态。

使用方法：
    python -m research.pgs_factor.examples.add_conversion_status_field
"""

import logging
from research.tools.context import ResearchContext


def add_conversion_status_field():
    """为PIT表添加conversion_status字段"""
    
    logger = logging.getLogger('ConversionStatusMigration')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    with ResearchContext() as ctx:
        try:
            # 1. 检查字段是否已存在
            logger.info("检查conversion_status字段是否已存在...")
            
            check_income_field = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'pgs_factors' 
              AND table_name = 'pit_income_quarterly' 
              AND column_name = 'conversion_status'
            """
            
            check_balance_field = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'pgs_factors' 
              AND table_name = 'pit_balance_quarterly' 
              AND column_name = 'conversion_status'
            """
            
            income_field_exists = ctx.query_dataframe(check_income_field)
            balance_field_exists = ctx.query_dataframe(check_balance_field)
            
            # 2. 为pit_income_quarterly表添加字段
            if income_field_exists is None or income_field_exists.empty:
                logger.info("为pit_income_quarterly表添加conversion_status字段...")
                add_income_field_sql = """
                ALTER TABLE pgs_factors.pit_income_quarterly 
                ADD COLUMN conversion_status VARCHAR(20) DEFAULT 'SINGLE'
                """
                ctx.db_manager.execute_sync(add_income_field_sql)
                logger.info("pit_income_quarterly表字段添加完成")
            else:
                logger.info("pit_income_quarterly表的conversion_status字段已存在")
            
            # 3. 为pit_balance_quarterly表添加字段
            if balance_field_exists is None or balance_field_exists.empty:
                logger.info("为pit_balance_quarterly表添加conversion_status字段...")
                add_balance_field_sql = """
                ALTER TABLE pgs_factors.pit_balance_quarterly 
                ADD COLUMN conversion_status VARCHAR(20) DEFAULT 'SINGLE'
                """
                ctx.db_manager.execute_sync(add_balance_field_sql)
                logger.info("pit_balance_quarterly表字段添加完成")
            else:
                logger.info("pit_balance_quarterly表的conversion_status字段已存在")
            
            # 4. 更新现有数据的conversion_status
            logger.info("开始更新现有数据的conversion_status...")
            
            # 更新forecast数据
            update_forecast_sql = """
            UPDATE pgs_factors.pit_income_quarterly 
            SET conversion_status = 'FORECAST'
            WHERE data_source LIKE 'forecast%'
            """
            ctx.db_manager.execute_sync(update_forecast_sql)
            
            # 更新资产负债表数据（时点数据，保持SINGLE）
            update_balance_sql = """
            UPDATE pgs_factors.pit_balance_quarterly 
            SET conversion_status = 'SINGLE'
            WHERE conversion_status IS NULL OR conversion_status = ''
            """
            ctx.db_manager.execute_sync(update_balance_sql)
            
            # 对于report和express数据，由于无法准确推断历史的转换状态，
            # 暂时保持默认的'SINGLE'状态，建议重新运行全量重建以获得准确状态
            logger.info("现有数据conversion_status更新完成")
            logger.info("建议：为获得最准确的conversion_status，请重新运行全量重建")
            
            # 5. 验证结果
            logger.info("验证迁移结果...")
            
            verify_income_sql = """
            SELECT conversion_status, COUNT(*) as count
            FROM pgs_factors.pit_income_quarterly
            GROUP BY conversion_status
            ORDER BY conversion_status
            """
            
            verify_balance_sql = """
            SELECT conversion_status, COUNT(*) as count
            FROM pgs_factors.pit_balance_quarterly
            GROUP BY conversion_status
            ORDER BY conversion_status
            """
            
            income_stats = ctx.query_dataframe(verify_income_sql)
            balance_stats = ctx.query_dataframe(verify_balance_sql)
            
            if income_stats is not None and not income_stats.empty:
                logger.info("pit_income_quarterly表conversion_status分布:")
                for _, row in income_stats.iterrows():
                    logger.info(f"  {row['conversion_status']}: {row['count']}条")
            
            if balance_stats is not None and not balance_stats.empty:
                logger.info("pit_balance_quarterly表conversion_status分布:")
                for _, row in balance_stats.iterrows():
                    logger.info(f"  {row['conversion_status']}: {row['count']}条")
            
            logger.info("字段迁移完成！")
            
        except Exception as e:
            logger.error(f"迁移失败: {e}")
            raise


if __name__ == '__main__':
    add_conversion_status_field()
