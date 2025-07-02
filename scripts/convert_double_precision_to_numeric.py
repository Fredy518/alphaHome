#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
将数据库中的double precision字段批量转换为NUMERIC类型

此脚本会：
1. 连接到PostgreSQL数据库
2. 查找tushare schema中所有的double precision字段
3. 根据字段名和用途将其转换为对应精度的NUMERIC类型
4. 提供详细的转换日志和进度反馈
"""

import sys
import os
import logging
from typing import Dict, List, Tuple
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# 添加项目根目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from alphahome.common.config_manager import ConfigManager
from alphahome.common.logging_utils import get_logger

# 设置日志
logger = get_logger('convert_precision')

class FieldTypeConverter:
    """数据库字段类型转换器"""
    
    def __init__(self):
        """初始化转换器"""
        self.config = ConfigManager()
        self.conn = None
        self.cursor = None
        
        # 字段类型映射规则
        self.field_type_mapping = {
            # 价格类字段 - NUMERIC(15,4)
            'open': 'NUMERIC(15,4)',
            'high': 'NUMERIC(15,4)', 
            'low': 'NUMERIC(15,4)',
            'close': 'NUMERIC(15,4)',
            'pre_close': 'NUMERIC(15,4)',
            'change': 'NUMERIC(15,4)',
            'settle': 'NUMERIC(15,4)',
            'pre_settle': 'NUMERIC(15,4)',
            'exercise_price': 'NUMERIC(15,4)',
            'list_price': 'NUMERIC(15,4)',
            'his_low': 'NUMERIC(15,4)',
            'his_high': 'NUMERIC(15,4)',
            'cost_5pct': 'NUMERIC(15,4)',
            'cost_15pct': 'NUMERIC(15,4)',
            'cost_50pct': 'NUMERIC(15,4)',
            'cost_85pct': 'NUMERIC(15,4)',
            'cost_95pct': 'NUMERIC(15,4)',
            'weight_avg': 'NUMERIC(15,4)',
            'p_value': 'NUMERIC(15,4)',
            
            # 成交量字段 - NUMERIC(20,2)
            'volume': 'NUMERIC(20,2)',
            'fd_share': 'NUMERIC(20,2)',
            'oi': 'NUMERIC(20,2)',
            'total_share': 'NUMERIC(20,2)',
            'float_share': 'NUMERIC(20,2)',
            'free_share': 'NUMERIC(20,2)',
            
            # 成交额/市值字段 - NUMERIC(20,3)
            # amount字段会在get_target_type方法中根据表名特殊处理
            'mkv': 'NUMERIC(20,3)',
            'net_asset': 'NUMERIC(20,3)',
            'total_netasset': 'NUMERIC(20,3)',
            'issue_amount': 'NUMERIC(20,3)',
            'total_mv': 'NUMERIC(20,3)',
            'float_mv': 'NUMERIC(20,3)',
            'circ_mv': 'NUMERIC(20,3)',
            
            # 比率类字段 - NUMERIC(10,4)
            'pct_chg': 'NUMERIC(10,4)',
            'pct_change': 'NUMERIC(10,4)',
            'pe': 'NUMERIC(10,4)',
            'pb': 'NUMERIC(10,4)',
            'winner_rate': 'NUMERIC(10,4)',
            'est_peg': 'NUMERIC(10,4)',
            'turnover_rate': 'NUMERIC(10,4)',
            'turnover_rate_f': 'NUMERIC(10,4)',
            'volume_ratio': 'NUMERIC(10,4)',
            'pe_ttm': 'NUMERIC(10,4)',
            'ps': 'NUMERIC(10,4)',
            'ps_ttm': 'NUMERIC(10,4)',
            'dv_ratio': 'NUMERIC(10,4)',
            'dv_ttm': 'NUMERIC(10,4)',
            'duration_year': 'NUMERIC(8,4)',
            'base_point': 'NUMERIC(10,4)',
            
            # 复权因子 - NUMERIC(12,8)
            'adj_factor': 'NUMERIC(12,8)',
            
            # 净值类字段 - NUMERIC(15,6)
            'unit_nav': 'NUMERIC(15,6)',
            'accum_nav': 'NUMERIC(15,6)',
            'accum_div': 'NUMERIC(15,6)',
            'adj_nav': 'NUMERIC(15,6)',
            
            # 费率/权重字段 - NUMERIC(8,6)
            'm_fee': 'NUMERIC(8,6)',
            'c_fee': 'NUMERIC(8,6)',
            'stk_mkv_ratio': 'NUMERIC(10,4)',
            'stk_float_ratio': 'NUMERIC(10,4)',
            'weight': 'NUMERIC(8,6)',
            'exp_return': 'NUMERIC(10,6)',
            
            # 合约参数 - NUMERIC(10,4)
            'per_unit': 'NUMERIC(10,4)',
            'multiplier': 'NUMERIC(10,2)',
            'min_amount': 'NUMERIC(15,2)',
        }
        
        # 默认类型（当字段名不在映射中时使用）
        self.default_numeric_type = 'NUMERIC(18,6)'
    
    def connect(self):
        """连接数据库"""
        try:
            config = self.config.load_config()
            db_config = config.get('database', {})
            
            # 如果有URL，直接使用URL连接
            db_url = db_config.get('url')
            if db_url:
                self.conn = psycopg2.connect(db_url)
            else:
                # 否则使用单独的参数
                self.conn = psycopg2.connect(
                    host=db_config.get('host', 'localhost'),
                    port=db_config.get('port', 5432),
                    database=db_config.get('database', 'alphahome'),
                    user=db_config.get('user', 'postgres'),
                    password=db_config.get('password', '')
                )
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.conn.cursor()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def disconnect(self):
        """断开数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("数据库连接已关闭")
    
    def get_double_precision_fields(self) -> List[Tuple[str, str]]:
        """获取基础表中的double precision字段"""
        query = """
        SELECT t.table_name, c.column_name
        FROM information_schema.tables t
        JOIN information_schema.columns c ON t.table_name = c.table_name AND t.table_schema = c.table_schema
        WHERE t.table_schema = 'tushare'
            AND t.table_type = 'BASE TABLE'
            AND c.data_type = 'double precision'
        ORDER BY t.table_name, c.column_name
        """
        
        self.cursor.execute(query)
        fields = self.cursor.fetchall()
        logger.info(f"找到 {len(fields)} 个double precision字段")
        return fields
    
    def get_target_type(self, table_name: str, column_name: str) -> str:
        """根据字段名获取目标NUMERIC类型"""
        # 特殊处理amount字段（根据表名区分用途）
        if column_name == 'amount':
            if 'portfolio' in table_name:
                return 'NUMERIC(20,2)'  # 持仓数量
            else:
                return 'NUMERIC(20,3)'  # 成交额
        
        # 从映射表中查找
        target_type = self.field_type_mapping.get(column_name, self.default_numeric_type)
        return target_type
    
    def convert_field(self, table_name: str, column_name: str) -> bool:
        """转换单个字段类型"""
        target_type = self.get_target_type(table_name, column_name)
        
        try:
            # 构建ALTER TABLE语句
            alter_sql = f"""
            ALTER TABLE tushare.{table_name} 
            ALTER COLUMN {column_name} TYPE {target_type}
            USING {column_name}::{target_type}
            """
            
            logger.info(f"正在转换 {table_name}.{column_name} -> {target_type}")
            self.cursor.execute(alter_sql)
            logger.info(f"✅ 成功转换 {table_name}.{column_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 转换失败 {table_name}.{column_name}: {e}")
            return False
    
    def convert_all_fields(self):
        """批量转换所有字段"""
        fields = self.get_double_precision_fields()
        if not fields:
            logger.info("没有找到需要转换的double precision字段")
            return
        
        total_fields = len(fields)
        success_count = 0
        failed_count = 0
        
        logger.info(f"开始转换 {total_fields} 个字段...")
        
        for i, (table_name, column_name) in enumerate(fields, 1):
            logger.info(f"进度: {i}/{total_fields}")
            
            if self.convert_field(table_name, column_name):
                success_count += 1
            else:
                failed_count += 1
        
        logger.info(f"转换完成！成功: {success_count}, 失败: {failed_count}")
    
    def verify_conversion(self):
        """验证转换结果"""
        logger.info("验证转换结果...")
        
        # 检查是否还有double precision字段
        remaining_fields = self.get_double_precision_fields()
        
        if not remaining_fields:
            logger.info("✅ 验证成功：所有double precision字段已转换完成")
        else:
            logger.warning(f"⚠️ 还有 {len(remaining_fields)} 个字段未转换:")
            for table_name, column_name in remaining_fields:
                logger.warning(f"  - {table_name}.{column_name}")
    
    def run(self):
        """执行转换流程"""
        try:
            self.connect()
            self.convert_all_fields()
            self.verify_conversion()
        except Exception as e:
            logger.error(f"转换过程中发生错误: {e}")
            raise
        finally:
            self.disconnect()

def main():
    """主函数"""
    logger.info("开始数据库字段类型转换...")
    
    converter = FieldTypeConverter()
    converter.run()
    
    logger.info("字段类型转换完成！")

if __name__ == "__main__":
    main() 