#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查表结构
"""

from research.tools.context import ResearchContext


def check_table_structure():
    """检查表结构"""
    
    with ResearchContext() as ctx:
        # 检查conversion_status字段的定义
        query = """
        SELECT column_name, column_default, is_nullable, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'pgs_factors' 
          AND table_name = 'pit_income_quarterly' 
          AND column_name = 'conversion_status'
        """
        
        result = ctx.query_dataframe(query)
        
        if result is not None and not result.empty:
            print("conversion_status字段定义:")
            for _, row in result.iterrows():
                print(f"  字段名: {row['column_name']}")
                print(f"  默认值: {row['column_default']}")
                print(f"  可为空: {row['is_nullable']}")
                print(f"  数据类型: {row['data_type']}")
        else:
            print("conversion_status字段不存在")
        
        # 检查实际数据的分布
        data_query = """
        SELECT conversion_status, COUNT(*) as count
        FROM pgs_factors.pit_income_quarterly
        WHERE ts_code = '601020.SH' AND data_source = 'report'
        GROUP BY conversion_status
        ORDER BY conversion_status
        """
        
        data_result = ctx.query_dataframe(data_query)
        
        if data_result is not None and not data_result.empty:
            print("\n实际数据分布:")
            for _, row in data_result.iterrows():
                print(f"  {row['conversion_status']}: {row['count']}条")
        else:
            print("\n未找到数据")


if __name__ == '__main__':
    check_table_structure()
