#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查数据库中conversion_status的实际值
"""

from research.tools.context import ResearchContext


def check_db_values():
    """检查数据库中的实际值"""
    
    with ResearchContext() as ctx:
        # 检查2011-2014年的数据
        query = """
        SELECT ts_code, year, quarter, conversion_status 
        FROM pgs_factors.pit_income_quarterly 
        WHERE ts_code = '601020.SH' 
          AND year IN (2011, 2012, 2013, 2014) 
          AND data_source = 'report'
        ORDER BY year, quarter
        """
        
        result = ctx.query_dataframe(query)
        
        if result is not None and not result.empty:
            print("数据库中2011-2014年的conversion_status值:")
            for _, row in result.iterrows():
                print(f"  {row['year']}年Q{row['quarter']}: {row['conversion_status']}")
        else:
            print("未找到数据")


if __name__ == '__main__':
    check_db_values()
