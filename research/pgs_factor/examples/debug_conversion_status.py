#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调试conversion_status字段设置问题
"""

import pandas as pd
from research.tools.context import ResearchContext
from research.pgs_factor.examples.pit_data_manager import PITDataManager


def debug_conversion_status():
    """调试conversion_status字段设置"""
    
    # 创建一个简单的测试数据
    test_data = pd.DataFrame({
        'ts_code': ['601020.SH', '601020.SH', '601020.SH', '601020.SH'],
        'end_date': ['2011-12-31', '2012-12-31', '2014-06-30', '2014-12-31'],
        'ann_date': ['2012-04-20', '2013-04-20', '2014-08-20', '2015-04-20'],
        'year': [2011, 2012, 2014, 2014],
        'quarter': [4, 4, 2, 4],
        'n_income': [1000, 2000, 1500, 3000],
        'revenue': [5000, 6000, 3000, 7000]
    })
    
    print("原始测试数据:")
    print(test_data)
    print()
    
    with ResearchContext() as ctx:
        manager = PITDataManager(ctx, 100)
        
        # 调用_convert_to_single_quarter方法
        result = manager._convert_to_single_quarter(test_data, 'n_income')
        
        print("单季化处理后的数据:")
        print(result[['ts_code', 'year', 'quarter', 'conversion_status', 'n_income_single']])
        print()
        
        # 检查每年的季报模式
        for year in result['year'].unique():
            year_data = result[result['year'] == year]
            quarters = sorted(year_data['quarter'].tolist())
            conversion_status = year_data['conversion_status'].iloc[0]
            print(f"{year}年: quarters={quarters}, conversion_status={conversion_status}")


if __name__ == '__main__':
    debug_conversion_status()
