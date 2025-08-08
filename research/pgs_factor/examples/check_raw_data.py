#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查原始数据的季报模式
"""

from research.tools.context import ResearchContext


def check_raw_data():
    """检查原始数据"""
    
    with ResearchContext() as ctx:
        # 检查601020.SH的原始财务数据
        query = """
        SELECT ts_code, end_date, ann_date,
               EXTRACT(YEAR FROM end_date)::int as year,
               EXTRACT(QUARTER FROM end_date)::int as quarter
        FROM tushare.fina_income
        WHERE ts_code = '601020.SH'
          AND EXTRACT(YEAR FROM end_date) BETWEEN 2011 AND 2015
        ORDER BY end_date
        """
        
        result = ctx.query_dataframe(query)
        
        if result is not None and not result.empty:
            print("601020.SH原始财务数据:")
            for _, row in result.iterrows():
                print(f"  {row['year']}年Q{row['quarter']}: {row['end_date']} (公告日: {row['ann_date']})")
            
            # 按年份统计季报模式
            print("\n按年份统计季报模式:")
            for year in sorted(result['year'].unique()):
                year_data = result[result['year'] == year]
                quarters = sorted(year_data['quarter'].tolist())
                print(f"  {year}年: {quarters}")
        else:
            print("未找到数据")


if __name__ == '__main__':
    check_raw_data()
