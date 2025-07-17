#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AlphaDataTool 使用示例

演示如何使用AlphaDataTool进行各种数据查询操作，
展示80/20原则的实际应用：80%的需求通过5个核心方法满足，
20%的特殊需求通过灵活接口处理。
"""

import sys
from pathlib import Path
import pandas as pd

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from research.tools.context import ResearchContext


def demo_core_methods():
    """演示5个核心方法的使用（覆盖80%需求）"""
    print("=" * 60)
    print("演示核心方法（80%需求覆盖）")
    print("=" * 60)
    
    with ResearchContext() as context:
        data_tool = context.data_tool
        
        # 1. 获取股票行情数据
        print("\n1. 获取股票行情数据")
        print("-" * 30)
        try:
            stock_data = data_tool.get_stock_data(
                symbols=['000001.SZ', '000002.SZ'], 
                start_date='2024-01-01', 
                end_date='2024-01-31'
            )
            print(f"获取到 {len(stock_data)} 条股票行情数据")
            if not stock_data.empty:
                print("数据预览:")
                print(stock_data.head())
                print(f"数据列: {list(stock_data.columns)}")
        except Exception as e:
            print(f"获取股票数据失败: {e}")
        
        # 2. 获取指数权重数据
        print("\n2. 获取指数权重数据")
        print("-" * 30)
        try:
            index_weights = data_tool.get_index_weights(
                index_code='000300.SH',
                start_date='2024-01-01',
                end_date='2024-01-31',
                monthly=True  # 只获取月末数据
            )
            print(f"获取到 {len(index_weights)} 条指数权重数据")
            if not index_weights.empty:
                print("数据预览:")
                print(index_weights.head())
        except Exception as e:
            print(f"获取指数权重数据失败: {e}")
        
        # 3. 获取股票基本信息
        print("\n3. 获取股票基本信息")
        print("-" * 30)
        try:
            stock_info = data_tool.get_stock_info(
                symbols=['000001.SZ', '000002.SZ'],
                list_status='L'  # 上市状态
            )
            print(f"获取到 {len(stock_info)} 条股票基本信息")
            if not stock_info.empty:
                print("数据预览:")
                print(stock_info[['ts_code', 'name', 'industry', 'area']].head())
        except Exception as e:
            print(f"获取股票基本信息失败: {e}")
        
        # 4. 获取交易日历
        print("\n4. 获取交易日历")
        print("-" * 30)
        try:
            trade_dates = data_tool.get_trade_dates(
                start_date='2024-01-01',
                end_date='2024-01-31',
                exchange='SSE'
            )
            print(f"获取到 {len(trade_dates)} 条交易日历数据")
            if not trade_dates.empty:
                trading_days = trade_dates[trade_dates['is_open'] == 1]
                print(f"其中交易日 {len(trading_days)} 天")
                print("交易日预览:")
                print(trading_days.head())
        except Exception as e:
            print(f"获取交易日历失败: {e}")
        
        # 5. 获取行业分类数据
        print("\n5. 获取行业分类数据")
        print("-" * 30)
        try:
            industry_data = data_tool.get_industry_data(
                symbols=['000001.SZ', '000002.SZ'],
                level='sw_l1'  # 申万一级行业
            )
            print(f"获取到 {len(industry_data)} 条行业分类数据")
            if not industry_data.empty:
                print("数据预览:")
                print(industry_data.head())
        except Exception as e:
            print(f"获取行业分类数据失败: {e}")


def demo_flexible_interfaces():
    """演示灵活接口的使用（处理20%特殊需求）"""
    print("\n" + "=" * 60)
    print("演示灵活接口（20%特殊需求处理）")
    print("=" * 60)
    
    with ResearchContext() as context:
        data_tool = context.data_tool
        
        # 1. 自定义查询 - 复杂联表查询
        print("\n1. 自定义查询 - 复杂联表查询")
        print("-" * 40)
        try:
            # 查询沪深300成分股的行情和权重信息
            complex_query = """
            SELECT 
                s.ts_code,
                s.trade_date,
                s.close,
                s.pct_chg,
                w.weight,
                i.industry_name
            FROM stock_daily s
            LEFT JOIN index_weight w ON s.ts_code = w.con_code 
                AND s.trade_date = w.trade_date
            LEFT JOIN stock_industry i ON s.ts_code = i.ts_code 
                AND i.level = 'sw_l1'
            WHERE w.index_code = '000300.SH'
                AND s.trade_date = '2024-01-31'
                AND w.weight > 1.0
            ORDER BY w.weight DESC
            LIMIT 10
            """
            
            result = data_tool.custom_query(complex_query)
            print(f"复杂查询获取到 {len(result)} 条数据")
            if not result.empty:
                print("查询结果预览:")
                print(result.head())
        except Exception as e:
            print(f"复杂查询失败: {e}")
        
        # 2. 参数化查询
        print("\n2. 参数化查询")
        print("-" * 20)
        try:
            # 使用参数化查询避免SQL注入
            param_query = """
            SELECT 
                ts_code,
                trade_date,
                close,
                vol,
                pct_chg
            FROM stock_daily
            WHERE ts_code = %(stock_code)s
                AND trade_date >= %(start_date)s
                AND trade_date <= %(end_date)s
            ORDER BY trade_date DESC
            LIMIT 5
            """
            
            params = {
                'stock_code': '000001.SZ',
                'start_date': '2024-01-01',
                'end_date': '2024-01-31'
            }
            
            result = data_tool.custom_query(param_query, params)
            print(f"参数化查询获取到 {len(result)} 条数据")
            if not result.empty:
                print("查询结果:")
                print(result)
        except Exception as e:
            print(f"参数化查询失败: {e}")
        
        # 3. 获取原始数据库管理器进行高级操作
        print("\n3. 使用原始数据库管理器")
        print("-" * 30)
        try:
            db_manager = data_tool.get_raw_db_manager()
            print(f"获取到数据库管理器: {type(db_manager).__name__}")
            print(f"数据库模式: {getattr(db_manager, 'mode', 'unknown')}")
            
            # 可以使用所有底层功能
            # 例如：批量数据导入、事务处理、连接池管理等
            print("可以使用所有底层数据库功能，如:")
            print("- 批量数据导入 (copy_from_dataframe)")
            print("- 事务处理")
            print("- 连接池管理")
            print("- 自定义SQL执行")
            
        except Exception as e:
            print(f"获取数据库管理器失败: {e}")


def demo_practical_scenarios():
    """演示实际研究场景的应用"""
    print("\n" + "=" * 60)
    print("演示实际研究场景应用")
    print("=" * 60)
    
    with ResearchContext() as context:
        data_tool = context.data_tool
        
        # 场景1：构建投资组合
        print("\n场景1：构建沪深300投资组合")
        print("-" * 35)
        try:
            # 获取沪深300最新成分股
            weights = data_tool.get_index_weights(
                index_code='000300.SH',
                start_date='2024-01-31',
                end_date='2024-01-31'
            )
            
            if not weights.empty:
                # 获取前10大权重股
                top_stocks = weights.nlargest(10, 'weight')
                stock_codes = top_stocks['con_code'].tolist()
                
                # 获取这些股票的基本信息
                stock_info = data_tool.get_stock_info(symbols=stock_codes)
                
                # 合并数据
                portfolio = top_stocks.merge(
                    stock_info[['ts_code', 'name', 'industry']], 
                    left_on='con_code', 
                    right_on='ts_code',
                    how='left'
                )
                
                print(f"构建投资组合，包含 {len(portfolio)} 只股票:")
                print(portfolio[['con_code', 'name', 'industry', 'weight']].head())
                
        except Exception as e:
            print(f"构建投资组合失败: {e}")
        
        # 场景2：行业分析
        print("\n场景2：行业分布分析")
        print("-" * 25)
        try:
            # 获取所有上市股票的行业分类
            industry_data = data_tool.get_industry_data(level='sw_l1')
            
            if not industry_data.empty:
                # 统计各行业股票数量
                industry_count = industry_data['industry_name'].value_counts()
                
                print("各行业股票数量分布（前10）:")
                print(industry_count.head(10))
                
        except Exception as e:
            print(f"行业分析失败: {e}")
        
        # 场景3：交易日分析
        print("\n场景3：交易日分析")
        print("-" * 20)
        try:
            # 获取2024年1月的交易日历
            trade_cal = data_tool.get_trade_dates(
                start_date='2024-01-01',
                end_date='2024-01-31'
            )
            
            if not trade_cal.empty:
                total_days = len(trade_cal)
                trading_days = len(trade_cal[trade_cal['is_open'] == 1])
                
                print(f"2024年1月:")
                print(f"  总天数: {total_days}")
                print(f"  交易日: {trading_days}")
                print(f"  休市日: {total_days - trading_days}")
                
        except Exception as e:
            print(f"交易日分析失败: {e}")


def main():
    """主函数"""
    print("AlphaDataTool 使用示例")
    print("基于80/20原则的极简数据访问层")
    
    try:
        # 演示核心方法
        demo_core_methods()
        
        # 演示灵活接口
        demo_flexible_interfaces()
        
        # 演示实际应用场景
        demo_practical_scenarios()
        
        print("\n" + "=" * 60)
        print("示例演示完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"演示过程中出现错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
