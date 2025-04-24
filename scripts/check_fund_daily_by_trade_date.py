#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
从交易日视角检查tushare_fund_daily表中数据质量
分析每个交易日有多少基金数据缺失，识别数据质量问题的时间模式
"""

import asyncio
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import dotenv
import logging
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.ticker as ticker

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('check_fund_daily_by_date')

# 设置路径
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# 导入
from data_module.task_factory import TaskFactory
from data_module.tools.calendar import get_trade_cal


async def get_fund_date_range():
    """获取所有基金的存续日期范围"""
    db_manager = TaskFactory.get_db_manager()
    
    # 获取所有基金代码及其数据的起止日期
    query = """
    SELECT ts_code, 
           MIN(trade_date) AS first_date, 
           MAX(trade_date) AS last_date
    FROM tushare_fund_daily
    GROUP BY ts_code
    ORDER BY ts_code
    """
    
    fund_ranges = await db_manager.fetch(query)
    
    # 转换为字典格式，便于后续查询
    fund_date_ranges = {}
    for row in fund_ranges:
        ts_code, first_date, last_date = row
        # 确保日期格式一致
        first_date = first_date.strftime("%Y%m%d") if hasattr(first_date, 'strftime') else first_date
        last_date = last_date.strftime("%Y%m%d") if hasattr(last_date, 'strftime') else last_date
        
        fund_date_ranges[ts_code] = {
            'first_date': first_date,
            'last_date': last_date
        }
    
    return fund_date_ranges


async def check_date_coverage(trade_date, fund_date_ranges, db_manager):
    """检查指定交易日的数据覆盖情况"""
    # 找出在该交易日应该有数据的基金（在存续期内的基金）
    active_funds = [
        ts_code for ts_code, date_range in fund_date_ranges.items()
        if date_range['first_date'] <= trade_date <= date_range['last_date']
    ]
    
    expected_count = len(active_funds)
    
    if expected_count == 0:
        return {
            'trade_date': trade_date,
            'expected_funds': 0,
            'actual_funds': 0,
            'missing_funds': 0,
            'missing_ratio': 0.0,
            'active_funds': []
        }
    
    # 查询该交易日实际有数据的基金数量
    query = f"""
    SELECT COUNT(DISTINCT ts_code)
    FROM tushare_fund_daily
    WHERE trade_date = '{trade_date}'
    """
    
    actual_count = await db_manager.fetch_val(query)
    
    # 如果需要，获取实际有数据的基金列表
    if actual_count < expected_count:
        query_actual = f"""
        SELECT DISTINCT ts_code
        FROM tushare_fund_daily
        WHERE trade_date = '{trade_date}'
        """
        actual_funds = await db_manager.fetch(query_actual)
        actual_funds = [row[0] for row in actual_funds]
        
        # 找出缺失数据的基金
        missing_funds = set(active_funds) - set(actual_funds)
    else:
        missing_funds = set()
    
    # 计算缺失率
    missing_count = expected_count - actual_count
    missing_ratio = missing_count / expected_count if expected_count > 0 else 0.0
    
    return {
        'trade_date': trade_date,
        'expected_funds': expected_count,
        'actual_funds': actual_count,
        'missing_funds': missing_count,
        'missing_ratio': missing_ratio,
        'active_funds': active_funds,
        'missing_fund_codes': list(missing_funds)[:10] if len(missing_funds) <= 10 else list(missing_funds)[:10] + ['...']
    }


async def check_missing_dates():
    """从交易日视角检查数据质量"""
    print("开始从交易日视角检查tushare_fund_daily表数据质量...")

    # 初始化TaskFactory
    await TaskFactory.initialize()
    db_manager = TaskFactory.get_db_manager()

    try:
        # 步骤1: 获取交易日历中的所有交易日
        start_date = "20100101"  # 设置一个相对较早的起始日期
        end_date = datetime.now().strftime("%Y%m%d")
        
        print(f"获取交易日历数据 ({start_date} 到 {end_date})...")
        calendar_df = await get_trade_cal(start_date=start_date, end_date=end_date)
        
        # 过滤出交易日（is_open=1）
        calendar_df = calendar_df[calendar_df['is_open'] == 1]
        trade_dates = calendar_df['cal_date'].tolist()
        print(f"交易日历中共有 {len(trade_dates)} 个交易日")

        # 步骤2: 获取所有基金的存续日期范围
        print("获取所有基金的数据日期范围...")
        fund_date_ranges = await get_fund_date_range()
        print(f"数据库中共有 {len(fund_date_ranges)} 个基金")
        
        if not fund_date_ranges:
            print("数据库中没有基金数据，请先更新数据")
            return

        # 步骤3: 检查每个交易日的数据质量
        # 为了提高效率，可以选择抽样检查或者按时间段分组检查
        print("开始检查每个交易日的数据质量...")
        
        # 可选：对交易日进行抽样或分组
        # 这里我们选择对所有交易日进行检查，但可以根据实际情况调整
        # 例如，可以每月抽取一天，或者按季度、年度分组
        
        # 根据日期的数量决定是否需要抽样
        if len(trade_dates) > 365:  # 如果交易日超过一年
            # 每月抽取一天进行详细分析
            sampled_dates = []
            current_month = None
            
            for date in sorted(trade_dates):
                month = date[:6]  # 取年月部分
                if month != current_month:
                    sampled_dates.append(date)
                    current_month = month
            
            print(f"交易日数量较多，采用抽样方式检查 {len(sampled_dates)}/{len(trade_dates)} 个交易日")
            check_dates = sampled_dates
        else:
            check_dates = trade_dates
        
        # 检查选定的交易日
        date_results = []
        total_dates = len(check_dates)
        
        for i, trade_date in enumerate(sorted(check_dates)):
            print(f"检查交易日 {i+1}/{total_dates}: {trade_date}", end="\r")
            result = await check_date_coverage(trade_date, fund_date_ranges, db_manager)
            date_results.append(result)
        
        print("\n\n交易日数据质量检查结果:")
        print("-" * 80)
        
        # 按照缺失率排序，展示数据质量最差的日期
        date_results.sort(key=lambda x: x['missing_ratio'], reverse=True)
        
        # 展示数据质量最差的前10个交易日
        print("数据质量最差的交易日:")
        for i, result in enumerate(date_results[:10]):
            print(f"排名 {i+1}:")
            print(f"交易日: {result['trade_date']}")
            print(f"应有基金数: {result['expected_funds']}, 实际基金数: {result['actual_funds']}")
            missing_count = result['missing_funds']
            
            if missing_count > 0:
                print(f"缺失基金数: {missing_count} ({result['missing_ratio']:.2%})")
                if 'missing_fund_codes' in result and result['missing_fund_codes']:
                    missing_codes_str = ", ".join(str(code) for code in result['missing_fund_codes'])
                    print(f"部分缺失基金代码: {missing_codes_str}")
            else:
                print("无缺失数据")
            print("-" * 80)
        
        # 按时间顺序排序，用于时间序列分析
        date_results.sort(key=lambda x: x['trade_date'])
        
        # 统计整体情况
        total_dates = len(date_results)
        complete_dates = sum(1 for r in date_results if r['missing_ratio'] == 0)
        missing_dates = total_dates - complete_dates
        high_missing_dates = sum(1 for r in date_results if r['missing_ratio'] > 0.5)
        
        print("\n时间维度统计信息:")
        print(f"检查的交易日数量: {total_dates}")
        print(f"数据完整的交易日数量: {complete_dates} ({complete_dates/total_dates:.2%})")
        print(f"存在缺失的交易日数量: {missing_dates} ({missing_dates/total_dates:.2%})")
        print(f"缺失率超过50%的交易日数量: {high_missing_dates} ({high_missing_dates/total_dates:.2%})")
        
        # 计算平均缺失率
        avg_missing_ratio = sum(r['missing_ratio'] for r in date_results) / total_dates
        print(f"平均每个交易日的数据缺失率: {avg_missing_ratio:.2%}")
        
        # 时间趋势分析
        if len(date_results) >= 2:
            try:
                # 准备数据
                trade_dates = [datetime.strptime(r['trade_date'], "%Y%m%d") for r in date_results]
                missing_ratios = [r['missing_ratio'] for r in date_results]
                
                # 绘制时间序列图
                plt.figure(figsize=(12, 6))
                plt.plot(trade_dates, missing_ratios, marker='o', linestyle='-', markersize=3)
                plt.title('基金数据缺失率时间趋势')
                plt.xlabel('交易日')
                plt.ylabel('数据缺失率')
                plt.grid(True)
                
                # 设置日期格式
                if max(trade_dates) - min(trade_dates) > timedelta(days=365*2):
                    # 对于跨度超过2年的数据，使用年格式
                    plt.gca().xaxis.set_major_formatter(DateFormatter('%Y-%m'))
                else:
                    # 对于跨度较短的数据，使用年月格式
                    plt.gca().xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
                
                # 设置纵轴为百分比格式
                plt.gca().yaxis.set_major_formatter(ticker.PercentFormatter(1.0))
                
                # 自动旋转日期标签
                plt.gcf().autofmt_xdate()
                
                # 保存图表
                save_path = 'fund_data_missing_ratio_trend.png'
                plt.savefig(save_path)
                print(f"\n时间趋势图已保存至: {save_path}")
                plt.close()
                
                # 按年度/月度分析
                year_month_stats = {}
                for r in date_results:
                    trade_date = r['trade_date']
                    year_month = trade_date[:6]  # 取年月
                    
                    if year_month not in year_month_stats:
                        year_month_stats[year_month] = {
                            'dates': 0,
                            'missing_ratio_sum': 0.0
                        }
                    
                    year_month_stats[year_month]['dates'] += 1
                    year_month_stats[year_month]['missing_ratio_sum'] += r['missing_ratio']
                
                print("\n按月度分析数据质量:")
                print("-" * 80)
                print("年月\t\t平均缺失率\t\t检查日数")
                print("-" * 80)
                
                for year_month in sorted(year_month_stats.keys()):
                    stats = year_month_stats[year_month]
                    avg_ratio = stats['missing_ratio_sum'] / stats['dates']
                    formatted_year_month = f"{year_month[:4]}-{year_month[4:6]}"
                    print(f"{formatted_year_month}\t\t{avg_ratio:.2%}\t\t{stats['dates']}")
                
            except Exception as plot_error:
                print(f"绘制图表时发生错误: {str(plot_error)}")

    except Exception as e:
        print(f"检查过程中发生错误: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        # 关闭连接
        await TaskFactory.shutdown()
        print("检查完成")


if __name__ == "__main__":
    asyncio.run(check_missing_dates()) 