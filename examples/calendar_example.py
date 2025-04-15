"""
交易日历工具函数使用示例
"""
import os
import sys
import datetime
import pandas as pd

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入交易日历工具函数
from data_module.tools.calendar import (
    get_trade_cal, 
    is_trade_day, 
    get_last_trade_day, 
    get_next_trade_day,
    get_trade_days_between
)

def main():
    # 确保设置了Tushare API token
    if not os.environ.get('TUSHARE_TOKEN'):
        # 这里可以直接设置token，但建议使用环境变量
        # os.environ['TUSHARE_TOKEN'] = '你的tushare token'
        print("请先设置TUSHARE_TOKEN环境变量")
        return
    
    # 示例1: 获取交易日历
    print("=== 示例1: 获取交易日历 ===")
    today = datetime.datetime.now()
    start_date = (today - datetime.timedelta(days=10)).strftime('%Y%m%d')
    end_date = today.strftime('%Y%m%d')
    
    calendar = get_trade_cal(start_date=start_date, end_date=end_date)
    print(f"从{start_date}到{end_date}的交易日历:")
    print(calendar)
    print()
    
    # 示例2: 判断日期是否为交易日
    print("=== 示例2: 判断日期是否为交易日 ===")
    date_to_check = today
    is_trading = is_trade_day(date_to_check)
    print(f"{date_to_check.strftime('%Y-%m-%d')}是否为交易日: {is_trading}")
    print()
    
    # 示例3: 获取上一个交易日
    print("=== 示例3: 获取上一个交易日 ===")
    last_trade_date = get_last_trade_day()
    print(f"上一个交易日是: {last_trade_date[:4]}-{last_trade_date[4:6]}-{last_trade_date[6:]}")
    
    # 获取前5个交易日
    last_5_trade_date = get_last_trade_day(n=5)
    print(f"前5个交易日是: {last_5_trade_date[:4]}-{last_5_trade_date[4:6]}-{last_5_trade_date[6:]}")
    print()
    
    # 示例4: 获取下一个交易日
    print("=== 示例4: 获取下一个交易日 ===")
    next_trade_date = get_next_trade_day()
    print(f"下一个交易日是: {next_trade_date[:4]}-{next_trade_date[4:6]}-{next_trade_date[6:]}")
    
    # 获取后3个交易日
    next_3_trade_date = get_next_trade_day(n=3)
    print(f"后3个交易日是: {next_3_trade_date[:4]}-{next_3_trade_date[4:6]}-{next_3_trade_date[6:]}")
    print()
    
    # 示例5: 获取日期范围内的所有交易日
    print("=== 示例5: 获取日期范围内的所有交易日 ===")
    start = today - datetime.timedelta(days=30)
    end = today
    trade_days = get_trade_days_between(start, end)
    print(f"过去30天内的交易日数量: {len(trade_days)}")
    print("交易日列表:")
    for day in trade_days:
        print(f"{day[:4]}-{day[4:6]}-{day[6:]}")
    print()
    
    # 示例6: 缓存机制演示
    print("=== 示例6: 缓存机制演示 ===")
    print("首次调用 get_trade_cal...")
    import time
    start_time = time.time()
    calendar1 = get_trade_cal(start_date='20220101', end_date='20221231')
    time1 = time.time() - start_time
    print(f"获取2022年交易日历用时: {time1:.4f}秒")
    
    print("再次调用 get_trade_cal (使用缓存)...")
    start_time = time.time()
    calendar2 = get_trade_cal(start_date='20220101', end_date='20221231')
    time2 = time.time() - start_time
    print(f"从缓存获取2022年交易日历用时: {time2:.4f}秒")
    print(f"性能提升: {time1/time2:.2f}倍")

if __name__ == "__main__":
    main() 