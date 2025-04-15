import os
import pandas as pd
import datetime
from typing import Optional, Union, List, Tuple
from functools import lru_cache

# 导入项目中已有的Tushare API
from ..sources.tushare.tushare_api import TushareAPI

# 缓存交易日历数据
_TRADE_CAL_CACHE = {}

# 移除LRU缓存装饰器，因为我们要改成异步函数
async def get_trade_cal(start_date: str = None, end_date: str = None, exchange: str = 'SSE') -> pd.DataFrame:
    """获取交易日历数据
    
    Args:
        start_date (str, optional): 开始日期，格式：YYYYMMDD，默认为从2000年开始
        end_date (str, optional): 结束日期，格式：YYYYMMDD，默认为当前日期往后一年
        exchange (str, optional): 交易所代码，默认为 'SSE' (上海证券交易所)
        
    Returns:
        pd.DataFrame: 包含交易日历信息的DataFrame，字段包括：
                     - exchange: 交易所代码
                     - cal_date: 日历日期
                     - is_open: 是否交易日（1：是，0：否）
                     - pretrade_date: 上一个交易日
    """
    # 默认参数处理
    if not start_date:
        start_date = '20000101'
    if not end_date:
        # 默认获取到当前日期往后一年的交易日历
        today = datetime.datetime.now()
        next_year = today.replace(year=today.year + 1)
        end_date = next_year.strftime('%Y%m%d')
    
    # 生成缓存键
    cache_key = f"{exchange}_{start_date}_{end_date}"
    
    # 检查缓存
    if cache_key in _TRADE_CAL_CACHE:
        return _TRADE_CAL_CACHE[cache_key].copy()
    
    # 从Tushare获取数据
    token = os.environ.get('TUSHARE_TOKEN')
    
    if not token:
        raise ValueError("未设置TUSHARE_TOKEN环境变量，无法获取交易日历数据")
    
    # 使用项目中的TushareAPI
    api = TushareAPI(token=token)
    
    # 准备查询参数
    params = {
        'exchange': exchange,
        'start_date': start_date,
        'end_date': end_date
    }
    
    # 直接使用await调用异步API，不再创建新的事件循环
    df = await api.query('trade_cal', params=params)
    
    # 处理上一个交易日信息
    df_open = df[df['is_open'] == 1].copy()
    df_open['pretrade_date'] = df_open['cal_date'].shift(1)
    
    # 合并回原始数据
    df = pd.merge(df, df_open[['cal_date', 'pretrade_date']], 
                 on='cal_date', how='left')
    
    # 存入缓存
    _TRADE_CAL_CACHE[cache_key] = df.copy()
    
    return df

async def is_trade_day(date: Union[str, datetime.datetime, datetime.date], exchange: str = 'SSE') -> bool:
    """判断指定日期是否为交易日
    
    Args:
        date (Union[str, datetime.datetime, datetime.date]): 
            指定日期，可以是字符串（格式：YYYYMMDD）或datetime对象
        exchange (str, optional): 交易所代码，默认为 'SSE'
        
    Returns:
        bool: 如果是交易日返回True，否则返回False
    """
    # 将日期转换为统一格式
    if isinstance(date, (datetime.datetime, datetime.date)):
        date_str = date.strftime('%Y%m%d')
    else:
        date_str = date
    
    # 获取交易日历
    calendar = await get_trade_cal(start_date=date_str, end_date=date_str, exchange=exchange)
    
    # 检查该日期是否在交易日历中，并且is_open为1
    if calendar.empty:
        return False
    
    return calendar[calendar['cal_date'] == date_str]['is_open'].iloc[0] == 1

async def get_last_trade_day(date: Union[str, datetime.datetime, datetime.date] = None, 
                       n: int = 1, 
                       exchange: str = 'SSE') -> str:
    """获取指定日期前n个交易日
    
    Args:
        date (Union[str, datetime.datetime, datetime.date], optional): 
            指定日期，可以是字符串（格式：YYYYMMDD）或datetime对象，默认为当前日期
        n (int, optional): 向前获取的交易日数量，默认为1，即上一个交易日
        exchange (str, optional): 交易所代码，默认为 'SSE'
        
    Returns:
        str: 前n个交易日的日期，格式：YYYYMMDD
    """
    # 处理默认日期
    if date is None:
        date = datetime.datetime.now()
    
    # 将日期转换为统一格式
    if isinstance(date, (datetime.datetime, datetime.date)):
        date_str = date.strftime('%Y%m%d')
    else:
        date_str = date
    
    # 获取较长时间段的交易日历，确保能找到前n个交易日
    year_ago = (datetime.datetime.strptime(date_str, '%Y%m%d') - 
               datetime.timedelta(days=365)).strftime('%Y%m%d')
    
    calendar = await get_trade_cal(start_date=year_ago, end_date=date_str, exchange=exchange)
    
    # 过滤出交易日
    trade_days = calendar[calendar['is_open'] == 1]['cal_date'].tolist()
    
    # 找到当前日期在交易日列表中的位置
    try:
        today_index = trade_days.index(date_str)
    except ValueError:
        # 如果当前日期不是交易日，找到前一个交易日
        less_than_today = [d for d in trade_days if d < date_str]
        if not less_than_today:
            raise ValueError(f"无法找到{date_str}之前的交易日")
        today_index = trade_days.index(max(less_than_today))
    
    # 获取前n个交易日
    if today_index < n:
        raise ValueError(f"交易日历中没有足够的历史交易日，请扩大日期范围")
    
    return trade_days[today_index - n]

async def get_next_trade_day(date: Union[str, datetime.datetime, datetime.date] = None, 
                      n: int = 1, 
                      exchange: str = 'SSE') -> str:
    """获取指定日期后n个交易日
    
    Args:
        date (Union[str, datetime.datetime, datetime.date], optional): 
            指定日期，可以是字符串（格式：YYYYMMDD）或datetime对象，默认为当前日期
        n (int, optional): 向后获取的交易日数量，默认为1，即下一个交易日
        exchange (str, optional): 交易所代码，默认为 'SSE'
        
    Returns:
        str: 后n个交易日的日期，格式：YYYYMMDD
    """
    # 处理默认日期
    if date is None:
        date = datetime.datetime.now()
    
    # 将日期转换为统一格式
    if isinstance(date, (datetime.datetime, datetime.date)):
        date_str = date.strftime('%Y%m%d')
    else:
        date_str = date
    
    # 获取较长时间段的交易日历，确保能找到后n个交易日
    year_later = (datetime.datetime.strptime(date_str, '%Y%m%d') + 
                 datetime.timedelta(days=365)).strftime('%Y%m%d')
    
    calendar = await get_trade_cal(start_date=date_str, end_date=year_later, exchange=exchange)
    
    # 过滤出交易日
    trade_days = calendar[calendar['is_open'] == 1]['cal_date'].tolist()
    
    # 找到当前日期在交易日列表中的位置
    try:
        today_index = trade_days.index(date_str)
    except ValueError:
        # 如果当前日期不是交易日，找到后一个交易日
        greater_than_today = [d for d in trade_days if d > date_str]
        if not greater_than_today:
            raise ValueError(f"无法找到{date_str}之后的交易日")
        # 为了获取后n个交易日，需要从今天开始算
        today_index = trade_days.index(min(greater_than_today)) - 1
    
    # 获取后n个交易日
    if today_index + n >= len(trade_days):
        raise ValueError(f"交易日历中没有足够的未来交易日，请扩大日期范围")
    
    return trade_days[today_index + n]

async def get_trade_days_between(start_date: Union[str, datetime.datetime, datetime.date],
                         end_date: Union[str, datetime.datetime, datetime.date],
                         exchange: str = 'SSE') -> List[str]:
    """获取两个日期之间的所有交易日
    
    Args:
        start_date (Union[str, datetime.datetime, datetime.date]): 
            开始日期，可以是字符串（格式：YYYYMMDD）或datetime对象
        end_date (Union[str, datetime.datetime, datetime.date]): 
            结束日期，可以是字符串（格式：YYYYMMDD）或datetime对象
        exchange (str, optional): 交易所代码，默认为 'SSE'
    
    Returns:
        List[str]: 交易日列表，格式：YYYYMMDD
    """
    # 将日期转换为统一格式
    if isinstance(start_date, (datetime.datetime, datetime.date)):
        start_date_str = start_date.strftime('%Y%m%d')
    else:
        start_date_str = start_date
        
    if isinstance(end_date, (datetime.datetime, datetime.date)):
        end_date_str = end_date.strftime('%Y%m%d')
    else:
        end_date_str = end_date
    
    # 获取交易日历
    calendar = await get_trade_cal(start_date=start_date_str, end_date=end_date_str, exchange=exchange)
    
    # 过滤出指定日期范围内的交易日
    trade_days = calendar[
        (calendar['is_open'] == 1) & 
        (calendar['cal_date'] >= start_date_str) & 
        (calendar['cal_date'] <= end_date_str)
    ]['cal_date'].tolist()
    
    return trade_days 