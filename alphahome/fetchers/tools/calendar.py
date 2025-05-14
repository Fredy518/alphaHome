import os
import pandas as pd
import datetime
from typing import Optional, Union, List, Tuple
from functools import lru_cache
import logging
from datetime import timedelta
import calendar as std_calendar # 新增导入 Python 标准库 calendar

# 导入项目中已有的Tushare API
from ..sources.tushare.tushare_api import TushareAPI
# from ..sources.tushare.tushare_shared import TushareToken # 删除此错误导入

logger = logging.getLogger(__name__)

# 全局缓存交易日历数据，键为 (start_date, end_date, exchange)
_TRADE_CAL_CACHE = {}

# 移除LRU缓存装饰器，因为我们要改成异步函数，并且内部有自定义缓存
# @lru_cache(maxsize=4)
async def get_trade_cal(start_date: str = None, end_date: str = None, exchange: str = 'SSE') -> pd.DataFrame:
    """获取交易日历数据
    
    Args:
        start_date (str, optional): 开始日期，格式：YYYYMMDD，默认为从2000年开始
        end_date (str, optional): 结束日期，格式：YYYYMMDD，默认为当前日期往后一年
        exchange (str, optional): 交易所代码，默认为 'SSE' (上海证券交易所). 
                                 支持 'HK' 或 'HKEX' 获取港股日历。
        
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
        end_date_dt = datetime.datetime.now() + timedelta(days=365)
        end_date = end_date_dt.strftime('%Y%m%d')

    cache_key = (start_date, end_date, exchange.upper())
    if cache_key in _TRADE_CAL_CACHE:
        logger.debug(f"交易日历缓存命中: {cache_key}")
        return _TRADE_CAL_CACHE[cache_key].copy() # 返回副本以防外部修改缓存内容

    logger.debug(f"交易日历缓存未命中，尝试从API获取: {cache_key}")

    # 初始化TushareAPI实例 (如果尚未初始化或需要特定于此调用的实例)
    try:
        current_api_instance = TushareAPI(token=None, logger=logger) # TushareAPI 将尝试从环境变量获取token
        if not current_api_instance.token: # 检查token是否已在TushareAPI内部成功加载
            logger.error("TushareAPI未能获取token (环境变量 TUSHARE_TOKEN 可能未设置)，无法查询交易日历。")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"初始化TushareAPI时发生错误: {e}")
        return pd.DataFrame()

    is_hk_exchange = exchange.upper() in ('HK', 'HKEX')
    api_name_to_call = "hk_tradecal" if is_hk_exchange else "trade_cal"
    params = {'start_date': start_date, 'end_date': end_date}
    
    final_df = pd.DataFrame()

    if api_name_to_call == "hk_tradecal":
        logger.info(f"Fetching Hong Kong trade calendar (hk_tradecal) for range: {start_date} - {end_date}")
        try:
            s_date_obj = datetime.datetime.strptime(start_date, '%Y%m%d').date()
            e_date_obj = datetime.datetime.strptime(end_date, '%Y%m%d').date()
        except ValueError as e:
            logger.error(f"日期格式错误 for hk_tradecal: {start_date}, {end_date}. Error: {e}")
            return pd.DataFrame()

        chunk_years = 2  # 修改：将分块大小从5年减小到2年
        all_chunk_dfs = []
        loop_s_date = s_date_obj

        while loop_s_date <= e_date_obj:
            year_to_add = chunk_years - 1 
            
            # 修改：改进日期块结束点的计算逻辑
            target_year = loop_s_date.year + year_to_add
            target_month = loop_s_date.month
            target_day = loop_s_date.day

            if target_month == 2 and target_day == 29:
                # 检查目标年份是否是闰年
                is_leap = (target_year % 4 == 0 and target_year % 100 != 0) or (target_year % 400 == 0)
                if not is_leap:
                    target_day = 28 # 如果目标年不是闰年，则设为2月28日
            
            try:
                chunk_e_date_calc = datetime.date(target_year, target_month, target_day)
            except ValueError:
                # 后备方案：取目标年、目标月的最后一天
                month_range = std_calendar.monthrange(target_year, target_month)
                chunk_e_date_calc = datetime.date(target_year, target_month, month_range[1])
            
            current_chunk_e_date = min(chunk_e_date_calc, e_date_obj)
            
            chunk_start_str = loop_s_date.strftime('%Y%m%d')
            chunk_end_str = current_chunk_e_date.strftime('%Y%m%d')

            logger.info(f"Fetching hk_tradecal chunk: {chunk_start_str} - {chunk_end_str}")
            try:
                chunk_params = {
                    'start_date': chunk_start_str,
                    'end_date': chunk_end_str,
                    'is_open': ''  # Tushare文档说明 is_open='' 表示不筛选
                }
                df_chunk = await current_api_instance.query(
                    api_name="hk_tradecal",
                    params=chunk_params, # API特定参数通过 params 字典传递
                    fields='',           # 明确传递 fields, 为空则查询所有默认字段
                    page_size=2500       # 控制 TushareAPI.query 内部的分页判断逻辑
                )
                if df_chunk is not None and not df_chunk.empty:
                    all_chunk_dfs.append(df_chunk)
                elif df_chunk is None:
                    logger.warning(f"hk_tradecal chunk {chunk_start_str}-{chunk_end_str} returned None (error during fetch)")

            except Exception as e:
                logger.error(f"Error fetching hk_tradecal chunk {chunk_start_str}-{chunk_end_str}: {e}")
                # Decide if we should stop or continue with other chunks
                # For now, log and continue, result might be partial
            
            # Move to the start of the next chunk
            if current_chunk_e_date >= e_date_obj: # Reached overall end date
                break
            loop_s_date = current_chunk_e_date + timedelta(days=1)
        
        if all_chunk_dfs:
            final_df = pd.concat(all_chunk_dfs, ignore_index=True)
            # Ensure correct data types before dropping duplicates, especially for dates if mixed from chunks
            if 'cal_date' in final_df.columns:
                final_df['cal_date'] = pd.to_datetime(final_df['cal_date']).dt.strftime('%Y%m%d')
                final_df.drop_duplicates(subset=['cal_date'], keep='first', inplace=True)
                final_df.sort_values(by='cal_date', inplace=True)
            else:
                logger.warning("hk_tradecal result missing 'cal_date' column after concat.")
                final_df = pd.DataFrame() # Invalid result
        else:
            logger.warning(f"hk_tradecal fetch for {start_date}-{end_date} resulted in no data after chunking.")
            final_df = pd.DataFrame()

    else: # For other exchanges (e.g., SSE using 'trade_cal')
        logger.info(f"Fetching standard trade calendar ({api_name_to_call}) for {exchange}: {start_date} - {end_date}")
        try:
            df_api = await current_api_instance.query(
                api_name=api_name_to_call, 
                params=params, # 将 **params 修改为 params=params
                fields=''  # Let TushareAPI handle default/all fields for trade_cal
            )
            if df_api is not None:
                final_df = df_api
            else:
                logger.warning(f"{api_name_to_call} for {exchange} returned None.")
                final_df = pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching {api_name_to_call} for {exchange}: {e}")
            final_df = pd.DataFrame()

    # Add exchange column if not present (Tushare might not include it for single exchange queries)
    if not final_df.empty and 'exchange' not in final_df.columns:
        final_df['exchange'] = exchange.upper()
    
    # Cache the final result (even if empty, to avoid re-fetching on errors for a while)
    _TRADE_CAL_CACHE[cache_key] = final_df.copy()
    logger.info(f"Fetched and cached {len(final_df)} records for {api_name_to_call} ({exchange}) range {start_date}-{end_date}")
    return final_df.copy() # Return a copy

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
                      exchange: str = 'SSE') -> Optional[str]:
    """获取指定日期后n个交易日
    
    Args:
        date (Union[str, datetime.datetime, datetime.date], optional): 
            指定日期，可以是字符串（格式：YYYYMMDD）或datetime对象，默认为当前日期
        n (int, optional): 向后获取的交易日数量，默认为1，即下一个交易日
        exchange (str, optional): 交易所代码，默认为 'SSE'
        
    Returns:
        Optional[str]: 后n个交易日的日期（格式：YYYYMMDD），如果找不到则返回None
    """
    # 处理默认日期
    if date is None:
        date = datetime.datetime.now()
    
    # 将日期转换为统一格式
    if isinstance(date, (datetime.datetime, datetime.date)):
        date_str = date.strftime('%Y%m%d')
    else:
        date_str = date
    
    # 获取更长时间段的交易日历（例如，未来两年），确保能找到后n个交易日
    future_end_date = (datetime.datetime.strptime(date_str, '%Y%m%d') + 
                 datetime.timedelta(days=365 * 2)).strftime('%Y%m%d') # Increased to 2 years
    
    try:
        # 尝试获取从 date_str 开始的日历
        calendar = await get_trade_cal(start_date=date_str, end_date=future_end_date, exchange=exchange)
    except Exception as e:
        # Log error if fetching calendar fails
        # Consider adding logging here if not already present in get_trade_cal
        print(f"Error fetching trade calendar: {e}") # Basic print, replace with logger if available
        return None
    
    if calendar.empty:
        return None
    
    # 过滤出交易日
    trade_days = calendar[calendar['is_open'] == 1]['cal_date'].tolist()
    
    if not trade_days:
        return None # No trade days found in the fetched range
    
    # 找到当前日期在交易日列表中的位置或之后的位置
    today_index = -1
    for i, day in enumerate(trade_days):
        if day >= date_str:
            today_index = i
            break
    
    if today_index == -1:
        # date_str is after the last known trade day in the range
        return None
    
    # Adjust index if date_str itself is not a trade day but we found the next one
    if trade_days[today_index] > date_str:
         # If date_str is not a trade day, the first day >= date_str is the next one.
         # We want the nth day *after* date_str, so effectively we need the (n)th day from today_index
        target_index = today_index + n - 1 # Example: date=Sun, n=1 (next trade day Mon). day[today_index]=Mon. target=Mon (index + 1 - 1)
    else: # date_str is a trade day
         # We want the nth day *after* date_str, so target is index + n
        target_index = today_index + n
    
    # 获取目标交易日
    if target_index < len(trade_days):
        return trade_days[target_index]
    else:
        # 如果计算出的索引超出了范围，说明获取的日历数据仍然不足
        # Log this situation
        print(f"Warning: Could not find {n} trade days after {date_str} within the {future_end_date} range.") # Basic print
        return None # Return None instead of raising error

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