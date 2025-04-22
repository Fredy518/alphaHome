import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Literal, Optional
import logging

# 假设 get_trade_days_between 位于 tools.calendar 中
# 根据实际项目结构调整导入路径
try:
    from .calendar import get_trade_days_between
except ImportError:
    # 提供备选导入路径，如有必要请调整
    from ..tools.calendar import get_trade_days_between


# 定义切分策略类型 (保留作为类型提示)
SplitStrategy = Literal['trade_days', 'natural_days', 'quarter_end'] 

# =============================================================================
# 专用批次生成函数
# =============================================================================

async def generate_trade_day_batches(
    start_date: str,
    end_date: str,
    batch_size: int,
    ts_code: Optional[str] = None,
    exchange: str = 'SSE',
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, Any]]:
    """
    生成基于交易日的日期批次。

    参数:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        batch_size: 每个批次包含的交易日数量
        ts_code: 可选的股票代码，如果提供则会添加到每个批次参数中
        exchange: 交易所代码，默认为'SSE'（上交所）
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次是包含'start_date'和'end_date'的字典
    """
    _logger = logger if logger else logging.getLogger(__name__)
    _logger.info(f"生成交易日批次: {start_date} 到 {end_date}, 批次大小: {batch_size}")

    try:
        # 获取交易日列表
        trade_days = await get_trade_days_between(start_date, end_date, exchange=exchange)
        
        if not trade_days:
            _logger.warning(f"在 {start_date} 和 {end_date} 之间未找到交易日")
            return []
            
        _logger.info(f"找到 {len(trade_days)} 个交易日")
        
        # 按批次大小分割交易日
        batch_list = []
        for i in range(0, len(trade_days), batch_size):
            batch_days = trade_days[i : i + batch_size]
            if not batch_days:
                continue
                
            batch_params = {
                'start_date': batch_days[0],
                'end_date': batch_days[-1]
            }
            
            # 如果提供了ts_code，添加到批次参数中
            if ts_code:
                batch_params['ts_code'] = ts_code
                
            batch_list.append(batch_params)
            _logger.debug(f"创建批次 {len(batch_list)}: {batch_days[0]} - {batch_days[-1]}")
        
        # 处理边缘情况：找到交易日但未生成批次
        if not batch_list and trade_days:
            _logger.warning("交易日列表非空但未生成批次，使用整个范围作为单个批次")
            batch_params = {
                'start_date': trade_days[0],
                'end_date': trade_days[-1]
            }
            if ts_code:
                batch_params['ts_code'] = ts_code
            return [batch_params]
            
        _logger.info(f"成功生成 {len(batch_list)} 个交易日批次")
        return batch_list
        
    except Exception as e:
        _logger.error(f"生成交易日批次时出错: {e}", exc_info=True)
        raise RuntimeError(f"生成交易日批次失败: {e}") from e


async def generate_natural_day_batches(
    start_date: str,
    end_date: str,
    batch_size: int,
    ts_code: Optional[str] = None,
    date_format: str = '%Y%m%d',
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, Any]]:
    """
    生成基于自然日的日期批次。

    参数:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        batch_size: 每个批次包含的自然日数量
        ts_code: 可选的股票代码，如果提供则会添加到每个批次参数中
        date_format: 日期格式字符串，默认为'%Y%m%d'
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次是包含'start_date'和'end_date'的字典
    """
    _logger = logger if logger else logging.getLogger(__name__)
    _logger.info(f"生成自然日批次: {start_date} 到 {end_date}, 批次大小: {batch_size}")

    try:
        # 转换日期字符串为日期对象
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        # 生成日期范围
        date_range = pd.date_range(start=start_dt, end=end_dt, freq='D')
        natural_days = [d.strftime(date_format) for d in date_range]
        
        if not natural_days:
            _logger.warning(f"在 {start_date} 和 {end_date} 之间未找到有效日期")
            return []
            
        _logger.info(f"找到 {len(natural_days)} 个自然日")
        
        # 按批次大小分割自然日
        batch_list = []
        for i in range(0, len(natural_days), batch_size):
            batch_days = natural_days[i : i + batch_size]
            if not batch_days:
                continue
                
            batch_params = {
                'start_date': batch_days[0],
                'end_date': batch_days[-1]
            }
            
            # 如果提供了ts_code，添加到批次参数中
            if ts_code:
                batch_params['ts_code'] = ts_code
                
            batch_list.append(batch_params)
            _logger.debug(f"创建批次 {len(batch_list)}: {batch_days[0]} - {batch_days[-1]}")
        
        # 处理边缘情况：找到自然日但未生成批次
        if not batch_list and natural_days:
            _logger.warning("自然日列表非空但未生成批次，使用整个范围作为单个批次")
            batch_params = {
                'start_date': natural_days[0],
                'end_date': natural_days[-1]
            }
            if ts_code:
                batch_params['ts_code'] = ts_code
            return [batch_params]
            
        _logger.info(f"成功生成 {len(batch_list)} 个自然日批次")
        return batch_list
        
    except Exception as e:
        _logger.error(f"生成自然日批次时出错: {e}", exc_info=True)
        raise RuntimeError(f"生成自然日批次失败: {e}") from e


async def generate_quarter_end_batches(
    start_date: str,
    end_date: str,
    ts_code: Optional[str] = None,
    date_format: str = '%Y%m%d',
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, str]]:
    """
    生成基于季度末的批次，每个季度末作为一个批次。

    参数:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        ts_code: 可选的股票代码，如果提供则会添加到每个批次参数中
        date_format: 日期格式字符串，默认为'%Y%m%d'
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次是包含'period'的字典，如{'period': '20210331'}
    """
    _logger = logger if logger else logging.getLogger(__name__)
    _logger.info(f"生成季度末批次: {start_date} 到 {end_date}")

    try:
        # 转换日期字符串为日期对象
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        # 生成季度末日期序列
        quarter_ends = pd.date_range(
            start=start_dt, 
            end=end_dt, 
            freq='Q'  # 季度末频率
        )
        
        quarter_end_dates = [d.strftime(date_format) for d in quarter_ends]
        
        if not quarter_end_dates:
            _logger.warning(f"在 {start_date} 和 {end_date} 之间未找到季度末日期")
            return []
            
        _logger.info(f"找到 {len(quarter_end_dates)} 个季度末日期")
        
        # 每个季度末生成一个批次
        batch_list = []
        for quarter_date in quarter_end_dates:
            # 使用 'period' 参数而不是 'start_date'/'end_date'
            batch_params = {'period': quarter_date}
            
            # 如果提供了ts_code，添加到批次参数中
            if ts_code:
                batch_params['ts_code'] = ts_code
                
            batch_list.append(batch_params)
            _logger.debug(f"创建季度末批次: {quarter_date}")
            
        _logger.info(f"成功生成 {len(batch_list)} 个季度末批次")
        return batch_list
        
    except Exception as e:
        _logger.error(f"生成季度末批次时出错: {e}", exc_info=True)
        raise RuntimeError(f"生成季度末批次失败: {e}") from e
