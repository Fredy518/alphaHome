import logging
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

# 假设 get_trade_days_between 位于 tools.calendar 中
# 根据实际项目结构调整导入路径
# try:
#     from .calendar import get_trade_days_between # <-- REMOVE
# except ImportError:
#     # 提供备选导入路径，如有必要请调整
#     from ..tools.calendar import get_trade_days_between # <-- REMOVE


# 定义切分策略类型 (保留作为类型提示)
SplitStrategy = Literal["trade_days", "natural_days", "quarter_end"]

# =============================================================================
# 专用批次生成函数
# =============================================================================


async def generate_trade_day_batches(
    start_date: str,
    end_date: str,
    batch_size: int,
    ts_code: Optional[str] = None,
    exchange: str = "SSE",
    additional_params: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """
    生成基于交易日的日期批次。

    参数:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        batch_size: 每个批次包含的交易日数量
        ts_code: 可选的股票代码，如果提供则会添加到每个批次参数中
        exchange: 交易所代码，默认为'SSE'（上交所）
        additional_params: 可选的附加参数字典，将合并到每个批次中
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次是包含'start_date'和'end_date'的字典
    """
    from .calendar import get_trade_days_between

    _logger = logger if logger else logging.getLogger(__name__)
    _logger.info(f"生成交易日批次: {start_date} 到 {end_date}, 批次大小: {batch_size}")

    try:
        # 获取交易日列表
        trade_days = await get_trade_days_between(
            start_date, end_date, exchange=exchange
        )

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

            batch_params = {"start_date": batch_days[0], "end_date": batch_days[-1]}

            # 如果提供了ts_code，添加到批次参数中
            if ts_code:
                batch_params["ts_code"] = ts_code

            # 添加额外参数
            if additional_params:
                batch_params.update(additional_params)

            batch_list.append(batch_params)
            _logger.debug(
                f"创建批次 {len(batch_list)}: {batch_days[0]} - {batch_days[-1]}"
            )

        # 处理边缘情况：找到交易日但未生成批次
        if not batch_list and trade_days:
            _logger.warning("交易日列表非空但未生成批次，使用整个范围作为单个批次")
            batch_params = {"start_date": trade_days[0], "end_date": trade_days[-1]}
            if ts_code:
                batch_params["ts_code"] = ts_code
            if additional_params:
                batch_params.update(additional_params)
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
    date_format: str = "%Y%m%d",
    additional_params: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """
    生成基于自然日的日期批次。

    参数:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        batch_size: 每个批次包含的自然日数量
        ts_code: 可选的股票代码，如果提供则会添加到每个批次参数中
        date_format: 日期格式字符串，默认为'%Y%m%d'
        additional_params: 可选的附加参数字典，将合并到每个批次中
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
        date_range = pd.date_range(start=start_dt, end=end_dt, freq="D")
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

            batch_params = {"start_date": batch_days[0], "end_date": batch_days[-1]}

            # 如果提供了ts_code，添加到批次参数中
            if ts_code:
                batch_params["ts_code"] = ts_code

            # 添加额外参数
            if additional_params:
                batch_params.update(additional_params)

            batch_list.append(batch_params)
            _logger.debug(
                f"创建批次 {len(batch_list)}: {batch_days[0]} - {batch_days[-1]}"
            )

        # 处理边缘情况：找到自然日但未生成批次
        if not batch_list and natural_days:
            _logger.warning("自然日列表非空但未生成批次，使用整个范围作为单个批次")
            batch_params = {"start_date": natural_days[0], "end_date": natural_days[-1]}
            if ts_code:
                batch_params["ts_code"] = ts_code
            if additional_params:
                batch_params.update(additional_params)
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
    date_format: str = "%Y%m%d",
    additional_params: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, str]]:
    """
    生成基于季度末的批次，每个季度末作为一个批次。

    参数:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        ts_code: 可选的股票代码，如果提供则会添加到每个批次参数中
        date_format: 日期格式字符串，默认为'%Y%m%d'
        additional_params: 可选的附加参数字典，将合并到每个批次中
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
        quarter_ends = pd.date_range(start=start_dt, end=end_dt, freq="Q")  # 季度末频率

        quarter_end_dates = [d.strftime(date_format) for d in quarter_ends]

        if not quarter_end_dates:
            _logger.warning(f"在 {start_date} 和 {end_date} 之间未找到季度末日期")
            return []

        _logger.info(f"找到 {len(quarter_end_dates)} 个季度末日期")

        # 每个季度末生成一个批次
        batch_list = []
        for quarter_date in quarter_end_dates:
            # 使用 'period' 参数而不是 'start_date'/'end_date'
            batch_params = {"period": quarter_date}

            # 如果提供了ts_code，添加到批次参数中
            if ts_code:
                batch_params["ts_code"] = ts_code

            # 添加额外参数
            if additional_params:
                batch_params.update(additional_params)

            batch_list.append(batch_params)
            _logger.debug(f"创建季度末批次: {quarter_date}")

        _logger.info(f"成功生成 {len(batch_list)} 个季度末批次")
        return batch_list

    except Exception as e:
        _logger.error(f"生成季度末批次时出错: {e}", exc_info=True)
        raise RuntimeError(f"生成季度末批次失败: {e}") from e


async def generate_single_date_batches(
    start_date: str,
    end_date: str,
    date_field: str = "trade_date",
    ts_code: Optional[str] = None,
    exchange: str = "SSE",
    additional_params: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """
    为指定日期范围内的每一个交易日生成单独的批次，每个批次包含单一日期。
    适用于需要对每个日期单独查询的API，如 fund_nav 等。

    参数:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        date_field: API调用时使用的日期字段名（例如 'trade_date', 'nav_date'），默认为'trade_date'
        ts_code: 可选的股票/基金代码，如果提供则会添加到每个批次参数中
        exchange: 交易所代码，用于获取交易日历，默认为'SSE'
        additional_params: 可选的附加参数字典，将合并到每个批次中
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次是包含指定日期字段和可选附加参数的字典
    """
    from .calendar import get_trade_days_between

    _logger = logger if logger else logging.getLogger(__name__)
    _logger.info(
        f"生成单交易日批次: {start_date} 到 {end_date}, 日期字段: {date_field}"
    )

    try:
        # 获取指定范围内的所有交易日
        trade_days = await get_trade_days_between(
            start_date, end_date, exchange=exchange
        )

        if not trade_days:
            _logger.warning(f"在 {start_date} 和 {end_date} 之间未找到交易日")
            return []

        _logger.info(f"找到 {len(trade_days)} 个交易日")

        # 为每个交易日创建一个批次
        batch_list = []
        for trade_day in trade_days:
            # 创建基本参数字典，使用指定的日期字段
            batch_params = {date_field: trade_day}

            # 如果提供了ts_code，添加到批次参数中
            if ts_code:
                batch_params["ts_code"] = ts_code

            # 添加额外参数
            if additional_params:
                batch_params.update(additional_params)

            batch_list.append(batch_params)

        _logger.info(f"成功生成 {len(batch_list)} 个单日期批次")
        return batch_list

    except Exception as e:
        _logger.error(f"生成单日期批次时出错: {e}", exc_info=True)
        raise RuntimeError(f"生成单日期批次失败: {e}") from e


# =============================================================================
# 月度批次生成函数
# =============================================================================


async def generate_month_batches(
    start_m: str,
    end_m: str,
    batch_size: int = 12,
    ts_code: Optional[str] = None,
    date_format: str = "%Y%m",
    additional_params: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """
    生成基于月份的批次（如宏观数据常用的YYYYMM格式）。

    参数:
        start_m: 开始月份字符串（YYYYMM格式）
        end_m: 结束月份字符串（YYYYMM格式）
        batch_size: 每个批次包含的月份数，默认12（即一年）
        ts_code: 可选的代码参数，如有则添加到每个批次
        date_format: 月份格式字符串，默认为'%Y%m'
        additional_params: 可选的附加参数字典，将合并到每个批次中
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次是包含'start_m'和'end_m'的字典
    """
    _logger = logger if logger else logging.getLogger(__name__)
    _logger.info(f"生成月度批次: {start_m} 到 {end_m}, 批次大小: {batch_size}")

    try:
        # 校验并转换参数
        start_dt = pd.to_datetime(start_m, format=date_format)
        end_dt = pd.to_datetime(end_m, format=date_format)
        if start_dt > end_dt:
            _logger.warning(f"开始月份 {start_m} 晚于结束月份 {end_m}")
            return []
        # 生成月份范围
        month_range = pd.date_range(start=start_dt, end=end_dt, freq="MS")
        month_list = [d.strftime(date_format) for d in month_range]
        if not month_list:
            _logger.warning(f"在 {start_m} 和 {end_m} 之间未找到有效月份")
            return []
        _logger.info(f"找到 {len(month_list)} 个月份")
        # 按批次大小分割月份
        batch_list = []
        for i in range(0, len(month_list), batch_size):
            batch_months = month_list[i : i + batch_size]
            if not batch_months:
                continue
            batch_params = {"start_m": batch_months[0], "end_m": batch_months[-1]}
            if ts_code:
                batch_params["ts_code"] = ts_code
            # 添加额外参数
            if additional_params:
                batch_params.update(additional_params)
            batch_list.append(batch_params)
            _logger.debug(
                f"创建月度批次 {len(batch_list)}: {batch_months[0]} - {batch_months[-1]}"
            )
        # 边缘情况：找到月份但未生成批次
        if not batch_list and month_list:
            _logger.warning("月份列表非空但未生成批次，使用整个范围作为单个批次")
            batch_params = {"start_m": month_list[0], "end_m": month_list[-1]}
            if ts_code:
                batch_params["ts_code"] = ts_code
            if additional_params:
                batch_params.update(additional_params)
            return [batch_params]
        _logger.info(f"成功生成 {len(batch_list)} 个月度批次")
        return batch_list
    except Exception as e:
        _logger.error(f"生成月度批次时出错: {e}", exc_info=True)
        raise RuntimeError(f"生成月度批次失败: {e}") from e


# 用法示例：
# batches = await generate_month_batches('202001', '202312', batch_size=6)


# =============================================================================
# 股票代码批次生成函数
# =============================================================================


async def generate_stock_code_batches(
    db_connection,
    table_name: str = "tushare_stock_basic",
    code_column: str = "ts_code",
    filter_condition: Optional[str] = None,
    api_instance=None,
    additional_params: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """
    生成按单个股票代码的批次参数列表，支持从数据库或API获取股票代码。

    这是一个通用的工具函数，可用于需要按股票代码进行批量处理的各种任务，
    如分红数据、财务数据、公司公告等。每个批次包含单个股票代码，符合大多数Tushare接口的要求。

    参数:
        db_connection: 数据库连接对象
        table_name: 股票基础信息表名，默认'tushare_stock_basic'
        code_column: 股票代码列名，默认'ts_code'
        filter_condition: 可选的SQL过滤条件，如'list_status = "L"'
        api_instance: 可选的API实例，用于从API获取股票代码（当数据库方法失败时）
        additional_params: 可选的附加参数字典，将添加到每个批次中
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次包含单个 'ts_code' 和可选的附加参数
    """
    _logger = logger if logger else logging.getLogger(__name__)
    _logger.info(f"开始生成单股票代码批次 - 表名: {table_name}")

    try:
        # 获取股票代码列表
        stock_codes = await _get_stock_codes_from_sources(
            db_connection=db_connection,
            table_name=table_name,
            code_column=code_column,
            filter_condition=filter_condition,
            api_instance=api_instance,
            logger=_logger,
        )

        if not stock_codes:
            _logger.warning("未获取到任何股票代码")
            return []

        _logger.info(f"获取到 {len(stock_codes)} 个股票代码，为每个代码生成单独批次")

        # 为每个股票代码生成单独的批次
        batch_list = []
        for ts_code in stock_codes:
            # 创建批次参数，包含单个股票代码
            batch_params = {"ts_code": ts_code}

            # 添加额外参数
            if additional_params:
                batch_params.update(additional_params)

            batch_list.append(batch_params)

        _logger.info(f"成功生成 {len(batch_list)} 个单股票代码批次")
        return batch_list

    except Exception as e:
        _logger.error(f"生成单股票代码批次时出错: {e}", exc_info=True)
        raise RuntimeError(f"生成单股票代码批次失败: {e}") from e


async def _get_stock_codes_from_sources(
    db_connection,
    table_name: str,
    code_column: str,
    filter_condition: Optional[str],
    api_instance,
    logger: logging.Logger,
) -> List[str]:
    """
    从多个数据源获取股票代码列表的内部方法

    优先级: 数据库 -> API -> 预定义列表
    """

    # 方法1: 从数据库获取
    if db_connection:
        try:
            # 构建SQL查询
            query = f"SELECT {code_column} FROM {table_name}"
            if filter_condition:
                query += f" WHERE {filter_condition}"
            query += f" ORDER BY {code_column}"

            logger.info(f"从数据库获取股票代码: {query}")
            result = await db_connection.fetch_all(query)

            if result:
                codes = [row[code_column] for row in result]
                logger.info(f"从数据库获取到 {len(codes)} 个股票代码")
                return codes

        except Exception as e:
            logger.warning(f"从数据库获取股票代码失败: {e}")

    # 方法2: 通过API获取（如果提供了API实例）
    if api_instance:
        try:
            logger.info("尝试通过API获取股票代码列表")

            # 调用stock_basic接口获取股票列表
            df = await api_instance.query(
                api_name="stock_basic",
                fields=[code_column],
                params={"list_status": "L"},  # 只获取上市状态的股票
            )

            if df is not None and not df.empty:
                codes = df[code_column].tolist()
                logger.info(f"通过API获取到 {len(codes)} 个股票代码")
                return codes

        except Exception as e:
            logger.warning(f"通过API获取股票代码失败: {e}")

    # 方法3: 使用预定义的主要股票代码（兜底方案）
    logger.warning("无法从数据库或API获取股票列表，使用预定义代码")
    predefined_codes = [
        "000001.SZ",
        "000002.SZ",
        "000858.SZ",
        "000725.SZ",
        "600000.SH",
        "600036.SH",
        "600519.SH",
        "600276.SH",
        "002415.SZ",
        "002594.SZ",
        "300750.SZ",
        "300059.SZ",
    ]

    logger.info(f"使用预定义股票代码: {len(predefined_codes)} 个")
    return predefined_codes
