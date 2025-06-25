import logging
from datetime import datetime
from re import I
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

# BatchPlanner 导入
from ....common.planning.batch_planner import BatchPlanner, Source, Partition, Map

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
# 通用工具函数
# =============================================================================

def normalize_date_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    default_start_date: str = "20200101",
    logger: Optional[logging.Logger] = None,
    task_name: str = "unknown",
) -> tuple[str, str]:
    """
    标准化日期范围处理逻辑，供所有任务复用

    Args:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        default_start_date: 默认起始日期
        logger: 可选的日志记录器
        task_name: 任务名称，用于日志记录

    Returns:
        tuple: (start_date, end_date) 标准化后的日期字符串元组
    """
    _logger = logger or logging.getLogger(__name__)

    # 处理开始日期
    if not start_date:
        start_date = default_start_date
        _logger.info(f"任务 {task_name}: 未提供 start_date，使用默认起始日期: {start_date}")

    # 处理结束日期
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
        _logger.info(f"任务 {task_name}: 未提供 end_date，使用当前日期: {end_date}")

    return start_date, end_date

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
    生成基于交易日的日期批次，批次参数固定为 'start_date' 和 'end_date'。

    参数:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        batch_size: 每个批次包含的交易日数量
        ts_code: 可选的股票代码，如果提供则会添加到每个批次参数中
        exchange: 交易所代码，默认为'SSE'（上交所）
        additional_params: 可选的附加参数字典，将合并到每个批次中
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次是包含 'start_date' 和 'end_date' 的字典
    """
    from ...tools.calendar import get_trade_days_between

    _logger = logger or logging.getLogger(__name__)
    _logger.info(f"生成交易日批次: {start_date} 到 {end_date}, 批次大小: {batch_size}")

    try:
        # 1. 定义数据源：获取交易日列表
        async def get_trade_days():
            return await get_trade_days_between(start_date, end_date, exchange=exchange)
        
        trade_days_source = Source.from_callable(get_trade_days)

        # 2. 定义分区策略：按指定数量分批
        partition_strategy = Partition.by_size(batch_size)

        # 3. 定义映射策略：将每个批次映射为开始和结束日期
        # 根据 date_field 参数灵活设置映射目标
        map_strategy = Map.to_date_range("start_date", "end_date")

        # 4. 创建 BatchPlanner 实例
        planner = BatchPlanner(
            source=trade_days_source,
            partition_strategy=partition_strategy,
            map_strategy=map_strategy
        )

        # 5. 生成批次列表，并添加可选的 ts_code 参数和 additional_params
        final_additional_params = (additional_params or {}).copy()
        if ts_code:
            final_additional_params["ts_code"] = ts_code

        batch_list = await planner.generate(additional_params=final_additional_params)

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
    生成基于自然日的日期批次，批次参数固定为 'start_date' 和 'end_date'。

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
    _logger = logger or logging.getLogger(__name__)
    _logger.info(f"生成自然日批次: {start_date} 到 {end_date}, 批次大小: {batch_size}")

    try:
        # 1. 定义数据源：生成自然日期列表
        def generate_date_range(start, end):
            return [d.strftime(date_format) for d in pd.date_range(start=pd.to_datetime(start), end=pd.to_datetime(end), freq="D")]

        async def get_natural_days_callable():
            return generate_date_range(start_date, end_date)
        
        natural_days_source = Source.from_callable(get_natural_days_callable)

        # 2. 定义分区策略：按指定天数分批
        partition_strategy = Partition.by_size(batch_size)

        # 3. 定义映射策略：将每个批次映射为开始和结束日期
        map_strategy = Map.to_date_range("start_date", "end_date")

        # 4. 创建 BatchPlanner 实例
        planner = BatchPlanner(
            source=natural_days_source,
            partition_strategy=partition_strategy,
            map_strategy=map_strategy
        )

        # 5. 生成批次列表，并添加可选的 ts_code 参数和 additional_params
        final_additional_params = (additional_params or {}).copy()
        if ts_code:
            final_additional_params["ts_code"] = ts_code

        batch_list = await planner.generate(additional_params=final_additional_params)

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
    date_field: str = "end_date",  # 默认映射到 end_date
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
        date_field: 日期字段的名称，用于映射到批次参数中，默认为 'end_date'

    返回:
        批次参数列表，每个批次是包含'end_date'和'quarter'的字典
    """
    _logger = logger or logging.getLogger(__name__)
    _logger.info(f"生成季度末批次: {start_date} 到 {end_date}")

    try:
        # 1. 定义数据源：获取季度末日期字符串列表
        async def get_quarter_ends_callable() -> List[str]:
            return [
                d.strftime(date_format) 
                for d in pd.date_range(start=start_date, end=end_date, freq="QE")
            ]
        
        quarter_ends_source = Source.from_callable(get_quarter_ends_callable)

        # 2. 定义分区策略：每个季度末一个批次
        partition_strategy = Partition.by_size(1)

        # 3. 定义映射策略：将单个日期批次转换为目标字典格式
        def map_quarter_end(batch: List[str]) -> Dict[str, str]:
            """将包含单个季度末日期的批次转换为参数字典。"""
            if not batch:
                return {}
            q_date = batch[0]
            dt = pd.to_datetime(q_date)
            return {
                date_field: q_date,
                "quarter": f"{dt.year}Q{dt.quarter}"
            }

        map_strategy = Map.with_custom_func(map_quarter_end)

        # 4. 创建 BatchPlanner 实例
        planner = BatchPlanner(
            source=quarter_ends_source,
            partition_strategy=partition_strategy,
            map_strategy=map_strategy
        )

        # 5. 生成批次列表，并添加可选的 ts_code 参数和 additional_params
        final_additional_params = (additional_params or {}).copy()
        if ts_code:
            final_additional_params["ts_code"] = ts_code

        batch_list = await planner.generate(additional_params=final_additional_params)

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
    生成按单个日期的批次，每个批次包含单个交易日。

    参数:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        date_field: 日期字段的名称，用于映射到批次参数中
        ts_code: 可选的股票代码，如果提供则会添加到每个批次参数中
        exchange: 交易所代码，默认为'SSE'（上交所）
        additional_params: 可选的附加参数字典，将合并到每个批次中
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次包含单个日期
    """
    _logger = logger or logging.getLogger(__name__)
    _logger.info(f"生成单日期批次: {start_date} 到 {end_date}")

    try:
        # 1. 定义数据源：获取交易日列表 (每个日期一个批次，无需分区)
        from ...tools.calendar import get_trade_days_between

        async def get_trade_days_callable():
            return await get_trade_days_between(start_date, end_date, exchange=exchange)

        trade_days_source = Source.from_callable(get_trade_days_callable)

        # 2. 定义分区策略：每个交易日一个批次 (已在Source中隐含)
        partition_strategy = Partition.by_size(1)

        # 3. 定义映射策略：将单个日期映射到指定日期字段参数
        map_strategy = Map.to_dict(date_field)

        # 4. 创建 BatchPlanner 实例
        planner = BatchPlanner(
            source=trade_days_source,
            partition_strategy=partition_strategy,
            map_strategy=map_strategy
        )

        # 5. 生成批次列表，并添加可选的 ts_code 参数和 additional_params
        final_additional_params = (additional_params or {}).copy()
        if ts_code:
            final_additional_params["ts_code"] = ts_code

        batch_list = await planner.generate(additional_params=final_additional_params)

        _logger.info(f"成功生成 {len(batch_list)} 个单日期批次")
        return batch_list

    except Exception as e:
        _logger.error(f"生成单日期批次时出错: {e}", exc_info=True)
        raise RuntimeError(f"生成单日期批-次失败: {e}") from e


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
    生成基于月份的日期批次，批次参数固定为 'start_m' 和 'end_m'。

    参数:
        start_m: 开始月份字符串（YYYYMM格式）
        end_m: 结束月份字符串（YYYYMM格式）
        batch_size: 每个批次包含的月份数量
        ts_code: 可选的股票代码，如果提供则会添加到每个批次参数中
        date_format: 日期格式字符串，默认为'%Y%m'
        additional_params: 可选的附加参数字典，将合并到每个批次中
        logger: 可选的日志记录器

    返回:
        批次参数列表，每个批次是包含'start_m'和'end_m'的字典
    """
    _logger = logger or logging.getLogger(__name__)
    _logger.info(f"生成月份批次: {start_m} 到 {end_m}, 批次大小: {batch_size}")

    try:
        # 1. 定义数据源：生成月份列表
        def generate_month_range(start, end):
            return [d.strftime(date_format) for d in pd.date_range(start=f"{start}01", end=f"{end}01", freq="MS")]

        async def get_month_range_callable():
            return generate_month_range(start_m, end_m)
        
        month_range_source = Source.from_callable(get_month_range_callable)

        # 2. 定义分区策略：按指定月份数分批
        partition_strategy = Partition.by_size(batch_size)

        # 3. 定义映射策略：将每个批次映射为开始和结束月份
        map_strategy = Map.to_date_range("start_m", "end_m")

        # 4. 创建 BatchPlanner 实例
        planner = BatchPlanner(
            source=month_range_source,
            partition_strategy=partition_strategy,
            map_strategy=map_strategy
        )

        # 5. 生成批次列表，并添加可选的 ts_code 参数和 additional_params
        final_additional_params = (additional_params or {}).copy()
        if ts_code:
            final_additional_params["ts_code"] = ts_code

        batch_list = await planner.generate(additional_params=final_additional_params)

        _logger.info(f"成功生成 {len(batch_list)} 个月份批次")
        return batch_list

    except Exception as e:
        _logger.error(f"生成月份批次时出错: {e}", exc_info=True)
        raise RuntimeError(f"生成月份批次失败: {e}") from e


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
    _logger = logger or logging.getLogger(__name__)
    _logger.info(f"开始生成单股票代码批次 - 表名: {table_name}")

    try:
        # 1. 定义数据源：从多个来源获取股票代码列表
        async def get_stock_codes_callable():
            return await _get_stock_codes_from_sources(
                db_connection=db_connection,
                table_name=table_name,
                code_column=code_column,
                filter_condition=filter_condition,
                api_instance=api_instance,
                logger=_logger,
            )
        
        stock_codes_source = Source.from_callable(get_stock_codes_callable)

        # 2. 定义分区策略：每个股票代码一个批次
        partition_strategy = Partition.by_size(1)

        # 3. 定义映射策略：将单个股票代码映射到参数字典
        map_strategy = Map.to_dict(code_column)

        # 4. 创建 BatchPlanner 实例
        planner = BatchPlanner(
            source=stock_codes_source,
            partition_strategy=partition_strategy,
            map_strategy=map_strategy
        )

        # 5. 生成批次列表，并添加 additional_params
        final_additional_params = (additional_params or {}).copy()

        batch_list = await planner.generate(additional_params=final_additional_params)

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


# =============================================================================
# 财务数据专用批次生成函数
# =============================================================================

async def generate_financial_data_batches(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    default_start_date: str = "19901231",
    batch_size: int = 365,
    logger: Optional[logging.Logger] = None,
    task_name: str = "financial_task"
) -> List[Dict[str, Any]]:
    """
    财务数据专用的批次生成函数，标准化所有财务任务的批次生成逻辑

    Args:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        ts_code: 可选的股票代码
        default_start_date: 默认起始日期
        batch_size: 批次大小（天数），默认365天
        logger: 可选的日志记录器
        task_name: 任务名称，用于日志记录

    Returns:
        List[Dict]: 批处理参数列表
    """
    _logger = logger or logging.getLogger(__name__)

    # 使用标准化的日期范围处理
    start_date, end_date = normalize_date_range(
        start_date=start_date,
        end_date=end_date,
        default_start_date=default_start_date,
        logger=_logger,
        task_name=task_name
    )

    _logger.info(
        f"任务 {task_name}: 使用自然日批次工具生成批处理列表，范围: {start_date} 到 {end_date}，批次大小: {batch_size} 天"
    )

    # 使用现有的自然日批次生成函数
    return await generate_natural_day_batches(
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size,
        ts_code=ts_code,
        logger=_logger,
    )

async def generate_financial_report_batches(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    default_start_date: str = "19901231",
    batch_size: int = 365,
    logger: Optional[logging.Logger] = None,
    task_name: str = "financial_task"
) -> List[Dict[str, Any]]:
    """
    财务数据专用的批次生成函数，标准化所有财务任务的批次生成逻辑

    Args:
        start_date: 开始日期字符串（YYYYMMDD格式）
        end_date: 结束日期字符串（YYYYMMDD格式）
        ts_code: 可选的股票代码
        default_start_date: 默认起始日期
        batch_size: 批次大小（天数），默认365天
        logger: 可选的日志记录器
        task_name: 任务名称，用于日志记录

    Returns:
        List[Dict]: 批处理参数列表
    """
    _logger = logger or logging.getLogger(__name__)

    # 使用标准化的日期范围处理
    start_date, end_date = normalize_date_range(
        start_date=start_date,
        end_date=end_date,
        default_start_date=default_start_date,
        logger=_logger,
        task_name=task_name
    )

    _logger.info(
        f"任务 {task_name}: 使用自然日批次工具生成批处理列表，范围: {start_date} 到 {end_date}，批次大小: {batch_size} 天"
    )

    # 财务数据接口通常使用'ann_date'作为日期参数，但更通用的API参数是'start_date'和'end_date'
    # 此处强制指定date_field为'date'，以生成正确的'start_date'和'end_date'参数
    return await generate_natural_day_batches(
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size,
        ts_code=ts_code,
        logger=_logger,
    )