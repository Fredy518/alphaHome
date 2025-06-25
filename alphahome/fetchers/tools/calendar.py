import calendar as std_calendar
import datetime
import inspect
import json
import logging
import os
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import asyncpg
import pandas as pd

logger = logging.getLogger(__name__)

# 全局交易日历数据缓存
_TRADE_CAL_CACHE: Dict[Tuple[str, str, str], pd.DataFrame] = {}
_DB_POOL: Optional[asyncpg.Pool] = None


def _normalize_date_to_yyyymmdd(
    date: Union[str, datetime.datetime, datetime.date]
) -> Optional[str]:
    """将多种格式的日期输入标准化为 'YYYYMMDD' 格式的字符串。

    支持的格式:
    - datetime.datetime 或 datetime.date 对象
    - 'YYYYMMDD'
    - 'YYYY-MM-DD'
    - 'YYYY-M-D' (例如 '2023-5-1')
    - 'YYYY/MM/DD'
    - 'YYYY/M/D'

    Args:
        date: 日期输入。

    Returns:
        Optional[str]: 'YYYYMMDD' 格式的日期字符串，如果无法转换则返回 None。
    """
    if isinstance(date, (datetime.datetime, datetime.date)):
        return date.strftime("%Y%m%d")
    if isinstance(date, str):
        # 优先处理 YYYYMMDD 格式，避免不必要的解析
        if len(date) == 8 and date.isdigit():
            return date
        # 尝试解析多种带分隔符的格式
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.datetime.strptime(date, fmt).strftime("%Y%m%d")
            except ValueError:
                continue
    # 如果所有尝试都失败
    logger.warning(f"无法将日期 '{date}' 标准化为 YYYYMMDD 格式。")
    return None


# 新的辅助函数：加载数据库配置
def _load_db_config() -> Optional[Dict[str, Any]]:
    """从用户 AppData 或项目示例文件加载数据库配置。

    优先尝试从 'LOCALAPPDATA/trademaster/alphahome/config.json' 加载。
    如果失败，则回退到项目根目录的 'config.example.json'。
    """
    user_config_path_parts = []
    local_app_data = os.getenv("LOCALAPPDATA")

    if local_app_data:
        user_config_path_parts = [
            local_app_data,
            "trademaster",
            "alphahome",
            "config.json",
        ]
        user_config_path = os.path.join(*user_config_path_parts)
        logger.info(
            f"_load_db_config: 尝试读取用户特定配置文件: {os.path.abspath(user_config_path)}"
        )
        if os.path.exists(user_config_path):
            actual_path = user_config_path
            logger.info(
                f"_load_db_config: 实际读取的用户配置文件路径: {os.path.abspath(actual_path)}"
            )
            try:
                with open(actual_path, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
                db_config = config_data.get("database")
                logger.info(
                    f"_load_db_config: 从 {os.path.abspath(actual_path)} 加载的 database 配置: {db_config}"
                )
                if db_config and isinstance(db_config, dict) and db_config.get("url"):
                    return db_config
                else:
                    logger.warning(
                        f"用户配置文件 {os.path.abspath(actual_path)} 中缺少 'database' 部分或 'url'。"
                    )
            except Exception as e:
                logger.error(
                    f"加载或解析用户配置文件 {os.path.abspath(actual_path)} 时出错: {e}"
                )
        else:
            logger.info(
                f"_load_db_config: 用户特定配置文件未找到: {os.path.abspath(user_config_path)}"
            )
    else:
        logger.warning(
            "_load_db_config: LOCALAPPDATA 环境变量未设置，无法定位用户特定配置文件。"
        )

    # 如果用户特定配置加载失败或未找到，则回退到项目根目录的 example config
    logger.info("_load_db_config: 回退到尝试加载项目根目录的 config.example.json。")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 项目根目录相对于 alphahome/fetchers/tools/ 是 ../../../
    example_config_file_path = os.path.join(
        current_dir, "..", "..", "..", "config.example.json"
    )

    logger.info(
        f"_load_db_config: 尝试读取示例配置文件: {os.path.abspath(example_config_file_path)}"
    )
    actual_path = example_config_file_path  # 现在 actual_path 指向 example config

    if os.path.exists(actual_path):
        logger.info(
            f"_load_db_config: 实际读取的示例配置文件路径: {os.path.abspath(actual_path)}"
        )
        try:
            with open(actual_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
            db_config = config_data.get("database")
            logger.info(
                f"_load_db_config: 从 {os.path.abspath(actual_path)} 加载的 database 配置 (回退): {db_config}"
            )
            if db_config and isinstance(db_config, dict) and db_config.get("url"):
                return db_config
            else:
                logger.error(
                    f"回退配置文件 {os.path.abspath(actual_path)} 中缺少 'database' 部分或 'url'。"
                )
                return None
        except Exception as e:
            logger.error(
                f"加载或解析回退配置文件 {os.path.abspath(actual_path)} 时出错: {e}"
            )
            return None
    else:
        logger.error(f"数据库回退配置文件也未找到：{os.path.abspath(actual_path)}")
        return None


# 新的辅助函数：获取数据库连接池
async def _get_db_pool() -> Optional[asyncpg.Pool]:
    """获取或初始化数据库连接池。"""
    global _DB_POOL
    if _DB_POOL is None or _DB_POOL._closed:  # 检查连接池是否需要初始化或已关闭
        db_settings = _load_db_config()
        if not db_settings:
            logger.error("无法初始化数据库连接池：加载数据库设置失败。")
            return None

        db_url = db_settings.get("url")
        if not db_url:
            logger.error("无法初始化数据库连接池：在设置中未找到数据库URL。")
            return None

        try:
            # 如果URL中存在密码，在日志中进行掩码处理以确保安全
            log_url = db_url
            if "@" in db_url and "://" in db_url:
                protocol_part, rest_part = db_url.split("://", 1)
                if ":" in rest_part.split("@", 1)[0]:  # 格式: user:pass@host
                    user_pass, host_part = rest_part.split("@", 1)
                    user, _ = user_pass.split(":", 1)
                    log_url = f"{protocol_part}://{user}:********@{host_part}"

            logger.info(f"尝试使用URL创建数据库连接池: {log_url}")
            logger.info(f"即将使用以下DSN创建连接池 (详情见掩码后URL): {log_url}")
            _DB_POOL = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5)
            logger.info("数据库连接池创建成功。")
        except Exception as e:
            logger.error(f"创建数据库连接池失败: {e}")
            _DB_POOL = None  # 如果创建失败，确保 _DB_POOL 为 None
    return _DB_POOL


async def get_trade_cal(
    start_date: str = None, end_date: str = None, exchange: str = "SSE"
) -> pd.DataFrame:
    """获取交易日历数据 (从数据库)

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
    if exchange == "":
        logger.warning(
            f"函数 {__name__}.{inspect.currentframe().f_code.co_name} 收到空的 'exchange' 参数，已自动修正为 'SSE'。"
        )
        exchange = "SSE"

    logger.info(
        f"_get_trade_cal: Called with exchange='{exchange}', start_date='{start_date}', end_date='{end_date}'"
    )

    if not start_date:
        start_date = "20000101"
    if not end_date:
        end_date_dt = datetime.datetime.now() + timedelta(days=365)
        end_date = end_date_dt.strftime("%Y%m%d")

    upper_exchange = exchange.upper()
    db_exchange_code = "HKEX" if upper_exchange in ("HK", "HKEX") else upper_exchange
    logger.info(
        f"_get_trade_cal: Calculated exchange codes: input_exchange='{exchange}', upper_exchange='{upper_exchange}', db_exchange_code='{db_exchange_code}'"
    )

    cache_key = (start_date, end_date, upper_exchange)
    if cache_key in _TRADE_CAL_CACHE:
        logger.debug(f"交易日历缓存命中: {cache_key}")
        return _TRADE_CAL_CACHE[cache_key].copy()

    logger.debug(f"交易日历缓存未命中，尝试从数据库获取: {cache_key}")

    pool = await _get_db_pool()
    if not pool:
        logger.error("数据库连接池不可用，无法获取交易日历。")
        return pd.DataFrame()

    sql_query = '''
    SELECT exchange, cal_date, is_open, pretrade_date
    FROM "tushare"."others_calendar"
    WHERE exchange = $1 
      AND cal_date >= TO_DATE($2, 'YYYYMMDD')
      AND cal_date <= TO_DATE($3, 'YYYYMMDD')
    ORDER BY cal_date ASC;
    '''

    final_df = pd.DataFrame()

    try:
        async with pool.acquire() as conn:
            records = await conn.fetch(
                sql_query, db_exchange_code, start_date, end_date
            )

        logger.info(
            f"_get_trade_cal: Database raw records count for {db_exchange_code} ({start_date}-{end_date}): {len(records) if records else 'None'}"
        )
        if records:
            logger.debug(
                f"_get_trade_cal: First raw record (sample): {dict(records[0]) if records else 'N/A'}"
            )

        if records:
            # final_df = pd.DataFrame(records, columns=['exchange', 'cal_date', 'is_open', 'pretrade_date'])
            # 更改为更稳健地处理字典列表或类Record对象列表
            final_df = pd.DataFrame([dict(r) for r in records])
            if not final_df.empty:
                # 确保正确的列顺序和存在性，即使 dict(r) 有更多/更少或不同的顺序
                expected_columns = ["exchange", "cal_date", "is_open", "pretrade_date"]
                # 添加缺失的列并用 None 填充，然后重新排序
                for col in expected_columns:
                    if col not in final_df.columns:
                        final_df[col] = None
                final_df = final_df[expected_columns]

                for col_name in ["cal_date", "pretrade_date"]:
                    if col_name in final_df.columns:
                        final_df[col_name] = final_df[col_name].apply(
                            lambda x: (
                                x.strftime("%Y%m%d")
                                if pd.notnull(x)
                                and isinstance(x, (datetime.date, datetime.datetime))
                                else None
                            )
                        )
                if "is_open" in final_df.columns:  # 确保 is_open 是 Int64 类型以支持 NA
                    final_df["is_open"] = pd.to_numeric(
                        final_df["is_open"], errors="coerce"
                    ).astype("Int64")
            logger.info(
                f"从数据库获取并处理了 {len(final_df)} 条 {db_exchange_code} 交易日历数据 ({start_date}-{end_date})。"
            )
        else:
            logger.info(
                f"数据库未返回 {db_exchange_code} 交易日历数据 ({start_date}-{end_date})。"
            )
            # final_df 已经是一个空的 DataFrame

    except Exception as e:
        logger.error(
            f"从数据库查询交易日历失败 (exchange: {db_exchange_code}, range: {start_date}-{end_date}): {e}"
        )
        # 如果在赋值前发生错误，final_df 已经是一个空的 DataFrame 或将被设置

    _TRADE_CAL_CACHE[cache_key] = final_df.copy()
    return final_df.copy()


async def is_trade_day(
    date: Union[str, datetime.datetime, datetime.date], exchange: str = "SSE"
) -> bool:
    """检查指定日期是否为交易日。

    Args:
        date (Union[str, datetime.datetime, datetime.date]): 要检查的日期 (格式 YYYYMMDD 或兼容格式)。
        exchange (str, optional): 交易所代码，默认为 'SSE'。

    Returns:
        bool: 如果是交易日则返回 True，否则返回 False。
    """
    date_str = _normalize_date_to_yyyymmdd(date)
    if not date_str:
        logger.error(f"is_trade_day 收到无效日期格式: {date}")
        return False

    # 优化：直接使用 get_trade_cal 并检查结果
    # 这样可以利用缓存，并且只查询一次数据库
    df = await get_trade_cal(start_date=date_str, end_date=date_str, exchange=exchange)

    if not df.empty:
        # 确保查询结果包含当天
        day_info = df[df["cal_date"] == date_str]
        if not day_info.empty:
            return day_info.iloc[0]["is_open"] == 1

    logger.warning(f"无法获取日期 {date_str} 的交易日历信息。")
    return False


async def get_last_trade_day(
    date: Union[str, datetime.datetime, datetime.date] = None,
    n: int = 1,
    exchange: str = "SSE",
) -> Optional[str]:
    """获取指定日期（或当前日期）之前的第 n 个交易日。

    Args:
        date (Union[str, datetime.datetime, datetime.date], optional):
            基准日期 (格式 YYYYMMDD 或兼容格式)。如果为 None，则使用当前日期。
        n (int, optional): 向前推移的交易日数。默认为 1。
        exchange (str, optional): 交易所代码，默认为 'SSE'。

    Returns:
        Optional[str]: YYYYMMDD 格式的交易日字符串，如果找不到则返回 None。
    """
    if n <= 0:
        logger.warning("get_last_trade_day 的 n 必须为正整数。")
        return None

    base_date_str = _normalize_date_to_yyyymmdd(date or datetime.date.today())
    if not base_date_str:
        logger.error(f"get_last_trade_day 收到无效日期格式: {date}")
        return None

    # 为了安全起见，向前查询一段较长的时间范围
    # 估算查询范围：n个交易日大约需要 n * 1.5 个日历日
    start_date_dt = datetime.datetime.strptime(
        base_date_str, "%Y%m%d"
    ) - timedelta(days=int(n * 1.5) + 15)
    start_date_str_query = start_date_dt.strftime("%Y%m%d")

    df = await get_trade_cal(
        start_date=start_date_str_query, end_date=base_date_str, exchange=exchange
    )

    if df.empty:
        logger.warning(
            f"在 {start_date_str_query} 和 {base_date_str} 之间未找到交易日历数据。"
        )
        return None

    # 筛选出实际的交易日
    trade_days = df[(df["is_open"] == 1) & (df["cal_date"] < base_date_str)][
        "cal_date"
    ].sort_values(ascending=False)

    if len(trade_days) >= n:
        return trade_days.iloc[n - 1]
    else:
        logger.warning(
            f"在日期 {base_date_str} 之前没有足够的 {n} 个交易日 (只找到 {len(trade_days)} 个)。"
        )
        return None


async def get_next_trade_day(
    date: Union[str, datetime.datetime, datetime.date] = None,
    n: int = 1,
    exchange: str = "SSE",
) -> Optional[str]:
    """获取指定日期（或当前日期）之后的第 n 个交易日。

    Args:
        date (Union[str, datetime.datetime, datetime.date], optional):
            基准日期 (格式 YYYYMMDD 或兼容格式)。如果为 None，则使用当前日期。
        n (int, optional): 向后推移的交易日数。默认为 1。
        exchange (str, optional): 交易所代码，默认为 'SSE'。

    Returns:
        Optional[str]: YYYYMMDD 格式的交易日字符串，如果找不到则返回 None。
    """
    if n <= 0:
        logger.warning("get_next_trade_day 的 n 必须为正整数。")
        return None

    base_date_str = _normalize_date_to_yyyymmdd(date or datetime.date.today())
    if not base_date_str:
        logger.error(f"get_next_trade_day 收到无效日期格式: {date}")
        return None

    # 为了安全起见，向后查询一段较长的时间范围
    end_date_dt = datetime.datetime.strptime(base_date_str, "%Y%m%d") + timedelta(
        days=int(n * 1.5) + 15
    )
    end_date_str_query = end_date_dt.strftime("%Y%m%d")

    df = await get_trade_cal(
        start_date=base_date_str, end_date=end_date_str_query, exchange=exchange
    )

    if df.empty:
        logger.warning(
            f"在 {base_date_str} 和 {end_date_str_query} 之间未找到交易日历数据。"
        )
        return None

    # 筛选出实际的交易日
    trade_days = df[(df["is_open"] == 1) & (df["cal_date"] > base_date_str)][
        "cal_date"
    ].sort_values(ascending=True)

    if len(trade_days) >= n:
        return trade_days.iloc[n - 1]
    else:
        logger.warning(
            f"在日期 {base_date_str} 之后没有足够的 {n} 个交易日 (只找到 {len(trade_days)} 个)。"
        )
        return None


async def get_trade_days_between(
    start_date: Union[str, datetime.datetime, datetime.date],
    end_date: Union[str, datetime.datetime, datetime.date],
    exchange: str = "SSE",
) -> List[str]:
    """获取两个日期之间的所有交易日列表。

    Args:
        start_date (Union[str, datetime.datetime, datetime.date]): 开始日期 (含)，格式 YYYYMMDD 或兼容格式。
        end_date (Union[str, datetime.datetime, datetime.date]): 结束日期 (含)，格式 YYYYMMDD 或兼容格式。
        exchange (str, optional): 交易所代码，默认为 'SSE'。

    Returns:
        List[str]: 交易日列表 (格式 YYYYMMDD)。
    """
    start_date_str = _normalize_date_to_yyyymmdd(start_date)
    end_date_str = _normalize_date_to_yyyymmdd(end_date)

    if not start_date_str or not end_date_str:
        logger.error(
            f"get_trade_days_between 的日期格式无效或无法转换: {start_date} 或 {end_date}."
        )
        return []

    df = await get_trade_cal(
        start_date=start_date_str, end_date=end_date_str, exchange=exchange
    )
    if not df.empty and "is_open" in df.columns:
        # 筛选出 is_open 为 1 的交易日
        trade_days_df = df[df["is_open"] == 1]
        # 返回 cal_date 列表
        return trade_days_df["cal_date"].tolist()
    return []


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """生成指定日期范围内的所有自然日期列表

    Args:
        start_date (str): 开始日期，格式为 YYYYMMDD
        end_date (str): 结束日期，格式为 YYYYMMDD

    Returns:
        List[str]: 日期列表，格式为 YYYYMMDD
    """
    try:
        # 将字符串日期转换为 datetime 对象
        start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")

        # 生成日期范围
        date_range = pd.date_range(start=start_dt, end=end_dt, freq="D")

        # 转换为 YYYYMMDD 格式的字符串列表
        return [d.strftime("%Y%m%d") for d in date_range]

    except Exception as e:
        logger.error(f"生成日期范围失败: start_date={start_date}, end_date={end_date}, error={e}")
        return []


async def get_month_ends(start_date, end_date, re_trade_day=True) -> list:
    """
    获取指定日期区间内的所有月末（自然日或交易日）

    Args:
        start_date: 开始日期
        end_date: 结束日期
        re_trade_day: 是否返回交易日

    Returns:
        list: 月末日期列表
    """
    # 实现获取月末日期的逻辑
    pass
