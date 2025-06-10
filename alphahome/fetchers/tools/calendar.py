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

    sql_query = """
    SELECT exchange, cal_date, is_open, pretrade_date
    FROM tushare_others_calendar
    WHERE exchange = $1 
      AND cal_date >= TO_DATE($2, 'YYYYMMDD')
      AND cal_date <= TO_DATE($3, 'YYYYMMDD')
    ORDER BY cal_date ASC;
    """

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
    """判断指定日期是否为交易日

    Args:
        date (Union[str, datetime.datetime, datetime.date]):
            指定日期，可以是字符串（格式：YYYYMMDD）或datetime对象
        exchange (str, optional): 交易所代码，默认为 'SSE'

    Returns:
        bool: 如果是交易日返回True，否则返回False
    """
    if exchange == "":
        logger.warning(
            f"函数 {__name__}.{inspect.currentframe().f_code.co_name} 收到空的 'exchange' 参数，已自动修正为 'SSE'。"
        )
        exchange = "SSE"

    if isinstance(date, (datetime.datetime, datetime.date)):
        date_str = date.strftime("%Y%m%d")
    else:
        date_str = date

    calendar_df = await get_trade_cal(
        start_date=date_str, end_date=date_str, exchange=exchange
    )

    if calendar_df.empty:
        return False

    # 检查日期是否存在且 is_open 为 1
    day_info = calendar_df[calendar_df["cal_date"] == date_str]
    if not day_info.empty:
        # return day_info['is_open'].iloc[0] == 1
        is_open_val = day_info["is_open"].iloc[0]
        if pd.isna(is_open_val):  # 处理来自 Int64 类型的 pd.NA
            return False
        return bool(is_open_val == 1)  # 比较后显式转换为 bool 类型
    return False


async def get_last_trade_day(
    date: Union[str, datetime.datetime, datetime.date] = None,
    n: int = 1,
    exchange: str = "SSE",
) -> Optional[str]:
    """获取指定日期前n个交易日

    Args:
        date (Union[str, datetime.datetime, datetime.date], optional):
            指定日期，可以是字符串（格式：YYYYMMDD）或datetime对象，默认为当前日期
        n (int, optional): 向前获取的交易日数量，默认为1，即上一个交易日
        exchange (str, optional): 交易所代码，默认为 'SSE'

    Returns:
        Optional[str]: 前n个交易日的日期（格式：YYYYMMDD），如果找不到则返回None
    """
    if exchange == "":
        logger.warning(
            f"函数 {__name__}.{inspect.currentframe().f_code.co_name} 收到空的 'exchange' 参数，已自动修正为 'SSE'。"
        )
        exchange = "SSE"

    if date is None:
        date_obj = datetime.datetime.now().date()
    elif isinstance(date, str):
        try:
            date_obj = datetime.datetime.strptime(date, "%Y%m%d").date()
        except ValueError:
            logger.error(f"get_last_trade_day 的日期格式无效: {date}. 需要 YYYYMMDD.")
            return None
    else:  # datetime.datetime 或 datetime.date 类型
        date_obj = date if isinstance(date, datetime.date) else date.date()

    # 获取包含目标日期及之前一段时间的交易日历
    # 为了找到前n个交易日，可能需要回溯较长时间，这里使用 n * 7 (大约 n 周) + 30 天的缓冲区
    start_search_dt = date_obj - timedelta(days=(n * 7 + 30))
    start_search_str = start_search_dt.strftime("%Y%m%d")
    date_str = date_obj.strftime("%Y%m%d")

    calendar_df = await get_trade_cal(
        start_date=start_search_str, end_date=date_str, exchange=exchange
    )

    if calendar_df.empty:
        logger.warning(
            f"在 {start_search_str} 和 {date_str} 之间未找到 {exchange} 的交易日历数据."
        )
        return None

    if n <= 0:
        logger.warning(f"参数 n 必须为正数以便查找过去的交易日，但收到: {n}")
        return None

    # 获取在 date_str 当天或之前的所有唯一交易日，按日期从新到旧排序。
    trade_days_at_or_before = sorted(
        calendar_df[
            (calendar_df["is_open"] == 1) & (calendar_df["cal_date"] <= date_str)
        ]["cal_date"]
        .unique()
        .tolist(),
        reverse=True,
    )

    if not trade_days_at_or_before:
        logger.warning(
            f"在 {date_str} 或之前未找到 {exchange} 的交易日 (在搜索范围 {start_search_str} 内)."
        )
        return None

    # 判断 date_str 本身是否是列表中最近的交易日
    # 这意味着 date_str 是一个交易日。
    is_input_date_the_head_trade_day = trade_days_at_or_before[0] == date_str

    target_idx = -1
    if is_input_date_the_head_trade_day:
        # 如果 date_str 是一个交易日并且是找到的最新一个,
        # 则前第 n 个交易日在此 (0索引, 降序排列) 列表中的索引为 n。
        # 例如, 列表: ['20230104', '20230103', '20230102'] (date_str='20230104')
        # n=1 (严格的前1个) -> 索引 1 ('20230103')
        # n=2 (严格的前2个) -> 索引 2 ('20230102')
        target_idx = n
    else:
        # 如果 date_str 不是交易日, 或者它不是列表中最近的一个
        # (例如，对于 date_str='20230104'，列表是 ['20230103', '20230102'])
        # 那么 trade_days_at_or_before[0] 已经是严格在 date_str 之前的第一个交易日。
        # 从此算起，前第 n 个交易日的索引是 n-1。
        target_idx = n - 1

    if target_idx >= 0 and target_idx < len(trade_days_at_or_before):
        return trade_days_at_or_before[target_idx]
    else:
        logger.warning(
            f"没有足够的历史交易日来找到 {date_str} 前的满足条件的第 {n} 个交易日 ({exchange}). 可用: {len(trade_days_at_or_before)}, 目标索引: {target_idx}"
        )
        return None


async def get_next_trade_day(
    date: Union[str, datetime.datetime, datetime.date] = None,
    n: int = 1,
    exchange: str = "SSE",
) -> Optional[str]:
    """获取指定日期后n个交易日

    Args:
        date (Union[str, datetime.datetime, datetime.date], optional):
            指定日期，可以是字符串（格式：YYYYMMDD）或datetime对象，默认为当前日期
        n (int, optional): 向后获取的交易日数量，默认为1，即下一个交易日
        exchange (str, optional): 交易所代码，默认为 'SSE'

    Returns:
        Optional[str]: 后n个交易日的日期（格式：YYYYMMDD），如果找不到则返回None
    """
    if exchange == "":
        logger.warning(
            f"函数 {__name__}.{inspect.currentframe().f_code.co_name} 收到空的 'exchange' 参数，已自动修正为 'SSE'。"
        )
        exchange = "SSE"

    if date is None:
        date_obj = datetime.datetime.now().date()
    elif isinstance(date, str):
        try:
            date_obj = datetime.datetime.strptime(date, "%Y%m%d").date()
        except ValueError:
            logger.error(f"get_next_trade_day 的日期格式无效: {date}. 需要 YYYYMMDD.")
            return None
    else:  # datetime.datetime 或 datetime.date 类型
        date_obj = date if isinstance(date, datetime.date) else date.date()

    # 获取包含目标日期及之后一段时间的交易日历
    # 类似于 get_last_trade_day，获取一个缓冲期
    end_search_dt = date_obj + timedelta(days=(n * 7 + 30))
    start_search_str = date_obj.strftime("%Y%m%d")
    end_search_str = end_search_dt.strftime("%Y%m%d")

    calendar_df = await get_trade_cal(
        start_date=start_search_str, end_date=end_search_str, exchange=exchange
    )

    if calendar_df.empty:
        logger.warning(
            f"在 {start_search_str} 和 {end_search_str} 之间未找到 {exchange} 的交易日历数据."
        )
        return None

    if n <= 0:
        logger.warning(f"参数 n 必须为正数以便查找未来的交易日，但收到: {n}")
        return None

    # 筛选出在 date_obj (即 start_search_str) 当天或之后，且 is_open 为 1 的交易日，并升序排序
    trade_days_on_or_after = sorted(
        calendar_df[
            (calendar_df["is_open"] == 1)
            & (
                calendar_df["cal_date"] >= start_search_str
            )  # 修正：这里之前错误地使用了 date_str，应为 start_search_str
        ]["cal_date"]
        .unique()
        .tolist()
    )

    if not trade_days_on_or_after:
        logger.warning(
            f"在 {start_search_str} 或之后未找到 {exchange} 的交易日 (在搜索范围直到 {end_search_str} 内)."
        )
        return None

    # 判断 start_search_str 本身是否是列表中第一个交易日
    is_input_date_the_head_trade_day = trade_days_on_or_after[0] == start_search_str

    target_idx = -1
    if is_input_date_the_head_trade_day:
        # 如果 start_search_str 是一个交易日并且是找到的最早一个 (在它当天或之后),
        # 则严格的后第 n 个交易日在此 (0索引, 升序排列) 列表中的索引为 n。
        # 例如, 列表: ['20230103', '20230104', '20230106'] (start_search_str='20230103')
        # n=1 (严格的后1个) -> 索引 1 ('20230104')
        # n=2 (严格的后2个) -> 索引 2 ('20230106')
        target_idx = n
    else:
        # 如果 start_search_str 不是交易日 (那么 trade_days_on_or_after[0] 严格在 start_search_str 之后)
        # 那么 trade_days_on_or_after[0] 已经是严格在 start_search_str 之后的第一个交易日。
        # 从此算起，后第 n 个交易日的索引是 n-1。
        # 例如 start_search_str='20230105', 列表: ['20230106', '20230109']
        # n=1 -> 索引 0 ('20230106')
        target_idx = n - 1

    if target_idx >= 0 and target_idx < len(trade_days_on_or_after):
        return trade_days_on_or_after[target_idx]
    else:
        logger.warning(
            f"没有足够的未来交易日来找到 {start_search_str} 后的满足条件的第 {n} 个交易日 ({exchange}). 可用: {len(trade_days_on_or_after)}, 目标索引: {target_idx}"
        )
        return None


async def get_trade_days_between(
    start_date: Union[str, datetime.datetime, datetime.date],
    end_date: Union[str, datetime.datetime, datetime.date],
    exchange: str = "SSE",
) -> List[str]:
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
    if exchange == "":
        logger.warning(
            f"函数 {__name__}.{inspect.currentframe().f_code.co_name} 收到空的 'exchange' 参数，已自动修正为 'SSE'。"
        )
        exchange = "SSE"

    if isinstance(start_date, (datetime.datetime, datetime.date)):
        start_date_str = start_date.strftime("%Y%m%d")
    else:
        start_date_str = start_date

    if isinstance(end_date, (datetime.datetime, datetime.date)):
        end_date_str = end_date.strftime("%Y%m%d")
    else:
        end_date_str = end_date

    # 验证日期字符串
    try:
        datetime.datetime.strptime(start_date_str, "%Y%m%d")
        datetime.datetime.strptime(end_date_str, "%Y%m%d")
    except ValueError:
        logger.error(
            f"get_trade_days_between 的日期格式无效: {start_date_str} 或 {end_date_str}. 需要 YYYYMMDD."
        )
        return []

    if start_date_str > end_date_str:
        logger.warning(
            f"开始日期 {start_date_str} 在结束日期 {end_date_str} 之后，将返回空列表。"
        )
        return []

    calendar_df = await get_trade_cal(
        start_date=start_date_str, end_date=end_date_str, exchange=exchange
    )

    if calendar_df.empty:
        return []

    trade_days = calendar_df[
        (calendar_df["is_open"] == 1)
        & (calendar_df["cal_date"] >= start_date_str)
        & (calendar_df["cal_date"] <= end_date_str)
    ]["cal_date"].tolist()

    return sorted(list(set(trade_days)))  # 确保排序和唯一性
