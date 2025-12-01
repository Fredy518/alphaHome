#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
同花顺涨停原因扩展接口（AkShare 风格）

数据来源:
    列表: http://zx.10jqka.com.cn/event/api/getharden/date/{YYYY-MM-DD}/orderby/date/orderway/desc/charset/GBK/
    详情: http://zx.10jqka.com.cn/event/harden/stockreason/id/{id}

提供一个统一函数:
    stock_limitup_reason(date, fetch_detail=True) -> pd.DataFrame

返回字段采用相对标准的英文列名，方便后续映射入库:
    - trade_date: 交易日期 (date)
    - ts_code:    股票代码 (str)
    - name:       股票名称 (str)
    - reason:     涨停原因标签 (str)，如 "重整受理+无偿捐赠+精细化工"
    - reason_detail: 涨停详细原因 (str)，从详情页获取的完整描述

注意: 接口仅在交易日有数据，非交易日返回空 DataFrame。
"""

from __future__ import annotations

import datetime as _dt
import logging
import re
import time
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# 默认重试配置
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 5.0  # 秒，增大初始重试延迟
DEFAULT_RETRY_BACKOFF = 2.0  # 指数退避因子
LIST_REQUEST_INTERVAL = 3.0  # 每次列表请求前的节流时间（秒）
DETAIL_REQUEST_INTERVAL = 1.0  # 每次详情请求前的节流时间（秒）
FORBIDDEN_COOLDOWN = 60.0  # 遇到 403 封禁后的冷却时间（秒）


def _normalize_date(date: Union[str, _dt.date, _dt.datetime]) -> str:
    """
    将输入日期标准化为同花顺接口要求的 YYYY-MM-DD 字符串。
    """
    if isinstance(date, _dt.datetime):
        return date.date().strftime("%Y-%m-%d")
    if isinstance(date, _dt.date):
        return date.strftime("%Y-%m-%d")

    # 字符串情况
    text = str(date).strip()
    if not text:
        raise ValueError("date 参数不能为空")

    # 支持 YYYYMMDD / YYYY-MM-DD / 其他 pandas 可解析格式
    if re.fullmatch(r"\d{8}", text):
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"

    # 尝试用 pandas 解析
    try:
        dt = pd.to_datetime(text)
        return dt.strftime("%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"无法解析日期参数: {date!r}, 错误: {e}") from e


def _fetch_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 15,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    retry_backoff: float = DEFAULT_RETRY_BACKOFF,
) -> Dict[str, Any]:
    """
    GET 请求并解析 JSON，带重试机制。
    
    Args:
        url: 请求 URL
        params: 请求参数
        timeout: 超时时间（秒）
        max_retries: 最大重试次数
        retry_delay: 初始重试延迟（秒）
        retry_backoff: 重试延迟的指数退避因子
    
    Returns:
        解析后的 JSON 字典
        
    Raises:
        requests.RequestException: 所有重试失败后抛出
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "http://zx.10jqka.com.cn/",
    }
    
    last_error: Optional[Exception] = None
    delay = retry_delay
    
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            last_error = e
            # 403 表示被封禁，需要长时间冷却
            if e.response is not None and e.response.status_code == 403:
                logger.warning(f"IP 被封禁 (403)，冷却 {FORBIDDEN_COOLDOWN}s: {url}")
                time.sleep(FORBIDDEN_COOLDOWN)
                delay = retry_delay  # 重置延迟
            elif attempt < max_retries:
                logger.debug(f"请求失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}, {delay:.1f}s 后重试...")
                time.sleep(delay)
                delay *= retry_backoff
            else:
                logger.warning(f"请求失败，已达最大重试次数: {url}")
        except (requests.RequestException, ValueError) as e:
            last_error = e
            if attempt < max_retries:
                logger.debug(f"请求失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}, {delay:.1f}s 后重试...")
                time.sleep(delay)
                delay *= retry_backoff
            else:
                logger.warning(f"请求失败，已达最大重试次数: {url}")
    
    raise last_error  # type: ignore


def _fetch_text(
    url: str,
    timeout: int = 10,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    retry_backoff: float = DEFAULT_RETRY_BACKOFF,
) -> str:
    """
    GET 请求并返回文本内容，带重试机制。
    
    Args:
        url: 请求 URL
        timeout: 超时时间（秒）
        max_retries: 最大重试次数
        retry_delay: 初始重试延迟（秒）
        retry_backoff: 重试延迟的指数退避因子
    
    Returns:
        响应文本
        
    Raises:
        requests.RequestException: 所有重试失败后抛出
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "http://zx.10jqka.com.cn/",
    }
    
    last_error: Optional[Exception] = None
    delay = retry_delay
    
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except requests.HTTPError as e:
            last_error = e
            # 403 表示被封禁，需要长时间冷却
            if e.response is not None and e.response.status_code == 403:
                logger.warning(f"IP 被封禁 (403)，冷却 {FORBIDDEN_COOLDOWN}s: {url}")
                time.sleep(FORBIDDEN_COOLDOWN)
                delay = retry_delay
            elif attempt < max_retries:
                logger.debug(f"请求失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}, {delay:.1f}s 后重试...")
                time.sleep(delay)
                delay *= retry_backoff
            else:
                logger.warning(f"请求失败，已达最大重试次数: {url}")
        except requests.RequestException as e:
            last_error = e
            if attempt < max_retries:
                logger.debug(f"请求失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}, {delay:.1f}s 后重试...")
                time.sleep(delay)
                delay *= retry_backoff
            else:
                logger.warning(f"请求失败，已达最大重试次数: {url}")
    
    raise last_error  # type: ignore


def _fetch_limitup_detail(
    event_id: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> str:
    """
    拉取单条涨停记录的详细原因文本，带重试机制。
    
    详情页 URL: http://zx.10jqka.com.cn/event/harden/stockreason/id/{event_id}
    页面中的 var data = '...' 包含详细原因描述。
    
    Args:
        event_id: 涨停记录 ID
        max_retries: 最大重试次数
        retry_delay: 初始重试延迟（秒）
    
    Returns:
        详细原因文本，失败返回空字符串
    """
    url = f"http://zx.10jqka.com.cn/event/harden/stockreason/id/{event_id}"
    try:
        time.sleep(DETAIL_REQUEST_INTERVAL)
        html = _fetch_text(url, max_retries=max_retries, retry_delay=retry_delay)
        # 从 HTML 中提取 var data = '...' 内容
        pattern = re.search(r"var\s+data\s*=\s*'(.*?)';", html, flags=re.S)
        if not pattern:
            return ""
        
        raw = pattern.group(1)
        
        # 清理 HTML 转义和标签
        text = raw.replace("&lt;spanclass=&quot;hl&quot;&gt;", "")
        text = text.replace("&lt;/span&gt;", "")
        text = text.replace("&amp;quot;", '"')
        text = text.replace("&lt;br/&gt;", "\n")
        text = text.replace("&lt;br /&gt;", "\n")
        text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
        
        return text.strip()
    except Exception as e:
        logger.debug(f"获取涨停详情失败 id={event_id}: {e}")
        return ""


def stock_limitup_reason(
    date: Union[str, _dt.date, _dt.datetime],
    fetch_detail: bool = True
) -> pd.DataFrame:
    """
    同花顺涨停原因列表。

    Args:
        date: 交易日期，支持 YYYYMMDD / YYYY-MM-DD / date / datetime
        fetch_detail: 是否获取详细原因（需要额外请求每条记录的详情页，默认 True）

    Returns:
        DataFrame，列包含:
            - trade_date: 交易日期 (date)
            - ts_code: 股票代码 (str)
            - name: 股票名称 (str)
            - reason: 涨停原因标签 (str)
            - reason_detail: 涨停详细原因 (str)，仅当 fetch_detail=True 时有值

        如果当日无数据（非交易日或无涨停），返回空 DataFrame。
    """
    trade_date_str = _normalize_date(date)

    url = (
        f"http://zx.10jqka.com.cn/event/api/getharden/"
        f"date/{trade_date_str}/orderby/date/orderway/desc/charset/GBK/"
    )

    # 同花顺对频繁请求较敏感，这里添加固定延迟降低访问速率
    time.sleep(LIST_REQUEST_INTERVAL)

    try:
        data_json = _fetch_json(url)
    except requests.RequestException as e:
        logger.warning(f"请求涨停数据失败 date={trade_date_str}: {e}")
        return pd.DataFrame()

    raw_data: List[Dict[str, Any]] = data_json.get("data") or []
    if not raw_data:
        return pd.DataFrame()

    # 接口返回的是字典列表，字段名为英文
    # 典型字段: id, name, code, reason, date, close, zhangdie, zhangfu, 
    #          huanshou, chengjiaoe, chengjiaoliang, ddejingliang, market
    df = pd.DataFrame(raw_data)

    # 检查必要字段
    if "code" not in df.columns:
        logger.warning(f"涨停数据缺少 code 字段: {trade_date_str}")
        return pd.DataFrame()

    # 构建结果 DataFrame，统一字段名
    # 注意：必须先赋值有数据的列，再赋值标量，否则 DataFrame 为空
    result = pd.DataFrame()
    
    # 股票代码（先赋值，确定行数）
    result["ts_code"] = df["code"].astype(str)
    
    # 交易日期（标量广播到所有行）
    result["trade_date"] = pd.to_datetime(trade_date_str).date()

    # 股票名称
    result["name"] = df.get("name", "")

    # 涨停原因标签（接口直接返回的简短标签）
    result["reason"] = df.get("reason", "")

    # 涨停详细原因（需要单独请求详情页）
    if fetch_detail and "id" in df.columns:
        logger.debug(f"获取 {len(df)} 条涨停记录的详细原因...")
        result["reason_detail"] = df["id"].astype(str).apply(_fetch_limitup_detail)
    else:
        result["reason_detail"] = ""

    # 清理代码为空的行
    result = result[result["ts_code"].notna() & (result["ts_code"] != "")]

    result.reset_index(drop=True, inplace=True)
    
    logger.debug(f"涨停数据 {trade_date_str}: {len(result)} 条")
    return result


__all__ = ["stock_limitup_reason"]
