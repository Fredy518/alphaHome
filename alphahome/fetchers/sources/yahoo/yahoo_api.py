#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Yahoo Finance v8 chart API 客户端

直连 Yahoo v8 chart 接口获取日频历史行情，绕过 yfinance 库的限流。
作为 FRED 不可达环境下的备用数据源（DXY/VIX）。

接口：https://query1.finance.yahoo.com/v8/finance/chart/<SYMBOL>?period1=&period2=&interval=1d
返回 JSON：timestamp(unix秒) + indicators.quote[0].{open,high,low,close,volume}
返回 DataFrame 形态对齐 FredAPI：observation_date + <value_col> 两列。
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests


class YahooAPIError(Exception):
    """Yahoo API 调用异常。"""
    pass


class YahooAPI:
    """Yahoo v8 chart API 客户端。"""

    BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    DEFAULT_TIMEOUT = 20
    DEFAULT_MAX_RETRIES = 2
    DEFAULT_RETRY_DELAY = 3
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.timeout = int(timeout)
        self.max_retries = int(max_retries)
        self.retry_delay = float(retry_delay)

    @staticmethod
    def _to_unix(date_str: Optional[str]) -> Optional[int]:
        """将 YYYYMMDD 或 YYYY-MM-DD 转为 unix 时间戳（UTC）。"""
        if not date_str:
            return None
        s = str(date_str).strip()
        if len(s) == 8 and s.isdigit() and "-" not in s:
            s = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
        try:
            dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return None

    def _get(self, url: str, params: dict) -> requests.Response:
        return requests.get(
            url,
            params=params,
            headers={"User-Agent": self.USER_AGENT},
            timeout=self.timeout,
        )

    async def fetch_close(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> Optional[pd.DataFrame]:
        """获取 Yahoo 行情收盘序列。

        Args:
            symbol: Yahoo 代码，如 "^VIX"、"DX-Y.NYB"
            start_date/end_date: YYYYMMDD 或 YYYY-MM-DD
            stop_event: 取消事件

        Returns:
            两列 DataFrame：observation_date(YYYY-MM-DD) + close(float)，
            对齐 FredAPI 输出形态，便于 fallback 复用 FredTask 的合并逻辑。
            无数据时返回 None。
        """
        period1 = self._to_unix(start_date)
        period2 = self._to_unix(end_date)
        if period2 is not None:
            # Yahoo chart API treats period2 as an exclusive boundary.
            period2 += int(timedelta(days=1).total_seconds())
        # 若未提供日期，回退到 range 模式取最近 30 年
        if period1 is None or period2 is None:
            params = {"range": "30y", "interval": "1d"}
        else:
            params = {"period1": period1, "period2": period2, "interval": "1d"}

        url = self.BASE_URL.format(symbol=requests.utils.quote(symbol, safe=""))
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            if stop_event is not None and stop_event.is_set():
                return None
            try:
                resp = await asyncio.to_thread(self._get, url, params)
            except requests.RequestException as e:
                last_exc = e
                self.logger.warning(
                    "Yahoo %s 第 %d 次请求网络错误：%s", symbol, attempt, e
                )
                await self._sleep(self.retry_delay, stop_event)
                continue

            if resp.status_code != 200:
                last_exc = YahooAPIError(
                    f"Yahoo {symbol} 返回 HTTP {resp.status_code}: {resp.text[:200]}"
                )
                if 400 <= resp.status_code < 500:
                    # 客户端错误（如代码无效），不重试
                    raise last_exc
                self.logger.warning(
                    "Yahoo %s 返回 HTTP %s，第 %d 次重试",
                    symbol, resp.status_code, attempt,
                )
                await self._sleep(self.retry_delay, stop_event)
                continue

            try:
                payload = resp.json()
            except ValueError as e:
                raise YahooAPIError(f"Yahoo {symbol} JSON 解析失败: {e}") from e

            result = payload.get("chart", {}).get("result")
            if not result:
                err = payload.get("chart", {}).get("error", {})
                raise YahooAPIError(
                    f"Yahoo {symbol} 无结果: {err.get('description', err)}"
                )

            data = result[0]
            timestamps = data.get("timestamp") or []
            quote = (data.get("indicators", {}).get("quote") or [{}])[0]
            closes = quote.get("close") or []
            if not timestamps or not closes:
                return None

            rows = []
            for ts, c in zip(timestamps, closes):
                d = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                rows.append({"observation_date": d, "close": c})
            df = pd.DataFrame(rows)
            # 丢弃收盘为 None 的行（节假日/缺失）
            df = df.dropna(subset=["close"]).reset_index(drop=True)
            return df if not df.empty else None

        if last_exc:
            raise YahooAPIError(
                f"Yahoo {symbol} 经 {self.max_retries} 次重试仍失败: {last_exc}"
            ) from last_exc
        return None

    async def _sleep(self, seconds: float, stop_event: Optional[asyncio.Event]) -> None:
        while seconds > 0:
            if stop_event is not None and stop_event.is_set():
                return
            step = min(seconds, 0.5)
            await asyncio.sleep(step)
            seconds -= step
