#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
FRED API 封装层

通过 FRED 官方的 fredgraph.csv 端点获取经济数据序列，无需 API key。

接口说明：
- URL: https://fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES>&cosd=<START>&coed=<END>
- 返回标准 CSV，首列为 observation_date（YYYY-MM-DD），次列为序列值
- 缺失值以 "." 表示，解析时转为 NaN
- cosd/coed 为可选的服务端日期过滤参数（YYYY-MM-DD）

设计参照 AkShareAPI：同步 requests 调用经 asyncio.to_thread 异步化，
通过请求间隔 + 重试机制避免触发限流。
"""

import asyncio
import io
import logging
from typing import Optional

import pandas as pd
import requests


class FredAPIError(Exception):
    """FRED API 调用异常。"""
    pass


class FredAPI:
    """FRED 经济数据 API 客户端。"""

    BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

    DEFAULT_REQUEST_INTERVAL = 1.0
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY = 5
    DEFAULT_TIMEOUT = 30

    # 触发更长退避的限流关键字
    RATE_LIMIT_KEYWORDS = ("rate limit", "too many requests", "429")

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        request_interval: float = DEFAULT_REQUEST_INTERVAL,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.request_interval = float(request_interval)
        self.max_retries = int(max_retries)
        self.retry_delay = float(retry_delay)
        self.timeout = int(timeout)
        self._last_request_time = 0.0
        self._request_lock = asyncio.Lock()

    async def _wait_for_rate_limit(self, stop_event: Optional[asyncio.Event] = None) -> None:
        """按 request_interval 间隔限流，避免连续请求触发封锁。"""
        if self.request_interval <= 0:
            return
        async with self._request_lock:
            elapsed = asyncio.get_event_loop().time() - self._last_request_time
            wait = self.request_interval - elapsed
            if wait > 0:
                # 分片睡眠以便响应取消
                while wait > 0:
                    if stop_event is not None and stop_event.is_set():
                        return
                    step = min(wait, 0.5)
                    await asyncio.sleep(step)
                    wait -= step
            self._last_request_time = asyncio.get_event_loop().time()

    def _is_rate_limit_response(self, status_code: int, body: str) -> bool:
        if status_code == 429:
            return True
        body_lower = body.lower()
        return any(kw in body_lower for kw in self.RATE_LIMIT_KEYWORDS)

    @staticmethod
    def _normalize_date(value: Optional[str]) -> Optional[str]:
        """将日期归一化为 FRED 期望的 YYYY-MM-DD 格式。

        接受 YYYYMMDD 或 YYYY-MM-DD；其余原样返回。
        """
        if not value:
            return None
        s = str(value).strip()
        if len(s) == 8 and s.isdigit() and "/" not in s and "-" not in s:
            return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
        return s

    def _get(self, params: dict) -> requests.Response:
        """同步 GET 请求（在 to_thread 中执行）。"""
        return requests.get(self.BASE_URL, params=params, timeout=self.timeout)

    async def fetch_series(
        self,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> Optional[pd.DataFrame]:
        """获取单个 FRED 序列的历史数据。

        Args:
            series_id: FRED 序列 ID，如 "DFEDTARU"、"DTWEXBGS"、"VIXCLS"
            start_date: 起始日期（YYYYMMDD 或 YYYY-MM-DD），对应 FRED cosd
            end_date: 截止日期（YYYYMMDD 或 YYYY-MM-DD），对应 FRED coed
            stop_event: 取消事件

        Returns:
            包含 observation_date 与序列值两列的 DataFrame；无数据时返回 None
        """
        params = {"id": series_id}
        cosd = self._normalize_date(start_date)
        coed = self._normalize_date(end_date)
        if cosd:
            params["cosd"] = cosd
        if coed:
            params["coed"] = coed

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            if stop_event is not None and stop_event.is_set():
                return None
            try:
                await self._wait_for_rate_limit(stop_event=stop_event)
            except asyncio.CancelledError:
                raise

            try:
                resp = await asyncio.to_thread(self._get, params)
            except requests.RequestException as e:
                last_exc = e
                self.logger.warning(
                    "FRED 序列 %s 第 %d 次请求网络错误：%s", series_id, attempt, e
                )
                await self._sleep(self.retry_delay, stop_event)
                continue

            if resp.status_code != 200:
                body = resp.text or ""
                rate_limited = self._is_rate_limit_response(resp.status_code, body)
                last_exc = FredAPIError(
                    f"FRED 序列 {series_id} 返回 HTTP {resp.status_code}: {body[:200]}"
                )
                if rate_limited:
                    # 限流：更长退避
                    backoff = self.retry_delay * attempt * 2
                    self.logger.warning(
                        "FRED 序列 %s 触发限流（HTTP %s），第 %d 次重试，等待 %.1fs",
                        series_id, resp.status_code, attempt, backoff,
                    )
                    await self._sleep(backoff, stop_event)
                    continue
                if 400 <= resp.status_code < 500:
                    # 客户端错误（如序列 ID 无效），重试无意义
                    raise last_exc
                # 5xx：重试
                self.logger.warning(
                    "FRED 序列 %s 返回 HTTP %s，第 %d 次重试",
                    series_id, resp.status_code, attempt,
                )
                await self._sleep(self.retry_delay, stop_event)
                continue

            # 200 成功：解析 CSV
            try:
                df = pd.read_csv(io.StringIO(resp.text), na_values=["."])
            except Exception as e:  # noqa: BLE001 - 解析错误直接抛出
                raise FredAPIError(f"FRED 序列 {series_id} CSV 解析失败: {e}") from e

            if df.empty or "observation_date" not in df.columns:
                self.logger.warning("FRED 序列 %s 返回空数据或缺少 observation_date 列", series_id)
                return None
            return df

        if last_exc:
            raise FredAPIError(
                f"FRED 序列 {series_id} 经 {self.max_retries} 次重试仍失败: {last_exc}"
            ) from last_exc
        return None

    async def _sleep(self, seconds: float, stop_event: Optional[asyncio.Event]) -> None:
        """分片睡眠，可响应取消事件。"""
        while seconds > 0:
            if stop_event is not None and stop_event.is_set():
                return
            step = min(seconds, 0.5)
            await asyncio.sleep(step)
            seconds -= step

    def function_exists(self, series_id: str) -> bool:
        """占位兼容方法：FRED 无函数注册表，始终返回 True。"""
        return True
