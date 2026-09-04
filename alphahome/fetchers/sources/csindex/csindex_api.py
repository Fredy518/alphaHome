#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""中证指数官网指数表现接口客户端。"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import pandas as pd
import requests


CSINDEX_PERFORMANCE_URL = "https://www.csindex.com.cn/csindex-home/perf/index-perf"


class CsindexAPIError(Exception):
    """中证指数公开接口调用异常。"""


class CsindexAPI:
    """通过中证指数官网公开接口获取指数日度表现。"""

    DEFAULT_TIMEOUT = 60
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY = 2.0

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

    def _get(self, params: dict) -> requests.Response:
        return requests.get(
            CSINDEX_PERFORMANCE_URL,
            params=params,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.csindex.com.cn/",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=self.timeout,
        )

    async def fetch_performance(
        self,
        index_code: str,
        start_date: str,
        end_date: str,
        stop_event: Optional[asyncio.Event] = None,
    ) -> Optional[pd.DataFrame]:
        params = {
            "indexCode": str(index_code).strip(),
            "startDate": pd.Timestamp(start_date).strftime("%Y%m%d"),
            "endDate": pd.Timestamp(end_date).strftime("%Y%m%d"),
        }
        last_error: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            if stop_event is not None and stop_event.is_set():
                return None
            try:
                response = await asyncio.to_thread(self._get, params)
                if response.status_code != 200:
                    raise CsindexAPIError(
                        f"中证指数 {index_code} 返回 HTTP {response.status_code}: "
                        f"{(response.text or '')[:200]}"
                    )
                payload = response.json()
                if not payload.get("success"):
                    raise CsindexAPIError(
                        f"中证指数 {index_code} 返回失败: code={payload.get('code')}, "
                        f"msg={payload.get('msg')}"
                    )
                rows = payload.get("data") or []
                return None if not rows else pd.DataFrame(rows)
            except (requests.RequestException, ValueError, CsindexAPIError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                self.logger.warning(
                    "中证指数 %s 第 %s/%s 次请求失败: %s",
                    index_code,
                    attempt,
                    self.max_retries,
                    exc,
                )
                await self._sleep(self.retry_delay * attempt, stop_event)

        raise CsindexAPIError(
            f"中证指数 {index_code} 经 {self.max_retries} 次请求仍失败: {last_error}"
        ) from last_error

    @staticmethod
    async def _sleep(seconds: float, stop_event: Optional[asyncio.Event]) -> None:
        while seconds > 0:
            if stop_event is not None and stop_event.is_set():
                return
            step = min(seconds, 0.5)
            await asyncio.sleep(step)
            seconds -= step
