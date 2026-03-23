#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tinysoft pyTSL API 封装层

特性：
- pyTSL 依赖可用性检查
- 统一登录与鉴权错误处理
- 同步接口到异步封装（asyncio.to_thread）
- 请求间隔控制
"""

import asyncio
import logging
import time
from functools import partial
from typing import Any, Iterable, List, Optional

import pandas as pd

try:
    import pyTSL
except ImportError:
    pyTSL = None  # type: ignore


class TinySoftAPIError(Exception):
    """Tinysoft API 调用错误。"""


class TinySoftAuthError(TinySoftAPIError):
    """Tinysoft 登录或鉴权失败。"""


class TinySoftDependencyError(TinySoftAPIError):
    """缺少 pyTSL 依赖。"""


class TinySoftAPI:
    """
    Tinysoft API 客户端

    通过 pyTSL.Client 登录并调用 query。
    """

    DEFAULT_HOST = "tsl.tinysoft.com.cn"
    DEFAULT_PORT = 443
    DEFAULT_TIMEOUT_MS = 30_000
    DEFAULT_REQUEST_INTERVAL = 0.2

    def __init__(
        self,
        *,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        ini_path: Optional[str] = None,
        service: str = "",
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        request_interval: float = DEFAULT_REQUEST_INTERVAL,
        logger: Optional[logging.Logger] = None,
    ):
        self.user = (user or "").strip()
        self.password = password or ""
        self.host = (host or self.DEFAULT_HOST).strip()
        self.port = int(port or self.DEFAULT_PORT)
        self.ini_path = (ini_path or "").strip() or None
        self.service = service or ""
        self.timeout_ms = int(timeout_ms)
        self.request_interval = float(request_interval)
        self.logger = logger or logging.getLogger(__name__)

        self._client = None
        self._client_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._last_request_time = 0.0

    @staticmethod
    def _ensure_dependency() -> None:
        if pyTSL is None:
            raise TinySoftDependencyError(
                "pyTSL 未安装，无法使用 Tinysoft 数据源。"
            )

    async def _wait_for_request_slot(self) -> None:
        async with self._request_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self.request_interval:
                await asyncio.sleep(self.request_interval - elapsed)
            self._last_request_time = time.monotonic()

    async def _get_client(self):
        if self._client is not None:
            return self._client

        async with self._client_lock:
            if self._client is None:
                self._client = await asyncio.to_thread(self._build_client_sync)
        return self._client

    def _build_client_sync(self):
        self._ensure_dependency()
        try:
            if self.ini_path:
                self.logger.debug("使用 ini 文件初始化 pyTSL.Client: %s", self.ini_path)
                return pyTSL.Client(self.ini_path)  # type: ignore[attr-defined]

            if not self.user:
                raise TinySoftAuthError(
                    "Tinysoft 用户名为空，请在配置中设置 api.tinysoft.user 或 TINYSOFT_USER。"
                )
            if not self.password:
                raise TinySoftAuthError(
                    "Tinysoft 密码为空，请在配置中设置 api.tinysoft.password 或 TINYSOFT_PASSWORD。"
                )

            self.logger.debug(
                "使用账号初始化 pyTSL.Client: user=%s host=%s port=%s",
                self.user,
                self.host,
                self.port,
            )
            return pyTSL.Client(self.user, self.password, self.host, self.port)  # type: ignore[attr-defined]
        except TinySoftAuthError:
            raise
        except Exception as e:
            raise TinySoftAPIError(f"初始化 pyTSL.Client 失败: {e}") from e

    async def _safe_last_error(self, client) -> Any:
        try:
            return await asyncio.to_thread(client.last_error)
        except Exception:
            return None

    @staticmethod
    def _normalize_fields(fields: Optional[Iterable[Any]]) -> Optional[List[Any]]:
        if fields is None:
            return None
        if isinstance(fields, list):
            return fields
        return list(fields)

    @staticmethod
    def _is_login_error(error_code: int, message: str) -> bool:
        msg = (message or "").lower()
        return error_code in {-1, -13} or "login" in msg or "invalid user" in msg

    async def login(self, force: bool = False) -> None:
        client = await self._get_client()
        try:
            if not force:
                is_logined = int(await asyncio.to_thread(client.is_logined))
                if is_logined == 1:
                    return

            result = int(await asyncio.to_thread(client.login))
            if result == 1:
                self.logger.debug("Tinysoft 登录成功")
                return

            last_error = await self._safe_last_error(client)
            raise TinySoftAuthError(f"Tinysoft 登录失败: {last_error}")
        except TinySoftAuthError:
            raise
        except Exception as e:
            raise TinySoftAuthError(f"Tinysoft 登录异常: {e}") from e

    async def logout(self) -> None:
        if self._client is None:
            return
        client = self._client
        try:
            is_logined = int(await asyncio.to_thread(client.is_logined))
            if is_logined == 1:
                await asyncio.to_thread(client.logout)
        except Exception:
            # 注销失败不影响主流程
            pass

    async def query(
        self,
        *,
        stock: str,
        cycle: str,
        begin_time: Any,
        end_time: Any,
        fields: Optional[Iterable[Any]] = None,
        rate: int = 0,
        rateday: Any = None,
        precision: Any = None,
        viewpoint: Any = None,
        cyclefilter: Any = None,
        service: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> pd.DataFrame:
        """
        调用 pyTSL query 并返回 DataFrame。
        """
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft query 被取消")

        client = await self._get_client()
        await self.login()

        timeout = int(timeout_ms or self.timeout_ms)
        use_service = self.service if service is None else service
        normalized_fields = self._normalize_fields(fields)

        kwargs = {
            "stock": stock,
            "cycle": cycle,
            "begin_time": begin_time,
            "end_time": end_time,
            "rate": int(rate),
            "rateday": rateday,
            "precision": precision,
            "viewpoint": viewpoint,
            "service": use_service or "",
            "timeout": timeout,
            "fields": normalized_fields,
        }
        if cyclefilter is not None:
            kwargs["cyclefilter"] = cyclefilter

        # 鉴权错误触发强制重登后重试一次
        for attempt in range(1, 3):
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("Tinysoft query 被取消")

            await self._wait_for_request_slot()

            query_call = partial(client.query, **kwargs)
            result = await asyncio.to_thread(query_call)

            if result is None:
                return pd.DataFrame(columns=normalized_fields or [])

            try:
                error_code = int(result.error())
            except Exception:
                error_code = -999
            try:
                message = str(result.message())
            except Exception:
                message = "unknown error"

            if error_code == 0:
                try:
                    df = result.dataframe()
                    if isinstance(df, pd.DataFrame):
                        return df
                except Exception as e:
                    raise TinySoftAPIError(f"转换 Tinysoft 结果为 DataFrame 失败: {e}") from e

                return pd.DataFrame(columns=normalized_fields or [])

            if attempt == 1 and self._is_login_error(error_code, message):
                self.logger.warning(
                    "Tinysoft query 鉴权失败，尝试强制重登后重试: code=%s, message=%s",
                    error_code,
                    message,
                )
                await self.login(force=True)
                continue

            raise TinySoftAPIError(
                f"Tinysoft query 失败: code={error_code}, message={message}, stock={stock}, cycle={cycle}"
            )

        raise TinySoftAPIError("Tinysoft query 未返回有效结果")
