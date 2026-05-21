"""Synchronous pyTSL client wrapper used by tinydata."""

from __future__ import annotations

import importlib
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from functools import partial
from typing import Any, Iterable, Optional

import pandas as pd

from .config import TinyDataConfig, get_config
from .errors import (
    TinyDataAuthError,
    TinyDataDependencyError,
    TinyDataQueryError,
    TinyDataTimeoutError,
)


class TinyClient:
    """Thin, defensive wrapper around ``pyTSL.Client``."""

    def __init__(
        self,
        config: Optional[TinyDataConfig] = None,
        *,
        pytsl_module: Any = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.config = config or get_config()
        self._pytsl = pytsl_module
        self.logger = logger or logging.getLogger(__name__)
        self._client = None
        self._client_lock = threading.Lock()
        self._login_lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._last_request_time = 0.0

    def _load_pytsl(self):
        if self._pytsl is not None:
            return self._pytsl
        try:
            self._pytsl = importlib.import_module("pyTSL")
        except ImportError as exc:
            raise TinyDataDependencyError(
                "pyTSL is not installed. Install tinydata with the tinysoft extra: "
                "pip install tinydata[tinysoft]"
            ) from exc
        return self._pytsl

    def _build_client(self):
        pytsl = self._load_pytsl()
        if self.config.ini_path:
            return pytsl.Client(self.config.ini_path)
        if not self.config.user:
            raise TinyDataAuthError("Tinysoft user is empty. Set TINYDATA_USER or call configure(user=...).")
        if not self.config.password:
            raise TinyDataAuthError(
                "Tinysoft password is empty. Set TINYDATA_PASSWORD or call configure(password=...)."
            )
        return pytsl.Client(
            self.config.user,
            self.config.password,
            self.config.host,
            self.config.port,
        )

    def _get_client(self):
        if self._client is not None:
            return self._client
        with self._client_lock:
            if self._client is None:
                self._client = self._build_client()
        return self._client

    def _discard_client(self, client: Any = None) -> None:
        with self._client_lock:
            if client is None or self._client is client:
                self._client = None

    def _wait_for_request_slot(self) -> None:
        with self._request_lock:
            elapsed = time.monotonic() - self._last_request_time
            if elapsed < self.config.request_interval:
                time.sleep(self.config.request_interval - elapsed)
            self._last_request_time = time.monotonic()

    def _run_with_timeout(self, func, *, timeout_ms: Optional[int], op_name: str, client: Any = None):
        timeout = int(timeout_ms or self.config.timeout_ms)
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(func)
        try:
            result = future.result(timeout=max(0.001, timeout / 1000.0))
            executor.shutdown(wait=True)
            return result
        except FutureTimeoutError as exc:
            future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            if client is not None:
                self._discard_client(client)
            raise TinyDataTimeoutError(f"{op_name} timed out after {timeout}ms") from exc
        except Exception:
            executor.shutdown(wait=True)
            raise

    @staticmethod
    def _last_error(client: Any) -> Any:
        try:
            return client.last_error()
        except Exception:
            try:
                return client.last_error
            except Exception:
                return None

    @staticmethod
    def _is_login_error(error_code: int, message: str) -> bool:
        lower = (message or "").lower()
        return error_code in {-1, -13} or "login" in lower or "invalid user" in lower

    def login(self, *, force: bool = False) -> None:
        with self._login_lock:
            client = self._get_client()
            try:
                if not force:
                    is_logined = int(client.is_logined())
                    if is_logined == 1:
                        return
                result = int(client.login())
                if result == 1:
                    return
                raise TinyDataAuthError(f"Tinysoft login failed: {self._last_error(client)}")
            except TinyDataAuthError:
                raise
            except Exception as exc:
                raise TinyDataAuthError(f"Tinysoft login error: {exc}") from exc

    def logout(self) -> None:
        client = self._client
        if client is None:
            return
        try:
            client.logout()
        finally:
            self._discard_client(client)

    def _parse_result(self, result: Any, *, as_dataframe: bool = True):
        if result is None:
            return pd.DataFrame() if as_dataframe else None
        try:
            error_code = int(result.error())
        except Exception:
            error_code = -999
        try:
            message = str(result.message())
        except Exception:
            message = "unknown error"
        if error_code != 0:
            raise TinyDataQueryError(f"Tinysoft call failed: code={error_code}, message={message}")
        if as_dataframe:
            try:
                df = result.dataframe()
                return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
            except Exception as exc:
                raise TinyDataQueryError(f"Failed to convert Tinysoft result to DataFrame: {exc}") from exc
        try:
            return result.value()
        except Exception as exc:
            raise TinyDataQueryError(f"Failed to read Tinysoft result value: {exc}") from exc

    def exec(self, tsl_code: str, *, as_dataframe: bool = True, timeout_ms: Optional[int] = None):
        client = self._get_client()
        self.login()
        self._wait_for_request_slot()
        result = self._run_with_timeout(
            partial(client.exec, tsl_code),
            timeout_ms=timeout_ms,
            op_name="Tinysoft exec",
            client=client,
        )
        return self._parse_result(result, as_dataframe=as_dataframe)

    def call(
        self,
        func_name: str,
        *args: Any,
        code: str = "",
        as_dataframe: bool = True,
        timeout_ms: Optional[int] = None,
    ):
        client = self._get_client()
        self.login()
        self._wait_for_request_slot()
        kwargs = {"code": code} if code else {}
        result = self._run_with_timeout(
            partial(client.call, func_name, *args, **kwargs),
            timeout_ms=timeout_ms,
            op_name=f"Tinysoft call({func_name})",
            client=client,
        )
        return self._parse_result(result, as_dataframe=as_dataframe)

    def query(
        self,
        *,
        stock: str,
        cycle: str,
        begin_time: str,
        end_time: str,
        fields: Optional[Iterable[Any]] = None,
        service: str = "",
        timeout_ms: Optional[int] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        client = self._get_client()
        self.login()
        self._wait_for_request_slot()
        query_kwargs = {
            "stock": stock,
            "cycle": cycle,
            "begin_time": begin_time,
            "end_time": end_time,
            "fields": list(fields) if fields is not None else None,
            "service": service,
            "timeout": int(timeout_ms or self.config.timeout_ms),
            **kwargs,
        }
        for attempt in range(1, 3):
            result = self._run_with_timeout(
                partial(client.query, **query_kwargs),
                timeout_ms=timeout_ms,
                op_name="Tinysoft query",
                client=client,
            )
            try:
                error_code = int(result.error())
            except Exception:
                error_code = -999
            try:
                message = str(result.message())
            except Exception:
                message = "unknown error"
            if error_code == 0:
                df = result.dataframe()
                return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
            if attempt == 1 and self._is_login_error(error_code, message):
                self.login(force=True)
                continue
            raise TinyDataQueryError(
                f"Tinysoft query failed: code={error_code}, message={message}, stock={stock}, cycle={cycle}"
            )
        raise TinyDataQueryError("Tinysoft query returned no valid result")
