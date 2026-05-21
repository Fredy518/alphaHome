#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft TS-OPI HTTP backend.

This backend mirrors the small method surface used by Tinysoft fetcher tasks:
``exec``, ``call``, ``call_dataframe*`` and ``query``. It lets existing
InfoTable/InfoArray tasks switch from pyTSL to TS-OPI without changing task
code. Minute-bar ``query`` is translated into a ``markettable`` TSL script.
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import logging
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import aiohttp
import pandas as pd

from .tinysoft_api import TinySoftAPI, TinySoftAPIError, TinySoftAuthError


TransportResponse = Any
TransportCallable = Callable[..., TransportResponse]


class TinySoftOPIAPI:
    """Async TS-OPI client compatible with the existing Tinysoft task API."""

    DEFAULT_BASE_URL = "https://opi.tinysoft.com.cn"
    DEFAULT_TIMEOUT_MS = TinySoftAPI.DEFAULT_TIMEOUT_MS
    DEFAULT_REQUEST_INTERVAL = TinySoftAPI.DEFAULT_REQUEST_INTERVAL

    def __init__(
        self,
        *,
        user: Optional[str] = None,
        password: Optional[str] = None,
        base_url: Optional[str] = None,
        opi_url: Optional[str] = None,
        auth_mode: str = "basic",
        session_key: Optional[str] = None,
        session_password: Optional[str] = None,
        api_key: Optional[str] = None,
        service: str = "",
        event_name: Optional[str] = None,
        json_encode: str = "utf8",
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        request_interval: float = DEFAULT_REQUEST_INTERVAL,
        run_func_name: Optional[str] = None,
        query_func_name: Optional[str] = None,
        transport: Optional[TransportCallable] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.user = (user or "").strip()
        self.password = password or ""
        self.base_url = (opi_url or base_url or self.DEFAULT_BASE_URL).strip().rstrip("/")
        self.auth_mode = (auth_mode or "basic").strip().lower().replace("_", "-")
        self.session_key = (session_key or api_key or "").strip()
        self.session_password = session_password or ""
        self.service = service or ""
        self.event_name = event_name
        self.json_encode = json_encode or "utf8"
        self.timeout_ms = int(timeout_ms or self.DEFAULT_TIMEOUT_MS)
        self.request_interval = float(request_interval)
        self.run_func_name = (run_func_name or "").strip() or None
        self.query_func_name = (query_func_name or "").strip() or None
        self.transport = transport
        self.logger = logger or logging.getLogger(__name__)

        self._request_lock = asyncio.Lock()
        self._last_request_time = 0.0

    async def _wait_for_request_slot(self) -> None:
        async with self._request_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self.request_interval:
                await asyncio.sleep(self.request_interval - elapsed)
            self._last_request_time = time.monotonic()

    def _auth_headers(self) -> Dict[str, str]:
        if self.auth_mode in {"none", "no-auth"}:
            return {}

        if self.auth_mode in {"basic", "base"}:
            if not self.user:
                raise TinySoftAuthError("Tinysoft OPI user is empty.")
            if not self.password:
                raise TinySoftAuthError("Tinysoft OPI password is empty.")
            token = base64.b64encode(f"{self.user}:{self.password}".encode("utf-8")).decode("ascii")
            return {"Authorization": f"Basic {token}"}

        if self.auth_mode in {"bearer", "session", "session-key"}:
            if not self.session_key:
                raise TinySoftAuthError("Tinysoft OPI session_key is empty.")
            token = self.session_key
            if self.session_password:
                token = f"{token}:{self.session_password}"
            return {"Authorization": f"Bearer {token}"}

        if self.auth_mode in {"x-api-key", "api-key", "apikey"}:
            if not self.session_key:
                raise TinySoftAuthError("Tinysoft OPI api key is empty.")
            token = self.session_key
            if self.session_password:
                token = f"{token}:{self.session_password}"
            return {"X-API-Key": token}

        raise TinySoftAuthError(f"Unsupported Tinysoft OPI auth_mode: {self.auth_mode}")

    def _headers(self, *, service: Optional[str] = None, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "JSON-Encode": self.json_encode,
        }
        headers.update(self._auth_headers())
        event_name = service if service is not None else (self.event_name or self.service)
        if event_name:
            headers["TS-EVENTNAME"] = str(event_name)
        if extra:
            headers.update({str(k): str(v) for k, v in extra.items() if v is not None})
        return headers

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    @staticmethod
    def _decode_text(body: bytes) -> str:
        for encoding in ("utf-8", "gbk", "gb18030"):
            try:
                return body.decode(encoding)
            except UnicodeDecodeError:
                continue
        return body.decode("utf-8", errors="replace")

    @classmethod
    def _decode_body(cls, body: Any) -> Any:
        if body is None or isinstance(body, (dict, list, int, float, bool)):
            return body
        if isinstance(body, bytes):
            body = cls._decode_text(body)
        text = str(body).strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    @classmethod
    def _unpack_transport_response(cls, response: Any) -> Tuple[int, Dict[str, str], Any]:
        if isinstance(response, tuple):
            if len(response) == 3:
                status, headers, body = response
                return int(status), dict(headers or {}), body
            if len(response) == 2:
                status, body = response
                return int(status), {}, body

        if isinstance(response, dict) and {"status", "body"} & set(response):
            status = int(response.get("status", 200))
            headers = dict(response.get("headers") or {})
            body = response.get("body", response.get("payload"))
            return status, headers, body

        return 200, {}, response

    @staticmethod
    def _maybe_raise_payload_error(payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        lowered = {str(k).lower(): v for k, v in payload.items()}
        code = lowered.get("code", lowered.get("error_code", lowered.get("status")))
        message = lowered.get("message", lowered.get("msg", lowered.get("error")))
        has_data = any(key in lowered for key in ("data", "result", "value", "rows", "body", "res"))
        if code is None or has_data:
            return
        code_text = str(code).strip()
        if code_text and code_text not in {"0", "200", "success", "ok"}:
            raise TinySoftAPIError(f"Tinysoft OPI returned error: code={code}, message={message}")

    async def _request_json(
        self,
        path: str,
        payload: Any,
        *,
        service: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        if not self.base_url:
            raise TinySoftAPIError("Tinysoft OPI base_url is empty.")

        timeout = int(timeout_ms or self.timeout_ms or self.DEFAULT_TIMEOUT_MS)
        headers = self._headers(service=service, extra=extra_headers)
        url = self._url(path)

        await self._wait_for_request_slot()

        if self.transport is not None:
            response = self.transport(
                path=path,
                url=url,
                headers=headers,
                json=payload,
                timeout_ms=timeout,
            )
            if inspect.isawaitable(response):
                response = await response
            status, response_headers, body = self._unpack_transport_response(response)
        else:
            client_timeout = aiohttp.ClientTimeout(total=max(0.001, timeout / 1000.0))
            try:
                async with aiohttp.ClientSession(timeout=client_timeout) as session:
                    async with session.post(url, headers=headers, json=payload) as resp:
                        status = int(resp.status)
                        response_headers = dict(resp.headers)
                        body = await resp.read()
            except asyncio.TimeoutError as exc:
                raise TinySoftAPIError(f"Tinysoft OPI request timeout: timeout_ms={timeout}") from exc
            except aiohttp.ClientError as exc:
                raise TinySoftAPIError(f"Tinysoft OPI request failed: {exc}") from exc

        decoded = self._decode_body(body)
        if status < 200 or status >= 300:
            raise TinySoftAPIError(f"Tinysoft OPI HTTP {status}: {decoded}")
        self._maybe_raise_payload_error(decoded)
        return decoded

    @staticmethod
    def _extract_data_payload(payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload
        for key in (
            "data",
            "Data",
            "result",
            "Result",
            "value",
            "Value",
            "rows",
            "Rows",
            "body",
            "Body",
            "res",
            "Res",
        ):
            if key in payload:
                return payload[key]
        return payload

    @classmethod
    def _payload_to_dataframe(cls, payload: Any) -> pd.DataFrame:
        if isinstance(payload, dict):
            lowered = {str(k).lower(): k for k in payload}
            if "columns" in lowered and "rows" in lowered:
                data = payload
            else:
                data = cls._extract_data_payload(payload)
        else:
            data = cls._extract_data_payload(payload)
        if data is None:
            return pd.DataFrame()
        if isinstance(data, str):
            data = cls._decode_body(data)
            if isinstance(data, str):
                return pd.DataFrame({"value": [data]})

        if isinstance(data, dict):
            lowered = {str(k).lower(): k for k in data}
            if "columns" in lowered and "rows" in lowered:
                columns = data[lowered["columns"]]
                rows = data[lowered["rows"]]
                return pd.DataFrame(rows, columns=columns)

            list_values = [v for v in data.values() if isinstance(v, list)]
            if list_values and len(list_values) == len(data):
                lengths = {len(v) for v in list_values}
                if len(lengths) == 1:
                    return pd.DataFrame(data)

            return pd.DataFrame([data])

        if isinstance(data, list):
            if not data:
                return pd.DataFrame()
            first = data[0]
            if isinstance(first, dict):
                return pd.DataFrame(data)
            if (
                isinstance(first, (list, tuple))
                and first
                and all(isinstance(item, str) for item in first)
                and all(isinstance(row, (list, tuple)) and len(row) == len(first) for row in data[1:])
            ):
                return pd.DataFrame(data[1:], columns=list(first))
            return pd.DataFrame(data)

        return pd.DataFrame({"value": [data]})

    @staticmethod
    def _quote_tsl_string(value: Any) -> str:
        return TinySoftAPI._quote_tsl_string(value)

    @classmethod
    def _format_stock_selector(cls, stocks: Iterable[Any]) -> str:
        return TinySoftAPI._format_stock_selector(stocks)

    @staticmethod
    def _format_datetime_literal(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Tinysoft OPI date/time value cannot be empty")
        dt = pd.to_datetime(text, errors="raise")
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and not any(ch in text for ch in (":", ".")):
            return dt.strftime("%Y%m%dT")
        return dt.strftime("%Y%m%d.%H%M%ST")

    @staticmethod
    def _cycle_to_tsl_expr(cycle: str) -> str:
        raw = str(cycle or "").strip()
        lowered = raw.lower()
        if lowered.startswith("cy_") and lowered.endswith(")"):
            return raw
        mapping = {
            "1m": "cy_1m()",
            "1min": "cy_1m()",
            "1分钟": "cy_1m()",
            "1分钟线": "cy_1m()",
            "5m": "cy_5m()",
            "5min": "cy_5m()",
            "5分钟": "cy_5m()",
            "5分钟线": "cy_5m()",
            "15m": "cy_15m()",
            "15min": "cy_15m()",
            "15分钟": "cy_15m()",
            "15分钟线": "cy_15m()",
            "30m": "cy_30m()",
            "30min": "cy_30m()",
            "30分钟": "cy_30m()",
            "30分钟线": "cy_30m()",
            "60m": "cy_60m()",
            "60min": "cy_60m()",
            "60分钟": "cy_60m()",
            "60分钟线": "cy_60m()",
            "d": "cy_day()",
            "day": "cy_day()",
            "daily": "cy_day()",
            "日线": "cy_day()",
        }
        if lowered in mapping:
            return mapping[lowered]
        raise ValueError(f"Unsupported Tinysoft OPI cycle: {cycle}")

    @staticmethod
    def _format_select_field(field: Any) -> str:
        raw = str(field or "").strip()
        if not raw:
            raise ValueError("Tinysoft OPI field cannot be empty")
        if raw.lower() == "date":
            return 'datetimetostr(["date"]) as "date"'
        if raw.startswith("[") or " as " in raw.lower() or "(" in raw:
            return raw
        return f'["{raw}"]'

    @classmethod
    def _build_markettable_tsl(
        cls,
        *,
        stock: str,
        cycle: str,
        begin_time: Any,
        end_time: Any,
        fields: Optional[Iterable[Any]],
    ) -> str:
        field_list = list(fields or ["date", "StockID", "open", "high", "low", "close", "vol", "amount"])
        select_fields = ",".join(cls._format_select_field(field) for field in field_list)
        stock_selector = cls._quote_tsl_string(stock)
        begin_literal = cls._format_datetime_literal(begin_time)
        end_literal = cls._format_datetime_literal(end_time)
        cycle_expr = cls._cycle_to_tsl_expr(cycle)
        return (
            f"setsysparam(pn_cycle(),{cycle_expr});"
            f"return select {select_fields} "
            f"from markettable datekey {begin_literal} to {end_literal} "
            f"of {stock_selector} end;"
        )

    async def login(self, force: bool = False) -> None:
        # OPI authenticates every HTTP request via headers.
        self._auth_headers()

    async def logout(self) -> None:
        return None

    def _uses_session_call_uri(self) -> bool:
        return self.auth_mode in {
            "bearer",
            "session",
            "session-key",
            "x-api-key",
            "api-key",
            "apikey",
        }

    async def exec(
        self,
        tsl_code: str,
        *,
        as_dataframe: bool = True,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ):
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft OPI exec cancelled")
        if self._uses_session_call_uri():
            if self.run_func_name:
                return await self.call(
                    self.run_func_name,
                    {"body": tsl_code},
                    as_dataframe=as_dataframe,
                    timeout_ms=timeout_ms,
                    stop_event=stop_event,
                )
            raise TinySoftAPIError(
                "Tinysoft OPI /Service/Run/ requires developer-user authentication. "
                "SESSION-KEY tenants must expose a wrapper function and configure run_func_name."
            )
        payload = await self._request_json(
            "/Service/Run/",
            {"body": tsl_code},
            timeout_ms=timeout_ms,
        )
        return self._payload_to_dataframe(payload) if as_dataframe else payload

    def _call_path(self, func_name: str) -> str:
        func = str(func_name or "").strip().strip("/")
        if not func:
            return "/Service/Session/Call/" if self._uses_session_call_uri() else "/Service/Call/"
        encoded = "/".join(quote(part) for part in func.split("/"))
        if self._uses_session_call_uri():
            return f"/Service/Session/Call/{encoded}"
        return f"/Service/Call/{encoded}"

    async def call(
        self,
        func_name: str,
        *args: Any,
        code: str = "",
        as_dataframe: bool = True,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ):
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft OPI call cancelled")
        if code:
            raise TinySoftAPIError("Tinysoft OPI call does not support inline TSL code.")
        if not args:
            payload_data: Any = {}
        elif len(args) == 1 and isinstance(args[0], (dict, list)):
            payload_data = args[0]
        else:
            payload_data = list(args)
        payload = await self._request_json(
            self._call_path(func_name),
            payload_data,
            timeout_ms=timeout_ms,
        )
        return self._payload_to_dataframe(payload) if as_dataframe else payload

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
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft OPI query cancelled")

        if self.query_func_name:
            params = {
                "StockID": stock,
                "Cycle": cycle,
                "BegT": begin_time,
                "EndT": end_time,
                "Fields": list(fields or []),
            }
            if rate:
                params["Rate"] = rate
            if rateday is not None:
                params["RateDay"] = rateday
            if precision is not None:
                params["Precision"] = precision
            if viewpoint is not None:
                params["ViewPoint"] = viewpoint
            if cyclefilter is not None:
                params["CycleFilter"] = cyclefilter
            return await self.call(
                self.query_func_name,
                params,
                as_dataframe=True,
                timeout_ms=timeout_ms,
                stop_event=stop_event,
            )

        tsl_code = self._build_markettable_tsl(
            stock=stock,
            cycle=cycle,
            begin_time=begin_time,
            end_time=end_time,
            fields=fields,
        )
        payload = await self._request_json(
            "/Service/Run/",
            {"body": tsl_code},
            service=service,
            timeout_ms=timeout_ms,
        )
        return self._payload_to_dataframe(payload)

    async def call_dataframe(
        self,
        func_name: str,
        table_id: int,
        *,
        stock: str = "",
        where_clause: Optional[str] = None,
        service: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> pd.DataFrame:
        return await self.call_dataframe_for_stocks(
            func_name,
            table_id,
            stocks=[stock],
            where_clause=where_clause,
            service=service,
            timeout_ms=timeout_ms,
            stop_event=stop_event,
        )

    async def call_dataframe_for_stocks(
        self,
        func_name: str,
        table_id: int,
        *,
        stocks: Iterable[str],
        where_clause: Optional[str] = None,
        service: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> pd.DataFrame:
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft OPI call_dataframe cancelled")
        stock_selector = self._format_stock_selector(stocks)
        where_part = f" where {where_clause}" if where_clause else ""
        tsl_code = (
            f"return select * from infotable {int(table_id)} of {stock_selector}"
            f"{where_part} end;"
        )
        return await self.exec(
            tsl_code,
            as_dataframe=True,
            timeout_ms=timeout_ms,
            stop_event=stop_event,
        )

    async def call_dataframe_table(
        self,
        func_name: str,
        table_id: int,
        *,
        where_clause: Optional[str] = None,
        service: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> pd.DataFrame:
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft OPI call_dataframe_table cancelled")
        where_part = f" where {where_clause}" if where_clause else ""
        tsl_code = f"return select * from infotable {int(table_id)}{where_part} end;"
        return await self.exec(
            tsl_code,
            as_dataframe=True,
            timeout_ms=timeout_ms,
            stop_event=stop_event,
        )
