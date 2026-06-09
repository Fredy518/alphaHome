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
import re
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


class TinySoftRateLimitError(TinySoftAPIError):
    """Tinysoft API 限流错误。"""


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
        self._login_lock = asyncio.Lock()
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
    def _format_infotable_select_field(field: Any) -> Optional[str]:
        raw = str(field or "").strip()
        if not raw:
            return None
        lowered = raw.lower()
        if raw.startswith("[") or " as " in lowered:
            return raw
        if re.match(r"^[A-Za-z_][A-Za-z0-9_.]*\s*\(", raw):
            return raw
        return f'["{raw}"]'

    @classmethod
    def _format_infotable_select_fields(cls, fields: Optional[Iterable[Any]]) -> str:
        if not fields:
            return "*"
        formatted = [cls._format_infotable_select_field(field) for field in fields]
        selected = [field for field in formatted if field]
        return ",".join(selected) if selected else "*"

    @staticmethod
    def _quote_tsl_string(value: Any) -> str:
        if value is None:
            raise ValueError("Tinysoft stock code cannot be empty")
        text = str(value).strip()
        if not text:
            raise ValueError("Tinysoft stock code cannot be empty")
        return "'" + text.replace("'", "''") + "'"

    @classmethod
    def _format_stock_selector(cls, stocks: Iterable[Any]) -> str:
        normalized = []
        for stock in stocks:
            if stock is None:
                continue
            text = str(stock).strip()
            if text:
                normalized.append(text)
        if not normalized:
            raise ValueError("Tinysoft stock selector cannot be empty")
        if len(normalized) == 1:
            return cls._quote_tsl_string(normalized[0])
        return "array(" + ",".join(cls._quote_tsl_string(stock) for stock in normalized) + ")"

    @staticmethod
    def _format_datetime_literal(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Tinysoft date/time value cannot be empty")
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
            "w": "cy_week()",
            "week": "cy_week()",
            "weekly": "cy_week()",
            "周线": "cy_week()",
            "m": "cy_month()",
            "month": "cy_month()",
            "monthly": "cy_month()",
            "月线": "cy_month()",
        }
        if lowered in mapping:
            return mapping[lowered]
        raise ValueError(f"Unsupported Tinysoft cycle: {cycle}")

    @staticmethod
    def _format_market_select_field(field: Any) -> str:
        raw = str(field or "").strip()
        if not raw:
            raise ValueError("Tinysoft market field cannot be empty")
        if raw.lower() == "date":
            return 'datetimetostr(["date"]) as "date"'
        if raw.startswith("[") or " as " in raw.lower() or "(" in raw:
            return raw
        return f'["{raw}"]'

    @classmethod
    def _build_markettable_panel_tsl(
        cls,
        *,
        stocks: Iterable[Any],
        cycle: str,
        begin_time: Any,
        end_time: Any,
        fields: Optional[Iterable[Any]],
    ) -> str:
        field_list = list(fields or ["date", "StockID", "open", "high", "low", "close", "vol", "amount"])
        select_fields = ",".join(cls._format_market_select_field(field) for field in field_list)
        begin_literal = cls._format_datetime_literal(begin_time)
        end_literal = cls._format_datetime_literal(end_time)
        cycle_expr = cls._cycle_to_tsl_expr(cycle)
        stock_selector = cls._format_stock_selector(stocks)
        return (
            f"setsysparam(pn_cycle(),{cycle_expr});"
            f"return select {select_fields} "
            f"from markettable datekey {begin_literal} to {end_literal} "
            f"of {stock_selector} end;"
        )

    @staticmethod
    def _is_login_error(error_code: int, message: str) -> bool:
        msg = (message or "").lower()
        return error_code in {-1, -13} or "login" in msg or "invalid user" in msg

    async def login(self, force: bool = False) -> None:
        async with self._login_lock:
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

    async def _discard_client(self, client=None) -> None:
        async with self._client_lock:
            if client is None or self._client is client:
                self._client = None

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
            result = await self._to_thread_with_timeout(
                query_call,
                timeout_ms=timeout,
                op_name="Tinysoft query",
                client=client,
            )

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

    async def query_panel(
        self,
        *,
        stocks: Iterable[Any],
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
        """通过 ``markettable ... of array(...)`` 拉取多个标的行情。"""
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft query_panel 被取消")
        if (
            int(rate or 0) != 0
            or rateday is not None
            or precision is not None
            or viewpoint is not None
            or cyclefilter is not None
        ):
            raise TinySoftAPIError("Tinysoft query_panel 暂不支持复权/精度/过滤参数，请使用单标的 query。")

        tsl_code = self._build_markettable_panel_tsl(
            stocks=stocks,
            cycle=cycle,
            begin_time=begin_time,
            end_time=end_time,
            fields=fields,
        )
        exec_kwargs = {"as_dataframe": True, "stop_event": stop_event}
        if timeout_ms is not None:
            exec_kwargs["timeout_ms"] = timeout_ms
        return await self.exec(tsl_code, **exec_kwargs)

    # ------------------------------------------------------------------
    # exec / call / call_dataframe — 通用 TSL 接口
    # ------------------------------------------------------------------

    async def _parse_result(self, result, *, as_dataframe: bool = True):
        """统一解析 pyTSL 调用结果。"""
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
            raise TinySoftAPIError(
                f"Tinysoft 调用失败: code={error_code}, message={message}"
            )

        if as_dataframe:
            try:
                df = result.dataframe()
                return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
            except Exception as e:
                raise TinySoftAPIError(f"转换结果为 DataFrame 失败: {e}") from e
        else:
            try:
                return result.value()
            except Exception as e:
                raise TinySoftAPIError(f"获取结果 value 失败: {e}") from e

    async def _to_thread_with_timeout(
        self,
        func,
        *args,
        timeout_ms: Optional[int] = None,
        op_name: str = "Tinysoft 调用",
        client=None,
        **kwargs,
    ):
        timeout = int(timeout_ms or self.timeout_ms or self.DEFAULT_TIMEOUT_MS)
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(func, *args, **kwargs),
                timeout=max(0.001, timeout / 1000.0),
            )
        except asyncio.TimeoutError as e:
            if client is not None:
                await self._discard_client(client)
                self.logger.warning(
                    "%s 超时，已废弃当前 Tinysoft 客户端，下次请求将重新连接。timeout_ms=%s",
                    op_name,
                    timeout,
                )
            raise TinySoftAPIError(f"{op_name} 超时: timeout_ms={timeout}") from e

    async def exec(
        self,
        tsl_code: str,
        *,
        as_dataframe: bool = True,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ):
        """
        执行任意 TSL 代码。

        Args:
            tsl_code: TSL 脚本字符串。
            as_dataframe: True 返回 DataFrame，False 返回原始 value()。
            timeout_ms: 客户端调用硬超时，防止 pyTSL exec 无限等待。
            stop_event: 取消事件。
        """
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft exec 被取消")

        client = await self._get_client()
        await self.login()
        await self._wait_for_request_slot()

        result = await self._to_thread_with_timeout(
            client.exec,
            tsl_code,
            timeout_ms=timeout_ms,
            op_name="Tinysoft exec",
            client=client,
        )
        return await self._parse_result(result, as_dataframe=as_dataframe)

    async def call(
        self,
        func_name: str,
        *args,
        code: str = "",
        as_dataframe: bool = True,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ):
        """
        调用 TSL 自定义函数。

        Args:
            func_name: TSL 函数名。
            *args: 传给函数的位置参数。
            code: 包含函数定义的 TSL 代码。
            as_dataframe: True 返回 DataFrame，False 返回原始 value()。
            timeout_ms: 客户端调用硬超时。
            stop_event: 取消事件。
        """
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft call 被取消")

        client = await self._get_client()
        await self.login()
        await self._wait_for_request_slot()

        call_kwargs = {}
        if code:
            call_kwargs["code"] = code
        result = await self._to_thread_with_timeout(
            client.call,
            func_name,
            *args,
            timeout_ms=timeout_ms,
            op_name=f"Tinysoft call({func_name})",
            client=client,
            **call_kwargs,
        )
        return await self._parse_result(result, as_dataframe=as_dataframe)

    async def call_dataframe(
        self,
        func_name: str,
        table_id: int,
        *,
        stock: str = "",
        where_clause: Optional[str] = None,
        fields: Optional[Iterable[Any]] = None,
        service: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> pd.DataFrame:
        """
        通过 exec 拉取 InfoArray / InfoTable 数据并返回 DataFrame。

        等价 TSL::

            select {fields or *} from infotable {table_id} of '{stock}' [where {where_clause}] end;

        Args:
            func_name: 逻辑上的数据函数名（如 ``"infoarray"``），仅用于日志；
                       实际取数一律使用 ``infotable`` SQL 语法。
            table_id: 天软表 ID（如 127 停牌、139 行业、42 财务）。
            stock: 天软格式股票代码（如 ``"SZ000001"``）。
            where_clause: 可选 TSL WHERE 条件（如 ``'["停牌开始日"]>=20260101'``）。
                          为 None 时拉取全量记录。
            fields: 可选字段投影列表；为 None 时使用 ``select *``。
            service: 可选 service 参数（当前未使用，保留兼容）。
            timeout_ms: 查询硬超时。
            stop_event: 取消事件。

        Returns:
            pd.DataFrame: 返回表格数据。
        """
        return await self.call_dataframe_for_stocks(
            func_name,
            table_id,
            stocks=[stock],
            where_clause=where_clause,
            fields=fields,
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
        fields: Optional[Iterable[Any]] = None,
        service: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> pd.DataFrame:
        """
        通过 exec 拉取单个或多个标的的 InfoArray / InfoTable 数据。

        多标的调用使用 ``infotable ... of array('SZ000001','SH600000')``，
        用于减少全市场 InfoTable 任务的在线请求数。``service`` 和
        ``timeout_ms`` 由 pyTSL exec 外层硬超时控制。
        """
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft call_dataframe 被取消")

        stock_selector = self._format_stock_selector(stocks)
        select_part = self._format_infotable_select_fields(fields)
        where_part = f" where {where_clause}" if where_clause else ""
        tsl_code = (
            f"return select {select_part} from infotable {int(table_id)} of {stock_selector}"
            f"{where_part} end;"
        )
        self.logger.debug(
            "call_dataframe_for_stocks: func=%s, table_id=%s, stocks=%s, fields=%s, where=%s",
            func_name, table_id, stock_selector, select_part, where_clause or "(none)",
        )
        exec_kwargs = {"as_dataframe": True, "stop_event": stop_event}
        if timeout_ms is not None:
            exec_kwargs["timeout_ms"] = timeout_ms
        return await self.exec(tsl_code, **exec_kwargs)

    async def call_dataframe_table(
        self,
        func_name: str,
        table_id: int,
        *,
        where_clause: Optional[str] = None,
        fields: Optional[Iterable[Any]] = None,
        service: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> pd.DataFrame:
        """
        通过 exec 拉取无需显式 ``of code`` 的 InfoTable 数据。

        某些 Tinysoft 表（例如部分经理维度衍生表）在数据字典中未说明
        取数代码；该方法保留无 ``of`` 的查询形态，供任务侧配置使用。
        ``timeout_ms`` 由 pyTSL exec 外层硬超时控制。
        """
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("Tinysoft call_dataframe_table 被取消")

        select_part = self._format_infotable_select_fields(fields)
        where_part = f" where {where_clause}" if where_clause else ""
        tsl_code = f"return select {select_part} from infotable {int(table_id)}{where_part} end;"
        self.logger.debug(
            "call_dataframe_table: func=%s, table_id=%s, fields=%s, where=%s",
            func_name, table_id, select_part, where_clause or "(none)",
        )
        exec_kwargs = {"as_dataframe": True, "stop_event": stop_event}
        if timeout_ms is not None:
            exec_kwargs["timeout_ms"] = timeout_ms
        return await self.exec(tsl_code, **exec_kwargs)
