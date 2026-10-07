"""Read-only Massive REST client; credentials never enter URLs or error logs."""

import asyncio
import hashlib
import logging
import os
import threading
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests


class MassiveAPIError(RuntimeError):
    """A failed request or an incomplete/malformed response."""


def get_massive_key():
    """Read lazily so discovery works without credentials or a GUI restart."""
    key = os.environ.get("MASSIVE_KEY", "").strip()
    if not key and os.name == "nt":
        import winreg

        for hive, path in (
            (
                winreg.HKEY_LOCAL_MACHINE,
                r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment",
            ),
            (winreg.HKEY_CURRENT_USER, "Environment"),
        ):
            try:
                with winreg.OpenKey(hive, path) as registry:
                    value, _ = winreg.QueryValueEx(registry, "MASSIVE_KEY")
                if isinstance(value, str) and value.strip():
                    key = value.strip()
                    break
            except OSError:
                continue
    if not key:
        raise MassiveAPIError("请设置环境变量 MASSIVE_KEY 后重试")
    return key


class MassiveAPI:
    BASE_URL = "https://api.massive.com"
    # Shared across task instances and event loops in one application process.
    # Spacing requests avoids the burst allowed by a token-bucket limiter.
    _rate_lock = threading.Lock()
    _next_request = {}

    def __init__(
        self, *, api_key=None, request_interval=13.0, max_attempts=3, logger=None
    ):
        self._api_key = api_key
        self.request_interval = max(13.0, float(request_interval))
        self.max_attempts = max(1, int(max_attempts))
        self.logger = logger or logging.getLogger(__name__)

    @staticmethod
    def check_cancelled(stop_event):
        if stop_event is not None and stop_event.is_set():
            raise asyncio.CancelledError

    async def _sleep(self, seconds, stop_event):
        deadline = time.monotonic() + seconds
        while True:
            self.check_cancelled(stop_event)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 0.25))

    async def _wait_for_rate_limit(self, key, stop_event):
        fingerprint = hashlib.sha256(key.encode()).digest()
        # Recheck after sleeping: cancellation cannot leave reserved future slots,
        # and multiple worker loops must still share the same request spacing.
        while True:
            self.check_cancelled(stop_event)
            with self._rate_lock:
                now = time.monotonic()
                remaining = self._next_request.get(fingerprint, 0) - now
                if remaining <= 0:
                    self._next_request[fingerprint] = now + self.request_interval
                    return
            await self._sleep(remaining, stop_event)

    @classmethod
    def _safe_url(cls, path):
        url = cls.BASE_URL + path if path.startswith("/") else path
        parts = urlsplit(url)
        if (
            parts.scheme != "https"
            or parts.netloc != "api.massive.com"
            or parts.fragment
        ):
            raise MassiveAPIError("拒绝非 Massive 官方地址的分页链接")
        query = [
            (k, v)
            for k, v in parse_qsl(parts.query)
            if k.lower() not in {"apikey", "api_key"}
        ]
        return urlunsplit(
            (parts.scheme, parts.netloc, parts.path, urlencode(query), "")
        )

    def _get(self, url, params, key):
        return requests.get(
            url,
            params=params,
            headers={"Authorization": "Bearer " + key, "Accept": "application/json"},
            timeout=(10, 45),
            allow_redirects=False,
        )

    async def request(self, path, params=None, stop_event=None):
        url = self._safe_url(path)
        key = self._api_key or get_massive_key()
        params = dict(params or {})
        if any(k.lower() in {"apikey", "api_key"} for k in params):
            raise MassiveAPIError("API key 只能通过认证请求头传递")
        for attempt in range(self.max_attempts):
            await self._wait_for_rate_limit(key, stop_event)
            try:
                response = await asyncio.to_thread(self._get, url, params, key)
            except requests.RequestException:
                if attempt + 1 == self.max_attempts:
                    # Exception strings may contain request URLs/credentials.
                    raise MassiveAPIError(
                        "Massive 网络请求失败，重试次数已耗尽"
                    ) from None
                await self._sleep(2 ** (attempt + 1), stop_event)
                continue
            self.check_cancelled(stop_event)
            status = response.status_code
            if status == 429 or 500 <= status < 600:
                if attempt + 1 == self.max_attempts:
                    raise MassiveAPIError(f"Massive HTTP {status}，重试次数已耗尽")
                delay = 60.0 if status == 429 else 2 ** (attempt + 1)
                try:
                    delay = max(delay, float(response.headers.get("Retry-After", 0)))
                except (ValueError, TypeError):
                    pass
                if delay > 300:
                    raise MassiveAPIError(f"Massive HTTP {status}，服务端要求稍后重试")
                await self._sleep(delay, stop_event)
                continue
            if status != 200:
                reason = (
                    "（检查密钥、套餐权限与两年历史窗口）"
                    if status in (401, 403)
                    else ""
                )
                raise MassiveAPIError(f"Massive HTTP {status}{reason}")
            try:
                payload = response.json()
            except ValueError:
                raise MassiveAPIError("Massive 未返回有效 JSON") from None
            if not isinstance(payload, dict) or payload.get("status") != "OK":
                raise MassiveAPIError("Massive 响应状态不是 OK，拒绝保存")
            rows = payload.get("results")
            if rows is None and payload.get("resultsCount") == 0:
                rows = payload["results"] = []
            if not isinstance(rows, list) or any(
                not isinstance(row, dict) for row in rows
            ):
                raise MassiveAPIError("Massive results 不是记录列表")
            for field in ("resultsCount", "count"):
                if field in payload and payload[field] != len(rows):
                    raise MassiveAPIError("Massive 返回条数与记录列表不一致")
            return payload
        raise MassiveAPIError("Massive 请求失败")

    async def list_records(self, path, params=None, stop_event=None):
        """Finish every page before returning; never save a partial snapshot."""
        records, seen = [], set()
        while path:
            self.check_cancelled(stop_event)
            safe_url = self._safe_url(path)
            if safe_url in seen:
                raise MassiveAPIError("Massive 分页链接重复，拒绝不完整快照")
            seen.add(safe_url)
            payload = await self.request(safe_url, params, stop_event)
            records.extend(payload["results"])
            self.logger.info(
                "Massive 分页进度：%s 页，累计 %s 条", len(seen), len(records)
            )
            path = payload.get("next_url")
            if path and not isinstance(path, str):
                raise MassiveAPIError("Massive 分页链接无效")
            params = None
        return records
