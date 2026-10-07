"""GLMS 候选分类连接配置；密钥只从进程或 Windows 用户环境读取。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit


DEFAULT_PROVIDER = "glms"
DEFAULT_BASE_URL = "https://models.glms.com.cn/ucloud/v1"
DEFAULT_MODEL = "deepseek-v4-flash"
DEFAULT_REVIEW_MODEL = "deepseek-v4-pro"


def _read_windows_user_variable(name: str) -> str | None:
    # GUI 启动后新增用户变量也能生效，不要求重启桌面应用。
    try:
        import winreg

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as registry:
            value, _ = winreg.QueryValueEx(registry, name)
        if isinstance(value, str):
            return value.strip() or None
        return None
    except (ImportError, OSError):
        return None


def _environment_value(name: str) -> str | None:
    return os.environ.get(name, "").strip() or _read_windows_user_variable(name)


def get_glms_api_key() -> str | None:
    return _environment_value("GLMS_API_KEY")


def _normalize_base_url(value: str) -> str:
    parts = urlsplit(value.strip())
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        raise ValueError(
            "GLMS_BASE_URL must be an HTTPS API base URL without credentials, query or fragment"
        )
    path = parts.path.rstrip("/")
    if parts.hostname.lower() == "models.glms.com.cn" and not path:
        path = "/ucloud/v1"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


@dataclass(frozen=True)
class CandidateLLMConfig:
    base_url: str
    model: str
    provider: str = DEFAULT_PROVIDER

    def audit(self) -> dict[str, str]:
        """可安全写入计划、缓存和日志的连接身份，不包含 token。"""
        return {
            "provider": self.provider,
            "base_url": self.base_url,
            "model": self.model,
        }


def resolve_candidate_llm_config(
    *,
    model: str | None = None,
    base_url: str | None = None,
    default_model: str = DEFAULT_MODEL,
) -> CandidateLLMConfig:
    return CandidateLLMConfig(
        base_url=_normalize_base_url(
            (base_url or "").strip()
            or _environment_value("GLMS_BASE_URL")
            or DEFAULT_BASE_URL
        ),
        model=(model or "").strip()
        or _environment_value("GLMS_MODEL")
        or default_model,
    )
