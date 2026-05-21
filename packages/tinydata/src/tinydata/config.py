"""Configuration loading for tinydata."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

try:  # pragma: no cover - py39 fallback is covered by behavior, not branch.
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore


DEFAULT_HOST = "tsl.tinysoft.com.cn"
DEFAULT_PORT = 443
DEFAULT_TIMEOUT_MS = 60_000
DEFAULT_REQUEST_INTERVAL = 0.2
DEFAULT_HOME = Path.home() / ".tinydata"
DEFAULT_CONFIG_PATH = DEFAULT_HOME / "config.toml"
DEFAULT_CACHE_DIR = DEFAULT_HOME / "cache"
DEFAULT_CODE_DIR = DEFAULT_HOME / "codes"

_EXPLICIT_OVERRIDES: Dict[str, Any] = {}


@dataclass(frozen=True)
class TinyDataConfig:
    user: str = ""
    password: str = ""
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    ini_path: str = ""
    cache_dir: Path = DEFAULT_CACHE_DIR
    code_dir: Path = DEFAULT_CODE_DIR
    request_interval: float = DEFAULT_REQUEST_INTERVAL
    timeout_ms: int = DEFAULT_TIMEOUT_MS

    def safe_dict(self) -> Dict[str, Any]:
        return {
            "user": self.user,
            "password": "***" if self.password else "",
            "host": self.host,
            "port": self.port,
            "ini_path": self.ini_path,
            "cache_dir": str(self.cache_dir),
            "code_dir": str(self.code_dir),
            "request_interval": self.request_interval,
            "timeout_ms": self.timeout_ms,
        }


def _read_config_file(path: Path = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    if not isinstance(data, dict):
        return {}
    section = data.get("tinydata", data)
    return section.copy() if isinstance(section, dict) else {}


def _coerce_int(value: Any, default: int) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_path(value: Any, default: Path) -> Path:
    if value in (None, ""):
        return default
    return Path(str(value)).expanduser()


def configure(**kwargs: Any) -> TinyDataConfig:
    """Set process-level explicit configuration overrides.

    ``None`` values are ignored so callers can pass optional variables directly.
    """

    for key, value in kwargs.items():
        if value is not None:
            _EXPLICIT_OVERRIDES[key] = value
    return get_config()


def reset_config() -> None:
    """Clear explicit overrides. Intended for tests and notebooks."""

    _EXPLICIT_OVERRIDES.clear()


def get_config(overrides: Optional[Mapping[str, Any]] = None) -> TinyDataConfig:
    file_cfg = _read_config_file()
    env_cfg: Dict[str, Any] = {
        "user": os.environ.get("TINYDATA_USER"),
        "password": os.environ.get("TINYDATA_PASSWORD"),
        "host": os.environ.get("TINYDATA_HOST"),
        "port": os.environ.get("TINYDATA_PORT"),
        "ini_path": os.environ.get("TINYDATA_INI"),
        "cache_dir": os.environ.get("TINYDATA_CACHE_DIR"),
        "code_dir": os.environ.get("TINYDATA_CODE_DIR"),
        "request_interval": os.environ.get("TINYDATA_REQUEST_INTERVAL"),
        "timeout_ms": os.environ.get("TINYDATA_TIMEOUT_MS"),
    }
    merged: Dict[str, Any] = {}
    merged.update({k: v for k, v in file_cfg.items() if v not in (None, "")})
    merged.update({k: v for k, v in env_cfg.items() if v not in (None, "")})
    merged.update(_EXPLICIT_OVERRIDES)
    if overrides:
        merged.update({k: v for k, v in overrides.items() if v is not None})

    return TinyDataConfig(
        user=str(merged.get("user") or ""),
        password=str(merged.get("password") or ""),
        host=str(merged.get("host") or DEFAULT_HOST),
        port=_coerce_int(merged.get("port"), DEFAULT_PORT),
        ini_path=str(merged.get("ini_path") or ""),
        cache_dir=_coerce_path(merged.get("cache_dir"), DEFAULT_CACHE_DIR),
        code_dir=_coerce_path(merged.get("code_dir"), DEFAULT_CODE_DIR),
        request_interval=_coerce_float(
            merged.get("request_interval"), DEFAULT_REQUEST_INTERVAL
        ),
        timeout_ms=_coerce_int(merged.get("timeout_ms"), DEFAULT_TIMEOUT_MS),
    )
