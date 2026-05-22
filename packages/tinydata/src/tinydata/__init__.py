"""tinydata public API."""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence

import pandas as pd

from .client import TinyClient
from .config import configure as _configure
from .config import get_config, reset_config as _reset_config
from .datasets import (
    bond_basic_ext,
    fund_basic,
    fund_bond_holding,
    fund_fof_holding_detail,
    fund_manager,
    fund_stock_holding,
    future_basic_ext,
    option_basic_daily_ext,
    stock_basic_ext,
)
from .errors import (
    TinyDataAuthError,
    TinyDataCodePoolError,
    TinyDataConfigError,
    TinyDataDependencyError,
    TinyDataError,
    TinyDataQueryError,
    TinyDataTimeoutError,
)
from .infotable import InfoTableOptions, query_infotable as _query_infotable

__version__ = "0.1.0"

_DEFAULT_CLIENT: Optional[TinyClient] = None


def get_client() -> TinyClient:
    global _DEFAULT_CLIENT
    if _DEFAULT_CLIENT is None:
        _DEFAULT_CLIENT = TinyClient(get_config())
    return _DEFAULT_CLIENT


def configure(
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    ini_path: Optional[str] = None,
    opi_url: Optional[str] = None,
    opi_auth_mode: Optional[str] = None,
    session_key: Optional[str] = None,
    session_password: Optional[str] = None,
    service: Optional[str] = None,
    json_encode: Optional[str] = None,
    run_func_name: Optional[str] = None,
    query_func_name: Optional[str] = None,
    cache_dir: Any = None,
    request_interval: Optional[float] = None,
    timeout_ms: Optional[int] = None,
):
    """Configure tinydata and reset the default Tinysoft client."""

    global _DEFAULT_CLIENT
    cfg = _configure(
        user=user,
        password=password,
        host=host,
        port=port,
        ini_path=ini_path,
        opi_url=opi_url,
        opi_auth_mode=opi_auth_mode,
        session_key=session_key,
        session_password=session_password,
        service=service,
        json_encode=json_encode,
        run_func_name=run_func_name,
        query_func_name=query_func_name,
        cache_dir=cache_dir,
        request_interval=request_interval,
        timeout_ms=timeout_ms,
    )
    _DEFAULT_CLIENT = TinyClient(cfg)
    return cfg


def configure_client(**kwargs: Any) -> TinyClient:
    """Configure tinydata and return a fresh default Tinysoft client."""

    configure(**kwargs)
    assert _DEFAULT_CLIENT is not None
    return _DEFAULT_CLIENT


def reset_config() -> None:
    """Clear explicit configuration and discard the default client."""

    global _DEFAULT_CLIENT
    _reset_config()
    _DEFAULT_CLIENT = None


def query_infotable(
    table_id: int,
    codes: Optional[Iterable[Any]] = None,
    start_date: Any = None,
    end_date: Any = None,
    date_field: Optional[str] = None,
    fields: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    return _query_infotable(
        get_client(),
        table_id,
        codes=codes,
        start_date=start_date,
        end_date=end_date,
        date_field=date_field,
        fields=fields,
        allow_full_table=not codes,
        options=InfoTableOptions(),
    )


def query_market(
    symbol: str,
    start_time: str,
    end_time: str,
    cycle: str = "1分钟线",
    fields: Optional[Iterable[Any]] = None,
) -> pd.DataFrame:
    return get_client().query(
        stock=symbol,
        cycle=cycle,
        begin_time=start_time,
        end_time=end_time,
        fields=fields,
    )


__all__ = [
    "TinyClient",
    "TinyDataAuthError",
    "TinyDataCodePoolError",
    "TinyDataConfigError",
    "TinyDataDependencyError",
    "TinyDataError",
    "TinyDataQueryError",
    "TinyDataTimeoutError",
    "bond_basic_ext",
    "configure",
    "configure_client",
    "fund_basic",
    "fund_bond_holding",
    "fund_fof_holding_detail",
    "fund_manager",
    "fund_stock_holding",
    "future_basic_ext",
    "get_config",
    "option_basic_daily_ext",
    "query_infotable",
    "query_market",
    "reset_config",
    "stock_basic_ext",
    "__version__",
]
