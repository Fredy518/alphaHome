"""Data access adapters for factor calculators."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager


class FactorDBContext:
    """Minimal context exposing the methods used by legacy factor calculators."""

    def __init__(self, db_manager: Any):
        self.db_manager = db_manager

    def query_dataframe(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        records = self.db_manager.fetch_sync(query, params)
        if records:
            return pd.DataFrame(records)
        return pd.DataFrame()


def create_factor_db_manager(database_url: Optional[str] = None) -> DBManager:
    """Create a sync DB manager for standalone production script execution."""
    connection_string = database_url or ConfigManager().get_database_url()
    if not connection_string:
        raise ValueError("数据库连接字符串未配置，请设置config.json或环境变量DATABASE_URL")
    return DBManager(connection_string, mode="sync")


def ensure_factor_context(
    context: Any = None,
    db_manager: Any = None,
    database_url: Optional[str] = None,
) -> Any:
    """Return an object with ``db_manager`` and ``query_dataframe`` attributes."""
    if context is not None:
        if hasattr(context, "query_dataframe") and hasattr(context, "db_manager"):
            return context
        if hasattr(context, "fetch_sync"):
            return FactorDBContext(context)
        if hasattr(context, "db_manager"):
            return FactorDBContext(context.db_manager)
        raise TypeError("context must expose query_dataframe/db_manager or be a DB manager")

    if db_manager is None:
        db_manager = create_factor_db_manager(database_url)

    return FactorDBContext(db_manager)
