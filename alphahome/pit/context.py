"""Context adapter for package-level PIT managers."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager


class PITContext:
    """Minimal context used by PIT managers and calculators."""

    def __init__(self, db_manager: Any = None, database_url: Optional[str] = None):
        self.db_manager = db_manager or self._create_db_manager(database_url)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query_dataframe(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        records = self.db_manager.fetch_sync(query, params)
        if records:
            return pd.DataFrame(records)
        return pd.DataFrame()

    def close(self) -> None:
        close_sync = getattr(self.db_manager, "close_sync", None)
        if callable(close_sync):
            close_sync()

    @staticmethod
    def _create_db_manager(database_url: Optional[str] = None) -> DBManager:
        connection_string = database_url or ConfigManager().get_database_url()
        if not connection_string:
            raise ValueError("数据库连接字符串未配置，请设置config.json或环境变量DATABASE_URL")
        return DBManager(connection_string, mode="sync")
