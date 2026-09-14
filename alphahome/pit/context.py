"""Context adapter for package-level PIT managers."""

from __future__ import annotations

from typing import Any, Optional
from threading import get_ident

import pandas as pd

from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager


class PITContext:
    """Minimal context used by PIT managers and calculators."""

    def __init__(self, db_manager: Any = None, database_url: Optional[str] = None):
        if db_manager is not None and database_url is not None:
            raise ValueError("Specify a borrowed manager or a database URL, not both")
        self._owner_thread = get_ident()
        self._owns_manager = db_manager is None
        self._closed = False
        self._db_manager = db_manager if db_manager is not None else self._create_db_manager(database_url)

    def _check_thread(self) -> None:
        if get_ident() != self._owner_thread:
            raise RuntimeError("PITContext must be used and closed in its creating thread")

    @property
    def db_manager(self):
        self._check_thread()
        if self._closed:
            raise RuntimeError("PITContext is closed")
        return self._db_manager

    def __enter__(self):
        self._check_thread()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query_dataframe(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        records = self.db_manager.fetch_sync(query, params)
        if records:
            return pd.DataFrame(records)
        return pd.DataFrame()

    def close(self) -> None:
        self._check_thread()
        if self._closed:
            return
        if self._owns_manager:
            close_sync = getattr(self._db_manager, "close_sync", None)
            if callable(close_sync):
                close_sync()
        self._closed = True

    @staticmethod
    def _create_db_manager(database_url: Optional[str] = None) -> DBManager:
        connection_string = database_url or ConfigManager().get_database_url()
        if not connection_string:
            raise ValueError("数据库连接字符串未配置，请设置config.json或环境变量DATABASE_URL")
        return DBManager(connection_string, mode="sync")
