from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd

from alphahome.common.config_manager import ConfigManager
from alphahome.common.logging_utils import get_logger

from .exceptions import (
    DolphinDBConnectionError,
    DolphinDBNotInstalledError,
    DolphinDBScriptError,
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class DolphinDBConfig:
    host: str = "localhost"
    port: int = 8848
    username: str = ""
    password: str = ""


class DolphinDBManager:
    """DolphinDB connection manager (single-node, lightweight).

    Designed for:
    - running scripts/queries
    - appending pandas DataFrames into DFS partitioned tables
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        *,
        session_factory=None,
    ):
        cfg = ConfigManager().get_dolphindb_config()
        self.config = DolphinDBConfig(
            host=host or cfg.get("host", "localhost"),
            port=int(port or cfg.get("port", 8848)),
            username=username if username is not None else cfg.get("username", ""),
            password=password if password is not None else cfg.get("password", ""),
        )

        self._session_factory = session_factory
        self._session = None

    @property
    def session(self):
        if self._session is None:
            raise DolphinDBConnectionError("DolphinDB session is not connected")
        return self._session

    def connect(self) -> bool:
        """Establish a DolphinDB session connection."""
        try:
            if self._session_factory is None:
                try:
                    import dolphindb as ddb  # type: ignore
                except Exception as e:  # pragma: no cover
                    raise DolphinDBNotInstalledError(
                        "Missing dependency: dolphindb. Install it via `pip install dolphindb`."
                    ) from e
                self._session_factory = ddb.session

            self._session = self._session_factory()
            if self.config.username or self.config.password:
                ok = self._session.connect(
                    self.config.host,
                    self.config.port,
                    self.config.username,
                    self.config.password,
                )
            else:
                ok = self._session.connect(self.config.host, self.config.port)

            # Some DolphinDB client versions may not raise on connection failure.
            # Treat an explicit False as failure and also verify by running a tiny script.
            if ok is False:
                self.close()
                raise DolphinDBConnectionError(
                    f"Failed to connect DolphinDB at {self.config.host}:{self.config.port} (connect returned False)"
                )

            try:
                self._session.run("1")
            except Exception as e:
                self.close()
                raise DolphinDBConnectionError(
                    f"Failed to connect DolphinDB at {self.config.host}:{self.config.port} (handshake failed)"
                ) from e

            logger.info(
                "Connected to DolphinDB %s:%s", self.config.host, self.config.port
            )
            return True
        except DolphinDBNotInstalledError:
            raise
        except Exception as e:
            raise DolphinDBConnectionError(
                f"Failed to connect DolphinDB at {self.config.host}:{self.config.port}: {e}"
            ) from e

    def close(self) -> None:
        """Close the DolphinDB session."""
        try:
            if self._session is not None:
                self._session.close()
        finally:
            self._session = None

    def run_script(self, script: str) -> Any:
        """Run a DolphinDB script (DolphinDB language)."""
        try:
            return self.session.run(script)
        except Exception as e:
            raise DolphinDBScriptError(f"Failed to run script: {e}") from e

    def query(self, sql_or_script: str) -> Any:
        """Execute a query/script and return DolphinDB Python client result (often a DataFrame)."""
        return self.run_script(sql_or_script)

    def get_max_trade_time(
        self, *, db_path: str, table_name: str, ts_code: str
    ) -> Optional[pd.Timestamp]:
        """Get max(trade_time) for a symbol in a DFS table.

        Returns:
            pandas.Timestamp (timezone-naive) or None when no rows exist.
        """
        script = (
            f'select max(trade_time) as max_tt from loadTable("{db_path}", "{table_name}") '
            f'where ts_code = "{ts_code}"'
        )
        result = self.session.run(script)

        if result is None:
            return None
        if isinstance(result, pd.DataFrame):
            if result.empty or "max_tt" not in result.columns:
                return None
            val = result.iloc[0]["max_tt"]
        elif isinstance(result, dict) and "max_tt" in result:
            val = result["max_tt"]
        else:
            # Best effort for client variations
            try:
                val = result[0]["max_tt"]  # type: ignore[index]
            except Exception:
                return None

        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        ts = pd.to_datetime(val, errors="coerce")
        if pd.isna(ts):
            return None
        # Ensure naive timestamp for comparisons with local Hikyuu timestamps
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_convert(None)  # type: ignore[union-attr]
        return ts

    def append_dataframe(self, df: pd.DataFrame, *, db_path: str, table_name: str) -> Any:
        """Append a pandas DataFrame into a DFS table using `tableInsert`."""
        if df is None or df.empty:
            return 0

        try:
            # DolphinDB variable name must be a qualified identifier; avoid leading "__".
            var_name = "alphahome_df"
            self.session.upload({var_name: df})
            script = (
                f'tableInsert{{loadTable("{db_path}", "{table_name}")}}({var_name})'
            )
            return self.session.run(script)
        except Exception as e:
            raise DolphinDBScriptError(f"Failed to append DataFrame: {e}") from e

