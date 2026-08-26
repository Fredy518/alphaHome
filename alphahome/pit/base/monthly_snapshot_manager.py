"""Shared mechanics for PIT month-end snapshot managers."""

from __future__ import annotations

import math
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
from psycopg2.extras import Json, execute_values

from .pit_config import PITConfig
from .pit_table_manager import PITTableManager


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


class PITMonthlySnapshotManager(PITTableManager):
    """Date planning and transactional replacement for monthly PIT tables."""

    DEFAULT_FULL_START = date(2014, 1, 31)

    def _apply_idempotent_table_ddl(self) -> None:
        """Apply the table's checked-in DDL even when the table already exists.

        ``PITTableManager._ensure_table_exists`` only creates missing tables. Monthly
        snapshot schemas also evolve through idempotent ``ALTER TABLE`` statements,
        so managers that add columns must call this helper before writing rows.
        """

        ddl_path = (
            Path(__file__).resolve().parents[1]
            / "database"
            / f"create_{self.table_name}_table.sql"
        )
        if not ddl_path.exists():
            raise FileNotFoundError(f"未找到月度PIT表DDL: {ddl_path}")
        self.context.db_manager.execute_sync(ddl_path.read_text(encoding="utf-8"))

    @staticmethod
    def latest_complete_month(today: date | None = None) -> date:
        current = today or datetime.now().date()
        return current.replace(day=1) - timedelta(days=1)

    @staticmethod
    def previous_month_end(value: date | str | pd.Timestamp) -> date:
        stamp = pd.Timestamp(value).normalize()
        return (stamp - pd.offsets.MonthEnd(1)).date()

    @staticmethod
    def next_month_end(value: date | str | pd.Timestamp) -> date:
        stamp = pd.Timestamp(value).normalize()
        return (stamp + pd.offsets.MonthEnd(1)).date()

    @staticmethod
    def as_month_end(value: date | str | pd.Timestamp) -> date:
        stamp = pd.Timestamp(value).normalize()
        return (stamp + pd.offsets.MonthEnd(0)).date()

    @classmethod
    def month_ends(
        cls,
        start_date: date | str | pd.Timestamp,
        end_date: date | str | pd.Timestamp,
    ) -> list[date]:
        start = cls.as_month_end(start_date)
        end = cls.as_month_end(end_date)
        if start > end:
            return []
        return [value.date() for value in pd.date_range(start, end, freq="ME")]

    @classmethod
    def incremental_months(
        cls,
        months: int,
        end_date: date | str | pd.Timestamp | None = None,
    ) -> list[date]:
        count = max(int(months), 1)
        end = cls.as_month_end(end_date or cls.latest_complete_month())
        start = (pd.Timestamp(end) - pd.offsets.MonthEnd(count - 1)).date()
        return cls.month_ends(start, end)

    def _atomic_replace_months(
        self,
        frame: pd.DataFrame,
        obs_dates: Sequence[date | str | pd.Timestamp],
        columns: Sequence[str],
        primary_keys: Sequence[str],
    ) -> int:
        """Validate staging and replace all requested months in one transaction."""

        normalized_dates = sorted({pd.Timestamp(value).date() for value in obs_dates})
        if not normalized_dates:
            return 0
        missing_columns = [column for column in columns if column not in frame.columns]
        if missing_columns and not frame.empty:
            raise ValueError(f"待写入数据缺少字段: {missing_columns}")

        data = frame.reindex(columns=columns).copy()
        if not data.empty:
            data["obs_date"] = pd.to_datetime(data["obs_date"], errors="coerce").dt.date
            unexpected_dates = sorted(
                set(data["obs_date"].dropna()) - set(normalized_dates)
            )
            if unexpected_dates:
                raise ValueError(f"staging 含目标范围外月份: {unexpected_dates}")
            if data[list(primary_keys)].isna().any(axis=None):
                raise ValueError("staging 主键存在空值")
            duplicates = int(data.duplicated(list(primary_keys), keep=False).sum())
            if duplicates:
                raise ValueError(f"staging 主键重复行: {duplicates}")

        schema = PITConfig.PIT_SCHEMA
        table = self.table_name
        relation = f"{_quote_identifier(schema)}.{_quote_identifier(table)}"
        staging = f"staging_{table}_{uuid.uuid4().hex}"
        quoted_staging = _quote_identifier(staging)
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        key_list = ", ".join(_quote_identifier(column) for column in primary_keys)

        connection = self.context.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TEMP TABLE {quoted_staging} "
                    f"(LIKE {relation} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP"
                )
                if not data.empty:
                    records = [
                        tuple(self._postgres_value(value) for value in row)
                        for row in data.itertuples(index=False, name=None)
                    ]
                    execute_values(
                        cursor,
                        f"INSERT INTO {quoted_staging} ({quoted_columns}) VALUES %s",
                        records,
                        page_size=max(int(self.batch_size or 1000), 1),
                    )

                cursor.execute(f"SELECT COUNT(*) FROM {quoted_staging}")
                staged_count = int(cursor.fetchone()[0])
                if staged_count != len(data):
                    raise RuntimeError(
                        f"staging 行数不一致: expected={len(data)}, actual={staged_count}"
                    )
                cursor.execute(
                    f"SELECT COUNT(*) FROM ("
                    f"SELECT {key_list}, COUNT(*) FROM {quoted_staging} "
                    f"GROUP BY {key_list} HAVING COUNT(*) > 1"
                    f") duplicate_keys"
                )
                duplicate_groups = int(cursor.fetchone()[0])
                if duplicate_groups:
                    raise RuntimeError(f"staging 主键重复组: {duplicate_groups}")

                cursor.execute(
                    f"DELETE FROM {relation} WHERE obs_date = ANY(%s)",
                    (normalized_dates,),
                )
                if staged_count:
                    cursor.execute(
                        f"INSERT INTO {relation} ({quoted_columns}) "
                        f"SELECT {quoted_columns} FROM {quoted_staging}"
                    )
            connection.commit()
            return staged_count
        except Exception:
            connection.rollback()
            raise

    @staticmethod
    def _postgres_value(value: Any) -> Any:
        if isinstance(value, (list, tuple, dict)):
            return Json(value)
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, np.generic):
            value = value.item()
        if value is None:
            return None
        try:
            if bool(pd.isna(value)):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value


__all__ = ["PITMonthlySnapshotManager"]
