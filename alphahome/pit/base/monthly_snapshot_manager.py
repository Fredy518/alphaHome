"""Shared mechanics for PIT month-end snapshot managers."""

from __future__ import annotations

import math
import uuid
from datetime import date, timedelta
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

    def install_table_ddl(self) -> None:
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
        from ..planning_time import business_date

        current = today or business_date()
        return current.replace(day=1) - timedelta(days=1)

    @classmethod
    def complete_month_cutoff(
        cls, cutoff_date: date | str | pd.Timestamp | None = None
    ) -> date:
        """Return the latest complete month, optionally capped by a batch cutoff."""

        latest = cls.latest_complete_month()
        if cutoff_date is None:
            return latest
        return min(cls.as_month_end(cutoff_date), latest)

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
        *,
        expected_empty_months: dict[date, str] | None = None,
        scope_filters: dict[str, Any] | None = None,
        required_scope_columns: Sequence[str] = (),
    ) -> int:
        """Validate staging and replace requested months in one transaction.

        ``scope_filters`` narrows deletion for shared physical tables.  It is
        validated against staging before any database statement, so a custom
        scoped writer cannot bypass month-completion proof or publish rows into
        a different method/index scope.  ``required_scope_columns`` additionally
        proves that every requested month contains every requested scope value.
        """

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

        normalized_scope: list[tuple[str, str, Any]] = []
        for column, expected in (scope_filters or {}).items():
            if column not in columns:
                raise ValueError(f"删除范围字段不在写入列中: {column}")
            if isinstance(expected, (list, tuple, set, frozenset)):
                values = list(dict.fromkeys(expected))
                if not values:
                    raise ValueError(f"pit_empty_scope: {column}")
                if not data.empty:
                    unexpected = set(data[column].dropna()) - set(values)
                    if unexpected:
                        raise ValueError(
                            f"staging 含删除范围外 {column}: {sorted(unexpected)}"
                        )
                normalized_scope.append((column, "any", values))
            else:
                if expected is None:
                    raise ValueError(f"pit_empty_scope: {column}")
                if not data.empty and not bool(data[column].eq(expected).all()):
                    raise ValueError(f"staging 含删除范围外 {column}")
                normalized_scope.append((column, "eq", expected))

        present = set(data['obs_date']) if not data.empty else set()
        empty_reasons = {pd.Timestamp(key).date(): value for key, value in (expected_empty_months or {}).items()}
        missing = set(normalized_dates) - present
        unproven = sorted(month for month in missing if not isinstance(empty_reasons.get(month), str)
                          or not empty_reasons[month].strip())
        if unproven:
            raise ValueError(f"pit_incomplete_months: {unproven}; an empty partition requires an explicit expected-no-data reason")

        for column in required_scope_columns:
            matching = [item for item in normalized_scope if item[0] == column]
            if len(matching) != 1 or matching[0][1] != "any":
                raise ValueError(
                    f"完整性范围字段必须使用非空序列过滤: {column}"
                )
            expected_values = matching[0][2]
            observed_pairs = (
                set(zip(data["obs_date"], data[column])) if not data.empty else set()
            )
            missing_pairs = [
                (month, value)
                for month in normalized_dates
                for value in expected_values
                if month not in missing and (month, value) not in observed_pairs
            ]
            if missing_pairs:
                raise ValueError(
                    f"pit_incomplete_scope: {column} missing {missing_pairs}"
                )

        schema = PITConfig.PIT_SCHEMA
        table = self.table_name
        relation = f"{_quote_identifier(schema)}.{_quote_identifier(table)}"
        staging = f"staging_{table}_{uuid.uuid4().hex}"
        quoted_staging = _quote_identifier(staging)
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        key_list = ", ".join(_quote_identifier(column) for column in primary_keys)
        delete_predicates = ["obs_date = ANY(%s)"]
        delete_params: list[Any] = [normalized_dates]
        for column, operator, value in normalized_scope:
            delete_predicates.append(
                f"{_quote_identifier(column)} = {'ANY(%s)' if operator == 'any' else '%s'}"
            )
            delete_params.append(value)

        connection = self.context.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '30s'")
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                               ('alphahome.pit', f'{schema}.{table}'))
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
                    f"DELETE FROM {relation} WHERE " + " AND ".join(delete_predicates),
                    tuple(delete_params),
                )
                if staged_count:
                    cursor.execute(
                        f"INSERT INTO {relation} ({quoted_columns}) "
                        f"SELECT {quoted_columns} FROM {quoted_staging}"
                    )
            connection.commit()
            self._verified_replacement_months = sorted(
                set(getattr(self, '_verified_replacement_months', ())) | set(normalized_dates)
            )
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
