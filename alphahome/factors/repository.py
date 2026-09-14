"""Database queries shared by factor planning, audit and repair workflows."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .base import FactorTaskContract
from .date_policy import FactorDatePolicy
from .source_boundary import SNAPSHOT_XMIN_KEY


_RELATION_RE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
_SOURCE_TIME_KEYS = {
    "pit.pit_financial_indicators": "ann_date",
    "pit.pit_industry_classification": "obs_date",
    "factors.p_factor": "calc_date",
}


class FactorSourceQueryError(RuntimeError):
    """A failed dirty-source check must never be interpreted as no changes."""

    def __init__(self, source: str, reason: str):
        self.source = source
        self.reason = reason
        super().__init__(f"dirty_source_query_failed:{source}:{reason}")


class FactorRepository:
    def __init__(self, db_manager: Any):
        if not hasattr(db_manager, "fetch_sync"):
            raise TypeError("FactorRepository需要同步DBManager")
        self.db = db_manager

    @staticmethod
    def _relation(value: str) -> str:
        if not _RELATION_RE.fullmatch(value):
            raise ValueError(f"非法relation: {value}")
        return value

    def relation_exists(self, relation: str) -> bool:
        return bool(
            self.db.fetch_val_sync("SELECT to_regclass(%s) IS NOT NULL", (relation,))
        )

    def table_date_stats(self, contract: FactorTaskContract) -> Dict[str, Any]:
        relation = self._relation(contract.output_table)
        if not self.relation_exists(relation):
            return {
                "row_count": 0,
                "distinct_date_count": 0,
                "first_calc_date": None,
                "latest_calc_date": None,
                "nonstandard_date_count": 0,
                "latest_date_row_count": 0,
            }
        row = (
            self.db.fetch_one_sync(
                f"""
            SELECT COUNT(*) AS row_count,
                   COUNT(DISTINCT calc_date) AS distinct_date_count,
                   MIN(calc_date) AS first_calc_date,
                   MAX(calc_date) AS latest_calc_date,
                   COUNT(DISTINCT calc_date) FILTER (
                       WHERE EXTRACT(ISODOW FROM calc_date) <> 5
                   ) AS nonstandard_date_count
            FROM {relation}
            """
            )
            or {}
        )
        latest = row.get("latest_calc_date")
        latest_rows = 0
        if latest is not None:
            latest_rows = int(
                self.db.fetch_val_sync(
                    f"SELECT COUNT(*) FROM {relation} WHERE calc_date = %s",
                    (latest,),
                )
                or 0
            )
        row["latest_date_row_count"] = latest_rows
        return dict(row)

    def existing_dates(
        self,
        contract: FactorTaskContract,
        start_date: date,
        end_date: date,
    ) -> set[date]:
        relation = self._relation(contract.output_table)
        if not self.relation_exists(relation):
            return set()
        rows = self.db.fetch_sync(
            f"""
            SELECT DISTINCT calc_date
            FROM {relation}
            WHERE calc_date BETWEEN %s AND %s
              AND EXTRACT(ISODOW FROM calc_date) = 5
            """,
            (start_date, end_date),
        )
        return {row["calc_date"] for row in rows}

    def row_counts_by_date(
        self,
        contract: FactorTaskContract,
        dates: Sequence[date],
    ) -> Dict[date, int]:
        relation = self._relation(contract.output_table)
        if not dates or not self.relation_exists(relation):
            return {}
        rows = self.db.fetch_sync(
            f"""
            SELECT calc_date, COUNT(*) AS row_count
            FROM {relation}
            WHERE calc_date = ANY(%s::date[])
            GROUP BY calc_date
            """,
            ([value.isoformat() for value in dates],),
        )
        return {row["calc_date"]: int(row["row_count"]) for row in rows}

    def missing_dates(
        self,
        contract: FactorTaskContract,
        start_date: date,
        end_date: date,
    ) -> List[date]:
        expected = FactorDatePolicy.fridays(start_date, end_date)
        existing = self.existing_dates(contract, start_date, end_date)
        expected_no_data = self.expected_no_data_dates(
            contract.task_name, start_date, end_date
        )
        return [
            value
            for value in expected
            if value not in existing and value not in expected_no_data
        ]

    def expected_no_data_dates(
        self,
        task_name: str,
        start_date: date,
        end_date: date,
    ) -> set[date]:
        """Return dates whose latest ledger outcome is an evidenced empty source."""
        if not self.relation_exists("factors.factor_run_date"):
            return set()
        rows = self.db.fetch_sync(
            """
            WITH latest AS (
                SELECT DISTINCT ON (calc_date) calc_date, status
                FROM factors.factor_run_date
                WHERE task_name = %s
                  AND calc_date BETWEEN %s AND %s
                ORDER BY calc_date, created_at DESC, run_id DESC
            )
            SELECT calc_date
            FROM latest
            WHERE status = 'expected_no_data'
            """,
            (task_name, start_date, end_date),
        )
        return {row["calc_date"] for row in rows}

    def first_source_date(self, contract: FactorTaskContract) -> Optional[date]:
        candidates: List[date] = []
        for source in contract.source_tables:
            if not self.relation_exists(source):
                continue
            key = _SOURCE_TIME_KEYS.get(source)
            if not key:
                continue
            relation = self._relation(source)
            value = self.db.fetch_val_sync(f"SELECT MIN({key}) FROM {relation}")
            if isinstance(value, datetime):
                value = value.date()
            if isinstance(value, date):
                candidates.append(value)
        return min(candidates) if candidates else None

    def source_watermarks(self, contract: FactorTaskContract) -> Dict[str, Any]:
        watermarks: Dict[str, Any] = {}
        for source in contract.source_tables:
            if not self.relation_exists(source):
                watermarks[source] = None
                continue
            schema, table = source.split(".", 1)
            has_updated_at = bool(
                self.db.fetch_val_sync(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                          AND column_name = 'updated_at'
                    )
                    """,
                    (schema, table),
                )
            )
            if not has_updated_at:
                watermarks[source] = None
                continue
            watermarks[source] = self.db.fetch_val_sync(
                f"SELECT MAX(updated_at) FROM {self._relation(source)}"
            )
        return watermarks

    def snapshot_xmin(self) -> int:
        return int(self.db.fetch_val_sync("SELECT pg_snapshot_xmin(pg_current_snapshot())::text"))

    def dirty_start_date(
        self,
        contract: FactorTaskContract,
        previous_watermarks: Mapping[str, Any],
    ) -> Optional[date]:
        candidates: List[date] = []
        checkpoint = previous_watermarks.get(SNAPSHOT_XMIN_KEY)
        if checkpoint is not None:
            from alphahome.common.db_session import query_timeout

            try:
                current = int(self.db.fetch_val_sync("SELECT pg_snapshot_xmax(pg_current_snapshot())::text"))
            except Exception as exc:
                raise FactorSourceQueryError("snapshot", type(exc).__name__) from None
            if not isinstance(checkpoint, int) or not 0 <= current - checkpoint < 2**31:
                raise FactorSourceQueryError("snapshot", "expired_or_invalid_mvcc_cursor")
        for source in contract.source_tables:
            previous = previous_watermarks.get(source)
            key = _SOURCE_TIME_KEYS.get(source)
            if (not previous and checkpoint is None) or not key:
                continue
            relation = self._relation(source)
            try:
                if not self.relation_exists(source):
                    raise FactorSourceQueryError(source, "missing_relation")
                if checkpoint is None:
                    value = self.db.fetch_val_sync(
                        f"SELECT MIN({key}) FROM {relation} WHERE updated_at > %s", (previous,),
                    )
                else:
                    with query_timeout(self.db):
                        value = self.db.fetch_val_sync(
                            f"SELECT MIN({key}) FROM {relation} WHERE updated_at > %s "
                            "OR age(xmin) <= age(%s::text::xid)",
                            (previous, str(checkpoint % (2**32))),
                        )
            except FactorSourceQueryError:
                raise
            except Exception as exc:
                # Keep source and error category, without echoing driver SQL/DSNs.
                raise FactorSourceQueryError(source, type(exc).__name__) from None
            if isinstance(value, datetime):
                value = value.date()
            if isinstance(value, date):
                candidates.append(value)
            elif value is not None:
                raise FactorSourceQueryError(source, "invalid_date_result")
        return min(candidates) if candidates else None

    def readiness(
        self,
        contract: FactorTaskContract,
        cutoff_date: date,
        planned_dates: Sequence[date],
        dependency_plans: Mapping[str, Sequence[date]],
    ) -> List[str]:
        blockers: List[str] = []
        for source in contract.source_tables:
            if not self.relation_exists(source):
                blockers.append(f"missing_relation:{source}")
        if contract.task_name == "factor_p" and self.relation_exists(
            "pit.pit_financial_indicators"
        ):
            eligible = self.db.fetch_val_sync(
                """
                SELECT COUNT(*)
                FROM pit.pit_financial_indicators
                WHERE ann_date <= %s
                  AND calculation_status = 'success'
                """,
                (cutoff_date,),
            )
            if not eligible:
                blockers.append("pit_financial_indicators:no_eligible_rows")
            if "pit_financial_indicators" in contract.readiness_dependencies:
                report = self.financial_input_gaps(cutoff_date)
                if report.get("status") != "checked":
                    blockers.append("pit_input_eligibility:unverified")
                elif report["eligible_missing"]:
                    blockers.append(f"pit_input_eligibility:missing={report['eligible_missing']}")
        if contract.task_name == "factor_g":
            p_planned = set(dependency_plans.get("factor_p") or ())
            if self.relation_exists("factors.p_factor"):
                for calc_date in planned_dates:
                    if calc_date in p_planned:
                        continue
                    exists = self.db.fetch_val_sync(
                        "SELECT EXISTS(SELECT 1 FROM factors.p_factor WHERE calc_date = %s)",
                        (calc_date,),
                    )
                    if not exists:
                        blockers.append(f"missing_same_date_p:{calc_date.isoformat()}")
        return blockers

    def financial_input_gaps(self, cutoff_date):
        from alphahome.pit.eligibility import INPUT_RELATIONS, financial_input_gap_sql

        missing = [source for source in INPUT_RELATIONS if not self.relation_exists(source)]
        if missing:
            return {"status": "unverified", "missing_relations": missing}
        row = self.db.fetch_one_sync(financial_input_gap_sql(), (cutoff_date,))
        if not row or "eligible_missing" not in row:
            return {"status": "unverified"}
        return {"status": "checked", **{key: int(value or 0) for key, value in row.items()}}

    def changed_p_dates_since(self, watermark: Any, cutoff_date: date) -> List[date]:
        if not watermark or not self.relation_exists("factors.p_factor"):
            return []
        rows = self.db.fetch_sync(
            """
            SELECT DISTINCT calc_date
            FROM factors.p_factor
            WHERE updated_at > %s
              AND calc_date <= %s
              AND EXTRACT(ISODOW FROM calc_date) = 5
            ORDER BY calc_date
            """,
            (watermark, cutoff_date),
        )
        return [row["calc_date"] for row in rows]


__all__ = ["FactorRepository"]
