"""Recoverable repair workflow for historical P/G cadence contamination."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence
from uuid import UUID, uuid4

import pandas as pd
from psycopg2.extras import Json

from alphahome.common.db_manager import DBManager

from .core import GFactorCalculator, PFactorCalculator
from .core.context import FactorDBContext
from .coordinator import FactorCoordinator
from .date_policy import FACTOR_TIMEZONE, FactorDatePolicy, coerce_date
from .governance import FactorGovernanceStore, json_ready
from .persistence import (
    G_FACTOR_COLUMNS,
    P_FACTOR_COLUMNS,
    factor_frame_checksum,
)
from .repository import FactorRepository
from .validation import validate_factor_frame


REPAIR_WINDOW_START = date(2025, 10, 3)


class FactorRepairService:
    """Archive every touched date and make a repair reversible by repair_id."""

    def __init__(self, db_manager: Any):
        if not hasattr(db_manager, "_get_sync_connection"):
            raise TypeError("FactorRepairService需要同步DBManager")
        self.db = db_manager
        self.governance = FactorGovernanceStore(db_manager)
        self.policy = FactorDatePolicy()
        self.logger = logging.getLogger("FactorRepairService")

    def plan(self, cutoff_date: date | str | None = None) -> Dict[str, Any]:
        self.governance.ensure_schema()
        automatic = self.policy.automatic_cutoff()
        cutoff = FactorDatePolicy.require_valid(cutoff_date or automatic)
        if cutoff > automatic:
            raise ValueError(f"修复截止日不能晚于最近完整周五: {automatic}")
        result: Dict[str, Any] = {
            "status": "dry_run",
            "apply": False,
            "effective_cutoff_date": cutoff.isoformat(),
            "repair_window_start": REPAIR_WINDOW_START.isoformat(),
            "tasks": {},
        }
        for factor_type in ("p", "g"):
            table = f"{factor_type}_factor"
            anomalies = self.db.fetch_sync(
                f"""
                SELECT calc_date, COUNT(*) AS row_count
                FROM factors.{table}
                WHERE EXTRACT(ISODOW FROM calc_date) <> 5
                GROUP BY calc_date
                ORDER BY calc_date
                """
            )
            window_dates = FactorDatePolicy.fridays(REPAIR_WINDOW_START, cutoff)
            existing = {
                row["calc_date"]
                for row in self.db.fetch_sync(
                    f"""
                    SELECT DISTINCT calc_date FROM factors.{table}
                    WHERE calc_date BETWEEN %s AND %s
                      AND EXTRACT(ISODOW FROM calc_date) = 5
                    """,
                    (REPAIR_WINDOW_START, cutoff),
                )
            }
            result["tasks"][factor_type] = {
                "non_friday_dates": [
                    {
                        "calc_date": row["calc_date"].isoformat(),
                        "row_count": int(row["row_count"]),
                    }
                    for row in anomalies
                ],
                "non_friday_date_count": len(anomalies),
                "non_friday_row_count": sum(int(row["row_count"]) for row in anomalies),
                "window_friday_count": len(window_dates),
                "window_missing_dates": [
                    value.isoformat() for value in window_dates if value not in existing
                ],
            }
        result["early_p_missing_dates"] = [
            value.isoformat() for value in self._early_p_missing_dates()
        ]
        result["acceptance"] = self.acceptance(cutoff)
        return result

    def apply(self, cutoff_date: date | str | None = None) -> Dict[str, Any]:
        preflight = self.plan(cutoff_date)
        cutoff = coerce_date(preflight["effective_cutoff_date"])
        source_cutoff_at = datetime.now(FACTOR_TIMEZONE)
        repair_id = uuid4()
        self.governance.ensure_schema()
        self._ensure_repair_tables()
        self._insert_manifest(
            repair_id,
            "running",
            source_cutoff_at,
            cutoff,
            details={"preflight": preflight},
        )
        run_id: Optional[UUID] = None
        try:
            # Install the NOT VALID constraints inside the guarded section so a
            # partial startup failure is recoverable through the same repair_id.
            self._install_weekday_constraints(validate=False)
            run_id = self.governance.start_run(
                ["factor_p", "factor_g"],
                "repair",
                cutoff,
                requested_start_date=REPAIR_WINDOW_START,
                requested_end_date=cutoff,
                formula_versions={"factor_p": "v2.0", "factor_g": "v1.1"},
                config={"repair_id": str(repair_id), "shadow_compare_p": True},
                details={"repair_id": str(repair_id)},
            )
            self.db.execute_sync(
                """
                UPDATE factors.factor_repair_manifest
                SET details_json = details_json || %s
                WHERE repair_id = %s
                """,
                (Json({"factor_run_id": str(run_id)}), str(repair_id)),
            )
            summary: Dict[str, Any] = {
                "repair_id": str(repair_id),
                "factor_run_id": str(run_id),
                "effective_cutoff_date": cutoff.isoformat(),
                "p_replaced_dates": [],
                "p_unchanged_dates": [],
                "p_expected_no_data_dates": [],
                "p_removed_no_data_dates": [],
                "g_rebuilt_dates": [],
                "deleted_non_friday_dates": {"p": [], "g": []},
            }
            for factor_type in ("p", "g"):
                anomalies = [
                    row["calc_date"]
                    for row in self.db.fetch_sync(
                        f"""
                        SELECT DISTINCT calc_date
                        FROM factors.{factor_type}_factor
                        WHERE EXTRACT(ISODOW FROM calc_date) <> 5
                        ORDER BY calc_date
                        """
                    )
                ]
                for calc_date_value in anomalies:
                    self._prepare_date(
                        repair_id,
                        factor_type,
                        calc_date_value,
                        "delete_non_friday",
                    )
                    self._delete_date(factor_type, calc_date_value)
                    self._complete_date(
                        repair_id, factor_type, calc_date_value, 0, None
                    )
                    summary["deleted_non_friday_dates"][factor_type].append(
                        calc_date_value.isoformat()
                    )

            p_dates = FactorDatePolicy.fridays(REPAIR_WINDOW_START, cutoff)
            p_dates.extend(self._early_p_missing_dates())
            self._repair_p_dates(repair_id, run_id, sorted(set(p_dates)), summary)
            self._rebuild_g_dates(
                repair_id,
                run_id,
                FactorDatePolicy.fridays(REPAIR_WINDOW_START, cutoff),
                summary,
            )
            self._install_weekday_constraints(validate=True)
            acceptance = self.acceptance(cutoff)
            if not acceptance["passed"]:
                raise RuntimeError(f"修复验收失败: {acceptance}")
            summary["acceptance"] = acceptance
            contracts = FactorCoordinator.contracts()
            final_watermarks = {
                name: FactorRepository(self.db).source_watermarks(contracts[name])
                for name in ("factor_p", "factor_g")
            }
            self.governance.finish_run(
                run_id,
                "success",
                details=summary,
                source_watermarks=final_watermarks,
            )
            self._finish_manifest(repair_id, "success", summary)
            return {"status": "success", **summary}
        except Exception as exc:
            self.logger.error(
                "因子修复失败，开始按repair_id回滚: %s", exc, exc_info=True
            )
            rollback = self.rollback(repair_id, reason=str(exc))
            if run_id is not None:
                self.governance.finish_run(
                    run_id,
                    "rolled_back_failed",
                    details={"error": str(exc), "rollback": rollback},
                )
            raise

    def rollback(
        self, repair_id: UUID | str, *, reason: str = "manual_rollback"
    ) -> Dict[str, Any]:
        repair_uuid = str(repair_id)
        self.governance.ensure_schema()
        self._ensure_repair_tables()
        manifest = self.db.fetch_one_sync(
            """
            SELECT status, details_json
            FROM factors.factor_repair_manifest
            WHERE repair_id = %s
            """,
            (repair_uuid,),
        )
        if not manifest:
            raise ValueError(f"未知repair_id，拒绝修改生产约束: {repair_uuid}")
        if manifest.get("status") == "rolled_back":
            return {
                "status": "already_rolled_back",
                "repair_id": repair_uuid,
                "restored": {"p": 0, "g": 0},
            }
        manifest_details = manifest.get("details_json") or {}
        factor_run_id = (
            manifest_details.get("factor_run_id")
            if isinstance(manifest_details, Mapping)
            else None
        )
        rows = self.db.fetch_sync(
            """
            SELECT task_name, calc_date
            FROM factors.factor_repair_date
            WHERE repair_id = %s
            ORDER BY CASE task_name WHEN 'factor_g' THEN 1 ELSE 2 END,
                     calc_date DESC
            """,
            (repair_uuid,),
        )
        restored: Dict[str, int] = {"p": 0, "g": 0}
        connection = self.db._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                for factor_type in ("p", "g"):
                    table = f"{factor_type}_factor"
                    cursor.execute(
                        f"""
                        ALTER TABLE factors.{table}
                        DROP CONSTRAINT IF EXISTS ck_{table}_calc_date_friday
                        """
                    )
                for row in rows:
                    factor_type = "p" if row["task_name"] == "factor_p" else "g"
                    table = f"{factor_type}_factor"
                    archive = f"{table}_repair_archive"
                    columns = self._business_columns(table)
                    quoted = ", ".join(f'"{column}"' for column in columns)
                    cursor.execute(
                        f"DELETE FROM factors.{table} WHERE calc_date = %s",
                        (row["calc_date"],),
                    )
                    cursor.execute(
                        f"""
                        INSERT INTO factors.{table} ({quoted})
                        SELECT {quoted}
                        FROM factors.{archive}
                        WHERE repair_id = %s AND calc_date = %s
                        """,
                        (repair_uuid, row["calc_date"]),
                    )
                    restored[factor_type] += cursor.rowcount
                    cursor.execute(
                        """
                        UPDATE factors.factor_repair_date
                        SET status = 'rolled_back', updated_at = CURRENT_TIMESTAMP
                        WHERE repair_id = %s AND task_name = %s AND calc_date = %s
                        """,
                        (repair_uuid, row["task_name"], row["calc_date"]),
                    )
                cursor.execute(
                    """
                    UPDATE factors.factor_run_date
                    SET is_current = FALSE
                    WHERE run_id = (
                        SELECT (details_json->>'factor_run_id')::uuid
                        FROM factors.factor_repair_manifest
                        WHERE repair_id = %s
                    )
                    """,
                    (repair_uuid,),
                )
                if factor_run_id:
                    cursor.execute(
                        """
                        WITH previous AS (
                            SELECT DISTINCT ON (task_name, calc_date)
                                   run_id, task_name, calc_date
                            FROM factors.factor_run_date
                            WHERE run_id <> %s
                              AND (task_name, calc_date) IN (
                                  SELECT task_name, calc_date
                                  FROM factors.factor_run_date
                                  WHERE run_id = %s
                              )
                              AND status IN ('success', 'shadow_unchanged')
                            ORDER BY task_name, calc_date, created_at DESC, run_id DESC
                        )
                        UPDATE factors.factor_run_date ledger
                        SET is_current = TRUE
                        FROM previous
                        WHERE ledger.run_id = previous.run_id
                          AND ledger.task_name = previous.task_name
                          AND ledger.calc_date = previous.calc_date
                        """,
                        (str(factor_run_id), str(factor_run_id)),
                    )
                    cursor.execute(
                        """
                        UPDATE factors.factor_run
                        SET status = 'rolled_back',
                            details_json = details_json || %s::jsonb,
                            finished_at = CURRENT_TIMESTAMP
                        WHERE run_id = %s
                        """,
                        (
                            json.dumps({"rollback_reason": reason}, ensure_ascii=False),
                            str(factor_run_id),
                        ),
                    )
                cursor.execute(
                    """
                    UPDATE factors.factor_repair_manifest
                    SET status = 'rolled_back',
                        details_json = details_json || %s::jsonb,
                        finished_at = CURRENT_TIMESTAMP
                    WHERE repair_id = %s
                    """,
                    (
                        json.dumps({"rollback_reason": reason}, ensure_ascii=False),
                        repair_uuid,
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return {"status": "rolled_back", "repair_id": repair_uuid, "restored": restored}

    def acceptance(self, cutoff_date: date | str) -> Dict[str, Any]:
        cutoff = coerce_date(cutoff_date)
        checks: Dict[str, Any] = {}
        for factor_type in ("p", "g"):
            table = f"{factor_type}_factor"
            checks[f"{factor_type}_non_friday_rows"] = int(
                self.db.fetch_val_sync(
                    f"SELECT COUNT(*) FROM factors.{table} WHERE EXTRACT(ISODOW FROM calc_date) <> 5"
                )
                or 0
            )
            checks[f"{factor_type}_null_keys"] = int(
                self.db.fetch_val_sync(
                    f"SELECT COUNT(*) FROM factors.{table} WHERE ts_code IS NULL OR calc_date IS NULL"
                )
                or 0
            )
            checks[f"{factor_type}_duplicate_keys"] = int(
                self.db.fetch_val_sync(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT ts_code, calc_date FROM factors.{table}
                        GROUP BY ts_code, calc_date HAVING COUNT(*) > 1
                    ) duplicate_groups
                    """
                )
                or 0
            )
            checks[f"{factor_type}_pit_violations"] = int(
                self.db.fetch_val_sync(
                    f"SELECT COUNT(*) FROM factors.{table} WHERE ann_date > calc_date"
                )
                or 0
            )
            score_column = "p_score" if factor_type == "p" else "g_score"
            checks[f"{factor_type}_score_violations"] = int(
                self.db.fetch_val_sync(
                    f"""
                    SELECT COUNT(*) FROM factors.{table}
                    WHERE {score_column} IS NULL
                       OR {score_column} < 0
                       OR {score_column} > 100
                    """
                )
                or 0
            )
            missing = self.db.fetch_sync(
                f"""
                WITH expected AS (
                    SELECT value::date AS calc_date
                    FROM generate_series(%s::date, %s::date, interval '1 day') value
                    WHERE EXTRACT(ISODOW FROM value) = 5
                ), actual AS (
                    SELECT DISTINCT calc_date FROM factors.{table}
                ), ledger AS (
                    SELECT DISTINCT ON (calc_date) calc_date, status
                    FROM factors.factor_run_date
                    WHERE task_name = %s
                      AND calc_date BETWEEN %s::date AND %s::date
                    ORDER BY calc_date, created_at DESC, run_id DESC
                )
                SELECT expected.calc_date FROM expected
                LEFT JOIN actual USING (calc_date)
                LEFT JOIN ledger USING (calc_date)
                WHERE actual.calc_date IS NULL
                  AND ledger.status IS DISTINCT FROM 'expected_no_data'
                ORDER BY expected.calc_date
                """,
                (
                    REPAIR_WINDOW_START,
                    cutoff,
                    f"factor_{factor_type}",
                    REPAIR_WINDOW_START,
                    cutoff,
                ),
            )
            checks[f"{factor_type}_window_missing_dates"] = [
                row["calc_date"].isoformat() for row in missing
            ]
        checks["g_eligibility_set_mismatches"] = int(
            self.db.fetch_val_sync(
                """
                SELECT COUNT(*) FROM (
                    SELECT COALESCE(p.calc_date, g.calc_date) AS calc_date,
                           COALESCE(p.ts_code, g.ts_code) AS ts_code
                    FROM (
                        SELECT ts_code, calc_date
                        FROM factors.p_factor
                        WHERE calc_date BETWEEN %s AND %s
                    ) p
                    FULL OUTER JOIN (
                        SELECT ts_code, calc_date
                        FROM factors.g_factor
                        WHERE calc_date BETWEEN %s AND %s
                    ) g USING (ts_code, calc_date)
                    WHERE p.ts_code IS NULL OR g.ts_code IS NULL
                ) mismatches
                """,
                (REPAIR_WINDOW_START, cutoff, REPAIR_WINDOW_START, cutoff),
            )
            or 0
        )
        if self.db.fetch_val_sync(
            "SELECT to_regclass('pgs_factors.p_factor') IS NOT NULL"
        ):
            checks["p_compat_count_delta"] = int(
                self.db.fetch_val_sync(
                    """
                    SELECT (SELECT COUNT(*) FROM pgs_factors.p_factor)
                         - (SELECT COUNT(*) FROM factors.p_factor)
                    """
                )
                or 0
            )
            checks["g_compat_count_delta"] = int(
                self.db.fetch_val_sync(
                    """
                    SELECT (SELECT COUNT(*) FROM pgs_factors.g_factor)
                         - (SELECT COUNT(*) FROM factors.g_factor)
                    """
                )
                or 0
            )
            for factor_type, score_column in (("p", "p_score"), ("g", "g_score")):
                base_checksum = self._date_summary_checksum(
                    f"factors.{factor_type}_factor", score_column
                )
                compat_checksum = self._date_summary_checksum(
                    f"pgs_factors.{factor_type}_factor", score_column
                )
                checks[f"{factor_type}_compat_checksum_match"] = (
                    base_checksum == compat_checksum
                )
                checks[f"{factor_type}_compat_checksum"] = compat_checksum
        blocking = []
        for key, value in checks.items():
            if key.endswith("_checksum_match"):
                if value is not True:
                    blocking.append(key)
            elif key.endswith("window_missing_dates"):
                if value:
                    blocking.append(key)
            elif isinstance(value, int) and not isinstance(value, bool) and value != 0:
                blocking.append(key)
        return {"passed": not blocking, "blocking_checks": blocking, "checks": checks}

    def _date_summary_checksum(self, relation: str, score_column: str) -> str:
        return str(
            self.db.fetch_val_sync(
                f"""
                WITH dates AS (
                    SELECT calc_date, COUNT(*) AS row_count,
                           ROUND(SUM({score_column})::numeric, 6) AS score_sum
                    FROM {relation}
                    GROUP BY calc_date
                )
                SELECT md5(COALESCE(string_agg(
                    calc_date::text || ':' || row_count::text || ':' ||
                    COALESCE(score_sum::text, 'NULL'),
                    '|' ORDER BY calc_date
                ), ''))
                FROM dates
                """
            )
            or ""
        )

    def _repair_p_dates(
        self,
        repair_id: UUID,
        run_id: UUID,
        dates: Sequence[date],
        summary: Dict[str, Any],
    ) -> None:
        read_db = self._open_snapshot_db()
        calculator = PFactorCalculator(
            context=FactorDBContext(read_db),
            config={
                "factor_run_id": str(run_id),
                "factor_task_name": "factor_p",
                "formula_version": "v2.0",
                "factor_writer_db_manager": self.db,
            },
        )
        try:
            for calc_date_value in dates:
                codes = calculator._get_trading_stock_codes(calc_date_value.isoformat())
                frame = calculator.compute_p_factors_pit(
                    calc_date_value.isoformat(), codes
                )
                old = self._fetch_frame("p", calc_date_value)
                if frame.empty:
                    eligible_codes = list(
                        getattr(calculator, "_last_eligible_codes", ()) or ()
                    )
                    if eligible_codes:
                        raise RuntimeError(
                            "P影子计算资格集合非空但结果为空: "
                            f"{calc_date_value.isoformat()}, eligible={len(eligible_codes)}"
                        )
                    if not old.empty:
                        self._prepare_date(
                            repair_id,
                            "p",
                            calc_date_value,
                            "remove_ineligible_p_snapshot",
                        )
                        self._delete_date("p", calc_date_value)
                        self._complete_date(repair_id, "p", calc_date_value, 0, None)
                        summary["p_removed_no_data_dates"].append(
                            calc_date_value.isoformat()
                        )
                    summary["p_expected_no_data_dates"].append(
                        calc_date_value.isoformat()
                    )
                    self.governance.record_date(
                        run_id,
                        "factor_p",
                        calc_date_value,
                        "expected_no_data",
                        input_count=len(codes),
                        details={"reason": "no_eligible_pit_input"},
                    )
                    continue
                validation = validate_factor_frame(
                    frame,
                    "p",
                    calc_date_value,
                    expected_codes=calculator._last_eligible_codes,
                )
                new_checksum = factor_frame_checksum(frame, P_FACTOR_COLUMNS)
                old_checksum = (
                    factor_frame_checksum(old, P_FACTOR_COLUMNS)
                    if not old.empty
                    else None
                )
                if old_checksum == new_checksum:
                    summary["p_unchanged_dates"].append(calc_date_value.isoformat())
                    self.governance.record_date(
                        run_id,
                        "factor_p",
                        calc_date_value,
                        "shadow_unchanged",
                        input_count=validation.expected_count or len(frame),
                        output_count=len(frame),
                        coverage_rate=validation.coverage_rate,
                        output_checksum=new_checksum,
                        is_current=True,
                        details={"formula_version": "v2.0"},
                    )
                    continue
                self._prepare_date(
                    repair_id, "p", calc_date_value, "replace_p_shadow_diff"
                )
                count, checksum = calculator._save_p_factors_mvp(frame)
                self._complete_date(repair_id, "p", calc_date_value, count, checksum)
                summary["p_replaced_dates"].append(calc_date_value.isoformat())
        finally:
            self._close_snapshot_db(read_db)

    def _rebuild_g_dates(
        self,
        repair_id: UUID,
        run_id: UUID,
        dates: Sequence[date],
        summary: Dict[str, Any],
    ) -> None:
        read_db = self._open_snapshot_db()
        calculator = GFactorCalculator(
            context=FactorDBContext(read_db),
            config={
                "factor_run_id": str(run_id),
                "factor_task_name": "factor_g",
                "formula_version": "v1.1",
                "factor_writer_db_manager": self.db,
            },
        )
        try:
            for calc_date_value in dates:
                codes = calculator._get_trading_stock_codes(calc_date_value.isoformat())
                if not codes:
                    raise RuntimeError(f"G重算缺少同日P: {calc_date_value.isoformat()}")
                frame = calculator.compute_g_factors_pit(
                    calc_date_value.isoformat(), codes
                )
                if frame.empty:
                    raise RuntimeError(f"G重算结果为空: {calc_date_value}")
                self._prepare_date(
                    repair_id, "g", calc_date_value, "rebuild_g_contaminated_window"
                )
                count, checksum = calculator._save_g_factor_results_pit(
                    frame, calc_date_value.isoformat()
                )
                self._complete_date(repair_id, "g", calc_date_value, count, checksum)
                summary["g_rebuilt_dates"].append(calc_date_value.isoformat())
        finally:
            self._close_snapshot_db(read_db)

    def _open_snapshot_db(self) -> DBManager:
        read_db = DBManager(self.db.connection_string, mode="sync")
        connection = read_db._get_sync_connection()
        connection.set_session(
            isolation_level="REPEATABLE READ", readonly=True, autocommit=False
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        return read_db

    @staticmethod
    def _close_snapshot_db(db_manager: DBManager) -> None:
        connection = db_manager._get_sync_connection()
        connection.rollback()
        db_manager.close_sync()

    def _early_p_missing_dates(self) -> List[date]:
        first = self.db.fetch_val_sync(
            "SELECT MIN(calc_date) FROM factors.p_factor WHERE EXTRACT(ISODOW FROM calc_date) = 5"
        )
        if not first or first >= REPAIR_WINDOW_START:
            return []
        rows = self.db.fetch_sync(
            """
            WITH expected AS (
                SELECT value::date AS calc_date
                FROM generate_series(%s::date, %s::date, interval '1 day') value
                WHERE EXTRACT(ISODOW FROM value) = 5
            ), actual AS (
                SELECT DISTINCT calc_date FROM factors.p_factor
            )
            SELECT expected.calc_date FROM expected
            LEFT JOIN actual USING (calc_date)
            WHERE actual.calc_date IS NULL
            ORDER BY expected.calc_date
            """,
            (first, REPAIR_WINDOW_START - timedelta(days=1)),
        )
        return [row["calc_date"] for row in rows]

    def _ensure_repair_tables(self) -> None:
        connection = self.db._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                for factor_type in ("p", "g"):
                    table = f"{factor_type}_factor"
                    archive = f"{table}_repair_archive"
                    cursor.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS factors.{archive}
                        (LIKE factors.{table} INCLUDING DEFAULTS)
                        """
                    )
                    cursor.execute(
                        f"ALTER TABLE factors.{archive} ADD COLUMN IF NOT EXISTS repair_id UUID"
                    )
                    cursor.execute(
                        f"ALTER TABLE factors.{archive} ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"
                    )
                    cursor.execute(
                        f"ALTER TABLE factors.{archive} ADD COLUMN IF NOT EXISTS repair_reason TEXT"
                    )
                    cursor.execute(
                        f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS uq_{archive}_repair
                        ON factors.{archive} (repair_id, ts_code, calc_date)
                        """
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def _install_weekday_constraints(self, *, validate: bool) -> None:
        connection = self.db._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                for factor_type in ("p", "g"):
                    table = f"{factor_type}_factor"
                    constraint = f"ck_{table}_calc_date_friday"
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = %s
                              AND conrelid = %s::regclass
                        )
                        """,
                        (constraint, f"factors.{table}"),
                    )
                    if not cursor.fetchone()[0]:
                        cursor.execute(
                            f"""
                            ALTER TABLE factors.{table}
                            ADD CONSTRAINT {constraint}
                            CHECK (EXTRACT(ISODOW FROM calc_date) = 5) NOT VALID
                            """
                        )
                    if validate:
                        cursor.execute(
                            f"ALTER TABLE factors.{table} VALIDATE CONSTRAINT {constraint}"
                        )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def _drop_weekday_constraints(self) -> None:
        connection = self.db._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                for factor_type in ("p", "g"):
                    table = f"{factor_type}_factor"
                    cursor.execute(
                        f"""
                        ALTER TABLE factors.{table}
                        DROP CONSTRAINT IF EXISTS ck_{table}_calc_date_friday
                        """
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def _prepare_date(
        self,
        repair_id: UUID,
        factor_type: str,
        calc_date_value: date,
        action: str,
    ) -> None:
        table = f"{factor_type}_factor"
        archive = f"{table}_repair_archive"
        task_name = f"factor_{factor_type}"
        columns = self._business_columns(table)
        quoted = ", ".join(f'"{column}"' for column in columns)
        old = self._fetch_frame(factor_type, calc_date_value)
        selected_columns = P_FACTOR_COLUMNS if factor_type == "p" else G_FACTOR_COLUMNS
        old_checksum = (
            factor_frame_checksum(old, selected_columns) if not old.empty else None
        )
        connection = self.db._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO factors.{archive}
                        ({quoted}, repair_id, archived_at, repair_reason)
                    SELECT {quoted}, %s, CURRENT_TIMESTAMP, %s
                    FROM factors.{table}
                    WHERE calc_date = %s
                    ON CONFLICT (repair_id, ts_code, calc_date) DO NOTHING
                    """,
                    (str(repair_id), action, calc_date_value),
                )
                cursor.execute(
                    """
                    INSERT INTO factors.factor_repair_date (
                        repair_id, task_name, calc_date, action,
                        old_row_count, old_checksum, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, 'prepared')
                    ON CONFLICT (repair_id, task_name, calc_date) DO UPDATE SET
                        action = EXCLUDED.action,
                        old_row_count = EXCLUDED.old_row_count,
                        old_checksum = EXCLUDED.old_checksum,
                        status = 'prepared',
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        str(repair_id),
                        task_name,
                        calc_date_value,
                        action,
                        len(old),
                        old_checksum,
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def _complete_date(
        self,
        repair_id: UUID,
        factor_type: str,
        calc_date_value: date,
        row_count: int,
        checksum: Optional[str],
    ) -> None:
        self.db.execute_sync(
            """
            UPDATE factors.factor_repair_date
            SET new_row_count = %s,
                new_checksum = %s,
                status = 'completed',
                updated_at = CURRENT_TIMESTAMP
            WHERE repair_id = %s AND task_name = %s AND calc_date = %s
            """,
            (
                row_count,
                checksum,
                str(repair_id),
                f"factor_{factor_type}",
                calc_date_value,
            ),
        )

    def _delete_date(self, factor_type: str, calc_date_value: date) -> None:
        self.db.execute_sync(
            f"DELETE FROM factors.{factor_type}_factor WHERE calc_date = %s",
            (calc_date_value,),
        )

    def _fetch_frame(self, factor_type: str, calc_date_value: date) -> pd.DataFrame:
        columns = P_FACTOR_COLUMNS if factor_type == "p" else G_FACTOR_COLUMNS
        quoted = ", ".join(f'"{column}"' for column in columns)
        rows = self.db.fetch_sync(
            f"""
            SELECT {quoted}
            FROM factors.{factor_type}_factor
            WHERE calc_date = %s
            ORDER BY ts_code
            """,
            (calc_date_value,),
        )
        return pd.DataFrame(rows, columns=list(columns))

    def _business_columns(self, table: str) -> List[str]:
        rows = self.db.fetch_sync(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'factors' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [row["column_name"] for row in rows]

    def _insert_manifest(
        self,
        repair_id: UUID,
        status: str,
        source_cutoff_at: datetime,
        cutoff: date,
        *,
        details: Mapping[str, Any],
    ) -> None:
        self.db.execute_sync(
            """
            INSERT INTO factors.factor_repair_manifest (
                repair_id, status, source_cutoff_at, effective_cutoff_date,
                apply_requested, details_json
            ) VALUES (%s, %s, %s, %s, TRUE, %s)
            """,
            (
                str(repair_id),
                status,
                source_cutoff_at,
                cutoff,
                Json(json_ready(details)),
            ),
        )

    def _finish_manifest(
        self, repair_id: UUID, status: str, details: Mapping[str, Any]
    ) -> None:
        self.db.execute_sync(
            """
            UPDATE factors.factor_repair_manifest
            SET status = %s,
                details_json = details_json || %s,
                finished_at = CURRENT_TIMESTAMP
            WHERE repair_id = %s
            """,
            (status, Json(json_ready(details)), str(repair_id)),
        )


__all__ = ["FactorRepairService", "REPAIR_WINDOW_START"]
