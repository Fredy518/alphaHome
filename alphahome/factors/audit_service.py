"""Live-state and persisted audit service for governed factor tasks."""

from __future__ import annotations

import json
import re
from copy import copy
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.common.audit_models import AuditDimensions
from alphahome.common.db_session import readonly_snapshot

from .base import FactorTaskContract
from .date_policy import FactorDatePolicy, coerce_date
from .governance import DDL_PATH, FactorGovernanceStore, MIGRATION_HINT, stable_config_hash
from .source_boundary import WATERMARK_CONTRACT, SNAPSHOT_XMIN_KEY
from .repository import _SOURCE_TIME_KEYS


_RELATION_RE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")


class FactorAuditService:
    def __init__(self, db_manager: Any):
        self.db = db_manager
        self.date_policy = FactorDatePolicy()

    async def ensure_schema(self) -> None:
        await self.db.execute(DDL_PATH.read_text(encoding="utf-8"))

    @staticmethod
    def _contracts() -> Dict[str, Tuple[Any, FactorTaskContract]]:
        from .tasks import discover_tasks

        discover_tasks()
        result: Dict[str, Tuple[Any, FactorTaskContract]] = {}
        for name, task_class in UnifiedTaskFactory._task_registry.items():
            if getattr(task_class, "task_type", None) != "factor":
                continue
            contract = getattr(task_class, "contract", None)
            if isinstance(contract, FactorTaskContract):
                result[name] = (task_class, contract)
        return result

    async def list_factor_tasks(self) -> List[Dict[str, Any]]:
        tasks: List[Dict[str, Any]] = []
        for name, (task_class, contract) in self._contracts().items():
            audit = await self.audit_task(name, persist=False)
            tasks.append(
                {
                    "name": name,
                    "task_name": name,
                    "task_type": "factor",
                    "description": getattr(task_class, "description", ""),
                    "domain": contract.domain,
                    "source_tables": list(contract.source_tables),
                    "output_table": contract.output_table,
                    "calc_date_key": contract.calc_date_key,
                    "primary_keys": list(contract.primary_keys),
                    "dependencies": list(contract.dependencies),
                    "readiness_dependencies": list(contract.readiness_dependencies),
                    "supported_modes": list(contract.supported_modes),
                    "formula_version": contract.formula_version,
                    "cadence": contract.cadence,
                    "eligibility_policy": contract.eligibility_policy,
                    **audit,
                }
            )
        return tasks

    async def audit_all(self, *, persist: bool = True) -> List[Dict[str, Any]]:
        return [
            await self.audit_task(name, persist=persist) for name in self._contracts()
        ]

    async def audit_task(
        self, task_name: str, *, persist: bool = True
    ) -> Dict[str, Any]:
        async with readonly_snapshot(self.db) as snapshot:
            inspector = copy(self)
            inspector.db = snapshot
            result = await inspector._audit_task_live(task_name)
        if persist and result.get("status") != "migration_required":
            await self._persist(self._contracts()[task_name][1], result)
            result["last_audit_time"] = result["audited_at"]
            result["persisted"] = True
        return result

    async def _audit_task_live(self, task_name: str) -> Dict[str, Any]:
        task_info = self._contracts().get(task_name)
        if task_info is None:
            raise ValueError(f"未注册的因子任务: {task_name}")
        _, contract = task_info
        if not _RELATION_RE.fullmatch(contract.output_table):
            raise ValueError(f"非法输出表: {contract.output_table}")
        expected_latest = self.date_policy.automatic_cutoff()
        audited_at = datetime.now(timezone.utc)
        schema_issues = await FactorGovernanceStore.async_schema_issues(self.db)
        if schema_issues:
            result = self._empty_audit(contract, expected_latest, "migration_required")
            result.update({"audited_at": audited_at, "schema_issues": schema_issues, "migration_hint": MIGRATION_HINT,
                           "dimensions": AuditDimensions(structure="migration_required").to_dict(), "persisted": False})
            return result
        exists = await self.db.fetch_val(
            "SELECT to_regclass($1) IS NOT NULL", contract.output_table
        )
        if not exists:
            result = self._empty_audit(contract, expected_latest, "missing_table")
        else:
            stats_record = await self.db.fetch_one(
                f"""
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT calc_date) AS distinct_date_count,
                       MIN(calc_date) AS first_calc_date,
                       MAX(calc_date) AS actual_latest_date,
                       COUNT(DISTINCT calc_date) FILTER (
                           WHERE EXTRACT(ISODOW FROM calc_date) <> 5
                       ) AS nonstandard_date_count
                FROM {contract.output_table}
                """
            )
            stats = dict(stats_record or {})
            first_date = stats.get("first_calc_date")
            actual_latest = stats.get("actual_latest_date")
            latest_rows = 0
            missing_dates: List[Any] = []
            expected_no_data_dates: List[Any] = []
            if actual_latest:
                latest_rows = int(
                    await self.db.fetch_val(
                        f"SELECT COUNT(*) FROM {contract.output_table} WHERE calc_date = $1",
                        actual_latest,
                    )
                    or 0
                )
            if first_date:
                records = await self.db.fetch(
                    f"""
                    WITH expected AS (
                        SELECT value::date AS calc_date
                        FROM generate_series($1::date, $2::date, interval '1 day') AS value
                        WHERE EXTRACT(ISODOW FROM value) = 5
                    ), actual AS (
                        SELECT DISTINCT calc_date
                        FROM {contract.output_table}
                        WHERE EXTRACT(ISODOW FROM calc_date) = 5
                    ), ledger AS (
                        SELECT DISTINCT ON (calc_date) calc_date, status
                        FROM factors.factor_run_date
                        WHERE task_name = $3
                          AND calc_date BETWEEN $1::date AND $2::date
                        ORDER BY calc_date, created_at DESC, run_id DESC
                    )
                    SELECT expected.calc_date, ledger.status AS ledger_status
                    FROM expected
                    LEFT JOIN actual USING (calc_date)
                    LEFT JOIN ledger USING (calc_date)
                    WHERE actual.calc_date IS NULL
                    ORDER BY expected.calc_date
                    """,
                    first_date,
                    expected_latest,
                    task_name,
                )
                expected_no_data_dates = [
                    record["calc_date"]
                    for record in records
                    if record["ledger_status"] == "expected_no_data"
                ]
                missing_dates = [
                    record["calc_date"]
                    for record in records
                    if record["ledger_status"] != "expected_no_data"
                ]
            dependency_status, dependency_details = await self._dependency_status(
                contract, expected_latest
            )
            denominator = await self._coverage_denominator(contract, actual_latest) if dependency_status == "ready" else 0
            coverage_rate = (
                latest_rows / denominator if denominator and latest_rows else 0.0
            )
            last_execution = await self._last_execution(task_name)
            last_audit = await self._last_audit(task_name)
            consumption = await self._source_consumption(contract) if dependency_status == "ready" else {"status": "unverified"}
            status = "healthy"
            if stats.get("nonstandard_date_count"):
                status = "nonstandard_dates"
            elif actual_latest and actual_latest > expected_latest:
                status = "future_dates"
            elif missing_dates:
                status = "gaps"
            elif not actual_latest or actual_latest < expected_latest:
                status = "stale"
            elif dependency_status != "ready":
                status = "blocked_source"
            elif denominator != latest_rows:
                status = "coverage_mismatch"
            elif consumption["status"] != "current":
                status = "source_unconsumed" if consumption["status"] == "changed" else "consumption_unverified"
            dimensions = AuditDimensions(
                structure="ready",
                dates="complete" if not missing_dates and actual_latest == expected_latest and not stats.get("nonstandard_date_count") else "incomplete",
                source_consumption=consumption["status"],
                coverage="complete" if denominator == latest_rows and denominator > 0 else "incomplete",
                eligibility="qualified" if dependency_status == "ready" and denominator > 0 else "unknown",
            )
            if status == "healthy" and not dimensions.healthy:
                status = "readiness_unverified"
            result = {
                "task_name": task_name,
                "row_count": int(stats.get("row_count") or 0),
                "distinct_date_count": int(stats.get("distinct_date_count") or 0),
                "first_calc_date": first_date,
                "actual_latest_date": actual_latest,
                "expected_latest_date": expected_latest,
                "latest_date_row_count": latest_rows,
                "eligibility_count": denominator,
                "coverage_rate": coverage_rate,
                "gap_count": len(missing_dates),
                "missing_date_count": len(missing_dates),
                "expected_no_data_count": len(expected_no_data_dates),
                "nonstandard_date_count": int(stats.get("nonstandard_date_count") or 0),
                "missing_dates": missing_dates,
                "expected_no_data_dates": expected_no_data_dates,
                "dependency_status": dependency_status,
                "dependency_details": dependency_details,
                "live_status": status,
                "status": status,
                "last_execution_status": last_execution.get("status"),
                "last_execution_time": last_execution.get("update_time"),
                "last_execution_details": last_execution.get("details"),
                "last_audit_time": last_audit.get("snapshot_time"),
                "audited_latest_date": last_audit.get("actual_latest_date"),
                "audited_row_count": last_audit.get("row_count"),
                "audited_coverage_rate": last_audit.get("coverage_rate"),
                "audited_gap_count": last_audit.get("missing_date_count"),
                "dimensions": dimensions.to_dict(),
                "source_consumption": consumption,
            }
        result["audited_at"] = audited_at
        result.setdefault("dimensions", AuditDimensions(structure="missing").to_dict())
        result["persisted"] = False
        return result

    async def get_date_gaps(
        self, task_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        selected = set(task_names or self._contracts().keys())
        rows = []
        for name in self._contracts():
            if name not in selected:
                continue
            audit = await self.audit_task(name, persist=False)
            rows.append(
                {
                    "task_name": name,
                    "expected_latest_date": audit.get("expected_latest_date"),
                    "actual_latest_date": audit.get("actual_latest_date"),
                    "missing_dates": audit.get("missing_dates") or [],
                    "nonstandard_date_count": audit.get("nonstandard_date_count"),
                }
            )
        return {"status": "success", "rows": rows}

    async def diagnose_date(
        self, task_name: str, calc_date: date | str
    ) -> Dict[str, Any]:
        contract = self._contracts().get(task_name, (None, None))[1]
        if contract is None:
            raise ValueError(f"未注册的因子任务: {task_name}")
        target = coerce_date(calc_date)
        bounds = "p_score" if task_name == "factor_p" else "g_score"
        row = await self.db.fetch_one(
            f"""
            SELECT COUNT(*) AS row_count,
                   COUNT(*) - COUNT(DISTINCT ts_code) AS duplicate_rows,
                   COUNT(*) FILTER (WHERE ts_code IS NULL OR calc_date IS NULL) AS null_keys,
                   COUNT(*) FILTER (WHERE ann_date > calc_date) AS pit_violations,
                   COUNT(*) FILTER (
                       WHERE {bounds} IS NULL OR {bounds} < 0 OR {bounds} > 100
                   ) AS score_violations
            FROM {contract.output_table}
            WHERE calc_date = $1
            """,
            target,
        )
        return {
            "status": "success",
            "task_name": task_name,
            "calc_date": target,
            "is_friday": FactorDatePolicy.is_valid(target),
            **dict(row or {}),
        }

    async def diagnose_stock(self, ts_code: str) -> Dict[str, Any]:
        tasks = []
        for name, (_, contract) in self._contracts().items():
            rows = await self.db.fetch(
                f"""
                SELECT * FROM {contract.output_table}
                WHERE ts_code = $1
                ORDER BY calc_date DESC
                LIMIT 20
                """,
                ts_code,
            )
            tasks.append({"task_name": name, "rows": [dict(row) for row in rows]})
        return {"status": "success", "ts_code": ts_code, "tasks": tasks}

    async def _coverage_denominator(
        self, contract: FactorTaskContract, calc_date: Any
    ) -> int:
        if not calc_date:
            return 0
        if contract.task_name == "factor_g":
            return int(
                await self.db.fetch_val(
                    "SELECT COUNT(*) FROM factors.p_factor WHERE calc_date = $1",
                    calc_date,
                )
                or 0
            )
        return int(
            await self.db.fetch_val(
                """
                WITH eligible AS (
                    SELECT DISTINCT ON (pit.ts_code) pit.ts_code
                    FROM pit.pit_financial_indicators pit
                    JOIN tushare.stock_basic sb ON sb.ts_code = pit.ts_code
                    WHERE pit.ann_date <= $1
                      AND pit.end_date >= ($1::date - INTERVAL '10 months')
                      AND pit.calculation_status = 'success'
                      AND pit.data_quality IN (
                          'high', 'normal', 'outlier_high', 'outlier_low'
                      )
                      AND sb.list_date <= $1
                      AND (sb.delist_date IS NULL OR sb.delist_date > $1)
                    ORDER BY pit.ts_code, pit.ann_date DESC, pit.end_date DESC
                )
                SELECT COUNT(*) FROM eligible
                """,
                calc_date,
            )
            or 0
        )

    async def _dependency_status(
        self, contract: FactorTaskContract, expected_latest: date
    ) -> tuple[str, Dict[str, Any]]:
        details: Dict[str, Any] = {}
        sources = list(contract.source_tables)
        if contract.task_name == "factor_p":
            sources.append("tushare.stock_basic")
        for source in dict.fromkeys(sources):
            exists = bool(
                await self.db.fetch_val("SELECT to_regclass($1) IS NOT NULL", source)
            )
            details[source] = {"exists": exists}
            if not exists:
                return "blocked", details
        if contract.task_name == "factor_p":
            from alphahome.pit.eligibility import INPUT_RELATIONS, financial_input_gap_sql

            if any([not await self.db.fetch_val("SELECT to_regclass($1) IS NOT NULL", source) for source in INPUT_RELATIONS]):
                details["eligible_inputs"] = {"status": "unverified"}
                return "blocked", details
            row = dict(await self.db.fetch_one(financial_input_gap_sql("$1"), expected_latest) or {})
            details["eligible_inputs"] = row
            if row.get("eligible_missing") is None or row["eligible_missing"]:
                return "blocked", details
        if contract.task_name == "factor_g":
            latest_p = await self.db.fetch_val(
                "SELECT MAX(calc_date) FROM factors.p_factor WHERE EXTRACT(ISODOW FROM calc_date) = 5"
            )
            details["factors.p_factor"]["latest_calc_date"] = latest_p
            if not latest_p or latest_p < expected_latest:
                return "blocked", details
        return "ready", details

    async def _source_consumption(self, contract):
        if contract.task_name == "factor_p" and "tushare.stock_basic" not in contract.source_tables:
            return {"status": "unverified", "reason": "stock_master_not_in_consumption_contract"}
        row = await self.db.fetch_one(
            "SELECT run_id, source_watermarks, finished_at FROM factors.factor_run "
            "WHERE $1=ANY(task_names) AND status IN ('success','partial_success') "
            "AND details_json->>'watermark_contract'=$2 AND source_watermarks ? $1 "
            "ORDER BY finished_at DESC NULLS LAST LIMIT 1", contract.task_name, WATERMARK_CONTRACT,
        )
        if not row:
            return {"status": "unverified", "reason": "no_certified_consumption"}
        payload = row["source_watermarks"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        watermarks = payload.get(contract.task_name) or {}
        xmin = watermarks.get(SNAPSHOT_XMIN_KEY)
        xmax = int(await self.db.fetch_val("SELECT pg_snapshot_xmax(pg_current_snapshot())::text"))
        if not isinstance(xmin, int) or not 0 <= xmax - xmin < 2**31:
            return {"status": "unverified", "reason": "invalid_or_expired_cursor"}
        changed = []
        for source in contract.source_tables:
            key = _SOURCE_TIME_KEYS.get(source)
            if not key or not _RELATION_RE.fullmatch(source):
                return {"status": "unverified", "reason": "unsupported_source_contract"}
            value = await self.db.fetch_val(
                f"SELECT MIN({key}) FROM {source} WHERE updated_at > $1 OR age(xmin) <= age($2::text::xid)",
                datetime.fromisoformat(watermarks[source]) if isinstance(watermarks.get(source), str) else watermarks.get(source),
                str(xmin % (2**32)),
            )
            if value is not None:
                changed.append(source)
        return {"status": "changed" if changed else "current", "changed_sources": changed,
                "consumed_run_id": str(row["run_id"]), "consumed_at": row["finished_at"]}

    async def _last_execution(self, task_name: str) -> Dict[str, Any]:
        exists = await self.db.fetch_val(
            "SELECT to_regclass('public.task_status') IS NOT NULL"
        )
        if not exists:
            return {}
        row = await self.db.fetch_one(
            """
            SELECT status, update_time, details
            FROM public.task_status
            WHERE task_name = $1
            ORDER BY update_time DESC
            LIMIT 1
            """,
            task_name,
        )
        return dict(row or {})

    async def _last_audit(self, task_name: str) -> Dict[str, Any]:
        row = await self.db.fetch_one(
            """
            SELECT snapshot_time, actual_latest_date, row_count,
                   coverage_rate, missing_date_count
            FROM factors.factor_audit_snapshot
            WHERE task_name = $1
            ORDER BY snapshot_time DESC
            LIMIT 1
            """,
            task_name,
        )
        return dict(row or {})

    async def _persist(
        self, contract: FactorTaskContract, result: Dict[str, Any]
    ) -> None:
        details = {
            "audited_at": result.get("audited_at"),
            "dimensions": result.get("dimensions"),
            "source_consumption": result.get("source_consumption"),
            "missing_dates": result.get("missing_dates") or [],
            "expected_no_data_dates": result.get("expected_no_data_dates") or [],
            "dependency_details": result.get("dependency_details") or {},
        }
        await self.db.execute(
            """
            INSERT INTO factors.factor_audit_snapshot (
                task_name, output_table, expected_latest_date, actual_latest_date,
                first_calc_date, row_count, distinct_date_count, latest_date_row_count,
                coverage_rate, missing_date_count, nonstandard_date_count,
                dependency_status, formula_version, config_hash, status, details_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16::jsonb
            )
            """,
            contract.task_name,
            contract.output_table,
            result.get("expected_latest_date"),
            result.get("actual_latest_date"),
            result.get("first_calc_date"),
            int(result.get("row_count") or 0),
            int(result.get("distinct_date_count") or 0),
            int(result.get("latest_date_row_count") or 0),
            result.get("coverage_rate"),
            int(result.get("missing_date_count") or 0),
            int(result.get("nonstandard_date_count") or 0),
            result.get("dependency_status"),
            contract.formula_version,
            stable_config_hash(contract.to_dict()),
            result.get("status"),
            json.dumps(details, ensure_ascii=False, default=str),
        )

    @staticmethod
    def _empty_audit(
        contract: FactorTaskContract, expected_latest: date, status: str
    ) -> Dict[str, Any]:
        return {
            "task_name": contract.task_name,
            "row_count": 0,
            "distinct_date_count": 0,
            "first_calc_date": None,
            "actual_latest_date": None,
            "expected_latest_date": expected_latest,
            "latest_date_row_count": 0,
            "eligibility_count": 0,
            "coverage_rate": 0.0,
            "gap_count": 0,
            "missing_date_count": 0,
            "expected_no_data_count": 0,
            "nonstandard_date_count": 0,
            "missing_dates": [],
            "expected_no_data_dates": [],
            "dependency_status": "blocked",
            "dependency_details": {},
            "live_status": status,
            "status": status,
            "last_execution_status": None,
            "last_execution_time": None,
            "last_execution_details": None,
            "last_audit_time": None,
            "audited_latest_date": None,
            "audited_row_count": None,
            "audited_coverage_rate": None,
            "audited_gap_count": None,
        }


__all__ = ["FactorAuditService"]
