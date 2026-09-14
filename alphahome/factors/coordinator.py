"""Dependency-aware production coordinator for P/G factor tasks."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence
from uuid import UUID

from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.common.db_manager import DBManager

from .base import FactorTaskContract
from .date_policy import FACTOR_TIMEZONE, FactorDatePolicy, coerce_date
from .governance import FactorGovernanceStore, MIGRATION_HINT, json_ready
from .persistence import FactorSnapshotWriter
from .repository import FactorRepository
from .source_boundary import WATERMARK_CONTRACT, consumed_watermarks


@dataclass
class FactorTaskPlan:
    task_name: str
    dates: List[date] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    readiness_dependencies: List[str] = field(default_factory=list)
    blockers: List[str] = field(default_factory=list)
    source_watermarks: Dict[str, Any] = field(default_factory=dict)
    existing_rows_to_replace: int = 0
    estimated_output_rows: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return json_ready(asdict(self))


@dataclass
class FactorRunPlan:
    task_names: List[str]
    mode: str
    effective_cutoff_date: date
    task_plans: List[FactorTaskPlan]
    status: str = "ready"
    message: str = ""

    @property
    def total_dates(self) -> int:
        return sum(len(item.dates) for item in self.task_plans)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["effective_cutoff_date"] = self.effective_cutoff_date.isoformat()
        payload["task_plans"] = [item.to_dict() for item in self.task_plans]
        payload["total_dates"] = self.total_dates
        return payload


@dataclass
class FactorRunResult:
    run_id: Optional[str]
    status: str
    mode: str
    task_names: List[str]
    effective_cutoff_date: str
    planned_date_count: int
    successful_date_count: int = 0
    failed_date_count: int = 0
    skipped_date_count: int = 0
    output_count: int = 0
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class FactorCoordinator:
    """Plan dependencies first, then compute complete dates serially."""

    def __init__(self, db_manager: Any, *, max_automatic_dates: int = 26):
        self.db = db_manager
        self.repository = FactorRepository(db_manager)
        self.governance = FactorGovernanceStore(db_manager)
        self.date_policy = FactorDatePolicy()
        self.max_automatic_dates = max(1, int(max_automatic_dates))
        self.logger = logging.getLogger("FactorCoordinator")

    @staticmethod
    def contracts() -> Dict[str, FactorTaskContract]:
        from .tasks import discover_tasks

        discover_tasks()
        result: Dict[str, FactorTaskContract] = {}
        for name, task_class in UnifiedTaskFactory._task_registry.items():
            if getattr(task_class, "task_type", None) != "factor":
                continue
            contract = getattr(task_class, "contract", None)
            if isinstance(contract, FactorTaskContract):
                result[name] = contract
        return result

    def expand_dependencies(self, task_names: Iterable[str]) -> List[str]:
        contracts = self.contracts()
        requested = list(dict.fromkeys(task_names))
        unknown = sorted(set(requested) - set(contracts))
        if unknown:
            raise ValueError(f"未注册的因子任务: {unknown}")
        ordered: List[str] = []
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visited:
                return
            if name in visiting:
                raise ValueError(f"因子任务依赖循环: {name}")
            visiting.add(name)
            for dependency in contracts[name].dependencies:
                visit(dependency)
            visiting.remove(name)
            visited.add(name)
            ordered.append(name)

        for name in requested:
            visit(name)
        return ordered

    def plan(
        self,
        task_names: Sequence[str],
        *,
        mode: str = "smart",
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        batch_started_at: datetime | date | None = None,
        date_range: Optional[Sequence[date | str]] = None,
        source_cutoff: datetime | None = None,
        expand_dependencies: bool = True,
    ) -> FactorRunPlan:
        if date_range is not None:
            if len(date_range) != 2:
                raise ValueError("date_range必须是(start_date, end_date)")
            if start_date is not None or end_date is not None:
                raise ValueError("date_range不能与start_date/end_date同时提供")
            start_date, end_date = date_range
        if source_cutoff is not None and source_cutoff.tzinfo is None:
            raise ValueError("source_cutoff必须包含时区")
        mode = {"incremental": "smart", "backfill": "manual"}.get(mode, mode)
        if mode not in {"smart", "manual", "full", "audit"}:
            raise ValueError(f"不支持的因子运行模式: {mode}")
        cutoff = self.date_policy.automatic_cutoff(batch_started_at)
        requested_end = coerce_date(end_date) if end_date else cutoff
        if requested_end > cutoff:
            raise ValueError(
                f"end_date不能晚于最近完整周五 {cutoff.isoformat()}: {requested_end}"
            )
        effective_end = requested_end
        requested_start = coerce_date(start_date) if start_date else None
        if requested_start and requested_start > effective_end:
            raise ValueError("start_date must be <= end_date")
        if mode == "manual" and requested_start is None:
            raise ValueError("manual模式必须提供start_date")

        names = (
            self.expand_dependencies(task_names)
            if expand_dependencies
            else list(dict.fromkeys(task_names))
        )
        contracts = self.contracts()
        if not names:
            raise ValueError("至少需要一个因子任务")
        unknown = sorted(set(names) - set(contracts))
        if unknown:
            raise ValueError(f"未注册的因子任务: {unknown}")

        schema_issues = self.governance.schema_issues()
        if schema_issues:
            return FactorRunPlan(
                names, mode, effective_end, [], status="migration_required",
                message="; ".join(schema_issues) + "; " + MIGRATION_HINT,
            )
        task_plans: List[FactorTaskPlan] = []
        dates_by_task: Dict[str, List[date]] = {}

        for name in names:
            contract = contracts[name]
            # Capture the consumption ceiling before querying missing/dirty dates.
            watermarks = self.repository.source_watermarks(contract)
            dates = self._plan_dates(
                contract,
                mode,
                requested_start=requested_start,
                effective_end=effective_end,
            )
            if name == "factor_g" and "factor_p" in dates_by_task:
                dates = sorted(
                    set(dates)
                    | set(
                        self._propagate_p_dates(
                            dates_by_task["factor_p"],
                            effective_end,
                            contract.history_lookback_days,
                        )
                    )
                )
            dates_by_task[name] = dates
            blockers = (
                self.repository.readiness(contract, effective_end, dates, dates_by_task)
                if mode != "audit"
                else []
            )
            existing_counts = self.repository.row_counts_by_date(contract, dates)
            stats = self.repository.table_date_stats(contract)
            latest_rows = int(stats.get("latest_date_row_count") or 0)
            task_plans.append(
                FactorTaskPlan(
                    task_name=name,
                    dates=dates,
                    dependencies=list(contract.dependencies),
                    readiness_dependencies=list(contract.readiness_dependencies),
                    blockers=blockers,
                    source_watermarks=watermarks,
                    existing_rows_to_replace=sum(existing_counts.values()),
                    estimated_output_rows=latest_rows * len(dates),
                )
            )

        blockers = [item for plan in task_plans for item in plan.blockers]
        if blockers:
            return FactorRunPlan(
                names,
                mode,
                effective_end,
                task_plans,
                status="blocked_source",
                message="; ".join(blockers[:20]),
            )
        if mode == "smart" and any(
            len(item.dates) > self.max_automatic_dates for item in task_plans
        ):
            oversized = [
                f"{item.task_name}={len(item.dates)}"
                for item in task_plans
                if len(item.dates) > self.max_automatic_dates
            ]
            return FactorRunPlan(
                names,
                mode,
                effective_end,
                task_plans,
                status="needs_manual_backfill",
                message=(
                    f"自动任务单项最多{self.max_automatic_dates}个日期: "
                    + ", ".join(oversized)
                ),
            )
        return FactorRunPlan(names, mode, effective_end, task_plans)

    def _plan_dates(
        self,
        contract: FactorTaskContract,
        mode: str,
        *,
        requested_start: Optional[date],
        effective_end: date,
    ) -> List[date]:
        if mode == "audit":
            return []
        if mode == "manual":
            if requested_start is None:
                raise ValueError("manual模式必须提供start_date")
            return FactorDatePolicy.fridays(requested_start, effective_end)
        if mode == "full":
            first = requested_start or self.repository.first_source_date(contract)
            if first is None:
                return []
            return FactorDatePolicy.fridays(first, effective_end)

        recent_start = effective_end - timedelta(days=730)
        stats = self.repository.table_date_stats(contract)
        first_existing = stats.get("first_calc_date")
        if isinstance(first_existing, datetime):
            first_existing = first_existing.date()
        start = requested_start or recent_start
        if not first_existing:
            source_start = self.repository.first_source_date(contract)
            if source_start:
                start = max(start, source_start)
        missing = self.repository.missing_dates(contract, start, effective_end)

        previous = self.governance.latest_source_watermarks(contract.task_name)
        previous_for_task = previous.get(contract.task_name, previous)
        if not previous_for_task and first_existing:
            # Legacy successful runs did not certify consumption. Revalidate
            # the declared smart window, subject to the existing manual-size gate.
            return FactorDatePolicy.fridays(start, effective_end)
        dirty_start = self.repository.dirty_start_date(contract, previous_for_task)
        dirty: List[date] = []
        if dirty_start:
            dirty = FactorDatePolicy.fridays(max(dirty_start, start), effective_end)
        return sorted(set(missing) | set(dirty))

    @staticmethod
    def _propagate_p_dates(
        changed_dates: Sequence[date], cutoff: date, lookback_days: int
    ) -> List[date]:
        propagated: set[date] = set()
        for changed in changed_dates:
            end = min(cutoff, changed + timedelta(days=lookback_days))
            propagated.update(FactorDatePolicy.fridays(changed, end))
        return sorted(propagated)

    def run(
        self,
        task_names: Sequence[str],
        *,
        mode: str = "smart",
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        batch_started_at: datetime | date | None = None,
        date_range: Optional[Sequence[date | str]] = None,
        source_cutoff: datetime | None = None,
        expand_dependencies: bool = True,
        stop_requested: Optional[Callable[[], bool]] = None,
    ) -> FactorRunResult:
        started = batch_started_at or datetime.now(FACTOR_TIMEZONE)
        requested_start = start_date
        requested_end = end_date
        if date_range is not None:
            if len(date_range) != 2:
                raise ValueError("date_range必须是(start_date, end_date)")
            if start_date is not None or end_date is not None:
                raise ValueError("date_range不能与start_date/end_date同时提供")
            requested_start, requested_end = date_range
        plan = self.plan(
            task_names,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            batch_started_at=started,
            date_range=date_range,
            source_cutoff=source_cutoff,
            expand_dependencies=expand_dependencies,
        )
        if plan.status == "migration_required":
            return FactorRunResult(
                run_id=None,
                status=plan.status,
                mode=plan.mode,
                task_names=plan.task_names,
                effective_cutoff_date=plan.effective_cutoff_date.isoformat(),
                planned_date_count=0,
                message=plan.message,
                details={"plan": plan.to_dict(), "tasks": {}},
            )
        contracts = self.contracts()
        formula_versions = {
            name: contracts[name].formula_version for name in plan.task_names
        }
        source_watermarks = {
            item.task_name: item.source_watermarks for item in plan.task_plans
        }
        run_id = self.governance.start_run(
            plan.task_names,
            plan.mode,
            plan.effective_cutoff_date,
            requested_start_date=(
                coerce_date(requested_start) if requested_start else None
            ),
            requested_end_date=coerce_date(requested_end) if requested_end else None,
            formula_versions=formula_versions,
            config={
                "max_automatic_dates": self.max_automatic_dates,
                "timezone": str(FACTOR_TIMEZONE),
                "cadence": "weekly_friday",
            },
            source_watermarks={},
            details={
                "plan": plan.to_dict(),
                "batch_started_at": started,
                "source_cutoff": source_cutoff or started,
                "planned_source_watermarks": source_watermarks,
            },
        )
        result = FactorRunResult(
            run_id=str(run_id),
            status=plan.status,
            mode=plan.mode,
            task_names=plan.task_names,
            effective_cutoff_date=plan.effective_cutoff_date.isoformat(),
            planned_date_count=plan.total_dates,
            message=plan.message,
            details={"plan": plan.to_dict(), "tasks": {}},
        )
        if plan.status != "ready":
            self.governance.finish_run(run_id, plan.status, details=result.to_dict())
            for name in plan.task_names:
                self.governance.record_public_status(name, plan.status, plan.message)
            return result

        if plan.mode == "audit":
            try:
                audits = self._run_audit_tasks(plan.task_names)
            except Exception as exc:
                self.governance.finish_run(
                    run_id,
                    "error",
                    details={"error": str(exc), "phase": "audit"},
                )
                for name in plan.task_names:
                    self.governance.record_public_status(name, "error", str(exc))
                raise
            result.status = "success"
            result.message = f"完成{len(audits)}个因子任务审计"
            result.details["audits"] = audits
            self.governance.finish_run(run_id, result.status, details=result.to_dict())
            return result

        failed_tasks: set[str] = set()
        for task_plan in plan.task_plans:
            contract = contracts[task_plan.task_name]
            dependency_failures = sorted(set(contract.dependencies) & failed_tasks)
            if dependency_failures:
                task_result = {
                    "status": "skipped_dependency_failed",
                    "dependencies": dependency_failures,
                }
                result.details["tasks"][task_plan.task_name] = task_result
                result.skipped_date_count += len(task_plan.dates)
                failed_tasks.add(task_plan.task_name)
                self.governance.record_public_status(
                    task_plan.task_name,
                    "skipped_dependency_failed",
                    f"依赖失败: {', '.join(dependency_failures)}",
                )
                continue
            task_result = self._run_task_dates(
                run_id,
                contract,
                task_plan.dates,
                stop_requested=stop_requested,
            )
            result.details["tasks"][task_plan.task_name] = task_result
            result.successful_date_count += task_result["successful_dates"]
            result.failed_date_count += task_result["failed_dates"]
            result.skipped_date_count += task_result["skipped_dates"]
            result.output_count += task_result["output_count"]
            if task_result["status"] not in {"success", "no_op"}:
                failed_tasks.add(task_plan.task_name)
            self.governance.record_public_status(
                task_plan.task_name,
                task_result["status"],
                (
                    f"dates={len(task_plan.dates)}, output={task_result['output_count']}, "
                    f"failed={task_result['failed_dates']}"
                ),
            )
            if task_result["status"] == "cancelled":
                break

        if stop_requested and stop_requested():
            result.status = "cancelled"
        elif result.failed_date_count:
            result.status = (
                "partial_success" if result.successful_date_count else "error"
            )
        elif failed_tasks:
            result.status = "error"
        else:
            result.status = "success"
        result.message = (
            f"成功日期={result.successful_date_count}, 失败日期={result.failed_date_count}, "
            f"跳过日期={result.skipped_date_count}, 输出行={result.output_count}"
        )
        certified = {}
        current_cutoff = self.date_policy.automatic_cutoff(started)
        complete_scope = plan.effective_cutoff_date == current_cutoff and (
            plan.mode == "smart" or
            (plan.mode == "full" and not requested_start) or
            (requested_start and coerce_date(requested_start) <= current_cutoff - timedelta(days=730))
        )
        if complete_scope:
            for task_plan in plan.task_plans:
                task_result = result.details["tasks"].get(task_plan.task_name) or {}
                if len(task_result.get("dates") or {}) != len(task_plan.dates):
                    continue
                watermarks = consumed_watermarks(task_result, task_plan.source_watermarks)
                if watermarks:
                    certified[task_plan.task_name] = watermarks
        result.details["watermark_contract"] = WATERMARK_CONTRACT
        result.details["consumed_watermarks"] = certified
        result.details["consumption_scope_complete"] = bool(complete_scope)
        try:
            result.details["end_observed_watermarks"] = {
                name: self.repository.source_watermarks(contracts[name])
                for name in plan.task_names
            }
        except Exception as exc:
            result.details["end_observation_error"] = type(exc).__name__
        self.governance.finish_run(
            run_id,
            result.status,
            details={**result.to_dict(), "watermark_contract": WATERMARK_CONTRACT},
            source_watermarks=certified,
        )
        return result

    def _run_task_dates(
        self,
        run_id: UUID,
        contract: FactorTaskContract,
        dates: Sequence[date],
        *,
        stop_requested: Optional[Callable[[], bool]],
    ) -> Dict[str, Any]:
        if not dates:
            return {
                "status": "no_op",
                "successful_dates": 0,
                "failed_dates": 0,
                "skipped_dates": 0,
                "output_count": 0,
                "dates": {},
            }
        source_db = self.db
        source_connection = None
        owns_source_db = False
        details: Dict[str, Any] = {}
        success = failed = skipped = output = 0
        cancelled = False
        try:
            db_url = getattr(self.db, "connection_string", None)
            if db_url:
                source_db = DBManager(db_url, mode="sync")
                owns_source_db = True
                source_connection = source_db._get_sync_connection()
                source_connection.set_session(
                    isolation_level="REPEATABLE READ", readonly=True, autocommit=False
                )
                with source_connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT transaction_timestamp() AS snapshot_started_at,
                               pg_current_wal_lsn()::text AS snapshot_wal_lsn
                        """
                    )
                    snapshot_row = cursor.fetchone()
                source_snapshot = {
                    "consistent": True,
                    "started_at": snapshot_row[0],
                    "wal_lsn": snapshot_row[1],
                    "watermarks": FactorRepository(source_db).source_watermarks(
                        contract
                    ),
                }
            else:
                source_snapshot = {
                    "consistent": False,
                    "started_at": datetime.now(FACTOR_TIMEZONE),
                    "wal_lsn": None,
                    "watermarks": self.repository.source_watermarks(contract),
                }
            calculator = contract.resolve_calculator_class()(
                db_manager=source_db,
                config={
                    "factor_run_id": str(run_id),
                    "factor_task_name": contract.task_name,
                    "formula_version": contract.formula_version,
                    "factor_writer_db_manager": self.db,
                    "factor_source_snapshot": source_snapshot,
                },
            )
            for index, calc_date in enumerate(dates):
                if stop_requested and stop_requested():
                    remaining = dates[index:]
                    for remaining_date in remaining:
                        details[remaining_date.isoformat()] = {"status": "cancelled"}
                        self.governance.record_date(
                            run_id,
                            contract.task_name,
                            remaining_date,
                            "cancelled",
                        )
                    skipped += len(remaining)
                    cancelled = True
                    break
                started = time.monotonic()
                try:
                    stock_codes = calculator._get_trading_stock_codes(
                        calc_date.isoformat()
                    )
                    if not stock_codes:
                        if contract.task_name == "factor_p":
                            deleted = FactorSnapshotWriter(
                                self.db
                            ).clear_expected_no_data(
                                "p",
                                calc_date,
                                run_id=run_id,
                                task_name=contract.task_name,
                                details={"reason": "empty_source_universe"},
                            )
                            status = "expected_no_data"
                            skipped += 1
                        else:
                            status = "skipped_dependency_failed"
                            deleted = 0
                            self.governance.record_date(
                                run_id,
                                contract.task_name,
                                calc_date,
                                status,
                                details={"reason": "missing_same_date_p"},
                            )
                            failed += 1
                        details[calc_date.isoformat()] = {
                            "status": status,
                            "rows": 0,
                            "deleted_stale_rows": deleted,
                        }
                        continue
                    calculator.config["factor_input_count"] = len(stock_codes)
                    calculator.config["factor_expected_codes"] = list(stock_codes)
                    if contract.task_name == "factor_p":
                        item = calculator.calculate_p_factors_pit(
                            calc_date.isoformat(), stock_codes
                        )
                    else:
                        item = calculator.calculate_g_factors_pit(
                            calc_date.isoformat(), stock_codes
                        )
                    rows = int(item.get("success_count") or 0)
                    item_status = item.get("status") or ("success" if rows else "error")
                    eligible_count = int(
                        calculator.config.get("factor_input_count") or 0
                    )
                    if (
                        contract.task_name == "factor_p"
                        and item_status == "no_data"
                        and rows == 0
                    ):
                        item_status = (
                            "expected_no_data" if eligible_count == 0 else "error"
                        )
                        if item_status == "error":
                            item = {
                                **item,
                                "error": (
                                    "P因子资格集合非空但计算结果为空: "
                                    f"eligible={eligible_count}"
                                ),
                            }
                    details[calc_date.isoformat()] = {**item, "status": item_status}
                    if item_status == "success" and rows > 0:
                        success += 1
                        output += rows
                    elif item_status == "expected_no_data":
                        skipped += 1
                        deleted = FactorSnapshotWriter(self.db).clear_expected_no_data(
                            "p",
                            calc_date,
                            run_id=run_id,
                            task_name=contract.task_name,
                            details={
                                **item,
                                "reason": "no_eligible_pit_input",
                                "duration_ms": int((time.monotonic() - started) * 1000),
                            },
                        )
                        details[calc_date.isoformat()]["deleted_stale_rows"] = deleted
                    else:
                        failed += 1
                        self.governance.record_date(
                            run_id,
                            contract.task_name,
                            calc_date,
                            item_status,
                            input_count=len(stock_codes),
                            output_count=rows,
                            duration_ms=int((time.monotonic() - started) * 1000),
                            details=item,
                        )
                except Exception as exc:
                    failed += 1
                    details[calc_date.isoformat()] = {
                        "status": "error",
                        "error": str(exc),
                    }
                    self.governance.record_date(
                        run_id,
                        contract.task_name,
                        calc_date,
                        "error",
                        duration_ms=int((time.monotonic() - started) * 1000),
                        details={"error": str(exc)},
                    )
                    self.logger.error(
                        "%s %s执行失败: %s",
                        contract.task_name,
                        calc_date,
                        exc,
                        exc_info=True,
                    )
                    break
        finally:
            if owns_source_db:
                try:
                    if source_connection is not None:
                        source_connection.rollback()
                finally:
                    source_db.close_sync()
        status = (
            "cancelled"
            if cancelled
            else "success" if failed == 0 else "partial_success" if success else "error"
        )
        return {
            "status": status,
            "successful_dates": success,
            "failed_dates": failed,
            "skipped_dates": skipped,
            "output_count": output,
            "dates": details,
            "source_snapshot": source_snapshot,
        }

    def _run_audit_tasks(self, task_names: Sequence[str]) -> List[Dict[str, Any]]:
        db_url = getattr(self.db, "connection_string", None)
        if not db_url:
            raise RuntimeError("审计模式需要数据库连接URL")

        async def _audit() -> List[Dict[str, Any]]:
            from .audit_service import FactorAuditService

            async_db = DBManager(db_url, mode="async")
            await async_db.connect()
            try:
                service = FactorAuditService(async_db)
                return [
                    await service.audit_task(name, persist=True) for name in task_names
                ]
            finally:
                await async_db.close()

        return asyncio.run(_audit())


__all__ = [
    "FactorCoordinator",
    "FactorRunPlan",
    "FactorRunResult",
    "FactorTaskPlan",
]
