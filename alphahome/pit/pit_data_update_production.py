#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compatibility coordinator for production PIT updates."""

from __future__ import annotations

import argparse
import asyncio
import logging
import json
from uuid import uuid4
from zoneinfo import ZoneInfo
import sys
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

from alphahome.common.config_manager import get_database_url
from alphahome.common.constants import UpdateTypes
from alphahome.common.logging_utils import get_logger
from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.pit.base.monthly_snapshot_manager import PITMonthlySnapshotManager
from alphahome.pit.base.pit_task import (
    PIT_MONTH_END_CUTOFF_CONFIG_KEY,
    PITTaskContract,
)

logger = get_logger(__name__)


TARGET_TO_TASK = {
    "income": "pit_income_quarterly",
    "balance": "pit_balance_quarterly",
    "cashflow": "pit_cashflow_quarterly",
    "financial_indicators": "pit_financial_indicators",
    "industry_classification": "pit_industry_classification",
    "stock_fttm": "pit_stock_fttm_monthly",
    "stock_consensus_fy": "pit_stock_consensus_fy_monthly",
    "industry_fttm": "pit_industry_fttm_monthly",
    "industry_fapi": "pit_industry_fapi_monthly",
    "index_fttm": "pit_index_fttm_monthly",
    "etf_index_members": "pit_etf_index_members_monthly",
    "etf_index_a_share_proxy_members": "pit_etf_index_a_share_proxy_members_monthly",
    "etf_index_fapi": "pit_etf_index_fapi_monthly",
    "etf_index_a_share_proxy_fapi": "pit_etf_index_a_share_proxy_fapi_monthly",
    "earnings_surprise_annual": "pit_earnings_surprise_annual",
}

DEFAULT_TARGET_ORDER = [
    "income",
    "balance",
    "cashflow",
    "industry_classification",
    "stock_fttm",
    "stock_consensus_fy",
    "financial_indicators",
    "industry_fttm",
    "industry_fapi",
    "index_fttm",
    "etf_index_members",
    "etf_index_a_share_proxy_members",
    "etf_index_fapi",
    "etf_index_a_share_proxy_fapi",
    "earnings_surprise_annual",
]


class PITDataUpdateCoordinator:
    """Run registered PIT tasks through UnifiedTaskFactory."""

    def __init__(self, max_workers: int = 2, max_retries: int = 3, retry_delay: int = 5, *, db_manager=None):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._db_manager = db_manager
        self.db_url = getattr(db_manager, "connection_string", None)
        self.last_plan = None
        self._owns_factory = False

    async def initialize(self, database_url=None):
        from alphahome.pit.tasks import discover_tasks

        discover_tasks()
        self.db_url = database_url or self.db_url or get_database_url()
        if not self.db_url:
            raise ValueError("数据库连接配置未找到，请检查config.json文件")
        if self._db_manager is None:
            await UnifiedTaskFactory.initialize(self.db_url)
            self._db_manager = UnifiedTaskFactory.get_db_manager()
            self._owns_factory = True
        logger.info("PIT数据更新协调器初始化完成")

    async def cleanup(self):
        if not self._owns_factory:
            return
        try:
            await UnifiedTaskFactory.shutdown()
        except Exception as exc:
            logger.warning("关闭PIT任务工厂连接失败: %s", exc)

    async def plan(self, targets, mode="incremental", *, cutoff=None, start_date=None, end_date=None):
        from .run_plan import build_pit_plan

        normalized = self._normalize_targets(targets)
        task_names = [TARGET_TO_TASK.get(value, value) for value in normalized]
        if not self.db_url:
            raise ValueError("An explicit initialized PIT database target is required")
        plan = await asyncio.to_thread(build_pit_plan, self.db_url, task_names, mode,
                                       cutoff=cutoff, start_date=start_date, end_date=end_date)
        self.last_plan = plan
        return plan

    async def run_updates(self, targets, mode="incremental", parallel=False, *, plan=None,
                          expected_plan_hash=None, stop_event=None, start_date=None, end_date=None):
        import asyncpg

        if not isinstance(self.db_url, str) or not self.db_url:
            raise RuntimeError('An explicit PIT database target is required')
        connection = await asyncpg.connect(self.db_url, command_timeout=7200)
        try:
            await connection.execute("SET lock_timeout = '30s'")
            await connection.execute("SELECT pg_advisory_lock(hashtext('alphahome.pit'), hashtext('pipeline'))")
            return await self._run_updates_locked(
                targets, mode, parallel, plan=plan, expected_plan_hash=expected_plan_hash,
                stop_event=stop_event, start_date=start_date, end_date=end_date,
                ledger_connection=connection,
            )
        finally:
            await connection.close()

    async def _run_updates_locked(self, targets, mode="incremental", parallel=False, *, plan=None,
                                  expected_plan_hash=None, stop_event=None, start_date=None, end_date=None,
                                  ledger_connection):
        from alphahome.common.run_models import RunPlan

        if isinstance(plan, dict):
            plan = RunPlan.from_dict(plan)
        if plan is None:
            plan = await self.plan(targets, mode, start_date=start_date, end_date=end_date)
        normalized_targets = self._normalize_targets(targets)
        requested = tuple(sorted(TARGET_TO_TASK.get(value, value) for value in normalized_targets))
        normalized_mode = {"smart": "incremental", "full": "full_backfill", "manual": "manual_range"}.get(mode, mode)
        if plan.request.domain != "pit" or requested != plan.request.tasks or normalized_mode != plan.request.mode:
            raise ValueError("PIT execution request differs from its submitted plan")
        plan.require_matching(expected_plan_hash or plan.plan_hash)
        current = await self.plan(targets, plan.request.mode, cutoff=plan.effective_cutoff,
                                  start_date=plan.request.start_date, end_date=plan.request.end_date)
        current.require_matching(plan.plan_hash)
        self.last_plan = plan
        update_type = self._update_type_from_mode(plan.request.mode)
        contracts = self._registered_contracts()
        execution_tasks = {unit.task_name for unit in plan.units}
        layers = self._topological_layers(execution_tasks, contracts)
        unit_by_name = {unit.task_name: unit for unit in plan.units}
        pit_month_end_cutoff = self._freeze_pit_month_end_cutoff(
            datetime.combine(plan.effective_cutoff, datetime.min.time(), tzinfo=ZoneInfo("Asia/Shanghai"))
        )
        if pit_month_end_cutoff is not None:
            logger.info(
                "本批月度PIT任务冻结完整月末截止日: %s",
                pit_month_end_cutoff,
            )
        target_for_task = {task_name: target for target, task_name in TARGET_TO_TASK.items()}
        results_by_task: Dict[str, Dict[str, Any]] = {}

        semaphore = asyncio.Semaphore(max(int(self.max_workers or 1), 1))
        ledger_lock = asyncio.Lock()
        batch_id = uuid4()
        batch_started = await ledger_connection.fetchval('SELECT clock_timestamp()')

        async def finish_run(run_id, result, baseline_ready=False):
            status = 'cancelled' if result.get('status') == 'cancelled' else (
                'error' if self._is_failed_result(result) else 'success'
            )
            async with ledger_lock:
                await ledger_connection.execute(
                    """UPDATE pit.task_run SET status=$2, finished_at=clock_timestamp(),
                           baseline_ready=$3, result=$4::jsonb WHERE run_id=$1""",
                    run_id, status, status == 'success' and baseline_ready,
                    json.dumps(result, ensure_ascii=False, default=str),
                )

        async def _execute_task(task_name: str) -> Dict[str, Any]:
            run_started_at = datetime.now().astimezone().isoformat()
            if stop_event and stop_event.is_set():
                return {"task": task_name, "status": "cancelled", "committed_rows": 0, "plan_hash": plan.plan_hash}
            dependency_statuses = {
                dependency: (results_by_task.get(dependency) or {}).get("status", "missing")
                for dependency in contracts[task_name].dependencies
            }
            dependency_failures = [
                dependency
                for dependency in contracts[task_name].dependencies
                if self._is_failed_result(results_by_task.get(dependency))
            ]
            target = target_for_task.get(task_name, task_name)
            task_cutoff = (
                pit_month_end_cutoff
                if contracts[task_name].pit_time_key == "obs_date"
                else None
            )
            unit = unit_by_name[task_name]
            task_config = {
                **json.loads(unit.parameters_json), "pit_mode": plan.request.mode,
                "pit_business_date": plan.effective_cutoff.isoformat(), "plan_hash": plan.plan_hash,
            }
            if unit.start_date:
                task_config["start_date"] = unit.start_date.isoformat()
            if unit.end_date:
                task_config["end_date"] = unit.end_date.isoformat()
            if task_cutoff:
                task_config[PIT_MONTH_END_CUTOFF_CONFIG_KEY] = task_cutoff
            if dependency_failures:
                skipped = {
                    "target": target,
                    "task": task_name,
                    "status": "skipped_dependency_failed",
                    "failed_dependencies": dependency_failures,
                    "dependency_statuses": dependency_statuses,
                    "run_started_at": run_started_at,
                    "run_completed_at": datetime.now().astimezone().isoformat(),
                }
                if task_cutoff is not None:
                    skipped["pit_month_end_cutoff"] = task_cutoff
                return skipped
            run_id = uuid4()
            async with ledger_lock:
                await ledger_connection.execute(
                    """INSERT INTO pit.task_run
                       (run_id,batch_id,task_name,plan_hash,mode,started_at,start_date,end_date,status)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'running')""",
                    run_id, batch_id, task_name, plan.plan_hash, plan.request.mode,
                    batch_started, unit.start_date, unit.end_date,
                )
            try:
                if parallel:
                    async with semaphore:
                        result = await self._run_task(
                            task_name,
                            target,
                            update_type,
                            task_config=task_config,
                        )
                else:
                    result = await self._run_task(
                        task_name,
                        target,
                        update_type,
                        task_config=task_config,
                    )
                result.setdefault("plan_hash", plan.plan_hash)
                result['run_id'] = str(run_id)
                result['source_consumption'] = 'unverified'
                result.setdefault("dependency_statuses", dependency_statuses)
                if task_cutoff is not None:
                    result.setdefault("pit_month_end_cutoff", task_cutoff)
                result.setdefault("run_started_at", run_started_at)
                result.setdefault(
                    "run_completed_at", datetime.now().astimezone().isoformat()
                )
                if (plan.request.mode == 'full_backfill' and task_config.get('baseline_ready')
                        and not self._is_failed_result(result)
                        and not (result.get('committed_rows') or result.get('rows'))):
                    result.update(status='error', error='pit_baseline_unproven: full history produced no committed rows')
                await finish_run(run_id, result, bool(task_config.get('baseline_ready')))
                return result
            except asyncio.CancelledError:
                await finish_run(run_id, {'task': task_name, 'status': 'cancelled'})
                raise
            except Exception as exc:
                logger.error("PIT任务失败: %s: %s", target, exc, exc_info=True)
                failed = {
                    "target": target,
                    "task": task_name,
                    "status": "error",
                    "error": str(exc),
                    "dependency_statuses": dependency_statuses,
                    "run_started_at": run_started_at,
                    "run_completed_at": datetime.now().astimezone().isoformat(),
                }
                if task_cutoff is not None:
                    failed["pit_month_end_cutoff"] = task_cutoff
                await finish_run(run_id, failed)
                return failed

        for layer in layers:
            if parallel:
                workers = [asyncio.create_task(_execute_task(task_name)) for task_name in layer]
                try:
                    layer_results = await asyncio.gather(*workers)
                except BaseException:
                    # A cancelled/failed child must not release the pipeline
                    # lock while another synchronous manager is still writing.
                    for worker in workers:
                        worker.cancel()
                    drained = asyncio.gather(*workers, return_exceptions=True)
                    while not drained.done():
                        try:
                            await asyncio.shield(drained)
                        except asyncio.CancelledError:
                            continue
                    raise
            else:
                layer_results = []
                for task_name in layer:
                    layer_results.append(await _execute_task(task_name))
            for task_name, result in zip(layer, layer_results):
                results_by_task[task_name] = result

        return [results_by_task[task_name] for layer in layers for task_name in layer]

    async def update_income_data(self, mode: str = "incremental", **kwargs):
        return await self._run_target("income", self._update_type_from_mode(mode), kwargs)

    async def update_balance_data(self, mode: str = "incremental", **kwargs):
        return await self._run_target("balance", self._update_type_from_mode(mode), kwargs)

    async def update_cashflow_data(self, mode: str = "incremental", **kwargs):
        return await self._run_target("cashflow", self._update_type_from_mode(mode), kwargs)

    async def update_financial_indicators(self, mode: str = "incremental", **kwargs):
        return await self._run_target("financial_indicators", self._update_type_from_mode(mode), kwargs)

    async def update_industry_classification(self, mode: str = "incremental", **kwargs):
        return await self._run_target("industry_classification", self._update_type_from_mode(mode), kwargs)

    async def update_stock_fttm(self, mode: str = "incremental", **kwargs):
        return await self._run_target("stock_fttm", self._update_type_from_mode(mode), kwargs)

    async def update_stock_consensus_fy(self, mode: str = "incremental", **kwargs):
        return await self._run_target("stock_consensus_fy", self._update_type_from_mode(mode), kwargs)

    async def update_industry_fttm(self, mode: str = "incremental", **kwargs):
        return await self._run_target("industry_fttm", self._update_type_from_mode(mode), kwargs)

    async def update_industry_fapi(self, mode: str = "incremental", **kwargs):
        return await self._run_target("industry_fapi", self._update_type_from_mode(mode), kwargs)

    async def update_index_fttm(self, mode: str = "incremental", **kwargs):
        return await self._run_target("index_fttm", self._update_type_from_mode(mode), kwargs)

    async def update_etf_index_members(self, mode: str = "incremental", **kwargs):
        return await self._run_target(
            "etf_index_members", self._update_type_from_mode(mode), kwargs
        )

    async def update_etf_index_fapi(self, mode: str = "incremental", **kwargs):
        return await self._run_target(
            "etf_index_fapi", self._update_type_from_mode(mode), kwargs
        )

    async def update_etf_index_a_share_proxy_members(
        self, mode: str = "incremental", **kwargs
    ):
        return await self._run_target(
            "etf_index_a_share_proxy_members",
            self._update_type_from_mode(mode),
            kwargs,
        )

    async def update_etf_index_a_share_proxy_fapi(
        self, mode: str = "incremental", **kwargs
    ):
        return await self._run_target(
            "etf_index_a_share_proxy_fapi",
            self._update_type_from_mode(mode),
            kwargs,
        )

    async def update_earnings_surprise_annual(self, mode: str = "incremental", **kwargs):
        return await self._run_target("earnings_surprise_annual", self._update_type_from_mode(mode), kwargs)

    async def _run_target(
        self,
        target: str,
        update_type: str,
        task_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        config = dict(task_config or {})
        if set(config) - {"start_date", "end_date"}:
            raise ValueError("Compatibility target methods accept dates only; use an explicit domain plan for execution")
        mode = {UpdateTypes.SMART: "incremental", UpdateTypes.FULL: "full_backfill", UpdateTypes.MANUAL: "manual_range"}[update_type]
        results = await self.run_updates([target], mode, **config)
        return next(result for result in results if result["task"] == TARGET_TO_TASK[target])

    async def _run_task(
        self,
        task_name: str,
        target: str,
        update_type: str,
        task_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        task_class = UnifiedTaskFactory.get_tasks_by_type("pit")[task_name]
        if self._db_manager is None:
            raise RuntimeError("PIT coordinator has no bound database manager")
        task = task_class(self._db_manager, update_type=update_type, task_config=task_config or {})
        result = await task.execute()
        if not isinstance(result, dict):
            result = {"status": "error", "error": "PIT task returned an invalid result contract"}
        result.setdefault("target", target)
        result.setdefault("task", task_name)
        logger.info("PIT任务完成: target=%s, task=%s, result=%s", target, task_name, result)
        return result

    @staticmethod
    def _normalize_targets(targets: List[str]) -> List[str]:
        if not targets or "all" in targets:
            return list(DEFAULT_TARGET_ORDER)
        reverse = {value: key for key, value in TARGET_TO_TASK.items()}
        targets = [reverse.get(value, value) for value in targets]
        unknown = [target for target in targets if target not in TARGET_TO_TASK]
        if unknown:
            raise ValueError(f"未知PIT target: {unknown}")
        requested = list(dict.fromkeys(targets))
        return [target for target in DEFAULT_TARGET_ORDER if target in requested]

    @staticmethod
    def _freeze_pit_month_end_cutoff(reference_time: Optional[datetime] = None) -> str:
        started_at = reference_time or datetime.now().astimezone()
        return PITMonthlySnapshotManager.latest_complete_month(
            started_at.date()
        ).isoformat()

    @staticmethod
    def _registered_contracts() -> Dict[str, PITTaskContract]:
        from alphahome.pit.tasks import discover_tasks

        discover_tasks()
        task_classes = UnifiedTaskFactory.get_tasks_by_type("pit")
        contracts = {
            task_name: task_class.contract
            for task_name, task_class in task_classes.items()
            if isinstance(getattr(task_class, "contract", None), PITTaskContract)
        }
        return contracts

    @staticmethod
    def _expand_dependency_closure(
        requested_tasks: Sequence[str],
        contracts: Mapping[str, PITTaskContract],
    ) -> set[str]:
        selected: set[str] = set()

        def visit(task_name: str) -> None:
            if task_name in selected:
                return
            if task_name not in contracts:
                raise ValueError(f"PIT任务未注册或缺少contract: {task_name}")
            selected.add(task_name)
            for dependency in contracts[task_name].dependencies:
                visit(str(dependency))

        for task_name in requested_tasks:
            visit(task_name)
        return selected

    @staticmethod
    def _topological_layers(
        selected_tasks: set[str],
        contracts: Mapping[str, PITTaskContract],
    ) -> List[List[str]]:
        order_hint = {
            TARGET_TO_TASK[target]: index
            for index, target in enumerate(DEFAULT_TARGET_ORDER)
        }
        remaining = set(selected_tasks)
        completed: set[str] = set()
        layers: List[List[str]] = []
        while remaining:
            ready = [
                task_name
                for task_name in remaining
                if set(contracts[task_name].dependencies).intersection(selected_tasks)
                <= completed
            ]
            if not ready:
                cycle_nodes = sorted(remaining)
                raise ValueError(f"检测到PIT任务循环依赖: {cycle_nodes}")
            ready.sort(key=lambda name: (order_hint.get(name, len(order_hint)), name))
            layers.append(ready)
            completed.update(ready)
            remaining.difference_update(ready)
        return layers

    @staticmethod
    def _is_failed_result(result: Optional[Dict[str, Any]]) -> bool:
        if result is None:
            return True
        return result.get("status") not in {"success", "no_op", "expected_no_data"} or bool(result.get("error_records") or result.get("error"))


    @staticmethod
    def _update_type_from_mode(mode: str) -> str:
        if mode in ("incremental", "smart", UpdateTypes.SMART):
            return UpdateTypes.SMART
        if mode in ("full", "full_backfill", UpdateTypes.FULL):
            return UpdateTypes.FULL
        if mode in ("manual", "manual_range", UpdateTypes.MANUAL):
            return UpdateTypes.MANUAL
        raise ValueError(f"未知PIT更新模式: {mode}")


async def main():
    parser = argparse.ArgumentParser(description="PIT数据统一更新生产脚本")
    parser.add_argument(
        "--target",
        nargs="+",
        choices=list(TARGET_TO_TASK.keys()) + ["all"],
        default=["all"],
        help="要更新的目标数据类型",
    )
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental", help="更新模式")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--expected-plan-hash")
    parser.add_argument("--cutoff")
    parser.add_argument("--parallel", action="store_true", help="是否并行执行")
    parser.add_argument("--workers", type=int, default=2, help="最大并发任务数")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="日志级别")
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    coordinator = PITDataUpdateCoordinator(max_workers=args.workers)

    try:
        # Preview uses an owned read-only session and never initializes the task factory.
        coordinator.db_url = get_database_url()
        plan = await coordinator.plan(args.target, args.mode, cutoff=args.cutoff)
        print(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2))
        if args.dry_run:
            return 1 if plan.blockers else 0
        await coordinator.initialize()
        results = await coordinator.run_updates(args.target, args.mode, args.parallel, plan=plan,
                                                expected_plan_hash=args.expected_plan_hash)
        failures = [
            result
            for result in results
            if coordinator._is_failed_result(result)
        ]
        if failures:
            logger.error("PIT数据更新存在失败任务: %s", failures)
            sys.exit(1)
        logger.info("PIT数据更新执行完成")
        return 0
    except Exception as exc:
        logger.error("PIT数据更新执行失败: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        await coordinator.cleanup()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
