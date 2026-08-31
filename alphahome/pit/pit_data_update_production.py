#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compatibility coordinator for production PIT updates."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

from alphahome.common.config_manager import get_database_url
from alphahome.common.constants import UpdateTypes
from alphahome.common.logging_utils import get_logger
from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.pit.base.pit_task import PITTaskContract

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

    def __init__(self, max_workers: int = 2, max_retries: int = 3, retry_delay: int = 5):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.db_url: Optional[str] = None

    async def initialize(self):
        from alphahome.pit.tasks import discover_tasks

        discover_tasks()
        self.db_url = get_database_url()
        if not self.db_url:
            raise ValueError("数据库连接配置未找到，请检查config.json文件")
        await UnifiedTaskFactory.initialize(self.db_url)
        logger.info("PIT数据更新协调器初始化完成")

    async def cleanup(self):
        try:
            await UnifiedTaskFactory.shutdown()
        except Exception as exc:
            logger.warning("关闭PIT任务工厂连接失败: %s", exc)

    async def run_updates(self, targets: List[str], mode: str = "incremental", parallel: bool = False):
        normalized_targets = self._normalize_targets(targets)
        update_type = self._update_type_from_mode(mode)
        logger.info("开始执行PIT任务: targets=%s, mode=%s, parallel=%s", normalized_targets, mode, parallel)
        contracts = self._registered_contracts()
        requested_tasks = [TARGET_TO_TASK[target] for target in normalized_targets]
        execution_tasks = self._expand_dependency_closure(requested_tasks, contracts)
        layers = self._topological_layers(execution_tasks, contracts)
        target_for_task = {task_name: target for target, task_name in TARGET_TO_TASK.items()}
        results_by_task: Dict[str, Dict[str, Any]] = {}

        semaphore = asyncio.Semaphore(max(int(self.max_workers or 1), 1))

        async def _execute_task(task_name: str) -> Dict[str, Any]:
            run_started_at = datetime.now().astimezone().isoformat()
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
            if dependency_failures:
                return {
                    "target": target,
                    "task": task_name,
                    "status": "skipped_dependency_failed",
                    "failed_dependencies": dependency_failures,
                    "dependency_statuses": dependency_statuses,
                    "run_started_at": run_started_at,
                    "run_completed_at": datetime.now().astimezone().isoformat(),
                }
            try:
                if parallel:
                    async with semaphore:
                        result = await self._run_task(task_name, target, update_type)
                else:
                    result = await self._run_task(task_name, target, update_type)
                result.setdefault("dependency_statuses", dependency_statuses)
                result.setdefault("run_started_at", run_started_at)
                result.setdefault(
                    "run_completed_at", datetime.now().astimezone().isoformat()
                )
                return result
            except Exception as exc:
                logger.error("PIT任务失败: %s: %s", target, exc, exc_info=True)
                return {
                    "target": target,
                    "task": task_name,
                    "status": "error",
                    "error": str(exc),
                    "dependency_statuses": dependency_statuses,
                    "run_started_at": run_started_at,
                    "run_completed_at": datetime.now().astimezone().isoformat(),
                }

        for layer in layers:
            if parallel:
                layer_results = await asyncio.gather(
                    *(_execute_task(task_name) for task_name in layer)
                )
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
        task_name = TARGET_TO_TASK[target]
        return await self._run_task(task_name, target, update_type, task_config)

    async def _run_task(
        self,
        task_name: str,
        target: str,
        update_type: str,
        task_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        task = await UnifiedTaskFactory.create_task_instance(
            task_name,
            update_type=update_type,
            task_config=task_config or {},
        )
        result = await task.execute()
        if not isinstance(result, dict):
            result = {"status": "success", "result": result}
        result.setdefault("target", target)
        result.setdefault("task", task_name)
        logger.info("PIT任务完成: target=%s, task=%s, result=%s", target, task_name, result)
        return result

    @staticmethod
    def _normalize_targets(targets: List[str]) -> List[str]:
        if not targets or "all" in targets:
            return list(DEFAULT_TARGET_ORDER)
        unknown = [target for target in targets if target not in TARGET_TO_TASK]
        if unknown:
            raise ValueError(f"未知PIT target: {unknown}")
        requested = list(dict.fromkeys(targets))
        return [target for target in DEFAULT_TARGET_ORDER if target in requested]

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
        return result.get("status") in {
            "error",
            "failed",
            "cancelled",
            "skipped_dependency_failed",
        }

    @staticmethod
    def _update_type_from_mode(mode: str) -> str:
        if mode in ("incremental", "smart", UpdateTypes.SMART):
            return UpdateTypes.SMART
        if mode in ("full", "full_backfill", UpdateTypes.FULL):
            return UpdateTypes.FULL
        if mode in ("manual", UpdateTypes.MANUAL):
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
    parser.add_argument("--parallel", action="store_true", help="是否并行执行")
    parser.add_argument("--workers", type=int, default=2, help="最大并发任务数")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="日志级别")
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    coordinator = PITDataUpdateCoordinator(max_workers=args.workers)

    try:
        await coordinator.initialize()
        results = await coordinator.run_updates(args.target, args.mode, args.parallel)
        failures = [
            result
            for result in results
            if isinstance(result, dict)
            and result.get("status")
            in {"error", "failed", "cancelled", "skipped_dependency_failed"}
        ]
        if failures:
            logger.error("PIT数据更新存在失败任务: %s", failures)
            sys.exit(1)
        logger.info("PIT数据更新执行完成")
    except Exception as exc:
        logger.error("PIT数据更新执行失败: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        await coordinator.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
