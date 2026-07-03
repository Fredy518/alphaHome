#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compatibility coordinator for production PIT updates."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any, Dict, List, Optional

from alphahome.common.config_manager import get_database_url
from alphahome.common.constants import UpdateTypes
from alphahome.common.logging_utils import get_logger
from alphahome.common.task_system import UnifiedTaskFactory

logger = get_logger(__name__)


TARGET_TO_TASK = {
    "income": "pit_income_quarterly",
    "balance": "pit_balance_quarterly",
    "cashflow": "pit_cashflow_quarterly",
    "financial_indicators": "pit_financial_indicators",
    "industry_classification": "pit_industry_classification",
}

DEFAULT_TARGET_ORDER = [
    "income",
    "balance",
    "cashflow",
    "financial_indicators",
    "industry_classification",
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

        if parallel and "financial_indicators" in normalized_targets and {"income", "balance"}.intersection(normalized_targets):
            logger.warning("financial_indicators 与 income/balance 存在依赖，同批执行时禁用并行")
            parallel = False

        if parallel:
            semaphore = asyncio.Semaphore(max(int(self.max_workers or 1), 1))

            async def _guarded(target: str):
                async with semaphore:
                    return await self._run_target(target, update_type)

            results = await asyncio.gather(*(_guarded(target) for target in normalized_targets), return_exceptions=True)
            failures = [
                (target, result)
                for target, result in zip(normalized_targets, results)
                if isinstance(result, Exception) or (isinstance(result, dict) and result.get("status") == "error")
            ]
            if failures:
                for target, result in failures:
                    logger.error("PIT任务失败: %s: %s", target, result)
                raise RuntimeError("PIT并行更新失败: " + ", ".join(target for target, _ in failures))
            return results

        results = []
        for target in normalized_targets:
            try:
                results.append(await self._run_target(target, update_type))
            except Exception as exc:
                logger.error("PIT任务失败: %s: %s", target, exc, exc_info=True)
                results.append({"target": target, "status": "error", "error": str(exc)})
        return results

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

    async def _run_target(
        self,
        target: str,
        update_type: str,
        task_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        task_name = TARGET_TO_TASK[target]
        task = await UnifiedTaskFactory.create_task_instance(
            task_name,
            update_type=update_type,
            task_config=task_config or {},
        )
        result = await task.execute()
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
        failures = [result for result in results if isinstance(result, dict) and result.get("status") == "error"]
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
