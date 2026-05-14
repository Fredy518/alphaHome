"""Run Tinysoft InfoArray tasks in FULL update mode (sequential).

Usage (from repo root):

    python scripts/run_tinysoft_infoarray_full.py
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import alphahome.fetchers.tasks  # noqa: F401 — register tasks

from alphahome.common.constants import UpdateTypes
from alphahome.common.task_system.task_factory import UnifiedTaskFactory

TASKS = (
    "tinysoft_stock_suspend",
    "tinysoft_stock_industry_versioned",
    "tinysoft_stock_fina_pit_ext",
)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    await UnifiedTaskFactory.initialize()
    try:
        for name in TASKS:
            logging.info("FULL: starting %s", name)
            task = await UnifiedTaskFactory.create_task_instance(
                name, update_type=UpdateTypes.FULL
            )
            result = await task.execute()
            logging.info("FULL: finished %s -> %s", name, result)
    finally:
        await UnifiedTaskFactory.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
