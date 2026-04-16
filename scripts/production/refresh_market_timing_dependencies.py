#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import alphahome.fetchers  # noqa: F401  # 触发任务注册
from alphahome.common.constants import UpdateTypes
from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.fetchers.tools.calendar import get_last_trade_day, is_trade_day


logger = logging.getLogger("market_timing_dependency_refresh")


@dataclass(frozen=True)
class TaskSpec:
    task_name: str
    freshness_mode: str
    update_type: str = UpdateTypes.SMART
    max_age_days: int | None = None
    data_lag_trade_days: int = 0


PROFILE_TASKS: Dict[str, List[TaskSpec]] = {
    "alphasniper": [
        TaskSpec("tushare_stock_dailybasic", "data_date_today"),
    ],
    "betanavigator": [
        TaskSpec("tushare_stock_dailybasic", "data_date_today"),
        TaskSpec("tushare_stock_margin", "data_date_trade_lag", data_lag_trade_days=1),
        TaskSpec("tushare_future_daily", "data_date_today"),
        TaskSpec("tushare_future_holding", "data_date_today"),
        TaskSpec("tushare_index_weight", "same_month"),
        TaskSpec("tushare_index_swmember", "recent_update", max_age_days=30),
    ],
}


def format_date(value: date | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value.date()
    return value.isoformat()


async def get_task_instance(task_name: str, update_type: str):
    return await UnifiedTaskFactory.create_task_instance(task_name, update_type=update_type)


async def resolve_expected_market_date(target_date: date) -> date:
    target_text = target_date.strftime("%Y%m%d")
    if await is_trade_day(target_text):
        return target_date
    last_trade_day = await get_last_trade_day(target_text, n=1)
    if not last_trade_day:
        return target_date
    return datetime.strptime(last_trade_day, "%Y%m%d").date()


async def resolve_trade_lag_threshold(base_date: date, lag_trade_days: int) -> date:
    if lag_trade_days <= 0:
        return base_date
    threshold = await get_last_trade_day(base_date.strftime("%Y%m%d"), n=lag_trade_days)
    if not threshold:
        return base_date
    return datetime.strptime(threshold, "%Y%m%d").date()


async def get_latest_update_date(task: Any) -> date | None:
    table_name = task.get_full_table_name()
    if not await task.db.table_exists(table_name):
        return None
    row = await task.db.fetch_one(f"SELECT MAX(update_time) AS latest_update FROM {table_name}")
    if not row or not row["latest_update"]:
        return None
    latest = row["latest_update"]
    if isinstance(latest, datetime):
        return latest.date()
    if isinstance(latest, date):
        return latest
    return datetime.fromisoformat(str(latest)).date()


async def snapshot_task(spec: TaskSpec, target_date: date, expected_market_date: date) -> Dict[str, Any]:
    task = await get_task_instance(spec.task_name, spec.update_type)
    latest_date: date | None = None
    threshold: str | None = None
    stale_reason = ""

    if spec.freshness_mode == "data_date_today":
        latest_date = await task.get_latest_date()
        is_stale = latest_date is None or latest_date < expected_market_date
        threshold = expected_market_date.isoformat()
        stale_reason = "latest data date is before expected market date"
    elif spec.freshness_mode == "data_date_trade_lag":
        latest_date = await task.get_latest_date()
        threshold_date = await resolve_trade_lag_threshold(
            expected_market_date,
            int(spec.data_lag_trade_days),
        )
        is_stale = latest_date is None or latest_date < threshold_date
        threshold = threshold_date.isoformat()
        stale_reason = "latest data date is behind allowed trade-day lag threshold"
    elif spec.freshness_mode == "same_month":
        latest_date = await task.get_latest_date()
        is_stale = latest_date is None or latest_date.year != target_date.year or latest_date.month != target_date.month
        threshold = f"{target_date.year:04d}-{target_date.month:02d}"
        stale_reason = "latest data month is behind current month"
    elif spec.freshness_mode == "recent_update":
        latest_date = await get_latest_update_date(task)
        max_age_days = int(spec.max_age_days or 0)
        threshold_date = target_date - timedelta(days=max_age_days)
        is_stale = latest_date is None or latest_date < threshold_date
        threshold = threshold_date.isoformat()
        stale_reason = f"latest update is older than {max_age_days} days"
    else:
        raise ValueError(f"Unsupported freshness_mode: {spec.freshness_mode}")

    return {
        "task_name": spec.task_name,
        "freshness_mode": spec.freshness_mode,
        "update_type": spec.update_type,
        "latest_value": format_date(latest_date),
        "threshold": threshold,
        "stale": bool(is_stale),
        "reason": stale_reason if is_stale else "fresh",
    }


async def collect_profile_status(profile: str, target_date: date) -> Dict[str, Any]:
    if profile not in PROFILE_TASKS:
        raise ValueError(f"Unsupported profile: {profile}")
    expected_market_date = await resolve_expected_market_date(target_date)
    items = [await snapshot_task(spec, target_date, expected_market_date) for spec in PROFILE_TASKS[profile]]
    return {
        "profile": profile,
        "target_date": target_date.isoformat(),
        "expected_market_date": expected_market_date.isoformat(),
        "needs_refresh": any(item["stale"] for item in items),
        "items": items,
    }


async def refresh_profile(profile: str, target_date: date, force: bool = False) -> Dict[str, Any]:
    before = await collect_profile_status(profile, target_date)
    stale_task_names = {item["task_name"] for item in before["items"] if item["stale"]}

    refresh_results: List[Dict[str, Any]] = []
    for spec in PROFILE_TASKS[profile]:
        should_run = force or spec.task_name in stale_task_names
        if not should_run:
            continue

        logger.info("Running task %s with update_type=%s", spec.task_name, spec.update_type)
        task = await get_task_instance(spec.task_name, spec.update_type)
        result = await task.execute(stop_event=None)
        refresh_results.append(
            {
                "task_name": spec.task_name,
                "update_type": spec.update_type,
                "result": result,
            }
        )

    after = await collect_profile_status(profile, target_date)
    return {
        "profile": profile,
        "target_date": target_date.isoformat(),
        "before": before,
        "refresh_results": refresh_results,
        "after": after,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check or refresh AlphaHome dependencies used by Market择时 pipelines."
    )
    parser.add_argument("--profile", required=True, choices=sorted(PROFILE_TASKS))
    parser.add_argument("--mode", choices=["check", "refresh"], default="check")
    parser.add_argument("--target-date", help="Override target date in YYYY-MM-DD format.")
    parser.add_argument("--force", action="store_true", help="Refresh all tasks in the profile.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON only.")
    return parser


def resolve_target_date(raw: str | None) -> date:
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    return datetime.now().date()


async def async_main(args: argparse.Namespace) -> Dict[str, Any]:
    target_date = resolve_target_date(args.target_date)
    await UnifiedTaskFactory.initialize()
    try:
        if args.mode == "refresh":
            return await refresh_profile(args.profile, target_date, force=bool(args.force))
        return await collect_profile_status(args.profile, target_date)
    finally:
        await UnifiedTaskFactory.shutdown()


def emit_payload(payload: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, default=str))
        return

    if "items" in payload:
        print(
            f"profile={payload['profile']} target_date={payload['target_date']} "
            f"expected_market_date={payload.get('expected_market_date')} needs_refresh={payload['needs_refresh']}"
        )
        for item in payload["items"]:
            print(
                f"  - {item['task_name']}: stale={item['stale']} "
                f"latest={item['latest_value']} threshold={item['threshold']} mode={item['freshness_mode']}"
            )
        return

    before = payload["before"]
    after = payload["after"]
    print(f"profile={payload['profile']} target_date={payload['target_date']}")
    print(f"before needs_refresh={before['needs_refresh']}")
    for item in before["items"]:
        print(
            f"  - before {item['task_name']}: stale={item['stale']} "
            f"latest={item['latest_value']} threshold={item['threshold']}"
        )
    print(f"refresh_results={len(payload['refresh_results'])}")
    for item in payload["refresh_results"]:
        print(f"  - ran {item['task_name']} update_type={item['update_type']} result={item['result']}")
    print(f"after needs_refresh={after['needs_refresh']}")
    for item in after["items"]:
        print(
            f"  - after {item['task_name']}: stale={item['stale']} "
            f"latest={item['latest_value']} threshold={item['threshold']}"
        )


def main(argv: List[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        payload = asyncio.run(async_main(args))
    except KeyboardInterrupt:
        logger.error("Interrupted by user.")
        return 130
    except Exception as exc:
        logger.exception("Failed to run dependency refresh.")
        if args.json:
            print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 1

    emit_payload(payload, as_json=bool(args.json))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
