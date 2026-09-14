"""CLI helpers for factor compatibility scripts."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from alphahome.factors.core import GFactorCalculator, PFactorCalculator
from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager
from alphahome.factors.coordinator import FactorCoordinator
from alphahome.factors.date_policy import FactorDatePolicy
from alphahome.factors.pipelines.factor_engine import (
    Quarter,
    allocate_contiguous_balanced,
    generate_quarter_range,
    generate_quarters_for_years,
    parse_quarter,
    shard_items,
    validate_date,
)


def _governed_operation(
    factor_types: Sequence[str],
    mode: str,
    start_date: str,
    end_date: str,
    *,
    dry_run: bool = False,
    max_automatic_dates: int = 26,
) -> Dict[str, Any]:
    """Compatibility bridge: every production write goes through one coordinator."""
    task_names = [f"factor_{value.lower()}" for value in factor_types]
    db_url = ConfigManager().get_database_url()
    if not db_url:
        raise RuntimeError("数据库连接未配置")
    db = DBManager(db_url, mode="sync")
    try:
        coordinator = FactorCoordinator(db, max_automatic_dates=max_automatic_dates)
        operation = coordinator.plan if dry_run else coordinator.run
        return operation(
            task_names,
            mode=mode,
            date_range=(start_date, end_date),
            expand_dependencies=True,
        ).to_dict()
    finally:
        db.close_sync()


def _planned_dates(payload: Mapping[str, Any]) -> List[str]:
    return sorted(
        {
            value
            for task_plan in payload.get("task_plans", [])
            for value in task_plan.get("dates", [])
        }
    )


def calculator_for_factor(factor_type: str):
    factor_type = factor_type.lower()
    if factor_type == "p":
        return PFactorCalculator()
    if factor_type == "g":
        return GFactorCalculator()
    raise ValueError(f"unsupported factor_type: {factor_type}")


def run_specific_dates(
    factor_type: str, dates: Sequence[str], log_level: str = "INFO"
) -> int:
    logging.basicConfig(level=getattr(logging, log_level))
    normalized = FactorDatePolicy.normalize_many(dates)
    results = [
        _governed_operation(
            (factor_type,),
            "manual",
            value.isoformat(),
            value.isoformat(),
        )
        for value in normalized
    ]
    output_count = sum(int(item.get("output_count") or 0) for item in results)
    failed_dates = sum(int(item.get("failed_date_count") or 0) for item in results)
    print(f"\n计算完成: 输出 {output_count} 行，失败日期 {failed_dates} 个")
    return (
        0 if results and all(item.get("status") == "success" for item in results) else 1
    )


def run_year_worker(
    factor_type: str, start_year: int, end_year: int, worker_id: int, total_workers: int
) -> int:
    years = allocate_contiguous_balanced(
        list(range(start_year, end_year + 1)), total_workers
    )
    if worker_id < 0 or worker_id >= len(years):
        raise ValueError("worker_id must be in [0, total_workers)")
    results = [
        _governed_operation((factor_type,), "manual", f"{year}-01-01", f"{year}-12-31")
        for year in years[worker_id]
    ]
    result = _combine_governed_results(results)
    _print_worker_summary(factor_type, worker_id, result)
    return 0 if result.get("failed_date_count", 0) == 0 else 1


def run_quarter_worker(
    factor_type: str,
    worker_id: int,
    total_workers: int,
    quarter: Optional[Sequence[str]] = None,
    start_quarter: Optional[str] = None,
    end_quarter: Optional[str] = None,
) -> int:
    if total_workers <= 0:
        raise ValueError("total_workers must be > 0")
    if worker_id < 0 or worker_id >= total_workers:
        raise ValueError("worker_id must be in [0, total_workers)")
    quarters = resolve_quarter_args(quarter, start_quarter, end_quarter)
    worker_quarters = shard_items(quarters, worker_id, total_workers)
    results = [
        _governed_operation((factor_type,), "manual", *item.date_range)
        for item in worker_quarters
    ]
    result = _combine_governed_results(results)
    _print_worker_summary(factor_type, worker_id, result)
    return 0 if result.get("failed_date_count", 0) == 0 else 1


def run_missing_factors(
    start_date: str,
    end_date: str,
    log_level: str = "INFO",
    dry_run: bool = False,
    factor_types: Sequence[str] = ("p", "g"),
) -> int:
    logging.basicConfig(level=getattr(logging, log_level))
    preview = _governed_operation(
        factor_types,
        "smart",
        start_date,
        end_date,
        dry_run=True,
        max_automatic_dates=10000,
    )
    if preview.get("status") != "ready":
        print(f"因子预检未通过: {preview.get('status')} {preview.get('message', '')}")
        return 2
    missing_dates = _planned_dates(preview)

    if not missing_dates:
        print("没有发现缺失的因子数据")
        return 0

    print("\n缺失数据分析:")
    print(f"   总缺失日期: {len(missing_dates)}")
    _print_grouped_dates(missing_dates, 4)

    if dry_run:
        print("\n这是预览模式，没有实际计算")
        return 0

    confirm = input(f"\n是否开始计算 {len(missing_dates)} 个日期的因子数据? (y/N): ")
    if confirm.lower() != "y":
        print("已取消")
        return 0

    result = _governed_operation(
        factor_types,
        "smart",
        start_date,
        end_date,
        max_automatic_dates=10000,
    )
    _print_governed_summary(result)
    return 0 if result.get("status") == "success" else 1


def run_recent_missing_factors(
    months: int, log_level: str = "INFO", factor_types: Sequence[str] = ("p", "g")
) -> int:
    logging.basicConfig(level=getattr(logging, log_level))
    if months <= 0:
        raise ValueError("months must be > 0")
    cutoff = FactorDatePolicy().automatic_cutoff()
    start_date = cutoff - timedelta(days=months * 31)
    preview = _governed_operation(
        factor_types,
        "smart",
        start_date.isoformat(),
        cutoff.isoformat(),
        dry_run=True,
        max_automatic_dates=10000,
    )
    if preview.get("status") != "ready":
        print(f"因子预检未通过: {preview.get('status')} {preview.get('message', '')}")
        return 2
    missing_dates = _planned_dates(preview)

    if not missing_dates:
        print("没有发现缺失的因子数据")
        return 0

    print("\n缺失数据分析:")
    print(f"   总缺失日期: {len(missing_dates)}")
    _print_grouped_dates(missing_dates, 7)

    confirm = input(f"\n是否开始计算 {len(missing_dates)} 个日期的因子数据? (y/N): ")
    if confirm.lower() != "y":
        print("已取消")
        return 0

    result = _governed_operation(
        factor_types,
        "smart",
        start_date.isoformat(),
        cutoff.isoformat(),
        max_automatic_dates=10000,
    )
    _print_governed_summary(result)
    return 0 if result.get("status") == "success" else 1


def run_range(
    factor_type: str,
    start_date: str,
    end_date: str,
    mode: str = "auto",
    dry_run: bool = False,
    validate_only: bool = False,
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    validate_express_forecast_ratio: bool = False,
) -> int:
    logging.basicConfig(level=getattr(logging, log_level))
    validate_date(start_date)
    validate_date(end_date)
    governed_mode = {
        "auto": "smart",
        "incremental": "smart",
        "backfill": "manual",
    }[mode]

    if dry_run:
        result = _governed_operation(
            (factor_type,),
            governed_mode,
            start_date,
            end_date,
            dry_run=True,
            max_automatic_dates=10000,
        )
        calc_dates = _planned_dates(result)
        print("试运行模式")
        print(f"计算范围: {start_date} ~ {end_date}")
        print(f"治理执行模式: {governed_mode}")
        print(f"预检状态: {result.get('status')}")
        print(f"需要计算的日期数: {len(calc_dates)}")
        if calc_dates:
            print(f"日期范围: {calc_dates[0]} ~ {calc_dates[-1]}")
        print("试运行完成，配置正常")
        return 0

    if validate_only:
        calculator = calculator_for_factor(factor_type)
        calc_dates = calculator.generate_calculation_dates(
            start_date, end_date, "backfill"
        )
        calculator._validate_calculation_results(calc_dates)
        if validate_express_forecast_ratio and hasattr(
            calculator, "_validate_express_forecast_ratio"
        ):
            calculator._validate_express_forecast_ratio(calc_dates)
        return 0

    result = _governed_operation(
        (factor_type,),
        governed_mode,
        start_date,
        end_date,
        max_automatic_dates=10000,
    )
    _print_range_summary(factor_type, result, log_file)
    return 0 if result.get("status") == "success" else 1


def launch_year_workers(
    factor_type: str, start_year: int, end_year: int, workers: int, delay: int = 2
) -> int:
    if start_year > end_year:
        raise ValueError("start_year must be <= end_year")
    if workers <= 0:
        raise ValueError("workers must be > 0")

    script = _script_path(factor_type, "year")
    print(f"{factor_type.upper()}因子年度并行计算启动器")
    print(f"年份范围: {start_year}-{end_year}")
    print(f"工作进程数: {workers}")

    for worker_id in range(workers):
        args = [
            "--start_year",
            str(start_year),
            "--end_year",
            str(end_year),
            "--worker_id",
            str(worker_id),
            "--total_workers",
            str(workers),
        ]
        _start_worker(script, args, f"{factor_type.upper()}-Factor-Worker-{worker_id}")
        if worker_id < workers - 1:
            time.sleep(delay)
    return 0


def launch_quarter_workers(
    factor_type: str,
    workers: int,
    delay: int = 2,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    start_quarter: Optional[str] = None,
    end_quarter: Optional[str] = None,
) -> int:
    if workers <= 0:
        raise ValueError("workers must be > 0")
    if (start_quarter or end_quarter) and (
        start_year is not None or end_year is not None
    ):
        raise ValueError("quarter range and year range are mutually exclusive")
    if bool(start_quarter) != bool(end_quarter):
        raise ValueError("start_quarter and end_quarter must be provided together")
    if bool(start_year is not None) != bool(end_year is not None):
        raise ValueError("start_year and end_year must be provided together")

    if start_quarter and end_quarter:
        quarters = generate_quarter_range(start_quarter, end_quarter)
    elif start_year is not None and end_year is not None:
        quarters = generate_quarters_for_years(start_year, end_year)
    else:
        quarters = generate_quarters_for_years(2020, 2024)

    if not quarters:
        print("没有需要计算的季度")
        return 0
    if workers > len(quarters):
        workers = len(quarters)

    allocation = allocate_contiguous_balanced(quarters, workers)
    script = _script_path(factor_type, "quarter")
    print(f"{factor_type.upper()}因子季度并行计算启动器")
    print(f"季度数: {len(quarters)}")
    print(f"工作进程数: {workers}")

    for worker_id, worker_quarters in enumerate(allocation):
        quarter_args: List[str] = []
        for quarter in worker_quarters:
            quarter_args.extend(["--quarter", quarter.label])
        args = [
            "--worker_id",
            str(worker_id),
            "--total_workers",
            str(workers),
        ] + quarter_args
        _start_worker(
            script, args, f"{factor_type.upper()}-Factor-Q-Worker-{worker_id}"
        )
        if worker_id < workers - 1:
            time.sleep(delay)
    return 0


def resolve_quarter_args(
    quarter: Optional[Sequence[str]],
    start_quarter: Optional[str],
    end_quarter: Optional[str],
) -> List[Quarter]:
    if quarter and (start_quarter or end_quarter):
        raise ValueError("quarter cannot be combined with start_quarter/end_quarter")
    if quarter:
        return sorted(parse_quarter(value) for value in quarter)
    if start_quarter and end_quarter:
        return generate_quarter_range(start_quarter, end_quarter)
    raise ValueError("must provide quarter or start_quarter/end_quarter")


def _run_dates_p_then_g(
    dates: Sequence[str], factor_types: Sequence[str], log_level: str
) -> dict:
    started_at = time.time()
    results = [
        _governed_operation(factor_types, "manual", calc_date, calc_date)
        for calc_date in FactorDatePolicy.normalize_many(dates)
    ]
    totals = _combine_governed_results(results)
    totals["total_time"] = time.time() - started_at
    return totals


def _combine_governed_results(results: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    return {
        "work_item_count": len(results),
        "planned_date_count": sum(
            int(item.get("planned_date_count") or 0) for item in results
        ),
        "successful_date_count": sum(
            int(item.get("successful_date_count") or 0) for item in results
        ),
        "failed_date_count": sum(
            int(item.get("failed_date_count") or 0) for item in results
        ),
        "skipped_date_count": sum(
            int(item.get("skipped_date_count") or 0) for item in results
        ),
        "output_count": sum(int(item.get("output_count") or 0) for item in results),
        "status": (
            "success"
            if results and all(item.get("status") == "success" for item in results)
            else "error"
        ),
        "details": list(results),
    }


def _print_grouped_dates(dates: Sequence[str], key_length: int) -> None:
    grouped = {}
    for value in dates:
        grouped.setdefault(value[:key_length], 0)
        grouped[value[:key_length]] += 1
    for key, count in grouped.items():
        print(f"   {key}: {count} 个日期")


def _print_missing_summary(result: dict) -> None:
    _print_governed_summary(result)


def _print_governed_summary(result: Mapping[str, Any]) -> None:
    print("\n治理运行结果摘要:")
    print(f"   状态: {result.get('status')}")
    print(f"   计划任务日期: {result.get('planned_date_count', 0)}")
    print(f"   成功日期: {result.get('successful_date_count', 0)}")
    print(f"   失败日期: {result.get('failed_date_count', 0)}")
    print(f"   跳过日期: {result.get('skipped_date_count', 0)}")
    print(f"   输出行数: {result.get('output_count', 0)}")


def _print_worker_summary(factor_type: str, worker_id: int, result: dict) -> None:
    print(f"{factor_type.upper()}因子 Worker {worker_id} 完成")
    print(f"  工作项: {result.get('work_item_count', 0)}")
    print(f"  成功日期: {result.get('successful_date_count', 0):,}")
    print(f"  失败日期: {result.get('failed_date_count', 0):,}")
    print(f"  输出行数: {result.get('output_count', 0):,}")


def _print_range_summary(
    factor_type: str, result: dict, log_file: Optional[str] = None
) -> None:
    print(f"{factor_type.upper()}因子计算完成")
    print(f"  状态: {result.get('status')}")
    print(f"  总任务日期: {result.get('planned_date_count', 0)}")
    print(f"  成功日期: {result.get('successful_date_count', 0)}")
    print(f"  失败日期: {result.get('failed_date_count', 0)}")
    print(f"  输出行数: {result.get('output_count', 0):,}")
    if log_file:
        print(f"  日志文件: {log_file}")


def _script_path(factor_type: str, mode: str) -> Path:
    root = Path.cwd()
    factor_type = factor_type.lower()
    if mode == "year":
        filename = f"{factor_type}_factor_parallel_by_year.py"
    elif mode == "quarter":
        filename = f"{factor_type}_factor_parallel_by_quarter.py"
    else:
        raise ValueError(f"unknown script mode: {mode}")
    return (
        root
        / "scripts"
        / "production"
        / "factor_calculators"
        / f"{factor_type}_factor"
        / filename
    )


def _start_worker(script: Path, args: Sequence[str], title: str) -> None:
    command = [sys.executable, str(script), *args]
    if os.name == "nt":
        cmd_line = subprocess.list2cmdline(command)
        os.system(f'start "{title}" cmd /k "cd /d {Path.cwd()} && {cmd_line}"')
        return
    subprocess.Popen(command, cwd=Path.cwd())
