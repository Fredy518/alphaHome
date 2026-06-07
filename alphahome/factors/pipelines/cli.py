"""CLI helpers for factor compatibility scripts."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from alphahome.factors.core import GFactorCalculator, PFactorCalculator
from alphahome.factors.pipelines.factor_engine import (
    FactorEngine,
    FactorEngineConfig,
    Quarter,
    allocate_contiguous_balanced,
    generate_quarter_range,
    generate_quarters_for_years,
    parse_quarter,
    validate_date,
)


def calculator_for_factor(factor_type: str):
    factor_type = factor_type.lower()
    if factor_type == "p":
        return PFactorCalculator()
    if factor_type == "g":
        return GFactorCalculator()
    raise ValueError(f"unsupported factor_type: {factor_type}")


def run_specific_dates(factor_type: str, dates: Sequence[str], log_level: str = "INFO") -> int:
    logging.basicConfig(level=getattr(logging, log_level))
    config = FactorEngineConfig(
        factor_types=(factor_type,),
        dates=list(dates),
        mode="backfill",
        log_level=log_level,
    )
    result = FactorEngine(config).run()
    success_count = result.get("success_count", 0)
    failed_count = result.get("failed_count", 0)
    print(f"\n计算完成: 成功 {success_count} 只股票，失败 {failed_count} 只股票")
    return 0 if success_count > 0 else 1


def run_year_worker(factor_type: str, start_year: int, end_year: int, worker_id: int, total_workers: int) -> int:
    config = FactorEngineConfig(
        factor_types=(factor_type,),
        start_year=start_year,
        end_year=end_year,
        worker_id=worker_id,
        total_workers=total_workers,
        mode="backfill",
    )
    result = FactorEngine(config).run()
    _print_worker_summary(factor_type, worker_id, result)
    return 0


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
    config = FactorEngineConfig(
        factor_types=(factor_type,),
        quarter=list(quarter) if quarter else None,
        start_quarter=start_quarter,
        end_quarter=end_quarter,
        mode="backfill",
    )
    result = FactorEngine(config).run()
    _print_worker_summary(factor_type, worker_id, result)
    return 0


def run_missing_factors(
    start_date: str,
    end_date: str,
    log_level: str = "INFO",
    dry_run: bool = False,
    factor_types: Sequence[str] = ("p", "g"),
) -> int:
    logging.basicConfig(level=getattr(logging, log_level))
    config = FactorEngineConfig(
        factor_types=factor_types,
        start_date=start_date,
        end_date=end_date,
        missing_mode="batch_missing",
        mode="backfill",
        dry_run=dry_run,
        log_level=log_level,
    )
    engine = FactorEngine(config)
    missing_dates = engine.resolve_dates()

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

    result = _run_dates_p_then_g(missing_dates, factor_types, log_level)
    _print_missing_summary(result)
    return 0


def run_recent_missing_factors(months: int, log_level: str = "INFO", factor_types: Sequence[str] = ("p", "g")) -> int:
    logging.basicConfig(level=getattr(logging, log_level))
    config = FactorEngineConfig(
        factor_types=factor_types,
        missing_mode="recent_missing",
        months_back=months,
        mode="backfill",
        log_level=log_level,
    )
    engine = FactorEngine(config)
    missing_dates = engine.resolve_dates()

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

    result = _run_dates_p_then_g(missing_dates, factor_types, log_level)
    _print_missing_summary(result)
    return 0


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
    calculator = calculator_for_factor(factor_type)
    force_mode = None if mode == "auto" else mode

    detected_mode = force_mode or calculator.detect_execution_mode(start_date, end_date)
    calc_dates = calculator.generate_calculation_dates(start_date, end_date, detected_mode)

    if dry_run:
        print("试运行模式")
        print(f"计算范围: {start_date} ~ {end_date}")
        print(f"检测到执行模式: {detected_mode}")
        print(f"需要计算的日期数: {len(calc_dates)}")
        if calc_dates:
            print(f"日期范围: {calc_dates[0]} ~ {calc_dates[-1]}")
        print("试运行完成，配置正常")
        return 0

    if validate_only:
        calculator._validate_calculation_results(calc_dates)
        if validate_express_forecast_ratio and hasattr(calculator, "_validate_express_forecast_ratio"):
            calculator._validate_express_forecast_ratio(calc_dates)
        return 0

    method_name = "calculate_p_factors_batch_pit" if factor_type.lower() == "p" else "calculate_g_factors_batch_pit"
    result = getattr(calculator, method_name)(start_date=start_date, end_date=end_date, mode=force_mode)
    _print_range_summary(factor_type, result, log_file)
    return 0 if result.get("success_count", 0) > 0 else 1


def launch_year_workers(factor_type: str, start_year: int, end_year: int, workers: int, delay: int = 2) -> int:
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
    if (start_quarter or end_quarter) and (start_year is not None or end_year is not None):
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
        args = ["--worker_id", str(worker_id), "--total_workers", str(workers)] + quarter_args
        _start_worker(script, args, f"{factor_type.upper()}-Factor-Q-Worker-{worker_id}")
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


def _run_dates_p_then_g(dates: Sequence[str], factor_types: Sequence[str], log_level: str) -> dict:
    started_at = time.time()
    details = {}
    p_calculator = PFactorCalculator() if "p" in [value.lower() for value in factor_types] else None
    g_calculator = GFactorCalculator() if "g" in [value.lower() for value in factor_types] else None
    totals = {
        "successful_dates": 0,
        "failed_dates": 0,
        "total_p_success": 0,
        "total_p_failed": 0,
        "total_g_success": 0,
        "total_g_failed": 0,
    }

    for calc_date in dates:
        date_details = {}
        date_success = True
        for factor_type in factor_types:
            result = FactorEngine(
                FactorEngineConfig(
                    factor_types=(factor_type,),
                    dates=[calc_date],
                    mode="backfill",
                    log_level=log_level,
                ),
                p_calculator=p_calculator,
                g_calculator=g_calculator,
            ).run()
            success = int(result.get("success_count", 0))
            failed = int(result.get("failed_count", 0))
            date_details[factor_type] = result
            if factor_type.lower() == "p":
                totals["total_p_success"] += success
                totals["total_p_failed"] += failed
            elif factor_type.lower() == "g":
                totals["total_g_success"] += success
                totals["total_g_failed"] += failed
            if success <= 0:
                date_success = False

        details[calc_date] = date_details
        if date_success:
            totals["successful_dates"] += 1
        else:
            totals["failed_dates"] += 1

    totals["total_dates"] = len(dates)
    totals["total_time"] = time.time() - started_at
    totals["details"] = details
    return totals


def _print_grouped_dates(dates: Sequence[str], key_length: int) -> None:
    grouped = {}
    for value in dates:
        grouped.setdefault(value[:key_length], 0)
        grouped[value[:key_length]] += 1
    for key, count in grouped.items():
        print(f"   {key}: {count} 个日期")


def _print_missing_summary(result: dict) -> None:
    print("\n计算结果摘要:")
    print(f"   成功计算日期: {result['successful_dates']}")
    print(f"   失败日期: {result['failed_dates']}")
    print(f"   P因子成功: {result['total_p_success']}")
    print(f"   P因子失败: {result['total_p_failed']}")
    print(f"   G因子成功: {result['total_g_success']}")
    print(f"   G因子失败: {result['total_g_failed']}")
    print(f"   总耗时: {result['total_time']:.2f} 秒")


def _print_worker_summary(factor_type: str, worker_id: int, result: dict) -> None:
    print(f"{factor_type.upper()}因子 Worker {worker_id} 完成")
    print(f"  工作项: {result.get('work_item_count', 0)}")
    print(f"  成功: {result.get('success_count', 0):,}")
    print(f"  失败: {result.get('failed_count', 0):,}")
    print(f"  耗时: {result.get('total_time', 0):.2f} 秒")


def _print_range_summary(factor_type: str, result: dict, log_file: Optional[str] = None) -> None:
    print(f"{factor_type.upper()}因子计算完成")
    print(f"  总日期: {result.get('total_dates', 0)}")
    print(f"  成功日期: {result.get('successful_dates', 0)}")
    print(f"  失败日期: {result.get('failed_dates', 0)}")
    print(f"  成功: {result.get('success_count', 0):,}")
    print(f"  失败: {result.get('failed_count', 0):,}")
    print(f"  耗时: {result.get('total_time', 0):.2f} 秒")
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
    return root / "scripts" / "production" / "factor_calculators" / f"{factor_type}_factor" / filename


def _start_worker(script: Path, args: Sequence[str], title: str) -> None:
    command = [sys.executable, str(script), *args]
    if os.name == "nt":
        cmd_line = subprocess.list2cmdline(command)
        os.system(f'start "{title}" cmd /k "cd /d {Path.cwd()} && {cmd_line}"')
        return
    subprocess.Popen(command, cwd=Path.cwd())
