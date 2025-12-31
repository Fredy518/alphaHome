#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""P因子年度并行计算启动器

此脚本为生产可执行入口（subprocess 透传）。
本脚本包含原先包内模块的核心启动逻辑（已完成回迁）。

使用方法：
python scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation.py --start_year 2020 --end_year 2024 --workers 10

或者通过统一CLI：
ah prod run p-factor -- --start_year 2020 --end_year 2024 --workers 10
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[4]
WORKER_SCRIPT = PROJECT_ROOT / "scripts" / "production" / "factor_calculators" / "p_factor" / "p_factor_parallel_by_year.py"


def smart_year_allocation(years: List[int], workers: int) -> List[List[int]]:
    total_years = len(years)

    if total_years <= workers:
        allocation = [[year] for year in years]
        while len(allocation) < workers:
            allocation.append([])
        return allocation

    allocation: List[List[int]] = [[] for _ in range(workers)]
    years_sorted = sorted(years)

    base_years_per_worker = total_years // workers
    extra_years = total_years % workers

    year_idx = 0
    for worker_idx in range(workers):
        years_to_assign = base_years_per_worker
        if worker_idx < extra_years:
            years_to_assign += 1

        for _ in range(years_to_assign):
            if year_idx < total_years:
                allocation[worker_idx].append(years_sorted[year_idx])
                year_idx += 1

    return allocation


def start_worker_process(worker_id: int, start_year: int, end_year: int, total_workers: int) -> None:
    if os.name == 'nt':
        title = f"P-Factor-Worker-{worker_id}"
        cmd = (
            f"\"{sys.executable}\" \"{WORKER_SCRIPT}\" "
            f"--start_year {start_year} --end_year {end_year} "
            f"--worker_id {worker_id} --total_workers {total_workers}"
        )

        system_cmd = f'start "{title}" cmd /k "cd /d \\\"{PROJECT_ROOT}\\\" && {cmd}"'
        os.system(system_cmd)
    else:
        cmd = [
            sys.executable,
            str(WORKER_SCRIPT),
            "--start_year",
            str(start_year),
            "--end_year",
            str(end_year),
            "--worker_id",
            str(worker_id),
            "--total_workers",
            str(total_workers),
        ]
        subprocess.Popen([
            "gnome-terminal",
            "--title",
            f"P-Factor-Worker-{worker_id}",
            "--",
            "bash",
            "-c",
            f"cd \"{PROJECT_ROOT}\" && {' '.join(cmd)}; exec bash",
        ])


def run_parallel_p_factor_calculation(args: argparse.Namespace) -> int:
    start_year = args.start_year
    end_year = args.end_year
    workers = args.workers
    delay = getattr(args, 'delay', 2)

    if start_year > end_year:
        print(f"❌ start_year ({start_year}) 必须小于等于 end_year ({end_year})")
        return 1

    if workers <= 0:
        print(f"❌ workers ({workers}) 必须大于0")
        return 1

    total_years = end_year - start_year + 1
    if workers > total_years:
        print(f"⚠️ 工作进程数 ({workers}) 大于年份数 ({total_years})")
        print(f"🔧 自动调整工作进程数为: {total_years}")
        workers = total_years

    print(">>> P因子年度并行计算启动器")
    print("=" * 50)
    print(f"计算年份范围: {start_year}-{end_year}")
    print(f"工作进程数: {workers}")
    print(f"启动间隔: {delay}秒")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    years = list(range(start_year, end_year + 1))
    worker_years_list = smart_year_allocation(years, workers)

    print("年份分配 (智能分配):")
    for worker_id, worker_years in enumerate(worker_years_list):
        print(f"   进程{worker_id}: {worker_years}")
    print()

    print(">>> 启动工作进程...")
    for worker_id in range(workers):
        if worker_years_list[worker_id]:
            print(f"   启动进程 {worker_id}...")
            start_worker_process(worker_id, start_year, end_year, workers)

            if worker_id < workers - 1:
                time.sleep(delay)
        else:
            print(f"   跳过进程 {worker_id} (无分配年份)")

    print()
    print(">>> 所有工作进程已启动!")
    print()
    print("监控说明:")
    print("   - 每个终端窗口显示一个工作进程的进度")
    print("   - 可以随时关闭单个终端窗口来停止对应进程")
    print("   - 所有进程完成后，P因子数据将保存到数据库")
    print()
    print("性能预期:")
    estimated_time_per_year = 1.5
    total_estimated_time = len(years) * estimated_time_per_year / workers
    print(f"   - 预计总耗时: {total_estimated_time:.1f}小时 (并行)")
    print(f"   - 串行耗时: {len(years) * estimated_time_per_year:.1f}小时")
    print(f"   - 理论加速比: {workers}x")
    print(f"   - 年份分配: 每个进程负责 {len(years)/workers:.1f} 年")
    print()
    print("建议:")
    print("   - 监控数据库连接数，避免连接池耗尽")
    print("   - 定期检查磁盘空间，确保有足够存储空间")
    print("   - 可以随时调整工作进程数来平衡负载")

    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description='P因子年度并行计算启动器')
    parser.add_argument('--start_year', type=int, default=2020, help='开始年份 (默认: 2020)')
    parser.add_argument('--end_year', type=int, default=2024, help='结束年份 (默认: 2024)')
    parser.add_argument('--workers', type=int, default=10, help='工作进程数 (默认: 10)')
    parser.add_argument('--delay', type=int, default=2, help='进程启动间隔秒数 (默认: 2)')

    args = parser.parse_args(argv)
    return run_parallel_p_factor_calculation(args)


if __name__ == "__main__":
    sys.exit(main())
