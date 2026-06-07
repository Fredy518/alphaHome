#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""P factor quarterly launcher compatibility entrypoint."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.factors.pipelines.cli import launch_quarter_workers


def main() -> int:
    parser = argparse.ArgumentParser(description="P因子季度并行计算启动器")
    parser.add_argument("--start_quarter", type=str, help="开始季度，格式: YYYYQN (例如: 2025Q3)")
    parser.add_argument("--end_quarter", type=str, help="结束季度，格式: YYYYQN (例如: 2025Q4)")
    parser.add_argument("--start_year", type=int, help="开始年份 (与季度参数互斥)")
    parser.add_argument("--end_year", type=int, help="结束年份 (与季度参数互斥)")
    parser.add_argument("--workers", type=int, default=16, help="工作进程数 (默认: 16)")
    parser.add_argument("--delay", type=int, default=2, help="进程启动间隔秒数 (默认: 2)")
    args = parser.parse_args()
    return launch_quarter_workers(
        "p",
        args.workers,
        args.delay,
        start_year=args.start_year,
        end_year=args.end_year,
        start_quarter=args.start_quarter,
        end_quarter=args.end_quarter,
    )


if __name__ == "__main__":
    sys.exit(main())
