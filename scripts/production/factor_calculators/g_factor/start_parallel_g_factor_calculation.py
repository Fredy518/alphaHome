#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""G factor yearly launcher compatibility entrypoint."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.factors.pipelines.cli import launch_year_workers


def main() -> int:
    parser = argparse.ArgumentParser(description="G因子年度并行计算启动器")
    parser.add_argument("--start_year", type=int, default=2020, help="开始年份 (默认: 2020)")
    parser.add_argument("--end_year", type=int, default=2024, help="结束年份 (默认: 2024)")
    parser.add_argument("--workers", type=int, default=10, help="工作进程数 (默认: 10)")
    parser.add_argument("--delay", type=int, default=2, help="进程启动间隔秒数 (默认: 2)")
    args = parser.parse_args()
    return launch_year_workers("g", args.start_year, args.end_year, args.workers, args.delay)


if __name__ == "__main__":
    sys.exit(main())
