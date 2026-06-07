#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""G factor yearly worker compatibility entrypoint."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.factors.pipelines.cli import run_year_worker


def main() -> int:
    parser = argparse.ArgumentParser(description="G因子年度并行计算")
    parser.add_argument("--start_year", type=int, required=True, help="开始年份")
    parser.add_argument("--end_year", type=int, required=True, help="结束年份")
    parser.add_argument("--worker_id", type=int, required=True, help="工作进程ID (0-based)")
    parser.add_argument("--total_workers", type=int, required=True, help="总工作进程数")
    args = parser.parse_args()
    return run_year_worker("g", args.start_year, args.end_year, args.worker_id, args.total_workers)


if __name__ == "__main__":
    sys.exit(main())
